#include "edge_runtime/transport/fd_broker.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>

#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/utility/checked_math.hpp"
#include "edge_runtime/utility/checksum.hpp"
#include "edge_runtime/utility/failpoint.hpp"
#include "edge_runtime/transport/socket_io.hpp"

#if defined(__linux__)
#define EDGE_HAVE_MEMFD_CREATE 1
#include <linux/memfd.h>
#include <sys/syscall.h>
#endif

namespace edge_runtime::detail {

namespace {

inline constexpr uint32_t kListenBacklog = 4;
inline constexpr uint64_t kConnectRetryIntervalNs = 10'000'000;
inline constexpr uint64_t kServePollIntervalNs = 10'000'000;
using socket_io::IoBudget;
using socket_io::budget_available;
using socket_io::wait_socket;
using socket_io::recv_full;
using socket_io::send_full;
using socket_io::peer_is_same_uid;

bool retry_pause(const IoBudget& budget) noexcept {
	const uint64_t remaining = remaining_time_ns(budget.deadline_ns);
	if (remaining == 0) return false;
	const uint64_t pause_end =
	        budget.deadline_ns - remaining + std::min(remaining, kConnectRetryIntervalNs);
	const struct timespec until {
		static_cast<time_t>(pause_end / 1'000'000'000),
		        static_cast<long>(pause_end % 1'000'000'000)
	};
	while (budget_available(budget)) {
		const int result =
		        ::clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &until, nullptr);
		if (result == 0) return budget_available(budget);
		if (result != EINTR) return false;
	}
	return false;
}

uint64_t reply_checksum_of(const FdBrokerReplyAbi& r) noexcept {
	FdBrokerReplyAbi tmp = r;
	tmp.checksum = 0;
	return fnv1a64(reinterpret_cast<const std::byte*>(&tmp), sizeof(tmp));
}

uint64_t request_checksum_of(const FdBrokerRequestAbi& r) noexcept {
	FdBrokerRequestAbi tmp = r;
	tmp.checksum = 0;
	return fnv1a64(reinterpret_cast<const std::byte*>(&tmp), sizeof(tmp));
}

bool request_fingerprint_set(const FdBrokerRequestAbi& r) noexcept {
	for (const uint8_t b : r.schema_fingerprint) {
		if (b != 0) return true;
	}
	return false;
}

bool socket_has_live_peer(const std::string& path) noexcept {
	const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
	if (fd < 0) return true;
	UniqueFd probe(fd);
	struct sockaddr_un addr {};
	addr.sun_family = AF_UNIX;
	if (path.size() >= sizeof(addr.sun_path)) return false;
	std::memcpy(addr.sun_path, path.c_str(), path.size() + 1);
	const int rc = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr),
	                         static_cast<socklen_t>(sizeof(addr)));
	return rc == 0 || (errno != ECONNREFUSED && errno != ENOENT);
}

bool unlink_socket(const std::string& path, uint64_t device = 0, uint64_t inode = 0) noexcept {
	struct stat observed {};
	if (::lstat(path.c_str(), &observed) != 0) return errno == ENOENT;
	if (!S_ISSOCK(observed.st_mode) || observed.st_uid != ::geteuid() ||
	    (inode != 0 && (static_cast<uint64_t>(observed.st_dev) != device ||
	                   static_cast<uint64_t>(observed.st_ino) != inode))) {
		errno = EEXIST;
		return false;
	}
	return ::unlink(path.c_str()) == 0 || errno == ENOENT;
}

bool send_reply_with_fd(int conn_fd, const FdBrokerReplyAbi& reply, int payload_fd,
                        const IoBudget& budget) noexcept {
	alignas(struct cmsghdr) char cmsg_buf[CMSG_SPACE(sizeof(int))]{};
	struct iovec iov {};
	iov.iov_base = const_cast<FdBrokerReplyAbi*>(&reply);
	iov.iov_len = sizeof(reply);
	struct msghdr msg {};
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;
	msg.msg_control = cmsg_buf;
	msg.msg_controllen = sizeof(cmsg_buf);
	auto* cmsg = CMSG_FIRSTHDR(&msg);
	cmsg->cmsg_level = SOL_SOCKET;
	cmsg->cmsg_type = SCM_RIGHTS;
	cmsg->cmsg_len = CMSG_LEN(sizeof(int));
	std::memcpy(CMSG_DATA(cmsg), &payload_fd, sizeof(int));
	msg.msg_controllen = cmsg->cmsg_len;
	while (budget_available(budget)) {
		const ssize_t sent = ::sendmsg(conn_fd, &msg, MSG_NOSIGNAL | MSG_DONTWAIT);
		if (sent >= 0)
			return sent == static_cast<ssize_t>(sizeof(reply)) &&
			       budget_available(budget);
		if (errno == EINTR) continue;
		if (errno != EAGAIN && errno != EWOULDBLOCK) return false;
		if (!wait_socket(conn_fd, POLLOUT, budget)) return false;
	}
	return false;
}

bool receive_reply(int fd, FdBrokerReplyAbi* reply, UniqueFd* received_fd,
                   const IoBudget& budget) noexcept {
	while (wait_socket(fd, POLLIN, budget)) {
		alignas(struct cmsghdr) char control[CMSG_SPACE(sizeof(int))]{};
		struct iovec vector {
			reply, sizeof(*reply)
		};
		struct msghdr message {};
		message.msg_iov = &vector;
		message.msg_iovlen = 1;
		message.msg_control = control;
		message.msg_controllen = sizeof(control);
		const ssize_t bytes = ::recvmsg(fd, &message, MSG_CMSG_CLOEXEC | MSG_DONTWAIT);
		if (bytes < 0) {
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
			return false;
		}
		size_t fd_count = 0;
		bool valid_control = true;
		for (auto* ancillary = CMSG_FIRSTHDR(&message); ancillary != nullptr;
		     ancillary = CMSG_NXTHDR(&message, ancillary)) {
			if (ancillary->cmsg_level != SOL_SOCKET ||
			    ancillary->cmsg_type != SCM_RIGHTS) {
				valid_control = false;
				continue;
			}
			const size_t available =
			        static_cast<size_t>(control + message.msg_controllen -
			                            reinterpret_cast<char*>(ancillary));
			if (ancillary->cmsg_len < CMSG_LEN(0)) {
				valid_control = false;
				continue;
			}
			const size_t payload_size =
			        std::min(ancillary->cmsg_len, available) - CMSG_LEN(0);
			if (ancillary->cmsg_len > available || payload_size % sizeof(int) != 0)
				valid_control = false;
			for (size_t offset = 0; offset + sizeof(int) <= payload_size;
			     offset += sizeof(int)) {
				int passed_fd = -1;
				std::memcpy(&passed_fd, CMSG_DATA(ancillary) + offset,
				            sizeof(passed_fd));
				++fd_count;
				if (received_fd->get() < 0)
					received_fd->reset(passed_fd);
				else
					(void)::close(passed_fd);
			}
		}
		return bytes == static_cast<ssize_t>(sizeof(*reply)) && valid_control &&
		       fd_count <= 1 && (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0 &&
		       budget_available(budget);
	}
	return false;
}

int hand_out_fd(int shm_fd, bool readonly) noexcept {
	if (!readonly) return ::fcntl(shm_fd, F_DUPFD_CLOEXEC, 0);
	char path[64]{};
	std::snprintf(path, sizeof(path), "/proc/self/fd/%d", shm_fd);
	return ::open(path, O_RDONLY | O_CLOEXEC);
}

}

uint64_t fd_broker_checksum(const void* record, size_t size) noexcept {
	if (record == nullptr || size < sizeof(uint64_t) || size > kFdBrokerReplySize) return 0;
	std::byte tmp[kFdBrokerReplySize]{};
	std::memcpy(tmp, record, size);
	uint64_t zero = 0;
	std::memcpy(tmp + size - sizeof(uint64_t), &zero, sizeof(uint64_t));
	return fnv1a64(tmp, size);
}

Result<UniqueFd> memfd_create_object(const std::string& channel_name) {
#ifdef EDGE_HAVE_MEMFD_CREATE
	const std::string tag = "edgeruntime." + channel_name;
	const int fd = static_cast<int>(
	        ::syscall(SYS_memfd_create, tag.c_str(), static_cast<unsigned long>(MFD_CLOEXEC)));
	if (fd < 0) {
		const int e = errno;
		return make_errno_error(e, "memfd_create_object", std::strerror(e));
	}
	UniqueFd owned(fd);

	if (::fchmod(fd, static_cast<mode_t>(S_IRUSR | S_IWUSR)) != 0) {
		const int e = errno;
		return make_errno_error(e, "memfd_create_object", std::strerror(e));
	}
	return Result<UniqueFd>(std::move(owned));
#else
	(void)channel_name;
	return make_error(ErrorCode::kUnsupportedPlatform, "memfd_create_object",
	                  "memfd_create unavailable");
#endif
}

Result<UniqueFd> fd_broker_bind(const std::string& socket_path) {
	const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
	if (fd < 0) {
		const int e = errno;
		return make_errno_error(e, "fd_broker_bind", std::strerror(e));
	}
	UniqueFd listen_fd(fd);
	struct sockaddr_un addr {};
	addr.sun_family = AF_UNIX;
	if (socket_path.size() >= sizeof(addr.sun_path)) {
		return make_error(ErrorCode::kInvalidName, "fd_broker_bind",
		                  "socket path too long");
	}
	std::memcpy(addr.sun_path, socket_path.c_str(), socket_path.size() + 1);

	int rc = ::bind(fd, reinterpret_cast<struct sockaddr*>(&addr),
	                static_cast<socklen_t>(sizeof(addr)));
	if (rc != 0 && errno == EADDRINUSE) {
		if (socket_has_live_peer(socket_path)) {
			return make_error(ErrorCode::kAlreadyOwned, "fd_broker_bind",
			                  "live broker socket under path");
		}
		if (!unlink_socket(socket_path)) {
			const int e = errno;
			return make_errno_error(e, "fd_broker_bind", std::strerror(e));
		}
		rc = ::bind(fd, reinterpret_cast<struct sockaddr*>(&addr),
		            static_cast<socklen_t>(sizeof(addr)));
	}
	if (rc != 0) {
		const int e = errno;
		return make_errno_error(e, "fd_broker_bind", std::strerror(e));
	}

	struct stat bound {};
	if (::lstat(socket_path.c_str(), &bound) != 0 || !S_ISSOCK(bound.st_mode) ||
	    bound.st_uid != ::geteuid()) {
		return make_error(ErrorCode::kTransportFailed, "fd_broker_bind", "bound socket identity unavailable");
	}
	if (::chmod(socket_path.c_str(), static_cast<mode_t>(S_IRUSR | S_IWUSR)) != 0) {
		const int e = errno;
		(void)unlink_socket(socket_path, static_cast<uint64_t>(bound.st_dev),
		                    static_cast<uint64_t>(bound.st_ino));
		return make_errno_error(e, "fd_broker_bind", std::strerror(e));
	}
	if (::listen(fd, static_cast<int>(kListenBacklog)) != 0) {
		const int e = errno;
		(void)unlink_socket(socket_path, static_cast<uint64_t>(bound.st_dev),
		                    static_cast<uint64_t>(bound.st_ino));
		return make_errno_error(e, "fd_broker_bind", std::strerror(e));
	}
	return Result<UniqueFd>(std::move(listen_fd));
}

void fd_broker_serve_loop(int listen_fd, int shm_fd, std::byte* base, const uint32_t* channel_hash,
                          const std::array<std::byte, 32>* schema_fingerprint,
                          std::atomic<bool>* stop) noexcept {
	while (!stop->load(std::memory_order_relaxed)) {
		struct pollfd pfd {};
		pfd.fd = listen_fd;
		pfd.events = POLLIN;
		const int prc = ::poll(&pfd, 1, static_cast<int>(kServePollIntervalNs / 1'000'000));
		if (prc < 0) {
			if (errno == EINTR) continue;
			break;
		}
		if (prc == 0) continue;
		if (stop->load(std::memory_order_relaxed) || (pfd.revents & POLLIN) == 0) break;
		const int conn =
		        ::accept4(listen_fd, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
		if (conn < 0) {
			if (errno == EINTR || errno == EAGAIN) continue;
			break;
		}
		UniqueFd conn_fd(conn);
		const IoBudget budget{monotonic_deadline_ns(kFdBrokerPeerIoTimeoutMs * 1'000'000),
		                      stop};
		if (budget.deadline_ns == 0) continue;

		if (!peer_is_same_uid(conn)) {
			FdBrokerReplyAbi reply{};
			std::memcpy(reply.magic, kFdBrokerReplyMagic,
			            sizeof(kFdBrokerReplyMagic) - 1);
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kRefused);
			reply.checksum = reply_checksum_of(reply);
			(void)send_full(conn, &reply, sizeof(reply), budget);
			continue;
		}

		FdBrokerRequestAbi req{};
		if (!recv_full(conn, &req, sizeof(req), budget)) continue;
		if (std::memcmp(req.magic, kFdBrokerRequestMagic,
		                sizeof(kFdBrokerRequestMagic) - 1) != 0) {
			continue;
		}
		if (req.version != kFdBrokerVersion || request_checksum_of(req) != req.checksum) {
			continue;
		}

		auto* boot = reinterpret_cast<BootstrapHeaderAbi*>(base);
		auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);

		FdBrokerReplyAbi reply{};
		std::memcpy(reply.magic, kFdBrokerReplyMagic, sizeof(kFdBrokerReplyMagic) - 1);
		reply.status = static_cast<uint32_t>(FdBrokerStatus::kOk);

		if (req.channel_hash != *channel_hash) {
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kChannelMismatch);
		} else if (request_fingerprint_set(req) &&
		           std::memcmp(req.schema_fingerprint, schema_fingerprint->data(), 32) !=
		                   0) {
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kChannelMismatch);
		} else if (shared_load_acquire(&boot->init_state) !=
		                   static_cast<uint32_t>(InitState::kReady) ||
		           shared_load_acquire(&header->init_state) !=
		                   static_cast<uint32_t>(InitState::kReady)) {
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kNotReady);
		}

		if (reply.status == static_cast<uint32_t>(FdBrokerStatus::kOk)) {
			reply.mapping_size = boot->expected_mapping_size;
			reply.generation = header->generation;
			reply.nonce_hi = header->instance_nonce_hi;
			reply.nonce_lo = header->instance_nonce_lo;
			reply.abi_major = header->abi_major;
			reply.abi_minor = header->abi_minor;
			const bool readonly = (req.flags & kFdBrokerFlagReadonly) != 0;
			const int out_fd = hand_out_fd(shm_fd, readonly);
			if (out_fd < 0) {
				reply.status = static_cast<uint32_t>(FdBrokerStatus::kSystem);
			} else {
				UniqueFd out_guard(out_fd);
				reply.checksum = reply_checksum_of(reply);
				if (send_reply_with_fd(conn, reply, out_fd, budget)) {
					EDGE_FAILPOINT(C14);
				}
				continue;
			}
		}
		reply.checksum = reply_checksum_of(reply);
		(void)send_full(conn, &reply, sizeof(reply), budget);
	}
}

Result<UniqueFd> fd_broker_request_fd(const std::string& socket_path, uint32_t channel_hash,
                                      const SchemaDescriptor& schema, bool readonly,
                                      FdBrokerReplyAbi* reply_out, uint64_t retry_ms) noexcept {
	uint64_t retry_ns = 0;
	if (!checked_mul_u64(retry_ms, 1'000'000ull, &retry_ns)) {
		return make_error(ErrorCode::kInvalidOptions, "fd_broker_request_fd",
		                  "retry timeout out of range");
	}
	if (retry_ms == 0) retry_ns = kFdBrokerSingleAttemptTimeoutMs * 1'000'000;
	const uint64_t deadline = monotonic_deadline_ns(retry_ns);
	if (deadline == 0) {
		return make_error(ErrorCode::kClockAnomaly, "fd_broker_request_fd",
		                  "monotonic clock unavailable");
	}

	return fd_broker_request_fd_until(socket_path, channel_hash, schema, readonly, reply_out,
	                                  deadline, retry_ms != 0);
}

Result<UniqueFd> fd_broker_request_fd_until(const std::string& socket_path, uint32_t channel_hash,
                                            const SchemaDescriptor& schema, bool readonly,
                                            FdBrokerReplyAbi* reply_out, uint64_t deadline_ns,
                                            bool allow_retry) noexcept {
	struct sockaddr_un address {};
	address.sun_family = AF_UNIX;
	if (socket_path.size() >= sizeof(address.sun_path)) {
		return make_error(ErrorCode::kInvalidName, "fd_broker_request_fd",
		                  "socket path too long");
	}
	if (deadline_ns == 0) {
		return make_error(ErrorCode::kClockAnomaly, "fd_broker_request_fd",
		                  "monotonic clock unavailable");
	}
	std::memcpy(address.sun_path, socket_path.c_str(), socket_path.size() + 1);
	FdBrokerRequestAbi request{};
	std::memcpy(request.magic, kFdBrokerRequestMagic, sizeof(kFdBrokerRequestMagic) - 1);
	request.version = kFdBrokerVersion;
	request.flags = readonly ? kFdBrokerFlagReadonly : 0;
	request.channel_hash = channel_hash;
	std::memcpy(request.schema_fingerprint, schema.fingerprint.data(), 32);
	request.checksum = request_checksum_of(request);
	const IoBudget budget{deadline_ns};
	while (budget_available(budget)) {
		const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
		if (fd < 0) {
			const int e = errno;
			return make_errno_error(ErrorCode::kTransportFailed, e,
			                        "fd_broker_request_fd", std::strerror(e));
		}
		UniqueFd conn_fd(fd);
		int connect_error = 0;
		if (::connect(fd, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) !=
		    0) {
			connect_error = errno;
			if (connect_error == EINPROGRESS || connect_error == EALREADY ||
			    connect_error == EINTR) {
				if (wait_socket(fd, POLLOUT, budget)) {
					socklen_t length = sizeof(connect_error);
					if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &connect_error,
					                 &length) != 0)
						connect_error = errno;
				} else
					connect_error = EAGAIN;
			}
		}
		if (connect_error != 0) {
			if (connect_error != ECONNREFUSED && connect_error != ENOENT &&
			    connect_error != EAGAIN && connect_error != EINTR &&
			    connect_error != EINPROGRESS && connect_error != EALREADY) {
				return make_errno_error(ErrorCode::kTransportFailed, connect_error,
				                        "fd_broker_request_fd",
				                        std::strerror(connect_error));
			}
		} else if (send_full(fd, &request, sizeof(request), budget)) {
			FdBrokerReplyAbi reply{};
			UniqueFd received_fd;
			if (receive_reply(fd, &reply, &received_fd, budget) &&
			    std::memcmp(reply.magic, kFdBrokerReplyMagic,
			                sizeof(kFdBrokerReplyMagic) - 1) == 0 &&
			    reply_checksum_of(reply) == reply.checksum) {
				const auto status = static_cast<FdBrokerStatus>(reply.status);
				if (status == FdBrokerStatus::kOk && received_fd.get() >= 0) {
					const int flags = ::fcntl(received_fd.get(), F_GETFD);
					if (flags < 0 || ::fcntl(received_fd.get(), F_SETFD,
					                         flags | FD_CLOEXEC) != 0) {
						const int error = errno;
						return make_errno_error(
						        ErrorCode::kTransportFailed, error,
						        "fd_broker_request_fd",
						        "fcntl on received fd failed");
					}
					if (!budget_available(budget)) break;
					if (reply_out != nullptr) *reply_out = reply;
					return Result<UniqueFd>(std::move(received_fd));
				}
				if (received_fd.get() < 0 &&
				    status == FdBrokerStatus::kChannelMismatch) {
					return make_error(ErrorCode::kSchemaMismatch,
					                  "fd_broker_request_fd",
					                  "broker rejected channel/schema");
				}
				if (received_fd.get() < 0 && status == FdBrokerStatus::kRefused) {
					return make_error(ErrorCode::kPermissionDenied,
					                  "fd_broker_request_fd",
					                  "broker refused peer");
				}
			}
		}
		if (!allow_retry || !retry_pause(budget)) break;
	}
	return make_error(ErrorCode::kProducerOffline, "fd_broker_request_fd",
	                  "broker exchange failed (total budget exhausted)");
}

}
