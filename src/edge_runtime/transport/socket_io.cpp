#include "edge_runtime/transport/socket_io.hpp"

#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "edge_runtime/channel/control_operation.hpp"
#include "edge_runtime/sync/cancellation.hpp"
#include "edge_runtime/sync/clock.hpp"

namespace edge_runtime::detail::socket_io {

bool budget_available(const IoBudget& budget) noexcept {
	return static_cast<bool>(control_check()) &&
	       (budget.stop == nullptr || !budget.stop->load(std::memory_order_relaxed)) &&
	       (budget.cancellation == nullptr || !budget.cancellation->is_cancelled()) &&
	       remaining_time_ns(budget.deadline_ns) != 0;
}

bool wait_socket(int fd, short events, const IoBudget& budget) noexcept {
	while (budget_available(budget)) {
		uint64_t remaining = remaining_time_ns(budget.deadline_ns);
		const auto* context = control_context();
		if (context)
			remaining = std::min(remaining, remaining_time_ns(context->deadline_ns()));
		if (budget.stop != nullptr || budget.cancellation != nullptr)
			remaining = std::min(remaining, uint64_t{10'000'000});
		if (context) remaining = std::min(remaining, uint64_t{10'000'000});
		remaining = std::min(remaining, uint64_t{1'000'000'000});
		struct pollfd descriptors[4]{
		        {fd, events, 0},
		        {context ? CancellationAccess::native_handle(context->cancellation()) : -1,
		         POLLIN, 0},
		        {control_stop_handle(), POLLIN, 0},
		        {budget.cancellation
		                 ? CancellationAccess::native_handle(*budget.cancellation)
		                 : -1,
		         POLLIN, 0}};
		const int ready =
		        ::poll(descriptors, 4, static_cast<int>((remaining + 999'999) / 1'000'000));
		if (ready < 0) {
			if (errno == EINTR) continue;
			return false;
		}
		if (ready == 0) continue;
		return budget_available(budget) &&
		       (descriptors[0].revents & (events | POLLHUP | POLLERR)) != 0;
	}
	errno = ETIMEDOUT;
	return false;
}

bool send_full(int fd, const void* buffer, size_t size, const IoBudget& budget) noexcept {
	const auto* bytes = static_cast<const char*>(buffer);
	size_t sent = 0;
	while (sent < size && budget_available(budget)) {
		const ssize_t count = ::send(fd, bytes + sent, size - sent,
		                             MSG_DONTWAIT | MSG_NOSIGNAL);
		if (count < 0) {
			if (errno == EINTR) continue;
			if (errno == EAGAIN && wait_socket(fd, POLLOUT, budget)) continue;
			return false;
		}
		if (count == 0) return false;
		sent += static_cast<size_t>(count);
	}
	return sent == size && budget_available(budget);
}

bool recv_full(int fd, void* buffer, size_t size, const IoBudget& budget) noexcept {
	auto* bytes = static_cast<char*>(buffer);
	size_t received = 0;
	while (received < size) {
		if (!wait_socket(fd, POLLIN, budget)) return false;
		const ssize_t count = ::recv(fd, bytes + received, size - received, MSG_DONTWAIT);
		if (count < 0) {
			if (errno == EINTR || errno == EAGAIN) continue;
			return false;
		}
		if (count == 0) return false;
		received += static_cast<size_t>(count);
	}
	return budget_available(budget);
}

bool peer_is_same_uid(int fd) noexcept {
	struct ucred credentials {};
	socklen_t size = sizeof(credentials);
	return ::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credentials, &size) == 0 &&
	       size == sizeof(credentials) && credentials.uid == ::geteuid();
}

bool connect_socket(int fd, const std::string& path, const IoBudget& budget) noexcept {
	struct sockaddr_un address {};
	address.sun_family = AF_UNIX;
	if (path.size() >= sizeof(address.sun_path)) return false;
	std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
	if (!budget_available(budget)) return false;
	if (::connect(fd, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) == 0)
		return budget_available(budget);
	if (errno != EINTR && errno != EINPROGRESS && errno != EALREADY) return false;
	if (!wait_socket(fd, POLLOUT, budget)) return false;
	int error = 0;
	socklen_t size = sizeof(error);
	if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size) != 0) return false;
	if (error != 0) { errno = error; return false; }
	return budget_available(budget);
}

bool send_fd(int fd, const void* buffer, size_t size, int payload_fd,
             const IoBudget& budget) noexcept {
	alignas(struct cmsghdr) char ancillary[CMSG_SPACE(sizeof(int))]{};
	struct iovec vector { const_cast<void*>(buffer), size };
	struct msghdr message {};
	message.msg_iov = &vector;
	message.msg_iovlen = 1;
	message.msg_control = ancillary;
	message.msg_controllen = sizeof(ancillary);
	auto* header = CMSG_FIRSTHDR(&message);
	header->cmsg_level = SOL_SOCKET;
	header->cmsg_type = SCM_RIGHTS;
	header->cmsg_len = CMSG_LEN(sizeof(int));
	std::memcpy(CMSG_DATA(header), &payload_fd, sizeof(payload_fd));
	while (budget_available(budget)) {
		const ssize_t count = ::sendmsg(fd, &message, MSG_NOSIGNAL | MSG_DONTWAIT);
		if (count > 0) {
			const size_t sent = static_cast<size_t>(count);
			return sent == size ? budget_available(budget) :
			       send_full(fd, static_cast<const char*>(buffer) + sent, size - sent, budget);
		}
		if (count == 0) return false;
		if (errno == EINTR) continue;
		if (errno != EAGAIN || !wait_socket(fd, POLLOUT, budget)) return false;
	}
	return false;
}

bool receive_fd(int fd, void* buffer, size_t size, UniqueFd* payload_fd,
                const IoBudget& budget) noexcept {
	size_t received = 0;
	size_t descriptor_count = 0;
	while (received < size && wait_socket(fd, POLLIN, budget)) {
		alignas(struct cmsghdr) char ancillary[CMSG_SPACE(sizeof(int) * 8)]{};
		struct iovec vector { static_cast<char*>(buffer) + received, size - received };
		struct msghdr message {};
		message.msg_iov = &vector;
		message.msg_iovlen = 1;
		message.msg_control = ancillary;
		message.msg_controllen = sizeof(ancillary);
		const ssize_t count = ::recvmsg(fd, &message, MSG_DONTWAIT | MSG_CMSG_CLOEXEC);
		if (count < 0) {
			if (errno == EINTR || errno == EAGAIN) continue;
			return false;
		}
		bool valid = (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0;
		for (auto* header = CMSG_FIRSTHDR(&message); header != nullptr;
		     header = CMSG_NXTHDR(&message, header)) {
			if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS ||
			    header->cmsg_len < CMSG_LEN(0)) { valid = false; continue; }
			const size_t available = static_cast<size_t>(ancillary + message.msg_controllen -
			                                           reinterpret_cast<char*>(header));
			if (available < CMSG_LEN(0)) { valid = false; continue; }
			if (header->cmsg_len > available) valid = false;
			const size_t length = std::min(header->cmsg_len, available) - CMSG_LEN(0);
			if (length % sizeof(int) != 0) valid = false;
			for (size_t offset = 0; offset + sizeof(int) <= length; offset += sizeof(int)) {
				int descriptor = -1;
				std::memcpy(&descriptor, CMSG_DATA(header) + offset, sizeof(descriptor));
				++descriptor_count;
				if (descriptor_count == 1) payload_fd->reset(descriptor);
				else (void)::close(descriptor);
			}
		}
		if (!valid || descriptor_count > 1 || count == 0) return false;
		received += static_cast<size_t>(count);
	}
	return received == size && descriptor_count == 1 && budget_available(budget);
}

}
