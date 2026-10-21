#include <fcntl.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <utility>

#include "edge_runtime/channel/channel_abi.hpp"
#include "edge_runtime/channel/consumer_impl.hpp"
#include "edge_runtime/channel/notification_backend.hpp"
#include "edge_runtime/channel/notification_service.hpp"
#include "edge_runtime/channel/producer_impl.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/transport/fd_broker.hpp"
#include "edge_runtime/transport/socket_io.hpp"
#include "edge_runtime/utility/checksum.hpp"

namespace edge_runtime::detail {
namespace {

constexpr const char* kOperation = "EventNotification";

struct ConsumerUse {
	ConsumerHandle& handle;
	bool acquired;
	explicit ConsumerUse(ConsumerHandle& value) noexcept
	    : handle(value), acquired(!handle.operation_in_use.exchange(true, std::memory_order_acq_rel)) {}
	~ConsumerUse() { if (acquired) handle.operation_in_use.store(false, std::memory_order_release); }
};

bool same_identity(const ProcessIdentity& first, const ProcessIdentity& second) noexcept {
	return process_identity_valid(first) && process_identity_valid(second) &&
	       first.pid == second.pid && first.proc_start_ticks == second.proc_start_ticks &&
	       first.boot_id_hash_hi == second.boot_id_hash_hi &&
	       first.boot_id_hash_lo == second.boot_id_hash_lo;
}

NotificationRole role_from(const ProcessIdentityAbi& identity) noexcept {
	return {identity.role_epoch, {identity.pid, identity.proc_start_ticks,
	                             identity.boot_id_hash_hi, identity.boot_id_hash_lo}};
}

bool same_role(const NotificationRole& first, const NotificationRole& second) noexcept {
	return first.epoch != 0 && (first.epoch & 1) == 0 && first.epoch == second.epoch &&
	       same_identity(first.identity, second.identity);
}

Result<NotificationRecord> snapshot(BootstrapHeaderAbi* bootstrap, ChannelHeaderAbi* header) noexcept {
	if (shared_load_acquire(&bootstrap->init_state) != static_cast<uint32_t>(InitState::kReady) ||
	    shared_load_acquire(&header->init_state) != static_cast<uint32_t>(InitState::kReady)) {
		return make_error(ErrorCode::kStaleHandle, kOperation, "instance not READY");
	}
	auto producer = identity_snapshot_read(&header->producer);
	auto consumer = identity_snapshot_read(&header->consumer);
	if (!producer) return producer.error();
	if (!consumer) return consumer.error();
	NotificationRecord record{};
	record.magic = kNotificationMagic;
	record.version = 1;
	record.kind = 1;
	record.generation = header->generation;
	record.nonce_hi = header->instance_nonce_hi;
	record.nonce_lo = header->instance_nonce_lo;
	record.producer = role_from(producer.value());
	record.consumer = role_from(consumer.value());
	std::memcpy(record.fingerprint.data(), header->schema_fingerprint, record.fingerprint.size());
	return record;
}

bool same_binding(const NotificationRecord& first, const NotificationRecord& second) noexcept {
	return first.generation == second.generation && first.nonce_hi == second.nonce_hi &&
	       first.nonce_lo == second.nonce_lo && first.fingerprint == second.fingerprint &&
	       same_role(first.producer, second.producer) && same_role(first.consumer, second.consumer);
}

bool peer_matches(int fd, const ProcessIdentity& identity) noexcept {
	struct ucred credentials {};
	socklen_t length = sizeof(credentials);
	if (!process_identity_valid(identity) ||
	    ::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credentials, &length) != 0 ||
	    length != sizeof(credentials) || credentials.uid != ::geteuid() ||
	    credentials.pid <= 0 || static_cast<uint64_t>(credentials.pid) != identity.pid) return false;
	auto start = proc_stat_starttime(credentials.pid);
	uint64_t boot_hi = 0, boot_lo = 0;
	auto boot = current_boot_id_hash(&boot_hi, &boot_lo);
	return start && boot && start.value() == identity.proc_start_ticks &&
	       boot_hi == identity.boot_id_hash_hi && boot_lo == identity.boot_id_hash_lo;
}

bool epoll_add(int epoll_fd, int fd, uint64_t token) noexcept {
	struct epoll_event event {};
	event.events = EPOLLIN | EPOLLRDHUP;
	event.data.u64 = token;
	return ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == 0;
}

Error system_error(const char* context) noexcept {
	return make_errno_error(ErrorCode::kTransportFailed, errno, kOperation, context);
}

}

uint64_t notification_checksum(NotificationRecord record) noexcept {
	record.checksum = 0;
	return fnv1a64(reinterpret_cast<const std::byte*>(&record), sizeof(record));
}

Result<void> eventfd_signal(int fd) noexcept {
	return notification_signal(fd, NotificationBackend::kEventFd);
}

struct NotificationSession {
	NotificationRecord record{};
	UniqueFd event;
	UniqueFd peer;
	~NotificationSession() { if (peer.get() >= 0) (void)::shutdown(peer.get(), SHUT_RDWR); }
};

Result<std::shared_ptr<NotificationServer>> NotificationServer::start(ProducerHandle& handle) {
	return start(handle, NotificationBackend::kEventFd);
}

Result<std::shared_ptr<NotificationServer>> NotificationServer::start(ProducerHandle& handle,
                                                                      NotificationBackend backend) {
	if (!notification_backend_valid(backend))
		return make_error(ErrorCode::kInvalidOptions, kOperation, "unknown backend");
	if (backend == NotificationBackend::kDriver) {
		auto checked = notification_open(backend);
		if (!checked) return checked.error();
	}
	auto lock = ControlLock::acquire(channel_lock_path(handle.channel_name));
	if (!lock) return lock.error();
	auto journal = lock.value().read_journal();
	if (!journal) return journal.error();
	if (journal.value().new_generation != handle.generation ||
	    journal.value().new_nonce_hi != handle.instance_nonce_hi ||
	    journal.value().new_nonce_lo != handle.instance_nonce_lo || !identity_matches_current(handle.self))
		return make_error(ErrorCode::kStaleHandle, kOperation, "producer ownership changed");
	if (handle.transport == Transport::kPosixShm) {
		auto named = shm_open_existing(channel_shm_name(handle.channel_name));
		if (!named) return make_error(ErrorCode::kStaleHandle, kOperation, "channel name gone");
		uint64_t device = 0, inode = 0, size = 0;
		auto captured = shm_fstat_and_capture(named.value(), &device, &inode, &size);
		if (!captured) return captured.error();
		if (device != handle.shm.dev || inode != handle.shm.ino)
			return make_error(ErrorCode::kStaleHandle, kOperation, "channel name replaced");
	} else if (handle.socket_unlinked) {
		return make_error(ErrorCode::kStaleHandle, kOperation, "channel broker removed");
	}
	auto server = std::shared_ptr<NotificationServer>(new NotificationServer);
	server->backend_ = backend;
	auto* base = static_cast<std::byte*>(handle.shm.mapping.get());
	server->bootstrap_ = reinterpret_cast<BootstrapHeaderAbi*>(base);
	server->header_ = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	server->owner_.generation = handle.generation;
	server->owner_.nonce_hi = handle.instance_nonce_hi;
	server->owner_.nonce_lo = handle.instance_nonce_lo;
	server->owner_.producer = {handle.role_epoch, handle.self};
	server->owner_.fingerprint = handle.schema_fingerprint;
	server->path_ = channel_socket_path(handle.channel_name) + ".notify";
	auto listener = fd_broker_bind(server->path_);
	if (!listener) return listener.error();
	server->listener_ = std::move(listener.value());
	struct stat status {};
	if (::lstat(server->path_.c_str(), &status) != 0 || !S_ISSOCK(status.st_mode) ||
	    status.st_uid != ::geteuid()) return system_error("cannot bind socket inode");
	server->socket_dev_ = static_cast<uint64_t>(status.st_dev);
	server->socket_ino_ = static_cast<uint64_t>(status.st_ino);
	try {
		server->thread_ = std::thread([pointer = server.get()] { pointer->run(); });
	} catch (...) {
		return make_error(ErrorCode::kSystemError, kOperation, "notification thread start failed");
	}
	return server;
}

NotificationServer::~NotificationServer() { stop(); }

void NotificationServer::replace_session(std::shared_ptr<NotificationSession> session) noexcept {
	auto previous = std::atomic_exchange(&session_, std::move(session));
	if (previous) (void)::shutdown(previous->peer.get(), SHUT_RDWR);
}

void NotificationServer::stop() noexcept {
	stopping_.store(true, std::memory_order_release);
	if (listener_.get() >= 0) (void)::shutdown(listener_.get(), SHUT_RDWR);
	if (thread_.joinable()) thread_.join();
	replace_session({});
	struct stat status {};
	if (socket_ino_ != 0 && ::lstat(path_.c_str(), &status) == 0 && S_ISSOCK(status.st_mode) &&
	    status.st_uid == ::geteuid() && static_cast<uint64_t>(status.st_dev) == socket_dev_ &&
	    static_cast<uint64_t>(status.st_ino) == socket_ino_) {
		(void)::unlink(path_.c_str());
	}
	socket_ino_ = 0;
}

void NotificationServer::notify() noexcept {
	auto session = std::atomic_load(&session_);
	if (!session) return;
	auto signalled = notification_signal(session->event.get(), backend_);
	if (!signalled) {
		last_error_.store(signalled.error().errno_value, std::memory_order_relaxed);
		(void)::shutdown(session->peer.get(), SHUT_RDWR);
	}
}

bool NotificationServer::binding_current(const NotificationRecord& record) noexcept {
	if (stopping_.load(std::memory_order_acquire) ||
	    shared_load_acquire(&header_->producer_state) != static_cast<uint32_t>(EndpointState::kOnline) ||
	    shared_load_acquire(&header_->consumer_state) != static_cast<uint32_t>(EndpointState::kOnline)) return false;
	auto current = snapshot(bootstrap_, header_);
	return current && same_binding(record, current.value()) &&
	       record.generation == owner_.generation && record.nonce_hi == owner_.nonce_hi &&
	       record.nonce_lo == owner_.nonce_lo && record.fingerprint == owner_.fingerprint &&
	       same_role(record.producer, owner_.producer);
}

void NotificationServer::accept_peer(int fd) noexcept {
	UniqueFd peer(fd);
	const socket_io::IoBudget budget{
	        monotonic_deadline_ns(kFdBrokerPeerIoTimeoutMs * 1'000'000), &stopping_};
	NotificationRecord request{};
	if (!socket_io::peer_is_same_uid(fd) ||
	    !socket_io::recv_full(fd, &request, sizeof(request), budget) ||
	    request.magic != kNotificationMagic || (request.version != 1 && request.version != 2) ||
	    request.kind != 1 || (request.registration_hi == 0 && request.registration_lo == 0) ||
	    request.checksum != notification_checksum(request) || !binding_current(request) ||
	    !peer_matches(fd, request.consumer.identity))
		return;
	if (request.version != static_cast<uint32_t>(backend_)) {
		request.kind = 3;
		request.checksum = notification_checksum(request);
		(void)socket_io::send_full(fd, &request, sizeof(request), budget);
		return;
	}
	std::shared_ptr<NotificationSession> session;
	try {
		session = std::make_shared<NotificationSession>();
	} catch (...) {
		last_error_.store(ENOMEM, std::memory_order_relaxed);
		return;
	}
	session->record = request;
	auto descriptor = notification_open(backend_);
	if (!descriptor) {
		last_error_.store(descriptor.error().errno_value, std::memory_order_relaxed);
		request.kind = 4;
		request.checksum = notification_checksum(request);
		(void)socket_io::send_full(fd, &request, sizeof(request), budget);
		return;
	}
	session->event = std::move(descriptor.value());
	session->peer = std::move(peer);
	replace_session(session);
	auto initial = notification_signal(session->event.get(), backend_);
	if (!initial) {
		last_error_.store(initial.error().errno_value, std::memory_order_relaxed);
		replace_session({});
		return;
	}
	request.kind = 2;
	request.checksum = notification_checksum(request);
	if (!socket_io::send_fd(fd, &request, sizeof(request), session->event.get(), budget) ||
	    !binding_current(session->record)) {
		replace_session({});
	}
}

void NotificationServer::run() noexcept {
	while (!stopping_.load(std::memory_order_acquire)) {
		auto session = std::atomic_load(&session_);
		struct pollfd descriptors[2]{{listener_.get(), POLLIN, 0},
		                             {session ? session->peer.get() : -1, POLLIN | POLLRDHUP, 0}};
		const int ready = ::poll(descriptors, 2, 10);
		if (ready < 0 && errno != EINTR) {
			last_error_.store(errno, std::memory_order_relaxed);
			break;
		}
		if (session && (descriptors[1].revents != 0 || !binding_current(session->record))) {
			replace_session({});
		}
		if (stopping_.load(std::memory_order_acquire)) break;
		if (descriptors[0].revents & POLLIN) {
			const int peer = ::accept4(listener_.get(), nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
			if (peer >= 0) accept_peer(peer);
		}
	}
	replace_session({});
}

struct EventNotificationState {
	std::shared_ptr<ConsumerHandle> consumer;
	NotificationRecord record{};
	NotificationBackend backend{NotificationBackend::kEventFd};
	UniqueFd event;
	UniqueFd peer;
	UniqueFd epoll;
	~EventNotificationState() {
		if (record.consumer.identity.pid == static_cast<uint64_t>(::getpid())) {
			if (epoll.get() >= 0) {
				(void)::epoll_ctl(epoll.get(), EPOLL_CTL_DEL, event.get(), nullptr);
				(void)::epoll_ctl(epoll.get(), EPOLL_CTL_DEL, peer.get(), nullptr);
			}
			if (peer.get() >= 0) (void)::shutdown(peer.get(), SHUT_RDWR);
		}
		epoll.reset();
	}
};

namespace {
Result<void> validate_state(const EventNotificationState& state) noexcept {
	const auto& handle = *state.consumer;
	if (handle.shutdown_pending.load(std::memory_order_acquire) ||
	    handle.generation != state.record.generation || handle.instance_nonce_hi != state.record.nonce_hi ||
	    handle.instance_nonce_lo != state.record.nonce_lo || handle.role_epoch != state.record.consumer.epoch ||
	    handle.schema_fingerprint != state.record.fingerprint ||
	    !identity_matches_current(handle.self))
		return make_error(ErrorCode::kStaleHandle, kOperation, "consumer changed or closed");
	auto* base = static_cast<std::byte*>(handle.shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	auto current = snapshot(reinterpret_cast<BootstrapHeaderAbi*>(base), header);
	if (!current || !same_binding(state.record, current.value()) ||
	    shared_load_acquire(&header->consumer_state) != static_cast<uint32_t>(EndpointState::kOnline))
		return make_error(ErrorCode::kStaleHandle, kOperation, "registration instance or role changed");
	if (shared_load_acquire(&header->producer_state) != static_cast<uint32_t>(EndpointState::kOnline))
		return make_error(ErrorCode::kProducerOffline, kOperation, "producer stopped");
	return Result<void>::ok();
}
}

Result<EventNotification> consumer_event_notification_impl(
        const std::shared_ptr<ConsumerHandle>& handle, uint64_t timeout_ms) {
	return consumer_event_notification_impl(handle, timeout_ms, NotificationBackend::kEventFd);
}

Result<EventNotification> consumer_event_notification_impl(
        const std::shared_ptr<ConsumerHandle>& handle, uint64_t timeout_ms,
        NotificationBackend backend) {
	if (!notification_backend_valid(backend))
		return make_error(ErrorCode::kInvalidOptions, kOperation, "unknown backend");
	if (!handle) return make_error(ErrorCode::kStaleHandle, kOperation, "empty consumer");
	ConsumerUse use(*handle);
	if (!use.acquired) return make_error(ErrorCode::kConcurrentHandleUse, kOperation);
	uint64_t timeout_ns = 0;
	if (!checked_mul_u64(timeout_ms == 0 ? kFdBrokerSingleAttemptTimeoutMs : timeout_ms,
	                     1'000'000, &timeout_ns))
		return make_error(ErrorCode::kInvalidOptions, kOperation, "timeout out of range");
	const socket_io::IoBudget budget{monotonic_deadline_ns(timeout_ns)};
	if (budget.deadline_ns == 0) return make_error(ErrorCode::kClockAnomaly, kOperation);
	auto state = std::make_shared<EventNotificationState>();
	state->consumer = handle;
	state->backend = backend;
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto request = snapshot(reinterpret_cast<BootstrapHeaderAbi*>(base),
	                        reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset));
	if (!request) return request.error();
	state->record = request.value();
	state->record.version = static_cast<uint32_t>(backend);
	if (!same_role(state->record.consumer, NotificationRole{handle->role_epoch, handle->self}))
		return make_error(ErrorCode::kStaleHandle, kOperation, "consumer ownership changed");
	auto valid = validate_state(*state);
	if (!valid) return valid.error();
	bool random_ok = false;
	uint64_t nonce[2]{};
	for (int attempt = 0; attempt < 8 && socket_io::budget_available(budget); ++attempt) {
		const ssize_t count = ::getrandom(nonce, sizeof(nonce), GRND_NONBLOCK);
		if (count == static_cast<ssize_t>(sizeof(nonce)) && (nonce[0] != 0 || nonce[1] != 0)) {
			random_ok = true;
			break;
		}
		if (count < 0 && errno != EINTR) break;
	}
	if (!random_ok) return make_error(ErrorCode::kSystemError, kOperation, "registration nonce unavailable");
	state->record.registration_hi = nonce[0];
	state->record.registration_lo = nonce[1];
	state->record.checksum = notification_checksum(state->record);
	const std::string path = channel_socket_path(handle->channel_name) + ".notify";
	while (socket_io::budget_available(budget)) {
		UniqueFd peer(::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
		if (peer.get() < 0) return system_error("socket create failed");
		if (socket_io::connect_socket(peer.get(), path, budget)) {
			if (!peer_matches(peer.get(), state->record.producer.identity))
				return make_error(ErrorCode::kPermissionDenied, kOperation, "producer peer identity mismatch");
			NotificationRecord reply{};
			UniqueFd event;
			const bool sent = socket_io::send_full(peer.get(), &state->record,
			                                       sizeof(state->record), budget);
			const bool received =
			        sent && socket_io::receive_fd(peer.get(), &reply, sizeof(reply),
			                                      &event, budget);
			const bool bound_reply = sent && reply.magic == kNotificationMagic &&
			                         reply.version == state->record.version &&
			                         reply.checksum == notification_checksum(reply) &&
			                         same_binding(state->record, reply) &&
			                         reply.registration_hi == nonce[0] &&
			                         reply.registration_lo == nonce[1];
			if (bound_reply && (reply.kind == 3 || reply.kind == 4))
				return make_errno_error(ErrorCode::kTransportFailed,
				                        reply.kind == 3 ? EPROTONOSUPPORT : ENODEV,
				                        kOperation,
				                        "producer rejected notification backend");
			if (received && bound_reply && reply.kind == 2 &&
			    notification_fd_valid(event.get(), backend)) {
				state->peer = std::move(peer);
				state->event = std::move(event);
				state->epoll.reset(::epoll_create1(EPOLL_CLOEXEC));
				if (state->epoll.get() < 0 || !epoll_add(state->epoll.get(), state->event.get(), 1) ||
				    !epoll_add(state->epoll.get(), state->peer.get(), 2)) return system_error("epoll setup failed");
				valid = validate_state(*state);
				if (!valid) return valid.error();
				if (!socket_io::budget_available(budget)) break;
				return EventNotification(std::move(state));
			}
		}
		if (timeout_ms == 0) break;
		const uint64_t remaining = remaining_time_ns(budget.deadline_ns);
		if (remaining == 0) break;
		(void)::poll(nullptr, 0, static_cast<int>(std::min(remaining / 1'000'000, uint64_t{10})));
	}
	return make_error(ErrorCode::kTimeout, kOperation, "notification registration deadline exhausted");
}
}

namespace edge_runtime {

int EventNotification::native_handle() const noexcept { return state_ ? state_->epoll.get() : -1; }

NotificationBackend EventNotification::backend() const noexcept {
	return state_ ? state_->backend : NotificationBackend::kEventFd;
}

Result<void> EventNotification::drain() noexcept {
	using namespace detail;
	if (!state_) return make_error(ErrorCode::kStaleHandle, "EventNotification::drain", "moved notification");
	ConsumerUse use(*state_->consumer);
	if (!use.acquired) return make_error(ErrorCode::kConcurrentHandleUse, "EventNotification::drain");
	auto valid = validate_state(*state_);
	if (!valid) return valid.error();
	auto drained = notification_drain(state_->event.get(), state_->backend);
	if (!drained) return drained.error();
	struct epoll_event events[2]{};
	(void)::epoll_wait(state_->epoll.get(), events, 2, 0);
	for (int attempt = 0; attempt < 8; ++attempt) {
		char unexpected = 0;
		const ssize_t count = ::recv(state_->peer.get(), &unexpected, 1, MSG_PEEK | MSG_DONTWAIT);
		if (count < 0 && errno == EINTR) continue;
		if (count < 0 && errno == EAGAIN) return validate_state(*state_);
		const auto& producer = state_->record.producer.identity;
		const auto alive = probe_liveness(producer.pid, producer.proc_start_ticks);
		if (alive == Liveness::kExited || alive == Liveness::kPidReused)
			return make_error(ErrorCode::kProducerOffline, "EventNotification::drain", "producer exited");
		return make_error(ErrorCode::kStaleHandle, "EventNotification::drain", "registration closed; register again");
	}
	errno = EINTR;
	return system_error("control check interrupted repeatedly");
}

}
