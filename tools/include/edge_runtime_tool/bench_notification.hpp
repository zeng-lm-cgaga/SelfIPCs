#ifndef EDGE_RUNTIME_TOOL_BENCH_NOTIFICATION_HPP
#define EDGE_RUNTIME_TOOL_BENCH_NOTIFICATION_HPP

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>

#include "edge_runtime/channel/event_notification.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/transport/socket_io.hpp"
#include "edge_runtime/utility/checked_math.hpp"
#include "edge_runtime_tool/tool_common.hpp"

namespace edge_tool {

enum class BenchNotification { kFutex, kEventfd, kDriver };

inline BenchNotification bench_notification_from_args(int argc, char** argv) {
	const char* value = arg_value(argc, argv, "--notification");
	if (value == nullptr && !arg_flag(argc, argv, "--notification")) {
		return BenchNotification::kFutex;
	}
	if (value != nullptr) {
		if (std::strcmp(value, "futex") == 0) return BenchNotification::kFutex;
		if (std::strcmp(value, "eventfd") == 0) return BenchNotification::kEventfd;
		if (std::strcmp(value, "driver") == 0) return BenchNotification::kDriver;
	}
	std::fprintf(stderr, "--notification requires futex|eventfd|driver\n");
	std::exit(2);
}

inline const char* bench_notification_name(BenchNotification notification) {
	if (notification == BenchNotification::kDriver) return "driver";
	return notification == BenchNotification::kEventfd ? "eventfd" : "futex";
}

inline edge_runtime::NotificationBackend bench_notification_backend(
        BenchNotification notification) {
	return notification == BenchNotification::kDriver
	               ? edge_runtime::NotificationBackend::kDriver
	               : edge_runtime::NotificationBackend::kEventFd;
}

inline int bench_ready_fd_from_args(int argc, char** argv) {
	const char* value = arg_value(argc, argv, "--ready-fd");
	if (value == nullptr && !arg_flag(argc, argv, "--ready-fd")) return -1;
	char* end = nullptr;
	errno = 0;
	const long descriptor = value == nullptr ? -1 : std::strtol(value, &end, 10);
	if (value != nullptr && end != value && *end == '\0' && errno == 0 && descriptor >= 0 &&
	    descriptor <= std::numeric_limits<int>::max() &&
	    ::fcntl(static_cast<int>(descriptor), F_GETFD) >= 0)
		return static_cast<int>(descriptor);
	std::fprintf(stderr, "--ready-fd requires an inherited startup socket descriptor\n");
	std::exit(2);
}

class BenchReadyPair {
       public:
	BenchReadyPair() = default;
	BenchReadyPair(const BenchReadyPair&) = delete;
	BenchReadyPair& operator=(const BenchReadyPair&) = delete;
	~BenchReadyPair() { close(); }
	bool open() {
		return ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0,
		                    descriptors_) == 0;
	}
	int reader() const { return descriptors_[0]; }
	int writer() const { return descriptors_[1]; }
	void close() {
		for (auto& descriptor : descriptors_) {
			if (descriptor >= 0) (void)::close(descriptor);
			descriptor = -1;
		}
	}

       private:
	int descriptors_[2]{-1, -1};
};

inline bool bench_wait_ready(int fd, uint64_t timeout_ms) {
	edge_runtime::detail::UniqueFd owned(fd);
	uint64_t timeout_ns = 0;
	if (!edge_runtime::detail::checked_mul_u64(timeout_ms, 1'000'000, &timeout_ns))
		return false;
	const edge_runtime::detail::socket_io::IoBudget budget{
	        edge_runtime::detail::monotonic_deadline_ns(timeout_ns)};
	char token = 0;
	return edge_runtime::detail::socket_io::recv_full(fd, &token, 1, budget) && token == 'R';
}

inline bool bench_signal_ready(int fd) {
	if (fd < 0) return true;
	edge_runtime::detail::UniqueFd owned(fd);
	const edge_runtime::detail::socket_io::IoBudget budget{
	        edge_runtime::detail::monotonic_deadline_ns(1'000'000'000)};
	const char token = 'R';
	return edge_runtime::detail::socket_io::send_full(fd, &token, 1, budget);
}

}

#endif
