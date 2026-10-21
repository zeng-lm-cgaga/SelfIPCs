#include "mixed_io_demo/support.hpp"

#include <linux/can.h>
#include <linux/can/raw.h>
#include <net/if.h>
#include <poll.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <utility>

namespace mixed_io_demo {

Descriptor::~Descriptor() { close(); }

void Descriptor::close() {
	if (descriptor_ >= 0) (void)::close(std::exchange(descriptor_, -1));
}

Watch::Watch(int poller, int descriptor, uint64_t tag)
    : poller_(poller), descriptor_(descriptor) {
	epoll_event event{};
	event.events = EPOLLIN;
	event.data.u64 = tag;
	installed_ = ::epoll_ctl(poller_, EPOLL_CTL_ADD, descriptor_, &event) == 0;
	if (!installed_) (void)system_error("epoll ADD");
}

Watch::~Watch() { (void)remove(); }

bool Watch::remove() {
	if (!installed_) return true;
	if (::epoll_ctl(poller_, EPOLL_CTL_DEL, descriptor_, nullptr) != 0) {
		return system_error("epoll DEL before descriptor destruction");
	}
	installed_ = false;
	return true;
}

Child::~Child() {
	if (pid_ <= 0) return;
	(void)::kill(pid_, SIGKILL);
	(void)wait(Clock::now() + std::chrono::milliseconds{500});
}

bool Child::wait(Deadline deadline) {
	while (Clock::now() < deadline) {
		int status = 0;
		const pid_t result = ::waitpid(pid_, &status, WNOHANG);
		if (result == pid_) {
			std::printf("{\"event\":\"child_exit\",\"pid\":%ld,\"status\":%d}\n",
			            static_cast<long>(pid_), status);
			pid_ = -1;
			return WIFEXITED(status) && WEXITSTATUS(status) == 0;
		}
		if (result < 0 && errno != EINTR) {
			if (errno == ECHILD) pid_ = -1;
			return system_error("waitpid");
		}
		(void)::poll(nullptr, 0, 5);
	}
	std::fprintf(stderr, "child reap deadline exceeded\n");
	return false;
}

uint64_t monotonic_ns() {
	timespec value{};
	if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0) return 0;
	return static_cast<uint64_t>(value.tv_sec) * 1000000000ULL +
	       static_cast<uint64_t>(value.tv_nsec);
}

uint64_t cpu_ns() {
	timespec value{};
	if (::clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &value) != 0) return 0;
	return static_cast<uint64_t>(value.tv_sec) * 1000000000ULL +
	       static_cast<uint64_t>(value.tv_nsec);
}

bool system_error(const char* operation) {
	std::perror(operation);
	return false;
}

bool api_error(const char* operation, const ipc::Error& error) {
	std::fprintf(stderr, "%s: %s errno=%d\n", operation, ipc::to_string(error.code),
	             error.errno_value);
	return false;
}

int remaining_ms(Deadline deadline) {
	const auto remaining =
	        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
	if (remaining.count() < 0) return 0;
	return static_cast<int>(std::min<int64_t>(remaining.count() + 1, 1000));
}

bool send_token(int descriptor, char token, Deadline deadline) {
	while (Clock::now() < deadline) {
		if (::send(descriptor, &token, 1, MSG_NOSIGNAL) == 1) return true;
		if (errno == EINTR) continue;
		if (errno != EAGAIN && errno != EWOULDBLOCK) return system_error("send status");
		pollfd event{descriptor, POLLOUT, 0};
		if (::poll(&event, 1, remaining_ms(deadline)) < 0 && errno != EINTR) {
			return system_error("poll status send");
		}
	}
	std::fprintf(stderr, "status send deadline exceeded\n");
	return false;
}

bool await_token(int descriptor, char expected, Deadline deadline) {
	while (Clock::now() < deadline) {
		pollfd event{descriptor, POLLIN, 0};
		const int result = ::poll(&event, 1, remaining_ms(deadline));
		if (result < 0 && errno == EINTR) continue;
		if (result < 0) return system_error("poll status receive");
		if (result == 0) continue;
		char token = 0;
		const ssize_t received = ::recv(descriptor, &token, 1, 0);
		if (received < 0 && (errno == EINTR || errno == EAGAIN)) continue;
		if (received == 1 && token == expected) return true;
		std::fprintf(stderr, "peer closed or unexpected status, expected %c\n", expected);
		return false;
	}
	std::fprintf(stderr, "status receive deadline exceeded, expected %c\n", expected);
	return false;
}

int open_can(const Options& options, bool receive) {
	if (options.interface.empty() || options.interface.size() >= IFNAMSIZ) {
		std::fprintf(stderr, "invalid CAN interface name\n");
		return -1;
	}
	const int descriptor = ::socket(AF_CAN, SOCK_RAW | SOCK_CLOEXEC | SOCK_NONBLOCK, CAN_RAW);
	if (descriptor < 0) {
		(void)system_error("socket AF_CAN");
		return -1;
	}
	ifreq request{};
	std::memcpy(request.ifr_name, options.interface.c_str(), options.interface.size() + 1);
	can_filter filter{};
	filter.can_id = options.can_id;
	filter.can_mask = CAN_SFF_MASK | CAN_EFF_FLAG | CAN_RTR_FLAG;
	const void* filters = receive ? &filter : nullptr;
	const socklen_t length = receive ? static_cast<socklen_t>(sizeof(filter)) : 0;
	if (::ioctl(descriptor, SIOCGIFINDEX, &request) != 0 ||
	    ::setsockopt(descriptor, SOL_CAN_RAW, CAN_RAW_FILTER, filters, length) != 0) {
		(void)system_error("configure actual SocketCAN");
		(void)::close(descriptor);
		return -1;
	}
	sockaddr_can address{};
	address.can_family = AF_CAN;
	address.can_ifindex = request.ifr_ifindex;
	if (::bind(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
		(void)system_error("bind actual SocketCAN interface");
		(void)::close(descriptor);
		return -1;
	}
	return descriptor;
}

bool arm_timer(int descriptor, uint32_t interval_ms) {
	itimerspec timer{};
	timer.it_value.tv_sec = static_cast<time_t>(interval_ms / 1000U);
	timer.it_value.tv_nsec = static_cast<long>(interval_ms % 1000U) * 1000000L;
	timer.it_interval = timer.it_value;
	return ::timerfd_settime(descriptor, 0, &timer, nullptr) == 0 ||
	       system_error("arm timerfd");
}

bool read_counter(int descriptor, uint64_t& count) {
	for (int attempt = 0; attempt < 8; ++attempt) {
		if (::read(descriptor, &count, sizeof(count)) == static_cast<ssize_t>(sizeof(count))) {
			return true;
		}
		if (errno != EINTR) break;
	}
	return system_error("read timerfd/eventfd");
}

bool write_counter(int descriptor, uint64_t count) {
	for (int attempt = 0; attempt < 8; ++attempt) {
		if (::write(descriptor, &count, sizeof(count)) == static_cast<ssize_t>(sizeof(count))) {
			return true;
		}
		if (errno != EINTR) break;
	}
	return system_error("write stop eventfd");
}

}
