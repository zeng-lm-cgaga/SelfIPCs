#include "queue_pool_demo/process.hpp"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <thread>

namespace queue_pool_demo {

namespace {

class SocketPair {
       public:
	~SocketPair() {
		for (const int descriptor : descriptors) {
			if (descriptor >= 0) (void)::close(descriptor);
		}
	}
	std::array<int, 2> descriptors{-1, -1};
};

bool wait_ready(int descriptor, short events, Clock::time_point deadline) {
	while (Clock::now() < deadline) {
		const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
		        deadline - Clock::now());
		const int timeout = remaining.count() > 20 ? 20 : 1;
		struct pollfd watched {};
		watched.fd = descriptor;
		watched.events = events;
		const int result = ::poll(&watched, 1, timeout);
		if (result > 0) return (watched.revents & events) != 0;
		if (result < 0 && errno != EINTR) return false;
	}
	return false;
}

}  // namespace

bool send_token(int descriptor, char token, Clock::time_point deadline) {
	while (wait_ready(descriptor, POLLOUT, deadline)) {
		const ssize_t count = ::send(descriptor, &token, 1, MSG_NOSIGNAL | MSG_DONTWAIT);
		if (count == 1) return true;
		if (count < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
			continue;
		return false;
	}
	return false;
}

bool expect_token(int descriptor, char expected, Clock::time_point deadline) {
	while (wait_ready(descriptor, POLLIN, deadline)) {
		char token = '\0';
		const ssize_t count = ::recv(descriptor, &token, 1, MSG_DONTWAIT);
		if (count == 1 && token == expected) return true;
		if (count < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
			continue;
		std::fprintf(stderr, "helper protocol: expected=%c received=%c bytes=%zd\n",
		             expected, token == '\0' ? '?' : token, count);
		return false;
	}
	std::fprintf(stderr, "helper protocol: EOF/error/deadline while expecting=%c\n", expected);
	return false;
}

std::string unique_name() {
	const auto ticks = std::chrono::duration_cast<std::chrono::nanoseconds>(
	                           Clock::now().time_since_epoch())
	                           .count();
	return "selfipcs_queue_pool_demo_" + std::to_string(::getpid()) + "_" +
	       std::to_string(ticks);
}

HelperProcess::~HelperProcess() {
	if (running()) (void)stop();
	close_socket();
}

bool HelperProcess::spawn(const std::string& name, const char* mode) {
	if (running() || descriptor_ >= 0) return false;
	SocketPair sockets;
	if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0,
	                 sockets.descriptors.data()) != 0) {
		std::perror("socketpair");
		return false;
	}
	const char* configured = std::getenv("SELFIPCS_DEMO_CHILD");
	std::string executable =
	        configured != nullptr && configured[0] != '\0' ? configured : "/proc/self/exe";
	std::string helper_mode = mode;
	std::string child_name = name;
	std::string child_descriptor = std::to_string(sockets.descriptors[1]);
	std::string parent_pid = std::to_string(::getpid());
	std::array<char*, 6> arguments{executable.data(), helper_mode.data(),
	                               child_name.data(), child_descriptor.data(),
	                               parent_pid.data(), nullptr};
	pid_ = ::fork();
	if (pid_ == 0) {
		(void)::close(sockets.descriptors[0]);
		if (::setpgid(0, 0) != 0 || ::fcntl(sockets.descriptors[1], F_SETFD, 0) != 0)
			::_exit(126);
		::execv(executable.c_str(), arguments.data());
		::_exit(127);
	}
	descriptor_ = sockets.descriptors[0];
	sockets.descriptors[0] = -1;
	if (pid_ < 0) {
		std::perror("fork");
		close_socket();
		return false;
	}
	if (::setpgid(pid_, pid_) != 0 && errno != EACCES && errno != ESRCH) {
		std::perror("setpgid");
		return false;
	}
	std::printf("helper: fork/exec pid=%ld pgid=%ld executable=%s\n", static_cast<long>(pid_),
	            static_cast<long>(pid_), executable.c_str());
	return true;
}

bool HelperProcess::send(char token) {
	const bool sent = send_token(descriptor_, token, Clock::now() + std::chrono::seconds(3));
	if (!sent) std::fprintf(stderr, "helper protocol: cannot send=%c\n", token);
	return sent;
}

bool HelperProcess::expect(char token) {
	return expect_token(descriptor_, token, Clock::now() + std::chrono::seconds(3));
}

bool HelperProcess::reap_until(Clock::time_point deadline) {
	while (running()) {
		siginfo_t observed{};
		const int observation = ::waitid(P_PID, static_cast<id_t>(pid_), &observed,
		                                 WEXITED | WNOHANG | WNOWAIT);
		if (observation < 0 && errno != EINTR) {
			std::perror("waitid(WNOWAIT)");
			return false;
		}
		if (observation == 0 && observed.si_pid == pid_) {
			if (observed.si_code != CLD_EXITED || observed.si_status != 0)
				signal_group(SIGKILL);
			const pid_t result = ::waitpid(pid_, &status_, WNOHANG);
			if (result == pid_) {
				pid_ = -1;
				return true;
			}
			if (result < 0 && errno != EINTR) {
				std::perror("waitpid(WNOHANG)");
				return false;
			}
		}
		if (Clock::now() >= deadline) return false;
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
	}
	return true;
}

bool HelperProcess::wait_for_success(Clock::time_point deadline) {
	if (!reap_until(deadline)) {
		std::fprintf(stderr, "helper: bounded waitpid failed\n");
		return false;
	}
	if (!WIFEXITED(status_) || WEXITSTATUS(status_) != 0) {
		std::fprintf(stderr, "helper: unsuccessful wait status=%d\n", status_);
		return false;
	}
	return true;
}

void HelperProcess::close_socket() noexcept {
	if (descriptor_ >= 0) {
		(void)::close(descriptor_);
		descriptor_ = -1;
	}
}

void HelperProcess::signal_group(int signal) const noexcept {
	if (::kill(-pid_, signal) != 0 && errno == ESRCH) (void)::kill(pid_, signal);
}

bool HelperProcess::stop() {
	close_socket();
	if (!running()) return true;
	signal_group(SIGTERM);
	const auto grace = Clock::now() + std::chrono::milliseconds(300);
	while (Clock::now() < grace) {
		siginfo_t observed{};
		const int result = ::waitid(P_PID, static_cast<id_t>(pid_), &observed,
		                            WEXITED | WNOHANG | WNOWAIT);
		if (result == 0 && observed.si_pid == pid_) break;
		if (result < 0 && errno != EINTR) break;
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
	}
	signal_group(SIGKILL);
	if (reap_until(Clock::now() + std::chrono::seconds(1))) return true;
	std::fprintf(stderr, "helper: process group pid=%ld not reaped within cleanup budget\n",
	             static_cast<long>(pid_));
	return false;
}

}  // namespace queue_pool_demo
