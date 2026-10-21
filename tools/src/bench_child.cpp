#include "edge_runtime_tool/bench_child.hpp"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/prctl.h>
#include <unistd.h>

#include <cerrno>
#include <limits>
#include <cstdio>
#include <cstring>
#include <utility>

#include "edge_runtime_tool/tool_common.hpp"

namespace edge_tool {
int64_t worker_deadline_ms(uint64_t work_budget_ms, int64_t now_ms) noexcept {
	constexpr int64_t startup_grace_ms = 17'000;
	const int64_t start = now_ms > 0 ? now_ms : 0;
	const int64_t limit = std::numeric_limits<int64_t>::max();
	const uint64_t work = work_budget_ms == 0 ? 60'000 : work_budget_ms;
	const uint64_t available = static_cast<uint64_t>(limit - start);
	if (work >= available || available - work < static_cast<uint64_t>(startup_grace_ms)) {
		return limit;
	}
	return start + static_cast<int64_t>(work) + startup_grace_ms;
}

ChildHandle spawn_child(const std::vector<std::string>& arguments, int inherited_fd) {
	std::vector<char*> pointers;
	for (const auto& argument : arguments) pointers.push_back(const_cast<char*>(argument.c_str()));
	pointers.push_back(nullptr);
	int descriptors[2]{-1, -1};
	if (::pipe2(descriptors, O_CLOEXEC | O_NONBLOCK) != 0) return {};
	const pid_t parent = ::getpid();
	const pid_t child = ::fork();
	if (child < 0) { (void)::close(descriptors[0]); (void)::close(descriptors[1]); return {}; }
	if (child == 0) {
		if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || ::getppid() != parent) _exit(126);
		if (::setpgid(0, 0) != 0) _exit(126);
		if (inherited_fd >= 0 && ::fcntl(inherited_fd, F_SETFD, 0) != 0) _exit(126);
		(void)::close(descriptors[0]);
		const int flags = ::fcntl(descriptors[1], F_GETFL);
		if (flags < 0 || ::fcntl(descriptors[1], F_SETFL, flags & ~O_NONBLOCK) != 0) _exit(126);
		if (::dup2(descriptors[1], STDOUT_FILENO) < 0 ||
		    ::dup2(descriptors[1], STDERR_FILENO) < 0) _exit(126);
		(void)::close(descriptors[1]);
		::execvp(pointers[0], pointers.data());
		_exit(127);
	}
	(void)::setpgid(child, child);
	(void)::close(descriptors[1]);
	struct timespec spawned {};
	(void)::clock_gettime(CLOCK_MONOTONIC, &spawned);
	return {child, descriptors[0], spawned};
}

ChildResult collect_child(ChildHandle handle, int64_t deadline_ms) {
	ChildResult result;
	if (!handle.valid()) return result;
	bool killed = false;
	int64_t cleanup_deadline = 0;
	for (;;) {
		bool eof = false;
		for (;;) {
			char buffer[4096]{};
			const ssize_t bytes = ::read(handle.pipe_read, buffer, sizeof(buffer));
			if (bytes > 0) {
				result.out.append(buffer, static_cast<size_t>(bytes));
				if (result.out.size() > 8 * 1024 * 1024) break;
				continue;
			}
			if (bytes == 0 || (bytes < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) eof = true;
			if (bytes < 0 && errno == EINTR &&
			    (deadline_ms == 0 || monotonic_ms_now() < deadline_ms)) continue;
			break;
		}
		if (eof) break;
		const bool output_limit = result.out.size() > 8 * 1024 * 1024;
		if (!killed && (output_limit || (deadline_ms != 0 && monotonic_ms_now() >= deadline_ms))) {
			result.timed_out = true;
			if (output_limit) result.out += "WATCHDOG output limit exceeded\n";
			else result.out += "WATCHDOG timeout waiting for benchmark child or descendants\n";
			(void)::kill(-handle.pid, SIGKILL);
			killed = true;
			cleanup_deadline = monotonic_ms_now() + 2000;
		}
		if (killed && monotonic_ms_now() >= cleanup_deadline) break;
		struct pollfd descriptor{handle.pipe_read, POLLIN, 0};
		if (::poll(&descriptor, 1, 10) < 0 && errno != EINTR) break;
	}
	(void)::close(handle.pipe_read);
	int status = 0;
	pid_t waited = 0;
	for (;;) {
		waited = ::wait4(handle.pid, &status, WNOHANG, &result.ru);
		if (waited == handle.pid || (waited < 0 && errno != EINTR)) break;
		if (deadline_ms != 0 && monotonic_ms_now() >= deadline_ms) {
			result.timed_out = true;
			(void)::kill(-handle.pid, SIGKILL);
		}
		(void)::poll(nullptr, 0, 1);
	}
	struct timespec finished {};
	(void)::clock_gettime(CLOCK_MONOTONIC, &finished);
	const int64_t seconds = static_cast<int64_t>(finished.tv_sec) - handle.spawned.tv_sec;
	const int64_t nanoseconds = static_cast<int64_t>(finished.tv_nsec) - handle.spawned.tv_nsec;
	result.wall_us = static_cast<uint64_t>(seconds * 1'000'000 + nanoseconds / 1000);
	if (waited == handle.pid && WIFEXITED(status)) {
		result.exec_ok = true;
		result.exit_code = WEXITSTATUS(status);
	} else if (waited == handle.pid && WIFSIGNALED(status)) {
		result.exec_ok = true;
		result.signum = WTERMSIG(status);
	}
	return result;
}
}
