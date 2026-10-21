#ifndef EDGE_RUNTIME_TOOL_BENCH_CHILD_HPP
#define EDGE_RUNTIME_TOOL_BENCH_CHILD_HPP

#include <sys/resource.h>
#include <sys/types.h>
#include <time.h>

#include <cstdint>
#include <string>
#include <vector>

namespace edge_tool {
struct ChildResult {
	int exit_code = 127;
	int signum = 0;
	bool exec_ok = false;
	bool timed_out = false;
	std::string out;
	uint64_t wall_us = 0;
	struct rusage ru {};
};
struct ChildHandle {
	pid_t pid = -1;
	int pipe_read = -1;
	struct timespec spawned {};
	bool valid() const { return pid > 0 && pipe_read >= 0; }
};
ChildHandle spawn_child(const std::vector<std::string>& arguments, int inherited_fd = -1);
int64_t worker_deadline_ms(uint64_t work_budget_ms, int64_t now_ms) noexcept;
ChildResult collect_child(ChildHandle handle, int64_t deadline_ms);
}
#endif
