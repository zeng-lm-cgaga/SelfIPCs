#include <gtest/gtest.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <poll.h>
#include <signal.h>

#include "edge_runtime_tool/bench_child.hpp"
#include "edge_runtime_tool/tool_common.hpp"

namespace {
using edge_tool::spawn_child;
using edge_tool::collect_child;
TEST(NotificationBenchDriver, ExpiredBudgetPreservesAlreadyFinishedOutput) {
	const auto child = spawn_child({"/bin/sh", "-c", "printf 'SUMMARY finished=1\\n'"});
	ASSERT_TRUE(child.valid());
	siginfo_t information{};
	ASSERT_EQ(::waitid(P_PID, static_cast<id_t>(child.pid), &information,
	                   WEXITED | WNOWAIT), 0);
	const auto result = collect_child(child, edge_tool::monotonic_ms_now() - 1);
	EXPECT_TRUE(result.exec_ok);
	EXPECT_EQ(result.exit_code, 0);
	EXPECT_EQ(result.signum, 0);
	EXPECT_FALSE(result.timed_out);
	EXPECT_NE(result.out.find("SUMMARY finished=1"), std::string::npos);
	EXPECT_EQ(result.out.find("WATCHDOG"), std::string::npos);
}

void check_descendant_cleanup(bool leader_exited) {
	int previous = 0;
	ASSERT_EQ(::prctl(PR_GET_CHILD_SUBREAPER, &previous), 0);
	ASSERT_EQ(::prctl(PR_SET_CHILD_SUBREAPER, 1), 0);
	const auto command = std::string("sleep 30 & printf 'CHILD=%s\\n' \"$!\"; ") +
	                     (leader_exited ? "exit 0" : "wait");
	const auto child = spawn_child({"/bin/sh", "-c", command});
	ASSERT_TRUE(child.valid());
	if (leader_exited) {
		siginfo_t information{};
		ASSERT_EQ(::waitid(P_PID, static_cast<id_t>(child.pid), &information,
		                   WEXITED | WNOWAIT), 0);
	}
	const int64_t started = edge_tool::monotonic_ms_now();
	const auto result = collect_child(child, started + 100);
	EXPECT_LT(edge_tool::monotonic_ms_now() - started, 2500);
	EXPECT_EQ(result.signum, leader_exited ? 0 : SIGKILL);
	EXPECT_TRUE(result.timed_out);
	EXPECT_NE(result.out.find("WATCHDOG"), std::string::npos);
	const size_t marker = result.out.find("CHILD=");
	if (marker != std::string::npos) {
		const pid_t descendant = static_cast<pid_t>(std::strtol(result.out.c_str() + marker + 6, nullptr, 10));
		int status = 0;
		pid_t reaped = 0;
		const int64_t deadline = edge_tool::monotonic_ms_now() + 1000;
		while (reaped == 0 && edge_tool::monotonic_ms_now() < deadline) {
			reaped = ::waitpid(descendant, &status, WNOHANG);
			if (reaped == 0) (void)::poll(nullptr, 0, 1);
		}
		if (reaped == 0) {
			(void)::kill(descendant, SIGKILL);
			while (::waitpid(descendant, &status, 0) < 0 && errno == EINTR) {}
		}
		EXPECT_EQ(reaped, descendant);
		EXPECT_TRUE(WIFSIGNALED(status));
		EXPECT_EQ(WTERMSIG(status), SIGKILL);
	} else {
		ADD_FAILURE() << result.out;
	}
	EXPECT_EQ(::prctl(PR_SET_CHILD_SUBREAPER, previous), 0);
}

TEST(NotificationBenchDriver, WatchdogKillsWrapperAndDescendantGroup) {
	check_descendant_cleanup(false);
}

TEST(NotificationBenchDriver, ExitedWrapperWithLivePipeIsWatchdogFailure) {
	check_descendant_cleanup(true);
}
}
