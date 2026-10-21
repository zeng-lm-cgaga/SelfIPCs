#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <thread>

#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/process/process_identity.hpp"

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using edge_runtime::ErrorCode;
using edge_runtime::SharedBufferPool;
using edge_runtime::detail::Liveness;

pid_t owner_pid = -1;
int control_fd = -1;
bool armed = false;
bool scheduled = false;
char action = 'A';

edge_runtime::SharedBufferPoolOptions options_for(const std::string& name) {
	edge_runtime::SharedBufferPoolOptions options;
	options.name = name;
	options.block_size = 4096;
	options.block_count = 1;
	options.schema.version = 1;
	return options;
}

bool send_byte(int descriptor, char value) {
	for (unsigned attempt = 0; attempt < 8; ++attempt) {
		const auto result = ::send(descriptor, &value, 1, MSG_NOSIGNAL);
		if (result == 1) return true;
		if (result < 0 && errno == EINTR) continue;
		return false;
	}
	return false;
}

bool expect_byte(int descriptor, char expected) {
	const auto deadline = Clock::now() + 2s;
	while (Clock::now() < deadline) {
		pollfd ready{descriptor, POLLIN, 0};
		const int result = ::poll(&ready, 1, 10);
		if (result < 0 && errno == EINTR) continue;
		if (result < 0) return false;
		if (result == 0) continue;
		char value = 0;
		return ::recv(descriptor, &value, 1, MSG_DONTWAIT) == 1 && value == expected;
	}
	return false;
}

bool reap_owner() {
	const auto deadline = Clock::now() + 2s;
	while (Clock::now() < deadline) {
		int status = 0;
		const pid_t result = ::waitpid(owner_pid, &status, WNOHANG);
		if (result == owner_pid) {
			owner_pid = -1;
			return WIFEXITED(status) && WEXITSTATUS(status) == 0;
		}
		if (result < 0 && errno != EINTR) return false;
		std::this_thread::sleep_for(1ms);
	}
	return false;
}

int owner_main(const std::string& name, int descriptor) {
	auto opened = SharedBufferPool::open(options_for(name));
	if (!opened) return 2;
	auto write = opened.value().try_acquire_write();
	if (!write) return 3;
	if (!send_byte(descriptor, 'R')) return 6;
	const auto deadline = Clock::now() + 4s;
	while (Clock::now() < deadline) {
		pollfd ready{descriptor, POLLIN, 0};
		const int result = ::poll(&ready, 1, 10);
		if (result < 0 && errno == EINTR) continue;
		if (result < 0) return 7;
		if (result == 0) continue;
		char command = 0;
		if (::recv(descriptor, &command, 1, MSG_DONTWAIT) != 1) return 8;
		if (command == 'D') ::_exit(0);
		if (command != 'A') return 9;
		write.value().abort();
		return send_byte(descriptor, 'F') ? 0 : 10;
	}
	return 11;
}

class PoolLivenessRace : public ::testing::Test {
       protected:
	void SetUp() override {
		name_ = "pool_probe_race_" + std::to_string(::getpid()) + "_" +
		        std::to_string(Clock::now().time_since_epoch().count());
		shm_name_ = "/edgeruntime.bufferpool." + std::to_string(::getuid()) + "." + name_;
		auto created = SharedBufferPool::create(options_for(name_));
		ASSERT_TRUE(created);
		pool_.emplace(std::move(created.value()));
		armed = false;
		scheduled = false;
		action = 'A';
	}

	void TearDown() override {
		armed = false;
		if (owner_pid > 0) {
			(void)::kill(owner_pid, SIGKILL);
			EXPECT_TRUE(reap_owner());
		}
		if (control_fd >= 0) ::close(control_fd);
		control_fd = -1;
		if (pool_) {
			const auto removed = pool_->remove_if_creator();
			if (preserve_dead_owner_) {
				EXPECT_FALSE(removed);
				if (!removed) {
					EXPECT_EQ(removed.error().code,
					          ErrorCode::kBufferPoolContention);
				}
				std::printf("fixture_preserved_fail_closed=%s\n",
				            shm_name_.c_str());
			} else {
				EXPECT_TRUE(removed);
			}
		}
	}

	void start_owner(bool die_holding = false) {
		int sockets[2];
		ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
		owner_pid = ::fork();
		if (owner_pid == 0) {
			::close(sockets[0]);
			const std::string descriptor = std::to_string(sockets[1]);
			::execl("/proc/self/exe", "pool_liveness_race_test", "--owner",
			        name_.c_str(), descriptor.c_str(), static_cast<char*>(nullptr));
			::_exit(127);
		}
		::close(sockets[1]);
		control_fd = sockets[0];
		ASSERT_GT(owner_pid, 0);
		ASSERT_TRUE(expect_byte(control_fd, 'R'));
		action = die_holding ? 'D' : 'A';
		armed = true;
	}

	void expect_free() {
		auto status = pool_->status();
		ASSERT_TRUE(status);
		EXPECT_EQ(status.value().free_blocks, 1u);
		EXPECT_EQ(status.value().recovery_blocked_blocks, 0u);
		EXPECT_TRUE(scheduled);
	}

	std::optional<SharedBufferPool> pool_;
	std::string name_;
	std::string shm_name_;
	bool preserve_dead_owner_ = false;
};

TEST_F(PoolLivenessRace, WaitRetriesAfterLegalAbortAndExit) {
	start_owner();
	ASSERT_FALSE(HasFatalFailure());
	auto result = pool_->wait_acquire_write_until(Clock::now() + 500ms);
	ASSERT_TRUE(result) << edge_runtime::to_string(result.error().code);
	result.value().abort();
	expect_free();
}

TEST_F(PoolLivenessRace, StatusReportsContentionInsteadOfStaleDeadOwner) {
	start_owner();
	ASSERT_FALSE(HasFatalFailure());
	auto status = pool_->status();
	ASSERT_FALSE(status);
	EXPECT_EQ(status.error().code, ErrorCode::kBufferPoolContention);
	expect_free();
}

TEST_F(PoolLivenessRace, DeadOwnerStillHoldingBlockRemainsRecoveryBlocked) {
	preserve_dead_owner_ = true;
	start_owner(true);
	ASSERT_FALSE(HasFatalFailure());
	auto result = pool_->wait_acquire_write_until(Clock::now() + 500ms);
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, ErrorCode::kRecoveryBlocked);
	EXPECT_TRUE(scheduled);
	auto status = pool_->status();
	ASSERT_TRUE(status);
	EXPECT_EQ(status.value().writing_blocks, 1u);
	EXPECT_EQ(status.value().recovery_blocked_blocks, 1u);
	EXPECT_FALSE(pool_->remove_if_creator());
}

}  // namespace

extern "C" Liveness real_probe(uint64_t,
                               uint64_t) asm("__real__ZN12edge_runtime6detail14probe_livenessEmm");
extern "C" Liveness wrapped_probe(uint64_t, uint64_t) asm(
        "__wrap__ZN12edge_runtime6detail14probe_livenessEmm");
extern "C" Liveness wrapped_probe(uint64_t pid, uint64_t start) {
	if (armed && pid == static_cast<uint64_t>(owner_pid)) {
		armed = false;
		scheduled = send_byte(control_fd, action) &&
		            (action == 'D' || expect_byte(control_fd, 'F')) && reap_owner();
		std::printf("scheduled_real_owner_exit=%d action=%c\n", scheduled ? 1 : 0, action);
	}
	return real_probe(pid, start);
}

int main(int argc, char** argv) {
	if (argc == 4 && std::string(argv[1]) == "--owner")
		return owner_main(argv[2], std::atoi(argv[3]));
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
