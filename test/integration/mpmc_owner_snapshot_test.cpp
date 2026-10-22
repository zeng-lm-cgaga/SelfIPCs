#include <gtest/gtest.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include <sys/mman.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include "edge_runtime/common/error.hpp"
#include "edge_runtime/queue/mpmc_layout.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "mpmc_payload.hpp"
#include "retained_shm.hpp"
#include "test_util.hpp"

namespace {

using Queue = edge_runtime::MpmcQueue<MpmcTestPayload>;
using edge_runtime::ErrorCode;
using edge_runtime::MpmcQueueOptions;

std::string g_helper;

MpmcQueueOptions options_for(const std::string& name, uint32_t capacity) {
	MpmcQueueOptions options;
	options.name = name;
	options.capacity = capacity;
	return options;
}

std::vector<std::string> producer_args(const std::string& name, uint32_t capacity,
									   uint64_t count = 1, uint32_t id = 7) {
	return {g_helper,
			"--role",
			"producer",
			"--name",
			name,
			"--capacity",
			std::to_string(capacity),
			"--count",
			std::to_string(count),
			"--id",
			std::to_string(id)};
}

std::vector<std::string> status_args(const std::string& name, uint32_t capacity) {
	return {g_helper,
			"--role",
			"status",
			"--name",
			name,
			"--capacity",
			std::to_string(capacity)};
}

std::vector<std::string> consumer_args(
	const std::string& name, uint32_t capacity, uint64_t count = 1) {
	return {g_helper,
				"--role",
				"consumer",
				"--name",
				name,
				"--capacity",
				std::to_string(capacity),
				"--count",
				std::to_string(count)};
}

std::vector<std::string> probe_producer_args(
	const std::string& name, uint32_t capacity, uint32_t id = 12) {
	return {g_helper,
				"--role",
				"probe-producer",
				"--name",
				name,
				"--capacity",
				std::to_string(capacity),
				"--id",
				std::to_string(id)};
}

std::vector<std::string> wait_probe_producer_args(
	const std::string& name, uint32_t capacity, int ready_fd, uint32_t id = 11) {
	return {g_helper,
				"--role",
				"wait-probe-producer",
				"--name",
				name,
				"--capacity",
				std::to_string(capacity),
				"--ready-fd",
				std::to_string(ready_fd),
				"--id",
				std::to_string(id)};
}

bool spawn_with_failpoint(edge_test::SpawnedChild* child,
		const std::vector<std::string>& args, const char* failpoint) {
	if (::setenv("EDGE_FAILPOINT", failpoint, 1) != 0) return false;
	if (::setenv("EDGE_FAILPOINT_MODE", "stop", 1) != 0) {
		::unsetenv("EDGE_FAILPOINT");
		return false;
	}
	const bool spawned = child->spawn(args);
	::unsetenv("EDGE_FAILPOINT");
	::unsetenv("EDGE_FAILPOINT_MODE");
	return spawned;
}

bool spawn_with_failpoint_and_trace(edge_test::SpawnedChild* child,
		const std::vector<std::string>& args, const char* failpoint,
		edge_test::FutexTracePipe* trace) {
	if (::setenv("EDGE_FAILPOINT", failpoint, 1) != 0 ||
		::setenv("EDGE_FAILPOINT_MODE", "stop", 1) != 0 ||
		!edge_test::set_futex_trace_fd(trace->write_fd()) ||
		!edge_test::set_futex_trace_status_fd(trace->status_write_fd())) {
		::unsetenv("EDGE_FAILPOINT");
		::unsetenv("EDGE_FAILPOINT_MODE");
		edge_test::clear_futex_trace_fd();
		edge_test::clear_futex_trace_status_fd();
		return false;
	}
	const bool spawned = child->spawn(args);
	::unsetenv("EDGE_FAILPOINT");
	::unsetenv("EDGE_FAILPOINT_MODE");
	edge_test::clear_futex_trace_fd();
	edge_test::clear_futex_trace_status_fd();
	return spawned;
}

bool wait_until_stopped(pid_t pid, int timeout_ms) {
	const int64_t deadline = edge_test::monotonic_ms_now() + timeout_ms;
	for (;;) {
		int status = 0;
		const pid_t result = ::waitpid(pid, &status, WUNTRACED | WNOHANG);
		if (result == pid) {
			return WIFSTOPPED(status);
		}
		if (result < 0 && errno != EINTR) {
			return false;
		}
		if (edge_test::monotonic_ms_now() >= deadline) return false;
		const timespec delay {0, 1'000'000};
		(void)::nanosleep(&delay, nullptr);
	}
}

bool kill_and_reap(edge_test::SpawnedChild* child) {
	child->kill(SIGKILL);
	std::string output;
	(void)child->wait(5000, &output);
	return child->reaped();
}

bool continue_and_reap(edge_test::SpawnedChild* child, std::string* output) {
	if (::kill(child->pid(), SIGCONT) != 0) return false;
	return child->wait(10000, output) && child->reaped();
}

bool read_ready_token(int fd, std::string* output, int timeout_ms) {
	struct pollfd descriptor {fd, POLLIN, 0};
	if (::poll(&descriptor, 1, timeout_ms) != 1 ||
	    (descriptor.revents & (POLLIN | POLLHUP)) == 0) {
		return false;
	}
	char buffer[128];
	const ssize_t count = ::read(fd, buffer, sizeof(buffer));
	if (count <= 0) return false;
	output->assign(buffer, static_cast<size_t>(count));
	return true;
}

struct ShmCleanup {
	explicit ShmCleanup(std::string queue_name) : name(std::move(queue_name)) {}
	~ShmCleanup() {
		const std::string shm_name = edge_runtime::detail::mpmc_shm_name(name);
		EXPECT_TRUE(edge_test::retain_shm_object(shm_name));
	}

	std::string name;
};

TEST(MpmcOwnerSnapshot, ReplacementDuringSnapshotReturnsOneCompleteOwner) {
	const std::string name = edge_test::unique_channel_name("mpmc_owner_aba");
	ShmCleanup cleanup(name);
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::SpawnedChild original;
	ASSERT_TRUE(spawn_with_failpoint(
			&original, producer_args(name, 2), "MPMC_ENQUEUE_GATE_OWNED"));
	ASSERT_TRUE(wait_until_stopped(original.pid(), 5000));

	edge_test::SpawnedChild observer;
	ASSERT_TRUE(spawn_with_failpoint(
			&observer, status_args(name, 2), "MPMC_OWNER_SNAPSHOT_AFTER_EPOCH1"));
	ASSERT_TRUE(wait_until_stopped(observer.pid(), 5000));

	std::string original_output;
	ASSERT_TRUE(continue_and_reap(&original, &original_output)) << original_output;
	EXPECT_EQ(original.exit_code(), 0) << original_output;

	edge_test::SpawnedChild replacement;
	ASSERT_TRUE(spawn_with_failpoint(
			&replacement, producer_args(name, 2, 1, 8), "MPMC_ENQUEUE_GATE_OWNED"));
	ASSERT_TRUE(wait_until_stopped(replacement.pid(), 5000));

	std::string output;
	ASSERT_TRUE(continue_and_reap(&observer, &output)) << output;
	EXPECT_EQ(observer.exit_code(), 0) << output;
	const std::string replacement_pid = "enqueue_pid=" +
										std::to_string(static_cast<uint64_t>(replacement.pid()));
	const std::string original_pid = "enqueue_pid=" +
									 std::to_string(static_cast<uint64_t>(original.pid()));
	EXPECT_NE(output.find(replacement_pid), std::string::npos) << output;
	EXPECT_EQ(output.find(original_pid), std::string::npos) << output;
	EXPECT_NE(output.find("enqueue_alive=1"), std::string::npos) << output;

	ASSERT_TRUE(kill_and_reap(&replacement));
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcOwnerSnapshot, OddEpochAfterReleaseIsContention) {
	const std::string name = edge_test::unique_channel_name("mpmc_owner_odd");
	ShmCleanup cleanup(name);
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::SpawnedChild original;
	ASSERT_TRUE(spawn_with_failpoint(
			&original, producer_args(name, 2), "MPMC_ENQUEUE_GATE_OWNED"));
	ASSERT_TRUE(wait_until_stopped(original.pid(), 5000));

	edge_test::SpawnedChild observer;
	ASSERT_TRUE(spawn_with_failpoint(
			&observer, status_args(name, 2), "MPMC_OWNER_SNAPSHOT_AFTER_STATE1"));
	ASSERT_TRUE(wait_until_stopped(observer.pid(), 5000));

	std::string original_output;
	ASSERT_TRUE(continue_and_reap(&original, &original_output)) << original_output;
	EXPECT_EQ(original.exit_code(), 0) << original_output;

	edge_test::SpawnedChild replacement;
	ASSERT_TRUE(spawn_with_failpoint(
			&replacement, producer_args(name, 2, 1, 8),
			"MPMC_ENQUEUE_GATE_OWNER_EPOCH_ODD"));
	ASSERT_TRUE(wait_until_stopped(replacement.pid(), 5000));

	std::string observer_output;
	ASSERT_TRUE(continue_and_reap(&observer, &observer_output)) << observer_output;
	EXPECT_EQ(observer.exit_code(), 3) << observer_output;
	EXPECT_NE(observer_output.find("STATUS_FAIL code=QueueContention"),
			  std::string::npos)
			<< observer_output;

	std::string replacement_output;
	ASSERT_TRUE(continue_and_reap(&replacement, &replacement_output))
			<< replacement_output;
	EXPECT_EQ(replacement.exit_code(), 0) << replacement_output;
	ASSERT_TRUE(queue.value().try_pop());
	ASSERT_TRUE(queue.value().try_pop());
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcOwnerSnapshot, InactiveBudgetSnapshotRetriesAcrossConsumerClaim) {
	const char* producer_roles[] = {"probe-producer", "wait-probe-producer"};
	for (const char* producer_role : producer_roles) {
		const std::string name = edge_test::unique_channel_name("mpmc_inactive_budget");
		ShmCleanup cleanup(name);
		auto queue = Queue::create(options_for(name, 4), MpmcTestSchema());
		ASSERT_TRUE(queue);
		ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{101, 1, 0x4D504D43u}));

		edge_test::SpawnedChild producer;
		std::vector<std::string> producer_argv;
		int ready_pipe[2] = {-1, -1};
		edge_test::FutexTracePipe futex_trace;
		if (std::strcmp(producer_role, "probe-producer") == 0) {
			producer_argv = probe_producer_args(name, 4);
		} else {
			ASSERT_EQ(::pipe(ready_pipe), 0);
			ASSERT_TRUE(futex_trace.open());
			producer_argv = wait_probe_producer_args(name, 4, ready_pipe[1]);
		}
		if (std::strcmp(producer_role, "probe-producer") == 0) {
			ASSERT_TRUE(spawn_with_failpoint(
					&producer, producer_argv, "MPMC_OWNER_SNAPSHOT_AFTER_STATE1"));
		} else {
			ASSERT_TRUE(spawn_with_failpoint_and_trace(
					&producer, producer_argv, "MPMC_OWNER_SNAPSHOT_AFTER_STATE1", &futex_trace));
		}
		if (ready_pipe[1] >= 0) {
			::close(ready_pipe[1]);
			ready_pipe[1] = -1;
		}
		ASSERT_TRUE(wait_until_stopped(producer.pid(), 5000));

		edge_test::SpawnedChild consumer;
		ASSERT_TRUE(spawn_with_failpoint(
				&consumer, consumer_args(name, 4), "MPMC_DEQUEUE_GATE_OWNER_EPOCH_ODD"));
		ASSERT_TRUE(wait_until_stopped(consumer.pid(), 5000));

		ASSERT_EQ(::kill(producer.pid(), SIGCONT), 0);
		if (std::strcmp(producer_role, "wait-probe-producer") == 0) {
			std::string ready_output;
			ASSERT_TRUE(read_ready_token(ready_pipe[0], &ready_output, 5000));
			::close(ready_pipe[0]);
			ready_pipe[0] = -1;
			EXPECT_EQ(ready_output, "FIRST_TRY_QUEUE_CONTENTION\n");
			ASSERT_TRUE(futex_trace.wait_for("FUTEX_WAIT_CALL", 5000))
					<< futex_trace.transcript();
			ASSERT_TRUE(futex_trace.wait_for("FUTEX_WAIT_RETURN", 5000))
					<< futex_trace.transcript();
			futex_trace.drain();
			EXPECT_FALSE(futex_trace.has_dropped_events()) << futex_trace.transcript();
		}

		std::string producer_output;
		if (std::strcmp(producer_role, "probe-producer") == 0) {
			ASSERT_TRUE(producer.wait(10000, &producer_output));
			EXPECT_EQ(producer.exit_code(), 3) << producer_output;
			EXPECT_NE(producer_output.find("PUSH_FAIL code=QueueContention"), std::string::npos)
					<< producer_output;
		} else {
			ASSERT_EQ(::kill(consumer.pid(), SIGCONT), 0);
			std::string consumer_output;
			ASSERT_TRUE(consumer.wait(10000, &consumer_output)) << consumer_output;
			EXPECT_EQ(consumer.exit_code(), 0) << consumer_output;
			EXPECT_NE(consumer_output.find("ITEM producer=1 sequence=101"), std::string::npos)
					<< consumer_output;
			ASSERT_TRUE(producer.wait(10000, &producer_output));
			EXPECT_EQ(producer.exit_code(), 0) << producer_output;
			uint64_t attempted = 0;
			uint64_t emitted = 0;
			uint64_t dropped = 0;
			bool complete = false;
			ASSERT_TRUE(futex_trace.wait_for_status(producer.pid(), &attempted, &emitted, &dropped,
			                                       &complete, 1000))
					<< futex_trace.status_transcript();
			EXPECT_TRUE(complete);
			EXPECT_EQ(dropped, 0u);
			EXPECT_EQ(attempted, emitted + dropped);
			EXPECT_NE(producer_output.find("WAIT_PRODUCED producer=11 sequence=0"), std::string::npos)
					<< producer_output;
			auto waited_value = queue.value().try_pop();
			ASSERT_TRUE(waited_value) << waited_value.error().context;
			EXPECT_EQ(waited_value.value().sequence, 0u);
			EXPECT_EQ(waited_value.value().producer, 11u);
		}

		if (std::strcmp(producer_role, "probe-producer") == 0) {
			std::string consumer_output;
			ASSERT_EQ(::kill(consumer.pid(), SIGCONT), 0);
			ASSERT_TRUE(consumer.wait(10000, &consumer_output)) << consumer_output;
			EXPECT_EQ(consumer.exit_code(), 0) << consumer_output;
			EXPECT_NE(consumer_output.find("ITEM producer=1 sequence=101"), std::string::npos)
					<< consumer_output;
		}

		const MpmcTestPayload recovered{303, 13, 0x4D504D43u};
		ASSERT_TRUE(queue.value().try_push(recovered));
		auto popped = queue.value().try_pop();
		ASSERT_TRUE(popped) << popped.error().context;
		EXPECT_EQ(popped.value().sequence, recovered.sequence);
		EXPECT_EQ(popped.value().producer, recovered.producer);
		ASSERT_TRUE(queue.value().remove_if_creator());
	}
}

TEST(MpmcOwnerSnapshot, ClaimingEpochsAreContentionUntilCommit) {
	const char* failpoints[] = {
			"MPMC_ENQUEUE_GATE_OWNER_EPOCH_ODD",
			"MPMC_ENQUEUE_GATE_OWNER_EPOCH_COMMITTED",
	};
	for (const char* failpoint : failpoints) {
		const std::string name = edge_test::unique_channel_name("mpmc_owner_claim");
		ShmCleanup cleanup(name);
		auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
		ASSERT_TRUE(queue);

		edge_test::SpawnedChild producer;
		ASSERT_TRUE(spawn_with_failpoint(&producer, producer_args(name, 2), failpoint));
		ASSERT_TRUE(wait_until_stopped(producer.pid(), 5000));

		auto fd = edge_runtime::detail::shm_open_existing(
				edge_runtime::detail::mpmc_shm_name(name));
		ASSERT_TRUE(fd);
		auto mapping = edge_runtime::detail::mmap_region(
				fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
		ASSERT_TRUE(mapping);
		auto* header =
				static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
		EXPECT_EQ(edge_runtime::detail::shared_load_seq_cst(&header->enqueue_lock),
				  static_cast<uint32_t>(edge_runtime::detail::MpmcLockState::kClaiming));
		const uint64_t epoch =
				edge_runtime::detail::shared_load_seq_cst(&header->enqueue_owner_epoch);
		if (std::strcmp(failpoint, "MPMC_ENQUEUE_GATE_OWNER_EPOCH_ODD") == 0) {
			EXPECT_EQ(epoch & 1u, 1u);
		} else {
			EXPECT_EQ(epoch & 1u, 0u);
			EXPECT_NE(epoch, 0u);
		}
		mapping.value().reset();
		fd.value().reset();

		auto status = queue.value().status();
		ASSERT_FALSE(status);
		EXPECT_EQ(status.error().code, ErrorCode::kQueueContention);

		auto blocked = queue.value().try_push(MpmcTestPayload{1, 99, 0x4D504D43u});
		ASSERT_FALSE(blocked);
		EXPECT_EQ(blocked.error().code, ErrorCode::kQueueContention);

		std::string output;
		ASSERT_TRUE(continue_and_reap(&producer, &output)) << output;
		EXPECT_EQ(producer.exit_code(), 0) << output;
		auto value = queue.value().try_pop();
		ASSERT_TRUE(value) << value.error().context;
		EXPECT_EQ(value.value().producer, 7u);
		ASSERT_TRUE(queue.value().remove_if_creator());
	}
}

TEST(MpmcOwnerSnapshot, CommittedOwnerDeathIsVisibleAndFailClosed) {
	const std::string name = edge_test::unique_channel_name("mpmc_owner_dead");
	ShmCleanup cleanup(name);
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(
			&victim, producer_args(name, 2), "MPMC_ENQUEUE_GATE_OWNED"));
	ASSERT_TRUE(wait_until_stopped(victim.pid(), 5000));

	auto live_status = queue.value().status();
	ASSERT_TRUE(live_status) << live_status.error().context;
	EXPECT_EQ(live_status.value().enqueue_lock_state,
			  static_cast<uint32_t>(edge_runtime::detail::MpmcLockState::kOwned));
	EXPECT_EQ(live_status.value().enqueue_owner_pid, static_cast<uint64_t>(victim.pid()));
	EXPECT_TRUE(live_status.value().enqueue_owner_alive);

	auto busy = queue.value().try_push(MpmcTestPayload{1, 99, 0x4D504D43u});
	ASSERT_FALSE(busy);
	EXPECT_EQ(busy.error().code, ErrorCode::kQueueContention);

	ASSERT_TRUE(kill_and_reap(&victim));
	auto dead_status = queue.value().status();
	ASSERT_TRUE(dead_status) << dead_status.error().context;
	EXPECT_EQ(dead_status.value().enqueue_owner_pid, static_cast<uint64_t>(victim.pid()));
	EXPECT_FALSE(dead_status.value().enqueue_owner_alive);

	auto blocked = queue.value().try_push(MpmcTestPayload{1, 99, 0x4D504D43u});
	ASSERT_FALSE(blocked);
	EXPECT_EQ(blocked.error().code, ErrorCode::kRecoveryBlocked);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcOwnerSnapshot, NormalGateReleaseIsNotReportedAsDeadOwner) {
	const std::string name = edge_test::unique_channel_name("mpmc_owner_release");
	ShmCleanup cleanup(name);
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	int ready_pipe[2] = {-1, -1};
	ASSERT_EQ(::pipe(ready_pipe), 0);
	edge_test::SpawnedChild original;
	ASSERT_TRUE(spawn_with_failpoint(
			&original, producer_args(name, 2), "MPMC_ENQUEUE_GATE_OWNED"));
	ASSERT_TRUE(wait_until_stopped(original.pid(), 5000));

	edge_test::SpawnedChild observer;
	ASSERT_TRUE(spawn_with_failpoint(
			&observer, wait_probe_producer_args(name, 2, ready_pipe[1]),
			"MPMC_GATE_OWNER_BEFORE_LIVENESS"));
	::close(ready_pipe[1]);
	ready_pipe[1] = -1;
	ASSERT_TRUE(wait_until_stopped(observer.pid(), 5000));

	std::string original_output;
	ASSERT_TRUE(continue_and_reap(&original, &original_output)) << original_output;
	EXPECT_EQ(original.exit_code(), 0) << original_output;
	ASSERT_EQ(::kill(observer.pid(), SIGCONT), 0);

	std::string ready_output;
	ASSERT_TRUE(read_ready_token(ready_pipe[0], &ready_output, 5000));
	EXPECT_EQ(ready_output, "FIRST_TRY_QUEUE_CONTENTION\n");
	::close(ready_pipe[0]);
	ready_pipe[0] = -1;
	std::string observer_output;
	ASSERT_TRUE(observer.wait(10000, &observer_output)) << observer_output;
	EXPECT_EQ(observer.exit_code(), 0) << observer_output;
	EXPECT_NE(observer_output.find("WAIT_PRODUCED producer=11 sequence=0"), std::string::npos)
			<< observer_output;

	auto first = queue.value().try_pop();
	ASSERT_TRUE(first) << first.error().context;
	EXPECT_EQ(first.value().producer, 7u);
	EXPECT_EQ(first.value().sequence, 0u);
	auto second = queue.value().try_pop();
	ASSERT_TRUE(second) << second.error().context;
	EXPECT_EQ(second.value().producer, 11u);
	EXPECT_EQ(second.value().sequence, 0u);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcOwnerSnapshot, DeadHeldGateStillFailsClosedAfterOwnerProbe) {
	const std::string name = edge_test::unique_channel_name("mpmc_owner_dead_probe");
	ShmCleanup cleanup(name);
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	int ready_pipe[2] = {-1, -1};
	ASSERT_EQ(::pipe(ready_pipe), 0);
	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(
			&victim, producer_args(name, 2), "MPMC_ENQUEUE_GATE_OWNED"));
	ASSERT_TRUE(wait_until_stopped(victim.pid(), 5000));

	edge_test::SpawnedChild observer;
	ASSERT_TRUE(spawn_with_failpoint(
			&observer, wait_probe_producer_args(name, 2, ready_pipe[1]),
			"MPMC_GATE_OWNER_BEFORE_LIVENESS"));
	::close(ready_pipe[1]);
	ready_pipe[1] = -1;
	ASSERT_TRUE(wait_until_stopped(observer.pid(), 5000));
	ASSERT_TRUE(kill_and_reap(&victim));
	ASSERT_EQ(::kill(observer.pid(), SIGCONT), 0);

	std::string observer_output;
	ASSERT_TRUE(observer.wait(10000, &observer_output)) << observer_output;
	EXPECT_EQ(observer.exit_code(), 3) << observer_output;
	EXPECT_NE(observer_output.find("FIRST_TRY_FAIL code=RecoveryBlocked"), std::string::npos)
			<< observer_output;
	::close(ready_pipe[0]);
	ready_pipe[0] = -1;
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcOwnerSnapshot, StatusDoesNotUseReleasedOwnerAfterLivenessProbe) {
	const std::string name = edge_test::unique_channel_name("mpmc_status_owner_release");
	ShmCleanup cleanup(name);
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::SpawnedChild original;
	ASSERT_TRUE(spawn_with_failpoint(
			&original, producer_args(name, 2), "MPMC_ENQUEUE_GATE_OWNED"));
	ASSERT_TRUE(wait_until_stopped(original.pid(), 5000));

	edge_test::SpawnedChild observer;
	ASSERT_TRUE(spawn_with_failpoint(
			&observer, status_args(name, 2), "MPMC_GATE_OWNER_BEFORE_LIVENESS"));
	ASSERT_TRUE(wait_until_stopped(observer.pid(), 5000));

	std::string original_output;
	ASSERT_TRUE(continue_and_reap(&original, &original_output)) << original_output;
	EXPECT_EQ(original.exit_code(), 0) << original_output;
	ASSERT_EQ(::kill(observer.pid(), SIGCONT), 0);

	std::string observer_output;
	ASSERT_TRUE(observer.wait(10000, &observer_output)) << observer_output;
	EXPECT_EQ(observer.exit_code(), 3) << observer_output;
	EXPECT_NE(observer_output.find("STATUS_FAIL code=QueueContention"), std::string::npos)
			<< observer_output;
	ASSERT_TRUE(queue.value().remove_if_creator());
}

// This regression observes slot state/epoch publication and gate contention only. It does not
// expose or claim full snapshot_slot_owner tuple diagnostics through the public status API.
TEST(MpmcOwnerSnapshot, SlotClaimEpochTransitionsBlockGateAndResume) {
	const char* failpoints[] = {
			"MPMC_ENQUEUE_SLOT_OWNER_EPOCH_ODD",
			"MPMC_ENQUEUE_SLOT_OWNER_EPOCH_COMMITTED",
	};
	for (const char* failpoint : failpoints) {
		const std::string name = edge_test::unique_channel_name("mpmc_slot_claim");
		ShmCleanup cleanup(name);
		auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
		ASSERT_TRUE(queue);

		edge_test::SpawnedChild producer;
		ASSERT_TRUE(spawn_with_failpoint(&producer, producer_args(name, 2), failpoint));
		ASSERT_TRUE(wait_until_stopped(producer.pid(), 5000));

		uint64_t offset = 0;
		ASSERT_TRUE(edge_runtime::detail::mpmc_slot_offset(0, 2, 128, &offset));
		auto fd = edge_runtime::detail::shm_open_existing(
				edge_runtime::detail::mpmc_shm_name(name));
		ASSERT_TRUE(fd);
		auto mapping = edge_runtime::detail::mmap_region(
				fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi) + 2u * 128u);
		ASSERT_TRUE(mapping);
		auto* slot = reinterpret_cast<edge_runtime::detail::MpmcSlotHeaderAbi*>(
				static_cast<std::byte*>(mapping.value().get()) + offset);
		EXPECT_EQ(edge_runtime::detail::shared_load_seq_cst(&slot->state),
				  static_cast<uint32_t>(edge_runtime::detail::MpmcSlotState::kEnqueueClaiming));
		const uint64_t epoch = edge_runtime::detail::shared_load_seq_cst(&slot->owner_epoch);
		if (std::strcmp(failpoint, "MPMC_ENQUEUE_SLOT_OWNER_EPOCH_ODD") == 0) {
			EXPECT_EQ(epoch & 1u, 1u);
		} else {
			EXPECT_EQ(epoch & 1u, 0u);
			EXPECT_NE(epoch, 0u);
		}
		mapping.value().reset();
		fd.value().reset();

		auto status = queue.value().status();
		ASSERT_TRUE(status) << status.error().context;
		EXPECT_EQ(status.value().enqueue_owner_pid, static_cast<uint64_t>(producer.pid()));
		EXPECT_TRUE(status.value().enqueue_owner_alive);
		auto blocked = queue.value().try_push(MpmcTestPayload{2, 99, 0x4D504D43u});
		ASSERT_FALSE(blocked);
		EXPECT_EQ(blocked.error().code, ErrorCode::kQueueContention);

		std::string output;
		ASSERT_TRUE(continue_and_reap(&producer, &output)) << output;
		EXPECT_EQ(producer.exit_code(), 0) << output;
		auto value = queue.value().try_pop();
		ASSERT_TRUE(value) << value.error().context;
		EXPECT_EQ(value.value().producer, 7u);
		ASSERT_TRUE(queue.value().remove_if_creator());
	}
}

TEST(MpmcOwnerSnapshot, EpochExhaustionDoesNotWrapOrDestroyPublishedPayload) {
	const uint64_t max_committed = UINT64_MAX - 1u;

	{
		const std::string name = edge_test::unique_channel_name("mpmc_gate_exhaust");
		ShmCleanup cleanup(name);
		auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
		ASSERT_TRUE(queue);

		auto fd = edge_runtime::detail::shm_open_existing(
				edge_runtime::detail::mpmc_shm_name(name));
		ASSERT_TRUE(fd);
		auto mapping = edge_runtime::detail::mmap_region(
				fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
		ASSERT_TRUE(mapping);
		auto* header =
				static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
		edge_runtime::detail::shared_store_seq_cst(&header->enqueue_owner_epoch, max_committed);
		mapping.value().reset();
		fd.value().reset();

		const auto before = queue.value().try_push(MpmcTestPayload{1, 1, 0x4D504D43u});
		ASSERT_FALSE(before);
		EXPECT_EQ(before.error().code, ErrorCode::kRecoveryBlocked);
	}

	{
		const std::string name = edge_test::unique_channel_name("mpmc_slot_exhaust");
		ShmCleanup cleanup(name);
		auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
		ASSERT_TRUE(queue);

		auto fd = edge_runtime::detail::shm_open_existing(
				edge_runtime::detail::mpmc_shm_name(name));
		ASSERT_TRUE(fd);
		auto mapping = edge_runtime::detail::mmap_region(
				fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi) + 128u);
		ASSERT_TRUE(mapping);
		uint64_t offset = 0;
		ASSERT_TRUE(edge_runtime::detail::mpmc_slot_offset(0, 1, 128, &offset));
		auto* slot = reinterpret_cast<edge_runtime::detail::MpmcSlotHeaderAbi*>(
				static_cast<std::byte*>(mapping.value().get()) + offset);
		edge_runtime::detail::shared_store_seq_cst(
				&slot->owner_epoch, max_committed - 4u);
		mapping.value().reset();
		fd.value().reset();

		ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{17, 3, 0x4D504D43u}));
		auto popped = queue.value().try_pop();
		ASSERT_TRUE(popped) << popped.error().context;
		EXPECT_EQ(popped.value().sequence, 17u);

		auto exhausted = queue.value().try_push(MpmcTestPayload{18, 3, 0x4D504D43u});
		ASSERT_FALSE(exhausted);
		EXPECT_EQ(exhausted.error().code, ErrorCode::kRecoveryBlocked);

		fd = edge_runtime::detail::shm_open_existing(
				edge_runtime::detail::mpmc_shm_name(name));
		ASSERT_TRUE(fd);
		mapping = edge_runtime::detail::mmap_region(
				fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi) + 128u);
		ASSERT_TRUE(mapping);
		slot = reinterpret_cast<edge_runtime::detail::MpmcSlotHeaderAbi*>(
				static_cast<std::byte*>(mapping.value().get()) + offset);
		EXPECT_EQ(edge_runtime::detail::shared_load_seq_cst(&slot->owner_epoch), max_committed);
		EXPECT_EQ(edge_runtime::detail::shared_load_seq_cst(&slot->state),
				  static_cast<uint32_t>(edge_runtime::detail::MpmcSlotState::kEpochExhausted));
		mapping.value().reset();
		fd.value().reset();

		ASSERT_TRUE(queue.value().remove_if_creator());
	}
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc != 2) {
		std::fprintf(stderr, "usage: mpmc_owner_snapshot_test <mpmc_child>\n");
		return 2;
	}
	g_helper = argv[1];
	return RUN_ALL_TESTS();
}
