#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include "edge_runtime/queue/mpmc_layout.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime/sync/futex.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "retained_shm.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/common/error.hpp"
#include "mpmc_payload.hpp"
#include "test_util.hpp"

namespace {

using Queue = edge_runtime::MpmcQueue<MpmcTestPayload>;
using edge_runtime::ErrorCode;
using edge_runtime::MpmcQueueOptions;

std::string g_wait_helper;

MpmcQueueOptions options_for(const std::string& name, uint32_t capacity) {
	MpmcQueueOptions options;
	options.name = name;
	options.capacity = capacity;
	return options;
}

std::vector<std::string> wait_child_args(const char* role, const std::string& name,
                                         uint32_t capacity, uint64_t timeout_ms,
                                         uint32_t producer = 11, uint64_t sequence = 7) {
	return {g_wait_helper,
	        "--role",
	        role,
	        "--name",
	        name,
	        "--capacity",
	        std::to_string(capacity),
	        "--timeout-ms",
	        std::to_string(timeout_ms),
	        "--id",
	        std::to_string(producer),
	        "--sequence",
	        std::to_string(sequence)};
}

void sleep_ms(uint64_t milliseconds) {
	struct timespec delay {};
	delay.tv_sec = static_cast<time_t>(milliseconds / 1000);
	delay.tv_nsec = static_cast<long>((milliseconds % 1000) * 1000000);
	(void)::nanosleep(&delay, nullptr);
}

bool contains(const std::string& text, const char* needle) {
	return text.find(needle) != std::string::npos;
}

bool process_is_stopped(pid_t pid) {
	char path[64];
	std::snprintf(path, sizeof(path), "/proc/%ld/stat", static_cast<long>(pid));
	std::FILE* file = std::fopen(path, "r");
	if (file == nullptr) return false;
	char line[512];
	const size_t size = std::fread(line, 1, sizeof(line) - 1, file);
	std::fclose(file);
	if (size == 0) return false;
	line[size] = '\0';
	const char* close_paren = std::strrchr(line, ')');
	if (close_paren == nullptr) return false;
	const char* state = close_paren + 1;
	while (*state == ' ') ++state;
	return *state == 'T';
}

bool wait_stopped(pid_t pid, int timeout_ms) {
	const int64_t deadline = edge_test::monotonic_ms_now() + timeout_ms;
	while (edge_test::monotonic_ms_now() < deadline) {
		if (process_is_stopped(pid)) return true;
		sleep_ms(10);
	}
	return process_is_stopped(pid);
}

bool process_is_in_futex_wait(pid_t pid) {
	char path[64];
	std::snprintf(path, sizeof(path), "/proc/%ld/syscall", static_cast<long>(pid));
	std::FILE* file = std::fopen(path, "r");
	if (file == nullptr) return false;
	char line[256];
	const bool read = std::fgets(line, sizeof(line), file) != nullptr;
	std::fclose(file);
	if (!read) return false;
	char* end = nullptr;
	const long syscall_number = std::strtol(line, &end, 10);
	return end != line && syscall_number == SYS_futex;
}

bool process_is_alive(pid_t pid) {
	if (::kill(pid, 0) == 0) return true;
	return errno == EPERM;
}

bool wait_for_process_futex_wait(pid_t pid, int timeout_ms) {
	const int64_t deadline = edge_test::monotonic_ms_now() + timeout_ms;
	while (edge_test::monotonic_ms_now() < deadline) {
		if (process_is_in_futex_wait(pid)) return true;
		sleep_ms(1);
	}
	return process_is_in_futex_wait(pid);
}

int bounded_remaining_ms(int64_t deadline_ms, int maximum_ms) {
	const int64_t remaining = deadline_ms - edge_test::monotonic_ms_now();
	if (remaining <= 0) return 0;
	if (remaining < maximum_ms) return static_cast<int>(remaining);
	return maximum_ms;
}

bool wait_for_child_futex_wait_until(edge_test::FutexTracePipe* trace, pid_t pid,
		size_t minimum_calls, int64_t deadline_ms) {
	while (bounded_remaining_ms(deadline_ms, 1) > 0) {
		trace->drain();
		if (trace->count_pid_events(pid, "FUTEX_WAIT_CALL") >= minimum_calls &&
				process_is_in_futex_wait(pid)) {
			return true;
		}
		if (!process_is_alive(pid)) return false;
		sleep_ms(1);
	}
	trace->drain();
	return trace->count_pid_events(pid, "FUTEX_WAIT_CALL") >= minimum_calls &&
			process_is_in_futex_wait(pid);
}

bool interrupt_child_until_return(edge_test::FutexTracePipe* trace, pid_t pid,
		int64_t phase_deadline_ms) {
	for (;;) {
		const int wait_ms = bounded_remaining_ms(phase_deadline_ms, 8);
		if (wait_ms <= 0) return false;
		if (!wait_for_process_futex_wait(pid, wait_ms)) {
			if (!process_is_alive(pid)) return false;
			continue;
		}
		trace->drain();
		const size_t returns_before =
				trace->count_pid_events(pid, "FUTEX_WAIT_RETURN rc=-1 errno=4");
		if (::kill(pid, SIGUSR1) != 0) return false;
		const int observe_ms = bounded_remaining_ms(phase_deadline_ms, 8);
		if (observe_ms > 0 && trace->wait_for_pid_event(
				pid, "FUTEX_WAIT_RETURN rc=-1 errno=4", returns_before + 1, observe_ms)) {
			return true;
		}
		if (!process_is_alive(pid)) return false;
	}
}

bool wake_child_until_return(edge_test::FutexTracePipe* trace, const void* address, pid_t pid,
		int64_t phase_deadline_ms) {
	bool wake_observed = false;
	for (;;) {
		const int wait_ms = bounded_remaining_ms(phase_deadline_ms, 8);
		if (wait_ms <= 0) return false;
		if (!wait_for_process_futex_wait(pid, wait_ms)) {
			if (!process_is_alive(pid)) return false;
			continue;
		}
		trace->drain();
		const size_t returns_before = trace->count_pid_events(pid, "FUTEX_WAIT_RETURN rc=0");
		if (!edge_test::set_futex_trace_fd(trace->write_fd())) return false;
		const int wake_result = edge_runtime::detail::futex_wake(address, INT_MAX);
		edge_test::clear_futex_trace_fd();
		if (wake_result < 0) return false;
		if (wake_result > 0) wake_observed = true;
		const int observe_ms = bounded_remaining_ms(phase_deadline_ms, 8);
		if (wake_observed && observe_ms > 0 && trace->wait_for_pid_event(
				pid, "FUTEX_WAIT_RETURN rc=0", returns_before + 1, observe_ms)) {
			return true;
		}
		if (!process_is_alive(pid)) return false;
	}
}

bool wake_process_futex_wait(const void* address, pid_t pid, int timeout_ms) {
	const int64_t deadline = edge_test::monotonic_ms_now() + timeout_ms;
	while (edge_test::monotonic_ms_now() < deadline) {
		const int64_t remaining = deadline - edge_test::monotonic_ms_now();
		if (!wait_for_process_futex_wait(pid, static_cast<int>(remaining))) continue;
		const int rc = edge_runtime::detail::futex_wake(address, 1);
		if (rc == 1) return true;
		if (rc < 0) return false;
		sleep_ms(1);
	}
	return false;
}

class ScopedTestFd {
	public:
	explicit ScopedTestFd(int fd = -1) : fd_(fd) {}
	ScopedTestFd(const ScopedTestFd&) = delete;
	ScopedTestFd& operator=(const ScopedTestFd&) = delete;
	~ScopedTestFd() { reset(); }

	int get() const { return fd_; }

	void reset(int fd = -1) {
		if (fd_ >= 0) ::close(fd_);
		fd_ = fd;
	}

	private:
	int fd_ = -1;
};

class ScopedMpmcShm {
	public:
	explicit ScopedMpmcShm(const std::string& name) : name_(name) {}
	~ScopedMpmcShm() {
		EXPECT_TRUE(edge_test::retain_shm_object(edge_runtime::detail::mpmc_shm_name(name_)));
	}

	private:
	std::string name_;
};

bool wait_for_child_futex_wait(edge_test::FutexTracePipe* trace, pid_t pid, int timeout_ms) {
	if (!trace->wait_for_pid_event(pid, "FUTEX_WAIT_CALL", 1, timeout_ms)) return false;
	return wait_for_process_futex_wait(pid, timeout_ms);
}

bool parse_i64_field(const std::string& text, const char* key, int64_t* value) {
	const size_t offset = text.find(key);
	if (offset == std::string::npos) return false;
	char* end = nullptr;
	const char* start = text.c_str() + offset + std::strlen(key);
	const int64_t parsed = std::strtoll(start, &end, 10);
	if (end == start) return false;
	*value = parsed;
	return true;
}

bool wait_for_complete_trace(edge_test::FutexTracePipe* trace, pid_t pid, int timeout_ms) {
	uint64_t attempted = 0;
	uint64_t emitted = 0;
	uint64_t dropped = 0;
	bool complete = false;
	if (!trace->wait_for_status(pid, &attempted, &emitted, &dropped, &complete, timeout_ms)) {
		return false;
	}
	return complete && dropped == 0 && attempted == emitted + dropped;
}

bool read_deadline_event(int fd, std::string* line_out, int timeout_ms) {
	const int flags = ::fcntl(fd, F_GETFL, 0);
	if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) return false;
	std::string received;
	const int64_t deadline = edge_test::monotonic_ms_now() + timeout_ms;
	for (;;) {
		const size_t end = received.find('\n');
		if (end != std::string::npos) {
			*line_out = received.substr(0, end + 1);
			return true;
		}
		const int64_t remaining = deadline - edge_test::monotonic_ms_now();
		if (remaining <= 0) return false;
		struct pollfd descriptor {fd, POLLIN, 0};
		const int rc = ::poll(&descriptor, 1, static_cast<int>(remaining));
		if (rc < 0 && errno == EINTR) continue;
		if (rc <= 0) return false;
		char buffer[256];
		const ssize_t count = ::read(fd, buffer, sizeof(buffer));
		if (count > 0) {
			received.append(buffer, static_cast<size_t>(count));
			continue;
		}
		if (count < 0 && errno == EINTR) continue;
		return false;
	}
}

void wait_until_monotonic_ms(int64_t target_ms) {
	for (;;) {
		const int64_t now = edge_test::monotonic_ms_now();
		if (now >= target_ms) return;
		struct timespec target {};
		target.tv_sec = static_cast<time_t>(target_ms / 1000);
		target.tv_nsec = static_cast<long>((target_ms % 1000) * 1000000);
		const int rc = ::clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &target, nullptr);
		if (rc == 0 || rc != EINTR) return;
	}
}

void kill_and_reap(edge_test::SpawnedChild* child) {
	child->kill(SIGKILL);
	std::string output;
	// SpawnedChild::wait reports false when it had to enter its kill/reap path; it still performs
	// waitpid and marks the child reaped. SIGSTOP victims use this intentional path.
	(void)child->wait(5000, &output);
	EXPECT_TRUE(child->reaped()) << output;
}

bool spawn_with_failpoint(edge_test::SpawnedChild* child,
                          const std::vector<std::string>& args, const char* failpoint) {
	::setenv("EDGE_FAILPOINT", failpoint, 1);
	::setenv("EDGE_FAILPOINT_MODE", "stop", 1);
	const bool spawned = child->spawn(args);
	::unsetenv("EDGE_FAILPOINT");
	::unsetenv("EDGE_FAILPOINT_MODE");
	return spawned;
}

bool spawn_with_trace(edge_test::SpawnedChild* child, const std::vector<std::string>& args,
			      edge_test::FutexTracePipe* trace) {
	if (!edge_test::set_futex_trace_fd(trace->write_fd()) ||
		!edge_test::set_futex_trace_status_fd(trace->status_write_fd())) {
		edge_test::clear_futex_trace_fd();
		edge_test::clear_futex_trace_status_fd();
		return false;
	}
	const bool spawned = child->spawn(args);
	edge_test::clear_futex_trace_fd();
	edge_test::clear_futex_trace_status_fd();
	return spawned;
}

TEST(MpmcWait, EmptyQueueWakesCrossProcessConsumer) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_empty");
	auto queue = Queue::create(options_for(name, 4), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	edge_test::SpawnedChild consumer;
	ASSERT_TRUE(spawn_with_trace(
			&consumer, wait_child_args("consumer", name, 4, 5000), &trace));
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, consumer.pid(), 5000)) << trace.transcript();
	ASSERT_TRUE(trace.no_new("FUTEX_WAIT_RETURN", 5)) << trace.transcript();
	trace.drain();
	const size_t returns_before =
			trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_RETURN rc=0");
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	const auto pushed = queue.value().try_push(MpmcTestPayload{31, 3, 0x4D504D43u});
	edge_test::clear_futex_trace_fd();
	ASSERT_TRUE(pushed);
	ASSERT_TRUE(trace.wait_for("FUTEX_WAKE_RETURN rc=1", 5000)) << trace.transcript();
	ASSERT_TRUE(trace.wait_for_pid_event(consumer.pid(), "FUTEX_WAIT_RETURN rc=0", returns_before + 1,
	                                    5000))
			<< trace.transcript();

	std::string output;
	ASSERT_TRUE(consumer.wait(10000, &output)) << output;
	EXPECT_EQ(consumer.exit_code(), 0) << output;
	EXPECT_TRUE(contains(output, "WAIT_POP_OK producer=3 sequence=31"));
	ASSERT_TRUE(wait_for_complete_trace(&trace, consumer.pid(), 1000))
			<< trace.status_transcript();
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, FullQueueWakesCrossProcessProducer) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_full");
	auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
	ASSERT_TRUE(queue);
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{1, 1, 0x4D504D43u}));

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	edge_test::SpawnedChild producer;
	ASSERT_TRUE(spawn_with_trace(
			&producer, wait_child_args("producer", name, 1, 5000, 8, 42), &trace));
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, producer.pid(), 5000)) << trace.transcript();
	ASSERT_TRUE(trace.no_new("FUTEX_WAIT_RETURN", 5)) << trace.transcript();
	trace.drain();
	const size_t returns_before = trace.count_pid_events(producer.pid(), "FUTEX_WAIT_RETURN rc=0");
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	auto first = queue.value().try_pop();
	edge_test::clear_futex_trace_fd();
	ASSERT_TRUE(first);
	EXPECT_EQ(first.value().sequence, 1u);
	ASSERT_TRUE(trace.wait_for("FUTEX_WAKE_RETURN rc=1", 5000)) << trace.transcript();
	ASSERT_TRUE(trace.wait_for_pid_event(producer.pid(), "FUTEX_WAIT_RETURN rc=0", returns_before + 1,
	                                    5000))
			<< trace.transcript();

	std::string output;
	ASSERT_TRUE(producer.wait(10000, &output)) << output;
	EXPECT_EQ(producer.exit_code(), 0) << output;
	EXPECT_TRUE(contains(output, "WAIT_PUSH_OK producer=8 sequence=42"));
	ASSERT_TRUE(wait_for_complete_trace(&trace, producer.pid(), 1000))
			<< trace.status_transcript();
	auto second = queue.value().try_pop();
	ASSERT_TRUE(second);
	EXPECT_EQ(second.value().producer, 8u);
	EXPECT_EQ(second.value().sequence, 42u);
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, MultipleConsumersCompeteAfterOnePushWave) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_many");
	auto queue = Queue::create(options_for(name, 4), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	edge_test::SpawnedChild consumer_a;
	edge_test::SpawnedChild consumer_b;
	ASSERT_TRUE(spawn_with_trace(
			&consumer_a, wait_child_args("consumer", name, 4, 5000, 0, 101), &trace));
	ASSERT_TRUE(spawn_with_trace(
			&consumer_b, wait_child_args("consumer", name, 4, 5000, 0, 102), &trace));
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, consumer_a.pid(), 5000)) << trace.transcript();
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, consumer_b.pid(), 5000)) << trace.transcript();
	trace.drain();
	const size_t returns_a = trace.count_pid_events(consumer_a.pid(), "FUTEX_WAIT_RETURN rc=0");
	const size_t returns_b = trace.count_pid_events(consumer_b.pid(), "FUTEX_WAIT_RETURN rc=0");
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	auto first_push = queue.value().wait_push_until(
			MpmcTestPayload{101, 4, 0x4D504D43u}, deadline);
	auto second_push = queue.value().wait_push_until(
			MpmcTestPayload{102, 4, 0x4D504D43u}, deadline);
	edge_test::clear_futex_trace_fd();
	ASSERT_TRUE(first_push) << first_push.error().context;
	ASSERT_TRUE(second_push) << second_push.error().context;
	ASSERT_TRUE(trace.wait_for_pid_event(consumer_a.pid(), "FUTEX_WAIT_RETURN rc=0", returns_a + 1,
	                                    5000))
			<< trace.transcript();
	ASSERT_TRUE(trace.wait_for_pid_event(consumer_b.pid(), "FUTEX_WAIT_RETURN rc=0", returns_b + 1,
	                                    5000))
			<< trace.transcript();

	std::string output_a;
	std::string output_b;
	ASSERT_TRUE(consumer_a.wait(10000, &output_a)) << output_a;
	ASSERT_TRUE(consumer_b.wait(10000, &output_b)) << output_b;
	EXPECT_EQ(consumer_a.exit_code(), 0) << output_a;
	EXPECT_EQ(consumer_b.exit_code(), 0) << output_b;
	ASSERT_TRUE(wait_for_complete_trace(&trace, consumer_a.pid(), 1000))
			<< trace.status_transcript();
	ASSERT_TRUE(wait_for_complete_trace(&trace, consumer_b.pid(), 1000))
			<< trace.status_transcript();
	const std::string all = output_a + output_b;
	EXPECT_TRUE(contains(all, "WAIT_POP_OK producer=4 sequence=101"));
	EXPECT_TRUE(contains(all, "WAIT_POP_OK producer=4 sequence=102"));
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, MultipleProducersCompeteAfterOnePopWave) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_many_producers");
	auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
	ASSERT_TRUE(queue);
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{200, 20, 0x4D504D43u}));

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	edge_test::SpawnedChild producer_a;
	edge_test::SpawnedChild producer_b;
	ASSERT_TRUE(spawn_with_trace(
			&producer_a, wait_child_args("producer", name, 1, 5000, 21, 201), &trace));
	ASSERT_TRUE(spawn_with_trace(
			&producer_b, wait_child_args("producer", name, 1, 5000, 22, 202), &trace));
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, producer_a.pid(), 5000)) << trace.transcript();
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, producer_b.pid(), 5000)) << trace.transcript();
	trace.drain();
	const size_t returns_a = trace.count_pid_events(producer_a.pid(), "FUTEX_WAIT_RETURN rc=0");
	const size_t returns_b = trace.count_pid_events(producer_b.pid(), "FUTEX_WAIT_RETURN rc=0");

	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	auto initial = queue.value().try_pop();
	edge_test::clear_futex_trace_fd();
	ASSERT_TRUE(initial);
	EXPECT_EQ(initial.value().sequence, 200u);
	ASSERT_TRUE(trace.wait_for_pid_event(producer_a.pid(), "FUTEX_WAIT_RETURN rc=0", returns_a + 1,
	                                    5000))
			<< trace.transcript();
	ASSERT_TRUE(trace.wait_for_pid_event(producer_b.pid(), "FUTEX_WAIT_RETURN rc=0", returns_b + 1,
	                                    5000))
			<< trace.transcript();

	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	auto first = queue.value().wait_pop_until(std::chrono::steady_clock::now() +
	                                         std::chrono::seconds(5));
	edge_test::clear_futex_trace_fd();
	ASSERT_TRUE(first) << first.error().context;
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	auto second = queue.value().wait_pop_until(std::chrono::steady_clock::now() +
	                                          std::chrono::seconds(5));
	edge_test::clear_futex_trace_fd();
	ASSERT_TRUE(second) << second.error().context;

	const auto matches = [](const MpmcTestPayload& value, uint32_t producer, uint64_t sequence) {
		return value.producer == producer && value.sequence == sequence;
	};
	const bool first_is_a = matches(first.value(), 21, 201);
	const bool first_is_b = matches(first.value(), 22, 202);
	EXPECT_TRUE(first_is_a || first_is_b);
	if (first_is_a) {
		EXPECT_TRUE(matches(second.value(), 22, 202));
	} else {
		EXPECT_TRUE(matches(second.value(), 21, 201));
	}

	std::string output_a;
	std::string output_b;
	ASSERT_TRUE(producer_a.wait(10000, &output_a)) << output_a;
	ASSERT_TRUE(producer_b.wait(10000, &output_b)) << output_b;
	EXPECT_EQ(producer_a.exit_code(), 0) << output_a;
	EXPECT_EQ(producer_b.exit_code(), 0) << output_b;
	ASSERT_TRUE(wait_for_complete_trace(&trace, producer_a.pid(), 1000))
			<< trace.status_transcript();
	ASSERT_TRUE(wait_for_complete_trace(&trace, producer_b.pid(), 1000))
			<< trace.status_transcript();
	EXPECT_TRUE(contains(output_a, "WAIT_PUSH_OK producer=21 sequence=201")) << output_a;
	EXPECT_TRUE(contains(output_b, "WAIT_PUSH_OK producer=22 sequence=202")) << output_b;
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, AbsoluteDeadlineTimeoutAndAlreadyAvailableValue) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_deadline");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	const auto start = std::chrono::steady_clock::now();
	const auto deadline = start + std::chrono::milliseconds(80);
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	auto timed_out = queue.value().wait_pop_until(deadline);
	edge_test::clear_futex_trace_fd();
	const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
	                             std::chrono::steady_clock::now() - start)
	                             .count();
	ASSERT_FALSE(timed_out);
	EXPECT_EQ(timed_out.error().code, ErrorCode::kTimeout);
	EXPECT_GE(elapsed, 40);
	EXPECT_LT(elapsed, 2000);
	ASSERT_TRUE(trace.wait_for("FUTEX_WAIT_CALL", 1000)) << trace.transcript();
	ASSERT_TRUE(trace.wait_for("FUTEX_WAIT_RETURN rc=-1 errno=110", 1000))
			<< trace.transcript();

	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{77, 6, 0x4D504D43u}));
	auto available = queue.value().wait_pop_until(std::chrono::steady_clock::time_point{});
	ASSERT_TRUE(available);
	EXPECT_EQ(available.value().sequence, 77u);
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{78, 6, 0x4D504D43u}));
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{79, 6, 0x4D504D43u}));
	auto push_timeout = queue.value().wait_push_until(
	        MpmcTestPayload{80, 6, 0x4D504D43u},
	        std::chrono::steady_clock::now() + std::chrono::milliseconds(60));
	ASSERT_FALSE(push_timeout);
	EXPECT_EQ(push_timeout.error().code, ErrorCode::kTimeout);
	ASSERT_TRUE(queue.value().try_pop());
	ASSERT_TRUE(queue.value().try_pop());
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, EintrAndSpuriousWakeDoNotBreakPredicateLoop) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_signals");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);
	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	edge_test::SpawnedChild waiter;
	auto args = wait_child_args("consumer", name, 2, 2000);
	args.push_back("--interruptible");
	ASSERT_TRUE(spawn_with_trace(&waiter, args, &trace));
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, waiter.pid(), 5000)) << trace.transcript();
	trace.drain();
	const size_t interrupted_before =
			trace.count_pid_events(waiter.pid(), "FUTEX_WAIT_RETURN rc=-1 errno=4");
	const size_t calls_before = trace.count_pid_events(waiter.pid(), "FUTEX_WAIT_CALL");
	waiter.kill(SIGUSR1);
	ASSERT_TRUE(trace.wait_for_pid_event(waiter.pid(), "FUTEX_WAIT_RETURN rc=-1 errno=4",
	                                    interrupted_before + 1, 5000))
			<< trace.transcript();
	ASSERT_TRUE(trace.wait_for_pid_event(waiter.pid(), "FUTEX_WAIT_CALL", calls_before + 1, 5000))
			<< trace.transcript();
	ASSERT_TRUE(wait_for_process_futex_wait(waiter.pid(), 5000)) << trace.transcript();

	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(
			fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	trace.drain();
	const size_t spurious_returns =
			trace.count_pid_events(waiter.pid(), "FUTEX_WAIT_RETURN rc=0");
	const size_t spurious_calls = trace.count_pid_events(waiter.pid(), "FUTEX_WAIT_CALL");
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	const int spurious_wake = edge_runtime::detail::futex_wake(&header->notify_epoch, INT_MAX);
	edge_test::clear_futex_trace_fd();
	ASSERT_EQ(spurious_wake, 1);
	ASSERT_TRUE(trace.wait_for_pid_event(waiter.pid(), "FUTEX_WAIT_RETURN rc=0", spurious_returns + 1,
	                                    5000))
			<< trace.transcript();
	ASSERT_TRUE(trace.wait_for_pid_event(waiter.pid(), "FUTEX_WAIT_CALL", spurious_calls + 1, 5000))
			<< trace.transcript();
	ASSERT_TRUE(wait_for_process_futex_wait(waiter.pid(), 5000)) << trace.transcript();

	trace.drain();
	const size_t payload_returns =
			trace.count_pid_events(waiter.pid(), "FUTEX_WAIT_RETURN rc=0");
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{88, 7, 0x4D504D43u}));
	edge_test::clear_futex_trace_fd();
	ASSERT_TRUE(trace.wait_for_pid_event(waiter.pid(), "FUTEX_WAIT_RETURN rc=0", payload_returns + 1,
	                                    5000))
			<< trace.transcript();
	std::string output;
	ASSERT_TRUE(waiter.wait(10000, &output)) << output;
	EXPECT_EQ(waiter.exit_code(), 0) << output;
	EXPECT_TRUE(contains(output, "WAIT_POP_OK producer=7 sequence=88")) << output;
	ASSERT_TRUE(wait_for_complete_trace(&trace, waiter.pid(), 1000))
			<< trace.status_transcript();
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, Abi2PeerWithoutNotifyUsesBoundedFallback) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_no_notify");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	edge_test::SpawnedChild consumer;
	auto args = wait_child_args("consumer", name, 2, 500);
	args.push_back("--no-notify");
	ASSERT_TRUE(spawn_with_trace(&consumer, args, &trace));
	ASSERT_TRUE(trace.wait_for("FUTEX_WAIT_CALL", 5000)) << trace.transcript();
	ASSERT_TRUE(trace.wait_for("FUTEX_WAIT_RETURN rc=-1 errno=110", 5000))
			<< trace.transcript();

	ASSERT_EQ(::setenv("EDGE_RUNTIME_MPMC_DISABLE_NOTIFY", "1", 1), 0);
	const auto pushed = queue.value().try_push(MpmcTestPayload{91, 12, 0x4D504D43u});
	(void)::unsetenv("EDGE_RUNTIME_MPMC_DISABLE_NOTIFY");
	ASSERT_TRUE(pushed);

	std::string output;
	ASSERT_TRUE(consumer.wait(10000, &output)) << output;
	EXPECT_EQ(consumer.exit_code(), 0) << output;
	EXPECT_TRUE(contains(output, "WAIT_POP_OK producer=12 sequence=91")) << output;
	ASSERT_TRUE(wait_for_complete_trace(&trace, consumer.pid(), 1000))
			<< trace.status_transcript();
	trace.drain();
	EXPECT_EQ(trace.count("FUTEX_WAKE_RETURN"), 0u) << trace.transcript();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, SpuriousWakeWithoutPayloadStillTimesOut) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_spurious_timeout");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	edge_test::SpawnedChild consumer;
	auto args = wait_child_args("consumer", name, 2, 120);
	args.push_back("--no-notify");
	ASSERT_TRUE(spawn_with_trace(&consumer, args, &trace));
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, consumer.pid(), 5000)) << trace.transcript();

	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(
			fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	for (size_t wave = 0; wave < 3; ++wave) {
		trace.drain();
		const size_t returns_before =
				trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_RETURN rc=0");
		const size_t calls_before = trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_CALL");
		ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
		const int wake_result = edge_runtime::detail::futex_wake(&header->notify_epoch, INT_MAX);
		edge_test::clear_futex_trace_fd();
		ASSERT_EQ(wake_result, 1);
		ASSERT_TRUE(trace.wait_for_pid_event(consumer.pid(), "FUTEX_WAIT_RETURN rc=0", returns_before + 1,
		                                    5000))
				<< trace.transcript();
		ASSERT_TRUE(trace.wait_for_pid_event(consumer.pid(), "FUTEX_WAIT_CALL", calls_before + 1, 5000))
				<< trace.transcript();
		ASSERT_TRUE(wait_for_process_futex_wait(consumer.pid(), 5000)) << trace.transcript();
	}

	std::string output;
	ASSERT_TRUE(consumer.wait(10000, &output)) << output;
	EXPECT_EQ(consumer.exit_code(), 3) << output;
	EXPECT_TRUE(contains(output, "WAIT_POP_FAIL code=Timeout")) << output;
	ASSERT_TRUE(wait_for_complete_trace(&trace, consumer.pid(), 1000))
			<< trace.status_transcript();
	int64_t start_ms = 0;
	int64_t deadline_ms = 0;
	int64_t end_ms = 0;
	int64_t elapsed_ms = 0;
	ASSERT_TRUE(parse_i64_field(output, "start_ms=", &start_ms)) << output;
	ASSERT_TRUE(parse_i64_field(output, "deadline_ms=", &deadline_ms)) << output;
	ASSERT_TRUE(parse_i64_field(output, "end_ms=", &end_ms)) << output;
	ASSERT_TRUE(parse_i64_field(output, "elapsed_ms=", &elapsed_ms)) << output;
	EXPECT_EQ(deadline_ms - start_ms, 120);
	EXPECT_GE(end_ms, deadline_ms);
	EXPECT_EQ(elapsed_ms, end_ms - start_ms);
	EXPECT_LT(elapsed_ms, 440);
	ASSERT_TRUE(trace.wait_for("FUTEX_WAIT_RETURN rc=-1 errno=110", 1000))
			<< trace.transcript();
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, RepeatedInterruptsAndSpuriousWakeKeepAbsoluteDeadline) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_deadline_events");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	edge_test::SpawnedChild consumer;
	auto args = wait_child_args("consumer", name, 2, 180);
	args.push_back("--interruptible");
	args.push_back("--no-notify");
	ASSERT_TRUE(spawn_with_trace(&consumer, args, &trace));
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, consumer.pid(), 5000)) << trace.transcript();

	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(
			fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	constexpr size_t kRounds = 3;
	for (size_t round = 0; round < kRounds; ++round) {
		trace.drain();
		const size_t interrupted_before =
				trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_RETURN rc=-1 errno=4");
		const size_t calls_before = trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_CALL");
		consumer.kill(SIGUSR1);
		ASSERT_TRUE(trace.wait_for_pid_event(consumer.pid(), "FUTEX_WAIT_RETURN rc=-1 errno=4",
		                                    interrupted_before + 1, 5000))
				<< trace.transcript();
		ASSERT_TRUE(trace.wait_for_pid_event(consumer.pid(), "FUTEX_WAIT_CALL", calls_before + 1, 5000))
				<< trace.transcript();
		ASSERT_TRUE(wait_for_process_futex_wait(consumer.pid(), 5000)) << trace.transcript();

		trace.drain();
		const size_t spurious_before =
				trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_RETURN rc=0");
		const size_t spurious_calls_before = trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_CALL");
		ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
		const int wake_result = edge_runtime::detail::futex_wake(&header->notify_epoch, INT_MAX);
		edge_test::clear_futex_trace_fd();
		ASSERT_EQ(wake_result, 1);
		ASSERT_TRUE(trace.wait_for_pid_event(consumer.pid(), "FUTEX_WAIT_RETURN rc=0",
		                                    spurious_before + 1, 5000))
				<< trace.transcript();
		ASSERT_TRUE(trace.wait_for_pid_event(consumer.pid(), "FUTEX_WAIT_CALL",
		                                    spurious_calls_before + 1, 5000))
				<< trace.transcript();
		ASSERT_TRUE(wait_for_process_futex_wait(consumer.pid(), 5000)) << trace.transcript();
	}

	std::string output;
	ASSERT_TRUE(consumer.wait(10000, &output)) << output;
	EXPECT_EQ(consumer.exit_code(), 3) << output;
	EXPECT_TRUE(contains(output, "WAIT_POP_FAIL code=Timeout")) << output;
	ASSERT_TRUE(wait_for_complete_trace(&trace, consumer.pid(), 1000))
			<< trace.status_transcript();
	int64_t start_ms = 0;
	int64_t deadline_ms = 0;
	int64_t end_ms = 0;
	int64_t elapsed_ms = 0;
	ASSERT_TRUE(parse_i64_field(output, "start_ms=", &start_ms)) << output;
	ASSERT_TRUE(parse_i64_field(output, "deadline_ms=", &deadline_ms)) << output;
	ASSERT_TRUE(parse_i64_field(output, "end_ms=", &end_ms)) << output;
	ASSERT_TRUE(parse_i64_field(output, "elapsed_ms=", &elapsed_ms)) << output;
	EXPECT_EQ(deadline_ms - start_ms, 180);
	EXPECT_GE(end_ms, deadline_ms);
	EXPECT_EQ(elapsed_ms, end_ms - start_ms);
	EXPECT_LT(elapsed_ms, 600);
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, LateInterruptsAndSpuriousWakeDoNotResetPopDeadline) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_late_pop_deadline");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);
	ScopedMpmcShm cleanup(name);

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(
			fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	int deadline_pipe[2] = {-1, -1};
	ASSERT_EQ(::pipe(deadline_pipe), 0);
	ScopedTestFd deadline_read(deadline_pipe[0]);
	ScopedTestFd deadline_write(deadline_pipe[1]);
	edge_test::SpawnedChild consumer;
	auto args = wait_child_args("consumer", name, 2, 300);
	args.push_back("--interruptible");
	args.push_back("--no-notify");
	args.push_back("--deadline-event-fd");
	args.push_back(std::to_string(deadline_write.get()));
	ASSERT_TRUE(spawn_with_trace(&consumer, args, &trace));
	deadline_write.reset();

	std::string deadline_event;
	ASSERT_TRUE(read_deadline_event(deadline_read.get(), &deadline_event, 5000));
	deadline_read.reset();
	int64_t start_ms = 0;
	int64_t deadline_ms = 0;
	int64_t event_pid = 0;
	ASSERT_TRUE(parse_i64_field(deadline_event, "start_ms=", &start_ms)) << deadline_event;
	ASSERT_TRUE(parse_i64_field(deadline_event, "deadline_ms=", &deadline_ms)) << deadline_event;
	ASSERT_TRUE(parse_i64_field(deadline_event, "pid=", &event_pid)) << deadline_event;
	ASSERT_EQ(event_pid, static_cast<int64_t>(consumer.pid()));
	ASSERT_EQ(deadline_ms - start_ms, 300);
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, consumer.pid(), 5000)) << trace.transcript();

	wait_until_monotonic_ms(start_ms + 80);
	trace.drain();
	const size_t first_call_before = trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_CALL");
	ASSERT_TRUE(interrupt_child_until_return(&trace, consumer.pid(), start_ms + 150))
			<< trace.transcript();
	ASSERT_TRUE(wait_for_child_futex_wait_until(
				&trace, consumer.pid(), first_call_before + 1, start_ms + 175))
			<< trace.transcript();

	wait_until_monotonic_ms(start_ms + 180);
	trace.drain();
	const size_t second_call_before = trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_CALL");
	ASSERT_TRUE(wake_child_until_return(
				&trace, &header->notify_epoch, consumer.pid(), start_ms + 230))
			<< trace.transcript();
	ASSERT_TRUE(wait_for_child_futex_wait_until(
				&trace, consumer.pid(), second_call_before + 1, start_ms + 255))
			<< trace.transcript();

	wait_until_monotonic_ms(start_ms + 260);
	trace.drain();
	ASSERT_TRUE(interrupt_child_until_return(&trace, consumer.pid(), deadline_ms - 5))
			<< trace.transcript();

	std::string output;
	ASSERT_TRUE(consumer.wait(10000, &output)) << output;
	EXPECT_EQ(consumer.exit_code(), 3) << output;
	EXPECT_TRUE(contains(output, "WAIT_POP_FAIL code=Timeout")) << output;
	int64_t output_start = 0;
	int64_t output_deadline = 0;
	int64_t output_end = 0;
	ASSERT_TRUE(parse_i64_field(output, "start_ms=", &output_start)) << output;
	ASSERT_TRUE(parse_i64_field(output, "deadline_ms=", &output_deadline)) << output;
	ASSERT_TRUE(parse_i64_field(output, "end_ms=", &output_end)) << output;
	EXPECT_EQ(output_start, start_ms);
	EXPECT_EQ(output_deadline, deadline_ms);
	EXPECT_GE(output_end, output_deadline);
	EXPECT_LT(output_end - output_deadline, 150);
	ASSERT_TRUE(wait_for_complete_trace(&trace, consumer.pid(), 1000))
			<< trace.status_transcript();
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, LateInterruptsAndSpuriousWakeDoNotResetPushDeadline) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_late_push_deadline");
	auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
	ASSERT_TRUE(queue);
	ScopedMpmcShm cleanup(name);
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{1, 1, 0x4D504D43u}));

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(
			fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	int deadline_pipe[2] = {-1, -1};
	ASSERT_EQ(::pipe(deadline_pipe), 0);
	ScopedTestFd deadline_read(deadline_pipe[0]);
	ScopedTestFd deadline_write(deadline_pipe[1]);
	edge_test::SpawnedChild producer;
	auto args = wait_child_args("producer", name, 1, 300);
	args.push_back("--interruptible");
	args.push_back("--no-notify");
	args.push_back("--deadline-event-fd");
	args.push_back(std::to_string(deadline_write.get()));
	ASSERT_TRUE(spawn_with_trace(&producer, args, &trace));
	deadline_write.reset();

	std::string deadline_event;
	ASSERT_TRUE(read_deadline_event(deadline_read.get(), &deadline_event, 5000));
	deadline_read.reset();
	int64_t start_ms = 0;
	int64_t deadline_ms = 0;
	int64_t event_pid = 0;
	ASSERT_TRUE(parse_i64_field(deadline_event, "start_ms=", &start_ms)) << deadline_event;
	ASSERT_TRUE(parse_i64_field(deadline_event, "deadline_ms=", &deadline_ms)) << deadline_event;
	ASSERT_TRUE(parse_i64_field(deadline_event, "pid=", &event_pid)) << deadline_event;
	ASSERT_EQ(event_pid, static_cast<int64_t>(producer.pid()));
	ASSERT_EQ(deadline_ms - start_ms, 300);
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, producer.pid(), 5000)) << trace.transcript();

	wait_until_monotonic_ms(start_ms + 80);
	trace.drain();
	const size_t first_call_before = trace.count_pid_events(producer.pid(), "FUTEX_WAIT_CALL");
	ASSERT_TRUE(interrupt_child_until_return(&trace, producer.pid(), start_ms + 150))
			<< trace.transcript();
	ASSERT_TRUE(wait_for_child_futex_wait_until(
				&trace, producer.pid(), first_call_before + 1, start_ms + 175))
			<< trace.transcript();

	wait_until_monotonic_ms(start_ms + 180);
	trace.drain();
	const size_t second_call_before = trace.count_pid_events(producer.pid(), "FUTEX_WAIT_CALL");
	ASSERT_TRUE(wake_child_until_return(
				&trace, &header->notify_epoch, producer.pid(), start_ms + 230))
			<< trace.transcript();
	ASSERT_TRUE(wait_for_child_futex_wait_until(
				&trace, producer.pid(), second_call_before + 1, start_ms + 255))
			<< trace.transcript();

	wait_until_monotonic_ms(start_ms + 260);
	trace.drain();
	ASSERT_TRUE(interrupt_child_until_return(&trace, producer.pid(), deadline_ms - 5))
			<< trace.transcript();

	std::string output;
	ASSERT_TRUE(producer.wait(10000, &output)) << output;
	EXPECT_EQ(producer.exit_code(), 3) << output;
	EXPECT_TRUE(contains(output, "WAIT_PUSH_FAIL code=Timeout")) << output;
	int64_t output_start = 0;
	int64_t output_deadline = 0;
	int64_t output_end = 0;
	ASSERT_TRUE(parse_i64_field(output, "start_ms=", &output_start)) << output;
	ASSERT_TRUE(parse_i64_field(output, "deadline_ms=", &output_deadline)) << output;
	ASSERT_TRUE(parse_i64_field(output, "end_ms=", &output_end)) << output;
	EXPECT_EQ(output_start, start_ms);
	EXPECT_EQ(output_deadline, deadline_ms);
	EXPECT_GE(output_end, output_deadline);
	EXPECT_LT(output_end - output_deadline, 150);
	ASSERT_TRUE(wait_for_complete_trace(&trace, producer.pid(), 1000))
			<< trace.status_transcript();
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, TraceOverflowIsReportedByIndependentCompletionStatus) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_trace_overflow");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(
			fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	ASSERT_TRUE(trace.set_trace_capacity(4096));
	edge_test::SpawnedChild consumer;
	ASSERT_TRUE(spawn_with_trace(
			&consumer, wait_child_args("consumer", name, 2, 800), &trace));
	ASSERT_TRUE(wait_for_process_futex_wait(consumer.pid(), 5000));

	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	ASSERT_TRUE(edge_test::set_futex_trace_status_fd(trace.status_write_fd()));
	for (size_t wave = 0; wave < 64; ++wave) {
		ASSERT_TRUE(wake_process_futex_wait(&header->notify_epoch, consumer.pid(), 250));
	}
	const bool parent_trace_complete = edge_runtime::detail::futex_trace_flush();
	edge_test::clear_futex_trace_fd();
	edge_test::clear_futex_trace_status_fd();
	ASSERT_TRUE(parent_trace_complete);

	std::string output;
	ASSERT_TRUE(consumer.wait(10000, &output)) << output;
	EXPECT_EQ(consumer.exit_code(), 3) << output;
	EXPECT_TRUE(contains(output, "WAIT_POP_FAIL code=Timeout")) << output;

	uint64_t parent_attempted = 0;
	uint64_t parent_emitted = 0;
	uint64_t parent_dropped = 0;
	bool parent_complete = false;
	ASSERT_TRUE(trace.wait_for_status(getpid(), &parent_attempted, &parent_emitted,
	                                  &parent_dropped, &parent_complete, 1000))
			<< trace.status_transcript();
	EXPECT_TRUE(parent_complete);
	EXPECT_GT(parent_dropped, 0u);
	EXPECT_EQ(parent_attempted, parent_emitted + parent_dropped);

	uint64_t child_attempted = 0;
	uint64_t child_emitted = 0;
	uint64_t child_dropped = 0;
	bool child_complete = false;
	ASSERT_TRUE(trace.wait_for_status(consumer.pid(), &child_attempted, &child_emitted,
	                                  &child_dropped, &child_complete, 1000))
			<< trace.status_transcript();
	EXPECT_TRUE(child_complete);
	EXPECT_GT(child_dropped, 0u);
	EXPECT_EQ(child_attempted, child_emitted + child_dropped);

	trace.drain();
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	ASSERT_TRUE(edge_test::set_futex_trace_status_fd(trace.status_write_fd()));
	ASSERT_TRUE(edge_runtime::detail::futex_trace_flush());
	edge_test::clear_futex_trace_fd();
	edge_test::clear_futex_trace_status_fd();
	trace.drain();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, TraceStatusFailureIsFailClosed) {
	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	edge_test::clear_futex_trace_status_fd();
#if EDGERUNTIME_ENABLE_FAILPOINTS
	EXPECT_FALSE(edge_runtime::detail::futex_trace_flush());
#else
	EXPECT_TRUE(edge_runtime::detail::futex_trace_flush());
#endif
	ASSERT_EQ(::setenv("EDGE_RUNTIME_FUTEX_TRACE_STATUS_FD", "-1", 1), 0);
#if EDGERUNTIME_ENABLE_FAILPOINTS
	EXPECT_FALSE(edge_runtime::detail::futex_trace_flush());
#else
	EXPECT_TRUE(edge_runtime::detail::futex_trace_flush());
#endif
	edge_test::clear_futex_trace_fd();
	edge_test::clear_futex_trace_status_fd();
	trace.drain();
}

TEST(MpmcWait, NotifyEpochWrapStillWakesCrossProcessConsumer) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_notify_wrap");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(
			fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	edge_runtime::detail::shared_store_seq_cst(&header->notify_epoch, UINT32_MAX);
	mapping.value().reset();
	fd.value().reset();

	edge_test::FutexTracePipe trace;
	ASSERT_TRUE(trace.open());
	edge_test::SpawnedChild consumer;
	auto args = wait_child_args("consumer", name, 2, 5000);
	args.push_back("--no-notify");
	ASSERT_TRUE(spawn_with_trace(&consumer, args, &trace));
	ASSERT_TRUE(wait_for_child_futex_wait(&trace, consumer.pid(), 5000)) << trace.transcript();
	trace.drain();
	const size_t returns_before =
			trace.count_pid_events(consumer.pid(), "FUTEX_WAIT_RETURN rc=0");
	ASSERT_TRUE(edge_test::set_futex_trace_fd(trace.write_fd()));
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{92, 13, 0x4D504D43u}));
	edge_test::clear_futex_trace_fd();
	ASSERT_TRUE(trace.wait_for("FUTEX_WAKE_RETURN rc=1", 5000)) << trace.transcript();
	ASSERT_TRUE(trace.wait_for_pid_event(consumer.pid(), "FUTEX_WAIT_RETURN rc=0", returns_before + 1,
	                                    5000))
			<< trace.transcript();

	std::string output;
	ASSERT_TRUE(consumer.wait(10000, &output)) << output;
	EXPECT_EQ(consumer.exit_code(), 0) << output;
	EXPECT_TRUE(contains(output, "WAIT_POP_OK producer=13 sequence=92")) << output;
	ASSERT_TRUE(wait_for_complete_trace(&trace, consumer.pid(), 1000))
			<< trace.status_transcript();
	fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	mapping = edge_runtime::detail::mmap_region(
			fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	EXPECT_EQ(edge_runtime::detail::shared_load_seq_cst(&header->notify_epoch), 0u);
	trace.drain();
	EXPECT_FALSE(trace.has_dropped_events()) << trace.transcript();
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, WaitPushPropagatesDeadOwnerRecoveryBlocked) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_dead_push");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);
	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(&victim,
	                                  {g_wait_helper, "--role", "producer", "--name", name,
	                                   "--capacity", "2", "--timeout-ms", "5000"},
	                                  "MPMC_ENQUEUE_GATE_OWNED"));
	// The wait helper has a different role binary, so use the existing producer helper to stop
	// after it owns the gate and then kill it before the waiting producer opens the queue.
	if (!wait_stopped(victim.pid(), 5000)) {
		kill_and_reap(&victim);
		FAIL() << "producer victim did not stop";
	}
	kill_and_reap(&victim);

	const auto result = edge_test::run_child_capture(
	        wait_child_args("producer", name, 2, 2000, 9, 91), 5000);
	ASSERT_FALSE(result.timed_out) << result.stdout_text;
	EXPECT_EQ(result.exit_code, 3) << result.stdout_text;
	EXPECT_TRUE(contains(result.stdout_text, "WAIT_PUSH_FAIL code=RecoveryBlocked"));
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, WaitPushPropagatesDeadConsumerRecoveryBlockedWhenFull) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_dead_consumer_full");
	auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
	ASSERT_TRUE(queue);
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{1, 1, 0x4D504D43u}));

	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(&victim,
	                                  wait_child_args("consumer", name, 1, 5000),
	                                  "MPMC_DEQUEUE_SLOT_OWNED"));
	ASSERT_TRUE(wait_stopped(victim.pid(), 5000));
	kill_and_reap(&victim);

	auto result = queue.value().wait_push_until(
	        MpmcTestPayload{2, 2, 0x4D504D43u},
	        std::chrono::steady_clock::now() + std::chrono::milliseconds(80));
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, ErrorCode::kRecoveryBlocked);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, WaitPushRejectsInvalidQueuePositions) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_inverted_positions");
	auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
	ASSERT_TRUE(queue);

	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(
	        fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	edge_runtime::detail::shared_store_release(&header->dequeue_position, uint64_t{1});
	edge_runtime::detail::shared_store_release(&header->enqueue_position, uint64_t{0});
	mapping.value().reset();
	fd.value().reset();

	const auto value = MpmcTestPayload{3, 3, 0x4D504D43u};
	auto immediate = queue.value().try_push(value);
	ASSERT_FALSE(immediate);
	EXPECT_EQ(immediate.error().code, ErrorCode::kRecoveryBlocked);

	auto waited = queue.value().wait_push_until(
	        value, std::chrono::steady_clock::now() + std::chrono::milliseconds(40));
	ASSERT_FALSE(waited);
	EXPECT_EQ(waited.error().code, ErrorCode::kRecoveryBlocked);

	// A distance larger than capacity is also corruption, not a full queue. Keep the mapping
	// local and mutate it only after the first no-concurrency assertion has completed.
	fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	mapping = edge_runtime::detail::mmap_region(
	        fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	edge_runtime::detail::shared_store_release(&header->dequeue_position, uint64_t{0});
	edge_runtime::detail::shared_store_release(&header->enqueue_position, uint64_t{2});
	mapping.value().reset();
	fd.value().reset();

	auto oversized = queue.value().try_push(value);
	ASSERT_FALSE(oversized);
	EXPECT_EQ(oversized.error().code, ErrorCode::kRecoveryBlocked);
	auto oversized_wait = queue.value().wait_push_until(
	        value, std::chrono::steady_clock::now() + std::chrono::milliseconds(40));
	ASSERT_FALSE(oversized_wait);
	EXPECT_EQ(oversized_wait.error().code, ErrorCode::kRecoveryBlocked);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, WaitPopRejectsOversizedQueueDistance) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_pop_oversized");
	auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
	ASSERT_TRUE(queue);
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{99, 9, 0x4D504D43u}));

	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(
	        fd.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	// Keep the slot/payload valid, but make the position distance exceed capacity.
	edge_runtime::detail::shared_store_release(&header->enqueue_position, uint64_t{2});
	mapping.value().reset();
	fd.value().reset();

	auto immediate = queue.value().try_pop();
	ASSERT_FALSE(immediate);
	EXPECT_EQ(immediate.error().code, ErrorCode::kRecoveryBlocked);

	auto waited = queue.value().wait_pop_until(
	        std::chrono::steady_clock::now() + std::chrono::milliseconds(40));
	ASSERT_FALSE(waited);
	EXPECT_EQ(waited.error().code, ErrorCode::kRecoveryBlocked);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcWait, WaitPopPropagatesCorruptSlotRecoveryBlocked) {
	const std::string name = edge_test::unique_channel_name("mpmc_wait_dead_pop");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{1, 1, 0x4D504D43u}));

	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(&victim,
	                                  {g_wait_helper, "--role", "consumer", "--name", name,
	                                   "--capacity", "2", "--timeout-ms", "5000"},
	                                  "MPMC_DEQUEUE_SLOT_OWNED"));
	// The failpoint is evaluated by the MPMC try_pop implementation in the exec'd helper.
	ASSERT_TRUE(wait_stopped(victim.pid(), 5000));
	kill_and_reap(&victim);
	const auto result = edge_test::run_child_capture(
	        wait_child_args("consumer", name, 2, 1000), 5000);
	ASSERT_FALSE(result.timed_out) << result.stdout_text;
	EXPECT_EQ(result.exit_code, 3) << result.stdout_text;
	EXPECT_TRUE(contains(result.stdout_text, "WAIT_POP_FAIL code=RecoveryBlocked"));
	ASSERT_TRUE(queue.value().remove_if_creator());
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc != 2) {
		std::fprintf(stderr, "usage: mpmc_wait_test <mpmc_wait_child_binary>\n");
		return 2;
	}
	g_wait_helper = argv[1];
	return RUN_ALL_TESTS();
}
