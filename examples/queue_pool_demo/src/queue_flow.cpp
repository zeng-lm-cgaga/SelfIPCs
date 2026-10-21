#include "queue_pool_demo/queue_flow.hpp"

#include <unistd.h>

#include <array>
#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <thread>
#include <utility>

#include "queue_pool_demo/api.hpp"
#include "queue_pool_demo/process.hpp"

namespace queue_pool_demo {

namespace {

struct Message {
	uint64_t sequence{0};
};

using Queue = ipc::MpmcQueue<Message>;

ipc::SchemaDescriptor schema() {
	ipc::SchemaDescriptor result;
	result.fingerprint[0] = std::byte{0x71};
	result.fingerprint[1] = std::byte{0x32};
	result.fingerprint[31] = std::byte{0x21};
	result.version = 1;
	result.debug_name = "queue_pool_demo.Message.v1";
	return result;
}

bool fail(const char* operation, const ipc::Error& error) {
	std::fprintf(stderr, "%s: %s errno=%d operation=%s context=%s\n", operation,
	             ipc::to_string(error.code), error.errno_value,
	             error.operation != nullptr ? error.operation : "?", error.context);
	return false;
}

bool check(bool condition, const char* expectation) {
	if (!condition) std::fprintf(stderr, "queue expectation failed: %s\n", expectation);
	return condition;
}

bool check_batch(const ipc::BatchResult& result, size_t count, ipc::BatchStopReason reason,
                 const char* operation) {
	if (result.count == count && result.reason == reason) return true;
	std::fprintf(stderr, "%s: count=%zu reason=%u expected_count=%zu expected_reason=%u\n",
	             operation, result.count, static_cast<unsigned>(result.reason), count,
	             static_cast<unsigned>(reason));
	if (result.reason != ipc::BatchStopReason::kComplete) (void)fail(operation, result.error);
	return false;
}

void print_diagnostics(const char* stage, const Queue& queue) {
	const auto snapshot = queue.diagnostics();
	std::printf("diagnostics[%s]: enabled=%d capacity=%u Full=%" PRIu64 " wait_calls=%" PRIu64
	            " waits=%" PRIu64 " committed_push=%" PRIu64 " committed_pop=%" PRIu64
	            " timeouts=%" PRIu64 " cancelled=%" PRIu64 "\n",
	            stage, snapshot.enabled ? 1 : 0, snapshot.capacity, snapshot.full,
	            snapshot.wait_calls, snapshot.waits, snapshot.push_successes,
	            snapshot.pop_successes, snapshot.timeouts, snapshot.cancelled);
}

bool remove_queue(Queue& queue, const std::string& name) {
	const auto deadline = Clock::now() + std::chrono::milliseconds(400);
	for (;;) {
		const auto removed = queue.remove_if_creator();
		if (removed) {
			std::printf("cleanup: remove_if_creator succeeded name=%s\n", name.c_str());
			return true;
		}
		if (removed.error().code != ipc::ErrorCode::kQueueContention ||
		    Clock::now() >= deadline) {
			(void)fail("cleanup remove_if_creator", removed.error());
			std::fprintf(stderr, "cleanup: IPC retained for inspection name=%s\n",
			             name.c_str());
			return false;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
	}
}

bool close_queue(Queue& queue) {
	const auto deadline = Clock::now() + std::chrono::milliseconds(400);
	for (;;) {
		const auto closed = queue.close();
		if (closed) return true;
		const auto code = closed.error().code;
		if ((code != ipc::ErrorCode::kQueueContention &&
		     code != ipc::ErrorCode::kConcurrentHandleUse) ||
		    Clock::now() >= deadline) {
			return fail("creator close", closed.error());
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
	}
}

bool parent_steps(Queue& queue, HelperProcess& child, const std::string& name) {
	if (!child.spawn(name) || !child.expect('R')) return false;
	const std::array<Message, 3> messages{{{1}, {2}, {3}}};
	static_assert(messages.size() <= ipc::kMpmcMaxBatchSize);
	const auto partial = queue.try_push_batch(messages.data(), messages.size());
	if (!check_batch(partial, 2, ipc::BatchStopReason::kFull, "parent push batch"))
		return false;
	std::printf("queue: capacity=2 push_batch(3) -> count=2 Full; element 3 not committed\n");
	print_diagnostics("partial-batch", queue);
	const auto full =
	        queue.wait_push_until(messages[2], Clock::now() + std::chrono::milliseconds(120));
	if (full) return check(false, "full queue wait must not commit before child consumption");
	if (full.error().code != ipc::ErrorCode::kTimeout) return fail("full wait", full.error());
	print_diagnostics("full-deadline", queue);
	if (!child.send('C')) return false;
	const auto pushed =
	        queue.wait_push_until(messages[2], Clock::now() + std::chrono::seconds(3));
	if (!pushed) return fail("wait push after child consume", pushed.error());
	if (!child.expect('P')) return false;
	std::printf("queue: exec helper consumed sequence=1; wait_push(sequence=3) committed\n");
	print_diagnostics("capacity-released", queue);
	ipc::CancellationSource source;
	const auto token = source.token();
	std::thread canceller([&source] {
		std::this_thread::sleep_for(std::chrono::milliseconds(60));
		(void)source.request_cancel();
	});
	const auto cancelled =
	        queue.wait_push_until(Message{4}, Clock::now() + std::chrono::seconds(2), token);
	canceller.join();
	if (cancelled) return check(false, "cancelled full wait must not commit sequence=4");
	if (cancelled.error().code != ipc::ErrorCode::kCancelled)
		return fail("cancel wait", cancelled.error());
	if (!check(token.is_cancelled() && !source.request_cancel(), "source is sticky one-shot"))
		return false;
	const auto sticky =
	        queue.wait_push_until(Message{4}, Clock::now() + std::chrono::seconds(1), token);
	if (sticky) return check(false, "sticky token must cancel another retryable full wait");
	if (sticky.error().code != ipc::ErrorCode::kCancelled)
		return fail("sticky wait", sticky.error());
	std::printf(
	        "cancellation: local source thread joined; sticky token, no cross-process "
	        "propagation\n");
	if (!close_queue(queue)) return false;
	const auto rejected =
	        queue.wait_push_until(Message{4}, Clock::now() + std::chrono::seconds(1), token);
	if (rejected) return check(false, "closed queue must reject push");
	if (rejected.error().code != ipc::ErrorCode::kClosed)
		return fail("closed wins over cancellation", rejected.error());
	std::printf("queue: creator close succeeded; Closed wins over cancelled token\n");
	if (!child.send('D') || !child.expect('D') ||
	    !child.wait_for_success(Clock::now() + std::chrono::seconds(3)))
		return false;
	const auto drained = queue.wait_pop_until(Clock::now() + std::chrono::seconds(1));
	if (drained) return check(false, "closed queue must remain drained");
	if (drained.error().code != ipc::ErrorCode::kClosed)
		return fail("parent drained wait", drained.error());
	print_diagnostics("parent-final", queue);
	const auto snapshot = queue.diagnostics();
	return check(
	        snapshot.enabled && snapshot.capacity == 2 && snapshot.push_successes == 3 &&
	                snapshot.pop_successes == 0 && snapshot.full > 0 &&
	                snapshot.wait_calls >= 4 && snapshot.timeouts == 1 &&
	                snapshot.cancelled == 2,
	        "parent-local diagnostics must match committed elements, not requested batch size");
}

}  // namespace

int run_queue_helper(const std::string& name, int descriptor, pid_t parent_pid) {
	if (!check(::getppid() == parent_pid && ::getpid() != parent_pid &&
	                   ::getpgrp() == ::getpid(),
	           "exec helper must be a distinct process in its own process group"))
		return 3;
	ipc::MpmcQueueOptions options;
	options.name = name;
	options.capacity = 2;
	auto opened = Queue::open(options, schema());
	if (!opened) {
		(void)fail("helper independent open", opened.error());
		return 3;
	}
	Queue queue = std::move(opened.value());
	queue.set_diagnostics_enabled(true);
	queue.reset_diagnostics();
	const auto forbidden = queue.close();
	if (!check(!forbidden, "noncreator close must fail")) return 3;
	if (forbidden.error().code != ipc::ErrorCode::kPermissionDenied) {
		(void)fail("noncreator close", forbidden.error());
		return 3;
	}
	std::printf(
	        "helper: independent open pid=%ld parent=%ld; noncreator close=PermissionDenied\n",
	        static_cast<long>(::getpid()), static_cast<long>(parent_pid));
	if (!send_token(descriptor, 'R', Clock::now() + std::chrono::seconds(3)) ||
	    !expect_token(descriptor, 'C', Clock::now() + std::chrono::seconds(3)))
		return 3;
	std::this_thread::sleep_for(std::chrono::milliseconds(60));
	Message first;
	const auto consumed = queue.try_pop_batch(&first, 1);
	if (!check_batch(consumed, 1, ipc::BatchStopReason::kComplete, "helper first batch") ||
	    !check(first.sequence == 1, "FIFO first sequence=1"))
		return 3;
	if (!send_token(descriptor, 'P', Clock::now() + std::chrono::seconds(3)) ||
	    !expect_token(descriptor, 'D', Clock::now() + std::chrono::seconds(3)))
		return 3;
	std::array<Message, 3> remaining{};
	const auto drained = queue.try_pop_batch(remaining.data(), remaining.size());
	if (!check_batch(drained, 2, ipc::BatchStopReason::kClosed, "helper drain batch") ||
	    !check(remaining[0].sequence == 2 && remaining[1].sequence == 3,
	           "FIFO drain sequence=2,3"))
		return 3;
	const auto closed = queue.wait_pop_until(Clock::now() + std::chrono::seconds(1));
	if (!check(!closed, "helper closed empty wait must not return a value")) return 3;
	if (closed.error().code != ipc::ErrorCode::kClosed) {
		(void)fail("helper drained wait", closed.error());
		return 3;
	}
	print_diagnostics("exec-helper-final", queue);
	const auto snapshot = queue.diagnostics();
	if (!check(snapshot.pop_successes == 3 && snapshot.push_successes == 0,
	           "helper diagnostics are independent of creator handle"))
		return 3;
	std::printf("helper: pop_batch(3) -> count=2 Closed; drained wait_pop=Closed\n");
	return send_token(descriptor, 'D', Clock::now() + std::chrono::seconds(3)) ? 0 : 3;
}

bool run_queue_flow() {
	const std::string name = unique_name();
	ipc::MpmcQueueOptions options;
	options.name = name;
	options.capacity = 2;
	auto created = Queue::create(options, schema());
	if (!created) return fail("creator create", created.error());
	Queue queue = std::move(created.value());
	HelperProcess child;
	std::printf("creator: pid=%ld name=%s\n", static_cast<long>(::getpid()), name.c_str());
	queue.set_diagnostics_enabled(true);
	queue.reset_diagnostics();
	bool succeeded = false;
	try {
		succeeded = parent_steps(queue, child, name);
	} catch (const std::exception& error) {
		std::fprintf(stderr, "queue flow exception: %s\n", error.what());
	}
	const bool stopped = child.stop();
	if (!stopped) {
		std::fprintf(stderr,
		             "cleanup: helper not confirmed exited; retaining IPC name=%s\n",
		             name.c_str());
		return false;
	}
	const bool removed = remove_queue(queue, name);
	if (!succeeded || !removed) {
		std::fprintf(stderr,
		             "queue flow FAILED (cleanup does not mask the original failure)\n");
		return false;
	}
	std::printf(
	        "queue flow OK; counters describe behavior only, not performance or shared "
	        "occupancy\n");
	return true;
}

}  // namespace queue_pool_demo
