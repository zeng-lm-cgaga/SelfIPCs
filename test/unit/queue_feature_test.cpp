#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

#include "edge_runtime/queue/mpmc_layout.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/shm_object.hpp"

namespace {

using namespace std::chrono_literals;
using edge_runtime::BatchStopReason;
using edge_runtime::CancellationSource;
using edge_runtime::ErrorCode;
using Queue = edge_runtime::MpmcQueue<uint64_t>;

edge_runtime::MpmcQueueOptions options() {
	static uint32_t next = 0;
	return {"queue_feature_" + std::to_string(::getpid()) + "_" + std::to_string(++next), 2};
}

edge_runtime::SchemaDescriptor schema() {
	edge_runtime::SchemaDescriptor descriptor{};
	descriptor.version = 1;
	return descriptor;
}

TEST(QueueFeatures, CloseDrainsSuccessfulBatchPrefix) {
	auto created = Queue::create(options(), schema());
	ASSERT_TRUE(created);
	auto queue = std::move(created.value());
	queue.set_diagnostics_enabled(true);
	const std::array<uint64_t, 3> input{11, 22, 33};
	auto pushed = queue.try_push_batch(input.data(), input.size());
	EXPECT_EQ(pushed.count, 2u);
	EXPECT_EQ(pushed.reason, BatchStopReason::kFull);
	EXPECT_TRUE(queue.close());
	EXPECT_TRUE(queue.close());
	auto refused = queue.try_push(44);
	EXPECT_FALSE(refused);
	EXPECT_EQ(refused.error().code, ErrorCode::kClosed);
	std::array<uint64_t, 3> output{};
	auto popped = queue.try_pop_batch(output.data(), output.size());
	EXPECT_EQ(popped.count, 2u);
	EXPECT_EQ(popped.reason, BatchStopReason::kClosed);
	EXPECT_EQ(output[0], 11u);
	EXPECT_EQ(output[1], 22u);
	auto status = queue.status();
	ASSERT_TRUE(status);
	EXPECT_TRUE(status.value().closed);
	EXPECT_EQ(status.value().abi_major, 3u);
	const auto stats = queue.diagnostics();
	EXPECT_EQ(stats.capacity, 2u);
	EXPECT_EQ(stats.full, 1u);
	EXPECT_EQ(stats.push_successes, 2u);
	EXPECT_EQ(stats.pop_successes, 2u);
	EXPECT_EQ(stats.batch_calls, 2u);
	queue.reset_diagnostics();
	EXPECT_EQ(queue.diagnostics().push_attempts, 0u);
	EXPECT_TRUE(queue.remove_if_creator());
}

TEST(QueueFeatures, SingleWaitCancellationAndCommittedSuccessPriority) {
	auto created = Queue::create(options(), schema());
	ASSERT_TRUE(created);
	auto queue = std::move(created.value());
	queue.set_diagnostics_enabled(true);
	CancellationSource cancelled;
	EXPECT_TRUE(cancelled.request_cancel());
	EXPECT_FALSE(cancelled.request_cancel());
	const auto expired = std::chrono::steady_clock::now() - 1ms;
	EXPECT_TRUE(queue.wait_push_until(41, expired, cancelled.token()));
	auto data = queue.wait_pop_until(expired, cancelled.token());
	ASSERT_TRUE(data);
	EXPECT_EQ(data.value(), 41u);
	auto stopped = queue.wait_pop_until(expired, cancelled.token());
	EXPECT_FALSE(stopped);
	EXPECT_EQ(stopped.error().code, ErrorCode::kCancelled);
	auto timed = queue.wait_pop_until(expired);
	EXPECT_FALSE(timed);
	EXPECT_EQ(timed.error().code, ErrorCode::kTimeout);
	CancellationSource asynchronous;
	std::thread cancelling([asynchronous] {
		std::this_thread::sleep_for(5ms);
		asynchronous.request_cancel();
	});
	auto waiting =
	        queue.wait_pop_until(std::chrono::steady_clock::now() + 1s, asynchronous.token());
	cancelling.join();
	EXPECT_FALSE(waiting);
	EXPECT_EQ(waiting.error().code, ErrorCode::kCancelled);
	EXPECT_GT(queue.diagnostics().waits, 0u);
	EXPECT_TRUE(queue.close());
	auto closed = queue.wait_pop_until(expired, cancelled.token());
	EXPECT_EQ(closed.error().code, ErrorCode::kClosed);
	EXPECT_TRUE(queue.remove_if_creator());
}

TEST(QueueFeatures, BatchBoundsAndDefaultDisabledDiagnostics) {
	auto created = Queue::create(options(), schema());
	ASSERT_TRUE(created);
	auto queue = std::move(created.value());
	EXPECT_TRUE(queue.try_push_batch(nullptr, 0));
	auto invalid = queue.try_push_batch(nullptr, edge_runtime::kMpmcMaxBatchSize + 1);
	EXPECT_EQ(invalid.count, 0u);
	EXPECT_EQ(invalid.reason, BatchStopReason::kError);
	EXPECT_EQ(invalid.error.code, ErrorCode::kInvalidOptions);
	CancellationSource cancelled;
	cancelled.request_cancel();
	uint64_t value = 8;
	auto stopped = queue.try_push_batch(&value, 1, cancelled.token());
	EXPECT_EQ(stopped.reason, BatchStopReason::kCancelled);
	EXPECT_EQ(stopped.count, 0u);
	EXPECT_TRUE(queue.try_push(value));
	EXPECT_TRUE(queue.try_pop());
	EXPECT_EQ(queue.diagnostics().push_attempts, 0u);
	EXPECT_FALSE(queue.diagnostics().enabled);
	EXPECT_TRUE(queue.remove_if_creator());
}

TEST(QueueFeatures, DrainedCloseWinsOverTicketExhaustion) {
	const auto config = options();
	auto created = Queue::create(config, schema());
	ASSERT_TRUE(created);
	auto queue = std::move(created.value());
	ASSERT_TRUE(queue.close());
	auto descriptor = edge_runtime::detail::shm_open_existing(
	        edge_runtime::detail::mpmc_shm_name(config.name));
	ASSERT_TRUE(descriptor);
	auto mapping = edge_runtime::detail::mmap_region(
	        descriptor.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header =
	        static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	edge_runtime::detail::shared_store_seq_cst(&header->enqueue_position, UINT64_MAX);
	edge_runtime::detail::shared_store_seq_cst(&header->dequeue_position, UINT64_MAX);
	auto drained = queue.try_pop();
	EXPECT_FALSE(drained);
	EXPECT_EQ(drained.error().code, ErrorCode::kClosed);
	EXPECT_TRUE(queue.remove_if_creator());
}

TEST(QueueFeatures, NewLibraryRejectsAbiTwoBeforeMutableFields) {
	const auto config = options();
	auto created = Queue::create(config, schema());
	ASSERT_TRUE(created);
	auto queue = std::move(created.value());
	auto descriptor = edge_runtime::detail::shm_open_existing(
	        edge_runtime::detail::mpmc_shm_name(config.name));
	ASSERT_TRUE(descriptor);
	auto mapping = edge_runtime::detail::mmap_region(
	        descriptor.value(), sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header =
	        static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	header->abi_major = 2;
	auto refused = Queue::open(config, schema());
	EXPECT_FALSE(refused);
	EXPECT_EQ(refused.error().code, ErrorCode::kAbiMismatch);
	header->abi_major = 3;
	EXPECT_TRUE(queue.remove_if_creator());
}

}  // namespace
