#include "edge_runtime/buffer/pool_queue.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>

#include "edge_runtime/buffer/shared_buffer_pool_layout.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/utility/failpoint.hpp"
#include "shared_buffer_pool_payload.hpp"
#include "test_util.hpp"

#if EDGERUNTIME_ENABLE_FAILPOINTS
namespace {
uint64_t* fault_generation = nullptr;
const char* fault_point = nullptr;
}
extern "C" void __real__ZN12edge_runtime6detail17failpoint_triggerEPKNS0_15FailpointRecordE(
        const edge_runtime::detail::FailpointRecord*) noexcept;
extern "C" void __wrap__ZN12edge_runtime6detail17failpoint_triggerEPKNS0_15FailpointRecordE(
        const edge_runtime::detail::FailpointRecord* point) noexcept {
	if (fault_point && std::strcmp(point->id, fault_point) == 0 && fault_generation) {
		__atomic_fetch_add(fault_generation, uint64_t{1}, __ATOMIC_SEQ_CST);
		fault_point = nullptr;
	}
	__real__ZN12edge_runtime6detail17failpoint_triggerEPKNS0_15FailpointRecordE(point);
}
#endif

namespace {
using namespace std::chrono_literals;
using edge_runtime::ErrorCode;
using edge_runtime::PoolQueueSendState;
using Clock = std::chrono::steady_clock;

class PoolQueueTest : public ::testing::Test {
       protected:
	void SetUp() override {
		auto name = edge_test::unique_channel_name("pool_queue");
		auto created_pool = edge_runtime::SharedBufferPool::create(
		        {name, 4096, 1, SharedBufferPoolTestSchema()});
		ASSERT_TRUE(created_pool);
		pool.emplace(std::move(created_pool.value()));
		pool_name = name;
		auto created_queue = edge_runtime::MpmcQueue<edge_runtime::BufferHandle>::create(
		        {name + "_q", 1}, SharedBufferHandleQueueSchema());
		ASSERT_TRUE(created_queue);
		queue.emplace(std::move(created_queue.value()));
	}
	void TearDown() override {
		if (pool) {
			EXPECT_TRUE(pool->remove_if_creator());
		}
		if (queue) {
			EXPECT_TRUE(queue->remove_if_creator());
		}
	}
	std::optional<edge_runtime::SharedBufferPool> pool;
	std::string pool_name;
	std::optional<edge_runtime::MpmcQueue<edge_runtime::BufferHandle>> queue;
};

TEST_F(PoolQueueTest, SuccessfulSendTransfersOnlyToConsumer) {
	auto write = pool->try_acquire_write();
	ASSERT_TRUE(write);
	write.value().data()[0] = std::byte{0x55};
	auto sent = edge_runtime::send_buffer_until(write.value(), *queue, Clock::now() + 1s);
	ASSERT_TRUE(sent);
	EXPECT_EQ(sent.state, PoolQueueSendState::kEnqueued);
	EXPECT_FALSE(write.value().active());
	EXPECT_FALSE(pool->try_acquire_write());
	auto popped = queue->try_pop();
	ASSERT_TRUE(popped);
	auto read = pool->acquire_read(popped.value());
	ASSERT_TRUE(read);
	EXPECT_EQ(read.value().data()[0], std::byte{0x55});
	EXPECT_TRUE(read.value().release_checked());
}

TEST_F(PoolQueueTest, FullDeadlineReturnsPublishedBlockAndOriginalError) {
	ASSERT_TRUE(queue->try_push(edge_runtime::BufferHandle{}));
	auto write = pool->try_acquire_write();
	ASSERT_TRUE(write);
	auto sent = edge_runtime::send_buffer_until(write.value(), *queue, Clock::now());
	EXPECT_EQ(sent.state, PoolQueueSendState::kReturned);
	ASSERT_TRUE(sent.enqueue_error);
	EXPECT_EQ(sent.enqueue_error->code, ErrorCode::kTimeout);
	EXPECT_FALSE(sent.return_error);
	EXPECT_EQ(pool->status().value().free_blocks, 1u);
	EXPECT_TRUE(queue->try_pop());
}

TEST_F(PoolQueueTest, CancelAndClosedReturnOnlyUnsentBlocks) {
	ASSERT_TRUE(queue->try_push(edge_runtime::BufferHandle{}));
	edge_runtime::CancellationSource cancellation;
	ASSERT_TRUE(cancellation.request_cancel());
	auto write = pool->try_acquire_write();
	ASSERT_TRUE(write);
	auto cancelled = edge_runtime::send_buffer_until(write.value(), *queue, Clock::now() + 1s,
	                                                 cancellation.token());
	EXPECT_EQ(cancelled.state, PoolQueueSendState::kReturned);
	ASSERT_TRUE(cancelled.enqueue_error);
	EXPECT_EQ(cancelled.enqueue_error->code, ErrorCode::kCancelled);
	ASSERT_TRUE(queue->try_pop());
	ASSERT_TRUE(queue->close());
	auto next = pool->try_acquire_write();
	ASSERT_TRUE(next);
	auto closed = edge_runtime::send_buffer_until(next.value(), *queue, Clock::now() + 1s);
	EXPECT_EQ(closed.state, PoolQueueSendState::kReturned);
	ASSERT_TRUE(closed.enqueue_error);
	EXPECT_EQ(closed.enqueue_error->code, ErrorCode::kClosed);
	EXPECT_EQ(pool->status().value().free_blocks, 1u);
}

TEST_F(PoolQueueTest, InactiveWritePreservesPublishErrorAndQueue) {
	auto write = pool->try_acquire_write();
	ASSERT_TRUE(write);
	write.value().abort();
	auto sent = edge_runtime::send_buffer_until(write.value(), *queue, Clock::now());
	EXPECT_EQ(sent.state, PoolQueueSendState::kPublishFailed);
	EXPECT_TRUE(sent.publish_error);
	EXPECT_FALSE(sent.descriptor);
	EXPECT_EQ(queue->try_pop().error().code, ErrorCode::kQueueEmpty);
}

#if EDGERUNTIME_ENABLE_FAILPOINTS
TEST_F(PoolQueueTest, CleanupFailureRetainsDescriptorErrorsAndReadOwnership) {
	using namespace edge_runtime::detail;
	auto descriptor = shm_open_existing("/edgeruntime.bufferpool." +
	                                    std::to_string(::getuid()) + "." + pool_name);
	ASSERT_TRUE(descriptor);
	uint64_t length = 0;
	ASSERT_TRUE(shared_buffer_pool_mapping_size_for(4096, 1, &length));
	auto mapped = mmap_region(descriptor.value(), length);
	ASSERT_TRUE(mapped);
	auto* block = reinterpret_cast<SharedBufferBlockHeaderAbi*>(
	        static_cast<std::byte*>(mapped.value().get()) + sizeof(SharedBufferPoolHeaderAbi));
	ASSERT_TRUE(queue->try_push(edge_runtime::BufferHandle{}));
	for (const char* point : {"POOL_QUEUE_ENQUEUE_FAILED", "POOL_QUEUE_BEFORE_RETURN"}) {
		auto write = pool->try_acquire_write();
		ASSERT_TRUE(write);
		const auto original = write.value().handle();
		fault_generation = &block->block_generation;
		fault_point = point;
		auto sent = edge_runtime::send_buffer_until(write.value(), *queue, Clock::now());
		fault_point = nullptr;
		fault_generation = nullptr;
		EXPECT_EQ(sent.state, PoolQueueSendState::kCleanupRequired);
		EXPECT_TRUE(sent.descriptor);
		EXPECT_TRUE(sent.enqueue_error);
		EXPECT_TRUE(sent.return_error);
		shared_store_seq_cst(&block->block_generation, original.block_generation);
		if (sent.pending_read) {
			EXPECT_TRUE(sent.pending_read->active());
			EXPECT_TRUE(sent.pending_read->release_checked());
		} else {
			auto reclaimed = pool->acquire_read(original);
			ASSERT_TRUE(reclaimed);
			EXPECT_TRUE(reclaimed.value().release_checked());
		}
	}
	EXPECT_TRUE(queue->try_pop());
}
#endif
}
