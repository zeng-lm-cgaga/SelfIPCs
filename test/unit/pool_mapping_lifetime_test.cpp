#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/sync/futex.hpp"
#include "shared_buffer_pool_payload.hpp"
#include "test_util.hpp"

namespace {
using namespace std::chrono_literals;
using edge_runtime::ErrorCode;
std::mutex gate_mutex;
std::condition_variable gate_condition;
bool armed = false;
bool arrived = false;
bool released = false;

edge_runtime::SharedBufferPoolOptions options() {
	return {edge_test::unique_channel_name("pool_mapping"), 4096, 1,
	        SharedBufferPoolTestSchema()};
}
}

extern "C" int __real__ZN12edge_runtime6detail10futex_waitEPKvjPK8timespec(
        const void*, uint32_t, const timespec*) noexcept;

extern "C" int __wrap__ZN12edge_runtime6detail10futex_waitEPKvjPK8timespec(
        const void* address, uint32_t expected, const timespec* timeout) noexcept {
	{
		std::unique_lock<std::mutex> lock(gate_mutex);
		if (armed) {
			armed = false;
			arrived = true;
			gate_condition.notify_all();
			(void)gate_condition.wait_for(lock, 2s, [] { return released; });
			errno = EINTR;
			return -1;
		}
	}
	return __real__ZN12edge_runtime6detail10futex_waitEPKvjPK8timespec(address, expected,
	                                                                   timeout);
}

TEST(PoolMappingLifetime, WaitAndCancellationRegistrationRejectDeletionAfterLastReturn) {
	auto created = edge_runtime::SharedBufferPool::create(options());
	ASSERT_TRUE(created);
	auto& pool = created.value();
	auto write = pool.try_acquire_write();
	ASSERT_TRUE(write);
	edge_runtime::CancellationSource cancellation;
	{
		std::lock_guard<std::mutex> lock(gate_mutex);
		armed = true;
		arrived = released = false;
	}
	std::optional<edge_runtime::Result<edge_runtime::WriteBuffer>> waited;
	std::thread waiter([&] {
		waited.emplace(pool.wait_acquire_write_until(std::chrono::steady_clock::now() + 3s,
		                                             cancellation.token()));
	});
	bool reached = false;
	{
		std::unique_lock<std::mutex> lock(gate_mutex);
		reached = gate_condition.wait_for(lock, 1s, [] { return arrived; });
	}
	write.value().abort();
	auto removed = pool.remove_if_creator();
	EXPECT_TRUE(reached);
	EXPECT_FALSE(removed);
	if (!removed) {
		EXPECT_EQ(removed.error().code, ErrorCode::kBufferPoolContention);
		EXPECT_TRUE(cancellation.request_cancel());
	}
	{
		std::lock_guard<std::mutex> lock(gate_mutex);
		released = true;
	}
	gate_condition.notify_all();
	waiter.join();
	ASSERT_TRUE(waited.has_value());
	if (!removed) {
		ASSERT_TRUE(*waited);
		waited->value().abort();
		EXPECT_TRUE(pool.remove_if_creator());
	}
}

TEST(PoolMappingLifetime, BorrowAndFailedDeletionLeaveMappingUsable) {
	auto created = edge_runtime::SharedBufferPool::create(options());
	ASSERT_TRUE(created);
	auto& pool = created.value();
	auto write = pool.try_acquire_write();
	ASSERT_TRUE(write);
	EXPECT_FALSE(pool.remove_if_creator());
	write.value().data()[0] = std::byte{0x44};
	auto descriptor = write.value().publish();
	ASSERT_TRUE(descriptor);
	auto read = pool.acquire_read(descriptor.value());
	ASSERT_TRUE(read);
	EXPECT_FALSE(pool.remove_if_creator());
	EXPECT_EQ(read.value().data()[0], std::byte{0x44});
	EXPECT_TRUE(read.value().release_checked());
	EXPECT_TRUE(pool.remove_if_creator());
	EXPECT_EQ(pool.block_size(), 0u);
	EXPECT_EQ(pool.block_count(), 0u);
	EXPECT_EQ(pool.generation(), 0u);
	EXPECT_FALSE(pool.try_acquire_write());
	EXPECT_FALSE(pool.status());
}
