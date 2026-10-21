#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>
#include <utility>

#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/buffer/shared_buffer_pool_layout.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "shared_buffer_pool_payload.hpp"
#include "test_util.hpp"

namespace {

using edge_runtime::CancellationSource;
using edge_runtime::ErrorCode;
using edge_runtime::SharedBufferPool;
using edge_runtime::SharedBufferPoolOptions;
using edge_runtime::detail::MappedRegion;
using edge_runtime::detail::shared_load_acquire;
using edge_runtime::detail::shared_store_seq_cst;
using edge_runtime::detail::SharedBufferBlockHeaderAbi;
using edge_runtime::detail::SharedBufferPoolHeaderAbi;
using namespace std::chrono_literals;

class PoolWaitFeatureTest : public ::testing::Test {
       protected:
	void SetUp() override {
		options_.name = edge_test::unique_channel_name("pool_wait_feature");
		options_.block_size = 4096;
		options_.block_count = 1;
		options_.schema = SharedBufferPoolTestSchema();
	}

	void TearDown() override { (void)::shm_unlink(shm_name().c_str()); }

	std::string shm_name() const {
		return "/edgeruntime.bufferpool." + std::to_string(::getuid()) + "." +
		       options_.name;
	}

	MappedRegion map_pool() {
		auto fd = edge_runtime::detail::shm_open_existing(shm_name());
		if (!fd) return {};
		uint64_t size = 0;
		if (!edge_runtime::detail::shared_buffer_pool_mapping_size_for(
		            options_.block_size, options_.block_count, &size))
			return {};
		auto mapping = edge_runtime::detail::mmap_region(fd.value(), size);
		if (!mapping) return {};
		return std::move(mapping.value());
	}

	bool observe_wait(const SharedBufferPool& pool) {
		const auto deadline = std::chrono::steady_clock::now() + 500ms;
		while (std::chrono::steady_clock::now() < deadline) {
			if (pool.diagnostics().waits != 0) return true;
			std::this_thread::sleep_for(1ms);
		}
		return false;
	}

	SharedBufferPoolOptions options_;
};

TEST_F(PoolWaitFeatureTest, DiagnosticsAreLocalDisabledByDefaultAndMoveWithHandle) {
	auto created = SharedBufferPool::create(options_);
	ASSERT_TRUE(created);
	auto& pool = created.value();
	{
		auto write = pool.try_acquire_write();
		ASSERT_TRUE(write);
	}
	EXPECT_FALSE(pool.diagnostics().enabled);
	EXPECT_EQ(pool.diagnostics().capacity, 1u);
	EXPECT_EQ(pool.diagnostics().acquire_attempts, 0u);
	EXPECT_EQ(pool.diagnostics().notifications, 0u);
	pool.set_diagnostics_enabled(true);
	auto write = pool.try_acquire_write();
	ASSERT_TRUE(write);
	auto busy = pool.try_acquire_write();
	ASSERT_FALSE(busy);
	EXPECT_EQ(busy.error().code, ErrorCode::kBufferPoolContention);
	auto descriptor = write.value().publish();
	ASSERT_TRUE(descriptor);
	auto full = pool.try_acquire_write();
	ASSERT_FALSE(full);
	EXPECT_EQ(full.error().code, ErrorCode::kBufferPoolFull);
	auto read = pool.acquire_read(descriptor.value());
	ASSERT_TRUE(read);
	read.value().release();
	auto opened = SharedBufferPool::open(options_);
	ASSERT_TRUE(opened);
	EXPECT_EQ(opened.value().diagnostics().acquire_attempts, 0u);
	SharedBufferPool moved(std::move(pool));
	EXPECT_EQ(moved.diagnostics().acquire_attempts, 3u);
	EXPECT_EQ(moved.diagnostics().acquire_successes, 1u);
	EXPECT_EQ(moved.diagnostics().contention, 1u);
	EXPECT_EQ(moved.diagnostics().full, 1u);
	EXPECT_EQ(moved.diagnostics().notifications, 1u);
	EXPECT_EQ(pool.diagnostics().capacity, 0u);
	moved.reset_diagnostics();
	EXPECT_TRUE(moved.diagnostics().enabled);
	EXPECT_EQ(moved.diagnostics().acquire_attempts, 0u);
	EXPECT_EQ(moved.diagnostics().notifications, 0u);
	moved.set_diagnostics_enabled(false);
	{
		auto uncounted = moved.try_acquire_write();
		ASSERT_TRUE(uncounted);
	}
	EXPECT_EQ(moved.diagnostics().acquire_attempts, 0u);
	EXPECT_EQ(moved.diagnostics().notifications, 0u);
	ASSERT_TRUE(moved.remove_if_creator());
	EXPECT_EQ(moved.diagnostics().capacity, 0u);
}

TEST_F(PoolWaitFeatureTest, SuccessWinsAndCancellationPrecedesTimeout) {
	auto created = SharedBufferPool::create(options_);
	ASSERT_TRUE(created);
	auto& pool = created.value();
	pool.set_diagnostics_enabled(true);
	CancellationSource source;
	ASSERT_TRUE(source.request_cancel());
	const auto expired = std::chrono::steady_clock::now() - 1ms;
	auto success = pool.wait_acquire_write_until(expired, source.token());
	ASSERT_TRUE(success);
	auto cancelled = pool.wait_acquire_write_until(expired, source.token());
	ASSERT_FALSE(cancelled);
	EXPECT_EQ(cancelled.error().code, ErrorCode::kCancelled);
	auto published = success.value().publish();
	ASSERT_TRUE(published);
	auto timeout = pool.wait_acquire_write_until(std::chrono::steady_clock::now() + 15ms);
	ASSERT_FALSE(timeout);
	EXPECT_EQ(timeout.error().code, ErrorCode::kTimeout);
	const auto snapshot = pool.diagnostics();
	EXPECT_EQ(snapshot.wait_calls, 3u);
	EXPECT_EQ(snapshot.acquire_successes, 1u);
	EXPECT_EQ(snapshot.cancelled, 1u);
	EXPECT_EQ(snapshot.timeouts, 1u);
	EXPECT_GE(snapshot.waits, 1u);
	EXPECT_EQ(snapshot.acquire_attempts, snapshot.full + snapshot.contention + 1u);
}

TEST_F(PoolWaitFeatureTest, AbortWakesAnIndependentlyOpenedHandle) {
	auto created = SharedBufferPool::create(options_);
	ASSERT_TRUE(created);
	auto write = created.value().try_acquire_write();
	ASSERT_TRUE(write);
	auto opened = SharedBufferPool::open(options_);
	ASSERT_TRUE(opened);
	created.value().set_diagnostics_enabled(true);
	opened.value().set_diagnostics_enabled(true);
	bool acquired = false;
	std::thread waiter([&] {
		auto result = opened.value().wait_acquire_write_until(
		        std::chrono::steady_clock::now() + 1s);
		acquired = static_cast<bool>(result);
	});
	const bool sleeping = observe_wait(opened.value());
	write.value().abort();
	waiter.join();
	EXPECT_TRUE(sleeping);
	EXPECT_TRUE(acquired);
	EXPECT_EQ(created.value().diagnostics().notifications, 1u);
	EXPECT_EQ(opened.value().diagnostics().notifications, 1u);
	EXPECT_EQ(opened.value().diagnostics().wait_calls, 1u);
	EXPECT_EQ(opened.value().diagnostics().acquire_successes, 1u);
	EXPECT_GE(opened.value().diagnostics().acquire_attempts, 2u);
	EXPECT_TRUE(SharedBufferPool::open(options_));
}

TEST_F(PoolWaitFeatureTest, ReadReleaseWakesWaiterAndPreservesHandleGeneration) {
	auto created = SharedBufferPool::create(options_);
	ASSERT_TRUE(created);
	auto& pool = created.value();
	auto write = pool.try_acquire_write();
	ASSERT_TRUE(write);
	auto descriptor = write.value().publish();
	ASSERT_TRUE(descriptor);
	auto read = pool.acquire_read(descriptor.value());
	ASSERT_TRUE(read);
	pool.set_diagnostics_enabled(true);
	uint64_t next_generation = 0;
	std::thread waiter([&] {
		auto result = pool.wait_acquire_write_until(std::chrono::steady_clock::now() + 1s);
		if (result) next_generation = result.value().handle().block_generation;
	});
	const bool sleeping = observe_wait(pool);
	read.value().release();
	waiter.join();
	EXPECT_TRUE(sleeping);
	EXPECT_EQ(next_generation, descriptor.value().block_generation + 1);
	EXPECT_EQ(pool.diagnostics().notifications, 2u);
	EXPECT_TRUE(SharedBufferPool::open(options_));
}

TEST_F(PoolWaitFeatureTest, RegisteredCancellationWakesWithoutCountingAFreeNotification) {
	auto created = SharedBufferPool::create(options_);
	ASSERT_TRUE(created);
	auto& pool = created.value();
	auto write = pool.try_acquire_write();
	ASSERT_TRUE(write);
	ASSERT_TRUE(write.value().publish());
	pool.set_diagnostics_enabled(true);
	CancellationSource source;
	ErrorCode reason = ErrorCode::kSystemError;
	std::thread waiter([&] {
		auto result = pool.wait_acquire_write_until(std::chrono::steady_clock::now() + 1s,
		                                            source.token());
		if (!result) reason = result.error().code;
	});
	const bool sleeping = observe_wait(pool);
	EXPECT_TRUE(source.request_cancel());
	waiter.join();
	EXPECT_TRUE(sleeping);
	EXPECT_EQ(reason, ErrorCode::kCancelled);
	EXPECT_EQ(pool.diagnostics().cancelled, 1u);
	EXPECT_EQ(pool.diagnostics().timeouts, 0u);
	EXPECT_EQ(pool.diagnostics().notifications, 0u);
	EXPECT_TRUE(SharedBufferPool::open(options_));
}

TEST_F(PoolWaitFeatureTest, RollbackNotifiesAndPermanentErrorsWinWithAbiTwoRejected) {
	auto created = SharedBufferPool::create(options_);
	ASSERT_TRUE(created);
	auto& pool = created.value();
	pool.set_diagnostics_enabled(true);
	auto mapping = map_pool();
	ASSERT_NE(mapping.get(), nullptr);
	auto* header = static_cast<SharedBufferPoolHeaderAbi*>(mapping.get());
	EXPECT_EQ(header->abi_major, 3u);
	EXPECT_EQ(header->abi_minor, 0u);
	header->abi_major = 2;
	auto legacy = SharedBufferPool::open(options_);
	ASSERT_FALSE(legacy);
	EXPECT_EQ(legacy.error().code, ErrorCode::kAbiMismatch);
	header->abi_major = 3;
	auto* block = reinterpret_cast<SharedBufferBlockHeaderAbi*>(
	        static_cast<std::byte*>(mapping.get()) + sizeof(SharedBufferPoolHeaderAbi));
	shared_store_seq_cst(&block->block_generation, UINT64_MAX);
	const auto epoch = shared_load_acquire(&header->notify_epoch);
	CancellationSource source;
	ASSERT_TRUE(source.request_cancel());
	auto exhausted = pool.wait_acquire_write_until(std::chrono::steady_clock::now() - 1ms,
	                                               source.token());
	ASSERT_FALSE(exhausted);
	EXPECT_EQ(exhausted.error().code, ErrorCode::kSequenceExhausted);
	EXPECT_EQ(shared_load_acquire(&header->notify_epoch), epoch + 1);
	EXPECT_EQ(pool.diagnostics().notifications, 1u);
	EXPECT_EQ(pool.diagnostics().cancelled, 0u);
	EXPECT_EQ(pool.diagnostics().timeouts, 0u);
	EXPECT_EQ(pool.diagnostics().waits, 0u);
	EXPECT_TRUE(SharedBufferPool::open(options_));
	shared_store_seq_cst(&block->block_generation, uint64_t{0});
	shared_store_seq_cst(&block->owner_epoch, uint64_t{1});
	auto fault = pool.wait_acquire_write_until(std::chrono::steady_clock::now() - 1ms,
	                                           source.token());
	ASSERT_FALSE(fault);
	EXPECT_EQ(fault.error().code, ErrorCode::kRecoveryBlocked);
	EXPECT_EQ(pool.diagnostics().cancelled, 0u);
	EXPECT_EQ(pool.diagnostics().timeouts, 0u);
	EXPECT_EQ(pool.diagnostics().notifications, 1u);
}

TEST_F(PoolWaitFeatureTest, InheritedHandlesCannotClaimPublishFreeOrRemoveParentBlocks) {
	options_.block_count = 2;
	auto created = SharedBufferPool::create(options_);
	ASSERT_TRUE(created);
	auto& pool = created.value();
	auto first = pool.try_acquire_write();
	ASSERT_TRUE(first);
	auto descriptor = first.value().publish();
	ASSERT_TRUE(descriptor);
	auto read = pool.acquire_read(descriptor.value());
	ASSERT_TRUE(read);
	auto write = pool.try_acquire_write();
	ASSERT_TRUE(write);
	const pid_t child = ::fork();
	ASSERT_GE(child, 0);
	if (child == 0) {
		auto claim = pool.try_acquire_write();
		auto wait = pool.wait_acquire_write_until(std::chrono::steady_clock::now() - 1ms);
		auto status = pool.status();
		auto remove = pool.remove_if_creator();
		auto publish = write.value().publish();
		const bool rejected =
		        !claim && claim.error().code == ErrorCode::kRecoveryBlocked && !wait &&
		        wait.error().code == ErrorCode::kRecoveryBlocked && !status &&
		        status.error().code == ErrorCode::kRecoveryBlocked && !remove &&
		        remove.error().code == ErrorCode::kRecoveryBlocked && !publish &&
		        publish.error().code == ErrorCode::kRecoveryBlocked;
		write.value().abort();
		read.value().release();
		auto reopened = SharedBufferPool::open(options_);
		::_exit(rejected && reopened && reopened.value().status() ? 0 : 1);
	}
	int status = 0;
	ASSERT_EQ(::waitpid(child, &status, 0), child);
	ASSERT_TRUE(WIFEXITED(status));
	EXPECT_EQ(WEXITSTATUS(status), 0);
	auto parent = pool.status();
	ASSERT_TRUE(parent);
	EXPECT_EQ(parent.value().writing_blocks, 1u);
	EXPECT_EQ(parent.value().reading_blocks, 1u);
	EXPECT_TRUE(write.value().publish());
}

}  // namespace
