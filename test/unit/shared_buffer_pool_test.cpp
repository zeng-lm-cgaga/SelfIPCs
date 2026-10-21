#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "edge_runtime/buffer/shared_buffer_pool_layout.hpp"
#include "edge_runtime/queue/mpmc_layout.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "shared_buffer_pool_payload.hpp"
#include "test_util.hpp"

namespace {

using edge_runtime::BufferHandle;
using edge_runtime::ErrorCode;
using edge_runtime::SharedBufferPool;
using edge_runtime::SharedBufferPoolOptions;
using edge_runtime::detail::SharedBufferBlockHeaderAbi;
using edge_runtime::detail::SharedBufferPoolHeaderAbi;
using edge_runtime::detail::kSharedBufferPoolBlockSize16K;
using edge_runtime::detail::kSharedBufferPoolBlockSize4K;
using edge_runtime::detail::kSharedBufferPoolAbiMajor;
using edge_runtime::detail::kSharedBufferPoolMagic;
using edge_runtime::detail::shared_buffer_pool_block_offset_for;
using edge_runtime::detail::shared_buffer_pool_mapping_size_for;
using edge_runtime::detail::shared_buffer_pool_stride_for;

SharedBufferPoolOptions options_for(const std::string& name, uint32_t block_size = 4096,
                                    uint32_t block_count = 1) {
	SharedBufferPoolOptions options;
	options.name = name;
	options.block_size = block_size;
	options.block_count = block_count;
	options.schema = SharedBufferPoolTestSchema();
	return options;
}

TEST(SharedBufferPoolLayout, FrozenAbiAndCheckedOffsets) {
	EXPECT_EQ(sizeof(BufferHandle), 88u);
	EXPECT_EQ(offsetof(BufferHandle, pool_generation), 0u);
	EXPECT_EQ(offsetof(BufferHandle, block_generation), 8u);
	EXPECT_EQ(offsetof(BufferHandle, offset), 32u);
	EXPECT_EQ(offsetof(BufferHandle, block_id), 40u);
	EXPECT_EQ(offsetof(BufferHandle, length), 44u);
	EXPECT_EQ(offsetof(BufferHandle, schema_fingerprint), 56u);

	EXPECT_EQ(sizeof(SharedBufferPoolHeaderAbi), 192u);
	EXPECT_EQ(alignof(SharedBufferPoolHeaderAbi), 64u);
	EXPECT_EQ(kSharedBufferPoolAbiMajor, 3u);
	EXPECT_EQ(edge_runtime::detail::kSharedBufferPoolAbiMinor, 0u);
	EXPECT_EQ(kSharedBufferPoolMagic[7], '2');
	EXPECT_EQ(offsetof(SharedBufferPoolHeaderAbi, schema_fingerprint), 64u);
	EXPECT_EQ(offsetof(SharedBufferPoolHeaderAbi, creator), 112u);
	EXPECT_EQ(offsetof(SharedBufferPoolHeaderAbi, notify_epoch), 144u);
	EXPECT_EQ(offsetof(SharedBufferPoolHeaderAbi, reserved), 148u);
	EXPECT_EQ(sizeof(SharedBufferBlockHeaderAbi), 64u);
	EXPECT_EQ(alignof(SharedBufferBlockHeaderAbi), 64u);
	EXPECT_EQ(offsetof(SharedBufferBlockHeaderAbi, block_generation), 8u);
	EXPECT_EQ(offsetof(SharedBufferBlockHeaderAbi, owner), 16u);
	EXPECT_EQ(offsetof(SharedBufferBlockHeaderAbi, owner_epoch), 48u);

	uint64_t stride = 0;
	ASSERT_TRUE(shared_buffer_pool_stride_for(kSharedBufferPoolBlockSize4K, &stride));
	EXPECT_EQ(stride, 4160u);
	ASSERT_TRUE(shared_buffer_pool_stride_for(kSharedBufferPoolBlockSize16K, &stride));
	EXPECT_EQ(stride, 16448u);

	uint64_t mapping_size = 0;
	ASSERT_TRUE(shared_buffer_pool_mapping_size_for(kSharedBufferPoolBlockSize4K, 2,
	                                                &mapping_size));
	EXPECT_EQ(mapping_size, 192u + 2u * 4160u);
	EXPECT_FALSE(shared_buffer_pool_stride_for(1024, &stride));
	EXPECT_FALSE(shared_buffer_pool_mapping_size_for(4096, 0, &mapping_size));
	EXPECT_FALSE(shared_buffer_pool_mapping_size_for(1024, 1, &mapping_size));

	uint64_t block_offset = 0;
	uint64_t payload_offset = 0;
	ASSERT_TRUE(shared_buffer_pool_block_offset_for(4096, 1, 2, &block_offset,
	                                               &payload_offset));
	EXPECT_EQ(block_offset, 192u + 4160u);
	EXPECT_EQ(payload_offset, 192u + 4160u + 64u);
	EXPECT_FALSE(shared_buffer_pool_block_offset_for(4096, 2, 2, &block_offset,
	                                                &payload_offset));
}

TEST(SharedBufferPoolLayout, QueueCloseAbiThreeIsFrozen) {
	using edge_runtime::detail::MpmcQueueHeaderAbi;
	EXPECT_EQ(edge_runtime::detail::kMpmcAbiMajor, 3u);
	EXPECT_EQ(edge_runtime::detail::kMpmcAbiMinor, 0u);
	EXPECT_STREQ(edge_runtime::detail::kMpmcHeaderMagic, "EDGMPMC2");
	EXPECT_EQ(sizeof(MpmcQueueHeaderAbi), 320u);
	EXPECT_EQ(alignof(MpmcQueueHeaderAbi), 64u);
	EXPECT_EQ(offsetof(MpmcQueueHeaderAbi, closed), 164u);
}

TEST(SharedBufferPool, FixedBlockLifecycleAndReuse) {
	const std::string name = edge_test::unique_channel_name("buffer_pool_lifecycle");
	auto created = SharedBufferPool::create(options_for(name));
	ASSERT_TRUE(created) << edge_runtime::to_string(created.error().code) << " "
	                     << created.error().context;
	auto& pool = created.value();

	const auto status = pool.status();
	ASSERT_TRUE(status);
	EXPECT_TRUE(status.value().ready);
	EXPECT_EQ(status.value().free_blocks, 1u);
	EXPECT_EQ(status.value().block_size, 4096u);

	auto write = pool.try_acquire_write();
	ASSERT_TRUE(write) << edge_runtime::to_string(write.error().code) << " "
	                   << write.error().context;
	ASSERT_EQ(write.value().size(), 4096u);
	std::memset(write.value().data(), 0xA5, write.value().size());
	auto first = write.value().publish();
	ASSERT_TRUE(first);
	EXPECT_EQ(first.value().block_generation, 1u);

	const auto published = pool.status();
	ASSERT_TRUE(published);
	EXPECT_EQ(published.value().published_blocks, 1u);

	{
		auto read = pool.acquire_read(first.value());
		ASSERT_TRUE(read) << edge_runtime::to_string(read.error().code) << " "
		                  << read.error().context;
		EXPECT_EQ(read.value().size(), 4096u);
		EXPECT_EQ(std::to_integer<unsigned char>(read.value().data()[0]), 0xA5);
		EXPECT_EQ(std::to_integer<unsigned char>(read.value().data()[4095]), 0xA5);
		read.value().release();
	}

	auto second_write = pool.try_acquire_write();
	ASSERT_TRUE(second_write);
	std::memset(second_write.value().data(), 0x5A, second_write.value().size());
	auto second = second_write.value().publish();
	ASSERT_TRUE(second);
	EXPECT_EQ(second.value().block_id, first.value().block_id);
	EXPECT_EQ(second.value().block_generation, first.value().block_generation + 1);

	const auto stale = pool.acquire_read(first.value());
	ASSERT_FALSE(stale);
	EXPECT_EQ(stale.error().code, ErrorCode::kBufferInvalidHandle);

	BufferHandle invalid = second.value();
	invalid.offset++;
	EXPECT_EQ(pool.acquire_read(invalid).error().code, ErrorCode::kBufferInvalidHandle);
	invalid = second.value();
	invalid.block_id = 1;
	EXPECT_EQ(pool.acquire_read(invalid).error().code, ErrorCode::kBufferInvalidHandle);
	invalid = second.value();
	invalid.length--;
	EXPECT_EQ(pool.acquire_read(invalid).error().code, ErrorCode::kBufferInvalidHandle);
	invalid = second.value();
	invalid.pool_generation++;
	EXPECT_EQ(pool.acquire_read(invalid).error().code, ErrorCode::kBufferInvalidHandle);
	invalid = second.value();
	invalid.instance_nonce_hi ^= 1u;
	EXPECT_EQ(pool.acquire_read(invalid).error().code, ErrorCode::kBufferInvalidHandle);
	invalid = second.value();
	invalid.schema_version++;
	EXPECT_EQ(pool.acquire_read(invalid).error().code, ErrorCode::kSchemaMismatch);
	invalid = second.value();
	invalid.reserved = 1;
	EXPECT_EQ(pool.acquire_read(invalid).error().code, ErrorCode::kBufferInvalidHandle);

	{
		auto read = pool.acquire_read(second.value());
		ASSERT_TRUE(read);
		auto blocked = pool.try_acquire_write();
		ASSERT_FALSE(blocked);
		EXPECT_EQ(blocked.error().code, ErrorCode::kBufferPoolContention);
	}

	ASSERT_TRUE(pool.remove_if_creator());
}

TEST(SharedBufferPool, InvalidOptionsAreRejected) {
	auto bad_size = options_for(edge_test::unique_channel_name("buffer_pool_bad_size"), 1024);
	auto result = SharedBufferPool::create(bad_size);
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, ErrorCode::kInvalidOptions);

	auto bad_schema = options_for(edge_test::unique_channel_name("buffer_pool_bad_schema"));
	bad_schema.schema.version = 0;
	result = SharedBufferPool::create(bad_schema);
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, ErrorCode::kInvalidOptions);
}

TEST(SharedBufferPool, SupportsSixteenKiBBlocks) {
	const std::string name = edge_test::unique_channel_name("buffer_pool_16k");
	auto created = SharedBufferPool::create(options_for(name, kSharedBufferPoolBlockSize16K, 2));
	ASSERT_TRUE(created) << edge_runtime::to_string(created.error().code) << " "
	                     << created.error().context;
	auto& pool = created.value();
	auto write = pool.try_acquire_write();
	ASSERT_TRUE(write);
	EXPECT_EQ(write.value().size(), kSharedBufferPoolBlockSize16K);
	std::memset(write.value().data(), 0x3C, write.value().size());
	auto published = write.value().publish();
	ASSERT_TRUE(published);
	auto read = pool.acquire_read(published.value());
	ASSERT_TRUE(read);
	EXPECT_EQ(std::to_integer<unsigned char>(read.value().data()[16383]), 0x3C);
	read.value().release();
	ASSERT_TRUE(pool.remove_if_creator());
}

}  // namespace
