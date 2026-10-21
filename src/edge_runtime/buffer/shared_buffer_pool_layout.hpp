#ifndef EDGE_RUNTIME_DETAIL_SHARED_BUFFER_POOL_LAYOUT_HPP
#define EDGE_RUNTIME_DETAIL_SHARED_BUFFER_POOL_LAYOUT_HPP

#include <cstddef>
#include <cstdint>

#include "edge_runtime/utility/checked_math.hpp"

namespace edge_runtime::detail {

inline constexpr char kSharedBufferPoolMagic[8] = {'E', 'R', 'B', 'P', 'O', 'O', 'L', '2'};
inline constexpr char kSharedBufferPoolLegacyMagic[8] = {'E', 'R', 'B', 'P', 'O', 'O', 'L', '1'};
inline constexpr uint16_t kSharedBufferPoolAbiMajor = 3;
inline constexpr uint16_t kSharedBufferPoolAbiMinor = 0;
inline constexpr uint32_t kSharedBufferPoolEndianMarker = 0x01020304u;
inline constexpr uint32_t kSharedBufferPoolMaxBlockCount = 4096;
inline constexpr uint32_t kSharedBufferPoolBlockSize4K = 4096;
inline constexpr uint32_t kSharedBufferPoolBlockSize16K = 16384;
inline constexpr uint32_t kSharedBufferPoolMaxNameLength = 64;

enum class SharedBufferPoolInitState : uint32_t {
	kInitializing = 1,
	kReady = 2,
};

enum class SharedBufferBlockState : uint32_t {
	kFree = 0,
	kWritingClaiming = 1,
	kWriting = 2,
	kPublished = 3,
	kReadingClaiming = 4,
	kReading = 5,
	kEpochExhausted = 6,
	kOwnerProtocolFault = 7,
};

struct BufferPoolOwnerAbi {
	uint64_t pid;
	uint64_t proc_start_ticks;
	uint64_t boot_id_hash_hi;
	uint64_t boot_id_hash_lo;
};
static_assert(sizeof(BufferPoolOwnerAbi) == 32, "BufferPoolOwnerAbi size");
static_assert(offsetof(BufferPoolOwnerAbi, pid) == 0, "BufferPoolOwnerAbi pid");
static_assert(offsetof(BufferPoolOwnerAbi, proc_start_ticks) == 8,
              "BufferPoolOwnerAbi start time");
static_assert(offsetof(BufferPoolOwnerAbi, boot_id_hash_hi) == 16,
              "BufferPoolOwnerAbi boot hi");
static_assert(offsetof(BufferPoolOwnerAbi, boot_id_hash_lo) == 24,
              "BufferPoolOwnerAbi boot lo");

struct alignas(64) SharedBufferPoolHeaderAbi {
	uint8_t magic[8];
	uint16_t abi_major;
	uint16_t abi_minor;
	uint32_t endian_marker;
	uint32_t header_size;
	uint32_t block_size;
	uint32_t block_count;
	uint64_t mapping_size;
	uint64_t generation;
	uint64_t instance_nonce_hi;
	uint64_t instance_nonce_lo;
	uint8_t schema_fingerprint[32];
	uint32_t schema_version;
	uint32_t init_state;
	uint64_t header_checksum;
	BufferPoolOwnerAbi creator;
	alignas(4) uint32_t notify_epoch;
	uint8_t reserved[20];
};
static_assert(sizeof(SharedBufferPoolHeaderAbi) == 192, "SharedBufferPoolHeaderAbi size");
static_assert(alignof(SharedBufferPoolHeaderAbi) == 64, "SharedBufferPoolHeaderAbi alignment");
static_assert(offsetof(SharedBufferPoolHeaderAbi, magic) == 0, "pool header magic");
static_assert(offsetof(SharedBufferPoolHeaderAbi, abi_major) == 8, "pool header abi major");
static_assert(offsetof(SharedBufferPoolHeaderAbi, endian_marker) == 12,
              "pool header endian marker");
static_assert(offsetof(SharedBufferPoolHeaderAbi, header_size) == 16,
              "pool header size");
static_assert(offsetof(SharedBufferPoolHeaderAbi, block_size) == 20,
              "pool block size");
static_assert(offsetof(SharedBufferPoolHeaderAbi, block_count) == 24,
              "pool block count");
static_assert(offsetof(SharedBufferPoolHeaderAbi, mapping_size) == 32,
              "pool mapping size");
static_assert(offsetof(SharedBufferPoolHeaderAbi, generation) == 40,
              "pool generation");
static_assert(offsetof(SharedBufferPoolHeaderAbi, schema_fingerprint) == 64,
              "pool schema fingerprint");
static_assert(offsetof(SharedBufferPoolHeaderAbi, schema_version) == 96,
              "pool schema version");
static_assert(offsetof(SharedBufferPoolHeaderAbi, init_state) == 100,
              "pool init state");
static_assert(offsetof(SharedBufferPoolHeaderAbi, header_checksum) == 104,
              "pool checksum");
static_assert(offsetof(SharedBufferPoolHeaderAbi, creator) == 112, "pool creator");
static_assert(offsetof(SharedBufferPoolHeaderAbi, notify_epoch) == 144, "pool notify epoch");
static_assert(offsetof(SharedBufferPoolHeaderAbi, reserved) == 148, "pool reserved");

struct alignas(64) SharedBufferBlockHeaderAbi {
	uint32_t state;
	uint32_t reserved0;
	uint64_t block_generation;
	BufferPoolOwnerAbi owner;
	alignas(8) uint64_t owner_epoch;
	uint8_t reserved[8];
};
static_assert(sizeof(SharedBufferBlockHeaderAbi) == 64, "SharedBufferBlockHeaderAbi size");
static_assert(alignof(SharedBufferBlockHeaderAbi) == 64,
              "SharedBufferBlockHeaderAbi alignment");
static_assert(offsetof(SharedBufferBlockHeaderAbi, state) == 0, "pool block state");
static_assert(offsetof(SharedBufferBlockHeaderAbi, block_generation) == 8,
              "pool block generation");
static_assert(offsetof(SharedBufferBlockHeaderAbi, owner) == 16, "pool block owner");
static_assert(offsetof(SharedBufferBlockHeaderAbi, owner_epoch) == 48,
              "pool block owner epoch");

constexpr bool shared_buffer_pool_block_size_valid(uint32_t block_size) noexcept {
	return block_size == kSharedBufferPoolBlockSize4K ||
	       block_size == kSharedBufferPoolBlockSize16K;
}

constexpr bool shared_buffer_pool_stride_for(uint32_t block_size, uint64_t* stride) noexcept {
	if (!shared_buffer_pool_block_size_valid(block_size)) return false;
	return round_up_to_multiple_u64(sizeof(SharedBufferBlockHeaderAbi) + block_size, 64, stride);
}

constexpr bool shared_buffer_pool_mapping_size_for(uint32_t block_size, uint32_t block_count,
                                                   uint64_t* mapping_size) noexcept {
	uint64_t stride = 0;
	uint64_t blocks_size = 0;
	if (block_count == 0 || block_count > kSharedBufferPoolMaxBlockCount ||
	    !shared_buffer_pool_stride_for(block_size, &stride) ||
	    !checked_mul_u64(stride, block_count, &blocks_size) ||
	    !checked_add_u64(sizeof(SharedBufferPoolHeaderAbi), blocks_size, mapping_size)) {
		return false;
	}
	return true;
}

constexpr bool shared_buffer_pool_block_offset_for(uint32_t block_size, uint32_t block_id,
                                                   uint32_t block_count, uint64_t* block_offset,
                                                   uint64_t* payload_offset) noexcept {
	uint64_t mapping_size = 0;
	uint64_t stride = 0;
	uint64_t index_offset = 0;
	if (block_id >= block_count ||
	    !shared_buffer_pool_mapping_size_for(block_size, block_count, &mapping_size) ||
	    !shared_buffer_pool_stride_for(block_size, &stride) ||
	    !checked_mul_u64(stride, block_id, &index_offset) ||
	    !checked_add_u64(sizeof(SharedBufferPoolHeaderAbi), index_offset, block_offset) ||
	    !checked_add_u64(*block_offset, sizeof(SharedBufferBlockHeaderAbi), payload_offset)) {
		return false;
	}
	return true;
}

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_SHARED_BUFFER_POOL_LAYOUT_HPP
