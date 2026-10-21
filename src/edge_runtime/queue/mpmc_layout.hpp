#ifndef EDGE_RUNTIME_DETAIL_MPMC_LAYOUT_HPP
#define EDGE_RUNTIME_DETAIL_MPMC_LAYOUT_HPP

#include <cstddef>
#include <cstdint>
#include <string>

#include "edge_runtime/utility/checked_math.hpp"

namespace edge_runtime::detail {

inline constexpr char kMpmcHeaderMagic[] = "EDGMPMC2";
inline constexpr char kMpmcLegacyHeaderMagic[] = "EDGMPMC1";
inline constexpr uint16_t kMpmcAbiMajor = 3;
inline constexpr uint16_t kMpmcAbiMinor = 0;
inline constexpr uint16_t kMpmcAbiMinorMax = 0;
inline constexpr uint32_t kMpmcEndianMarker = 0x01020304u;
inline constexpr uint32_t kMpmcHeaderSize = 320;
inline constexpr uint32_t kMpmcFirstSlotOffset = kMpmcHeaderSize;
inline constexpr uint32_t kMpmcMaxCapacity = 4096;
inline constexpr uint32_t kMpmcMaxPayloadSize = 64u * 1024u;
inline constexpr uint32_t kMpmcMaxNameLength = 64;

enum class MpmcInitState : uint32_t {
	kEmpty = 0,
	kInitializing = 1,
	kReady = 2,
};

enum class MpmcLockState : uint32_t {
	kFree = 0,
	kClaiming = 1,
	kOwned = 2,
	kEpochExhausted = 3,
	kOwnerProtocolFault = 4,
};

enum class MpmcSlotState : uint32_t {
	kFree = 0,
	kEnqueueClaiming = 1,
	kEnqueueOwned = 2,
	kReady = 3,
	kDequeueClaiming = 4,
	kDequeueOwned = 5,
	kEpochExhausted = 6,
	kOwnerProtocolFault = 7,
};

// 共享内存中只保存显式宽度的身份字段，不保存 pidfd 或指针。
struct MpmcOwnerAbi {
	uint64_t pid;
	uint64_t proc_start_ticks;
	uint64_t boot_id_hash_hi;
	uint64_t boot_id_hash_lo;
};
static_assert(sizeof(MpmcOwnerAbi) == 32, "MpmcOwnerAbi size");
static_assert(alignof(MpmcOwnerAbi) == 8, "MpmcOwnerAbi alignment");

struct alignas(64) MpmcQueueHeaderAbi {
	char magic[8];
	uint16_t abi_major;
	uint16_t abi_minor;
	uint32_t header_size;
	uint32_t endian_marker;
	uint32_t capacity;
	uint32_t payload_size;
	uint32_t schema_version;
	uint32_t reserved0;
	uint64_t mapping_size;
	uint8_t schema_fingerprint[32];
	uint64_t generation;
	uint64_t instance_nonce_hi;
	uint64_t instance_nonce_lo;
	MpmcOwnerAbi creator;
	alignas(8) uint64_t enqueue_position;
	alignas(8) uint64_t dequeue_position;
	alignas(4) uint32_t enqueue_lock;
	alignas(4) uint32_t dequeue_lock;
	alignas(4) uint32_t init_state;
	alignas(4) uint32_t closed;
	MpmcOwnerAbi enqueue_owner;
	MpmcOwnerAbi dequeue_owner;
	uint64_t header_checksum;
	// ABI-preserved reserved storage used only as a process-shared futex epoch.
	alignas(4) uint32_t notify_epoch;
	uint8_t reserved[12];
	// v2 mutable owner epochs. The remaining bytes are reserved and remain zero on create.
	alignas(8) uint64_t enqueue_owner_epoch;
	alignas(8) uint64_t dequeue_owner_epoch;
	uint8_t owner_epoch_reserved[48];
};
static_assert(sizeof(MpmcQueueHeaderAbi) == kMpmcHeaderSize, "MpmcQueueHeaderAbi size");
static_assert(alignof(MpmcQueueHeaderAbi) == 64, "MpmcQueueHeaderAbi alignment");
static_assert(offsetof(MpmcQueueHeaderAbi, magic) == 0, "mpmc magic offset");
static_assert(offsetof(MpmcQueueHeaderAbi, schema_fingerprint) == 48,
              "mpmc schema offset");
static_assert(offsetof(MpmcQueueHeaderAbi, creator) == 104, "mpmc creator offset");
static_assert(offsetof(MpmcQueueHeaderAbi, enqueue_position) == 136,
              "mpmc enqueue position offset");
static_assert(offsetof(MpmcQueueHeaderAbi, dequeue_position) == 144,
              "mpmc dequeue position offset");
static_assert(offsetof(MpmcQueueHeaderAbi, closed) == 164, "mpmc close offset");
static_assert(offsetof(MpmcQueueHeaderAbi, enqueue_owner) == 168,
              "mpmc enqueue owner offset");
static_assert(offsetof(MpmcQueueHeaderAbi, dequeue_owner) == 200,
              "mpmc dequeue owner offset");
static_assert(offsetof(MpmcQueueHeaderAbi, header_checksum) == 232,
              "mpmc checksum offset");
static_assert(offsetof(MpmcQueueHeaderAbi, notify_epoch) == 240,
              "mpmc notify epoch offset");
static_assert(offsetof(MpmcQueueHeaderAbi, enqueue_owner_epoch) == 256,
              "mpmc enqueue owner epoch offset");
static_assert(offsetof(MpmcQueueHeaderAbi, dequeue_owner_epoch) == 264,
              "mpmc dequeue owner epoch offset");

struct alignas(64) MpmcSlotHeaderAbi {
	alignas(8) uint64_t sequence;
	alignas(4) uint32_t state;
	uint32_t reserved0;
	MpmcOwnerAbi owner;
	uint64_t ticket;
	alignas(8) uint64_t owner_epoch;
};
static_assert(sizeof(MpmcSlotHeaderAbi) == 64, "MpmcSlotHeaderAbi size");
static_assert(alignof(MpmcSlotHeaderAbi) == 64, "MpmcSlotHeaderAbi alignment");
static_assert(offsetof(MpmcSlotHeaderAbi, sequence) == 0, "mpmc slot sequence offset");
static_assert(offsetof(MpmcSlotHeaderAbi, state) == 8, "mpmc slot state offset");
static_assert(offsetof(MpmcSlotHeaderAbi, owner) == 16, "mpmc slot owner offset");
static_assert(offsetof(MpmcSlotHeaderAbi, ticket) == 48, "mpmc slot ticket offset");
static_assert(offsetof(MpmcSlotHeaderAbi, owner_epoch) == 56, "mpmc slot owner epoch offset");

inline bool mpmc_mapping_size_for(uint32_t capacity, uint32_t payload_size,
                                  uint64_t* out) noexcept {
	if (capacity == 0 || capacity > kMpmcMaxCapacity || payload_size == 0 ||
	    payload_size > kMpmcMaxPayloadSize) {
		return false;
	}
	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(sizeof(MpmcSlotHeaderAbi) + payload_size, 64, &stride)) {
		return false;
	}
	uint64_t slots = 0;
	if (!checked_mul_u64(capacity, stride, &slots)) return false;
	return checked_add_u64(kMpmcFirstSlotOffset, slots, out);
}

inline bool mpmc_slot_offset(uint32_t slot_index, uint32_t capacity, uint64_t stride,
                             uint64_t* out) noexcept {
	if (slot_index >= capacity) return false;
	uint64_t offset = 0;
	if (!checked_mul_u64(slot_index, stride, &offset)) return false;
	return checked_add_u64(kMpmcFirstSlotOffset, offset, out);
}

inline bool mpmc_checked_ticket_bounds(uint64_t ticket, uint32_t capacity,
                                       uint64_t* next_ticket,
                                       uint64_t* reclaim_sequence) noexcept {
	if (capacity == 0 || next_ticket == nullptr || reclaim_sequence == nullptr) return false;
	return checked_add_u64(ticket, uint64_t{1}, next_ticket) &&
	       checked_add_u64(ticket, static_cast<uint64_t>(capacity), reclaim_sequence);
}

std::string mpmc_shm_name(const std::string& queue_name);

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_MPMC_LAYOUT_HPP
