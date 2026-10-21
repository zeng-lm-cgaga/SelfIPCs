#include "edge_runtime/queue/mpmc_queue.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstring>

#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/queue/mpmc_layout.hpp"
#include "edge_runtime/sync/cancellation.hpp"
#include "edge_runtime/sync/futex.hpp"
#include "edge_runtime/sync/owner_snapshot.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/utility/checksum.hpp"
#include "edge_runtime/utility/failpoint.hpp"
#include "edge_runtime/utility/local_diagnostics.hpp"
#include "edge_runtime/utility/random.hpp"

namespace edge_runtime::detail {

struct MpmcQueueHandle {
	ShmObject object;
	std::string shm_name;
	ProcessIdentity self;
	uint64_t slot_stride{0};
	bool created{false};
	bool removed{false};
	LocalDiagnostics diagnostics;

	MpmcQueueHeaderAbi* header() noexcept {
		return static_cast<MpmcQueueHeaderAbi*>(object.mapping.get());
	}
	const MpmcQueueHeaderAbi* header() const noexcept {
		return static_cast<const MpmcQueueHeaderAbi*>(object.mapping.get());
	}
};

namespace {

constexpr size_t kMpmcImmutableHeaderBytes =
        offsetof(MpmcQueueHeaderAbi, enqueue_position);
constexpr uint64_t kMpmcWaitSliceNs = 10'000'000ull;

struct GateRef {
	uint32_t* state;
	uint64_t* owner_epoch;
	MpmcOwnerAbi* owner;
	const char* operation;
};

struct ConstGateRef {
	const uint32_t* state;
	const uint64_t* owner_epoch;
	const MpmcOwnerAbi* owner;
	const char* operation;
};

GateRef gate_for(MpmcQueueHandle* handle, bool enqueue) noexcept {
	return enqueue ? GateRef{&handle->header()->enqueue_lock,
	                         &handle->header()->enqueue_owner_epoch,
	                         &handle->header()->enqueue_owner,
	                          "MpmcQueue::enqueue_gate"}
	               : GateRef{&handle->header()->dequeue_lock,
	                         &handle->header()->dequeue_owner_epoch,
	                         &handle->header()->dequeue_owner,
	                          "MpmcQueue::dequeue_gate"};
}

ConstGateRef gate_for(const MpmcQueueHandle* handle, bool enqueue) noexcept {
	return enqueue
	               ? ConstGateRef{&handle->header()->enqueue_lock,
                              &handle->header()->enqueue_owner_epoch,
                              &handle->header()->enqueue_owner,
                               "MpmcQueue::enqueue_gate"}
	               : ConstGateRef{&handle->header()->dequeue_lock,
                              &handle->header()->dequeue_owner_epoch,
                              &handle->header()->dequeue_owner,
                               "MpmcQueue::dequeue_gate"};
}

using OwnerObservationKind = OwnerSnapshotKind;
using OwnerObservation = OwnerSnapshot<MpmcOwnerAbi>;

enum class OwnerObservationDisposition {
	kNotUnrecoverable,
	kChanged,
	kUnrecoverable,
};

MpmcOwnerAbi owner_from_process(const ProcessIdentity& id) noexcept {
	return {id.pid, id.proc_start_ticks, id.boot_id_hash_hi, id.boot_id_hash_lo};
}

bool owner_valid(const MpmcOwnerAbi& owner) noexcept {
	return owner.pid != 0 && owner.proc_start_ticks != 0 && owner.boot_id_hash_hi != 0 &&
	       owner.boot_id_hash_lo != 0;
}

bool same_stable_owner_observation(const OwnerObservation& first,
							const OwnerObservation& second) noexcept {
	return first.kind == OwnerObservationKind::kStable &&
	       second.kind == OwnerObservationKind::kStable && first.state == second.state &&
	       first.epoch == second.epoch && first.owner.pid == second.owner.pid &&
	       first.owner.proc_start_ticks == second.owner.proc_start_ticks &&
	       first.owner.boot_id_hash_hi == second.owner.boot_id_hash_hi &&
	       first.owner.boot_id_hash_lo == second.owner.boot_id_hash_lo;
}

bool owner_matches(const MpmcOwnerAbi& owner, const ProcessIdentity& id) noexcept {
	return owner.pid == id.pid && owner.proc_start_ticks == id.proc_start_ticks &&
	       owner.boot_id_hash_hi == id.boot_id_hash_hi && owner.boot_id_hash_lo == id.boot_id_hash_lo;
}

void set_owner(MpmcOwnerAbi* owner, const ProcessIdentity& id) noexcept {
	const MpmcOwnerAbi value = owner_from_process(id);
	store_owner_seq_cst(owner, value);
}

OwnerObservation snapshot_owner(const uint32_t* state, const uint64_t* epoch,
	                              const MpmcOwnerAbi* owner, uint32_t inactive_state,
	                              uint32_t claiming_state, uint32_t owned_state,
	                              uint32_t exhausted_state, uint32_t fault_state) noexcept {
	const auto classify = [=](uint32_t current) noexcept {
		if (current == inactive_state) return OwnerStateKind::kInactive;
		if (current == claiming_state) return OwnerStateKind::kClaiming;
		if (current == owned_state) return OwnerStateKind::kOwned;
		if (current == exhausted_state) return OwnerStateKind::kEpochExhausted;
		if (current == fault_state) return OwnerStateKind::kProtocolFault;
		return OwnerStateKind::kUnexpected;
	};
	const auto hook = [](OwnerSnapshotPoint point) noexcept {
		if (point == OwnerSnapshotPoint::kAfterState1) {
			EDGE_FAILPOINT(MPMC_OWNER_SNAPSHOT_AFTER_STATE1);
		} else {
			EDGE_FAILPOINT(MPMC_OWNER_SNAPSHOT_AFTER_EPOCH1);
		}
	};
	return snapshot_owner_seq_cst<MpmcOwnerAbi>(state, epoch, owner, classify, owner_valid, hook);
}

OwnerObservation snapshot_gate_owner(const ConstGateRef& gate) noexcept {
	return snapshot_owner(gate.state, gate.owner_epoch, gate.owner,
	                      static_cast<uint32_t>(MpmcLockState::kFree),
	                      static_cast<uint32_t>(MpmcLockState::kClaiming),
	                      static_cast<uint32_t>(MpmcLockState::kOwned),
	                      static_cast<uint32_t>(MpmcLockState::kEpochExhausted),
	                      static_cast<uint32_t>(MpmcLockState::kOwnerProtocolFault));
}

OwnerObservationDisposition confirm_owner_liveness(const MpmcQueueHandle* handle, bool enqueue,
								const OwnerObservation& observation,
								bool trigger_failpoint) noexcept {
	const bool foreign_boot =
			observation.owner.boot_id_hash_hi != handle->self.boot_id_hash_hi ||
			observation.owner.boot_id_hash_lo != handle->self.boot_id_hash_lo;
	const ConstGateRef gate = gate_for(handle, enqueue);
	if (trigger_failpoint) EDGE_FAILPOINT(MPMC_GATE_OWNER_BEFORE_LIVENESS);
	const Liveness liveness = foreign_boot
									  ? Liveness::kUnverifiable
									  : probe_liveness(observation.owner.pid, observation.owner.proc_start_ticks);
	const OwnerObservation confirmed = snapshot_gate_owner(gate);
	if (!same_stable_owner_observation(observation, confirmed)) {
		return OwnerObservationDisposition::kChanged;
	}
	if (foreign_boot) return OwnerObservationDisposition::kUnrecoverable;
	return liveness == Liveness::kAlive ? OwnerObservationDisposition::kNotUnrecoverable
	                                    : OwnerObservationDisposition::kUnrecoverable;
}

void mark_gate_protocol_failure(GateRef gate, OwnerCommitResult result) noexcept {
	const auto state = result == OwnerCommitResult::kEpochExhausted
	                           ? MpmcLockState::kEpochExhausted
	                           : MpmcLockState::kOwnerProtocolFault;
	shared_store_seq_cst(gate.state, static_cast<uint32_t>(state));
}

uint64_t header_checksum(const MpmcQueueHeaderAbi* header) noexcept {
	return fnv1a64(reinterpret_cast<const std::byte*>(header), kMpmcImmutableHeaderBytes);
}

bool same_schema(const MpmcQueueHeaderAbi* header, const SchemaDescriptor& schema) noexcept {
	return header->schema_version == schema.version &&
	       std::memcmp(header->schema_fingerprint, schema.fingerprint.data(),
	                   schema.fingerprint.size()) == 0;
}

bool valid_header_magic(const MpmcQueueHeaderAbi* header) noexcept {
	return std::memcmp(header->magic, kMpmcHeaderMagic, sizeof(header->magic)) == 0;
}

bool legacy_header_magic(const MpmcQueueHeaderAbi* header) noexcept {
	return std::memcmp(header->magic, kMpmcLegacyHeaderMagic, sizeof(header->magic)) == 0;
}

MpmcSlotHeaderAbi* slot_at(MpmcQueueHandle* handle, uint64_t position) noexcept;

std::byte* payload_at(MpmcQueueHandle* handle, uint64_t position) noexcept;

Result<void> validate_options(const MpmcQueueOptions& options, const SchemaDescriptor& schema,
                              uint32_t payload_size) {
	if (!validate_channel_name(options.name.c_str(), options.name.size()) ||
	    options.name.size() > kMpmcMaxNameLength) {
		return make_error(ErrorCode::kInvalidName, "MpmcQueue::options", "invalid queue name");
	}
	if (options.capacity == 0 || options.capacity > kMpmcMaxCapacity || payload_size == 0 ||
	    payload_size > kMpmcMaxPayloadSize || schema.version == 0) {
		return make_error(ErrorCode::kInvalidOptions, "MpmcQueue::options",
		                  "capacity, payload, or schema version out of range");
	}
	return Result<void>::ok();
}

Result<void> validate_header(const MpmcQueueHeaderAbi* header, uint64_t actual_size,
                             const MpmcQueueOptions& options, const SchemaDescriptor& schema,
                             uint32_t payload_size) {
	if (legacy_header_magic(header)) {
		return make_error(ErrorCode::kAbiMismatch, "MpmcQueue::open",
		                  "legacy MPMC ABI requires recreation");
	}
	if (!valid_header_magic(header)) {
		return make_error(ErrorCode::kCorruptHeader, "MpmcQueue::open", "magic mismatch");
	}
	if (header->abi_major != kMpmcAbiMajor || header->abi_minor != kMpmcAbiMinor) {
		return make_error(ErrorCode::kAbiMismatch, "MpmcQueue::open", "unsupported ABI version");
	}
	if (header->header_size != sizeof(MpmcQueueHeaderAbi) ||
	    header->endian_marker != kMpmcEndianMarker) {
		return make_error(ErrorCode::kCorruptHeader, "MpmcQueue::open", "header shape mismatch");
	}
	if (header->capacity == 0 || header->capacity > kMpmcMaxCapacity ||
	    header->payload_size == 0 || header->payload_size > kMpmcMaxPayloadSize ||
	    header->generation == 0) {
		return make_error(ErrorCode::kCorruptHeader, "MpmcQueue::open", "invalid header values");
	}
	uint64_t expected_size = 0;
	if (!mpmc_mapping_size_for(header->capacity, header->payload_size, &expected_size) ||
	    expected_size != header->mapping_size || actual_size != header->mapping_size) {
		return make_error(ErrorCode::kCorruptHeader, "MpmcQueue::open", "mapping size mismatch");
	}
	if (header->header_checksum != header_checksum(header)) {
		return make_error(ErrorCode::kCorruptHeader, "MpmcQueue::open", "header checksum mismatch");
	}
	if (header->capacity != options.capacity || header->payload_size != payload_size) {
		return make_error(ErrorCode::kInvalidOptions, "MpmcQueue::open",
		                  "queue capacity or payload size mismatch");
	}
	if (!same_schema(header, schema)) {
		return make_error(ErrorCode::kSchemaMismatch, "MpmcQueue::open", "schema mismatch");
	}
	const uint32_t init = shared_load_acquire(&header->init_state);
	if (init != static_cast<uint32_t>(MpmcInitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, "MpmcQueue::open",
		                  "queue is not ready");
	}
	return Result<void>::ok();
}

Result<void> cleanup_created(const std::string& shm_name, ShmObject* object) noexcept {
	const uint64_t dev = object->dev;
	const uint64_t ino = object->ino;
	object->mapping.reset();
	object->fd.reset();
	if (dev == 0 || ino == 0) return Result<void>::ok();
	return shm_unlink_checked(shm_name, dev, ino);
}

Result<void> gate_busy_result(const MpmcQueueHandle* handle, bool enqueue,
                              uint32_t observed) noexcept {
	const ConstGateRef gate = gate_for(handle, enqueue);
	(void)observed;
	const OwnerObservation observation = snapshot_gate_owner(gate);
	switch (observation.kind) {
		case OwnerObservationKind::kInactive:
		case OwnerObservationKind::kClaiming:
		case OwnerObservationKind::kUnstable:
			return make_error(ErrorCode::kQueueContention, gate.operation,
			                  "gate owner snapshot is not stable");
		case OwnerObservationKind::kCorrupt:
		case OwnerObservationKind::kEpochExhausted:
			return make_error(ErrorCode::kRecoveryBlocked, gate.operation,
			                  "gate owner protocol is not recoverable");
		case OwnerObservationKind::kStable:
			break;
	}
	if (owner_matches(observation.owner, handle->self)) {
		const OwnerObservationDisposition disposition =
		        confirm_owner_liveness(handle, enqueue, observation, true);
		if (disposition == OwnerObservationDisposition::kChanged) {
			return make_error(ErrorCode::kQueueContention, gate.operation,
			                  "gate owner changed while checking concurrent use");
		}
		return make_error(ErrorCode::kConcurrentHandleUse, gate.operation,
		                  "gate is owned by this process");
	}
	const OwnerObservationDisposition disposition =
	        confirm_owner_liveness(handle, enqueue, observation, true);
	if (disposition == OwnerObservationDisposition::kNotUnrecoverable) {
		return make_error(ErrorCode::kQueueContention, gate.operation,
		                  "gate is owned by a live process");
	}
	if (disposition == OwnerObservationDisposition::kChanged) {
		return make_error(ErrorCode::kQueueContention, gate.operation,
		                  "gate owner changed while probing liveness");
	}
	return make_error(ErrorCode::kRecoveryBlocked, gate.operation,
	                  "gate owner is dead or unverifiable");
}

Result<void> claim_gate(MpmcQueueHandle* handle, bool enqueue) noexcept {
	const GateRef gate = gate_for(handle, enqueue);
	const uint32_t free = static_cast<uint32_t>(MpmcLockState::kFree);
	const uint32_t claiming = static_cast<uint32_t>(MpmcLockState::kClaiming);
	const uint32_t observed = shared_load_seq_cst(gate.state);
	if (observed != free) return gate_busy_result(handle, enqueue, observed);
	if (!shared_cas_seq_cst(gate.state, free, claiming)) {
		return make_error(ErrorCode::kQueueContention, gate.operation,
		                  "gate was claimed concurrently");
	}
	return Result<void>::ok();
}

Result<void> commit_claimed_gate(MpmcQueueHandle* handle, bool enqueue) noexcept {
	const GateRef gate = gate_for(handle, enqueue);
	uint64_t committed_epoch = 0;
	const OwnerCommitResult owner_commit =
	        begin_owner_commit(gate.owner_epoch, 2u, &committed_epoch);
	if (owner_commit != OwnerCommitResult::kReady) {
		mark_gate_protocol_failure(gate, owner_commit);
		return make_error(ErrorCode::kRecoveryBlocked, gate.operation,
		                  owner_commit == OwnerCommitResult::kEpochExhausted
		                          ? "gate owner epoch exhausted"
		                          : "gate owner epoch protocol is corrupt");
	}
	if (enqueue) {
		EDGE_FAILPOINT(MPMC_ENQUEUE_GATE_OWNER_EPOCH_ODD);
	} else {
		EDGE_FAILPOINT(MPMC_DEQUEUE_GATE_OWNER_EPOCH_ODD);
	}
	set_owner(gate.owner, handle->self);
	shared_store_seq_cst(gate.owner_epoch, committed_epoch);
	if (enqueue) {
		EDGE_FAILPOINT(MPMC_ENQUEUE_GATE_OWNER_EPOCH_COMMITTED);
	} else {
		EDGE_FAILPOINT(MPMC_DEQUEUE_GATE_OWNER_EPOCH_COMMITTED);
	}
	shared_store_seq_cst(gate.state, static_cast<uint32_t>(MpmcLockState::kOwned));
	return Result<void>::ok();
}

Result<void> acquire_gate(MpmcQueueHandle* handle, bool enqueue) noexcept {
	auto claimed = claim_gate(handle, enqueue);
	if (!claimed) return claimed.error();
	return commit_claimed_gate(handle, enqueue);
}

void release_gate(MpmcQueueHandle* handle, bool enqueue) noexcept {
	const GateRef gate = gate_for(handle, enqueue);
	// Keep the last committed identity stable while the gate transitions to FREE. A contender
	// may still observe the old OWNED state during this release; clearing it first would create a
	// plain-data race with that diagnostic read. A future claimant overwrites it while CLAIMING.
	shared_store_seq_cst(gate.state, static_cast<uint32_t>(MpmcLockState::kFree));
}

void notify_waiters(MpmcQueueHandle* handle) noexcept {
	// Data and position release stores, followed by gate release, happen before this hint. The
	// epoch is only a wakeup hint; try_push/try_pop remain the data correctness predicate.
#if EDGERUNTIME_ENABLE_FAILPOINTS
	const char* disabled = std::getenv("EDGE_RUNTIME_MPMC_DISABLE_NOTIFY");
	if (disabled != nullptr && std::strcmp(disabled, "1") == 0) return;
#endif
	(void)shared_fetch_add_relaxed(&handle->header()->notify_epoch, uint32_t{1});
	handle->diagnostics.add(DiagnosticCounter::kNotifications);
	(void)futex_wake(&handle->header()->notify_epoch, INT_MAX);
}

OwnerObservation classify_gate_owner(const MpmcQueueHandle* handle, bool enqueue) noexcept {
	const ConstGateRef gate = gate_for(handle, enqueue);
	return snapshot_gate_owner(gate);
}

OwnerObservationDisposition owner_observation_is_unrecoverable(
		const MpmcQueueHandle* handle, bool enqueue, const OwnerObservation& observation) noexcept {
	switch (observation.kind) {
		case OwnerObservationKind::kInactive:
		case OwnerObservationKind::kClaiming:
		case OwnerObservationKind::kUnstable:
			return OwnerObservationDisposition::kNotUnrecoverable;
		case OwnerObservationKind::kCorrupt:
		case OwnerObservationKind::kEpochExhausted:
			return OwnerObservationDisposition::kUnrecoverable;
		case OwnerObservationKind::kStable:
			return confirm_owner_liveness(handle, enqueue, observation, false);
	}
	return OwnerObservationDisposition::kUnrecoverable;
}

Result<uint64_t> dequeue_drain_budget(const MpmcQueueHandle* handle,
	                                    const char* operation) noexcept {
	const OwnerObservation observation = classify_gate_owner(handle, false);
	switch (observation.kind) {
		case OwnerObservationKind::kInactive:
		case OwnerObservationKind::kStable:
			break;
		case OwnerObservationKind::kClaiming:
		case OwnerObservationKind::kUnstable:
			return make_error(ErrorCode::kQueueContention, operation,
			                  "dequeue gate owner claim is in progress");
		case OwnerObservationKind::kEpochExhausted:
			return make_error(ErrorCode::kSequenceExhausted, operation,
			                  "dequeue gate owner epoch is exhausted");
		case OwnerObservationKind::kCorrupt:
			return make_error(ErrorCode::kRecoveryBlocked, operation,
			                  "dequeue gate owner protocol is corrupt");
	}
	if ((observation.epoch & 1u) != 0u || observation.epoch > kMaxCommittedOwnerEpoch) {
		return make_error(ErrorCode::kRecoveryBlocked, operation,
		                  "dequeue gate owner epoch is invalid");
	}
	if (observation.kind == OwnerObservationKind::kStable) {
		const OwnerObservationDisposition disposition =
		        owner_observation_is_unrecoverable(handle, false, observation);
		if (disposition == OwnerObservationDisposition::kChanged) {
			return make_error(ErrorCode::kQueueContention, operation,
			                  "dequeue gate owner changed while probing liveness");
		}
		if (disposition == OwnerObservationDisposition::kUnrecoverable) {
			return make_error(ErrorCode::kRecoveryBlocked, operation,
			                  "dequeue gate owner is dead or unverifiable");
		}
		return make_error(ErrorCode::kQueueContention, operation,
		                  "dequeue gate is owned by an active consumer");
	}
	const uint64_t future_claims =
	        (kMaxCommittedOwnerEpoch - observation.epoch) / uint64_t{2};
	return Result<uint64_t>(future_claims);
}

Result<void> validate_queue_positions(uint64_t enqueue, uint64_t dequeue,
	                                      uint32_t capacity, const char* operation) noexcept {
	if (enqueue < dequeue) {
		return make_error(ErrorCode::kRecoveryBlocked, operation,
		                  "queue positions are inconsistent");
	}
	if (enqueue - dequeue > capacity) {
		return make_error(ErrorCode::kRecoveryBlocked, operation,
		                  "queue positions exceed capacity");
	}
	return Result<void>::ok();
}

struct QueuePositionSnapshot {
	uint64_t enqueue{0};
	uint64_t dequeue{0};
};

Result<QueuePositionSnapshot> snapshot_queue_positions(const MpmcQueueHeaderAbi* header,
	                                                     const char* operation) noexcept {
	for (unsigned attempt = 0; attempt < kOwnerSnapshotRetries; ++attempt) {
		const uint64_t first_enqueue = shared_load_seq_cst(&header->enqueue_position);
		EDGE_FAILPOINT(MPMC_POSITION_AFTER_ENQUEUE);
		const uint64_t first_dequeue = shared_load_seq_cst(&header->dequeue_position);
		const uint64_t second_enqueue = shared_load_seq_cst(&header->enqueue_position);
		const uint64_t second_dequeue = shared_load_seq_cst(&header->dequeue_position);
		if (first_enqueue == second_enqueue && first_dequeue == second_dequeue) {
			return Result<QueuePositionSnapshot>(
					QueuePositionSnapshot{second_enqueue, second_dequeue});
		}
	}
	return make_error(ErrorCode::kQueueContention, operation,
	                  "queue positions changed while taking a consistent snapshot");
}

Result<void> validate_push_drain_admission(const MpmcQueueHandle* handle, uint64_t* enqueue,
	                                          uint64_t* dequeue, const char* operation) noexcept {
	if (enqueue == nullptr || dequeue == nullptr) {
		return make_error(ErrorCode::kInvalidOptions, operation, "position output is null");
	}
	const uint32_t capacity = handle->header()->capacity;
	for (unsigned attempt = 0; attempt < kOwnerSnapshotRetries; ++attempt) {
		auto positions = validate_queue_positions(*enqueue, *dequeue, capacity, operation);
		if (!positions) return positions.error();
		const uint64_t pending = *enqueue - *dequeue;
		uint64_t pending_after = 0;
		if (!checked_add_u64(pending, uint64_t{1}, &pending_after)) {
			return make_error(ErrorCode::kSequenceExhausted, operation,
			                  "pending message count cannot advance");
		}
		auto budget = dequeue_drain_budget(handle, operation);
		if (!budget) return budget.error();
		if (pending == capacity) {
			if (budget.value() == 0) {
				return make_error(ErrorCode::kSequenceExhausted, operation,
				                  "dequeue gate budget cannot drain the published message");
			}
			return make_error(ErrorCode::kQueueFull, operation, "queue became full while admitting push");
		}
		auto refreshed = snapshot_queue_positions(handle->header(), operation);
		if (!refreshed) return refreshed.error();
		if (refreshed.value().enqueue != *enqueue || refreshed.value().dequeue != *dequeue) {
			*enqueue = refreshed.value().enqueue;
			*dequeue = refreshed.value().dequeue;
			continue;
		}
		if (pending_after > budget.value()) {
			return make_error(ErrorCode::kSequenceExhausted, operation,
			                  "dequeue gate budget cannot drain the published message");
		}
		return Result<void>::ok();
	}
	return make_error(ErrorCode::kQueueContention, operation,
	                  "positions changed while taking push admission");
}

Result<void> validate_ticket_bounds(uint64_t ticket, uint32_t capacity,
	                                   uint64_t* next_ticket, uint64_t* reclaim_sequence,
	                                   const char* operation) noexcept {
	if (mpmc_checked_ticket_bounds(ticket, capacity, next_ticket, reclaim_sequence)) {
		return Result<void>::ok();
	}
	return make_error(ErrorCode::kSequenceExhausted, operation,
	                  "queue ticket cannot advance without wraparound");
}

Result<void> validate_live_handle(const std::shared_ptr<MpmcQueueHandle>& handle,
                                  const char* operation) noexcept {
	if (!handle || handle->removed) return make_error(ErrorCode::kStaleHandle, operation);
	if (handle->self.pid != static_cast<uint64_t>(::getpid())) {
		return make_error(ErrorCode::kStaleHandle, operation, "reopen handle after fork");
	}
	if (shared_load_acquire(&handle->header()->closed) > 1) {
		return make_error(ErrorCode::kRecoveryBlocked, operation, "invalid close state");
	}
	if (shared_load_acquire(&handle->header()->init_state) !=
	    static_cast<uint32_t>(MpmcInitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, operation,
		                  "queue is not ready");
	}
	return Result<void>::ok();
}

}  // namespace

namespace {

MpmcSlotHeaderAbi* slot_at(MpmcQueueHandle* handle, uint64_t position) noexcept {
	const uint32_t index = static_cast<uint32_t>(position % handle->header()->capacity);
	uint64_t offset = 0;
	if (!mpmc_slot_offset(index, handle->header()->capacity, handle->slot_stride, &offset)) return nullptr;
	return reinterpret_cast<MpmcSlotHeaderAbi*>(static_cast<std::byte*>(handle->object.mapping.get()) +
	                                             offset);
}

std::byte* payload_at(MpmcQueueHandle* handle, uint64_t position) noexcept {
	auto* slot = slot_at(handle, position);
	return slot == nullptr ? nullptr : reinterpret_cast<std::byte*>(slot) + sizeof(*slot);
}

OwnerObservation snapshot_slot_owner(const MpmcSlotHeaderAbi* slot, bool enqueue) noexcept {
	return snapshot_owner(&slot->state, &slot->owner_epoch, &slot->owner,
	                      enqueue ? static_cast<uint32_t>(MpmcSlotState::kFree)
	                              : static_cast<uint32_t>(MpmcSlotState::kReady),
	                      enqueue ? static_cast<uint32_t>(MpmcSlotState::kEnqueueClaiming)
	                              : static_cast<uint32_t>(MpmcSlotState::kDequeueClaiming),
	                      enqueue ? static_cast<uint32_t>(MpmcSlotState::kEnqueueOwned)
	                              : static_cast<uint32_t>(MpmcSlotState::kDequeueOwned),
	                      static_cast<uint32_t>(MpmcSlotState::kEpochExhausted),
	                      static_cast<uint32_t>(MpmcSlotState::kOwnerProtocolFault));
}

void mark_slot_protocol_failure(MpmcSlotHeaderAbi* slot, OwnerCommitResult result) noexcept {
	const auto state = result == OwnerCommitResult::kEpochExhausted
	                           ? MpmcSlotState::kEpochExhausted
	                           : MpmcSlotState::kOwnerProtocolFault;
	shared_store_seq_cst(&slot->state, static_cast<uint32_t>(state));
}

Result<std::shared_ptr<MpmcQueueHandle>> open_existing(const MpmcQueueOptions& options,
                                                       const SchemaDescriptor& schema,
                                                       uint32_t payload_size) {
	auto identity = read_current_process_identity();
	if (!identity) return identity.error();
	const std::string shm_name = mpmc_shm_name(options.name);
	auto fd = shm_open_existing(shm_name);
	if (!fd) return fd.error();
	ShmObject object;
	object.name = shm_name;
	object.fd = std::move(fd.value());
	auto stat = shm_fstat_and_capture(object.fd, &object.dev, &object.ino, &object.size);
	if (!stat) return stat.error();
	constexpr uint64_t kMpmcHeaderProbeSize =
	        offsetof(MpmcQueueHeaderAbi, init_state) + sizeof(uint32_t);
	if (object.size < kMpmcHeaderProbeSize) {
		return make_error(ErrorCode::kInitializationIncomplete, "MpmcQueue::open",
		                  "object is smaller than the queue header probe");
	}
	auto mapping = mmap_region(object.fd, object.size);
	if (!mapping) return mapping.error();
	object.mapping = std::move(mapping.value());
	auto* header = static_cast<MpmcQueueHeaderAbi*>(object.mapping.get());
	// Read only the common prefix before rejecting a legacy or short mapping. In particular,
	// do not touch either v2 owner epoch until the exact major and header size are accepted.
	if (legacy_header_magic(header) || header->abi_major != kMpmcAbiMajor ||
	    header->header_size != sizeof(MpmcQueueHeaderAbi) ||
	    object.size < sizeof(MpmcQueueHeaderAbi)) {
		return make_error(ErrorCode::kAbiMismatch, "MpmcQueue::open",
		                  "queue requires the v3 MPMC mapping ABI");
	}
	const uint32_t init = shared_load_acquire(&header->init_state);
	if (init != static_cast<uint32_t>(MpmcInitState::kReady)) {
		if (init == static_cast<uint32_t>(MpmcInitState::kInitializing)) {
			return make_error(ErrorCode::kInitializationIncomplete, "MpmcQueue::open",
			                  "creator did not commit READY");
		}
		return make_error(ErrorCode::kCorruptHeader, "MpmcQueue::open", "invalid init state");
	}
	auto valid = validate_header(header, object.size, options, schema, payload_size);
	if (!valid) return valid.error();
	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(sizeof(MpmcSlotHeaderAbi) + payload_size, 64, &stride)) {
		return make_error(ErrorCode::kInvalidOptions, "MpmcQueue::open", "slot stride overflow");
	}
	auto handle = std::make_shared<MpmcQueueHandle>();
	handle->object = std::move(object);
	handle->shm_name = shm_name;
	handle->self = identity.value();
	handle->slot_stride = stride;
	return Result<std::shared_ptr<MpmcQueueHandle>>(std::move(handle));
}

}  // namespace

std::string mpmc_shm_name(const std::string& queue_name) {
	return "/edgeruntime.mpmc." + std::to_string(getuid()) + "." + queue_name;
}

Result<std::shared_ptr<MpmcQueueHandle>> mpmc_create_impl(const MpmcQueueOptions& options,
                                                          const SchemaDescriptor& schema,
                                                          uint32_t payload_size) {
	auto opts = validate_options(options, schema, payload_size);
	if (!opts) return opts.error();
	auto identity = read_current_process_identity();
	if (!identity) return identity.error();
	uint64_t mapping_size = 0;
	if (!mpmc_mapping_size_for(options.capacity, payload_size, &mapping_size)) {
		return make_error(ErrorCode::kInvalidOptions, "MpmcQueue::create", "mapping size overflow");
	}
	const std::string shm_name = mpmc_shm_name(options.name);
	auto fd = shm_open_create(shm_name);
	if (!fd) return fd.error();
	ShmObject object;
	object.name = shm_name;
	object.fd = std::move(fd.value());
	auto stat_before = shm_fstat_and_capture(object.fd, &object.dev, &object.ino, &object.size);
	if (!stat_before) {
		auto captured = shm_capture_created_identity(object.fd, &object.dev, &object.ino);
		if (!captured) return captured.error();
		auto removed = shm_unlink_checked(shm_name, object.dev, object.ino);
		if (!removed && removed.error().code != ErrorCode::kNotFound)
			return removed.error();
		return stat_before.error();
	}
	auto truncate = shm_truncate(object.fd, mapping_size);
	if (!truncate) {
		(void)cleanup_created(shm_name, &object);
		return truncate.error();
	}
	auto mapping = mmap_region(object.fd, mapping_size);
	if (!mapping) {
		(void)cleanup_created(shm_name, &object);
		return mapping.error();
	}
	object.mapping = std::move(mapping.value());
	object.size = mapping_size;
	auto* header = static_cast<MpmcQueueHeaderAbi*>(object.mapping.get());
	std::memset(object.mapping.get(), 0, static_cast<size_t>(mapping_size));
	const ProcessIdentity self = identity.value();
	if (self.pid == 0 || self.proc_start_ticks == 0 || self.boot_id_hash_hi == 0 ||
	    self.boot_id_hash_lo == 0 || !random_bytes(&header->instance_nonce_hi, sizeof(uint64_t)) ||
	    !random_bytes(&header->instance_nonce_lo, sizeof(uint64_t))) {
		(void)cleanup_created(shm_name, &object);
		return make_error(ErrorCode::kSystemError, "MpmcQueue::create", "identity or nonce failed");
	}
	std::memcpy(header->magic, kMpmcHeaderMagic, sizeof(header->magic));
	header->abi_major = kMpmcAbiMajor;
	header->abi_minor = kMpmcAbiMinor;
	header->header_size = sizeof(MpmcQueueHeaderAbi);
	header->endian_marker = kMpmcEndianMarker;
	header->capacity = options.capacity;
	header->payload_size = payload_size;
	header->schema_version = schema.version;
	header->mapping_size = mapping_size;
	std::memcpy(header->schema_fingerprint, schema.fingerprint.data(), schema.fingerprint.size());
	header->generation = 1;
	header->creator = owner_from_process(self);
	shared_store_relaxed(&header->notify_epoch, uint32_t{0});
	shared_store_seq_cst(&header->enqueue_owner_epoch, uint64_t{0});
	shared_store_seq_cst(&header->dequeue_owner_epoch, uint64_t{0});
	shared_store_relaxed(&header->init_state,
	                     static_cast<uint32_t>(MpmcInitState::kInitializing));
	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(sizeof(MpmcSlotHeaderAbi) + payload_size, 64, &stride)) {
		(void)cleanup_created(shm_name, &object);
		return make_error(ErrorCode::kInvalidOptions, "MpmcQueue::create", "slot stride overflow");
	}
	for (uint32_t i = 0; i < options.capacity; ++i) {
		uint64_t offset = 0;
		if (!mpmc_slot_offset(i, options.capacity, stride, &offset)) {
			(void)cleanup_created(shm_name, &object);
			return make_error(ErrorCode::kInvalidOptions, "MpmcQueue::create", "slot offset overflow");
		}
		auto* slot = reinterpret_cast<MpmcSlotHeaderAbi*>(
		        static_cast<std::byte*>(object.mapping.get()) + offset);
		shared_store_relaxed(&slot->sequence, static_cast<uint64_t>(i));
		shared_store_seq_cst(&slot->state, static_cast<uint32_t>(MpmcSlotState::kFree));
		slot->ticket = i;
		shared_store_seq_cst(&slot->owner_epoch, uint64_t{0});
	}
	header->header_checksum = header_checksum(header);
	shared_store_release(&header->init_state, static_cast<uint32_t>(MpmcInitState::kReady));
	auto handle = std::make_shared<MpmcQueueHandle>();
	handle->object = std::move(object);
	handle->shm_name = shm_name;
	handle->self = self;
	handle->slot_stride = stride;
	handle->created = true;
	return Result<std::shared_ptr<MpmcQueueHandle>>(std::move(handle));
}

Result<std::shared_ptr<MpmcQueueHandle>> mpmc_open_impl(const MpmcQueueOptions& options,
                                                        const SchemaDescriptor& schema,
                                                        uint32_t payload_size) {
	auto opts = validate_options(options, schema, payload_size);
	if (!opts) return opts.error();
	return open_existing(options, schema, payload_size);
}

static Result<void> mpmc_try_push_once(const std::shared_ptr<MpmcQueueHandle>& handle,
                                       const void* payload, uint32_t payload_size) noexcept {
	auto valid = validate_live_handle(handle, "MpmcQueue::try_push");
	if (!valid) return valid.error();
	if (payload == nullptr || payload_size != handle->header()->payload_size) {
		return make_error(ErrorCode::kInvalidOptions, "MpmcQueue::try_push", "payload size mismatch");
	}
	if (shared_load_acquire(&handle->header()->closed) != 0) {
		return make_error(ErrorCode::kClosed, "MpmcQueue::try_push", "queue is closed");
	}
	const uint32_t capacity = handle->header()->capacity;
	auto position_snapshot = snapshot_queue_positions(handle->header(), "MpmcQueue::try_push");
	if (!position_snapshot) return position_snapshot.error();
	uint64_t enqueue = position_snapshot.value().enqueue;
	uint64_t dequeue = position_snapshot.value().dequeue;
	auto positions = validate_queue_positions(enqueue, dequeue, capacity, "MpmcQueue::try_push");
	if (!positions) return positions.error();
	uint64_t next_enqueue = 0;
	uint64_t reclaim_sequence = 0;
	auto bounds = validate_ticket_bounds(enqueue, capacity, &next_enqueue, &reclaim_sequence,
	                                    "MpmcQueue::try_push");
	if (!bounds) return bounds.error();
	(void)reclaim_sequence;
	if (enqueue - dequeue == capacity) {
		const uint32_t enqueue_state =
				shared_load_seq_cst(&handle->header()->enqueue_lock);
		if (enqueue_state != static_cast<uint32_t>(MpmcLockState::kFree)) {
			return gate_busy_result(handle.get(), true, enqueue_state);
		}
		const OwnerObservation consumer = classify_gate_owner(handle.get(), false);
		const OwnerObservationDisposition consumer_state =
		        owner_observation_is_unrecoverable(handle.get(), false, consumer);
		if (consumer_state == OwnerObservationDisposition::kChanged) {
			return make_error(ErrorCode::kQueueContention, "MpmcQueue::try_push",
			                  "consumer gate changed while probing liveness");
		}
		if (consumer_state == OwnerObservationDisposition::kUnrecoverable) {
			return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_push",
			                  "consumer gate has an unrecoverable claim");
		}
		auto drain_budget = dequeue_drain_budget(handle.get(), "MpmcQueue::try_push");
		if (!drain_budget) {
			if (drain_budget.error().code != ErrorCode::kQueueContention) {
				return drain_budget.error();
			}
		} else if (drain_budget.value() == 0) {
			return make_error(ErrorCode::kSequenceExhausted, "MpmcQueue::try_push",
			                  "dequeue gate has no remaining drain budget");
		}
		return make_error(ErrorCode::kQueueFull, "MpmcQueue::try_push", "queue is full");
	}
	EDGE_FAILPOINT(MPMC_BEFORE_PUSH_DRAIN_ADMISSION);
	auto admission = validate_push_drain_admission(handle.get(), &enqueue, &dequeue,
	                                               "MpmcQueue::try_push");
	if (!admission) return admission.error();
	auto gate = acquire_gate(handle.get(), true);
	if (!gate) return gate.error();
	if (shared_load_acquire(&handle->header()->closed) != 0) {
		release_gate(handle.get(), true);
		return make_error(ErrorCode::kClosed, "MpmcQueue::try_push", "queue is closed");
	}
	EDGE_FAILPOINT(MPMC_ENQUEUE_GATE_OWNED);
	position_snapshot = snapshot_queue_positions(handle->header(), "MpmcQueue::try_push");
	if (!position_snapshot) {
		release_gate(handle.get(), true);
		return position_snapshot.error();
	}
	enqueue = position_snapshot.value().enqueue;
	dequeue = position_snapshot.value().dequeue;
	positions = validate_queue_positions(enqueue, dequeue, capacity, "MpmcQueue::try_push");
	if (!positions) {
		release_gate(handle.get(), true);
		return positions.error();
	}
	bounds = validate_ticket_bounds(enqueue, capacity, &next_enqueue, &reclaim_sequence,
	                               "MpmcQueue::try_push");
	if (!bounds) {
		release_gate(handle.get(), true);
		return bounds.error();
	}
	(void)reclaim_sequence;
	if (enqueue - dequeue == capacity) {
		// A full queue is normally retryable. If the consumer gate owns the oldest slot and
		// its owner is dead or unverifiable, retrying would hide a fail-closed condition until
		// the deadline. Do not reclaim the slot; surface the recovery boundary instead.
		const OwnerObservation consumer = classify_gate_owner(handle.get(), false);
		const OwnerObservationDisposition consumer_state =
		        owner_observation_is_unrecoverable(handle.get(), false, consumer);
		release_gate(handle.get(), true);
		if (consumer_state == OwnerObservationDisposition::kChanged) {
			return make_error(ErrorCode::kQueueContention, "MpmcQueue::try_push",
			                  "consumer gate changed while probing liveness");
		}
		if (consumer_state == OwnerObservationDisposition::kUnrecoverable) {
			return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_push",
			                  "consumer gate has an unrecoverable claim");
		}
		auto drain_budget = dequeue_drain_budget(handle.get(), "MpmcQueue::try_push");
		if (!drain_budget) {
			if (drain_budget.error().code != ErrorCode::kQueueContention) {
				return drain_budget.error();
			}
		} else if (drain_budget.value() == 0) {
			return make_error(ErrorCode::kSequenceExhausted, "MpmcQueue::try_push",
			                  "dequeue gate has no remaining drain budget");
		}
		return make_error(ErrorCode::kQueueFull, "MpmcQueue::try_push", "queue is full");
	}
	admission = validate_push_drain_admission(handle.get(), &enqueue, &dequeue,
	                                          "MpmcQueue::try_push");
	if (!admission) {
		release_gate(handle.get(), true);
		return admission.error();
	}
	auto* slot = slot_at(handle.get(), enqueue);
	const bool slot_invalid =
	        slot == nullptr || shared_load_acquire(&slot->sequence) != enqueue ||
	        shared_load_seq_cst(&slot->state) != static_cast<uint32_t>(MpmcSlotState::kFree);
	if (slot_invalid) {
		if (slot != nullptr) {
			const OwnerObservation slot_owner = snapshot_slot_owner(slot, true);
			if (slot_owner.kind == OwnerObservationKind::kEpochExhausted) {
				release_gate(handle.get(), true);
				return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_push",
				                  "slot owner epoch is exhausted");
			}
		}
		release_gate(handle.get(), true);
		return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_push",
		                  "free slot sequence or state is inconsistent");
	}
	const OwnerCommitResult slot_capacity = owner_epoch_capacity(&slot->owner_epoch, 4u);
	if (slot_capacity != OwnerCommitResult::kReady) {
		const auto terminal_state = slot_capacity == OwnerCommitResult::kEpochExhausted
		                                    ? MpmcSlotState::kEpochExhausted
		                                    : MpmcSlotState::kOwnerProtocolFault;
		shared_store_seq_cst(&slot->state, static_cast<uint32_t>(terminal_state));
		release_gate(handle.get(), true);
		return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_push",
		                  slot_capacity == OwnerCommitResult::kEpochExhausted
		                          ? "slot owner epoch exhausted"
		                          : "slot owner epoch protocol is corrupt");
	}
	if (!shared_cas_seq_cst(&slot->state, static_cast<uint32_t>(MpmcSlotState::kFree),
	                        static_cast<uint32_t>(MpmcSlotState::kEnqueueClaiming))) {
		release_gate(handle.get(), true);
		return make_error(ErrorCode::kQueueContention, "MpmcQueue::try_push",
		                  "slot was claimed concurrently");
	}
	uint64_t committed_epoch = 0;
	const OwnerCommitResult owner_commit =
	        begin_owner_commit(&slot->owner_epoch, 4u, &committed_epoch);
	if (owner_commit != OwnerCommitResult::kReady) {
		mark_slot_protocol_failure(slot, owner_commit);
		release_gate(handle.get(), true);
		return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_push",
		                  owner_commit == OwnerCommitResult::kEpochExhausted
		                          ? "slot owner epoch exhausted"
		                          : "slot owner epoch protocol is corrupt");
	}
	EDGE_FAILPOINT(MPMC_ENQUEUE_SLOT_OWNER_EPOCH_ODD);
	set_owner(&slot->owner, handle->self);
	slot->ticket = enqueue;
	shared_store_seq_cst(&slot->owner_epoch, committed_epoch);
	EDGE_FAILPOINT(MPMC_ENQUEUE_SLOT_OWNER_EPOCH_COMMITTED);
	shared_store_seq_cst(&slot->state, static_cast<uint32_t>(MpmcSlotState::kEnqueueOwned));
	EDGE_FAILPOINT(MPMC_ENQUEUE_SLOT_OWNED);
	std::memcpy(payload_at(handle.get(), enqueue), payload, payload_size);
	shared_store_release(&slot->sequence, next_enqueue);
	shared_store_seq_cst(&slot->state, static_cast<uint32_t>(MpmcSlotState::kReady));
	shared_store_seq_cst(&handle->header()->enqueue_position, next_enqueue);
	release_gate(handle.get(), true);
	notify_waiters(handle.get());
	return Result<void>::ok();
}

static Result<void> mpmc_try_pop_once(const std::shared_ptr<MpmcQueueHandle>& handle, void* payload,
                                      uint32_t payload_size) noexcept {
	auto valid = validate_live_handle(handle, "MpmcQueue::try_pop");
	if (!valid) return valid.error();
	if (payload == nullptr || payload_size != handle->header()->payload_size) {
		return make_error(ErrorCode::kInvalidOptions, "MpmcQueue::try_pop", "payload size mismatch");
	}
	const bool closed = shared_load_acquire(&handle->header()->closed) != 0;
	const uint32_t capacity = handle->header()->capacity;
	auto position_snapshot = snapshot_queue_positions(handle->header(), "MpmcQueue::try_pop");
	if (!position_snapshot) return position_snapshot.error();
	uint64_t enqueue = position_snapshot.value().enqueue;
	uint64_t dequeue = position_snapshot.value().dequeue;
	auto positions = validate_queue_positions(enqueue, dequeue, capacity, "MpmcQueue::try_pop");
	if (!positions) return positions.error();
	if (closed && dequeue == enqueue) {
		const uint32_t state = shared_load_seq_cst(&handle->header()->dequeue_lock);
		if (state != static_cast<uint32_t>(MpmcLockState::kFree)) {
			return gate_busy_result(handle.get(), false, state);
		}
		return make_error(ErrorCode::kClosed, "MpmcQueue::try_pop", "queue is drained");
	}
	uint64_t next_dequeue = 0;
	uint64_t reclaim_sequence = 0;
	auto bounds = validate_ticket_bounds(dequeue, capacity, &next_dequeue, &reclaim_sequence,
	                                    "MpmcQueue::try_pop");
	if (!bounds) return bounds.error();
	if (dequeue == enqueue) {
		const uint32_t dequeue_state =
				shared_load_seq_cst(&handle->header()->dequeue_lock);
		if (dequeue_state != static_cast<uint32_t>(MpmcLockState::kFree)) {
			return gate_busy_result(handle.get(), false, dequeue_state);
		}
		if (closed)
			return make_error(ErrorCode::kClosed, "MpmcQueue::try_pop",
			                  "queue is drained");
		const OwnerObservation producer = classify_gate_owner(handle.get(), true);
		const OwnerObservationDisposition producer_state =
		        owner_observation_is_unrecoverable(handle.get(), true, producer);
		if (producer_state == OwnerObservationDisposition::kChanged) {
			return make_error(ErrorCode::kQueueContention, "MpmcQueue::try_pop",
			                  "producer gate changed while probing liveness");
		}
		if (producer_state == OwnerObservationDisposition::kUnrecoverable) {
			return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_pop",
			                  "producer gate has an unrecoverable claim");
		}
		return make_error(ErrorCode::kQueueEmpty, "MpmcQueue::try_pop", "queue is empty");
	}
	EDGE_FAILPOINT(MPMC_BEFORE_DEQUEUE_GATE_CLAIM);
	auto drain_budget = dequeue_drain_budget(handle.get(), "MpmcQueue::try_pop");
	if (!drain_budget) return drain_budget.error();
	if (drain_budget.value() == 0) {
		return make_error(ErrorCode::kSequenceExhausted, "MpmcQueue::try_pop",
		                  "dequeue gate has no remaining drain budget");
	}
	auto gate = claim_gate(handle.get(), false);
	if (!gate) return gate.error();
	position_snapshot = snapshot_queue_positions(handle->header(), "MpmcQueue::try_pop");
	if (!position_snapshot) {
		release_gate(handle.get(), false);
		return position_snapshot.error();
	}
	enqueue = position_snapshot.value().enqueue;
	dequeue = position_snapshot.value().dequeue;
	positions = validate_queue_positions(enqueue, dequeue, capacity, "MpmcQueue::try_pop");
	if (!positions) {
		release_gate(handle.get(), false);
		return positions.error();
	}
	if (closed && dequeue == enqueue) {
		release_gate(handle.get(), false);
		return make_error(ErrorCode::kClosed, "MpmcQueue::try_pop", "queue is drained");
	}
	bounds = validate_ticket_bounds(dequeue, capacity, &next_dequeue, &reclaim_sequence,
	                               "MpmcQueue::try_pop");
	if (!bounds) {
		release_gate(handle.get(), false);
		return bounds.error();
	}
	if (dequeue == enqueue) {
		const OwnerObservation producer = classify_gate_owner(handle.get(), true);
		const OwnerObservationDisposition producer_state =
		        owner_observation_is_unrecoverable(handle.get(), true, producer);
		release_gate(handle.get(), false);
		if (producer_state == OwnerObservationDisposition::kChanged) {
			return make_error(ErrorCode::kQueueContention, "MpmcQueue::try_pop",
			                  "producer gate changed while probing liveness");
		}
		if (producer_state == OwnerObservationDisposition::kUnrecoverable) {
			return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_pop",
			                  "producer gate has an unrecoverable claim");
		}
		return make_error(ErrorCode::kQueueEmpty, "MpmcQueue::try_pop", "queue is empty");
	}
	auto committed_gate = commit_claimed_gate(handle.get(), false);
	if (!committed_gate) return committed_gate.error();
	EDGE_FAILPOINT(MPMC_DEQUEUE_GATE_OWNED);
	auto* slot = slot_at(handle.get(), dequeue);
	const bool slot_invalid =
	        slot == nullptr || shared_load_acquire(&slot->sequence) != next_dequeue ||
	        shared_load_seq_cst(&slot->state) != static_cast<uint32_t>(MpmcSlotState::kReady);
	if (slot_invalid) {
		if (slot != nullptr) {
			const OwnerObservation slot_owner = snapshot_slot_owner(slot, false);
			if (slot_owner.kind == OwnerObservationKind::kEpochExhausted) {
				release_gate(handle.get(), false);
				return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_pop",
				                  "slot owner epoch is exhausted");
			}
		}
		release_gate(handle.get(), false);
		return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_pop",
		                  "published slot sequence or state is inconsistent");
	}
	const OwnerCommitResult slot_capacity = owner_epoch_capacity(&slot->owner_epoch, 2u);
	if (slot_capacity != OwnerCommitResult::kReady) {
		release_gate(handle.get(), false);
		return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_pop",
		                  slot_capacity == OwnerCommitResult::kEpochExhausted
		                          ? "slot owner epoch exhausted before read"
		                          : "slot owner epoch protocol is corrupt");
	}
	if (!shared_cas_seq_cst(&slot->state, static_cast<uint32_t>(MpmcSlotState::kReady),
	                        static_cast<uint32_t>(MpmcSlotState::kDequeueClaiming))) {
		release_gate(handle.get(), false);
		return make_error(ErrorCode::kQueueContention, "MpmcQueue::try_pop",
		                  "slot was claimed concurrently");
	}
	uint64_t committed_epoch = 0;
	const OwnerCommitResult owner_commit =
	        begin_owner_commit(&slot->owner_epoch, 2u, &committed_epoch);
	if (owner_commit != OwnerCommitResult::kReady) {
		shared_store_seq_cst(&slot->state, static_cast<uint32_t>(MpmcSlotState::kReady));
		release_gate(handle.get(), false);
		return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::try_pop",
		                  owner_commit == OwnerCommitResult::kEpochExhausted
		                          ? "slot owner epoch exhausted"
		                          : "slot owner epoch protocol is corrupt");
	}
	EDGE_FAILPOINT(MPMC_DEQUEUE_SLOT_OWNER_EPOCH_ODD);
	set_owner(&slot->owner, handle->self);
	slot->ticket = dequeue;
	shared_store_seq_cst(&slot->owner_epoch, committed_epoch);
	EDGE_FAILPOINT(MPMC_DEQUEUE_SLOT_OWNER_EPOCH_COMMITTED);
	shared_store_seq_cst(&slot->state, static_cast<uint32_t>(MpmcSlotState::kDequeueOwned));
	EDGE_FAILPOINT(MPMC_DEQUEUE_SLOT_OWNED);
	std::memcpy(payload, payload_at(handle.get(), dequeue), payload_size);
	shared_store_release(&slot->sequence, reclaim_sequence);
	shared_store_seq_cst(&slot->state, static_cast<uint32_t>(MpmcSlotState::kFree));
	shared_store_seq_cst(&handle->header()->dequeue_position, next_dequeue);
	release_gate(handle.get(), false);
	notify_waiters(handle.get());
	return Result<void>::ok();
}

namespace {

bool is_push_waitable(ErrorCode code) noexcept {
	return code == ErrorCode::kQueueFull || code == ErrorCode::kQueueContention;
}

bool is_pop_waitable(ErrorCode code) noexcept {
	return code == ErrorCode::kQueueEmpty || code == ErrorCode::kQueueContention;
}

bool remaining_wait_ns(std::chrono::steady_clock::time_point deadline,
                       uint64_t* remaining_ns) noexcept {
	const auto now = std::chrono::steady_clock::now();
	if (deadline <= now) return false;
	const auto remaining = deadline - now;
	const auto converted = std::chrono::duration_cast<std::chrono::nanoseconds>(remaining);
	if (converted.count() <= 0) {
		*remaining_ns = 1;
		return true;
	}
	*remaining_ns = static_cast<uint64_t>(converted.count());
	return true;
}

timespec relative_wait_timespec(uint64_t remaining_ns) noexcept {
	const uint64_t bounded_ns = std::min(remaining_ns, kMpmcWaitSliceNs);
	timespec timeout{};
	timeout.tv_sec = static_cast<time_t>(bounded_ns / 1'000'000'000ull);
	timeout.tv_nsec = static_cast<long>(bounded_ns % 1'000'000'000ull);
	return timeout;
}

Result<void> wait_on_notify_epoch(MpmcQueueHandle* handle,
                                  std::chrono::steady_clock::time_point deadline,
                                  uint32_t expected) noexcept {
	uint64_t remaining_ns = 0;
	if (!remaining_wait_ns(deadline, &remaining_ns)) {
		return make_error(ErrorCode::kTimeout, "MpmcQueue::wait", "deadline expired");
	}
	const timespec timeout = relative_wait_timespec(remaining_ns);
	handle->diagnostics.add(DiagnosticCounter::kWaits);
	const int rc = futex_wait(&handle->header()->notify_epoch, expected, &timeout);
	if (rc == 0) return Result<void>::ok();
	const int saved_errno = errno;
	if (saved_errno == EAGAIN || saved_errno == EINTR || saved_errno == ETIMEDOUT) {
		return Result<void>::ok();
	}
	return make_errno_error(ErrorCode::kSystemError, saved_errno, "MpmcQueue::wait",
	                        "futex_wait failed");
}

}  // namespace

Result<void> mpmc_try_push_impl(const std::shared_ptr<MpmcQueueHandle>& handle, const void* payload,
                                uint32_t payload_size) noexcept {
	if (handle) handle->diagnostics.add(DiagnosticCounter::kPushAttempts);
	auto result = mpmc_try_push_once(handle, payload, payload_size);
	if (handle) {
		if (result)
			handle->diagnostics.add(DiagnosticCounter::kPushSuccesses);
		else
			handle->diagnostics.record_error(result.error().code);
	}
	return result;
}

Result<void> mpmc_try_pop_impl(const std::shared_ptr<MpmcQueueHandle>& handle, void* payload,
                               uint32_t payload_size) noexcept {
	if (handle) handle->diagnostics.add(DiagnosticCounter::kPopAttempts);
	auto result = mpmc_try_pop_once(handle, payload, payload_size);
	if (handle) {
		if (result)
			handle->diagnostics.add(DiagnosticCounter::kPopSuccesses);
		else
			handle->diagnostics.record_error(result.error().code);
	}
	return result;
}

namespace {

Result<void> wait_until(const std::shared_ptr<MpmcQueueHandle>& input, const void* push_payload,
                        void* pop_payload, uint32_t payload_size,
                        std::chrono::steady_clock::time_point deadline,
                        const CancellationToken& token, bool pushing) noexcept {
	const auto handle = input;
	auto valid = validate_live_handle(handle, "MpmcQueue::wait");
	if (!valid) return valid.error();
	handle->diagnostics.add(DiagnosticCounter::kWaitCalls);
	CancellationRegistration registration(token, &handle->header()->notify_epoch);
	for (;;) {
		const uint32_t before = shared_load_acquire(&handle->header()->notify_epoch);
		auto attempt = pushing ? mpmc_try_push_impl(handle, push_payload, payload_size)
		                       : mpmc_try_pop_impl(handle, pop_payload, payload_size);
		if (attempt) return attempt;
		if (!(pushing ? is_push_waitable(attempt.error().code)
		              : is_pop_waitable(attempt.error().code)))
			return attempt.error();
		if (!pushing && attempt.error().code == ErrorCode::kQueueEmpty) {
			auto budget =
			        dequeue_drain_budget(handle.get(), "MpmcQueue::wait_pop_until");
			if (!budget && budget.error().code != ErrorCode::kQueueContention)
				return budget.error();
			if (budget && budget.value() == 0) {
				return make_error(ErrorCode::kSequenceExhausted, "MpmcQueue::wait_pop_until",
				                  "dequeue gate has no remaining drain budget");
			}
		}
		if (token.is_cancelled()) {
			handle->diagnostics.add(DiagnosticCounter::kCancelled);
			return make_error(ErrorCode::kCancelled, "MpmcQueue::wait",
			                  "wait cancelled");
		}
		if (std::chrono::steady_clock::now() >= deadline) {
			handle->diagnostics.add(DiagnosticCounter::kTimeouts);
			return make_error(ErrorCode::kTimeout, "MpmcQueue::wait",
			                  "deadline expired");
		}
		if (before != shared_load_acquire(&handle->header()->notify_epoch)) continue;
		auto waited = wait_on_notify_epoch(handle.get(), deadline, before);
		if (!waited && waited.error().code != ErrorCode::kTimeout) return waited.error();
	}
}

BatchResult stopped_batch(size_t count, const Error& error) noexcept {
	BatchStopReason reason = BatchStopReason::kError;
	switch (error.code) {
		case ErrorCode::kQueueFull:
			reason = BatchStopReason::kFull;
			break;
		case ErrorCode::kQueueEmpty:
			reason = BatchStopReason::kEmpty;
			break;
		case ErrorCode::kClosed:
			reason = BatchStopReason::kClosed;
			break;
		case ErrorCode::kCancelled:
			reason = BatchStopReason::kCancelled;
			break;
		case ErrorCode::kQueueContention:
			reason = BatchStopReason::kContention;
			break;
		default:
			break;
	}
	return {count, reason, error};
}

BatchResult try_batch(const std::shared_ptr<MpmcQueueHandle>& input, const void* push_payload,
                      void* pop_payload, uint32_t payload_size, size_t count,
                      const CancellationToken& token, bool pushing) noexcept {
	const auto handle = input;
	auto valid = validate_live_handle(handle, "MpmcQueue::try_batch");
	if (!valid) return stopped_batch(0, valid.error());
	handle->diagnostics.add(DiagnosticCounter::kBatchCalls);
	uint64_t total_bytes = 0;
	if (count > kMpmcMaxBatchSize || payload_size == 0 || payload_size > kMpmcMaxPayloadSize ||
	    !checked_mul_u64(count, payload_size, &total_bytes) || total_bytes > SIZE_MAX ||
	    payload_size != handle->header()->payload_size ||
	    (count != 0 && (pushing ? push_payload == nullptr : pop_payload == nullptr))) {
		return stopped_batch(0,
		                     make_error(ErrorCode::kInvalidOptions, "MpmcQueue::try_batch",
		                                "invalid pointer, element size or batch count"));
	}
	for (size_t index = 0; index < count; ++index) {
		if (token.is_cancelled()) {
			handle->diagnostics.add(DiagnosticCounter::kCancelled);
			return stopped_batch(
			        index, make_error(ErrorCode::kCancelled, "MpmcQueue::try_batch",
			                          "batch cancelled"));
		}
		const size_t offset = index * static_cast<size_t>(payload_size);
		auto result =
		        pushing ? mpmc_try_push_impl(
		                          handle,
		                          static_cast<const std::byte*>(push_payload) + offset,
		                          payload_size)
		                : mpmc_try_pop_impl(handle,
		                                    static_cast<std::byte*>(pop_payload) + offset,
		                                    payload_size);
		if (!result) return stopped_batch(index, result.error());
	}
	return {count, BatchStopReason::kComplete, {}};
}

}  // namespace

Result<void> mpmc_wait_push_until_impl(const std::shared_ptr<MpmcQueueHandle>& handle,
                                       const void* payload, uint32_t payload_size,
                                       std::chrono::steady_clock::time_point deadline) noexcept {
	return mpmc_wait_push_until_impl(handle, payload, payload_size, deadline, {});
}

Result<void> mpmc_wait_pop_until_impl(const std::shared_ptr<MpmcQueueHandle>& handle, void* payload,
                                      uint32_t payload_size,
                                      std::chrono::steady_clock::time_point deadline) noexcept {
	return mpmc_wait_pop_until_impl(handle, payload, payload_size, deadline, {});
}

Result<void> mpmc_wait_push_until_impl(const std::shared_ptr<MpmcQueueHandle>& handle,
                                       const void* payload, uint32_t payload_size,
                                       std::chrono::steady_clock::time_point deadline,
                                       const CancellationToken& token) noexcept {
	return wait_until(handle, payload, nullptr, payload_size, deadline, token, true);
}

Result<void> mpmc_wait_pop_until_impl(const std::shared_ptr<MpmcQueueHandle>& handle, void* payload,
                                      uint32_t payload_size,
                                      std::chrono::steady_clock::time_point deadline,
                                      const CancellationToken& token) noexcept {
	return wait_until(handle, nullptr, payload, payload_size, deadline, token, false);
}

Result<void> mpmc_close_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept {
	auto valid = validate_live_handle(handle, "MpmcQueue::close");
	if (!valid) return valid.error();
	handle->diagnostics.add(DiagnosticCounter::kCloseCalls);
	if (!owner_matches(handle->header()->creator, handle->self)) {
		return make_error(ErrorCode::kPermissionDenied, "MpmcQueue::close",
		                  "creator identity required");
	}
	if (shared_load_acquire(&handle->header()->closed) != 0) return {};
	auto gate = acquire_gate(handle.get(), true);
	if (!gate) return gate.error();
	shared_store_release(&handle->header()->closed, uint32_t{1});
	release_gate(handle.get(), true);
	notify_waiters(handle.get());
	return {};
}

BatchResult mpmc_try_push_batch_impl(const std::shared_ptr<MpmcQueueHandle>& handle,
                                     const void* payload, uint32_t payload_size, size_t count,
                                     const CancellationToken& token) noexcept {
	return try_batch(handle, payload, nullptr, payload_size, count, token, true);
}

BatchResult mpmc_try_pop_batch_impl(const std::shared_ptr<MpmcQueueHandle>& handle, void* payload,
                                    uint32_t payload_size, size_t count,
                                    const CancellationToken& token) noexcept {
	return try_batch(handle, nullptr, payload, payload_size, count, token, false);
}

void mpmc_set_diagnostics_impl(const std::shared_ptr<MpmcQueueHandle>& handle,
                               bool enabled) noexcept {
	if (handle) handle->diagnostics.enable(enabled);
}

void mpmc_reset_diagnostics_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept {
	if (handle) handle->diagnostics.reset();
}

DiagnosticsSnapshot mpmc_diagnostics_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept {
	return handle ? handle->diagnostics.snapshot(mpmc_capacity_impl(handle))
	              : DiagnosticsSnapshot{};
}

Result<void> fill_status_owner(const MpmcQueueHandle* handle, bool enqueue,
                               MpmcQueueStatus* status) noexcept {
	const ConstGateRef gate = gate_for(handle, enqueue);
	const OwnerObservation observation = snapshot_gate_owner(gate);
	uint64_t* owner_pid = enqueue ? &status->enqueue_owner_pid : &status->dequeue_owner_pid;
	bool* owner_alive = enqueue ? &status->enqueue_owner_alive : &status->dequeue_owner_alive;
	uint32_t* lock_state = enqueue ? &status->enqueue_lock_state : &status->dequeue_lock_state;
	*lock_state = observation.state;
	*owner_pid = 0;
	*owner_alive = false;
	switch (observation.kind) {
		case OwnerObservationKind::kInactive:
			return Result<void>::ok();
		case OwnerObservationKind::kClaiming:
		case OwnerObservationKind::kUnstable:
			return make_error(ErrorCode::kQueueContention, gate.operation,
			                  "gate owner snapshot is not stable");
		case OwnerObservationKind::kCorrupt:
		case OwnerObservationKind::kEpochExhausted:
			return make_error(ErrorCode::kRecoveryBlocked, gate.operation,
			                  "gate owner protocol is not recoverable");
		case OwnerObservationKind::kStable:
			*owner_pid = observation.owner.pid;
			const OwnerObservationDisposition disposition =
			        confirm_owner_liveness(handle, enqueue, observation, true);
			if (disposition == OwnerObservationDisposition::kChanged) {
				return make_error(ErrorCode::kQueueContention, gate.operation,
				                  "gate owner changed while probing liveness");
			}
			if (observation.owner.boot_id_hash_hi != handle->self.boot_id_hash_hi ||
			    observation.owner.boot_id_hash_lo != handle->self.boot_id_hash_lo) {
				return make_error(ErrorCode::kRecoveryBlocked, gate.operation,
				                  "gate owner belongs to another boot");
			}
			*owner_alive = disposition == OwnerObservationDisposition::kNotUnrecoverable;
			return Result<void>::ok();
	}
	return make_error(ErrorCode::kRecoveryBlocked, gate.operation,
	                  "unknown gate owner observation");
}

Result<MpmcQueueStatus> mpmc_status_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle) noexcept {
	auto valid = validate_live_handle(handle, "MpmcQueue::status");
	if (!valid) return valid.error();
	const auto* header = handle->header();
	MpmcQueueStatus status;
	status.ready = shared_load_acquire(&header->init_state) ==
	               static_cast<uint32_t>(MpmcInitState::kReady);
	status.closed = shared_load_acquire(&header->closed) != 0;
	status.abi_major = header->abi_major;
	status.abi_minor = header->abi_minor;
	status.capacity = header->capacity;
	status.payload_size = header->payload_size;
	status.mapping_size = header->mapping_size;
	status.generation = header->generation;
	status.instance_nonce_hi = header->instance_nonce_hi;
	status.instance_nonce_lo = header->instance_nonce_lo;
	status.enqueue_position = shared_load_acquire(&header->enqueue_position);
	status.dequeue_position = shared_load_acquire(&header->dequeue_position);
	status.enqueue_lock_state = shared_load_seq_cst(&header->enqueue_lock);
	status.dequeue_lock_state = shared_load_seq_cst(&header->dequeue_lock);
	status.enqueue_owner_pid = 0;
	status.dequeue_owner_pid = 0;
	status.enqueue_owner_alive = false;
	status.dequeue_owner_alive = false;
	auto enqueue_status = fill_status_owner(handle.get(), true, &status);
	if (!enqueue_status) return enqueue_status.error();
	auto dequeue_status = fill_status_owner(handle.get(), false, &status);
	if (!dequeue_status) return dequeue_status.error();
	return Result<MpmcQueueStatus>(status);
}

Result<void> mpmc_remove_if_creator_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle) noexcept {
	auto valid = validate_live_handle(handle, "MpmcQueue::remove_if_creator");
	if (!valid) return valid.error();
	if (!owner_matches(handle->header()->creator, handle->self)) {
		return make_error(ErrorCode::kPermissionDenied, "MpmcQueue::remove_if_creator",
		                  "handle is not the creator");
	}
	const bool enqueue_active = shared_load_seq_cst(&handle->header()->enqueue_lock) !=
	                            static_cast<uint32_t>(MpmcLockState::kFree);
	const bool dequeue_active = shared_load_seq_cst(&handle->header()->dequeue_lock) !=
	                            static_cast<uint32_t>(MpmcLockState::kFree);
	if (enqueue_active) {
		const OwnerObservation observation = classify_gate_owner(handle.get(), true);
		const OwnerObservationDisposition disposition =
		        owner_observation_is_unrecoverable(handle.get(), true, observation);
		if (disposition != OwnerObservationDisposition::kUnrecoverable) {
			return make_error(ErrorCode::kQueueContention, "MpmcQueue::remove_if_creator",
			                  "enqueue operation is still active or unstable");
		}
		if (observation.kind == OwnerObservationKind::kCorrupt ||
		    observation.kind == OwnerObservationKind::kEpochExhausted) {
			return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::remove_if_creator",
			                  "enqueue owner protocol is corrupt");
		}
	}
	if (dequeue_active) {
		const OwnerObservation observation = classify_gate_owner(handle.get(), false);
		const OwnerObservationDisposition disposition =
		        owner_observation_is_unrecoverable(handle.get(), false, observation);
		if (disposition != OwnerObservationDisposition::kUnrecoverable) {
			return make_error(ErrorCode::kQueueContention, "MpmcQueue::remove_if_creator",
			                  "dequeue operation is still active or unstable");
		}
		if (observation.kind == OwnerObservationKind::kCorrupt ||
		    observation.kind == OwnerObservationKind::kEpochExhausted) {
			return make_error(ErrorCode::kRecoveryBlocked, "MpmcQueue::remove_if_creator",
			                  "dequeue owner protocol is corrupt");
		}
	}
	auto unlinked = shm_unlink_checked(handle->shm_name, handle->object.dev, handle->object.ino);
	if (!unlinked) return unlinked.error();
	handle->removed = true;
	return Result<void>::ok();
}

uint32_t mpmc_capacity_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept {
	return handle == nullptr || handle->removed ? 0 : handle->header()->capacity;
}

uint64_t mpmc_generation_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept {
	return handle == nullptr || handle->removed ? 0 : handle->header()->generation;
}

}  // namespace edge_runtime::detail
