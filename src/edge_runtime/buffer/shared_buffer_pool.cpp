#include "edge_runtime/buffer/shared_buffer_pool.hpp"

#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/utility/checksum.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/utility/random.hpp"
#include "edge_runtime/utility/failpoint.hpp"
#include "edge_runtime/utility/local_diagnostics.hpp"
#include "edge_runtime/sync/cancellation.hpp"
#include "edge_runtime/sync/futex.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/sync/owner_snapshot.hpp"
#include "edge_runtime/buffer/shared_buffer_pool_layout.hpp"
#include "edge_runtime/transport/shm_object.hpp"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstring>

#include <unistd.h>

namespace edge_runtime::detail {

struct SharedBufferPoolHandle {
	ShmObject object;
	std::string shm_name;
	ProcessIdentity self;
	bool created{false};
	bool removed{false};
	LocalDiagnostics diagnostics;

	SharedBufferPoolHeaderAbi* header() noexcept {
		return static_cast<SharedBufferPoolHeaderAbi*>(object.mapping.get());
	}
	const SharedBufferPoolHeaderAbi* header() const noexcept {
		return static_cast<const SharedBufferPoolHeaderAbi*>(object.mapping.get());
	}
};

namespace {

BufferPoolOwnerAbi owner_from_process(const ProcessIdentity& identity) noexcept {
	return {identity.pid, identity.proc_start_ticks, identity.boot_id_hash_hi,
	        identity.boot_id_hash_lo};
}

bool owner_valid(const BufferPoolOwnerAbi& owner) noexcept {
	return owner.pid != 0 && owner.proc_start_ticks != 0 && owner.boot_id_hash_hi != 0 &&
	       owner.boot_id_hash_lo != 0;
}

bool owner_matches(const BufferPoolOwnerAbi& owner, const ProcessIdentity& identity) noexcept {
	return owner.pid == identity.pid && owner.proc_start_ticks == identity.proc_start_ticks &&
	       owner.boot_id_hash_hi == identity.boot_id_hash_hi &&
	       owner.boot_id_hash_lo == identity.boot_id_hash_lo;
}

void set_owner(BufferPoolOwnerAbi* owner, const ProcessIdentity& identity) noexcept {
	const BufferPoolOwnerAbi value = owner_from_process(identity);
	store_owner_seq_cst(owner, value);
}

OwnerStateKind classify_block_state(uint32_t state) noexcept {
	switch (static_cast<SharedBufferBlockState>(state)) {
		case SharedBufferBlockState::kFree:
		case SharedBufferBlockState::kPublished:
			return OwnerStateKind::kInactive;
		case SharedBufferBlockState::kWritingClaiming:
		case SharedBufferBlockState::kReadingClaiming:
			return OwnerStateKind::kClaiming;
		case SharedBufferBlockState::kWriting:
		case SharedBufferBlockState::kReading:
			return OwnerStateKind::kOwned;
		case SharedBufferBlockState::kEpochExhausted:
			return OwnerStateKind::kEpochExhausted;
		case SharedBufferBlockState::kOwnerProtocolFault:
			return OwnerStateKind::kProtocolFault;
	}
	return OwnerStateKind::kUnexpected;
}

using OwnerObservation = OwnerSnapshot<BufferPoolOwnerAbi>;

OwnerObservation snapshot_block_owner(const SharedBufferBlockHeaderAbi* block) noexcept {
	const auto hook = [](OwnerSnapshotPoint point) noexcept {
		if (point == OwnerSnapshotPoint::kAfterState1) {
			EDGE_FAILPOINT(SHARED_BUFFER_POOL_OWNER_SNAPSHOT_AFTER_STATE1);
		} else {
			EDGE_FAILPOINT(SHARED_BUFFER_POOL_OWNER_SNAPSHOT_AFTER_EPOCH1);
		}
	};
	return snapshot_owner_seq_cst<BufferPoolOwnerAbi>(
	        &block->state, &block->owner_epoch, &block->owner, classify_block_state, owner_valid,
	        hook);
}

uint64_t header_checksum(const SharedBufferPoolHeaderAbi* header) noexcept {
	SharedBufferPoolHeaderAbi copy{};
	auto* destination = reinterpret_cast<std::byte*>(&copy);
	const auto* source = reinterpret_cast<const std::byte*>(header);
	std::memcpy(destination, source, offsetof(SharedBufferPoolHeaderAbi, init_state));
	std::memcpy(&copy.creator, &header->creator, sizeof(copy.creator));
	constexpr size_t reserved_offset = offsetof(SharedBufferPoolHeaderAbi, reserved);
	std::memcpy(destination + reserved_offset, source + reserved_offset,
	            sizeof(copy) - reserved_offset);
	return fnv1a64(reinterpret_cast<const std::byte*>(&copy), sizeof(copy));
}

void publish_free(SharedBufferPoolHandle* handle, SharedBufferBlockHeaderAbi* block) noexcept {
	shared_store_seq_cst(&block->state, static_cast<uint32_t>(SharedBufferBlockState::kFree));
	__atomic_fetch_add(&handle->header()->notify_epoch, uint32_t{1}, __ATOMIC_RELEASE);
	handle->diagnostics.add(DiagnosticCounter::kNotifications);
	(void)futex_wake(&handle->header()->notify_epoch, INT_MAX);
}

bool same_schema(const SharedBufferPoolHeaderAbi* header,
                 const SchemaDescriptor& schema) noexcept {
	return header->schema_version == schema.version &&
	       std::memcmp(header->schema_fingerprint, schema.fingerprint.data(),
	                   schema.fingerprint.size()) == 0;
}

bool handle_schema_matches(const SharedBufferPoolHeaderAbi* header,
                           const BufferHandle& descriptor) noexcept {
	return descriptor.schema_version == header->schema_version &&
	       std::memcmp(descriptor.schema_fingerprint, header->schema_fingerprint,
	                   sizeof(descriptor.schema_fingerprint)) == 0;
}

std::string pool_shm_name(const std::string& pool_name) {
	return "/edgeruntime.bufferpool." + std::to_string(getuid()) + "." + pool_name;
}

SharedBufferBlockHeaderAbi* block_at(SharedBufferPoolHandle* handle,
                                     uint32_t block_id) noexcept {
	uint64_t block_offset = 0;
	uint64_t payload_offset = 0;
	if (!shared_buffer_pool_block_offset_for(handle->header()->block_size, block_id,
	                                         handle->header()->block_count, &block_offset,
	                                         &payload_offset)) {
		return nullptr;
	}
	if (block_offset > handle->object.size ||
	    sizeof(SharedBufferBlockHeaderAbi) > handle->object.size - block_offset) {
		return nullptr;
	}
	return reinterpret_cast<SharedBufferBlockHeaderAbi*>(
	        static_cast<std::byte*>(handle->object.mapping.get()) + block_offset);
}

std::byte* payload_at(SharedBufferPoolHandle* handle, uint32_t block_id) noexcept {
	uint64_t block_offset = 0;
	uint64_t payload_offset = 0;
	if (!shared_buffer_pool_block_offset_for(handle->header()->block_size, block_id,
	                                         handle->header()->block_count, &block_offset,
	                                         &payload_offset)) {
		return nullptr;
	}
	if (payload_offset > handle->object.size ||
	    handle->header()->block_size > handle->object.size - payload_offset) {
		return nullptr;
	}
	return static_cast<std::byte*>(handle->object.mapping.get()) + payload_offset;
}

BufferHandle make_handle(const SharedBufferPoolHandle* handle, uint32_t block_id,
                         const SharedBufferBlockHeaderAbi* block) noexcept {
	BufferHandle descriptor{};
	const auto* header = handle->header();
	uint64_t block_offset = 0;
	uint64_t payload_offset = 0;
	(void)shared_buffer_pool_block_offset_for(header->block_size, block_id, header->block_count,
	                                          &block_offset, &payload_offset);
	descriptor.pool_generation = header->generation;
	descriptor.block_generation = shared_load_seq_cst(&block->block_generation);
	descriptor.instance_nonce_hi = header->instance_nonce_hi;
	descriptor.instance_nonce_lo = header->instance_nonce_lo;
	descriptor.offset = payload_offset;
	descriptor.block_id = block_id;
	descriptor.length = header->block_size;
	descriptor.schema_version = header->schema_version;
	std::memcpy(descriptor.schema_fingerprint, header->schema_fingerprint,
	            sizeof(descriptor.schema_fingerprint));
	return descriptor;
}

Result<void> validate_options(const SharedBufferPoolOptions& options) {
	if (!validate_channel_name(options.name.c_str(), options.name.size()) ||
	    options.name.size() > kSharedBufferPoolMaxNameLength) {
		return make_error(ErrorCode::kInvalidName, "SharedBufferPool::options",
		                  "invalid pool name");
	}
	if (!shared_buffer_pool_block_size_valid(options.block_size) || options.block_count == 0 ||
	    options.block_count > kSharedBufferPoolMaxBlockCount || options.schema.version == 0) {
		return make_error(ErrorCode::kInvalidOptions, "SharedBufferPool::options",
		                  "unsupported block size, count, or schema version");
	}
	return Result<void>::ok();
}

Result<void> validate_header(const SharedBufferPoolHeaderAbi* header, uint64_t actual_size,
                             const SharedBufferPoolOptions& options) {
	if (std::memcmp(header->magic, kSharedBufferPoolLegacyMagic, sizeof(header->magic)) == 0) {
		return make_error(ErrorCode::kAbiMismatch, "SharedBufferPool::open",
		                  "legacy pool ABI requires recreation");
	}
	if (std::memcmp(header->magic, kSharedBufferPoolMagic, sizeof(header->magic)) != 0) {
		return make_error(ErrorCode::kCorruptHeader, "SharedBufferPool::open",
		                  "magic mismatch");
	}
	if (header->abi_major != kSharedBufferPoolAbiMajor ||
	    header->abi_minor > kSharedBufferPoolAbiMinor) {
		return make_error(ErrorCode::kAbiMismatch, "SharedBufferPool::open",
		                  "unsupported ABI version");
	}
	if (header->header_size != sizeof(SharedBufferPoolHeaderAbi) ||
	    header->endian_marker != kSharedBufferPoolEndianMarker) {
		return make_error(ErrorCode::kCorruptHeader, "SharedBufferPool::open",
		                  "header shape mismatch");
	}
	if (!shared_buffer_pool_block_size_valid(header->block_size) ||
	    header->block_count == 0 ||
	    header->block_count > kSharedBufferPoolMaxBlockCount || header->generation == 0 ||
	    (header->instance_nonce_hi == 0 && header->instance_nonce_lo == 0) ||
	    header->schema_version == 0 || !owner_valid(header->creator)) {
		return make_error(ErrorCode::kCorruptHeader, "SharedBufferPool::open",
		                  "invalid header values");
	}
	uint64_t expected_size = 0;
	if (!shared_buffer_pool_mapping_size_for(header->block_size, header->block_count,
	                                         &expected_size) ||
	    expected_size != header->mapping_size || actual_size != header->mapping_size) {
		return make_error(ErrorCode::kCorruptHeader, "SharedBufferPool::open",
		                  "mapping size mismatch");
	}
	const uint32_t init = shared_load_acquire(&header->init_state);
	if (init != static_cast<uint32_t>(SharedBufferPoolInitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, "SharedBufferPool::open",
		                  "pool is not ready");
	}
	if (header->header_checksum != header_checksum(header)) {
		return make_error(ErrorCode::kCorruptHeader, "SharedBufferPool::open",
		                  "header checksum mismatch");
	}
	if (header->block_size != options.block_size || header->block_count != options.block_count) {
		return make_error(ErrorCode::kInvalidOptions, "SharedBufferPool::open",
		                  "block layout mismatch");
	}
	if (!same_schema(header, options.schema)) {
		return make_error(ErrorCode::kSchemaMismatch, "SharedBufferPool::open",
		                  "schema mismatch");
	}
	return Result<void>::ok();
}

Result<void> validate_live_pool(const std::shared_ptr<SharedBufferPoolHandle>& handle,
                                const char* operation) noexcept {
	if (!handle || handle->removed || handle->object.mapping.get() == nullptr) {
		return make_error(ErrorCode::kStaleHandle, operation, "pool handle is stale");
	}
	if (static_cast<uint64_t>(::getpid()) != handle->self.pid) {
		return make_error(ErrorCode::kRecoveryBlocked, operation,
		                  "pool handle inherited across fork; reopen in this process");
	}
	if (shared_load_acquire(&handle->header()->init_state) !=
	    static_cast<uint32_t>(SharedBufferPoolInitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, operation,
		                  "pool is not ready");
	}
	return Result<void>::ok();
}

Result<void> validate_descriptor(const std::shared_ptr<SharedBufferPoolHandle>& handle,
                                 const BufferHandle& descriptor,
                                 const char* operation) noexcept {
	auto live = validate_live_pool(handle, operation);
	if (!live) return live.error();
	const auto* header = handle->header();
	if (descriptor.pool_generation != header->generation ||
	    descriptor.instance_nonce_hi != header->instance_nonce_hi ||
	    descriptor.instance_nonce_lo != header->instance_nonce_lo) {
		return make_error(ErrorCode::kBufferInvalidHandle, operation,
		                  "pool generation or instance nonce mismatch");
	}
	if (!handle_schema_matches(header, descriptor)) {
		return make_error(ErrorCode::kSchemaMismatch, operation, "buffer schema mismatch");
	}
	if (descriptor.reserved != 0 || descriptor.block_id >= header->block_count ||
	    descriptor.length != header->block_size) {
		return make_error(ErrorCode::kBufferInvalidHandle, operation,
		                  "block id or length is invalid");
	}
	uint64_t expected_block_offset = 0;
	uint64_t expected_payload_offset = 0;
	if (!shared_buffer_pool_block_offset_for(header->block_size, descriptor.block_id,
	                                         header->block_count, &expected_block_offset,
	                                         &expected_payload_offset) ||
	    descriptor.offset != expected_payload_offset || descriptor.block_generation == 0) {
		return make_error(ErrorCode::kBufferInvalidHandle, operation,
		                  "block offset or generation is invalid");
	}
	const auto* block = block_at(handle.get(), descriptor.block_id);
	if (block == nullptr ||
	    shared_load_seq_cst(&block->block_generation) != descriptor.block_generation) {
		return make_error(ErrorCode::kBufferInvalidHandle, operation,
		                  "block generation is stale");
	}
	return Result<void>::ok();
}

Result<void> classify_active_block(const SharedBufferPoolHandle* handle,
                                   const SharedBufferBlockHeaderAbi* block, uint32_t state,
                                   const char* operation) noexcept {
	(void)state;
	const OwnerObservation observation = snapshot_block_owner(block);
	switch (observation.kind) {
		case OwnerSnapshotKind::kInactive:
		case OwnerSnapshotKind::kClaiming:
		case OwnerSnapshotKind::kUnstable:
			return make_error(ErrorCode::kBufferPoolContention, operation,
			                  "block owner snapshot is not stable");
		case OwnerSnapshotKind::kCorrupt:
		case OwnerSnapshotKind::kEpochExhausted:
			return make_error(ErrorCode::kRecoveryBlocked, operation,
			                  "block owner protocol is not recoverable");
		case OwnerSnapshotKind::kStable:
			break;
	}
	if (owner_matches(observation.owner, handle->self)) {
		return make_error(ErrorCode::kConcurrentHandleUse, operation,
		                  "block is owned by this process");
	}
	if (observation.owner.boot_id_hash_hi != handle->self.boot_id_hash_hi ||
	    observation.owner.boot_id_hash_lo != handle->self.boot_id_hash_lo) {
		return make_error(ErrorCode::kRecoveryBlocked, operation,
		                  "block owner belongs to another boot");
	}
	const Liveness liveness =
	        probe_liveness(observation.owner.pid, observation.owner.proc_start_ticks);
	if (liveness == Liveness::kAlive) {
		return make_error(ErrorCode::kBufferPoolContention, operation,
		                  "block is owned by a live process");
	}
	return make_error(ErrorCode::kRecoveryBlocked, operation,
	                  "block owner is dead or unverifiable");
}

Result<void> cleanup_created(const std::string& shm_name, ShmObject* object) noexcept {
	const uint64_t dev = object->dev;
	const uint64_t ino = object->ino;
	object->mapping.reset();
	object->fd.reset();
	if (dev == 0 || ino == 0) return Result<void>::ok();
	return shm_unlink_checked(shm_name, dev, ino);
}

}  // namespace

namespace {

Result<std::shared_ptr<SharedBufferPoolHandle>> open_existing(
        const SharedBufferPoolOptions& options) {
	auto identity = read_current_process_identity();
	if (!identity) return identity.error();
	const std::string shm_name = pool_shm_name(options.name);
	auto fd = shm_open_existing(shm_name);
	if (!fd) return fd.error();
	ShmObject object;
	object.name = shm_name;
	object.fd = std::move(fd.value());
	auto stat = shm_fstat_and_capture(object.fd, &object.dev, &object.ino, &object.size);
	if (!stat) return stat.error();
	if (object.size < sizeof(SharedBufferPoolHeaderAbi)) {
		return make_error(ErrorCode::kInitializationIncomplete, "SharedBufferPool::open",
		                  "object is smaller than the pool header");
	}
	auto mapping = mmap_region(object.fd, object.size);
	if (!mapping) return mapping.error();
	object.mapping = std::move(mapping.value());
	const auto* header = static_cast<const SharedBufferPoolHeaderAbi*>(object.mapping.get());
	const uint32_t init = shared_load_acquire(&header->init_state);
	if (init != static_cast<uint32_t>(SharedBufferPoolInitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, "SharedBufferPool::open",
		                  "creator did not commit READY");
	}
	auto valid = validate_header(header, object.size, options);
	if (!valid) return valid.error();
	auto handle = std::make_shared<SharedBufferPoolHandle>();
	handle->object = std::move(object);
	handle->shm_name = shm_name;
	handle->self = identity.value();
	return Result<std::shared_ptr<SharedBufferPoolHandle>>(std::move(handle));
}

}  // namespace

Result<std::shared_ptr<SharedBufferPoolHandle>> shared_buffer_pool_create_impl(
        const SharedBufferPoolOptions& options) {
	auto valid = validate_options(options);
	if (!valid) return valid.error();
	auto identity = read_current_process_identity();
	if (!identity) return identity.error();
	uint64_t mapping_size = 0;
	if (!shared_buffer_pool_mapping_size_for(options.block_size, options.block_count,
	                                         &mapping_size)) {
		return make_error(ErrorCode::kInvalidOptions, "SharedBufferPool::create",
		                  "mapping size overflow");
	}
	const std::string shm_name = pool_shm_name(options.name);
	auto fd = shm_open_create(shm_name);
	if (!fd) return fd.error();
	ShmObject object;
	object.name = shm_name;
	object.fd = std::move(fd.value());
	auto stat = shm_fstat_and_capture(object.fd, &object.dev, &object.ino, &object.size);
	if (!stat) {
		auto captured = shm_capture_created_identity(object.fd, &object.dev, &object.ino);
		if (!captured) return captured.error();
		auto removed = shm_unlink_checked(shm_name, object.dev, object.ino);
		if (!removed && removed.error().code != ErrorCode::kNotFound)
			return removed.error();
		return stat.error();
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
	std::memset(object.mapping.get(), 0, static_cast<size_t>(mapping_size));
	const ProcessIdentity self = identity.value();
	uint64_t nonce_hi = 0;
	uint64_t nonce_lo = 0;
	if (!owner_valid(owner_from_process(self)) || !random_bytes(&nonce_hi, sizeof(nonce_hi)) ||
	    !random_bytes(&nonce_lo, sizeof(nonce_lo)) || (nonce_hi == 0 && nonce_lo == 0)) {
		(void)cleanup_created(shm_name, &object);
		return make_error(ErrorCode::kSystemError, "SharedBufferPool::create",
		                  "identity or nonce failed");
	}
	auto* header = static_cast<SharedBufferPoolHeaderAbi*>(object.mapping.get());
	std::memcpy(header->magic, kSharedBufferPoolMagic, sizeof(header->magic));
	header->abi_major = kSharedBufferPoolAbiMajor;
	header->abi_minor = kSharedBufferPoolAbiMinor;
	header->endian_marker = kSharedBufferPoolEndianMarker;
	header->header_size = sizeof(SharedBufferPoolHeaderAbi);
	header->block_size = options.block_size;
	header->block_count = options.block_count;
	header->mapping_size = mapping_size;
	header->generation = 1;
	header->instance_nonce_hi = nonce_hi;
	header->instance_nonce_lo = nonce_lo;
	std::memcpy(header->schema_fingerprint, options.schema.fingerprint.data(),
	            sizeof(header->schema_fingerprint));
	header->schema_version = options.schema.version;
	header->creator = owner_from_process(self);
	shared_store_relaxed(&header->init_state,
	                     static_cast<uint32_t>(SharedBufferPoolInitState::kInitializing));
	for (uint32_t block_id = 0; block_id < options.block_count; ++block_id) {
		uint64_t block_offset = 0;
		uint64_t payload_offset = 0;
		if (!shared_buffer_pool_block_offset_for(options.block_size, block_id,
		                                         options.block_count, &block_offset,
		                                         &payload_offset)) {
			(void)cleanup_created(shm_name, &object);
			return make_error(ErrorCode::kInvalidOptions, "SharedBufferPool::create",
			                  "block offset overflow");
		}
		auto* block_header = reinterpret_cast<SharedBufferBlockHeaderAbi*>(
		        static_cast<std::byte*>(object.mapping.get()) + block_offset);
		shared_store_seq_cst(&block_header->state,
		                     static_cast<uint32_t>(SharedBufferBlockState::kFree));
		shared_store_seq_cst(&block_header->block_generation, uint64_t{0});
		shared_store_seq_cst(&block_header->owner_epoch, uint64_t{0});
	}
	header->header_checksum = header_checksum(header);
	shared_store_release(&header->init_state,
	                     static_cast<uint32_t>(SharedBufferPoolInitState::kReady));
	auto handle = std::make_shared<SharedBufferPoolHandle>();
	handle->object = std::move(object);
	handle->shm_name = shm_name;
	handle->self = self;
	handle->created = true;
	return Result<std::shared_ptr<SharedBufferPoolHandle>>(std::move(handle));
}

Result<std::shared_ptr<SharedBufferPoolHandle>> shared_buffer_pool_open_impl(
        const SharedBufferPoolOptions& options) {
	auto valid = validate_options(options);
	if (!valid) return valid.error();
	return open_existing(options);
}

Result<WriteBuffer> shared_buffer_pool_try_acquire_write_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept {
	if (handle) handle->diagnostics.add(DiagnosticCounter::kAcquireAttempts);
	const auto acquire_once = [&handle]() noexcept -> Result<WriteBuffer> {
		auto valid = validate_live_pool(handle, "SharedBufferPool::try_acquire_write");
		if (!valid) return valid.error();
		bool saw_contention = false;
		bool saw_recovery = false;
		bool saw_exhausted = false;
		const auto* header = handle->header();
		for (uint32_t block_id = 0; block_id < header->block_count; ++block_id) {
			auto* block = block_at(handle.get(), block_id);
			if (block == nullptr) {
				return make_error(ErrorCode::kCorruptSlot,
				                  "SharedBufferPool::try_acquire_write",
				                  "block offset invalid");
			}
			const uint32_t state = shared_load_seq_cst(&block->state);
			if (state == static_cast<uint32_t>(SharedBufferBlockState::kPublished))
				continue;
			if (state != static_cast<uint32_t>(SharedBufferBlockState::kFree)) {
				if (state == static_cast<uint32_t>(
				                     SharedBufferBlockState::kEpochExhausted)) {
					saw_exhausted = true;
					continue;
				}
				auto busy = classify_active_block(
				        handle.get(), block, state,
				        "SharedBufferPool::try_acquire_write");
				if (busy.error().code == ErrorCode::kRecoveryBlocked)
					saw_recovery = true;
				if (busy.error().code == ErrorCode::kBufferPoolContention ||
				    busy.error().code == ErrorCode::kConcurrentHandleUse) {
					saw_contention = true;
				}
				continue;
			}
			if (!shared_cas_seq_cst(
			            &block->state,
			            static_cast<uint32_t>(SharedBufferBlockState::kFree),
			            static_cast<uint32_t>(
			                    SharedBufferBlockState::kWritingClaiming))) {
				saw_contention = true;
				continue;
			}
			const uint64_t previous_generation =
			        shared_load_seq_cst(&block->block_generation);
			if (previous_generation == UINT64_MAX) {
				publish_free(handle.get(), block);
				saw_exhausted = true;
				continue;
			}
			const OwnerCommitResult epoch_capacity =
			        owner_epoch_capacity(&block->owner_epoch, 4u);
			if (epoch_capacity != OwnerCommitResult::kReady) {
				shared_store_seq_cst(
				        &block->state,
				        epoch_capacity == OwnerCommitResult::kEpochExhausted
				                ? static_cast<uint32_t>(
				                          SharedBufferBlockState::kEpochExhausted)
				                : static_cast<uint32_t>(
				                          SharedBufferBlockState::
				                                  kOwnerProtocolFault));
				if (epoch_capacity == OwnerCommitResult::kEpochExhausted) {
					saw_exhausted = true;
				} else {
					saw_recovery = true;
				}
				continue;
			}
			uint64_t committed_epoch = 0;
			const OwnerCommitResult owner_commit =
			        begin_owner_commit(&block->owner_epoch, 4u, &committed_epoch);
			if (owner_commit != OwnerCommitResult::kReady) {
				shared_store_seq_cst(
				        &block->state,
				        owner_commit == OwnerCommitResult::kEpochExhausted
				                ? static_cast<uint32_t>(
				                          SharedBufferBlockState::kEpochExhausted)
				                : static_cast<uint32_t>(
				                          SharedBufferBlockState::
				                                  kOwnerProtocolFault));
				if (owner_commit == OwnerCommitResult::kEpochExhausted) {
					saw_exhausted = true;
				} else {
					saw_recovery = true;
				}
				continue;
			}
			EDGE_FAILPOINT(SHARED_BUFFER_POOL_WRITE_OWNER_EPOCH_ODD);
			shared_store_seq_cst(&block->block_generation, previous_generation + 1);
			set_owner(&block->owner, handle->self);
			shared_store_seq_cst(&block->owner_epoch, committed_epoch);
			EDGE_FAILPOINT(SHARED_BUFFER_POOL_WRITE_OWNER_EPOCH_COMMITTED);
			shared_store_seq_cst(
			        &block->state,
			        static_cast<uint32_t>(SharedBufferBlockState::kWriting));
			auto* payload = payload_at(handle.get(), block_id);
			if (payload == nullptr) {
				publish_free(handle.get(), block);
				return make_error(ErrorCode::kCorruptSlot,
				                  "SharedBufferPool::try_acquire_write",
				                  "payload offset invalid");
			}
			const BufferHandle descriptor = make_handle(handle.get(), block_id, block);
			return Result<WriteBuffer>(WriteBuffer(handle, block, payload,
			                                       header->block_size, descriptor));
		}
		if (saw_recovery) {
			return make_error(ErrorCode::kRecoveryBlocked,
			                  "SharedBufferPool::try_acquire_write",
			                  "an active block owner is dead or unverifiable");
		}
		if (saw_exhausted) {
			return make_error(
			        ErrorCode::kSequenceExhausted,
			        "SharedBufferPool::try_acquire_write",
			        "all writable blocks exhausted their owner or generation epochs");
		}
		if (saw_contention) {
			return make_error(ErrorCode::kBufferPoolContention,
			                  "SharedBufferPool::try_acquire_write",
			                  "all writable blocks are actively owned");
		}
		return make_error(ErrorCode::kBufferPoolFull, "SharedBufferPool::try_acquire_write",
		                  "all blocks are published or unavailable");
	};
	auto result = acquire_once();
	if (handle) {
		if (result)
			handle->diagnostics.add(DiagnosticCounter::kAcquireSuccesses);
		else
			handle->diagnostics.record_error(result.error().code);
	}
	return result;
}

Result<WriteBuffer> shared_buffer_pool_wait_acquire_write_until_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle,
        std::chrono::steady_clock::time_point deadline, const CancellationToken& token) noexcept {
	if (handle) handle->diagnostics.add(DiagnosticCounter::kWaitCalls);
	auto valid = validate_live_pool(handle, "SharedBufferPool::wait_acquire_write_until");
	if (!valid) return valid.error();
	auto* epoch = &handle->header()->notify_epoch;
	CancellationRegistration registration(token, epoch);
	for (;;) {
		const uint32_t expected = shared_load_acquire(epoch);
		auto result = shared_buffer_pool_try_acquire_write_impl(handle);
		if (result || (result.error().code != ErrorCode::kBufferPoolFull &&
		               result.error().code != ErrorCode::kBufferPoolContention)) {
			return result;
		}
		if (token.is_cancelled()) {
			handle->diagnostics.add(DiagnosticCounter::kCancelled);
			return make_error(ErrorCode::kCancelled,
			                  "SharedBufferPool::wait_acquire_write_until",
			                  "write acquisition cancelled");
		}
		const auto now = std::chrono::steady_clock::now();
		if (now >= deadline) {
			handle->diagnostics.add(DiagnosticCounter::kTimeouts);
			return make_error(ErrorCode::kTimeout,
			                  "SharedBufferPool::wait_acquire_write_until",
			                  "write acquisition deadline expired");
		}
		const std::chrono::steady_clock::duration max_slice = std::chrono::milliseconds(10);
		const auto slice = std::min(deadline - now, max_slice);
		const auto nanoseconds =
		        std::chrono::duration_cast<std::chrono::nanoseconds>(slice);
		const timespec relative{0, static_cast<long>(nanoseconds.count())};
		handle->diagnostics.add(DiagnosticCounter::kWaits);
		if (futex_wait(epoch, expected, &relative) < 0) {
			const int saved_errno = errno;
			if (saved_errno != EAGAIN && saved_errno != EINTR &&
			    saved_errno != ETIMEDOUT) {
				return make_errno_error(
				        saved_errno, "SharedBufferPool::wait_acquire_write_until",
				        "futex wait failed");
			}
		}
	}
}

void shared_buffer_pool_set_diagnostics_enabled_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle, bool enabled) noexcept {
	if (handle) handle->diagnostics.enable(enabled);
}

void shared_buffer_pool_reset_diagnostics_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept {
	if (handle) handle->diagnostics.reset();
}

DiagnosticsSnapshot shared_buffer_pool_diagnostics_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept {
	if (!handle) return {};
	return handle->diagnostics.snapshot(shared_buffer_pool_block_count_impl(handle));
}

Result<BufferHandle> shared_buffer_pool_publish_write_impl(WriteBuffer* buffer) noexcept {
	if (buffer == nullptr || !buffer->active_ || !buffer->pool_ || buffer->block_ == nullptr) {
		return make_error(ErrorCode::kInvalidOptions, "WriteBuffer::publish", "buffer inactive");
	}
	auto valid = validate_live_pool(buffer->pool_, "WriteBuffer::publish");
	if (!valid) return valid.error();
	auto* block = static_cast<SharedBufferBlockHeaderAbi*>(buffer->block_);
	const uint32_t state = shared_load_seq_cst(&block->state);
	if (state != static_cast<uint32_t>(SharedBufferBlockState::kWriting)) {
		return make_error(ErrorCode::kBufferStateMismatch, "WriteBuffer::publish",
		                  "block is not WRITING");
	}
	const OwnerObservation observation = snapshot_block_owner(block);
	if (observation.kind != OwnerSnapshotKind::kStable) {
		return observation.kind == OwnerSnapshotKind::kClaiming ||
		                       observation.kind == OwnerSnapshotKind::kUnstable
		               ? make_error(ErrorCode::kBufferPoolContention, "WriteBuffer::publish",
		                            "write owner snapshot is not stable")
		               : make_error(ErrorCode::kRecoveryBlocked, "WriteBuffer::publish",
		                            "write owner protocol is not recoverable");
	}
	if (!owner_matches(observation.owner, buffer->pool_->self)) {
		return make_error(ErrorCode::kRecoveryBlocked, "WriteBuffer::publish",
		                  "write owner identity changed");
	}
	if (shared_load_seq_cst(&block->block_generation) != buffer->descriptor_.block_generation) {
		return make_error(ErrorCode::kBufferInvalidHandle, "WriteBuffer::publish",
		                  "block generation changed");
	}
	shared_store_seq_cst(&block->state,
	                     static_cast<uint32_t>(SharedBufferBlockState::kPublished));
	const BufferHandle descriptor = buffer->descriptor_;
	buffer->active_ = false;
	buffer->block_ = nullptr;
	buffer->data_ = nullptr;
	buffer->pool_.reset();
	return Result<BufferHandle>(descriptor);
}

void shared_buffer_pool_abort_write_impl(WriteBuffer* buffer) noexcept {
	if (buffer == nullptr || !buffer->active_) return;
	if (buffer->pool_ && buffer->block_ != nullptr && !buffer->pool_->removed &&
	    static_cast<uint64_t>(::getpid()) == buffer->pool_->self.pid) {
		auto* block = static_cast<SharedBufferBlockHeaderAbi*>(buffer->block_);
		const OwnerObservation observation = snapshot_block_owner(block);
		if (observation.kind == OwnerSnapshotKind::kStable &&
		    observation.state == static_cast<uint32_t>(SharedBufferBlockState::kWriting) &&
		    owner_matches(observation.owner, buffer->pool_->self) &&
		    shared_load_seq_cst(&block->block_generation) ==
		            buffer->descriptor_.block_generation) {
			publish_free(buffer->pool_.get(), block);
		}
	}
	buffer->active_ = false;
	buffer->block_ = nullptr;
	buffer->data_ = nullptr;
	buffer->pool_.reset();
}

Result<ReadBuffer> shared_buffer_pool_acquire_read_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle,
        const BufferHandle& descriptor) noexcept {
	auto valid = validate_descriptor(handle, descriptor, "SharedBufferPool::acquire_read");
	if (!valid) return valid.error();
	auto* block = block_at(handle.get(), descriptor.block_id);
	auto* payload = payload_at(handle.get(), descriptor.block_id);
	if (block == nullptr || payload == nullptr) {
		return make_error(ErrorCode::kCorruptSlot, "SharedBufferPool::acquire_read",
		                  "block offset invalid");
	}
	const uint32_t state = shared_load_seq_cst(&block->state);
	if (state != static_cast<uint32_t>(SharedBufferBlockState::kPublished)) {
		if (state == static_cast<uint32_t>(SharedBufferBlockState::kFree)) {
			return make_error(ErrorCode::kBufferStateMismatch, "SharedBufferPool::acquire_read",
			                  "block is not PUBLISHED");
		}
		return classify_active_block(handle.get(), block, state,
		                             "SharedBufferPool::acquire_read")
		        .error();
	}
	if (shared_load_seq_cst(&block->block_generation) != descriptor.block_generation) {
		return make_error(ErrorCode::kBufferInvalidHandle, "SharedBufferPool::acquire_read",
		                  "block generation changed");
	}
	EDGE_FAILPOINT(SHARED_BUFFER_POOL_READ_GENERATION_CHECKED);
	if (!shared_cas_seq_cst(&block->state,
	                       static_cast<uint32_t>(SharedBufferBlockState::kPublished),
	                       static_cast<uint32_t>(SharedBufferBlockState::kReadingClaiming))) {
		return make_error(ErrorCode::kBufferPoolContention, "SharedBufferPool::acquire_read",
		                  "published block was claimed concurrently");
	}
	if (shared_load_seq_cst(&block->block_generation) != descriptor.block_generation) {
		// This reader already won the PUBLISHED -> READING_CLAIMING transition; the protocol
		// forbids a third party from changing that intermediate state. The release-CAS is only
		// a defensive check before rollback and never overwrites an owner.
		const bool restored = shared_cas_seq_cst(
		        &block->state, static_cast<uint32_t>(SharedBufferBlockState::kReadingClaiming),
		        static_cast<uint32_t>(SharedBufferBlockState::kPublished));
		if (!restored) {
			return make_error(ErrorCode::kRecoveryBlocked, "SharedBufferPool::acquire_read",
			                  "generation rollback lost the reading claim");
		}
		return make_error(ErrorCode::kBufferInvalidHandle, "SharedBufferPool::acquire_read",
		                  "block generation changed after read claim");
	}
	const OwnerCommitResult epoch_capacity = owner_epoch_capacity(&block->owner_epoch, 2u);
	if (epoch_capacity != OwnerCommitResult::kReady) {
		const bool restored = shared_cas_seq_cst(
		        &block->state, static_cast<uint32_t>(SharedBufferBlockState::kReadingClaiming),
		        static_cast<uint32_t>(SharedBufferBlockState::kPublished));
		if (!restored) {
			return make_error(ErrorCode::kRecoveryBlocked, "SharedBufferPool::acquire_read",
			                  "reader epoch rollback lost the reading claim");
		}
		return epoch_capacity == OwnerCommitResult::kEpochExhausted
		               ? make_error(ErrorCode::kSequenceExhausted,
		                            "SharedBufferPool::acquire_read",
		                            "reader owner epoch exhausted; payload remains published")
		               : make_error(ErrorCode::kRecoveryBlocked, "SharedBufferPool::acquire_read",
		                            "reader owner epoch protocol is corrupt");
	}
	uint64_t committed_epoch = 0;
	const OwnerCommitResult owner_commit =
	        begin_owner_commit(&block->owner_epoch, 2u, &committed_epoch);
	if (owner_commit != OwnerCommitResult::kReady) {
		const bool restored = shared_cas_seq_cst(
		        &block->state, static_cast<uint32_t>(SharedBufferBlockState::kReadingClaiming),
		        static_cast<uint32_t>(SharedBufferBlockState::kPublished));
		if (!restored) {
			return make_error(ErrorCode::kRecoveryBlocked, "SharedBufferPool::acquire_read",
			                  "reader epoch rollback lost the reading claim");
		}
		return owner_commit == OwnerCommitResult::kEpochExhausted
		               ? make_error(ErrorCode::kSequenceExhausted,
		                            "SharedBufferPool::acquire_read",
		                            "reader owner epoch exhausted; payload remains published")
		               : make_error(ErrorCode::kRecoveryBlocked, "SharedBufferPool::acquire_read",
		                            "reader owner epoch protocol is corrupt");
	}
	EDGE_FAILPOINT(SHARED_BUFFER_POOL_READ_OWNER_EPOCH_ODD);
	set_owner(&block->owner, handle->self);
	shared_store_seq_cst(&block->owner_epoch, committed_epoch);
	EDGE_FAILPOINT(SHARED_BUFFER_POOL_READ_OWNER_EPOCH_COMMITTED);
	shared_store_seq_cst(&block->state,
	                     static_cast<uint32_t>(SharedBufferBlockState::kReading));
	return Result<ReadBuffer>(
	        ReadBuffer(handle, block, payload, descriptor.length, descriptor));
}

void shared_buffer_pool_release_read_impl(ReadBuffer* buffer) noexcept {
	if (buffer == nullptr || !buffer->active_) return;
	if (buffer->pool_ && buffer->block_ != nullptr && !buffer->pool_->removed &&
	    static_cast<uint64_t>(::getpid()) == buffer->pool_->self.pid) {
		auto* block = static_cast<SharedBufferBlockHeaderAbi*>(buffer->block_);
		const OwnerObservation observation = snapshot_block_owner(block);
		if (observation.kind == OwnerSnapshotKind::kStable &&
		    observation.state == static_cast<uint32_t>(SharedBufferBlockState::kReading) &&
		    owner_matches(observation.owner, buffer->pool_->self) &&
		    shared_load_seq_cst(&block->block_generation) ==
		            buffer->descriptor_.block_generation) {
			publish_free(buffer->pool_.get(), block);
		}
	}
	buffer->active_ = false;
	buffer->block_ = nullptr;
	buffer->data_ = nullptr;
	buffer->pool_.reset();
}

Result<SharedBufferPoolStatus> shared_buffer_pool_status_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept {
	auto valid = validate_live_pool(handle, "SharedBufferPool::status");
	if (!valid) return valid.error();
	const auto* header = handle->header();
	SharedBufferPoolStatus status;
	status.ready = true;
	status.abi_major = header->abi_major;
	status.abi_minor = header->abi_minor;
	status.block_size = header->block_size;
	status.block_count = header->block_count;
	status.mapping_size = header->mapping_size;
	status.generation = header->generation;
	status.instance_nonce_hi = header->instance_nonce_hi;
	status.instance_nonce_lo = header->instance_nonce_lo;
	status.creator_pid = header->creator.pid;
	status.creator_alive =
	        owner_valid(header->creator) &&
	        header->creator.boot_id_hash_hi == handle->self.boot_id_hash_hi &&
	        header->creator.boot_id_hash_lo == handle->self.boot_id_hash_lo &&
	        probe_liveness(header->creator.pid, header->creator.proc_start_ticks) ==
	                Liveness::kAlive;
	for (uint32_t block_id = 0; block_id < header->block_count; ++block_id) {
		const auto* block = block_at(handle.get(), block_id);
		if (block == nullptr) {
			return make_error(ErrorCode::kCorruptSlot, "SharedBufferPool::status",
			                  "block offset invalid");
		}
		const uint32_t state = shared_load_seq_cst(&block->state);
		switch (state) {
		case static_cast<uint32_t>(SharedBufferBlockState::kFree):
			++status.free_blocks;
			break;
		case static_cast<uint32_t>(SharedBufferBlockState::kPublished):
			++status.published_blocks;
			break;
		case static_cast<uint32_t>(SharedBufferBlockState::kWriting):
		case static_cast<uint32_t>(SharedBufferBlockState::kReading): {
			const OwnerObservation observation = snapshot_block_owner(block);
			switch (observation.kind) {
				case OwnerSnapshotKind::kStable:
					switch (static_cast<SharedBufferBlockState>(observation.state)) {
						case SharedBufferBlockState::kWriting:
							++status.writing_blocks;
							break;
						case SharedBufferBlockState::kReading:
							++status.reading_blocks;
							break;
						default:
							return make_error(ErrorCode::kCorruptSlot,
							                  "SharedBufferPool::status",
							                  "stable owner snapshot has invalid state");
					}
					if (observation.owner.boot_id_hash_hi != handle->self.boot_id_hash_hi ||
					    observation.owner.boot_id_hash_lo != handle->self.boot_id_hash_lo ||
					    probe_liveness(observation.owner.pid, observation.owner.proc_start_ticks) !=
					            Liveness::kAlive) {
						++status.recovery_blocked_blocks;
					}
					break;
				case OwnerSnapshotKind::kInactive:
				case OwnerSnapshotKind::kClaiming:
				case OwnerSnapshotKind::kUnstable:
					return make_error(ErrorCode::kBufferPoolContention,
					                  "SharedBufferPool::status",
					                  "block owner snapshot is not stable");
				case OwnerSnapshotKind::kCorrupt:
				case OwnerSnapshotKind::kEpochExhausted:
					return make_error(ErrorCode::kRecoveryBlocked,
					                  "SharedBufferPool::status",
					                  "block owner protocol is not recoverable");
			}
			break;
		}
		case static_cast<uint32_t>(SharedBufferBlockState::kWritingClaiming):
		case static_cast<uint32_t>(SharedBufferBlockState::kReadingClaiming):
			return make_error(ErrorCode::kBufferPoolContention, "SharedBufferPool::status",
			                  "block owner claim is being committed");
		case static_cast<uint32_t>(SharedBufferBlockState::kEpochExhausted):
		case static_cast<uint32_t>(SharedBufferBlockState::kOwnerProtocolFault):
			return make_error(ErrorCode::kRecoveryBlocked, "SharedBufferPool::status",
			                  "block owner protocol is not recoverable");
		default:
			return make_error(ErrorCode::kCorruptSlot, "SharedBufferPool::status",
			                  "invalid block state");
		}
	}
	return Result<SharedBufferPoolStatus>(status);
}

Result<void> shared_buffer_pool_remove_if_creator_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept {
	auto valid = validate_live_pool(handle, "SharedBufferPool::remove_if_creator");
	if (!valid) return valid.error();
	if (!owner_matches(handle->header()->creator, handle->self)) {
		return make_error(ErrorCode::kPermissionDenied, "SharedBufferPool::remove_if_creator",
		                  "handle is not the creator");
	}
	for (uint32_t block_id = 0; block_id < handle->header()->block_count; ++block_id) {
		const auto* block = block_at(handle.get(), block_id);
		if (block == nullptr) {
			return make_error(ErrorCode::kCorruptSlot,
			                  "SharedBufferPool::remove_if_creator",
			                  "block offset invalid");
		}
		const uint32_t state = shared_load_seq_cst(&block->state);
		if (state == static_cast<uint32_t>(SharedBufferBlockState::kFree)) continue;
		if (state == static_cast<uint32_t>(SharedBufferBlockState::kPublished)) {
			return make_error(ErrorCode::kBufferPoolContention,
			                  "SharedBufferPool::remove_if_creator",
			                  "pool still has published blocks");
		}
		const OwnerObservation observation = snapshot_block_owner(block);
		if (observation.kind == OwnerSnapshotKind::kCorrupt ||
		    observation.kind == OwnerSnapshotKind::kEpochExhausted) {
			return make_error(ErrorCode::kRecoveryBlocked,
			                  "SharedBufferPool::remove_if_creator",
			                  "block owner protocol is not recoverable");
		}
		return make_error(ErrorCode::kBufferPoolContention,
		                  "SharedBufferPool::remove_if_creator",
		                  "pool still has active blocks");
	}
	auto unlinked =
	        shm_unlink_checked(handle->shm_name, handle->object.dev, handle->object.ino);
	if (!unlinked) return unlinked.error();
	handle->removed = true;
	handle->object.mapping.reset();
	handle->object.fd.reset();
	return Result<void>::ok();
}

uint32_t shared_buffer_pool_block_size_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept {
	return handle && !handle->removed ? handle->header()->block_size : 0;
}

uint32_t shared_buffer_pool_block_count_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept {
	return handle && !handle->removed ? handle->header()->block_count : 0;
}

uint64_t shared_buffer_pool_generation_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept {
	return handle && !handle->removed ? handle->header()->generation : 0;
}

}  // namespace edge_runtime::detail

namespace edge_runtime {

WriteBuffer::WriteBuffer(std::shared_ptr<detail::SharedBufferPoolHandle> pool, void* block,
                         std::byte* data, uint32_t size, BufferHandle descriptor) noexcept
    : pool_(std::move(pool)),
      block_(block),
      data_(data),
      size_(size),
      descriptor_(descriptor),
      active_(true) {}

WriteBuffer::WriteBuffer(WriteBuffer&& other) noexcept
    : pool_(std::move(other.pool_)),
      block_(other.block_),
      data_(other.data_),
      size_(other.size_),
      descriptor_(other.descriptor_),
      active_(other.active_) {
	other.block_ = nullptr;
	other.data_ = nullptr;
	other.size_ = 0;
	other.descriptor_ = {};
	other.active_ = false;
}

WriteBuffer& WriteBuffer::operator=(WriteBuffer&& other) noexcept {
	if (this == &other) return *this;
	abort();
	pool_ = std::move(other.pool_);
	block_ = other.block_;
	data_ = other.data_;
	size_ = other.size_;
	descriptor_ = other.descriptor_;
	active_ = other.active_;
	other.block_ = nullptr;
	other.data_ = nullptr;
	other.size_ = 0;
	other.descriptor_ = {};
	other.active_ = false;
	return *this;
}

WriteBuffer::~WriteBuffer() { abort(); }

Result<BufferHandle> WriteBuffer::publish() noexcept {
	return detail::shared_buffer_pool_publish_write_impl(this);
}

void WriteBuffer::abort() noexcept { detail::shared_buffer_pool_abort_write_impl(this); }

ReadBuffer::ReadBuffer(std::shared_ptr<detail::SharedBufferPoolHandle> pool, void* block,
                       const std::byte* data, uint32_t size, BufferHandle descriptor) noexcept
    : pool_(std::move(pool)),
      block_(block),
      data_(data),
      size_(size),
      descriptor_(descriptor),
      active_(true) {}

ReadBuffer::ReadBuffer(ReadBuffer&& other) noexcept
    : pool_(std::move(other.pool_)),
      block_(other.block_),
      data_(other.data_),
      size_(other.size_),
      descriptor_(other.descriptor_),
      active_(other.active_) {
	other.block_ = nullptr;
	other.data_ = nullptr;
	other.size_ = 0;
	other.descriptor_ = {};
	other.active_ = false;
}

ReadBuffer& ReadBuffer::operator=(ReadBuffer&& other) noexcept {
	if (this == &other) return *this;
	release();
	pool_ = std::move(other.pool_);
	block_ = other.block_;
	data_ = other.data_;
	size_ = other.size_;
	descriptor_ = other.descriptor_;
	active_ = other.active_;
	other.block_ = nullptr;
	other.data_ = nullptr;
	other.size_ = 0;
	other.descriptor_ = {};
	other.active_ = false;
	return *this;
}

ReadBuffer::~ReadBuffer() { release(); }

void ReadBuffer::release() noexcept { detail::shared_buffer_pool_release_read_impl(this); }

}  // namespace edge_runtime
