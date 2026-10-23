#include "edge_runtime/channel/channel_abi.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "edge_runtime/channel/control_operation.hpp"
#include "edge_runtime/utility/checksum.hpp"

namespace edge_runtime::detail {

namespace {
inline constexpr size_t kBootstrapMagicLen = sizeof(kBootstrapMagic) - 1;
inline constexpr size_t kHeaderMagicLen = sizeof(kChannelHeaderMagic) - 1;
}

Result<void> validate_channel_option_contract(const ChannelOptions& options,
                                              const char* operation) noexcept {
	if (!options.enable_payload_checksum) {
		return make_error(ErrorCode::kInvalidOptions, operation,
		                  "payload checksum is mandatory");
	}
	if (options.stale_timeout != std::chrono::milliseconds(100)) {
		return make_error(ErrorCode::kInvalidOptions, operation,
		                  "stale_timeout is deprecated; retain 100ms");
	}
	return Result<void>::ok();
}

// 读取身份时必须前后两次观察相同的偶数 epoch，否则只能报告恢复阻塞。
Result<ProcessIdentityAbi> identity_snapshot_read(ProcessIdentityAbi* abi) noexcept {
	for (int attempt = 0; attempt < kIdentitySnapshotRetries; ++attempt) {
		const uint64_t before = shared_load_acquire(&abi->role_epoch);
		if ((before & 1u) != 0) continue;
		ProcessIdentityAbi snap{};
		snap.pid = shared_load_relaxed(&abi->pid);
		snap.proc_start_ticks = shared_load_relaxed(&abi->proc_start_ticks);
		snap.boot_id_hash_hi = shared_load_relaxed(&abi->boot_id_hash_hi);
		snap.boot_id_hash_lo = shared_load_relaxed(&abi->boot_id_hash_lo);
		const uint64_t after = shared_load_acquire(&abi->role_epoch);
		if ((after & 1u) != 0) continue;
		if (after != before) continue;
		snap.role_epoch = after;
		return Result<ProcessIdentityAbi>(snap);
	}
	return make_error(ErrorCode::kRecoveryBlocked, "identity_snapshot_read",
	                  "identity epoch unstable");
}

Result<uint64_t> identity_snapshot_write(ProcessIdentityAbi* abi,
                                         const ProcessIdentityAbi& snap) noexcept {
	const uint64_t cur = shared_load_relaxed(&abi->role_epoch);
	if ((cur & 1u) != 0) {
		return make_error(ErrorCode::kRecoveryBlocked, "identity_snapshot_write",
		                  "identity epoch busy");
	}
	if (cur > UINT64_MAX - 2) {
		return make_error(ErrorCode::kRecoveryBlocked, "identity_snapshot_write",
		                  "identity epoch exhausted");
	}
	shared_store_relaxed(&abi->role_epoch, cur + 1);
	shared_store_relaxed(&abi->pid, snap.pid);
	shared_store_relaxed(&abi->proc_start_ticks, snap.proc_start_ticks);
	shared_store_relaxed(&abi->boot_id_hash_hi, snap.boot_id_hash_hi);
	shared_store_relaxed(&abi->boot_id_hash_lo, snap.boot_id_hash_lo);
	shared_store_release(&abi->role_epoch, cur + 2);
	return Result<uint64_t>(cur + 2);
}

// 校验引导区和完整头部，把未完成创建、ABI 不匹配和 Schema 不匹配分开报告。
uint64_t bootstrap_checksum_of(const BootstrapHeaderAbi& boot) noexcept {
	BootstrapHeaderAbi tmp = boot;
	tmp.init_state = 0;
	tmp.bootstrap_checksum = 0;
	return fnv1a64(reinterpret_cast<const std::byte*>(&tmp), sizeof(tmp));
}

Result<void> validate_bootstrap_parse(const BootstrapHeaderAbi& boot, uint64_t shm_size) noexcept {
	if (std::memcmp(boot.magic, kBootstrapMagic, kBootstrapMagicLen) != 0) {
		return make_error(ErrorCode::kCorruptHeader, "validate_bootstrap_parse",
		                  "bootstrap magic");
	}
	if (boot.abi_major != kAbiMajor || boot.abi_minor > kAbiMinorMax) {

		return make_error(ErrorCode::kAbiMismatch, "validate_bootstrap_parse",
		                  "bootstrap abi");
	}
	if (boot.header_size != sizeof(BootstrapHeaderAbi)) {
		return make_error(ErrorCode::kCorruptHeader, "validate_bootstrap_parse",
		                  "bootstrap header size");
	}
	if (boot.expected_mapping_size == 0) {
		return make_error(ErrorCode::kCorruptHeader, "validate_bootstrap_parse",
		                  "bootstrap mapping size");
	}
	if (bootstrap_checksum_of(boot) != boot.bootstrap_checksum) {
		return make_error(ErrorCode::kCorruptHeader, "validate_bootstrap_parse",
		                  "bootstrap checksum");
	}
	if (shm_size != 0 && shm_size < sizeof(BootstrapHeaderAbi)) {
		return make_error(ErrorCode::kInitializationIncomplete, "validate_bootstrap_parse",
		                  "segment too small");
	}
	return Result<void>::ok();
}

Result<void> validate_header_parse(const ChannelHeaderAbi& h, const SchemaDescriptor& schema,
                                   uint32_t payload_size) noexcept {
	if (std::memcmp(h.magic, kChannelHeaderMagic, kHeaderMagicLen) != 0) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_parse",
		                  "header magic");
	}
	if (h.abi_major != kAbiMajor || h.abi_minor > kAbiMinorMax) {
		return make_error(ErrorCode::kAbiMismatch, "validate_header_parse", "header abi");
	}
	if (h.header_size != sizeof(ChannelHeaderAbi)) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_parse",
		                  "header size");
	}
	if (h.endian_marker != kEndianMarker) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_parse",
		                  "endian marker");
	}
	if (h.slot_count != kSlotCount) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_parse", "slot count");
	}
	if (h.payload_size != payload_size) {
		return make_error(ErrorCode::kSchemaMismatch, "validate_header_parse",
		                  "payload size");
	}
	if (h.schema_version != schema.version) {
		return make_error(ErrorCode::kSchemaMismatch, "validate_header_parse",
		                  "schema version");
	}
	if (std::memcmp(h.schema_fingerprint, schema.fingerprint.data(), 32) != 0) {
		return make_error(ErrorCode::kSchemaMismatch, "validate_header_parse",
		                  "schema fingerprint");
	}
	if (h.mapping_size < kChannelHeaderOffset + kSlotHeaderSize) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_parse",
		                  "mapping too small");
	}
	if (h.generation == 0) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_parse",
		                  "zero generation");
	}
	if (h.instance_nonce_hi == 0 && h.instance_nonce_lo == 0) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_parse",
		                  "zero instance nonce");
	}
	return Result<void>::ok();
}

Result<void> validate_header_shape(const ChannelHeaderAbi& h) noexcept {

	if (std::memcmp(h.magic, kChannelHeaderMagic, kHeaderMagicLen) != 0) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_shape",
		                  "header magic");
	}
	if (h.abi_major != kAbiMajor || h.abi_minor > kAbiMinorMax) {
		return make_error(ErrorCode::kAbiMismatch, "validate_header_shape", "header abi");
	}
	if (h.header_size != sizeof(ChannelHeaderAbi)) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_shape",
		                  "header size");
	}
	if (h.endian_marker != kEndianMarker) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_shape",
		                  "endian marker");
	}
	if (h.slot_count != kSlotCount) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_shape",
		                  "slot count");
	}
	if (h.mapping_size < kChannelHeaderOffset + kSlotHeaderSize) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_shape",
		                  "mapping too small");
	}
	if (h.generation == 0) {
		return make_error(ErrorCode::kCorruptHeader, "validate_header_shape",
		                  "zero generation");
	}
	return Result<void>::ok();
}

bool next_generation_from_journal(const ControlJournalV1& journal, uint64_t* out) noexcept {
	uint64_t last = 0;
	if (journal.state == static_cast<uint32_t>(JournalState::kIdle)) {
		last = std::max(journal.old_generation, journal.new_generation);
	}
	if (last >= UINT64_MAX - 1) return false;
	*out = last + 1;
	return true;
}

ControlJournalV1 make_control_journal(const std::string& channel_name, JournalState state,
                                      uint64_t old_gen, uint64_t new_gen, uint64_t old_nonce_hi,
                                      uint64_t old_nonce_lo, uint64_t new_nonce_hi,
                                      uint64_t new_nonce_lo,
                                      const ProcessIdentity& creator) noexcept {
	ControlJournalV1 j{};
	std::memcpy(j.magic, kJournalMagic, sizeof(kJournalMagic) - 1);
	j.version = kJournalVersion;
	j.channel_hash = channel_name_hash(channel_name.c_str(), channel_name.size());
	j.state = static_cast<uint32_t>(state);
	j.old_generation = old_gen;
	j.new_generation = new_gen;
	j.old_nonce_hi = old_nonce_hi;
	j.old_nonce_lo = old_nonce_lo;
	j.new_nonce_hi = new_nonce_hi;
	j.new_nonce_lo = new_nonce_lo;
	j.creator = {};
	j.creator.pid = creator.pid;
	j.creator.proc_start_ticks = creator.proc_start_ticks;
	j.creator.boot_id_hash_hi = creator.boot_id_hash_hi;
	j.creator.boot_id_hash_lo = creator.boot_id_hash_lo;
	j.record_checksum = 0;
	return j;
}

Result<void> pread_full(int fd, void* buf, size_t size, uint64_t offset) noexcept {
	auto* dst = static_cast<char*>(buf);
	size_t got = 0;
	while (got < size) {
		auto available = control_check();
		if (!available) return available;
		const ssize_t n =
		        ::pread(fd, dst + got, size - got, static_cast<off_t>(offset + got));
		if (n < 0) {
			if (errno == EINTR) continue;
			const int e = errno;
			return make_errno_error(e, "pread_full", std::strerror(e));
		}
		if (n == 0) break;
		got += static_cast<size_t>(n);
	}
	if (got != size) {
		return make_error(ErrorCode::kInitializationIncomplete, "pread_full", "short read");
	}
	return Result<void>::ok();
}

Result<void> pwrite_full(int fd, const void* buf, size_t size, uint64_t offset) noexcept {
	const auto* src = static_cast<const char*>(buf);
	size_t written = 0;
	while (written < size) {
		auto available = control_check();
		if (!available) return available;
		const ssize_t n = ::pwrite(fd, src + written, size - written,
		                           static_cast<off_t>(offset + written));
		if (n < 0) {
			if (errno == EINTR) continue;
			const int e = errno;
			return make_errno_error(e, "pwrite_full", std::strerror(e));
		}
		if (n == 0) return make_errno_error(EIO, "pwrite_full", "zero write");
		written += static_cast<size_t>(n);
	}
	return Result<void>::ok();
}

Result<ChannelStatus> read_channel_status(std::byte* base) noexcept {
	ChannelStatus st{};
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	st.ready = shared_load_acquire(&header->init_state) ==
	           static_cast<uint32_t>(InitState::kReady);
	st.init_state = shared_load_acquire(&header->init_state);
	st.producer_state = shared_load_acquire(&header->producer_state);
	st.consumer_state = shared_load_acquire(&header->consumer_state);
	st.abi_major = header->abi_major;
	st.abi_minor = header->abi_minor;
	st.slot_count = header->slot_count;
	st.payload_size = header->payload_size;
	st.mapping_size = header->mapping_size;
	st.generation = header->generation;
	st.instance_nonce_hi = header->instance_nonce_hi;
	st.instance_nonce_lo = header->instance_nonce_lo;
	st.publish_count = shared_load_acquire(&header->publish_count);
	st.read_count = shared_load_acquire(&header->read_count);
	st.last_publish_boot_ns = shared_load_acquire(&header->last_publish_boot_ns);
	st.heartbeat_boot_ns = shared_load_acquire(&header->heartbeat_boot_ns);
	st.producer_heartbeat_interval_ns =
	        shared_load_acquire(&header->producer_heartbeat_interval_ns);

	auto p = identity_snapshot_read(&header->producer);
	if (p) {
		st.producer_pid = p.value().pid;
		st.producer_alive = probe_liveness(p.value().pid, p.value().proc_start_ticks) ==
		                    Liveness::kAlive;
	}
	auto c = identity_snapshot_read(&header->consumer);
	if (c) {
		st.consumer_pid = c.value().pid;
		st.consumer_alive = probe_liveness(c.value().pid, c.value().proc_start_ticks) ==
		                    Liveness::kAlive;
	}
	return Result<ChannelStatus>(st);
}

}
