#include "edge_runtime/channel/consumer_impl.hpp"

#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <ctime>
#include <utility>

#include "edge_runtime/channel/channel_abi.hpp"
#include "edge_runtime/channel/control_operation.hpp"
#include "edge_runtime/channel/slot_protocol.hpp"
#include "edge_runtime/sync/cancellation.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/sync/futex.hpp"
#include "edge_runtime/transport/fd_broker.hpp"
#include "edge_runtime/utility/checked_math.hpp"
#include "edge_runtime/utility/checksum.hpp"
#include "edge_runtime/utility/failpoint.hpp"

namespace edge_runtime::detail {

namespace {

// 只有已确认死亡的旧消费者才能回收 READING 槽；未知 epoch 一律阻断恢复。
Result<void> reclaim_dead_consumer_slots(std::byte* base, uint64_t old_role_epoch,
                                         uint32_t payload_size) noexcept {
	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(kSlotHeaderSize + payload_size, 64, &stride)) {
		return make_error(ErrorCode::kInvalidOptions, "Consumer::open", "stride overflow");
	}
	for (uint32_t i = 0; i < kSlotCount; ++i) {
		uint64_t offset = 0;
		if (!slot_byte_offset(i, stride, &offset)) {
			return make_error(ErrorCode::kCorruptSlot, "Consumer::open",
			                  "slot offset overflow");
		}
		auto* slot = reinterpret_cast<SlotHeaderAbi*>(base + offset);
		const uint32_t st = shared_load_relaxed(&slot->state);
		if (st == static_cast<uint32_t>(SlotState::kReadingClaiming)) {
			const uint64_t epoch = shared_load_relaxed(&slot->reader_role_epoch);
			if (epoch != 0 && epoch != old_role_epoch) {
				return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
				                  "claiming slot epoch unknown");
			}
			auto available = control_check();
			if (!available) return available;
			slot_release_read(slot);
		} else if (st == static_cast<uint32_t>(SlotState::kReading)) {
			const uint64_t epoch = shared_load_relaxed(&slot->reader_role_epoch);
			if (epoch != old_role_epoch) {
				return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
				                  "reading slot epoch unknown");
			}
			auto available = control_check();
			if (!available) return available;
			slot_release_read(slot);
		}

	}
	return Result<void>::ok();
}
}

Result<std::shared_ptr<ConsumerHandle>> consumer_open_impl(const ChannelOptions& options,
                                                           const SchemaDescriptor& schema,
                                                           uint32_t payload_size) {
	if (options.name.empty() || options.name.size() > kMaxChannelNameLen ||
	    !validate_channel_name(options.name.c_str(), options.name.size())) {
		return make_error(ErrorCode::kInvalidName, "Consumer::open", options.name.c_str());
	}
	if (payload_size == 0 || payload_size > kMaxPayloadSize) {
		return make_error(ErrorCode::kInvalidOptions, "Consumer::open",
		                  "payload size out of range");
	}
	if (options.transport != Transport::kPosixShm &&
	    options.transport != Transport::kMemfdFdPass) {
		return make_error(ErrorCode::kInvalidOptions, "Consumer::open", "unsupported transport");
	}
	const int64_t reconnect_timeout_ms = options.reconnect_timeout.count();
	auto contract = validate_channel_option_contract(options, "Consumer::open");
	if (!contract) return contract.error();
	if (reconnect_timeout_ms < 0 ||
	    static_cast<uint64_t>(reconnect_timeout_ms) > UINT64_MAX / 1'000'000ull) {
		return make_error(ErrorCode::kInvalidOptions, "Consumer::open",
		                  "reconnect timeout out of range");
	}
	bool fingerprint_set = false;
	for (const std::byte b : schema.fingerprint) {
		if (b != std::byte{0}) {
			fingerprint_set = true;
			break;
		}
	}
	if (!fingerprint_set) {
		return make_error(ErrorCode::kInvalidOptions, "Consumer::open",
		                  "schema fingerprint unset");
	}

	const std::string shm_name = channel_shm_name(options.name);
	auto identity = read_current_process_identity();
	if (!identity) return identity.error();
	const ProcessIdentity self = identity.value();
	const std::string lock_path = channel_lock_path(options.name);
	const std::string socket_path = channel_socket_path(options.name);
	const uint32_t name_hash = channel_name_hash(options.name.c_str(), options.name.size());
	const bool fd_mode = options.transport == Transport::kMemfdFdPass;

	UniqueFd fd;
	uint64_t dev = 0, ino = 0, size = 0;
	FdBrokerReplyAbi fd_reply{};
	if (fd_mode) {

		auto rf = fd_broker_request_fd(socket_path, name_hash, schema, false,
		                               &fd_reply, static_cast<uint64_t>(reconnect_timeout_ms));
		if (!rf) return rf.error();
		fd = std::move(rf.value());
		auto fst = memfd_fstat_and_capture(fd, &dev, &ino, &size);
		if (!fst) return fst.error();
	} else {
		auto fd_res = shm_open_existing(shm_name);
		if (!fd_res) {

			struct stat st {};
			if (fd_res.error().code == ErrorCode::kNotFound &&
			    ::stat(socket_path.c_str(), &st) == 0) {
				return make_error(ErrorCode::kInvalidOptions, "Consumer::open",
				                  "channel is fd-pass; use Transport::kMemfdFdPass");
			}
			return fd_res.error();
		}
		fd = std::move(fd_res.value());
		auto fst = shm_fstat_and_capture(fd, &dev, &ino, &size);
		if (!fst) return fst.error();
	}
	if (size < sizeof(BootstrapHeaderAbi)) {
		return make_error(ErrorCode::kInitializationIncomplete, "Consumer::open",
		                  "segment below bootstrap size");
	}

	BootstrapHeaderAbi boot{};
	auto rboot = pread_full(fd.get(), &boot, sizeof(boot), 0);
	if (!rboot) return rboot.error();
	auto vboot = validate_bootstrap_parse(boot, size);
	if (!vboot) return vboot.error();
	if (boot.expected_mapping_size != size) {
		return make_error(ErrorCode::kInitializationIncomplete, "Consumer::open",
		                  "mapping size mismatch (producer still initializing?)");
	}
	if (boot.init_state != static_cast<uint32_t>(InitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, "Consumer::open",
		                  "producer not ready");
	}

	{
		if (boot.creator_boot_id_hash_hi != self.boot_id_hash_hi ||
		    boot.creator_boot_id_hash_lo != self.boot_id_hash_lo) {
			return make_error(ErrorCode::kCorruptHeader, "Consumer::open",
			                  "segment from previous boot");
		}
	}

	auto mm = mmap_region(fd, size);
	if (!mm) return mm.error();
	MappedRegion mapping = std::move(mm.value());
	auto* base = static_cast<std::byte*>(mapping.get());
	auto* boot_map = reinterpret_cast<BootstrapHeaderAbi*>(base);

	if (shared_load_acquire(&boot_map->init_state) !=
	    static_cast<uint32_t>(InitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, "Consumer::open",
		                  "not ready on recheck");
	}

	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	auto vh = validate_header_parse(*header, schema, payload_size);
	if (!vh) return vh.error();

	auto lock_res = ControlLock::acquire(lock_path);
	if (!lock_res) return lock_res.error();
	ControlLock lock = std::move(lock_res.value());

	auto jr = lock.read_journal();
	if (!jr) return jr.error();
	const ControlJournalV1 journal = jr.value();
	if (journal.channel_hash != 0 && journal.channel_hash != name_hash) {
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
		                  "journal channel mismatch");
	}

	if (!fd_mode) {
		auto again = shm_open_existing(shm_name);
		if (!again) return again.error();
		uint64_t adev = 0, aino = 0;
		auto afst = shm_fstat_and_capture(again.value(), &adev, &aino, nullptr);
		if (!afst) return afst.error();
		if (adev != dev || aino != ino) {
			return make_error(ErrorCode::kNameRaceDetected, "Consumer::open",
			                  "instance replaced during open");
		}
	} else {

		if (fd_reply.generation != header->generation ||
		    fd_reply.nonce_hi != header->instance_nonce_hi ||
		    fd_reply.nonce_lo != header->instance_nonce_lo ||
		    fd_reply.mapping_size != header->mapping_size) {
			return make_error(ErrorCode::kTransportFailed, "Consumer::open",
			                  "broker reply disagrees with header");
		}
	}

	auto old_cid = identity_snapshot_read(&header->consumer);
	if (!old_cid) {
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
		                  "consumer identity unreadable");
	}
	const uint32_t old_state = shared_load_acquire(&header->consumer_state);
	if (old_state != static_cast<uint32_t>(EndpointState::kOffline) &&
	    !process_identity_valid({old_cid.value().pid, old_cid.value().proc_start_ticks,
	                             old_cid.value().boot_id_hash_hi,
	                             old_cid.value().boot_id_hash_lo})) {
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
		                  "invalid old consumer");
	}
	if (old_cid.value().pid != 0) {
		const uint32_t cstate = shared_load_acquire(&header->consumer_state);
		const bool actively_owned =
		        cstate == static_cast<uint32_t>(EndpointState::kOnline) ||
		        cstate == static_cast<uint32_t>(EndpointState::kStopping) ||
		        cstate == static_cast<uint32_t>(EndpointState::kFault);
		if (actively_owned) {
			const Liveness liv = probe_liveness(old_cid.value().pid,
			                                    old_cid.value().proc_start_ticks);
			if (liv == Liveness::kAlive) {
				return make_error(ErrorCode::kConsumerAlreadyOwned,
				                  "Consumer::open", "consumer alive");
			}
			if (liv == Liveness::kUnverifiable) {
				return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
				                  "cannot verify old consumer");
			}

			auto reclaim = reclaim_dead_consumer_slots(base, old_cid.value().role_epoch,
			                                           payload_size);
			if (!reclaim) return reclaim.error();
		}

	}

	auto handle = std::make_shared<ConsumerHandle>();
	handle->owner_pid = self.pid;
	(void)CancellationAccess::native_handle(handle->control_stop.token());
	handle->shm.name = options.name;
	handle->channel_name = options.name;
	auto committing = control_begin_commit();
	if (!committing) return committing.error();
	ProcessIdentityAbi cid{};
	cid.pid = self.pid;
	cid.proc_start_ticks = self.proc_start_ticks;
	cid.boot_id_hash_hi = self.boot_id_hash_hi;
	cid.boot_id_hash_lo = self.boot_id_hash_lo;
	auto cw = identity_snapshot_write(&header->consumer, cid);
	if (!cw) return cw.error();
	const uint64_t role_epoch = cw.value();
	shared_store_release(&header->consumer_state,
	                     static_cast<uint32_t>(EndpointState::kOnline));
	control_committed();
	EDGE_FAILPOINT(C15);

	handle->shm.fd = std::move(fd);
	handle->shm.mapping = std::move(mapping);
	handle->shm.dev = dev;
	handle->shm.ino = ino;
	handle->shm.size = size;
	handle->transport = options.transport;
	handle->generation = header->generation;
	handle->role_epoch = role_epoch;
	handle->instance_nonce_hi = header->instance_nonce_hi;
	handle->instance_nonce_lo = header->instance_nonce_lo;
	handle->schema_fingerprint = schema.fingerprint;
	handle->schema_version = schema.version;
	handle->payload_size = payload_size;
	handle->self = self;
	handle->reconnect_timeout_ms = static_cast<uint64_t>(reconnect_timeout_ms);
	return Result<std::shared_ptr<ConsumerHandle>>(std::move(handle));
}

namespace {
inline constexpr uint32_t kMaxReadRetries = 8;

struct ConsumerUseGuard {
	std::atomic<bool>* in_use = nullptr;
	bool armed = false;
	~ConsumerUseGuard() {
		if (armed) in_use->store(false, std::memory_order_release);
	}
};

void mark_consumer_offline(ConsumerHandle& handle) noexcept {
	consumer_clear_notifications(handle);
	auto* base = static_cast<std::byte*>(handle.shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	auto cid_res = identity_snapshot_read(&header->consumer);
	if (!cid_res || cid_res.value().role_epoch != handle.role_epoch) return;
	shared_store_release(&header->consumer_state,
	                     static_cast<uint32_t>(EndpointState::kOffline));
}

void service_consumer_shutdown(const std::shared_ptr<ConsumerHandle>& handle) noexcept {
	if (handle->control_closed || (handle->controlled_stop.load(std::memory_order_acquire) &&
	                               !handle->legacy_cleanup.load(std::memory_order_acquire)))
		return;
	if (!handle->shutdown_pending.load(std::memory_order_acquire)) return;
	bool idle = false;
	if (!handle->operation_in_use.compare_exchange_strong(idle, true, std::memory_order_acq_rel)) {
		return;
	}
	bool first = false;
	if (handle->shutdown_started.compare_exchange_strong(first, true, std::memory_order_acq_rel)) {
		mark_consumer_offline(*handle);
	}
	handle->operation_in_use.store(false, std::memory_order_release);
}

void release_consumer_loan_operation(const std::shared_ptr<ConsumerHandle>& handle) noexcept {
	handle->operation_in_use.store(false, std::memory_order_release);
	service_consumer_shutdown(handle);
}

void pack_nonce(const ConsumerHandle& handle, std::array<std::byte, 16>* out) {
	uint64_t hi = handle.instance_nonce_hi;
	uint64_t lo = handle.instance_nonce_lo;
	std::memcpy(out->data(), &hi, 8);
	std::memcpy(out->data() + 8, &lo, 8);
}
struct ClaimedRead {
	SlotHeaderAbi* slot{nullptr};
	const std::byte* payload{nullptr};
	ReadSnapshot snapshot{};
};

// 先抢占 latest 指向的槽，再复核 ticket；Producer 在复核前覆盖槽时立即重试。
Result<ClaimedRead> claim_latest_unlocked(const std::shared_ptr<ConsumerHandle>& handle,
                                         const char* operation, bool loaned = false) noexcept {
	if (handle->shutdown_pending.load(std::memory_order_acquire))
		return make_error(ErrorCode::kClosed, operation, "stop requested");

	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);

	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(kSlotHeaderSize + handle->payload_size, 64, &stride)) {
		return make_error(ErrorCode::kInvalidOptions, operation, "stride overflow");
	}

	for (uint32_t attempt = 0; attempt < kMaxReadRetries; ++attempt) {
		const uint64_t ticket = shared_load_acquire(&header->latest_ticket);
		if (ticket == 0 || ticket_sequence(ticket) <= handle->last_sequence) {
			return make_error(ErrorCode::kNoNewSample, operation, "no new sample");
		}
		const uint32_t slot_index = ticket_slot(ticket);
		uint64_t offset = 0;
		if (!slot_byte_offset(slot_index, stride, &offset)) {
			return make_error(ErrorCode::kCorruptSlot, operation, "ticket slot out of range");
		}
		auto* slot = reinterpret_cast<SlotHeaderAbi*>(base + offset);
		if (!slot_claim_readable(slot)) continue;
		EDGE_FAILPOINT(C07);
		slot_mark_reading(slot, handle->role_epoch);
		if (loaned) {
			EDGE_FAILPOINT(C23);
		} else {
			EDGE_FAILPOINT(C08);
		}

		const uint64_t claimed_ticket = make_ticket(slot->sample_sequence, slot_index);
		if (shared_load_acquire(&header->latest_ticket) != claimed_ticket) {
			slot_release_read(slot);
			continue;
		}

		const uint32_t slot_payload_size = slot->payload_size;
		const uint64_t sample_sequence = slot->sample_sequence;
		const uint64_t publish_boot_ns = slot->publish_boot_ns;
		const uint64_t expected_checksum = slot->payload_checksum;
		auto* payload =
		        reinterpret_cast<std::byte*>(slot) + static_cast<uint64_t>(kSlotHeaderSize);
		if (slot_payload_size != handle->payload_size) {
			slot_release_read(slot);
			return make_error(ErrorCode::kPayloadCorrupt, operation, "payload size mismatch");
		}
		const uint64_t receive_boot_ns = boottime_now_ns();
		if (receive_boot_ns == 0) {
			slot_release_read(slot);
			return make_error(ErrorCode::kClockAnomaly, operation, "monotonic clock unavailable");
		}
		if (fnv1a64(payload, static_cast<size_t>(slot_payload_size)) != expected_checksum) {
			slot_release_read(slot);
			return make_error(ErrorCode::kPayloadCorrupt, operation, "checksum mismatch");
		}

		ReadSnapshot snap;
		snap.encoded_size = slot_payload_size;
		snap.checksum_ok = true;
		snap.sample_sequence = sample_sequence;
		snap.publish_boot_ns = publish_boot_ns;
		snap.receive_boot_ns = receive_boot_ns;
		snap.generation = handle->generation;
		pack_nonce(*handle, &snap.instance_nonce);
		snap.missed_samples =
		        handle->first_sample_in_generation
		                ? 0
		                : saturated_gap(handle->last_sequence, sample_sequence);
		handle->last_sequence = sample_sequence;
		handle->first_sample_in_generation = false;
		shared_fetch_add_relaxed(&header->read_count, uint64_t{1});
		return ClaimedRead{slot, payload, std::move(snap)};
	}

	return make_error(ErrorCode::kReadContention, operation, "retry bound exceeded");
}

Result<ReadSnapshot> try_read_latest_unlocked(const std::shared_ptr<ConsumerHandle>& handle,
                                             std::byte* encoded_out,
                                             uint32_t encoded_cap) noexcept {
	auto claimed = claim_latest_unlocked(handle, "Consumer::try_read_latest");
	if (!claimed) return claimed.error();
	if (claimed.value().snapshot.encoded_size > encoded_cap) {
		slot_release_read(claimed.value().slot);
		return make_error(ErrorCode::kPayloadCorrupt, "Consumer::try_read_latest",
		                  "output capacity too small");
	}
	std::memcpy(encoded_out, claimed.value().payload,
	            static_cast<size_t>(claimed.value().snapshot.encoded_size));
	slot_release_read(claimed.value().slot);
	EDGE_FAILPOINT(C09);
	return Result<ReadSnapshot>(std::move(claimed.value().snapshot));
}

}

Result<ReadSnapshot> consumer_try_read_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                                   std::byte* encoded_out,
                                                   uint32_t encoded_cap) noexcept {
	ConsumerUseGuard guard;
	guard.in_use = &handle->operation_in_use;
	if (handle->operation_in_use.exchange(true, std::memory_order_acq_rel)) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Consumer::try_read_latest",
		                  "concurrent handle use");
	}
	guard.armed = true;
	return try_read_latest_unlocked(handle, encoded_out, encoded_cap);
}

namespace {

// 等待超时只做观察：存活但无数据是 DataStale，心跳失效是 Stalled，无法确认则阻断恢复。
Result<ReadSnapshot> classify_wait_timeout(const std::shared_ptr<ConsumerHandle>&,
                                           ChannelHeaderAbi* header,
                                           const char* operation) noexcept {
	const uint32_t pstate = shared_load_acquire(&header->producer_state);
	const bool actively_owned = pstate == static_cast<uint32_t>(EndpointState::kOnline) ||
	                            pstate == static_cast<uint32_t>(EndpointState::kStopping) ||
	                            pstate == static_cast<uint32_t>(EndpointState::kFault);
	if (!actively_owned) {

		return make_error(ErrorCode::kProducerOffline, operation,
		                  "producer offline on wait timeout");
	}
	auto pid_res = identity_snapshot_read(&header->producer);
	if (!pid_res) {
		return make_error(ErrorCode::kRecoveryBlocked, operation,
		                  "producer identity unreadable");
	}
	const ProcessIdentityAbi& pid_abi = pid_res.value();
	if (pid_abi.pid == 0) {
		return make_error(ErrorCode::kRecoveryBlocked, operation,
		                  "producer identity unknown");
	}
	switch (probe_liveness(pid_abi.pid, pid_abi.proc_start_ticks)) {
		case Liveness::kAlive: {

			const uint64_t interval =
			        shared_load_acquire(&header->producer_heartbeat_interval_ns);
			const uint64_t now = boottime_now_ns();
			const uint64_t last_publish =
			        shared_load_acquire(&header->last_publish_boot_ns);
			const uint64_t last_beat = shared_load_acquire(&header->heartbeat_boot_ns);

			const uint64_t stall_limit =
			        saturating_mul_u64(interval, kHeartbeatStallFactor);
			if (interval != 0 && now != 0 &&
			    (last_publish == 0 || elapsed_exceeds(now, last_publish, interval)) &&
			    (last_beat == 0 || elapsed_exceeds(now, last_beat, stall_limit))) {
				return make_error(ErrorCode::kProducerStalled, operation,
				                  "producer alive, heartbeat stale");
			}
			return make_error(ErrorCode::kDataStale, operation,
			                  "producer alive but no new sample");
		}
		case Liveness::kExited:
			return make_error(ErrorCode::kProducerOffline, operation,
			                  "producer exited");
		case Liveness::kPidReused:
		case Liveness::kUnverifiable:
			return make_error(ErrorCode::kRecoveryBlocked, operation,
			                  "producer identity unverifiable");
	}
	return make_error(ErrorCode::kRecoveryBlocked, operation,
	                  "producer identity unverifiable");
}
}

// 等待循环始终复用同一个绝对截止时间，避免 EINTR 或伪唤醒延长业务超时。
Result<ReadSnapshot> consumer_wait_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                               std::byte* encoded_out, uint32_t encoded_cap,
                                               uint64_t timeout_ns) noexcept {
	ConsumerUseGuard guard;
	guard.in_use = &handle->operation_in_use;
	if (handle->operation_in_use.exchange(true, std::memory_order_acq_rel)) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Consumer::wait_latest",
		                  "concurrent handle use");
	}
	guard.armed = true;

	const uint64_t deadline = monotonic_deadline_ns(timeout_ns);
	if (deadline == 0) {
		return make_error(ErrorCode::kClockAnomaly, "Consumer::wait_latest",
		                  "monotonic clock unavailable");
	}

	if (handle->shutdown_pending.load(std::memory_order_acquire))
		return make_error(ErrorCode::kClosed, "Consumer::wait_latest");
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	CancellationRegistration stop_registration(handle->control_stop.token(),
	                                           &header->notify_epoch);

	for (;;) {
		const auto read = try_read_latest_unlocked(handle, encoded_out, encoded_cap);
		if (read) return read;
		const ErrorCode ec = read.error().code;
		if (ec != ErrorCode::kNoNewSample && ec != ErrorCode::kReadContention) {

			return read.error();
		}

		const uint32_t expected = shared_load_acquire(&header->notify_epoch);

		const uint64_t ticket = shared_load_acquire(&header->latest_ticket);
		if (ticket != 0 && ticket_sequence(ticket) > handle->last_sequence) {
			continue;
		}

		const uint64_t remaining = remaining_time_ns(deadline);
		if (remaining == 0) {
			return classify_wait_timeout(handle, header, "Consumer::wait_latest");
		}
		struct timespec ts {};
		ts.tv_sec = static_cast<time_t>(remaining / 1000000000ull);
		ts.tv_nsec = static_cast<long>(remaining % 1000000000ull);
		if (handle->shutdown_pending.load(std::memory_order_acquire))
			return make_error(ErrorCode::kClosed, "Consumer::wait_latest");
		const int rc = futex_wait(&header->notify_epoch, expected, &ts);
		if (rc == 0) continue;
		if (rc < 0 && (errno == EAGAIN || errno == EINTR)) {
			continue;
		}
		if (rc < 0 && errno == ETIMEDOUT) {
			return classify_wait_timeout(handle, header, "Consumer::wait_latest");
		}
		if (rc < 0) {
			const int e = errno;
			return make_errno_error(ErrorCode::kSystemError, e,
			                        "Consumer::wait_latest", "futex_wait failed");
		}
	}
}

Result<ReadLoan> consumer_try_loan_latest_impl(
        const std::shared_ptr<ConsumerHandle>& handle) noexcept {
	if (handle->operation_in_use.exchange(true, std::memory_order_acq_rel)) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Consumer::try_loan_latest",
		                  "concurrent handle use");
	}
	auto claimed = claim_latest_unlocked(handle, "Consumer::try_loan_latest", true);
	if (!claimed) {
		release_consumer_loan_operation(handle);
		return claimed.error();
	}
	ReadSnapshot& snap = claimed.value().snapshot;
	return ReadLoan(handle, claimed.value().slot, claimed.value().payload, snap.encoded_size,
	                snap.generation, snap.instance_nonce, snap.sample_sequence,
	                snap.publish_boot_ns, snap.receive_boot_ns, snap.missed_samples);
}

Result<ReadLoan> consumer_wait_loan_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                               uint64_t timeout_ns) noexcept {
	if (handle->operation_in_use.exchange(true, std::memory_order_acq_rel)) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Consumer::wait_loan_latest",
		                  "concurrent handle use");
	}
	auto fail = [&handle](const Error& error) -> Result<ReadLoan> {
		release_consumer_loan_operation(handle);
		return error;
	};
	const uint64_t deadline = monotonic_deadline_ns(timeout_ns);
	if (deadline == 0) {
		return fail(make_error(ErrorCode::kClockAnomaly, "Consumer::wait_loan_latest",
		                       "monotonic clock unavailable"));
	}

	if (handle->shutdown_pending.load(std::memory_order_acquire))
		return fail(make_error(ErrorCode::kClosed, "Consumer::wait_loan_latest"));
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	CancellationRegistration stop_registration(handle->control_stop.token(),
	                                           &header->notify_epoch);
	for (;;) {
		auto claimed = claim_latest_unlocked(handle, "Consumer::wait_loan_latest", true);
		if (claimed) {
			ReadSnapshot& snap = claimed.value().snapshot;
			return ReadLoan(handle, claimed.value().slot, claimed.value().payload,
			                snap.encoded_size, snap.generation, snap.instance_nonce,
			                snap.sample_sequence, snap.publish_boot_ns, snap.receive_boot_ns,
			                snap.missed_samples);
		}
		const ErrorCode code = claimed.error().code;
		if (code != ErrorCode::kNoNewSample && code != ErrorCode::kReadContention) {
			return fail(claimed.error());
		}

		const uint32_t expected = shared_load_acquire(&header->notify_epoch);
		const uint64_t ticket = shared_load_acquire(&header->latest_ticket);
		if (ticket != 0 && ticket_sequence(ticket) > handle->last_sequence) continue;
		const uint64_t remaining = remaining_time_ns(deadline);
		if (remaining == 0) {
			auto classified =
			        classify_wait_timeout(handle, header, "Consumer::wait_loan_latest");
			return fail(classified.error());
		}
		struct timespec ts {};
		ts.tv_sec = static_cast<time_t>(remaining / 1000000000ull);
		ts.tv_nsec = static_cast<long>(remaining % 1000000000ull);
		if (handle->shutdown_pending.load(std::memory_order_acquire))
			return fail(make_error(ErrorCode::kClosed, "Consumer::wait_loan_latest"));
		const int rc = futex_wait(&header->notify_epoch, expected, &ts);
		if (rc == 0 || (rc < 0 && (errno == EAGAIN || errno == EINTR))) continue;
		if (rc < 0 && errno == ETIMEDOUT) {
			auto classified =
			        classify_wait_timeout(handle, header, "Consumer::wait_loan_latest");
			return fail(classified.error());
		}
		if (rc < 0) {
			const int saved_errno = errno;
			return fail(make_errno_error(ErrorCode::kSystemError, saved_errno,
			                             "Consumer::wait_loan_latest", "futex_wait failed"));
		}
	}
}

// 先完整打开并验证新实例，再替换旧映射；打开失败时旧句柄继续可诊断。
Result<ReconnectInfo> consumer_reconnect_impl(
        const std::shared_ptr<ConsumerHandle>& handle) noexcept {
	ConsumerUseGuard guard;
	guard.in_use = &handle->operation_in_use;
	if (handle->operation_in_use.exchange(true, std::memory_order_acq_rel)) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Consumer::reconnect",
		                  "concurrent handle use");
	}
	guard.armed = true;

	try {
		if (handle->shutdown_pending.load(std::memory_order_acquire))
			return make_error(ErrorCode::kClosed, "Consumer::reconnect");
		ChannelOptions options;
		options.name = handle->channel_name;
		options.transport = handle->transport;
		options.reconnect_timeout =
		        std::chrono::milliseconds(static_cast<int64_t>(handle->reconnect_timeout_ms));
		SchemaDescriptor schema;
		schema.fingerprint = handle->schema_fingerprint;
		schema.version = handle->schema_version;
		schema.debug_name = "Consumer::reconnect";

		auto opened = consumer_open_impl(options, schema, handle->payload_size);
		if (!opened) return opened.error();
		std::shared_ptr<ConsumerHandle> fresh = std::move(opened.value());

		ReconnectInfo info;
		info.old_generation = handle->generation;
		info.new_generation = fresh->generation;
		pack_nonce(*handle, &info.old_instance_nonce);
		pack_nonce(*fresh, &info.new_instance_nonce);
		info.schema_changed = handle->schema_fingerprint != fresh->schema_fingerprint ||
		                      handle->schema_version != fresh->schema_version;

		mark_consumer_offline(*handle);
		handle->shm = std::move(fresh->shm);
		handle->channel_name = std::move(fresh->channel_name);
		handle->transport = fresh->transport;
		handle->generation = fresh->generation;
		handle->role_epoch = fresh->role_epoch;
		handle->instance_nonce_hi = fresh->instance_nonce_hi;
		handle->instance_nonce_lo = fresh->instance_nonce_lo;
		handle->schema_fingerprint = fresh->schema_fingerprint;
		handle->schema_version = fresh->schema_version;
		handle->payload_size = fresh->payload_size;
		handle->self = fresh->self;
		handle->reconnect_timeout_ms = fresh->reconnect_timeout_ms;
		handle->last_sequence = 0;
		handle->first_sample_in_generation = true;
		return Result<ReconnectInfo>(std::move(info));
	} catch (...) {
		return make_error(ErrorCode::kSystemError, "Consumer::reconnect",
		                  "allocation failed while reopening channel");
	}
}

// status() 是慢路径诊断，POSIX 名称被替换时必须返回 StaleHandle。
Result<ChannelStatus> consumer_status_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept {
	ConsumerUseGuard guard;
	guard.in_use = &handle->operation_in_use;
	if (handle->operation_in_use.exchange(true, std::memory_order_acq_rel)) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Consumer::status",
		                  "concurrent handle use");
	}
	guard.armed = true;

	if (handle->control_closed) return make_error(ErrorCode::kClosed, "Consumer::status");
	if (handle->transport == Transport::kPosixShm) {
		const std::string shm_name = channel_shm_name(handle->channel_name);
		auto re = shm_open_existing(shm_name);
		if (!re) {
			return make_error(ErrorCode::kStaleHandle, "Consumer::status",
			                  "channel name gone");
		}
		uint64_t dev = 0, ino = 0;
		auto fst = shm_fstat_and_capture(re.value(), &dev, &ino, nullptr);
		if (!fst) return fst.error();
		if (dev != handle->shm.dev || ino != handle->shm.ino) {
			return make_error(ErrorCode::kStaleHandle, "Consumer::status",
			                  "instance replaced under name");
		}
	}

	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	return read_channel_status(base);
}

void consumer_shutdown_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept {
	if (handle->control_closed) return;
	handle->legacy_cleanup.store(true, std::memory_order_release);
	handle->shutdown_pending.store(true, std::memory_order_release);
	(void)handle->control_stop.request_cancel();
	service_consumer_shutdown(handle);
}

Result<std::shared_ptr<ConsumerHandle>> consumer_open_controlled(
        const ChannelOptions& options, const SchemaDescriptor& schema, uint32_t payload_size,
        const ControlOperationContext& context, ControlReceipt& receipt) {
	if (receipt.has_pending_cleanup())
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
		                  "finalize receipt first");
	ControlScope scope(context, receipt);
	auto checked = control_check();
	if (!checked) return checked.error();
	try {
		auto opened = consumer_open_impl(options, schema, payload_size);
		if (!opened) {
			checked = control_check();
			if (!checked) return checked.error();
		}
		return opened;
	} catch (...) {
		return make_errno_error(ENOMEM, "Consumer::open", "allocation before commit");
	}
}

Result<ReconnectInfo> consumer_reconnect_controlled(const std::shared_ptr<ConsumerHandle>& handle,
                                                    const ControlOperationContext& context,
                                                    ControlReceipt& receipt) noexcept {
	if (!handle) return make_error(ErrorCode::kClosed, "Consumer::reconnect");
	if (handle->owner_pid != static_cast<uint64_t>(::getpid()))
		return make_error(ErrorCode::kStaleHandle, "Consumer::reconnect",
		                  "foreign process handle");
	if (receipt.has_pending_cleanup())
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::reconnect",
		                  "finalize receipt first");
	ControlScope scope(context, receipt, handle->control_stop.token());
	auto checked = control_check();
	if (!checked) return checked.error();
	auto reopened = consumer_reconnect_impl(handle);
	if (!reopened) {
		checked = control_check();
		if (!checked) return checked.error();
	}
	return reopened;
}

void consumer_request_stop(const std::shared_ptr<ConsumerHandle>& handle) noexcept {
	if (!handle) return;
	if (handle->owner_pid != static_cast<uint64_t>(::getpid())) return;
	handle->controlled_stop.store(true, std::memory_order_release);
	handle->shutdown_pending.store(true, std::memory_order_release);
	(void)handle->control_stop.request_cancel();
}

Result<ControlCloseStatus> consumer_finalize(const std::shared_ptr<ConsumerHandle>& handle,
                                             const ControlOperationContext& context,
                                             ControlReceipt& receipt) noexcept {
	if (!handle) return ControlCloseStatus::kClosed;
	if (handle->owner_pid != static_cast<uint64_t>(::getpid()))
		return make_error(ErrorCode::kStaleHandle, "Consumer::finalize",
		                  "foreign process handle");
	if (receipt.has_pending_cleanup())
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::finalize",
		                  "finalize receipt first");
	ControlScope scope(context, receipt);
	consumer_request_stop(handle);
	control_handle_pending();
	ConsumerUseGuard guard;
	guard.in_use = &handle->operation_in_use;
	if (handle->operation_in_use.exchange(true, std::memory_order_acq_rel)) {
		control_handle_pending();
		return ControlCloseStatus::kPending;
	}
	guard.armed = true;
	if (handle->control_closed) {
		control_committed();
		return ControlCloseStatus::kClosed;
	}
	auto available = context.check();
	if (!available) {
		control_handle_pending(available.error());
		return ControlCloseStatus::kPending;
	}
	mark_consumer_offline(*handle);
	handle->shm.mapping.reset();
	handle->shm.fd.reset();
	handle->control_closed = true;
	handle->shutdown_started.store(true, std::memory_order_release);
	control_committed();
	return ControlCloseStatus::kClosed;
}

void consumer_release_loan_impl(ReadLoan* loan) noexcept {
	if (loan == nullptr || !loan->active_ || !loan->handle_) return;
	const std::shared_ptr<ConsumerHandle> handle = loan->handle_;
	if (loan->slot_ != nullptr) slot_release_read(static_cast<SlotHeaderAbi*>(loan->slot_));
	EDGE_FAILPOINT(C09);
	loan->active_ = false;
	loan->slot_ = nullptr;
	loan->data_ = nullptr;
	loan->handle_.reset();
	release_consumer_loan_operation(handle);
}

}
