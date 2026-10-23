#include "edge_runtime/channel/producer_impl.hpp"

#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

#include "edge_runtime/channel/channel_abi.hpp"
#include "edge_runtime/channel/control_operation.hpp"
#include "edge_runtime/channel/notification_service.hpp"
#include "edge_runtime/channel/slot_protocol.hpp"
#include "edge_runtime/sync/cancellation.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/sync/futex.hpp"
#include "edge_runtime/transport/fd_broker.hpp"
#include "edge_runtime/utility/checksum.hpp"
#include "edge_runtime/utility/failpoint.hpp"
#include "edge_runtime/utility/random.hpp"

namespace edge_runtime::detail {

namespace {

struct JournalGuard {
	ControlLock* lock = nullptr;
	ControlJournalV1 reset_to{};
	bool armed = false;
	bool rollback_blocked = false;
	std::string socket_path;
	uint64_t socket_dev{};
	uint64_t socket_ino{};

	~JournalGuard() {
		if (armed && lock != nullptr) {
			const auto expected = lock->attempted_journal();
			bool socket_pending = false;
			if (socket_ino != 0) {
				struct stat status {};
				const auto available = control_check();
				if (!available)
					socket_pending = true;
				else if (::lstat(socket_path.c_str(), &status) == 0) {
					if (static_cast<uint64_t>(status.st_dev) != socket_dev ||
					    static_cast<uint64_t>(status.st_ino) != socket_ino ||
					    !S_ISSOCK(status.st_mode) ||
					    ::unlink(socket_path.c_str()) != 0)
						socket_pending = true;
				} else if (errno != ENOENT)
					socket_pending = true;
			}
			if (rollback_blocked) {
				if (socket_pending) control_retain_socket(socket_dev, socket_ino);
				return;
			}
			if (socket_pending) {
				control_retain_cleanup(*lock, expected, reset_to, false, 0, 0);
				control_retain_socket(socket_dev, socket_ino);
				return;
			}
			auto reset = lock->write_journal(reset_to);
			if (!reset || socket_pending) {
				control_retain_cleanup(*lock, reset ? reset_to : expected, reset_to,
				                       false, 0, 0);
				if (socket_pending) control_retain_socket(socket_dev, socket_ino);
			}
		}
	}
};

struct CreatedObjectGuard {
	std::string name;
	JournalGuard* journal_guard = nullptr;
	bool armed = false;
	uint64_t dev = 0;
	uint64_t ino = 0;

	~CreatedObjectGuard() {
		if (armed) {
			auto available = control_check();
			auto removed = available ? shm_unlink_checked(name, dev, ino) : available;
			if (!removed && removed.error().code != ErrorCode::kNotFound &&
			    journal_guard != nullptr) {
				journal_guard->rollback_blocked = true;
				control_retain_cleanup(*journal_guard->lock,
				                       journal_guard->lock->attempted_journal(),
				                       journal_guard->reset_to, true, dev, ino);
			}
		}
	}
};

struct ProducerUseGuard {
	std::atomic<bool>* in_use = nullptr;
	bool armed = false;

	explicit ProducerUseGuard(std::atomic<bool>* value) noexcept
	    : in_use(value), armed(!value->exchange(true, std::memory_order_acq_rel)) {}
	~ProducerUseGuard() {
		if (armed) in_use->store(false, std::memory_order_release);
	}

	explicit operator bool() const noexcept { return armed; }
};

void mark_producer_offline(ProducerHandle& handle) noexcept {

	auto* base = static_cast<std::byte*>(handle.shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	auto pid_res = identity_snapshot_read(&header->producer);
	if (!pid_res || pid_res.value().role_epoch != handle.role_epoch) return;

	shared_store_release(&header->producer_state,
	                     static_cast<uint32_t>(EndpointState::kOffline));
	if (handle.notification_server) handle.notification_server->stop();
	if (handle.transport != Transport::kMemfdFdPass || handle.socket_unlinked) return;

	handle.serve_stop.store(true, std::memory_order_relaxed);
	if (handle.listen_fd.get() >= 0) (void)::shutdown(handle.listen_fd.get(), SHUT_RDWR);
	auto lock_res = ControlLock::acquire(channel_lock_path(handle.channel_name));
	if (!lock_res) return;
	auto jr = lock_res.value().read_journal();
	if (jr && jr.value().new_generation == handle.generation &&
	    jr.value().new_nonce_hi == handle.instance_nonce_hi &&
	    jr.value().new_nonce_lo == handle.instance_nonce_lo) {
		struct stat status {};
		if (::lstat(handle.socket_path.c_str(), &status) == 0 && S_ISSOCK(status.st_mode) &&
		    status.st_uid == ::geteuid() &&
		    static_cast<uint64_t>(status.st_dev) == handle.socket_dev &&
		    static_cast<uint64_t>(status.st_ino) == handle.socket_ino &&
		    ::unlink(handle.socket_path.c_str()) == 0)
			handle.socket_unlinked = true;
	}
}

void service_producer_shutdown(const std::shared_ptr<ProducerHandle>& handle) noexcept {
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
		mark_producer_offline(*handle);
	}
	handle->operation_in_use.store(false, std::memory_order_release);
}

void release_producer_loan_operation(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	handle->operation_in_use.store(false, std::memory_order_release);
	service_producer_shutdown(handle);
}

// 只有确认映射身份、代数和 Producer role_epoch 都未变化，句柄才允许写共享内存。
Result<ChannelHeaderAbi*> verify_handle_ownership(const ProducerHandle& handle,
                                                  const char* operation) noexcept {
	if (handle.controlled_stop.load(std::memory_order_acquire) || handle.control_closed)
		return make_error(ErrorCode::kClosed, operation, "stop requested");
	auto* base = static_cast<std::byte*>(handle.shm.mapping.get());
	auto* boot = reinterpret_cast<BootstrapHeaderAbi*>(base);
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	if (shared_load_acquire(&boot->init_state) != static_cast<uint32_t>(InitState::kReady) ||
	    shared_load_acquire(&header->init_state) != static_cast<uint32_t>(InitState::kReady)) {
		return make_error(ErrorCode::kStaleHandle, operation, "instance not ready");
	}
	if (header->generation != handle.generation ||
	    header->instance_nonce_hi != handle.instance_nonce_hi ||
	    header->instance_nonce_lo != handle.instance_nonce_lo) {
		return make_error(ErrorCode::kStaleHandle, operation, "instance replaced");
	}
	auto pid_res = identity_snapshot_read(&header->producer);
	if (!pid_res || pid_res.value().role_epoch != handle.role_epoch) {
		return make_error(ErrorCode::kStaleHandle, operation, "producer role changed");
	}
	if (shared_load_acquire(&header->producer_state) !=
	    static_cast<uint32_t>(EndpointState::kOnline)) {
		return make_error(ErrorCode::kProducerOffline, operation, "producer offline");
	}
	return Result<ChannelHeaderAbi*>(header);
}

bool payload_and_schema_valid(const ChannelOptions& options, const SchemaDescriptor& schema,
                              uint32_t payload_size) {
	if (options.name.empty() || options.name.size() > kMaxChannelNameLen ||
	    !validate_channel_name(options.name.c_str(), options.name.size())) {
		return false;
	}
	if (payload_size == 0 || payload_size > kMaxPayloadSize) return false;
	bool fingerprint_set = false;
	for (const std::byte b : schema.fingerprint) {
		if (b != std::byte{0}) {
			fingerprint_set = true;
			break;
		}
	}
	return fingerprint_set;
}

// 恢复旧事务时只处理已确认死亡的创建者；身份无法确认时保持现场并阻断操作。
Result<ControlJournalV1> reconcile_stale_journal(ControlLock& lock, const ControlJournalV1& journal,
                                                 const std::string& shm_name) {
	const auto state = static_cast<JournalState>(journal.state);
	if (state == JournalState::kIdle) return journal;
	if (!process_identity_valid({journal.creator.pid, journal.creator.proc_start_ticks,
	                             journal.creator.boot_id_hash_hi,
	                             journal.creator.boot_id_hash_lo})) {
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
		                  "invalid prior creator");
	}

	const Liveness creator_liv =
	        probe_liveness(journal.creator.pid, journal.creator.proc_start_ticks);
	if (creator_liv == Liveness::kAlive || creator_liv == Liveness::kUnverifiable) {
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
		                  "prior transaction owned or unverifiable");
	}

	ControlJournalV1 reset = journal;
	reset.state = static_cast<uint32_t>(JournalState::kIdle);
	reset.target_dev = 0;
	reset.target_ino = 0;
	reset.new_generation = journal.old_generation;
	reset.new_nonce_hi = journal.old_nonce_hi;
	reset.new_nonce_lo = journal.old_nonce_lo;

	switch (state) {
		case JournalState::kCreatingPreObject: {
			auto existing = shm_open_existing(shm_name);
			if (!existing) {
				if (existing.error().code != ErrorCode::kNotFound)
					return existing.error();
				break;
			}
			uint64_t dev = 0, ino = 0, size = 0;
			auto fst = shm_fstat_and_capture(existing.value(), &dev, &ino, &size);
			if (!fst) return fst.error();

			if (size != 0) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "object under name after preobject crash");
			}
			auto available = control_check();
			if (!available) return available.error();
			auto un = shm_unlink_checked(shm_name, dev, ino);
			if (!un) return un.error();
			break;
		}
		case JournalState::kCreatingObject:
		case JournalState::kReplacing: {
			auto existing = shm_open_existing(shm_name);
			if (!existing) {
				if (existing.error().code != ErrorCode::kNotFound)
					return existing.error();
				break;
			}
			uint64_t dev = 0, ino = 0, size = 0;
			auto fst = shm_fstat_and_capture(existing.value(), &dev, &ino, &size);
			if (!fst) return fst.error();
			if (dev != journal.target_dev || ino != journal.target_ino) {
				return make_error(ErrorCode::kNameRaceDetected, "Producer::create",
				                  "object inode differs from journal target");
			}

			BootstrapHeaderAbi boot{};
			auto rboot = pread_full(existing.value().get(), &boot, sizeof(boot), 0);
			if (!rboot) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "bootstrap unreadable after object crash");
			}
			auto vboot = validate_bootstrap_parse(boot, size);
			if (!vboot) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "bootstrap corrupt after object crash");
			}
			if (boot.creator_nonce_hi != journal.new_nonce_hi ||
			    boot.creator_nonce_lo != journal.new_nonce_lo) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "creator nonce does not bind to journal");
			}
			if (boot.init_state == static_cast<uint32_t>(InitState::kReady)) {

				ChannelHeaderAbi header{};
				auto rh = pread_full(existing.value().get(), &header,
				                     sizeof(header), kChannelHeaderOffset);
				if (!rh) {
					return make_error(ErrorCode::kRecoveryBlocked,
					                  "Producer::create",
					                  "committed header unreadable");
				}
				reset.new_generation = header.generation;
				reset.new_nonce_hi = header.instance_nonce_hi;
				reset.new_nonce_lo = header.instance_nonce_lo;
				break;
			}
			auto un = shm_unlink_checked(shm_name, dev, ino);
			if (!un) return un.error();
			break;
		}
		case JournalState::kRemoving: {
			auto existing = shm_open_existing(shm_name);
			if (!existing) {
				if (existing.error().code != ErrorCode::kNotFound)
					return existing.error();
				break;
			}
			uint64_t dev = 0, ino = 0;
			auto fst = shm_fstat_and_capture(existing.value(), &dev, &ino, nullptr);
			if (!fst) return fst.error();
			if (dev != journal.target_dev || ino != journal.target_ino) {
				return make_error(ErrorCode::kNameRaceDetected, "Producer::create",
				                  "object inode differs from removal target");
			}

			break;
		}
		default:
			break;
	}

	auto wr = lock.write_journal(reset);
	if (!wr) return wr.error();
	return Result<ControlJournalV1>(reset);
}

}

// 创建流程在控制锁内完成，从日志恢复代数，最后才发布 READY。
Result<std::shared_ptr<ProducerHandle>> producer_create_impl(const ChannelOptions& options,
                                                             const SchemaDescriptor& schema,
                                                             uint32_t payload_size) {
	if (!payload_and_schema_valid(options, schema, payload_size)) {
		return make_error(ErrorCode::kInvalidOptions, "Producer::create",
		                  "name/schema/payload invalid");
	}
	auto contract = validate_channel_option_contract(options, "Producer::create");
	if (!contract) return contract.error();
	if (options.transport != Transport::kPosixShm &&
	    options.transport != Transport::kMemfdFdPass) {
		return make_error(ErrorCode::kInvalidOptions, "Producer::create",
		                  "unknown transport");
	}
	auto identity = read_current_process_identity();
	if (!identity) return identity.error();
	const ProcessIdentity self = identity.value();
	const bool fd_mode = options.transport == Transport::kMemfdFdPass;
	const std::string shm_name = channel_shm_name(options.name);
	const std::string lock_path = channel_lock_path(options.name);
	const std::string socket_path = channel_socket_path(options.name);
	const uint32_t name_hash = channel_name_hash(options.name.c_str(), options.name.size());
	auto handle = std::make_shared<ProducerHandle>();
	handle->owner_pid = self.pid;
	(void)CancellationAccess::native_handle(handle->control_stop.token());
	handle->shm.name = options.name;
	handle->channel_name = options.name;
	handle->lock_path = lock_path;
	handle->shm_name = shm_name;
	if (fd_mode) handle->socket_path = socket_path;
	auto cleanup_prepared = control_prepare_cleanup(lock_path, shm_name);
	if (!cleanup_prepared) return cleanup_prepared.error();

	auto lock_res = ControlLock::acquire(lock_path);
	if (!lock_res) return lock_res.error();
	ControlLock lock = std::move(lock_res.value());

	auto jr = lock.read_journal();
	if (!jr) return jr.error();
	ControlJournalV1 journal = jr.value();
	if (journal.channel_hash != 0 && journal.channel_hash != name_hash) {
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
		                  "journal channel mismatch");
	}
	if (journal.version == 0) {
		journal = make_control_journal(options.name, JournalState::kIdle, 0, 0, 0, 0, 0, 0,
		                               {});
	}
	if (journal.state != static_cast<uint32_t>(JournalState::kIdle)) {

		auto rec = reconcile_stale_journal(lock, journal, shm_name);
		if (!rec) return rec.error();
		journal = rec.value();
	}

	uint64_t generation = 0;
	if (!next_generation_from_journal(journal, &generation)) {
		return make_error(ErrorCode::kSequenceExhausted, "Producer::create",
		                  "generation exhausted");
	}

	if (journal.transport != static_cast<uint32_t>(options.transport) &&
	    journal.creator.pid != 0) {
		const Liveness prev = probe_liveness(journal.creator.pid,
		                                     journal.creator.proc_start_ticks);
		if (prev == Liveness::kAlive) {
			return make_error(ErrorCode::kAlreadyOwned, "Producer::create",
			                  "previous owner alive under other transport");
		}
		if (prev == Liveness::kUnverifiable) {
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "cannot verify previous owner transport");
		}

	}

	auto existing = shm_open_existing(shm_name);
	if (existing) {
		uint64_t edev = 0, eino = 0, esize = 0;
		auto fst = shm_fstat_and_capture(existing.value(), &edev, &eino, &esize);
		if (!fst) return fst.error();
		if (esize < sizeof(BootstrapHeaderAbi)) {
			return make_error(ErrorCode::kInitializationIncomplete, "Producer::create",
			                  "existing object below bootstrap size");
		}
		auto emap = mmap_region(existing.value(), esize);
		if (!emap) return emap.error();
		auto* ebase = static_cast<std::byte*>(emap.value().get());
		auto* eboot = reinterpret_cast<BootstrapHeaderAbi*>(ebase);
		auto vboot = validate_bootstrap_parse(*eboot, esize);
		if (!vboot) {
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "existing bootstrap corrupt; use edge_shm_ctl");
		}
		if (shared_load_acquire(&eboot->init_state) !=
		    static_cast<uint32_t>(InitState::kReady)) {
			return make_error(ErrorCode::kInitializationIncomplete, "Producer::create",
			                  "existing instance not ready");
		}
		auto* eheader = reinterpret_cast<ChannelHeaderAbi*>(ebase + kChannelHeaderOffset);
		if (std::memcmp(eheader->magic, kChannelHeaderMagic,
		                sizeof(kChannelHeaderMagic) - 1) != 0 ||
		    eheader->abi_major != kAbiMajor) {
			return make_error(ErrorCode::kCorruptHeader, "Producer::create",
			                  "existing header invalid");
		}
		auto pid_res = identity_snapshot_read(&eheader->producer);
		if (!pid_res) {
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "old producer identity unreadable");
		}
		const ProcessIdentityAbi& opid = pid_res.value();
		const uint32_t pstate = shared_load_acquire(&eheader->producer_state);
		const bool actively_owned =
		        pstate == static_cast<uint32_t>(EndpointState::kOnline) ||
		        pstate == static_cast<uint32_t>(EndpointState::kStopping) ||
		        pstate == static_cast<uint32_t>(EndpointState::kFault);
		if (actively_owned &&
		    !process_identity_valid({opid.pid, opid.proc_start_ticks, opid.boot_id_hash_hi,
		                             opid.boot_id_hash_lo})) {
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "invalid old producer");
		}
		if (opid.pid != 0) {
			const Liveness liv = probe_liveness(opid.pid, opid.proc_start_ticks);
			if (liv == Liveness::kAlive && actively_owned) {
				return make_error(ErrorCode::kAlreadyOwned, "Producer::create",
				                  "producer alive");
			}
			if (liv == Liveness::kUnverifiable && actively_owned) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "cannot verify old producer");
			}

		}

		if (journal.new_generation != 0 && journal.new_generation != eheader->generation) {
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "journal and segment generations disagree");
		}
		if (eheader->generation == UINT64_MAX) {
			return make_error(ErrorCode::kSequenceExhausted, "Producer::create",
			                  "generation wrap");
		}
		generation = eheader->generation + 1;
		auto committing = control_begin_commit();
		if (!committing) return committing.error();
		EDGE_FAILPOINT(C10);
		auto un = shm_unlink_checked(shm_name, edev, eino);
		if (!un) return un.error();
	} else if (existing.error().code != ErrorCode::kNotFound) {
		return existing.error();
	}

	uint8_t nonce_bytes[16]{};
	if (!random_bytes(nonce_bytes, sizeof(nonce_bytes))) {
		const int e = errno;
		return make_errno_error(ErrorCode::kSystemError, e, "Producer::create", "getrandom");
	}
	uint64_t nonce_hi = 0;
	uint64_t nonce_lo = 0;
	std::memcpy(&nonce_hi, nonce_bytes, 8);
	std::memcpy(&nonce_lo, nonce_bytes + 8, 8);

	JournalGuard journal_guard;
	journal_guard.lock = &lock;
	journal_guard.reset_to = journal;
	journal_guard.socket_path = socket_path;

	auto j0 = make_control_journal(options.name, JournalState::kCreatingPreObject,
	                               journal.old_generation, generation, journal.old_nonce_hi,
	                               journal.old_nonce_lo, nonce_hi, nonce_lo, self);
	j0.transport = static_cast<uint32_t>(options.transport);
	auto committing = control_begin_commit();
	if (!committing) return committing.error();
	journal_guard.armed = true;
	auto wj0 = lock.write_journal(j0);
	if (!wj0) return wj0.error();

	UniqueFd fd;
	uint64_t cdev = 0;
	uint64_t cino = 0;
	CreatedObjectGuard created;
	created.journal_guard = &journal_guard;
	if (fd_mode) {

		auto mfd = memfd_create_object(options.name);
		if (!mfd) return mfd.error();
		fd = std::move(mfd.value());
		EDGE_FAILPOINT(C16);
		auto cst = memfd_fstat_and_capture(fd, &cdev, &cino, nullptr);
		if (!cst) return cst.error();
	} else {
		auto fd_res = shm_open_create(shm_name);
		if (!fd_res) {
			if (fd_res.error().code == ErrorCode::kAlreadyOwned) {
				return make_error(ErrorCode::kNameRaceDetected, "Producer::create",
				                  "object appeared under name");
			}
			return fd_res.error();
		}
		fd = std::move(fd_res.value());
		EDGE_FAILPOINT(C01);
		created.name = shm_name;
		auto cst = shm_fstat_and_capture(fd, &cdev, &cino, nullptr);
		if (!cst) {
			journal_guard.armed = false;
			auto captured = shm_capture_created_identity(fd, &cdev, &cino);
			j0.state = static_cast<uint32_t>(JournalState::kCreatingObject);
			j0.target_dev = cdev;
			j0.target_ino = cino;
			auto retained = lock.write_journal(j0);
			control_retain_cleanup(lock, j0, journal, true, cdev, cino);
			if (!retained) return retained.error();
			if (!captured) return captured.error();
			auto removed = shm_unlink_checked(shm_name, cdev, cino);
			if (!removed && removed.error().code != ErrorCode::kNotFound)
				return removed.error();
			journal_guard.armed = true;
			return cst.error();
		}
		created.dev = cdev;
		created.ino = cino;
		created.armed = true;
	}

	auto j1 = make_control_journal(options.name, JournalState::kCreatingObject,
	                               journal.old_generation, generation, journal.old_nonce_hi,
	                               journal.old_nonce_lo, nonce_hi, nonce_lo, self);
	j1.transport = static_cast<uint32_t>(options.transport);
	j1.target_dev = cdev;
	j1.target_ino = cino;
	auto wj1 = lock.write_journal(j1);
	if (!wj1) return wj1.error();

	const uint64_t heartbeat_interval_ns =
	        static_cast<uint64_t>(options.heartbeat_interval.count() > 0
	                                     ? options.heartbeat_interval.count()
	                                     : 0);
	const uint16_t abi_minor =
	        heartbeat_interval_ns > 0 ? static_cast<uint16_t>(kAbiMinorMax) : kAbiMinor;
	BootstrapHeaderAbi boot{};
	std::memcpy(boot.magic, kBootstrapMagic, sizeof(kBootstrapMagic) - 1);
	boot.abi_major = kAbiMajor;
	boot.abi_minor = abi_minor;
	boot.header_size = sizeof(BootstrapHeaderAbi);
	uint64_t mapping = 0;
	if (!mapping_size_for_payload(payload_size, &mapping)) {
		return make_error(ErrorCode::kInvalidOptions, "Producer::create",
		                  "mapping size overflow");
	}
	boot.expected_mapping_size = mapping;
	boot.creator_nonce_hi = nonce_hi;
	boot.creator_nonce_lo = nonce_lo;
	boot.creator_pid = self.pid;
	boot.creator_proc_start_ticks = self.proc_start_ticks;
	boot.creator_boot_id_hash_hi = self.boot_id_hash_hi;
	boot.creator_boot_id_hash_lo = self.boot_id_hash_lo;
	boot.init_state = static_cast<uint32_t>(InitState::kInitializing);
	boot.bootstrap_checksum = bootstrap_checksum_of(boot);
	auto wboot = pwrite_full(fd.get(), &boot, sizeof(boot), 0);
	if (!wboot) return wboot.error();
	EDGE_FAILPOINT(C02);

	auto tr = shm_truncate(fd, mapping);
	if (!tr) return tr.error();
	auto mm = mmap_region(fd, mapping);
	if (!mm) return mm.error();
	MappedRegion region = std::move(mm.value());
	auto* base = static_cast<std::byte*>(region.get());

	std::memset(base + kChannelHeaderOffset, 0, mapping - kChannelHeaderOffset);

	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	std::memcpy(header->magic, kChannelHeaderMagic, sizeof(kChannelHeaderMagic) - 1);
	header->abi_major = kAbiMajor;
	header->abi_minor = abi_minor;
	header->header_size = sizeof(ChannelHeaderAbi);
	header->endian_marker = kEndianMarker;
	header->slot_count = kSlotCount;
	header->payload_size = payload_size;
	header->max_payload_size = kMaxPayloadSize;
	header->schema_version = schema.version;
	std::memcpy(header->schema_fingerprint, schema.fingerprint.data(), 32);
	header->mapping_size = mapping;
	header->generation = generation;
	header->instance_nonce_hi = nonce_hi;
	header->instance_nonce_lo = nonce_lo;
	header->producer_heartbeat_interval_ns = heartbeat_interval_ns;

	ProcessIdentityAbi pid_abi{};
	pid_abi.pid = self.pid;
	pid_abi.proc_start_ticks = self.proc_start_ticks;
	pid_abi.boot_id_hash_hi = self.boot_id_hash_hi;
	pid_abi.boot_id_hash_lo = self.boot_id_hash_lo;
	auto pidw = identity_snapshot_write(&header->producer, pid_abi);
	if (!pidw) return pidw.error();
	const uint64_t role_epoch = pidw.value();

	shared_store_relaxed(&header->init_state, static_cast<uint32_t>(InitState::kInitializing));
	shared_store_relaxed(&header->producer_state,
	                     static_cast<uint32_t>(EndpointState::kOnline));

	UniqueFd listen_fd;
	if (fd_mode) {
		auto available = control_check();
		if (!available) return available.error();
		auto b = fd_broker_bind(socket_path);
		if (!b) return b.error();
		listen_fd = std::move(b.value());
		struct stat status {};
		if (::lstat(socket_path.c_str(), &status) != 0) {
			journal_guard.socket_ino = UINT64_MAX;
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "bound socket identity unavailable");
		}
		journal_guard.socket_dev = static_cast<uint64_t>(status.st_dev);
		journal_guard.socket_ino = static_cast<uint64_t>(status.st_ino);
		handle->socket_dev = journal_guard.socket_dev;
		handle->socket_ino = journal_guard.socket_ino;
		EDGE_FAILPOINT(C17);
	}

	auto* boot_map = reinterpret_cast<BootstrapHeaderAbi*>(base);

	auto jdone = make_control_journal(options.name, JournalState::kIdle, journal.old_generation,
	                                  generation, journal.old_nonce_hi, journal.old_nonce_lo,
	                                  nonce_hi, nonce_lo, self);
	jdone.transport = static_cast<uint32_t>(options.transport);
	jdone.target_dev = cdev;
	jdone.target_ino = cino;
	const bool controlled = control_context() != nullptr;
	auto available = control_begin_commit();
	if (!available) return available.error();
	handle->shm.fd = std::move(fd);
	handle->shm.mapping = std::move(region);
	handle->shm.dev = cdev;
	handle->shm.ino = cino;
	handle->shm.size = mapping;
	handle->generation = generation;
	handle->role_epoch = role_epoch;
	handle->instance_nonce_hi = nonce_hi;
	handle->instance_nonce_lo = nonce_lo;
	handle->schema_fingerprint = schema.fingerprint;
	handle->schema_version = schema.version;
	handle->payload_size = payload_size;
	handle->self = self;
	handle->transport = options.transport;
	handle->channel_hash = name_hash;
	handle->heartbeat_interval_ns = heartbeat_interval_ns;
	if (fd_mode) {
		handle->listen_fd = std::move(listen_fd);
	}

	auto start_server = [&]() -> Result<void> {
		if (!fd_mode) return Result<void>::ok();
		const int listen = handle->listen_fd.get();
		const int shm_fd = handle->shm.fd.get();
		std::byte* serve_base = static_cast<std::byte*>(handle->shm.mapping.get());
		try {
			handle->server_thread = std::thread([handle_pointer = handle.get(), listen,
			                                     shm_fd, serve_base] {
				fd_broker_serve_loop(listen, shm_fd, serve_base,
				                     &handle_pointer->channel_hash,
				                     &handle_pointer->schema_fingerprint,
				                     &handle_pointer->serve_stop);
				handle_pointer->serve_exited.store(true, std::memory_order_release);
			});
		} catch (...) {
			return make_error(ErrorCode::kSystemError, "Producer::create",
			                  "serving thread start failed");
		}
		return Result<void>::ok();
	};
	if (controlled) {
		auto started = start_server();
		if (!started) return started.error();
	}
	shared_store_release(&header->init_state, static_cast<uint32_t>(InitState::kReady));
	shared_store_release(&boot_map->init_state, static_cast<uint32_t>(InitState::kReady));
	control_committed();
	EDGE_FAILPOINT(CONTROL_CREATE_COMMITTED);
	if (controlled) {
		journal_guard.armed = false;
		created.armed = false;
		auto wjdone = lock.write_journal(jdone);
		if (!wjdone) control_retain_cleanup(lock, j1, jdone, false, 0, 0);
	} else {
		auto wjdone = lock.write_journal(jdone);
		if (!wjdone) return wjdone.error();
		auto started = start_server();
		if (!started) return started.error();
		journal_guard.armed = false;
		created.armed = false;
	}
	return Result<std::shared_ptr<ProducerHandle>>(std::move(handle));
}

// 发布先占用非 latest 槽，完成载荷和校验和写入后再推进 latest_ticket。
Result<PublishInfo> producer_publish_impl(const std::shared_ptr<ProducerHandle>& handle,
                                          const std::byte* encoded,
                                          uint32_t encoded_size) noexcept {
	ProducerUseGuard use(&handle->operation_in_use);
	if (!use) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Producer::publish",
		                  "concurrent handle use");
	}

	if (encoded_size != handle->payload_size) {
		return make_error(ErrorCode::kPayloadEncodeFailed, "Producer::publish",
		                  "codec size drift");
	}

	auto v = verify_handle_ownership(*handle, "Producer::publish");
	if (!v) return v.error();
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = v.value();

	const uint64_t current = shared_load_acquire(&header->latest_ticket);
	uint64_t next_sequence = 0;
	if (!checked_next_sequence(current, &next_sequence)) {
		return make_error(ErrorCode::kSequenceExhausted, "Producer::publish",
		                  "sequence exhausted");
	}
	const uint64_t publish_boot_ns = boottime_now_ns();
	if (publish_boot_ns == 0) {
		return make_error(ErrorCode::kClockAnomaly, "Producer::publish",
		                  "boottime unavailable");
	}
	const uint64_t checksum = fnv1a64(encoded, static_cast<size_t>(encoded_size));

	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(kSlotHeaderSize + handle->payload_size, 64, &stride)) {
		return make_error(ErrorCode::kInvalidOptions, "Producer::publish",
		                  "stride overflow");
	}
	const uint32_t current_slot = current == 0 ? kInvalidSlot : ticket_slot(current);

	SlotHeaderAbi* chosen = nullptr;
	uint32_t chosen_index = 0;
	for (uint32_t i = 0; i < kSlotCount; ++i) {
		if (i == current_slot) continue;
		uint64_t offset = 0;
		if (!slot_byte_offset(i, stride, &offset)) continue;
		auto* slot = reinterpret_cast<SlotHeaderAbi*>(base + offset);
		const uint32_t observed = shared_load_relaxed(&slot->state);
		if (observed != static_cast<uint32_t>(SlotState::kFree) &&
		    observed != static_cast<uint32_t>(SlotState::kPublished)) {
			continue;
		}
		if (slot_claim_writable(slot, observed)) {
			chosen = slot;
			chosen_index = i;
			break;
		}
	}
	if (chosen == nullptr) {
		return make_error(ErrorCode::kNoWritableSlot, "Producer::publish",
		                  "all slots busy");
	}
	EDGE_FAILPOINT(C03);

	shared_store_relaxed(&chosen->payload_size, encoded_size);
	shared_store_relaxed(&chosen->sample_sequence, next_sequence);
	shared_store_relaxed(&chosen->publish_boot_ns, publish_boot_ns);
	auto* payload =
	        reinterpret_cast<std::byte*>(chosen) + static_cast<uint64_t>(kSlotHeaderSize);
	std::memcpy(payload, encoded, static_cast<size_t>(encoded_size));
	EDGE_FAILPOINT(C04);
	shared_store_relaxed(&chosen->payload_checksum, checksum);

	slot_publish(chosen);
	EDGE_FAILPOINT(C05);

	shared_store_release(&header->latest_ticket, make_ticket(next_sequence, chosen_index));
	EDGE_FAILPOINT(C06);
	shared_fetch_add_relaxed(&header->publish_count, uint64_t{1});
	shared_store_relaxed(&header->last_publish_boot_ns, publish_boot_ns);
	shared_fetch_add_relaxed(&header->notify_epoch, uint32_t{1});
	(void)futex_wake(&header->notify_epoch, 1);
	if (handle->notification_server) handle->notification_server->notify();

	return Result<PublishInfo>(PublishInfo{handle->generation, next_sequence, publish_boot_ns});
}

uint64_t producer_generation_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	return handle ? handle->generation : 0;
}

Result<void> producer_enable_event_notifications_impl(const std::shared_ptr<ProducerHandle>& handle) {
	return producer_enable_event_notifications_impl(handle, NotificationBackend::kEventFd);
}

Result<void> producer_enable_event_notifications_impl(const std::shared_ptr<ProducerHandle>& handle,
                                                      NotificationBackend backend) {
	if (!handle) return make_error(ErrorCode::kStaleHandle, "Producer::enable_event_notifications");
	ProducerUseGuard use(&handle->operation_in_use);
	if (!use) return make_error(ErrorCode::kConcurrentHandleUse, "Producer::enable_event_notifications");
	auto verified = verify_handle_ownership(*handle, "Producer::enable_event_notifications");
	if (!verified) return verified.error();
	if (handle->notification_server) {
		if (handle->notification_server->stopping())
			return make_error(ErrorCode::kClosed,
			                  "Producer::enable_event_notifications",
			                  "finalize disable first");
		if (handle->notification_server->backend() != backend)
			return make_error(ErrorCode::kInvalidOptions,
			                  "Producer::enable_event_notifications",
			                  "disable notifications before switching backend");
		return Result<void>::ok();
	}
	auto server = NotificationServer::start(*handle, backend);
	if (!server) return server.error();
	handle->notification_server = std::move(server.value());
	return Result<void>::ok();
}

Result<void> producer_disable_event_notifications_impl(
        const std::shared_ptr<ProducerHandle>& handle) noexcept {
	if (!handle) return make_error(ErrorCode::kStaleHandle, "Producer::disable_event_notifications");
	ProducerUseGuard use(&handle->operation_in_use);
	if (!use) return make_error(ErrorCode::kConcurrentHandleUse, "Producer::disable_event_notifications");
	if (handle->notification_server) handle->notification_server->stop();
	handle->notification_server.reset();
	return Result<void>::ok();
}

int producer_event_notification_error_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	return handle && handle->notification_server ? handle->notification_server->last_error() : 0;
}

Result<ChannelStatus> producer_status_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	if (handle->control_closed) return make_error(ErrorCode::kClosed, "Producer::status");
	ProducerUseGuard use(&handle->operation_in_use);
	if (!use) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Producer::status",
		                  "concurrent handle use");
	}

	if (handle->transport == Transport::kPosixShm) {

		const std::string shm_name = channel_shm_name(handle->channel_name);
		auto re = shm_open_existing(shm_name);
		if (!re) {
			return make_error(ErrorCode::kStaleHandle, "Producer::status",
			                  "channel name gone");
		}
		uint64_t dev = 0, ino = 0;
		auto fst = shm_fstat_and_capture(re.value(), &dev, &ino, nullptr);
		if (!fst) return fst.error();
		if (dev != handle->shm.dev || ino != handle->shm.ino) {
			return make_error(ErrorCode::kStaleHandle, "Producer::status",
			                  "instance replaced under name");
		}
	}

	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	return read_channel_status(base);
}

void producer_shutdown_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	if (handle->control_closed) return;
	handle->legacy_cleanup.store(true, std::memory_order_release);
	handle->shutdown_pending.store(true, std::memory_order_release);
	service_producer_shutdown(handle);
}

// 借用路径只预留槽并保持 WRITING，具体编码由调用方完成，commit() 才进入发布流程。
Result<WriteLoan> producer_loan_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	if (handle->operation_in_use.exchange(true, std::memory_order_acq_rel)) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Producer::loan",
		                  "concurrent handle use");
	}
	auto release_on_error = [&handle]() {
		handle->operation_in_use.store(false, std::memory_order_release);
	};

	auto verified = verify_handle_ownership(*handle, "Producer::loan");
	if (!verified) {
		release_on_error();
		return verified.error();
	}
	auto* header = verified.value();
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	const uint64_t current = shared_load_acquire(&header->latest_ticket);
	uint64_t next_sequence = 0;
	if (!checked_next_sequence(current, &next_sequence)) {
		release_on_error();
		return make_error(ErrorCode::kSequenceExhausted, "Producer::loan",
		                  "sequence exhausted");
	}

	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(kSlotHeaderSize + handle->payload_size, 64, &stride)) {
		release_on_error();
		return make_error(ErrorCode::kInvalidOptions, "Producer::loan", "stride overflow");
	}
	const uint32_t current_slot = current == 0 ? kInvalidSlot : ticket_slot(current);
	SlotHeaderAbi* chosen = nullptr;
	uint32_t chosen_index = 0;
	for (uint32_t i = 0; i < kSlotCount; ++i) {
		if (i == current_slot) continue;
		uint64_t offset = 0;
		if (!slot_byte_offset(i, stride, &offset)) continue;
		auto* slot = reinterpret_cast<SlotHeaderAbi*>(base + offset);
		const uint32_t observed = shared_load_relaxed(&slot->state);
		if (observed != static_cast<uint32_t>(SlotState::kFree) &&
		    observed != static_cast<uint32_t>(SlotState::kPublished)) {
			continue;
		}
		if (slot_claim_writable(slot, observed)) {
			chosen = slot;
			chosen_index = i;
			break;
		}
	}
	if (chosen == nullptr) {
		release_on_error();
		return make_error(ErrorCode::kNoWritableSlot, "Producer::loan", "all slots busy");
	}
	EDGE_FAILPOINT(C22);
	auto* payload = reinterpret_cast<std::byte*>(chosen) + kSlotHeaderSize;
	return WriteLoan(handle, chosen, payload, handle->payload_size, chosen_index, next_sequence);
}

// commit 失败时回滚槽并释放句柄占用，不能把半写载荷暴露为 PUBLISHED。
Result<PublishInfo> producer_commit_loan_impl(WriteLoan* loan) noexcept {
	if (loan == nullptr || !loan->active_ || !loan->handle_ || loan->slot_ == nullptr) {
		return make_error(ErrorCode::kInvalidOptions, "WriteLoan::commit", "loan inactive");
	}
	const std::shared_ptr<ProducerHandle> handle = loan->handle_;
	auto* slot = static_cast<SlotHeaderAbi*>(loan->slot_);
	auto fail = [loan, &handle, slot](const Error& error) -> Result<PublishInfo> {
		slot_abort_write(slot);
		loan->active_ = false;
		loan->slot_ = nullptr;
		loan->data_ = nullptr;
		loan->handle_.reset();
		release_producer_loan_operation(handle);
		return error;
	};

	auto verified = verify_handle_ownership(*handle, "WriteLoan::commit");
	if (!verified) return fail(verified.error());
	const uint64_t publish_boot_ns = boottime_now_ns();
	if (publish_boot_ns == 0) {
		return fail(make_error(ErrorCode::kClockAnomaly, "WriteLoan::commit",
		                       "boottime unavailable"));
	}
	const uint64_t checksum = fnv1a64(loan->data_, static_cast<size_t>(loan->size_));

	shared_store_relaxed(&slot->payload_size, loan->size_);
	shared_store_relaxed(&slot->sample_sequence, loan->sequence_);
	shared_store_relaxed(&slot->publish_boot_ns, publish_boot_ns);
	EDGE_FAILPOINT(C04);
	shared_store_relaxed(&slot->payload_checksum, checksum);
	slot_publish(slot);
	EDGE_FAILPOINT(C05);
	auto* header = verified.value();
	shared_store_release(&header->latest_ticket, make_ticket(loan->sequence_, loan->slot_index_));
	EDGE_FAILPOINT(C06);
	shared_fetch_add_relaxed(&header->publish_count, uint64_t{1});
	shared_store_relaxed(&header->last_publish_boot_ns, publish_boot_ns);
	shared_fetch_add_relaxed(&header->notify_epoch, uint32_t{1});
	(void)futex_wake(&header->notify_epoch, 1);
	if (handle->notification_server) handle->notification_server->notify();

	const PublishInfo info{handle->generation, loan->sequence_, publish_boot_ns};
	loan->active_ = false;
	loan->slot_ = nullptr;
	loan->data_ = nullptr;
	loan->handle_.reset();
	release_producer_loan_operation(handle);
	return Result<PublishInfo>(info);
}

void producer_abort_loan_impl(WriteLoan* loan) noexcept {
	if (loan == nullptr || !loan->active_ || !loan->handle_) return;
	const std::shared_ptr<ProducerHandle> handle = loan->handle_;
	if (loan->slot_ != nullptr) slot_abort_write(static_cast<SlotHeaderAbi*>(loan->slot_));
	loan->active_ = false;
	loan->slot_ = nullptr;
	loan->data_ = nullptr;
	loan->handle_.reset();
	release_producer_loan_operation(handle);
}

// 删除必须重新确认实例身份，并通过日志记录 REMOVING 到最终 Idle 的完整过程。
Result<void> producer_remove_if_owner_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	ProducerUseGuard use(&handle->operation_in_use);
	if (!use) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Producer::remove_if_owner",
		                  "concurrent handle use");
	}
	if (handle->control_closed)
		return make_error(ErrorCode::kClosed, "Producer::remove_if_owner");

	const bool fd_mode = handle->transport == Transport::kMemfdFdPass;
	const std::string& shm_name = handle->shm_name;
	const std::string& lock_path = handle->lock_path;

	auto lock_res = ControlLock::acquire(lock_path);
	if (!lock_res) return lock_res.error();
	ControlLock lock = std::move(lock_res.value());
	auto prepared = control_prepare_cleanup(lock_path, shm_name);
	if (!prepared) return prepared;

	auto jr = lock.read_journal();
	if (!jr) return jr.error();
	const ControlJournalV1 journal = jr.value();

	uint64_t dev = 0, ino = 0;
	if (!fd_mode) {
		auto re = shm_open_existing(shm_name);
		if (!re) return re.error();
		auto fst = shm_fstat_and_capture(re.value(), &dev, &ino, nullptr);
		if (!fst) return fst.error();
		if (dev != handle->shm.dev || ino != handle->shm.ino) {
			return make_error(ErrorCode::kNameRaceDetected, "Producer::remove_if_owner",
			                  "inode changed");
		}
	}

	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	if (header->generation != handle->generation ||
	    header->instance_nonce_hi != handle->instance_nonce_hi ||
	    header->instance_nonce_lo != handle->instance_nonce_lo) {
		return make_error(ErrorCode::kStaleHandle, "Producer::remove_if_owner",
		                  "instance changed");
	}
	auto pid_res = identity_snapshot_read(&header->producer);
	if (!pid_res) return pid_res.error();
	if (pid_res.value().role_epoch != handle->role_epoch) {
		return make_error(ErrorCode::kStaleHandle, "Producer::remove_if_owner",
		                  "producer role changed");
	}

	if (handle->notification_server && control_context()) {
		auto finished = handle->notification_server->finalize(*control_context());
		if (!finished) {
			control_handle_pending(finished.error());
			return finished;
		}
	}
	auto committing = control_begin_commit();
	if (!committing) return committing;
	if (fd_mode) {
		handle->serve_stop.store(true, std::memory_order_relaxed);
		if (handle->listen_fd.get() >= 0) {
			(void)::shutdown(handle->listen_fd.get(), SHUT_RDWR);
		}
	}
	if (handle->notification_server) handle->notification_server->stop();
	handle->notification_server.reset();

	JournalGuard journal_guard;
	journal_guard.lock = &lock;
	journal_guard.reset_to = journal;

	auto jr2 = make_control_journal(handle->channel_name, JournalState::kRemoving,
	                                handle->generation, 0, handle->instance_nonce_hi,
	                                handle->instance_nonce_lo, 0, 0, handle->self);
	jr2.transport = static_cast<uint32_t>(handle->transport);
	jr2.target_dev = dev;
	jr2.target_ino = ino;
	journal_guard.armed = true;
	auto wj = lock.write_journal(jr2);
	if (!wj) return wj.error();
	committing = control_begin_commit();
	if (!committing) return committing;

	if (fd_mode) {

		if (!handle->socket_unlinked) {
			struct stat status {};
			if (::lstat(handle->socket_path.c_str(), &status) != 0 ||
			    static_cast<uint64_t>(status.st_dev) != handle->socket_dev ||
			    static_cast<uint64_t>(status.st_ino) != handle->socket_ino)
				return make_error(ErrorCode::kNameRaceDetected,
				                  "Producer::remove_if_owner",
				                  "socket identity changed");
			if (::unlink(handle->socket_path.c_str()) != 0)
				return make_errno_error(errno, "Producer::remove_if_owner",
				                        "unlink socket");
			handle->socket_unlinked = true;
		}
	} else {
		auto un = shm_unlink_checked(shm_name, dev, ino);
		if (!un) return un.error();
	}
	control_committed();
	journal_guard.armed = false;

	auto jdone = make_control_journal(
	        handle->channel_name, JournalState::kIdle, handle->generation, handle->generation,
	        handle->instance_nonce_hi, handle->instance_nonce_lo, handle->instance_nonce_hi,
	        handle->instance_nonce_lo, handle->self);
	jdone.transport = static_cast<uint32_t>(handle->transport);
	jdone.target_dev = dev;
	jdone.target_ino = ino;
	auto wjd = lock.write_journal(jdone);
	if (!wjd) {
		control_retain_cleanup(lock, jr2, jdone, false, 0, 0);
		return control_context() ? Result<void>::ok() : wjd;
	}
	return Result<void>::ok();
}

Result<std::shared_ptr<ProducerHandle>> producer_create_controlled(
        const ChannelOptions& options, const SchemaDescriptor& schema, uint32_t payload_size,
        const ControlOperationContext& context, ControlReceipt& receipt) {
	if (receipt.has_pending_cleanup())
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
		                  "finalize receipt first");
	ControlScope scope(context, receipt);
	auto checked = control_check();
	if (!checked) return checked.error();
	try {
		return producer_create_impl(options, schema, payload_size);
	} catch (...) {
		return make_errno_error(ENOMEM, "Producer::create", "allocation before commit");
	}
}

Result<void> producer_enable_controlled(const std::shared_ptr<ProducerHandle>& handle,
                                        NotificationBackend backend,
                                        const ControlOperationContext& context,
                                        ControlReceipt& receipt) {
	if (handle && handle->owner_pid != static_cast<uint64_t>(::getpid()))
		return make_error(ErrorCode::kStaleHandle, "Producer::enable",
		                  "foreign process handle");
	if (receipt.has_pending_cleanup())
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::enable",
		                  "finalize receipt first");
	ControlScope scope(context, receipt,
	                   handle ? handle->control_stop.token() : CancellationToken{});
	auto checked = control_check();
	if (!checked) return checked;
	auto enabled = producer_enable_event_notifications_impl(handle, backend);
	if (enabled)
		control_committed();
	else {
		checked = control_check();
		if (!checked) return checked;
	}
	return enabled;
}

Result<ControlCloseStatus> producer_disable_controlled(
        const std::shared_ptr<ProducerHandle>& handle, const ControlOperationContext& context,
        ControlReceipt& receipt) noexcept {
	if (!handle) return make_error(ErrorCode::kClosed, "Producer::disable");
	if (handle->owner_pid != static_cast<uint64_t>(::getpid()))
		return make_error(ErrorCode::kStaleHandle, "Producer::disable",
		                  "foreign process handle");
	if (receipt.has_pending_cleanup())
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::disable",
		                  "finalize receipt first");
	ControlScope scope(context, receipt);
	ProducerUseGuard use(&handle->operation_in_use);
	if (!use) return make_error(ErrorCode::kConcurrentHandleUse, "Producer::disable");
	auto available = context.check();
	if (!available) return available.error();
	if (handle->notification_server) {
		auto checked = context.check();
		if (!checked) return checked.error();
		auto stopped = handle->notification_server->finalize(context);
		if (!stopped) {
			control_handle_pending(stopped.error());
			return ControlCloseStatus::kPending;
		}
		handle->notification_server.reset();
	}
	control_committed();
	return ControlCloseStatus::kClosed;
}

Result<void> producer_remove_controlled(const std::shared_ptr<ProducerHandle>& handle,
                                        const ControlOperationContext& context,
                                        ControlReceipt& receipt) noexcept {
	if (!handle) return make_error(ErrorCode::kClosed, "Producer::remove");
	if (handle->owner_pid != static_cast<uint64_t>(::getpid()))
		return make_error(ErrorCode::kStaleHandle, "Producer::remove",
		                  "foreign process handle");
	if (receipt.has_pending_cleanup())
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::remove",
		                  "finalize receipt first");
	auto stop = handle->control_stop.token();
	if (stop.is_cancelled()) stop = {};
	ControlScope scope(context, receipt, std::move(stop));
	auto checked = control_check();
	if (!checked) return checked;
	return producer_remove_if_owner_impl(handle);
}

void producer_request_stop(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	if (!handle) return;
	if (handle->owner_pid != static_cast<uint64_t>(::getpid())) return;
	handle->controlled_stop.store(true, std::memory_order_release);
	handle->shutdown_pending.store(true, std::memory_order_release);
	handle->serve_stop.store(true, std::memory_order_release);
	(void)handle->control_stop.request_cancel();
}

Result<ControlCloseStatus> producer_finalize(const std::shared_ptr<ProducerHandle>& handle,
                                             const ControlOperationContext& context,
                                             ControlReceipt& receipt) noexcept {
	if (!handle) return ControlCloseStatus::kClosed;
	if (handle->owner_pid != static_cast<uint64_t>(::getpid()))
		return make_error(ErrorCode::kStaleHandle, "Producer::finalize",
		                  "foreign process handle");
	if (receipt.has_pending_cleanup())
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::finalize",
		                  "finalize receipt first");
	ControlScope scope(context, receipt);
	producer_request_stop(handle);
	control_handle_pending();
	ProducerUseGuard use(&handle->operation_in_use);
	if (!use) {
		control_handle_pending();
		return ControlCloseStatus::kPending;
	}
	if (handle->control_closed) {
		control_committed();
		return ControlCloseStatus::kClosed;
	}
	auto pending = [&]() -> Result<ControlCloseStatus> {
		auto checked = context.check();
		control_handle_pending(checked ? make_error(ErrorCode::kConcurrentHandleUse,
		                                            "Producer::finalize",
		                                            "service cleanup pending")
		                               : checked.error());
		return ControlCloseStatus::kPending;
	};
	if (!context.check()) return pending();
	if (handle->notification_server) {
		auto stopped = handle->notification_server->finalize(context);
		if (!stopped) {
			control_handle_pending(stopped.error());
			return ControlCloseStatus::kPending;
		}
		handle->notification_server.reset();
	}
	while (handle->server_thread.joinable() &&
	       !handle->serve_exited.load(std::memory_order_acquire)) {
		if (!control_pause(1'000'000)) return pending();
	}
	if (!context.check()) return pending();
	if (handle->server_thread.joinable()) handle->server_thread.join();
	if (handle->transport == Transport::kMemfdFdPass && !handle->socket_unlinked) {
		auto locked = ControlLock::acquire(handle->lock_path);
		if (!locked)
			return locked.error().code == ErrorCode::kTimeout ||
			                       locked.error().code == ErrorCode::kCancelled
			               ? pending()
			               : Result<ControlCloseStatus>(locked.error());
		auto journal = locked.value().read_journal();
		if (!journal) return journal.error();
		if (journal.value().new_generation != handle->generation ||
		    journal.value().new_nonce_hi != handle->instance_nonce_hi ||
		    journal.value().new_nonce_lo != handle->instance_nonce_lo)
			return make_error(ErrorCode::kStaleHandle, "Producer::finalize",
			                  "journal identity changed");
		struct stat status {};
		if (::lstat(handle->socket_path.c_str(), &status) == 0) {
			if (!S_ISSOCK(status.st_mode) ||
			    static_cast<uint64_t>(status.st_dev) != handle->socket_dev ||
			    static_cast<uint64_t>(status.st_ino) != handle->socket_ino)
				return make_error(ErrorCode::kNameRaceDetected,
				                  "Producer::finalize", "socket identity changed");
			if (!control_begin_commit()) return pending();
			if (::unlink(handle->socket_path.c_str()) != 0)
				return make_errno_error(errno, "Producer::finalize",
				                        "unlink socket");
		} else if (errno != ENOENT)
			return make_errno_error(errno, "Producer::finalize", "lstat socket");
		handle->socket_unlinked = true;
	}
	if (!context.check()) return pending();
	mark_producer_offline(*handle);
	handle->listen_fd.reset();
	handle->shm.mapping.reset();
	handle->shm.fd.reset();
	handle->control_closed = true;
	handle->shutdown_started.store(true, std::memory_order_release);
	control_committed();
	return ControlCloseStatus::kClosed;
}

// 心跳只表示应用仍在推进，不改变所有权，也不能单独触发接管。
Result<void> producer_heartbeat_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	ProducerUseGuard use(&handle->operation_in_use);
	if (!use) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Producer::heartbeat",
		                  "concurrent handle use");
	}

	if (handle->heartbeat_interval_ns == 0) {
		return Result<void>::ok();
	}
	auto v = verify_handle_ownership(*handle, "Producer::heartbeat");
	if (!v) return v.error();
	auto* header = v.value();

	const uint64_t now = boottime_now_ns();
	if (now == 0) {
		return make_error(ErrorCode::kClockAnomaly, "Producer::heartbeat",
		                  "boottime unavailable");
	}
	shared_store_release(&header->heartbeat_boot_ns, now);
	EDGE_FAILPOINT(C18);
	return Result<void>::ok();
}

}
