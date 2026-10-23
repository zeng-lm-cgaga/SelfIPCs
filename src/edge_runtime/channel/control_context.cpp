#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <exception>
#include <utility>

#include "edge_runtime/channel/control_operation.hpp"
#include "edge_runtime/sync/cancellation.hpp"
#include "edge_runtime/sync/clock.hpp"

namespace edge_runtime {

ControlReceipt::ControlReceipt() noexcept = default;
ControlReceipt::ControlReceipt(ControlReceipt&&) noexcept = default;
ControlReceipt::~ControlReceipt() {
	if (has_pending_cleanup()) {
		std::fputs(
		        "SelfIPCs: pending ControlReceipt must be finalized before destruction\n",
		        stderr);
		std::terminate();
	}
}

bool ControlReceipt::has_pending_cleanup() const noexcept {
	return recovery_ && recovery_->pending;
}

Result<void> ControlReceipt::finalize(const ControlOperationContext& context) noexcept {
	if (cleanup_owner == ControlCleanupOwner::kHandle)
		return make_error(ErrorCode::kInvalidOptions, "ControlReceipt::finalize",
		                  "continue through the original handle");
	if (!has_pending_cleanup()) return Result<void>::ok();
	auto& recovery = recovery_;
	if (recovery->owner_pid != static_cast<uint64_t>(::getpid()))
		return make_error(ErrorCode::kStaleHandle, "ControlReceipt::finalize",
		                  "foreign process receipt");
	ControlReceipt attempt;
	detail::ControlScope scope(context, attempt);
	auto checked = context.check();
	if (!checked) return checked;
	auto locked = detail::ControlLock::acquire(recovery->lock_path);
	if (!locked) return locked.error();
	if (locked.value().dev() != recovery->lock_dev ||
	    locked.value().ino() != recovery->lock_ino)
		return make_error(ErrorCode::kNameRaceDetected, "ControlReceipt::finalize",
		                  "control inode changed");
	auto journal = locked.value().read_journal();
	if (!journal) return journal.error();
	const auto& current = journal.value();
	auto comparable = current;
	comparable.record_checksum = recovery->reset.record_checksum;
	const bool already_reset =
	        std::memcmp(&comparable, &recovery->reset, sizeof(comparable)) == 0;
	comparable.record_checksum = recovery->expected.record_checksum;
	const bool expected =
	        std::memcmp(&comparable, &recovery->expected, sizeof(comparable)) == 0;
	comparable.record_checksum = recovery->alternate.record_checksum;
	const bool alternate =
	        std::memcmp(&comparable, &recovery->alternate, sizeof(comparable)) == 0;
	if (!already_reset && !expected && !alternate)
		return make_error(ErrorCode::kRecoveryBlocked, "ControlReceipt::finalize",
		                  "journal identity changed");
	checked = detail::control_begin_commit();
	if (!checked) return checked;
	if (recovery->socket_ino != 0) {
		struct stat status {};
		if (::lstat(recovery->socket_path.c_str(), &status) == 0) {
			if (!S_ISSOCK(status.st_mode) || status.st_uid != ::geteuid() ||
			    static_cast<uint64_t>(status.st_dev) != recovery->socket_dev ||
			    static_cast<uint64_t>(status.st_ino) != recovery->socket_ino)
				return make_error(ErrorCode::kNameRaceDetected,
				                  "ControlReceipt::finalize",
				                  "socket identity changed");
			if (::unlink(recovery->socket_path.c_str()) != 0)
				return make_errno_error(errno, "ControlReceipt::finalize",
				                        "unlink socket");
		} else if (errno != ENOENT)
			return make_errno_error(errno, "ControlReceipt::finalize", "lstat socket");
	}
	if (recovery->remove_object) {
		auto removed = detail::shm_unlink_checked(recovery->shm_name, recovery->dev,
		                                          recovery->ino);
		if (!removed && removed.error().code != ErrorCode::kNotFound) return removed;
	}
	auto result = locked.value().write_journal(recovery->reset);
	if (result) {
		recovery.reset();
		completion =
		        committed ? ControlCompletion::kCommitted : ControlCompletion::kUncommitted;
		cleanup_owner = ControlCleanupOwner::kNone;
	}
	return result;
}

uint32_t control_context_feature_version() noexcept { return kControlContextFeatureVersion; }

ControlOperationContext::ControlOperationContext(uint64_t deadline,
                                                 CancellationToken token) noexcept
    : deadline_ns_(deadline), cancellation_(std::move(token)) {}

Result<ControlOperationContext> ControlOperationContext::after(std::chrono::nanoseconds timeout,
                                                               CancellationToken token) noexcept {
	if (timeout.count() < 0)
		return make_error(ErrorCode::kInvalidOptions, "ControlOperationContext::after",
		                  "negative timeout");
	const uint64_t now = detail::monotonic_now_ns();
	const uint64_t duration = static_cast<uint64_t>(timeout.count());
	if (now == 0 || duration > UINT64_MAX - now)
		return make_error(ErrorCode::kClockAnomaly, "ControlOperationContext::after",
		                  "clock/overflow");
	return ControlOperationContext(now + duration, std::move(token));
}

Result<void> ControlOperationContext::check() const noexcept {
	if (cancellation_.is_cancelled())
		return make_error(ErrorCode::kCancelled, "control operation", "cancelled");
	if (deadline_ns_ == 0 || detail::remaining_time_ns(deadline_ns_) == 0)
		return make_error(ErrorCode::kTimeout, "control operation",
		                  "absolute deadline exhausted");
	return Result<void>::ok();
}

}  // namespace edge_runtime

namespace edge_runtime::detail {
namespace {
thread_local ControlScope* active_scope = nullptr;
}

struct ControlReceiptAccess {
	static std::unique_ptr<ControlRecovery>& recovery(ControlReceipt& receipt) noexcept {
		return receipt.recovery_;
	}
};

ControlScope::ControlScope(const ControlOperationContext& context, ControlReceipt& receipt,
                           CancellationToken stop) noexcept
    : previous_(active_scope), context_(context), receipt_(receipt), stop_(std::move(stop)) {
	if (receipt_.has_pending_cleanup()) std::terminate();
	receipt_.recovery_.reset();
	receipt_.completion = ControlCompletion::kUncommitted;
	receipt_.cleanup_owner = ControlCleanupOwner::kNone;
	receipt_.committed = false;
	receipt_.deadline_elapsed = false;
	receipt_.cancellation_observed = false;
	receipt_.cleanup_error = {};
	active_scope = this;
}

ControlScope::~ControlScope() {
	receipt_.deadline_elapsed = remaining_time_ns(context_.deadline_ns()) == 0;
	receipt_.cancellation_observed = context_.cancellation().is_cancelled();
	active_scope = previous_;
}

void ControlScope::committed() noexcept {
	receipt_.committed = true;
	receipt_.completion = ControlCompletion::kCommitted;
	receipt_.cleanup_owner = ControlCleanupOwner::kNone;
}

void ControlScope::pending() noexcept {
	receipt_.completion = ControlCompletion::kCleanupPending;
	receipt_.cleanup_owner = ControlCleanupOwner::kHandle;
}

Result<void> control_check() noexcept {
	if (active_scope && active_scope->stop_.is_cancelled())
		return make_error(ErrorCode::kClosed, "control operation", "stop requested");
	return !active_scope ? Result<void>::ok() : active_scope->context_.check();
}

Result<void> control_begin_commit() noexcept { return control_check(); }

const ControlOperationContext* control_context() noexcept {
	return active_scope ? &active_scope->context_ : nullptr;
}

void control_committed() noexcept {
	if (active_scope) active_scope->committed();
}
void control_handle_pending(const Error& error) noexcept {
	if (active_scope) {
		active_scope->pending();
		active_scope->receipt_.cleanup_error = error;
	}
}

Result<void> control_prepare_cleanup(const std::string& path, const std::string& shm_name) {
	if (!active_scope) return Result<void>::ok();
	try {
		auto recovery = std::make_unique<ControlRecovery>();
		recovery->owner_pid = static_cast<uint64_t>(::getpid());
		recovery->lock_path = path;
		recovery->shm_name = shm_name;
		recovery->socket_path = path.substr(0, path.size() - 5) + ".sock";
		ControlReceiptAccess::recovery(active_scope->receipt_) = std::move(recovery);
	} catch (...) {
		return make_errno_error(ENOMEM, "control cleanup", "allocation before transaction");
	}
	return Result<void>::ok();
}

void control_retain_socket(uint64_t dev, uint64_t ino) noexcept {
	if (!active_scope) return;
	auto& recovery = ControlReceiptAccess::recovery(active_scope->receipt_);
	if (!recovery) return;
	recovery->socket_dev = dev;
	recovery->socket_ino = ino;
}

void control_retain_cleanup(const ControlLock& lock, const ControlJournalV1& expected,
                            const ControlJournalV1& reset, bool remove_object, uint64_t dev,
                            uint64_t ino) noexcept {
	if (!active_scope || !ControlReceiptAccess::recovery(active_scope->receipt_)) return;
	auto& recovery = *ControlReceiptAccess::recovery(active_scope->receipt_);
	if (recovery.pending) return;
	recovery.expected = expected;
	recovery.alternate = lock.last_journal();
	recovery.lock_dev = lock.dev();
	recovery.lock_ino = lock.ino();
	recovery.reset = reset;
	recovery.dev = dev;
	recovery.ino = ino;
	recovery.remove_object = remove_object;
	recovery.pending = true;
	active_scope->pending();
	active_scope->receipt_.cleanup_owner = ControlCleanupOwner::kReceipt;
	const auto available = control_check();
	active_scope->receipt_.cleanup_error =
	        available ? make_error(ErrorCode::kRecoveryBlocked, "control cleanup",
	                               "journal/object finalization required")
	                  : available.error();
}

int control_stop_handle() noexcept {
	return active_scope ? CancellationAccess::native_handle(active_scope->stop_) : -1;
}

Result<void> control_pause(uint64_t maximum_ns) noexcept {
	const auto* context = control_context();
	const uint64_t start = monotonic_now_ns();
	while (true) {
		auto checked = control_check();
		if (!checked) return checked;
		const uint64_t elapsed = monotonic_now_ns() - start;
		uint64_t remaining = elapsed >= maximum_ns ? 0 : maximum_ns - elapsed;
		if (context)
			remaining = std::min(remaining, remaining_time_ns(context->deadline_ns()));
		struct pollfd descriptors[2]{
		        {context ? CancellationAccess::native_handle(context->cancellation()) : -1,
		         POLLIN, 0},
		        {control_stop_handle(), POLLIN, 0}};
		remaining = std::min(remaining, uint64_t{10'000'000});
		const int ready =
		        ::poll(descriptors, 2, static_cast<int>((remaining + 999'999) / 1'000'000));
		if (ready < 0 && errno == EINTR) continue;
		if (ready < 0) return make_errno_error(errno, "control retry", "poll");
		return control_check();
	}
}

}  // namespace edge_runtime::detail
