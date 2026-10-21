#ifndef EDGE_RUNTIME_UTILITY_LOCAL_DIAGNOSTICS_HPP
#define EDGE_RUNTIME_UTILITY_LOCAL_DIAGNOSTICS_HPP

#include <array>
#include <atomic>
#include <cstddef>
#include <limits>

#include "edge_runtime/common/diagnostics.hpp"
#include "edge_runtime/common/error.hpp"

namespace edge_runtime::detail {

enum class DiagnosticCounter : size_t {
	kPushAttempts,
	kPopAttempts,
	kAcquireAttempts,
	kPushSuccesses,
	kPopSuccesses,
	kAcquireSuccesses,
	kFull,
	kEmpty,
	kContention,
	kWaitCalls,
	kWaits,
	kTimeouts,
	kCancelled,
	kNotifications,
	kCloseCalls,
	kBatchCalls,
	kCount
};

class LocalDiagnostics {
       public:
	LocalDiagnostics() noexcept { reset(); }
	void enable(bool enabled) noexcept { enabled_.store(enabled, std::memory_order_relaxed); }
	void add(DiagnosticCounter counter) noexcept {
		if (!enabled_.load(std::memory_order_relaxed)) return;
		auto& value = counters_[static_cast<size_t>(counter)];
		auto observed = value.load(std::memory_order_relaxed);
		while (observed != std::numeric_limits<uint64_t>::max() &&
		       !value.compare_exchange_weak(observed, observed + 1,
		                                    std::memory_order_relaxed)) {
		}
	}
	void record_error(ErrorCode code) noexcept {
		switch (code) {
			case ErrorCode::kQueueFull:
			case ErrorCode::kBufferPoolFull:
				add(DiagnosticCounter::kFull);
				break;
			case ErrorCode::kQueueEmpty:
				add(DiagnosticCounter::kEmpty);
				break;
			case ErrorCode::kQueueContention:
			case ErrorCode::kBufferPoolContention:
				add(DiagnosticCounter::kContention);
				break;
			default:
				break;
		}
	}
	void reset() noexcept {
		for (auto& counter : counters_) counter.store(0, std::memory_order_relaxed);
	}
	DiagnosticsSnapshot snapshot(uint32_t capacity) const noexcept {
		DiagnosticsSnapshot result;
		result.enabled = enabled_.load(std::memory_order_relaxed);
		result.capacity = capacity;
		const std::array<uint64_t DiagnosticsSnapshot::*, 16> members{
		        &DiagnosticsSnapshot::push_attempts,
		        &DiagnosticsSnapshot::pop_attempts,
		        &DiagnosticsSnapshot::acquire_attempts,
		        &DiagnosticsSnapshot::push_successes,
		        &DiagnosticsSnapshot::pop_successes,
		        &DiagnosticsSnapshot::acquire_successes,
		        &DiagnosticsSnapshot::full,
		        &DiagnosticsSnapshot::empty,
		        &DiagnosticsSnapshot::contention,
		        &DiagnosticsSnapshot::wait_calls,
		        &DiagnosticsSnapshot::waits,
		        &DiagnosticsSnapshot::timeouts,
		        &DiagnosticsSnapshot::cancelled,
		        &DiagnosticsSnapshot::notifications,
		        &DiagnosticsSnapshot::close_calls,
		        &DiagnosticsSnapshot::batch_calls};
		for (size_t index = 0; index < members.size(); ++index) {
			result.*members[index] = counters_[index].load(std::memory_order_relaxed);
		}
		return result;
	}

       private:
	std::atomic<bool> enabled_{false};
	std::array<std::atomic<uint64_t>, static_cast<size_t>(DiagnosticCounter::kCount)>
	        counters_{};
};

}  // namespace edge_runtime::detail

#endif
