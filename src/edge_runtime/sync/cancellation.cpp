#include "edge_runtime/sync/cancellation.hpp"

#include <atomic>
#include <climits>
#include <mutex>

#include "edge_runtime/sync/futex.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"

namespace edge_runtime::detail {

struct CancellationState {
	std::atomic<bool> cancelled{false};
	std::mutex mutex;
	CancellationRegistration* first{nullptr};

	bool cancel() noexcept {
		std::lock_guard<std::mutex> guard(mutex);
		if (cancelled.exchange(true, std::memory_order_acq_rel)) return false;
		for (auto* registration = first; registration; registration = registration->next_) {
			__atomic_fetch_add(registration->epoch_, uint32_t{1}, __ATOMIC_RELEASE);
			(void)futex_wake(registration->epoch_, INT_MAX);
		}
		return true;
	}
};

CancellationRegistration::CancellationRegistration(const CancellationToken& token,
                                                   uint32_t* epoch) noexcept
    : state_(token.state_), epoch_(epoch) {
	if (!state_) return;
	std::lock_guard<std::mutex> guard(state_->mutex);
	next_ = state_->first;
	if (next_) next_->previous_ = this;
	state_->first = this;
}

CancellationRegistration::~CancellationRegistration() {
	if (!state_) return;
	std::lock_guard<std::mutex> guard(state_->mutex);
	if (previous_)
		previous_->next_ = next_;
	else
		state_->first = next_;
	if (next_) next_->previous_ = previous_;
}

}  // namespace edge_runtime::detail

namespace edge_runtime {

CancellationSource::CancellationSource() : state_(std::make_shared<detail::CancellationState>()) {}

bool CancellationSource::request_cancel() const noexcept { return state_ && state_->cancel(); }

bool CancellationToken::is_cancelled() const noexcept {
	return state_ && state_->cancelled.load(std::memory_order_acquire);
}

}  // namespace edge_runtime
