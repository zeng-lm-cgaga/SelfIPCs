#include "edge_runtime/sync/cancellation.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <climits>
#include <mutex>

#include "edge_runtime/sync/futex.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"

namespace edge_runtime::detail {

struct CancellationState {
	int wake_fd{-1};
	~CancellationState() {
		if (wake_fd >= 0) (void)::close(wake_fd);
	}
	std::atomic<bool> cancelled{false};
	std::mutex mutex;
	CancellationRegistration* first{nullptr};

	bool cancel() noexcept {
		std::lock_guard<std::mutex> guard(mutex);
		if (cancelled.exchange(true, std::memory_order_acq_rel)) return false;
		if (wake_fd >= 0) {
			const uint64_t value = 1;
			while (::write(wake_fd, &value, sizeof(value)) < 0 && errno == EINTR) {
			}
		}
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

int detail::CancellationAccess::native_handle(const CancellationToken& token) noexcept {
	if (!token.state_) return -1;
	std::lock_guard<std::mutex> guard(token.state_->mutex);
	if (token.state_->wake_fd < 0)
		token.state_->wake_fd =
		        ::eventfd(token.state_->cancelled.load(std::memory_order_acquire) ? 1 : 0,
		                  EFD_NONBLOCK | EFD_CLOEXEC);
	return token.state_->wake_fd;
}

}  // namespace edge_runtime
