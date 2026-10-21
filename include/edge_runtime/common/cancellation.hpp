#ifndef EDGE_RUNTIME_COMMON_CANCELLATION_HPP
#define EDGE_RUNTIME_COMMON_CANCELLATION_HPP

#include <memory>
#include <utility>

namespace edge_runtime {

namespace detail {
struct CancellationState;
class CancellationRegistration;
}  // namespace detail

class CancellationToken {
       public:
	CancellationToken() noexcept = default;
	bool is_cancelled() const noexcept;

       private:
	friend class CancellationSource;
	friend class detail::CancellationRegistration;
	explicit CancellationToken(std::shared_ptr<detail::CancellationState> state) noexcept
	    : state_(std::move(state)) {}
	std::shared_ptr<detail::CancellationState> state_;
};

class CancellationSource {
       public:
	CancellationSource();
	CancellationToken token() const noexcept { return CancellationToken(state_); }
	bool request_cancel() const noexcept;

       private:
	std::shared_ptr<detail::CancellationState> state_;
};

}  // namespace edge_runtime

#endif
