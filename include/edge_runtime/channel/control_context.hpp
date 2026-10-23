#ifndef EDGE_RUNTIME_CHANNEL_CONTROL_CONTEXT_HPP
#define EDGE_RUNTIME_CHANNEL_CONTROL_CONTEXT_HPP

#include <chrono>
#include <cstdint>
#include <memory>

#include "edge_runtime/common/cancellation.hpp"
#include "edge_runtime/common/result.hpp"

namespace edge_runtime {
namespace detail {
struct ControlRecovery;
class ControlScope;
struct ControlReceiptAccess;
}  // namespace detail

inline constexpr uint32_t kControlContextFeatureVersion = 1;
uint32_t control_context_feature_version() noexcept;

class ControlOperationContext {
       public:
	ControlOperationContext(uint64_t monotonic_deadline_ns,
	                        CancellationToken token = {}) noexcept;
	static Result<ControlOperationContext> after(std::chrono::nanoseconds timeout,
	                                             CancellationToken token = {}) noexcept;
	uint64_t deadline_ns() const noexcept { return deadline_ns_; }
	const CancellationToken& cancellation() const noexcept { return cancellation_; }
	Result<void> check() const noexcept;

       private:
	uint64_t deadline_ns_;
	CancellationToken cancellation_;
};

enum class ControlCompletion : uint32_t { kUncommitted, kCommitted, kCleanupPending };
enum class ControlCleanupOwner : uint32_t { kNone, kReceipt, kHandle };

struct ControlReceipt {
	ControlReceipt() noexcept;
	~ControlReceipt();
	ControlReceipt(ControlReceipt&&) noexcept;
	ControlReceipt& operator=(ControlReceipt&&) = delete;
	ControlReceipt(const ControlReceipt&) = delete;
	ControlReceipt& operator=(const ControlReceipt&) = delete;
	bool has_pending_cleanup() const noexcept;
	Result<void> finalize(const ControlOperationContext& context) noexcept;
	ControlCompletion completion{ControlCompletion::kUncommitted};
	ControlCleanupOwner cleanup_owner{ControlCleanupOwner::kNone};
	bool committed{false};
	bool deadline_elapsed{false};
	bool cancellation_observed{false};
	Error cleanup_error{};

       private:
	std::unique_ptr<detail::ControlRecovery> recovery_;
	friend class detail::ControlScope;
	friend struct detail::ControlReceiptAccess;
};

enum class ControlCloseStatus : uint32_t { kClosed, kPending };

}  // namespace edge_runtime
#endif
