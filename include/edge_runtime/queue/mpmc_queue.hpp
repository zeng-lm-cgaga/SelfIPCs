#ifndef EDGE_RUNTIME_MPMC_QUEUE_HPP
#define EDGE_RUNTIME_MPMC_QUEUE_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#include "edge_runtime/common/cancellation.hpp"
#include "edge_runtime/common/diagnostics.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/common/schema.hpp"

namespace edge_runtime {

struct MpmcQueueOptions {
	std::string name;
	uint32_t capacity{0};
};

inline constexpr size_t kMpmcMaxBatchSize = 256;

enum class BatchStopReason : uint32_t {
	kComplete,
	kFull,
	kEmpty,
	kClosed,
	kCancelled,
	kContention,
	kError,
};

struct BatchResult {
	size_t count{0};
	BatchStopReason reason{BatchStopReason::kComplete};
	Error error{};
	explicit operator bool() const noexcept { return reason == BatchStopReason::kComplete; }
};

struct MpmcQueueStatus {
	bool ready{false};
	bool closed{false};
	uint16_t abi_major{0};
	uint16_t abi_minor{0};
	uint32_t capacity{0};
	uint32_t payload_size{0};
	uint64_t mapping_size{0};
	uint64_t generation{0};
	uint64_t instance_nonce_hi{0};
	uint64_t instance_nonce_lo{0};
	uint64_t enqueue_position{0};
	uint64_t dequeue_position{0};
	uint32_t enqueue_lock_state{0};
	uint32_t dequeue_lock_state{0};
	uint64_t enqueue_owner_pid{0};
	uint64_t dequeue_owner_pid{0};
	bool enqueue_owner_alive{false};
	bool dequeue_owner_alive{false};
};

namespace detail {
struct MpmcQueueHandle;

Result<std::shared_ptr<MpmcQueueHandle>> mpmc_create_impl(const MpmcQueueOptions& options,
                                                          const SchemaDescriptor& schema,
                                                          uint32_t payload_size);
Result<std::shared_ptr<MpmcQueueHandle>> mpmc_open_impl(const MpmcQueueOptions& options,
                                                        const SchemaDescriptor& schema,
                                                        uint32_t payload_size);
Result<void> mpmc_try_push_impl(const std::shared_ptr<MpmcQueueHandle>& handle,
                                const void* payload, uint32_t payload_size) noexcept;
Result<void> mpmc_try_pop_impl(const std::shared_ptr<MpmcQueueHandle>& handle, void* payload,
                               uint32_t payload_size) noexcept;
Result<void> mpmc_wait_push_until_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle, const void* payload,
        uint32_t payload_size, std::chrono::steady_clock::time_point deadline) noexcept;
Result<void> mpmc_wait_pop_until_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle, void* payload, uint32_t payload_size,
        std::chrono::steady_clock::time_point deadline) noexcept;
Result<void> mpmc_wait_push_until_impl(const std::shared_ptr<MpmcQueueHandle>& handle,
                                       const void* payload, uint32_t payload_size,
                                       std::chrono::steady_clock::time_point deadline,
                                       const CancellationToken& token) noexcept;
Result<void> mpmc_wait_pop_until_impl(const std::shared_ptr<MpmcQueueHandle>& handle, void* payload,
                                      uint32_t payload_size,
                                      std::chrono::steady_clock::time_point deadline,
                                      const CancellationToken& token) noexcept;
Result<void> mpmc_close_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
BatchResult mpmc_try_push_batch_impl(const std::shared_ptr<MpmcQueueHandle>& handle,
                                     const void* payload, uint32_t payload_size, size_t count,
                                     const CancellationToken& token) noexcept;
BatchResult mpmc_try_pop_batch_impl(const std::shared_ptr<MpmcQueueHandle>& handle, void* payload,
                                    uint32_t payload_size, size_t count,
                                    const CancellationToken& token) noexcept;
void mpmc_set_diagnostics_impl(const std::shared_ptr<MpmcQueueHandle>& handle,
                               bool enabled) noexcept;
void mpmc_reset_diagnostics_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
DiagnosticsSnapshot mpmc_diagnostics_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
Result<MpmcQueueStatus> mpmc_status_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
Result<void> mpmc_remove_if_creator_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
uint32_t mpmc_capacity_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
uint64_t mpmc_generation_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
}

// 固定大小、跨进程、多生产者/多消费者的任务队列。每条消息至多成功交付一次。
template <typename T>
class MpmcQueue {
	static_assert(std::is_trivially_copyable_v<T>,
	              "MpmcQueue payload must be trivially copyable");
	static_assert(std::is_trivially_destructible_v<T>,
	              "MpmcQueue payload must be trivially destructible");
	static_assert(std::is_nothrow_default_constructible_v<T>,
	              "MpmcQueue payload must be nothrow default constructible");

	public:
	using value_type = T;

	static Result<MpmcQueue> create(const MpmcQueueOptions& options,
	                                const SchemaDescriptor& schema) {
		auto handle = detail::mpmc_create_impl(options, schema, sizeof(T));
		if (!handle) return handle.error();
		return MpmcQueue(std::move(handle.value()));
	}

	static Result<MpmcQueue> open(const MpmcQueueOptions& options,
	                              const SchemaDescriptor& schema) {
		auto handle = detail::mpmc_open_impl(options, schema, sizeof(T));
		if (!handle) return handle.error();
		return MpmcQueue(std::move(handle.value()));
	}

	Result<void> try_push(const T& value) noexcept {
		return detail::mpmc_try_push_impl(handle_, &value, sizeof(T));
	}

	Result<T> try_pop() noexcept {
		T value{};
		auto result = detail::mpmc_try_pop_impl(handle_, &value, sizeof(T));
		if (!result) return result.error();
		return Result<T>(std::move(value));
	}

	// The deadline is an absolute monotonic time point; timeout and recovery remain distinct.
	Result<void> wait_push_until(const T& value,
	                             std::chrono::steady_clock::time_point deadline) noexcept {
		return detail::mpmc_wait_push_until_impl(handle_, &value, sizeof(T), deadline);
	}

	Result<T> wait_pop_until(std::chrono::steady_clock::time_point deadline) noexcept {
		T value{};
		auto result = detail::mpmc_wait_pop_until_impl(handle_, &value, sizeof(T), deadline);
		if (!result) return result.error();
		return Result<T>(std::move(value));
	}

	Result<MpmcQueueStatus> status() const noexcept {
		return detail::mpmc_status_impl(handle_);
	}

	Result<void> wait_push_until(const T& value, std::chrono::steady_clock::time_point deadline,
	                             const CancellationToken& token) noexcept {
		return detail::mpmc_wait_push_until_impl(handle_, &value, sizeof(T), deadline,
		                                         token);
	}

	Result<T> wait_pop_until(std::chrono::steady_clock::time_point deadline,
	                         const CancellationToken& token) noexcept {
		T value{};
		auto result = detail::mpmc_wait_pop_until_impl(handle_, &value, sizeof(T), deadline,
		                                               token);
		if (!result) return result.error();
		return Result<T>(std::move(value));
	}

	Result<void> close() noexcept { return detail::mpmc_close_impl(handle_); }

	BatchResult try_push_batch(const T* values, size_t count,
	                           const CancellationToken& token = {}) noexcept {
		return detail::mpmc_try_push_batch_impl(handle_, values, sizeof(T), count, token);
	}

	BatchResult try_pop_batch(T* values, size_t count,
	                          const CancellationToken& token = {}) noexcept {
		return detail::mpmc_try_pop_batch_impl(handle_, values, sizeof(T), count, token);
	}

	void set_diagnostics_enabled(bool enabled) noexcept {
		detail::mpmc_set_diagnostics_impl(handle_, enabled);
	}
	void reset_diagnostics() noexcept { detail::mpmc_reset_diagnostics_impl(handle_); }
	DiagnosticsSnapshot diagnostics() const noexcept {
		return detail::mpmc_diagnostics_impl(handle_);
	}

	Result<void> remove_if_creator() noexcept {
		return detail::mpmc_remove_if_creator_impl(handle_);
	}

	uint32_t capacity() const noexcept { return detail::mpmc_capacity_impl(handle_); }
	uint64_t generation() const noexcept { return detail::mpmc_generation_impl(handle_); }

	MpmcQueue(const MpmcQueue&) = delete;
	MpmcQueue& operator=(const MpmcQueue&) = delete;
	MpmcQueue(MpmcQueue&& other) noexcept : handle_(std::move(other.handle_)) {}
	MpmcQueue& operator=(MpmcQueue&& other) noexcept {
		if (this == &other) return *this;
		handle_ = std::move(other.handle_);
		return *this;
	}

	private:
	explicit MpmcQueue(std::shared_ptr<detail::MpmcQueueHandle> handle)
	    : handle_(std::move(handle)) {}

	std::shared_ptr<detail::MpmcQueueHandle> handle_;
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_MPMC_QUEUE_HPP
