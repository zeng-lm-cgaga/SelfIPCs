#ifndef EDGE_RUNTIME_SHARED_BUFFER_POOL_HPP
#define EDGE_RUNTIME_SHARED_BUFFER_POOL_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#include "edge_runtime/common/cancellation.hpp"
#include "edge_runtime/common/diagnostics.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/common/schema.hpp"

namespace edge_runtime {

struct BufferHandle {
	uint64_t pool_generation{0};
	uint64_t block_generation{0};
	uint64_t instance_nonce_hi{0};
	uint64_t instance_nonce_lo{0};
	uint64_t offset{0};
	uint32_t block_id{0};
	uint32_t length{0};
	uint32_t schema_version{0};
	uint32_t reserved{0};
	uint8_t schema_fingerprint[32]{};
};
static_assert(std::is_trivially_copyable_v<BufferHandle>, "BufferHandle must be trivially copyable");
static_assert(std::is_trivially_destructible_v<BufferHandle>,
              "BufferHandle must be trivially destructible");
static_assert(sizeof(BufferHandle) == 88, "BufferHandle ABI size");
static_assert(offsetof(BufferHandle, pool_generation) == 0, "BufferHandle generation");
static_assert(offsetof(BufferHandle, block_generation) == 8, "BufferHandle block generation");
static_assert(offsetof(BufferHandle, offset) == 32, "BufferHandle offset");
static_assert(offsetof(BufferHandle, block_id) == 40, "BufferHandle block id");
static_assert(offsetof(BufferHandle, length) == 44, "BufferHandle length");
static_assert(offsetof(BufferHandle, schema_fingerprint) == 56, "BufferHandle schema");

struct SharedBufferPoolOptions {
	std::string name;
	uint32_t block_size{0};
	uint32_t block_count{0};
	SchemaDescriptor schema{};
};

struct SharedBufferPoolStatus {
	bool ready{false};
	uint16_t abi_major{0};
	uint16_t abi_minor{0};
	uint32_t block_size{0};
	uint32_t block_count{0};
	uint64_t mapping_size{0};
	uint64_t generation{0};
	uint64_t instance_nonce_hi{0};
	uint64_t instance_nonce_lo{0};
	uint32_t free_blocks{0};
	uint32_t writing_blocks{0};
	uint32_t published_blocks{0};
	uint32_t reading_blocks{0};
	uint32_t recovery_blocked_blocks{0};
	uint64_t creator_pid{0};
	bool creator_alive{false};
};

class WriteBuffer;
class ReadBuffer;

namespace detail {
struct SharedBufferPoolHandle;

Result<std::shared_ptr<SharedBufferPoolHandle>> shared_buffer_pool_create_impl(
        const SharedBufferPoolOptions& options);
Result<std::shared_ptr<SharedBufferPoolHandle>> shared_buffer_pool_open_impl(
        const SharedBufferPoolOptions& options);
Result<SharedBufferPoolStatus> shared_buffer_pool_status_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept;
Result<void> shared_buffer_pool_remove_if_creator_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept;
uint32_t shared_buffer_pool_block_size_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept;
uint32_t shared_buffer_pool_block_count_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept;
uint64_t shared_buffer_pool_generation_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept;

class WriteBufferAccess;
class ReadBufferAccess;

Result<WriteBuffer> shared_buffer_pool_try_acquire_write_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept;
Result<WriteBuffer> shared_buffer_pool_wait_acquire_write_until_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle,
        std::chrono::steady_clock::time_point deadline, const CancellationToken& token) noexcept;
void shared_buffer_pool_set_diagnostics_enabled_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle, bool enabled) noexcept;
void shared_buffer_pool_reset_diagnostics_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept;
DiagnosticsSnapshot shared_buffer_pool_diagnostics_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle) noexcept;
Result<BufferHandle> shared_buffer_pool_publish_write_impl(WriteBuffer* buffer) noexcept;
void shared_buffer_pool_abort_write_impl(WriteBuffer* buffer) noexcept;
Result<ReadBuffer> shared_buffer_pool_acquire_read_impl(
        const std::shared_ptr<SharedBufferPoolHandle>& handle, const BufferHandle& descriptor) noexcept;
void shared_buffer_pool_release_read_impl(ReadBuffer* buffer) noexcept;
}  // namespace detail

class WriteBuffer {
       public:
	WriteBuffer(const WriteBuffer&) = delete;
	WriteBuffer& operator=(const WriteBuffer&) = delete;
	WriteBuffer(WriteBuffer&& other) noexcept;
	WriteBuffer& operator=(WriteBuffer&& other) noexcept;
	~WriteBuffer();

	std::byte* data() noexcept { return data_; }
	const std::byte* data() const noexcept { return data_; }
	size_t size() const noexcept { return static_cast<size_t>(size_); }
	const BufferHandle& handle() const noexcept { return descriptor_; }
	bool active() const noexcept { return active_; }

	Result<BufferHandle> publish() noexcept;
	void abort() noexcept;

       private:
	friend Result<WriteBuffer> detail::shared_buffer_pool_try_acquire_write_impl(
	        const std::shared_ptr<detail::SharedBufferPoolHandle>& handle) noexcept;
	friend Result<BufferHandle> detail::shared_buffer_pool_publish_write_impl(
	        WriteBuffer* buffer) noexcept;
	friend void detail::shared_buffer_pool_abort_write_impl(WriteBuffer* buffer) noexcept;

	WriteBuffer(std::shared_ptr<detail::SharedBufferPoolHandle> pool, void* block,
	            std::byte* data, uint32_t size, BufferHandle descriptor) noexcept;

	std::shared_ptr<detail::SharedBufferPoolHandle> pool_;
	void* block_{nullptr};
	std::byte* data_{nullptr};
	uint32_t size_{0};
	BufferHandle descriptor_{};
	bool active_{false};
};

class ReadBuffer {
       public:
	ReadBuffer(const ReadBuffer&) = delete;
	ReadBuffer& operator=(const ReadBuffer&) = delete;
	ReadBuffer(ReadBuffer&& other) noexcept;
	ReadBuffer& operator=(ReadBuffer&& other) noexcept;
	~ReadBuffer();

	const std::byte* data() const noexcept { return data_; }
	size_t size() const noexcept { return static_cast<size_t>(size_); }
	const BufferHandle& handle() const noexcept { return descriptor_; }
	bool active() const noexcept { return active_; }

	void release() noexcept;

       private:
	friend Result<ReadBuffer> detail::shared_buffer_pool_acquire_read_impl(
	        const std::shared_ptr<detail::SharedBufferPoolHandle>& handle,
	        const BufferHandle& descriptor) noexcept;
	friend void detail::shared_buffer_pool_release_read_impl(ReadBuffer* buffer) noexcept;

	ReadBuffer(std::shared_ptr<detail::SharedBufferPoolHandle> pool, void* block,
	           const std::byte* data, uint32_t size, BufferHandle descriptor) noexcept;

	std::shared_ptr<detail::SharedBufferPoolHandle> pool_;
	void* block_{nullptr};
	const std::byte* data_{nullptr};
	uint32_t size_{0};
	BufferHandle descriptor_{};
	bool active_{false};
};

class SharedBufferPool {
       public:
	static Result<SharedBufferPool> create(const SharedBufferPoolOptions& options) {
		auto handle = detail::shared_buffer_pool_create_impl(options);
		if (!handle) return handle.error();
		return SharedBufferPool(std::move(handle.value()));
	}

	static Result<SharedBufferPool> open(const SharedBufferPoolOptions& options) {
		auto handle = detail::shared_buffer_pool_open_impl(options);
		if (!handle) return handle.error();
		return SharedBufferPool(std::move(handle.value()));
	}

	Result<WriteBuffer> try_acquire_write() noexcept {
		return detail::shared_buffer_pool_try_acquire_write_impl(handle_);
	}

	Result<WriteBuffer> wait_acquire_write_until(std::chrono::steady_clock::time_point deadline,
	                                             const CancellationToken& token = {}) noexcept {
		return detail::shared_buffer_pool_wait_acquire_write_until_impl(handle_, deadline,
		                                                                token);
	}

	void set_diagnostics_enabled(bool enabled) noexcept {
		detail::shared_buffer_pool_set_diagnostics_enabled_impl(handle_, enabled);
	}

	void reset_diagnostics() noexcept {
		detail::shared_buffer_pool_reset_diagnostics_impl(handle_);
	}

	DiagnosticsSnapshot diagnostics() const noexcept {
		return detail::shared_buffer_pool_diagnostics_impl(handle_);
	}

	Result<ReadBuffer> acquire_read(const BufferHandle& descriptor) noexcept {
		return detail::shared_buffer_pool_acquire_read_impl(handle_, descriptor);
	}

	Result<SharedBufferPoolStatus> status() const noexcept {
		return detail::shared_buffer_pool_status_impl(handle_);
	}

	Result<void> remove_if_creator() noexcept {
		return detail::shared_buffer_pool_remove_if_creator_impl(handle_);
	}

	uint32_t block_size() const noexcept { return detail::shared_buffer_pool_block_size_impl(handle_); }
	uint32_t block_count() const noexcept {
		return detail::shared_buffer_pool_block_count_impl(handle_);
	}
	uint64_t generation() const noexcept {
		return detail::shared_buffer_pool_generation_impl(handle_);
	}

	SharedBufferPool(const SharedBufferPool&) = delete;
	SharedBufferPool& operator=(const SharedBufferPool&) = delete;
	SharedBufferPool(SharedBufferPool&& other) noexcept : handle_(std::move(other.handle_)) {}
	SharedBufferPool& operator=(SharedBufferPool&& other) noexcept {
		if (this == &other) return *this;
		handle_ = std::move(other.handle_);
		return *this;
	}

       private:
	explicit SharedBufferPool(std::shared_ptr<detail::SharedBufferPoolHandle> handle)
	    : handle_(std::move(handle)) {}

	std::shared_ptr<detail::SharedBufferPoolHandle> handle_;
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_SHARED_BUFFER_POOL_HPP
