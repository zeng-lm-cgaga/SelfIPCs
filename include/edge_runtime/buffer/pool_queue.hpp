#ifndef EDGE_RUNTIME_BUFFER_POOL_QUEUE_HPP
#define EDGE_RUNTIME_BUFFER_POOL_QUEUE_HPP

#include <optional>

#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"

namespace edge_runtime {

enum class PoolQueueSendState {
	kPublishFailed,
	kEnqueued,
	kReturned,
	kCleanupRequired,
};

struct PoolQueueSendResult {
	PoolQueueSendState state{PoolQueueSendState::kPublishFailed};
	std::optional<BufferHandle> descriptor;
	std::optional<Error> publish_error;
	std::optional<Error> enqueue_error;
	std::optional<Error> return_error;
	std::optional<ReadBuffer> pending_read;
	explicit operator bool() const noexcept { return state == PoolQueueSendState::kEnqueued; }
};

PoolQueueSendResult send_buffer_until(WriteBuffer& buffer, MpmcQueue<BufferHandle>& queue,
                                      std::chrono::steady_clock::time_point deadline,
                                      const CancellationToken& token = {}) noexcept;

}

#endif
