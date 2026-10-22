#include "edge_runtime/buffer/pool_queue.hpp"

#include "edge_runtime/utility/failpoint.hpp"

namespace edge_runtime {

PoolQueueSendResult send_buffer_until(WriteBuffer& buffer, MpmcQueue<BufferHandle>& queue,
                                      std::chrono::steady_clock::time_point deadline,
                                      const CancellationToken& token) noexcept {
	PoolQueueSendResult result;
	auto pool = buffer.pool_;
	auto published = buffer.publish();
	if (!published) {
		result.publish_error = published.error();
		return result;
	}
	result.descriptor = published.value();
	EDGE_FAILPOINT(POOL_QUEUE_AFTER_PUBLISH);
	auto enqueued = queue.wait_push_until(*result.descriptor, deadline, token);
	if (enqueued) {
		result.state = PoolQueueSendState::kEnqueued;
		return result;
	}
	result.enqueue_error = enqueued.error();
	result.state = PoolQueueSendState::kCleanupRequired;
	EDGE_FAILPOINT(POOL_QUEUE_ENQUEUE_FAILED);
	auto read = detail::shared_buffer_pool_acquire_read_impl(pool, *result.descriptor);
	if (!read) {
		result.return_error = read.error();
		return result;
	}
	EDGE_FAILPOINT(POOL_QUEUE_BEFORE_RETURN);
	auto returned = read.value().release_checked();
	if (!returned) {
		result.return_error = returned.error();
		result.pending_read.emplace(std::move(read.value()));
		return result;
	}
	result.state = PoolQueueSendState::kReturned;
	return result;
}

}
