#include "queue_pool_demo/pool_flow.hpp"

#include <unistd.h>

#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <exception>
#include <optional>
#include <thread>
#include <utility>

#include "queue_pool_demo/api.hpp"
#include "queue_pool_demo/process.hpp"

namespace queue_pool_demo {

namespace {

using HandleQueue = ipc::MpmcQueue<ipc::BufferHandle>;
constexpr char kPayload[] = "installed Pool release/wait";

ipc::SchemaDescriptor pool_schema() {
	ipc::SchemaDescriptor schema;
	schema.fingerprint[0] = std::byte{0x50};
	schema.fingerprint[31] = std::byte{0x21};
	schema.version = 1;
	schema.debug_name = "queue_pool_demo.PoolPayload.v1";
	return schema;
}

ipc::SchemaDescriptor handle_schema() {
	auto schema = pool_schema();
	schema.fingerprint[0] = std::byte{0x48};
	schema.debug_name = "queue_pool_demo.BufferHandle.v1";
	return schema;
}

ipc::SharedBufferPoolOptions pool_options(const std::string& name) {
	ipc::SharedBufferPoolOptions options;
	options.name = name;
	options.block_size = 4096;
	options.block_count = 1;
	options.schema = pool_schema();
	return options;
}

ipc::MpmcQueueOptions queue_options(const std::string& name) {
	ipc::MpmcQueueOptions options;
	options.name = name + "_handles";
	options.capacity = 1;
	return options;
}

bool report_error(const char* operation, const ipc::Error& error) {
	std::fprintf(stderr, "%s: %s errno=%d operation=%s context=%s\n", operation,
	             ipc::to_string(error.code), error.errno_value,
	             error.operation != nullptr ? error.operation : "?", error.context);
	return false;
}

bool require(bool condition, const char* expectation) {
	if (!condition) std::fprintf(stderr, "Pool expectation failed: %s\n", expectation);
	return condition;
}

void print_pool_diagnostics(const char* stage, const ipc::SharedBufferPool& pool) {
	const auto snapshot = pool.diagnostics();
	std::printf("Pool diagnostics[%s]: capacity=%u Full=%" PRIu64 " contention=%" PRIu64
	            " wait_calls=%" PRIu64 " waits=%" PRIu64 " committed_acquire=%" PRIu64
	            " notifications=%" PRIu64 " timeouts=%" PRIu64 " cancelled=%" PRIu64 "\n",
	            stage, snapshot.capacity, snapshot.full, snapshot.contention,
	            snapshot.wait_calls, snapshot.waits, snapshot.acquire_successes,
	            snapshot.notifications, snapshot.timeouts, snapshot.cancelled);
}

bool all_free(const ipc::SharedBufferPool& pool) {
	const auto status = pool.status();
	if (!status) return report_error("Pool status", status.error());
	return status.value().free_blocks == pool.block_count() &&
	       status.value().writing_blocks == 0 && status.value().published_blocks == 0 &&
	       status.value().reading_blocks == 0 && status.value().recovery_blocked_blocks == 0;
}

bool parent_pool_steps(ipc::SharedBufferPool& pool, HandleQueue& queue, HelperProcess& child,
                       const std::string& name, std::optional<ipc::BufferHandle>& published) {
	auto write = pool.try_acquire_write();
	if (!write) return report_error("Pool initial write", write.error());
	if (!require(write.value().size() >= sizeof(kPayload), "write buffer fits payload"))
		return false;
	std::memcpy(write.value().data(), kPayload, sizeof(kPayload));
	const auto descriptor = write.value().publish();
	if (!descriptor) return report_error("Pool publish", descriptor.error());
	published = descriptor.value();
	const auto sent = queue.wait_push_until(*published, Clock::now() + std::chrono::seconds(3));
	if (!sent) return report_error("Pool descriptor queue push", sent.error());
	const auto published_full = pool.try_acquire_write();
	if (!require(!published_full, "published descriptor occupies the only block")) return false;
	if (published_full.error().code != ipc::ErrorCode::kBufferPoolFull)
		return report_error("Pool published Full", published_full.error());
	print_pool_diagnostics("published-full-before-exec", pool);
	if (!child.spawn(name, "--pool-helper") || !child.expect('R')) return false;
	const auto exhausted = pool.try_acquire_write();
	if (!require(!exhausted, "one block is held by exec reader")) return false;
	if (exhausted.error().code != ipc::ErrorCode::kBufferPoolContention)
		return report_error("Pool exhausted try", exhausted.error());
	std::printf(
	        "Pool: live exec reader owns only block -> retryable BufferPoolContention, not "
	        "published Full\n");
	const auto timeout =
	        pool.wait_acquire_write_until(Clock::now() + std::chrono::milliseconds(120));
	if (!require(!timeout, "exhausted Pool wait times out")) return false;
	if (timeout.error().code != ipc::ErrorCode::kTimeout)
		return report_error("Pool occupied wait", timeout.error());
	print_pool_diagnostics("reader-holds-only-block", pool);
	ipc::CancellationSource source;
	std::thread canceller([&source] {
		std::this_thread::sleep_for(std::chrono::milliseconds(60));
		(void)source.request_cancel();
	});
	const auto cancelled = pool.wait_acquire_write_until(Clock::now() + std::chrono::seconds(2),
	                                                     source.token());
	canceller.join();
	if (!require(!cancelled, "local cancellation cannot acquire a held block")) return false;
	if (cancelled.error().code != ipc::ErrorCode::kCancelled)
		return report_error("Pool cancelled wait", cancelled.error());
	if (!child.send('L')) return false;
	auto acquired = pool.wait_acquire_write_until(Clock::now() + std::chrono::seconds(3));
	if (!acquired)
		return report_error("Pool wait after remote reader release", acquired.error());
	if (!child.expect('F')) return false;
	std::printf(
	        "Pool: exec reader release -> FREE/notify -> creator wait acquired owned "
	        "WriteBuffer\n");
	published.reset();
	if (!child.send('W') || !child.expect('W')) return false;
	std::this_thread::sleep_for(std::chrono::milliseconds(60));
	acquired.value().abort();
	std::printf("Pool: creator abort returned block -> exec producer waiter can acquire\n");
	if (!child.expect('D') || !child.wait_for_success(Clock::now() + std::chrono::seconds(3)))
		return false;
	print_pool_diagnostics("creator-final", pool);
	const auto snapshot = pool.diagnostics();
	return require(all_free(pool),
	               "all blocks returned after reader release and write aborts") &&
	       require(snapshot.enabled && snapshot.capacity == 1 &&
	                       snapshot.acquire_successes == 2 && snapshot.wait_calls == 3 &&
	                       snapshot.full > 0 && snapshot.timeouts == 1 &&
	                       snapshot.cancelled == 1,
	               "creator-local counters count committed write acquisitions, not reader "
	               "operations");
}

bool cleanup_pool(ipc::SharedBufferPool& pool, HandleQueue& queue,
                  const std::optional<ipc::BufferHandle>& published, const std::string& name) {
	if (published && !all_free(pool)) {
		auto remaining = pool.acquire_read(*published);
		if (remaining)
			remaining.value().release();
		else
			(void)report_error("Pool cleanup acquire published descriptor",
			                   remaining.error());
	}
	const bool returned = all_free(pool);
	if (!returned)
		std::fprintf(stderr,
		             "Pool cleanup: blocks not all FREE; preserve original failure\n");
	const auto pool_removed = pool.remove_if_creator();
	if (!pool_removed) {
		(void)report_error("Pool cleanup remove_if_creator", pool_removed.error());
		std::fprintf(stderr, "Pool cleanup: retained IPC name=%s\n", name.c_str());
	} else {
		std::printf("Pool cleanup: all blocks returned=%d; creator removed name=%s\n",
		            returned ? 1 : 0, name.c_str());
	}
	const auto queue_removed = queue.remove_if_creator();
	if (!queue_removed) {
		(void)report_error("Pool descriptor queue cleanup", queue_removed.error());
		std::fprintf(stderr, "Pool cleanup: retained queue name=%s_handles\n",
		             name.c_str());
	}
	return returned && pool_removed && queue_removed;
}

}  // namespace

int run_pool_helper(const std::string& name, int descriptor, pid_t parent_pid) {
	if (!require(::getppid() == parent_pid && ::getpid() != parent_pid &&
	                     ::getpgrp() == ::getpid(),
	             "Pool helper is a distinct exec process in its own group"))
		return 3;
	auto opened = ipc::SharedBufferPool::open(pool_options(name));
	if (!opened) {
		(void)report_error("Pool helper independent open", opened.error());
		return 3;
	}
	ipc::SharedBufferPool pool = std::move(opened.value());
	pool.set_diagnostics_enabled(true);
	pool.reset_diagnostics();
	auto opened_queue = HandleQueue::open(queue_options(name), handle_schema());
	if (!opened_queue) {
		(void)report_error("Pool helper descriptor queue open", opened_queue.error());
		return 3;
	}
	HandleQueue queue = std::move(opened_queue.value());
	const auto received = queue.wait_pop_until(Clock::now() + std::chrono::seconds(3));
	if (!received) {
		(void)report_error("Pool helper descriptor pop", received.error());
		return 3;
	}
	auto read = pool.acquire_read(received.value());
	if (!read) {
		(void)report_error("Pool helper acquire read", read.error());
		return 3;
	}
	if (!require(read.value().size() >= sizeof(kPayload) &&
	                     std::memcmp(read.value().data(), kPayload, sizeof(kPayload)) == 0,
	             "exec reader sees published payload"))
		return 3;
	std::printf(
	        "Pool helper: independent open pid=%ld; BufferHandle received via queue, reader "
	        "holds block\n",
	        static_cast<long>(::getpid()));
	if (!send_token(descriptor, 'R', Clock::now() + std::chrono::seconds(3)) ||
	    !expect_token(descriptor, 'L', Clock::now() + std::chrono::seconds(3)))
		return 3;
	std::this_thread::sleep_for(std::chrono::milliseconds(60));
	read.value().release();
	if (!send_token(descriptor, 'F', Clock::now() + std::chrono::seconds(3)) ||
	    !expect_token(descriptor, 'W', Clock::now() + std::chrono::seconds(3)) ||
	    !send_token(descriptor, 'W', Clock::now() + std::chrono::seconds(3)))
		return 3;
	auto write = pool.wait_acquire_write_until(Clock::now() + std::chrono::seconds(3));
	if (!write) {
		(void)report_error("Pool exec producer wait", write.error());
		return 3;
	}
	write.value().abort();
	print_pool_diagnostics("exec-helper-final", pool);
	const auto snapshot = pool.diagnostics();
	if (!require(snapshot.acquire_successes == 1 && snapshot.wait_calls == 1,
	             "helper committed one write after creator returned block"))
		return 3;
	return send_token(descriptor, 'D', Clock::now() + std::chrono::seconds(3)) ? 0 : 3;
}

bool run_pool_flow() {
	const std::string name = unique_name() + "_pool";
	auto created_queue = HandleQueue::create(queue_options(name), handle_schema());
	if (!created_queue)
		return report_error("Pool descriptor queue create", created_queue.error());
	HandleQueue queue = std::move(created_queue.value());
	auto created_pool = ipc::SharedBufferPool::create(pool_options(name));
	if (!created_pool) {
		(void)report_error("Pool creator create", created_pool.error());
		const auto removed = queue.remove_if_creator();
		if (!removed) {
			(void)report_error("Pool create failure queue cleanup", removed.error());
			std::fprintf(stderr, "Pool cleanup: retained queue name=%s_handles\n",
			             name.c_str());
		}
		return false;
	}
	ipc::SharedBufferPool pool = std::move(created_pool.value());
	HelperProcess child;
	std::optional<ipc::BufferHandle> published;
	pool.set_diagnostics_enabled(true);
	pool.reset_diagnostics();
	std::printf("Pool creator: pid=%ld name=%s block_count=1\n", static_cast<long>(::getpid()),
	            name.c_str());
	bool succeeded = false;
	try {
		succeeded = parent_pool_steps(pool, queue, child, name, published);
	} catch (const std::exception& error) {
		std::fprintf(stderr, "Pool flow exception: %s\n", error.what());
	}
	if (!child.stop()) {
		std::fprintf(stderr,
		             "Pool cleanup: helper exit unconfirmed; retaining pool=%s "
		             "queue=%s_handles\n",
		             name.c_str(), name.c_str());
		return false;
	}
	const bool cleaned = cleanup_pool(pool, queue, published, name);
	if (!succeeded || !cleaned) {
		std::fprintf(stderr, "Pool flow FAILED (legal cleanup cannot mask IPC failure)\n");
		return false;
	}
	std::printf("Pool flow OK; all borrowed blocks returned, no performance claim\n");
	return true;
}

}  // namespace queue_pool_demo
