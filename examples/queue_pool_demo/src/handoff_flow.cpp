#include "queue_pool_demo/handoff_flow.hpp"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>

#include "queue_pool_demo/api.hpp"
#include "queue_pool_demo/process.hpp"

namespace queue_pool_demo {
namespace {
using Queue = ipc::MpmcQueue<ipc::BufferHandle>;
using namespace std::chrono_literals;

ipc::SchemaDescriptor schema() {
	ipc::SchemaDescriptor value;
	value.version = 1;
	value.fingerprint[0] = std::byte{0x77};
	return value;
}

ipc::SharedBufferPoolOptions pool_options(const std::string& name) {
	return {name, 4096, 1, schema()};
}

ipc::MpmcQueueOptions queue_options(const std::string& name) { return {name + "_queue", 1}; }

bool transfer(int descriptor, ipc::BufferHandle& handle, bool sending) {
	auto* bytes = reinterpret_cast<std::byte*>(&handle);
	size_t offset = 0;
	const auto deadline = Clock::now() + 2s;
	while (offset < sizeof(handle) && Clock::now() < deadline) {
		pollfd watched{descriptor, static_cast<short>(sending ? POLLOUT : POLLIN), 0};
		const int ready = ::poll(&watched, 1, 10);
		if (ready < 0 && errno == EINTR) continue;
		if (ready < 0) return false;
		if (!ready) continue;
		const ssize_t count =
		        sending ? ::send(descriptor, bytes + offset, sizeof(handle) - offset,
		                         MSG_DONTWAIT | MSG_NOSIGNAL)
		                : ::recv(descriptor, bytes + offset, sizeof(handle) - offset,
		                         MSG_DONTWAIT);
		if (count > 0)
			offset += static_cast<size_t>(count);
		else if (count == 0 || (errno != EINTR && errno != EAGAIN))
			return false;
	}
	return offset == sizeof(handle);
}

bool run_window(bool producer) {
	const std::string name = unique_name();
	auto created_pool = ipc::SharedBufferPool::create(pool_options(name));
	if (!created_pool) return false;
	auto& pool = created_pool.value();
	auto created_queue = Queue::create(queue_options(name), schema());
	if (!created_queue) {
		(void)pool.remove_if_creator();
		return false;
	}
	auto& queue = created_queue.value();
	ipc::BufferHandle descriptor;
	bool ready = true;
	if (!producer) {
		auto write = pool.try_acquire_write();
		if (!write)
			ready = false;
		else {
			auto sent = ipc::send_buffer_until(write.value(), queue, Clock::now() + 1s);
			ready = static_cast<bool>(sent);
			if (sent.descriptor) descriptor = *sent.descriptor;
		}
	}
	HelperProcess child;
	const bool started =
	        ready && child.spawn(name, producer ? "--handoff-producer" : "--handoff-consumer");
	const bool received = started && transfer(child.control_descriptor(), descriptor, false);
	const bool exited = received && child.wait_for_success(Clock::now() + 2s);
	if (child.running()) (void)child.stop();
	bool returned = false;
	if (exited) {
		auto empty = queue.try_pop();
		if (!empty && empty.error().code == ipc::ErrorCode::kQueueEmpty) {
			auto read = pool.acquire_read(descriptor);
			returned = read && static_cast<bool>(read.value().release_checked());
		}
	}
	const auto removed_pool = pool.remove_if_creator();
	const auto removed_queue = queue.remove_if_creator();
	std::printf("handoff window=%s exec_exit_confirmed=%d returned=%d cleanup=%d name=%s\n",
	            producer ? "publish/enqueue" : "pop/acquire_read", exited, returned,
	            static_cast<bool>(removed_pool) && static_cast<bool>(removed_queue),
	            name.c_str());
	return exited && returned && removed_pool && removed_queue;
}
}

int run_handoff_helper(const std::string& name, int descriptor, bool producer) {
	auto pool = ipc::SharedBufferPool::open(pool_options(name));
	auto queue = Queue::open(queue_options(name), schema());
	if (!pool || !queue) return 2;
	ipc::BufferHandle handle;
	if (producer) {
		auto write = pool.value().try_acquire_write();
		if (!write) return 3;
		auto published = write.value().publish();
		if (!published) return 4;
		handle = published.value();
	} else {
		auto popped = queue.value().wait_pop_until(Clock::now() + 2s);
		if (!popped) return 5;
		handle = popped.value();
	}
	if (!transfer(descriptor, handle, true)) return 6;
	::_exit(0);
}

bool run_handoff_flow() { return run_window(true) && run_window(false); }
}
