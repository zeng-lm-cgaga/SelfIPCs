#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "broker_observer_fixture.hpp"
#include "broker_observer_syscall_fixture.hpp"
#include "edge_runtime/channel/channel_abi.hpp"
#include "edge_runtime/channel/channel_observer.hpp"
#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/fd_broker.hpp"
#include "test_payload.hpp"
#include "retained_shm.hpp"

namespace {

using namespace edge_runtime;
using namespace edge_runtime::detail;
using namespace edge_test::broker_observer;

volatile sig_atomic_t signal_count = 0;

void handle_signal(int) { signal_count = signal_count + 1; }

bool quit_requested(int timeout_ms) {
	struct pollfd input {
		STDIN_FILENO, POLLIN, 0
	};
	return ::poll(&input, 1, timeout_ms) > 0;
}

void ready() {
	std::puts("READY");
	std::fflush(stdout);
}

void report(const char* code, uint64_t start, size_t before, int access = -1, int cloexec = 0) {
	std::printf("RESULT %s %llu %lld %d %d %d %u\n", code,
	            static_cast<unsigned long long>(elapsed_ms(start)),
	            static_cast<long long>(fd_count()) - static_cast<long long>(before),
	            static_cast<int>(signal_count), access, cloexec, syscall_interruption_count());
	std::fflush(stdout);
}

int request_client(const std::string& path, uint64_t retry_ms, bool readonly) {
	struct sigaction action {};
	action.sa_handler = handle_signal;
	(void)::sigemptyset(&action.sa_mask);
	if (::sigaction(SIGUSR1, &action, nullptr) != 0) return 2;
	const size_t before = fd_count();
	ready();
	const uint64_t start = monotonic_now_ns();
	std::string code;
	int access = -1;
	int cloexec = 0;
	{
		auto result = fd_broker_request_fd(path, 0, TestPayloadV1Schema(), readonly,
		                                   nullptr, retry_ms);
		code = result ? "Ok" : to_string(result.error().code);
		if (result) {
			access = ::fcntl(result.value().get(), F_GETFL) & O_ACCMODE;
			cloexec = ::fcntl(result.value().get(), F_GETFD) & FD_CLOEXEC;
		}
	}
	report(code.c_str(), start, before, access, cloexec);
	return 0;
}

int observer_client(const std::string& name, Transport transport, uint64_t retry_ms) {
	const size_t before = fd_count();
	ready();
	const uint64_t start = monotonic_now_ns();
	std::string code;
	int access = -1;
	int cloexec = 0;
	{
		auto result = open_channel_readonly(name, transport, retry_ms);
		code = result ? "Ok" : to_string(result.error().code);
		if (result) {
			access = ::fcntl(result.value().fd.get(), F_GETFL) & O_ACCMODE;
			cloexec = ::fcntl(result.value().fd.get(), F_GETFD) & FD_CLOEXEC;
		}
	}
	report(code.c_str(), start, before, access, cloexec);
	return 0;
}

int consumer_client(const std::string& name, uint64_t retry_ms) {
	ChannelOptions options;
	options.name = name;
	options.transport = Transport::kMemfdFdPass;
	options.reconnect_timeout = std::chrono::milliseconds(retry_ms);
	ready();
	const uint64_t start = monotonic_now_ns();
	auto result = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	if (!result) {
		report(to_string(result.error().code), start, fd_count());
		return 0;
	}
	auto sample = result.value().try_read_latest();
	report(sample && sample.value().value.counter == 42 ? "Ok" : "BadPayload", start,
	       fd_count());
	return 0;
}

int real_producer(const std::string& name) {
	ChannelOptions options;
	options.name = name;
	options.transport = Transport::kMemfdFdPass;
	{
		auto producer = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
		if (!producer || !producer.value().publish(TestPayloadV1{0x5A000001u, 42, 0}))
			return 2;
		ready();
		if (!quit_requested(5'000)) return 3;
		if (!producer.value().remove_if_owner()) return 4;
	}
	std::puts("STOPPED");
	std::fflush(stdout);
	return 0;
}

bool initialize_image(int descriptor, uint32_t boot_state, uint32_t header_state,
                      const std::string& mutation) {
	if (::ftruncate(descriptor, 4096) != 0) return false;
	BootstrapHeaderAbi boot{};
	std::memcpy(boot.magic, kBootstrapMagic, sizeof(boot.magic));
	boot.abi_major = static_cast<uint16_t>(kAbiMajor);
	boot.abi_minor = static_cast<uint16_t>(kAbiMinor);
	boot.header_size = sizeof(boot);
	boot.expected_mapping_size = 4096;
	boot.creator_nonce_hi = 11;
	boot.creator_nonce_lo = 22;
	boot.init_state = boot_state;
	boot.bootstrap_checksum = bootstrap_checksum_of(boot);
	ChannelHeaderAbi header{};
	std::memcpy(header.magic, kChannelHeaderMagic, sizeof(header.magic));
	header.abi_major = static_cast<uint16_t>(kAbiMajor);
	header.abi_minor = static_cast<uint16_t>(kAbiMinor);
	header.header_size = sizeof(header);
	header.endian_marker = kEndianMarker;
	header.slot_count = kSlotCount;
	header.mapping_size = 4096;
	header.generation = 1;
	header.instance_nonce_hi = 11;
	header.instance_nonce_lo = 22;
	header.init_state = header_state;
	if (mutation == "nonce") header.instance_nonce_lo = 33;
	if (mutation == "zero_nonce") {
		header.instance_nonce_hi = 0;
		header.instance_nonce_lo = 0;
	}
	if (mutation == "size") header.mapping_size = 8192;
	if (mutation == "tiny") boot.expected_mapping_size = sizeof(boot);
	boot.bootstrap_checksum = bootstrap_checksum_of(boot);
	if (!pwrite_full(descriptor, &boot, sizeof(boot), 0) ||
	    !pwrite_full(descriptor, &header, sizeof(header), kChannelHeaderOffset))
		return false;
	return mutation != "tiny" || ::ftruncate(descriptor, sizeof(boot)) == 0;
}

int posix_image(const std::string& name, uint32_t boot_state, uint32_t header_state,
                const std::string& mutation) {
	auto descriptor = shm_open_create(channel_shm_name(name));
	if (!descriptor) return 2;
	uint64_t dev = 0;
	uint64_t ino = 0;
	if (!shm_fstat_and_capture(descriptor.value(), &dev, &ino, nullptr)) return 3;
	const bool initialized =
	        initialize_image(descriptor.value().get(), boot_state, header_state, mutation);
	if (initialized) {
		ready();
		if (quit_requested(5'000)) {
			char command = 0;
			(void)::read(STDIN_FILENO, &command, 1);
			if (command == 'r') {
				auto mapping = mmap_region(descriptor.value(), 4096);
				if (!mapping) return 5;
				auto* base = static_cast<std::byte*>(mapping.value().get());
				auto* header = reinterpret_cast<ChannelHeaderAbi*>(
				        base + kChannelHeaderOffset);
				auto* boot = reinterpret_cast<BootstrapHeaderAbi*>(base);
				shared_store_release(&header->init_state,
				                     static_cast<uint32_t>(InitState::kReady));
				shared_store_release(&boot->init_state,
				                     static_cast<uint32_t>(InitState::kReady));
				std::puts("PROMOTED");
				std::fflush(stdout);
				(void)quit_requested(5'000);
			}
		}
	}
	return edge_test::retain_shm_object(channel_shm_name(name)) && initialized ? 0 : 4;
}

bool send_reply(int socket, FdBrokerReplyAbi reply, int payload, size_t descriptors, size_t bytes) {
	alignas(struct cmsghdr) char control[CMSG_SPACE(8 * sizeof(int))]{};
	struct iovec vector {
		&reply, bytes
	};
	struct msghdr message {};
	message.msg_iov = &vector;
	message.msg_iovlen = 1;
	if (descriptors > 0) {
		message.msg_control = control;
		message.msg_controllen = CMSG_SPACE(descriptors * sizeof(int));
		auto* header = CMSG_FIRSTHDR(&message);
		header->cmsg_level = SOL_SOCKET;
		header->cmsg_type = SCM_RIGHTS;
		header->cmsg_len = CMSG_LEN(descriptors * sizeof(int));
		for (size_t index = 0; index < descriptors; ++index)
			std::memcpy(CMSG_DATA(header) + index * sizeof(int), &payload, sizeof(int));
	}
	return ::sendmsg(socket, &message, MSG_NOSIGNAL) == static_cast<ssize_t>(bytes);
}

int fake_broker(const std::string& path, const std::string& behavior) {
	OwnedPath owned(path);
	auto listener = fd_broker_bind(path);
	auto descriptor = memfd_create_object("fix002_fixture");
	if (!listener || !descriptor || !owned.capture() ||
	    !initialize_image(descriptor.value().get(), behavior == "boot_not_ready" ? 1u : 2u,
	                      behavior == "header_not_ready" ? 1u : 2u, "none"))
		return 2;
	UniqueFd readonly(
	        ::open(("/proc/self/fd/" + std::to_string(descriptor.value().get())).c_str(),
	               O_RDONLY | O_CLOEXEC));
	if (readonly.get() < 0) return 3;
	ready();
	size_t attempts = 0;
	const uint64_t deadline = monotonic_deadline_ns(5'000'000'000);
	while (remaining_time_ns(deadline) != 0 && !quit_requested(0)) {
		struct pollfd socket {
			listener.value().get(), POLLIN, 0
		};
		if (::poll(&socket, 1, 10) <= 0) continue;
		UniqueFd connection(
		        ::accept4(listener.value().get(), nullptr, nullptr, SOCK_CLOEXEC));
		if (connection.get() < 0) continue;
		FdBrokerRequestAbi request{};
		struct pollfd request_ready {
			connection.get(), POLLIN, 0
		};
		if (::poll(&request_ready, 1, 100) <= 0 ||
		    ::recv(connection.get(), &request, sizeof(request), MSG_DONTWAIT) <= 0)
			continue;
		++attempts;
		if (behavior == "stall") {
			(void)quit_requested(2'000);
			continue;
		}
		FdBrokerReplyAbi reply{};
		std::memcpy(reply.magic, kFdBrokerReplyMagic, sizeof(kFdBrokerReplyMagic) - 1);
		reply.mapping_size = 4096;
		reply.generation = behavior == "generation" ? 2 : 1;
		reply.nonce_hi = 11;
		reply.nonce_lo = behavior == "nonce" ? 33 : 22;
		reply.abi_major = static_cast<uint16_t>(kAbiMajor);
		reply.abi_minor = static_cast<uint16_t>(kAbiMinor);
		if (behavior == "abi") reply.abi_major = 2;
		if (behavior == "size") reply.mapping_size = 8192;
		if (behavior == "not_ready")
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kNotReady);
		if (behavior == "mismatch")
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kChannelMismatch);
		if (behavior == "refused")
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kRefused);
		if (behavior == "wrong_magic") reply.magic[0] = 'X';
		reply.checksum = fd_broker_checksum(&reply, sizeof(reply));
		if (behavior == "wrong_checksum") ++reply.checksum;
		const bool early = attempts <= 2;
		size_t rights = 1;
		if (behavior == "no_fd" ||
		    reply.status != static_cast<uint32_t>(FdBrokerStatus::kOk))
			rights = 0;
		if (behavior == "multiple" && early) rights = 2;
		if (behavior == "truncated" && early) rights = 8;
		const size_t bytes = behavior == "short"  ? 32
		                     : behavior == "slow" ? 1
		                                          : sizeof(reply);
		if (behavior == "slow") (void)quit_requested(40);
		const int payload = (request.flags & kFdBrokerFlagReadonly) != 0
		                            ? readonly.get()
		                            : descriptor.value().get();
		(void)send_reply(connection.get(), reply, payload, rights, bytes);
		if (behavior == "slow")
			(void)quit_requested(60);
		else if (behavior != "legal")
			(void)quit_requested(10);
	}
	return 0;
}

int saturated_backlog(const std::string& path) {
	OwnedPath owned(path);
	auto listener = fd_broker_bind(path);
	if (!listener || !owned.capture()) return 2;
	std::vector<UniqueFd> connections;
	for (int attempt = 0; attempt < 16; ++attempt) {
		UniqueFd connection(
		        ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
		struct sockaddr_un address {};
		address.sun_family = AF_UNIX;
		if (connection.get() < 0 || path.size() >= sizeof(address.sun_path)) return 3;
		std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
		(void)::connect(connection.get(), reinterpret_cast<struct sockaddr*>(&address),
		                sizeof(address));
		connections.push_back(std::move(connection));
	}
	ready();
	(void)quit_requested(5'000);
	return 0;
}

}

int main(int argc, char** argv) {
	if (argc < 3) return 1;
	const std::string mode = argv[1];
	const std::string target = argv[2];
	if (mode == "producer" && argc == 4) interrupt_operation(argv[3]);
	if (mode == "request" && argc == 6) interrupt_operation(argv[5]);
	if (mode == "producer") return real_producer(target);
	if (mode == "fake" && argc == 4) return fake_broker(target, argv[3]);
	if (mode == "backlog") return saturated_backlog(target);
	if (mode == "image" && argc == 6)
		return posix_image(
		        target, static_cast<uint32_t>(std::strtoul(argv[3], nullptr, 10)),
		        static_cast<uint32_t>(std::strtoul(argv[4], nullptr, 10)), argv[5]);
	if (mode == "request" && (argc == 5 || argc == 6))
		return request_client(target, std::strtoull(argv[3], nullptr, 10),
		                      std::string(argv[4]) == "ro");
	if (mode == "consumer" && argc == 4)
		return consumer_client(target, std::strtoull(argv[3], nullptr, 10));
	if (mode == "observer" && argc == 5)
		return observer_client(target,
		                       std::string(argv[3]) == "fd" ? Transport::kMemfdFdPass
		                                                    : Transport::kPosixShm,
		                       std::strtoull(argv[4], nullptr, 10));
	return 1;
}
