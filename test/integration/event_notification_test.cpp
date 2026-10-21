#include <gtest/gtest.h>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <optional>
#include <string>

#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "edge_runtime/channel/channel_abi.hpp"
#include "edge_runtime/channel/notification_service.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/transport/fd_broker.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/transport/socket_io.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {
using namespace std::chrono_literals;
using Producer = edge_runtime::Producer<TestPayloadV1>;
using Consumer = edge_runtime::Consumer<TestPayloadV1>;
using edge_runtime::ErrorCode;
using edge_runtime::detail::UniqueFd;

class Peer {
       public:
	~Peer() { kill_and_wait(); }
	bool start(const char* role, const edge_runtime::ChannelOptions& options) {
		int sockets[2]{};
		if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, sockets) != 0) return false;
		UniqueFd parent(sockets[0]);
		UniqueFd child(sockets[1]);
		const char* transport = options.transport == edge_runtime::Transport::kMemfdFdPass ? "memfd" : "shm";
		pid_ = ::fork();
		if (pid_ < 0) return false;
		if (pid_ == 0) {
			(void)::setpgid(0, 0);
			if (::dup2(child.get(), STDIN_FILENO) < 0 || ::dup2(child.get(), STDOUT_FILENO) < 0) _exit(126);
			(void)::close(parent.get());
			(void)::close(child.get());
			::execl(NOTIFICATION_PEER, NOTIFICATION_PEER, role, options.name.c_str(), transport,
			        static_cast<char*>(nullptr));
			_exit(127);
		}
		(void)::setpgid(pid_, pid_);
		connection_ = std::move(parent);
		return true;
	}
	bool send(char command) { return ::send(connection_.get(), &command, 1, MSG_NOSIGNAL) == 1; }
	bool expect(const char* expected) {
		std::string text;
		const auto deadline = std::chrono::steady_clock::now() + 5s;
		while (std::chrono::steady_clock::now() < deadline) {
			struct pollfd descriptor{connection_.get(), POLLIN, 0};
			if (::poll(&descriptor, 1, 20) <= 0) continue;
			char value = 0;
			if (::recv(connection_.get(), &value, 1, MSG_DONTWAIT) != 1) return false;
			text.push_back(value);
			if (value == '\n') return text == expected;
		}
		return false;
	}
	void kill_and_wait() {
		if (pid_ <= 0) return;
		(void)::kill(-pid_, SIGKILL);
		(void)::kill(pid_, SIGKILL);
		int status = 0;
		while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {}
		pid_ = -1;
	}
	bool wait() {
		const auto deadline = std::chrono::steady_clock::now() + 3s;
		while (std::chrono::steady_clock::now() < deadline) {
			int status = 0;
			if (::waitpid(pid_, &status, WNOHANG) == pid_) {
				pid_ = -1;
				return WIFEXITED(status) && WEXITSTATUS(status) == 0;
			}
			(void)::poll(nullptr, 0, 5);
		}
		return false;
	}
       private:
	pid_t pid_ = -1;
	UniqueFd connection_;
};

int ready(int fd) {
	struct pollfd descriptor{fd, POLLIN, 0};
	return ::poll(&descriptor, 1, 1000);
}

class NotificationCrossExec : public ::testing::TestWithParam<edge_runtime::Transport> {
       protected:
	edge_runtime::ChannelOptions options;
	std::optional<Producer> owner;
	void SetUp() override {
		options.name = edge_test::unique_channel_name("notify_exec");
		options.transport = GetParam();
	}
	void TearDown() override {
		if (!owner) {
			auto recovered = Producer::create(options, TestPayloadV1Schema());
			if (recovered) owner.emplace(std::move(recovered.value()));
		}
		if (owner) { EXPECT_TRUE(owner->remove_if_owner()); }
	}
	void create() {
		auto producer = Producer::create(options, TestPayloadV1Schema());
		ASSERT_TRUE(producer);
		owner.emplace(std::move(producer.value()));
		ASSERT_TRUE(owner->enable_event_notifications());
		ASSERT_TRUE(owner->publish(TestPayloadV1{0x5A000001u, 1, 0}));
	}
};

TEST_P(NotificationCrossExec, IndependentProducerAndCleanExit) {
	Peer peer;
	ASSERT_TRUE(peer.start("producer", options));
	ASSERT_TRUE(peer.expect("READY\n"));
	auto consumer = Consumer::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	auto notification = consumer.value().event_notification();
	ASSERT_TRUE(notification);
	ASSERT_TRUE(notification.value().drain());
	auto initial = consumer.value().try_read_latest();
	ASSERT_TRUE(initial);
	EXPECT_EQ(initial.value().value.counter, 1u);
	ASSERT_TRUE(peer.send('p'));
	ASSERT_TRUE(peer.expect("PUBLISHED\n"));
	ASSERT_EQ(ready(notification.value().native_handle()), 1);
	ASSERT_TRUE(notification.value().drain());
	auto latest = consumer.value().try_read_latest();
	ASSERT_TRUE(latest);
	EXPECT_EQ(latest.value().value.counter, 2u);
	ASSERT_TRUE(peer.send('q'));
	ASSERT_TRUE(peer.expect("CLEAN\n"));
	ASSERT_TRUE(peer.wait());
	ASSERT_EQ(ready(notification.value().native_handle()), 1);
	auto stopped = notification.value().drain();
	ASSERT_FALSE(stopped);
	EXPECT_EQ(stopped.error().code, ErrorCode::kProducerOffline);
}

TEST_P(NotificationCrossExec, KilledProducerAndNewInstanceRejectOldFd) {
	Peer peer;
	ASSERT_TRUE(peer.start("producer", options));
	ASSERT_TRUE(peer.expect("READY\n"));
	auto consumer = Consumer::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	auto old = consumer.value().event_notification();
	ASSERT_TRUE(old);
	ASSERT_TRUE(old.value().drain());
	peer.kill_and_wait();
	ASSERT_EQ(ready(old.value().native_handle()), 1);
	auto stopped = old.value().drain();
	ASSERT_FALSE(stopped);
	EXPECT_EQ(stopped.error().code, ErrorCode::kProducerOffline);
	create();
	EXPECT_FALSE(old.value().drain());
	ASSERT_TRUE(consumer.value().reconnect());
	EXPECT_FALSE(old.value().drain());
	auto current = consumer.value().event_notification();
	ASSERT_TRUE(current);
	EXPECT_TRUE(current.value().drain());
	auto sample = consumer.value().try_read_latest();
	ASSERT_TRUE(sample);
	EXPECT_EQ(sample.value().generation, owner->generation());
}

TEST_P(NotificationCrossExec, KilledConsumerAndNewRoleRegisters) {
	create();
	Peer peer;
	ASSERT_TRUE(peer.start("consumer", options));
	ASSERT_TRUE(peer.expect("READY\n"));
	peer.kill_and_wait();
	auto consumer = Consumer::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	auto notification = consumer.value().event_notification();
	ASSERT_TRUE(notification);
	ASSERT_TRUE(notification.value().drain());
	ASSERT_TRUE(owner->publish(TestPayloadV1{0x5A000001u, 2, 0}));
	ASSERT_EQ(ready(notification.value().native_handle()), 1);
	EXPECT_TRUE(notification.value().drain());
	auto sample = consumer.value().try_read_latest();
	ASSERT_TRUE(sample);
	EXPECT_EQ(sample.value().value.counter, 2u);
}

TEST_P(NotificationCrossExec, SameUidDifferentProcessCannotStealConsumerNotification) {
	using namespace edge_runtime::detail;
	create();
	Peer peer;
	ASSERT_TRUE(peer.start("consumer", options));
	ASSERT_TRUE(peer.expect("READY\n"));
	auto descriptor = options.transport == edge_runtime::Transport::kPosixShm
	        ? shm_open_existing(channel_shm_name(options.name))
	        : fd_broker_request_fd(channel_socket_path(options.name),
	                               channel_name_hash(options.name.c_str(), options.name.size()),
	                               TestPayloadV1Schema(), false, nullptr, 100);
	ASSERT_TRUE(descriptor);
	uint64_t device = 0, inode = 0, size = 0;
	ASSERT_TRUE(shm_fstat_and_capture(descriptor.value(), &device, &inode, &size));
	auto mapping = mmap_region(descriptor.value(), size);
	ASSERT_TRUE(mapping);
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(
	        static_cast<std::byte*>(mapping.value().get()) + kChannelHeaderOffset);
	auto producer_identity = identity_snapshot_read(&header->producer);
	auto consumer_identity = identity_snapshot_read(&header->consumer);
	ASSERT_TRUE(producer_identity);
	ASSERT_TRUE(consumer_identity);
	const auto role = [](const ProcessIdentityAbi& identity) {
		return NotificationRole{identity.role_epoch, {identity.pid, identity.proc_start_ticks,
		                       identity.boot_id_hash_hi, identity.boot_id_hash_lo}};
	};
	NotificationRecord request{};
	request.magic = kNotificationMagic;
	request.version = request.kind = 1;
	request.generation = header->generation;
	request.nonce_hi = header->instance_nonce_hi;
	request.nonce_lo = header->instance_nonce_lo;
	request.producer = role(producer_identity.value());
	request.consumer = role(consumer_identity.value());
	std::memcpy(request.fingerprint.data(), header->schema_fingerprint, request.fingerprint.size());
	request.registration_hi = 123;
	request.checksum = notification_checksum(request);
	UniqueFd connection(::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
	const socket_io::IoBudget budget{monotonic_deadline_ns(500'000'000)};
	ASSERT_TRUE(socket_io::connect_socket(connection.get(), channel_socket_path(options.name) + ".notify", budget));
	ASSERT_TRUE(socket_io::send_full(connection.get(), &request, sizeof(request), budget));
	ASSERT_TRUE(socket_io::wait_socket(connection.get(), POLLIN, budget));
	char reply = 0;
	EXPECT_EQ(::recv(connection.get(), &reply, 1, MSG_DONTWAIT), 0);
	ASSERT_TRUE(peer.send('q'));
	ASSERT_TRUE(peer.expect("CLEAN\n"));
	ASSERT_TRUE(peer.wait());
	auto consumer = Consumer::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	auto registered = consumer.value().event_notification();
	ASSERT_TRUE(registered);
	EXPECT_TRUE(registered.value().drain());
}

INSTANTIATE_TEST_SUITE_P(Transports, NotificationCrossExec,
                        ::testing::Values(edge_runtime::Transport::kPosixShm,
                                          edge_runtime::Transport::kMemfdFdPass));
}
