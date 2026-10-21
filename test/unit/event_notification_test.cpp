#include <dirent.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <future>
#include <mutex>
#include <optional>

#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/notification_backend.hpp"
#include "edge_runtime/channel/notification_service.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/socket_io.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {
using namespace std::chrono_literals;
using edge_runtime::Consumer;
using edge_runtime::Producer;
using edge_runtime::ErrorCode;
using edge_runtime::detail::NotificationRecord;

std::atomic<int> server_event{-1};
std::atomic<int> reply_fault{0};
std::atomic<bool> partial_seen{false};
std::atomic<int> initial_signal_error{0};
thread_local int write_error = 0;
thread_local int write_interrupts = 0;
thread_local int read_interrupts = 0;
thread_local int request_mutation = 0;
thread_local int reply_mutation = 0;
thread_local int poll_interrupts = 0;
thread_local std::function<void()> after_event_read;

bool event_descriptor(int fd) {
	char path[64]{};
	char target[64]{};
	std::snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
	return ::readlink(path, target, sizeof(target) - 1) > 0 &&
	       std::strcmp(target, "anon_inode:[eventfd]") == 0;
}

struct Gate {
	std::mutex mutex;
	std::condition_variable condition;
	int stage = 0;
	bool reached = false;
	bool released = false;
	void arm(int value) {
		std::lock_guard<std::mutex> lock(mutex);
		stage = value; reached = false; released = false;
	}
	void visit(int value) {
		std::unique_lock<std::mutex> lock(mutex);
		if (stage != value) return;
		reached = true;
		condition.notify_all();
		(void)condition.wait_for(lock, 2s, [&] { return released; });
	}
	bool arrived() {
		std::unique_lock<std::mutex> lock(mutex);
		return condition.wait_for(lock, 2s, [&] { return reached; });
	}
	void open() {
		std::lock_guard<std::mutex> lock(mutex);
		released = true; stage = 0; condition.notify_all();
	}
} gate;

void mutate(NotificationRecord& record, int mutation) {
	switch (mutation) {
		case 1: ++record.generation; break;
		case 2: ++record.nonce_hi; break;
		case 3: ++record.producer.epoch; break;
		case 4: ++record.consumer.epoch; break;
		case 5: ++record.consumer.identity.proc_start_ticks; break;
		case 6: ++record.consumer.identity.boot_id_hash_hi; break;
		case 7: ++record.consumer.identity.pid; break;
		case 8: record.fingerprint[0] ^= std::byte{1}; break;
		case 9: ++record.registration_lo; break;
		case 10: ++record.version; break;
		case 11: ++record.magic; break;
		case 13: ++record.producer.identity.proc_start_ticks; break;
		case 14: ++record.producer.identity.boot_id_hash_lo; break;
		case 15: ++record.producer.identity.pid; break;
		case 16: ++record.nonce_lo; break;
		default: break;
	}
	record.checksum = edge_runtime::detail::notification_checksum(record);
	if (mutation == 12) ++record.checksum;
}

int descriptor_count() {
	DIR* directory = ::opendir("/proc/self/fd");
	if (directory == nullptr) return -1;
	int count = 0;
	while (const auto* entry = ::readdir(directory)) {
		if (entry->d_name[0] != '.') ++count;
	}
	(void)::closedir(directory);
	return count;
}

template <typename Word>
struct RestoreWord {
	Word* address;
	Word saved;
	explicit RestoreWord(Word* value) : address(value), saved(*value) {}
	~RestoreWord() { edge_runtime::detail::shared_store_release(address, saved); }
};

struct OwnedPath {
	std::string path;
	struct stat identity{};
	~OwnedPath() {
		struct stat current{};
		if (::lstat(path.c_str(), &current) == 0 && current.st_dev == identity.st_dev &&
		    current.st_ino == identity.st_ino) (void)::unlink(path.c_str());
	}
};
}

extern "C" {
int __real_eventfd(unsigned int, int);
ssize_t __real_write(int, const void*, size_t);
ssize_t __real_read(int, void*, size_t);
ssize_t __real_sendmsg(int, const struct msghdr*, int);
ssize_t __real_recvmsg(int, struct msghdr*, int);
ssize_t __real_send(int, const void*, size_t, int);
ssize_t __real_recv(int, void*, size_t, int);
int __real_poll(struct pollfd*, nfds_t, int);

int __wrap_eventfd(unsigned int value, int flags) {
	gate.visit(1);
	const int descriptor = __real_eventfd(value, flags);
	server_event.store(descriptor);
	return descriptor;
}
ssize_t __wrap_write(int fd, const void* data, size_t size) {
	if (event_descriptor(fd)) {
		if (initial_signal_error.load() != 0) { errno = initial_signal_error.load(); return -1; }
		if (write_interrupts > 0) { --write_interrupts; errno = EINTR; return -1; }
		if (write_error != 0) { errno = write_error; return -1; }
	}
	return __real_write(fd, data, size);
}
ssize_t __wrap_read(int fd, void* data, size_t size) {
	if (event_descriptor(fd) && read_interrupts > 0) { --read_interrupts; errno = EINTR; return -1; }
	const ssize_t result = __real_read(fd, data, size);
	if (event_descriptor(fd) && result == 8 && after_event_read) {
		auto callback = std::move(after_event_read);
		after_event_read = {};
		callback();
	}
	return result;
}
ssize_t __wrap_sendmsg(int fd, const struct msghdr* message, int flags) {
	if (message->msg_iovlen == 1 && message->msg_iov[0].iov_len == sizeof(NotificationRecord)) gate.visit(2);
	const int fault = reply_fault.load();
	if (fault == 1) {
		return __real_send(fd, message->msg_iov[0].iov_base, message->msg_iov[0].iov_len, flags);
	}
	if (fault == 2) {
		struct iovec vector{message->msg_iov[0].iov_base, 1};
		struct msghdr short_message = *message;
		short_message.msg_iov = &vector;
		const ssize_t count = __real_sendmsg(fd, &short_message, flags);
		(void)::shutdown(fd, SHUT_WR);
		return count;
	}
	if (fault == 3 || fault == 4) {
		edge_runtime::detail::UniqueFd invalid(::open("/dev/null", O_RDONLY | O_CLOEXEC | O_NONBLOCK));
		int descriptors[2]{invalid.get(), invalid.get()};
		alignas(struct cmsghdr) char ancillary[CMSG_SPACE(sizeof(descriptors))]{};
		struct msghdr wrong = *message;
		wrong.msg_control = ancillary;
		wrong.msg_controllen = fault == 3 ? CMSG_SPACE(sizeof(int)) : sizeof(ancillary);
		auto* header = CMSG_FIRSTHDR(&wrong);
		header->cmsg_level = SOL_SOCKET;
		header->cmsg_type = SCM_RIGHTS;
		header->cmsg_len = fault == 3 ? CMSG_LEN(sizeof(int)) : CMSG_LEN(sizeof(descriptors));
		std::memcpy(CMSG_DATA(header), descriptors, fault == 3 ? sizeof(int) : sizeof(descriptors));
		return __real_sendmsg(fd, &wrong, flags);
	}
	return __real_sendmsg(fd, message, flags);
}
ssize_t __wrap_recv(int fd, void* data, size_t size, int flags) {
	const ssize_t count = __real_recv(fd, data, size, flags);
	if (size == sizeof(NotificationRecord) && count == 1) partial_seen.store(true);
	return count;
}
ssize_t __wrap_recvmsg(int fd, struct msghdr* message, int flags) {
	const ssize_t result = __real_recvmsg(fd, message, flags);
	if (result == static_cast<ssize_t>(sizeof(NotificationRecord)) && reply_mutation != 0) {
		NotificationRecord record{};
		std::memcpy(&record, message->msg_iov[0].iov_base, sizeof(record));
		mutate(record, reply_mutation);
		std::memcpy(message->msg_iov[0].iov_base, &record, sizeof(record));
	}
	return result;
}
ssize_t __wrap_send(int fd, const void* data, size_t size, int flags) {
	if (size == sizeof(NotificationRecord) && request_mutation != 0) {
		NotificationRecord record{};
		std::memcpy(&record, data, sizeof(record));
		mutate(record, request_mutation);
		return __real_send(fd, &record, sizeof(record), flags);
	}
	return __real_send(fd, data, size, flags);
}
int __wrap_poll(struct pollfd* descriptors, nfds_t count, int timeout) {
	if (poll_interrupts > 0) { --poll_interrupts; errno = EINTR; return -1; }
	return __real_poll(descriptors, count, timeout);
}
}

namespace {
class EventNotificationTest : public ::testing::Test {
       protected:
	edge_runtime::ChannelOptions options;
	std::optional<Producer<TestPayloadV1>> producer;
	std::optional<Consumer<TestPayloadV1>> consumer;
	void SetUp() override {
		options.name = edge_test::unique_channel_name("notify_unit");
		auto created = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
		ASSERT_TRUE(created) << created.error().context;
		producer.emplace(std::move(created.value()));
		auto opened = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
		ASSERT_TRUE(opened);
		consumer.emplace(std::move(opened.value()));
	}
	void TearDown() override {
		gate.open();
		reply_fault.store(0);
		initial_signal_error.store(0);
		write_error = write_interrupts = read_interrupts = request_mutation = reply_mutation = poll_interrupts = 0;
		after_event_read = {};
		consumer.reset();
		if (producer) { EXPECT_TRUE(producer->remove_if_owner()); }
		producer.reset();
	}
	void enable() { ASSERT_TRUE(producer->enable_event_notifications()); }
	void publish(uint64_t value) { ASSERT_TRUE(producer->publish(TestPayloadV1{0x5A000001u, value, 0})); }
	int ready(int fd, int timeout = 0) {
		struct pollfd descriptor{fd, POLLIN, 0};
		return ::poll(&descriptor, 1, timeout);
	}
};

TEST_F(EventNotificationTest, DefaultFutexAndBoundedRegistrationOptions) {
	publish(1);
	ASSERT_TRUE(consumer->wait_latest(10ms));
	const auto started = std::chrono::steady_clock::now();
	auto disabled = consumer->event_notification(0ms);
	EXPECT_FALSE(disabled);
	EXPECT_EQ(disabled.error().code, ErrorCode::kTimeout);
	EXPECT_LT(std::chrono::steady_clock::now() - started, 500ms);
	EXPECT_FALSE(consumer->event_notification(-1ms));
	EXPECT_FALSE(consumer->event_notification(std::chrono::milliseconds::max()));
}

TEST_F(EventNotificationTest, UnknownBackendDoesNotChangeDefaultChannel) {
	const auto unknown = static_cast<edge_runtime::NotificationBackend>(99);
	auto enabled = producer->enable_event_notifications(unknown);
	ASSERT_FALSE(enabled);
	EXPECT_EQ(enabled.error().code, ErrorCode::kInvalidOptions);
	auto registered = consumer->event_notification(unknown, 50ms);
	ASSERT_FALSE(registered);
	EXPECT_EQ(registered.error().code, ErrorCode::kInvalidOptions);
	publish(1);
	EXPECT_TRUE(consumer->wait_latest(50ms));
}

TEST_F(EventNotificationTest, BackendMismatchIsExplicitAndEventfdStillRegisters) {
	enable();
	const auto started = std::chrono::steady_clock::now();
	auto mismatch =
	        consumer->event_notification(edge_runtime::NotificationBackend::kDriver, 500ms);
	ASSERT_FALSE(mismatch);
	EXPECT_EQ(mismatch.error().code, ErrorCode::kTransportFailed);
	EXPECT_EQ(mismatch.error().errno_value, EPROTONOSUPPORT);
	EXPECT_LT(std::chrono::steady_clock::now() - started, 500ms);
	auto registered = consumer->event_notification();
	ASSERT_TRUE(registered);
	EXPECT_EQ(registered.value().backend(), edge_runtime::NotificationBackend::kEventFd);
	EXPECT_TRUE(registered.value().drain());
	publish(1);
	EXPECT_EQ(ready(registered.value().native_handle(), 500), 1);
	EXPECT_TRUE(registered.value().drain());
	EXPECT_TRUE(consumer->try_read_latest());
	EXPECT_FALSE(
	        producer->enable_event_notifications(edge_runtime::NotificationBackend::kDriver));
}

TEST_F(EventNotificationTest, MissingDriverPreservesErrnoWithoutFallback) {
	struct stat device {};
	if (::lstat("/dev/self_ipcs_notification", &device) == 0)
		GTEST_SKIP() << "device present: real driver integration is a separate check";
	const int observed_errno = errno;
	const int before = descriptor_count();
	auto enabled =
	        producer->enable_event_notifications(edge_runtime::NotificationBackend::kDriver);
	ASSERT_FALSE(enabled);
	EXPECT_EQ(enabled.error().code, ErrorCode::kTransportFailed);
	EXPECT_EQ(enabled.error().errno_value, observed_errno);
	EXPECT_EQ(descriptor_count(), before);
	publish(1);
	EXPECT_TRUE(consumer->wait_latest(50ms));
	EXPECT_TRUE(producer->enable_event_notifications());
	EXPECT_TRUE(consumer->event_notification());
}

TEST_F(EventNotificationTest, DescriptorBackendAndFlagsAreStrict) {
	using edge_runtime::NotificationBackend;
	using edge_runtime::detail::notification_fd_valid;
	edge_runtime::detail::UniqueFd descriptor(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
	ASSERT_GE(descriptor.get(), 0);
	EXPECT_TRUE(notification_fd_valid(descriptor.get(), NotificationBackend::kEventFd));
	EXPECT_FALSE(notification_fd_valid(descriptor.get(), NotificationBackend::kDriver));
	ASSERT_EQ(::fcntl(descriptor.get(), F_SETFD, 0), 0);
	EXPECT_FALSE(notification_fd_valid(descriptor.get(), NotificationBackend::kEventFd));
	ASSERT_EQ(::fcntl(descriptor.get(), F_SETFD, FD_CLOEXEC), 0);
	ASSERT_EQ(::fcntl(descriptor.get(), F_SETFL, 0), 0);
	EXPECT_FALSE(notification_fd_valid(descriptor.get(), NotificationBackend::kEventFd));
	edge_runtime::detail::UniqueFd wrong(::open("/dev/null", O_RDWR | O_NONBLOCK | O_CLOEXEC));
	ASSERT_GE(wrong.get(), 0);
	EXPECT_FALSE(notification_fd_valid(wrong.get(), NotificationBackend::kDriver));
	EXPECT_FALSE(notification_fd_valid(wrong.get(), NotificationBackend::kEventFd));
}

TEST_F(EventNotificationTest, DriverSignalSyscallErrorsAreDistinctFromEventfdSaturation) {
	using edge_runtime::NotificationBackend;
	using edge_runtime::detail::notification_signal;
	edge_runtime::detail::UniqueFd descriptor(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
	ASSERT_GE(descriptor.get(), 0);
	write_error = EAGAIN;
	EXPECT_TRUE(notification_signal(descriptor.get(), NotificationBackend::kEventFd));
	auto busy = notification_signal(descriptor.get(), NotificationBackend::kDriver);
	ASSERT_FALSE(busy);
	EXPECT_EQ(busy.error().errno_value, EAGAIN);
	write_error = EOVERFLOW;
	auto overflow = notification_signal(descriptor.get(), NotificationBackend::kDriver);
	ASSERT_FALSE(overflow);
	EXPECT_EQ(overflow.error().errno_value, EOVERFLOW);
	write_error = 0;
	write_interrupts = 7;
	EXPECT_TRUE(notification_signal(descriptor.get(), NotificationBackend::kDriver));
	write_interrupts = 8;
	auto interrupted = notification_signal(descriptor.get(), NotificationBackend::kDriver);
	ASSERT_FALSE(interrupted);
	EXPECT_EQ(interrupted.error().errno_value, EINTR);
}

TEST_F(EventNotificationTest, PublishBeforeRegistrationMoveAndCloexec) {
	publish(17);
	enable();
	auto registered = consumer->event_notification();
	ASSERT_TRUE(registered) << registered.error().context;
	edge_runtime::EventNotification moved(std::move(registered.value()));
	EXPECT_EQ(registered.value().native_handle(), -1);
	EXPECT_FALSE(registered.value().drain());
	EXPECT_NE(::fcntl(moved.native_handle(), F_GETFD) & FD_CLOEXEC, 0);
	ASSERT_EQ(ready(moved.native_handle(), 100), 1);
	ASSERT_TRUE(moved.drain());
	auto sample = consumer->try_read_latest();
	ASSERT_TRUE(sample);
	EXPECT_EQ(sample.value().value.counter, 17u);
	EXPECT_EQ(ready(moved.native_handle()), 0);
}

TEST_F(EventNotificationTest, PublishBeforeInstallAndBeforeReplyDeterministic) {
	enable();
	for (int stage : {1, 2}) {
		gate.arm(stage);
		auto pending = std::async(std::launch::async, [&] { return consumer->event_notification(1500ms); });
		const bool arrived = gate.arrived();
		if (arrived) publish(static_cast<uint64_t>(stage));
		gate.open();
		ASSERT_TRUE(arrived);
		auto registered = pending.get();
		ASSERT_TRUE(registered);
		ASSERT_EQ(ready(registered.value().native_handle(), 100), 1);
		ASSERT_TRUE(registered.value().drain());
		auto sample = consumer->try_read_latest();
		ASSERT_TRUE(sample);
		EXPECT_EQ(sample.value().value.counter, static_cast<uint64_t>(stage));
	}
}

TEST_F(EventNotificationTest, CheckThenPublishAndPublishDuringDrain) {
	enable();
	auto notification = consumer->event_notification();
	ASSERT_TRUE(notification);
	ASSERT_TRUE(notification.value().drain());
	EXPECT_FALSE(consumer->try_read_latest());
	publish(1);
	ASSERT_EQ(ready(notification.value().native_handle(), 100), 1);
	after_event_read = [&] { publish(2); };
	ASSERT_TRUE(notification.value().drain());
	auto latest = consumer->try_read_latest();
	ASSERT_TRUE(latest);
	EXPECT_EQ(latest.value().value.counter, 2u);
	EXPECT_EQ(ready(notification.value().native_handle(), 100), 1);
	EXPECT_TRUE(notification.value().drain());
	EXPECT_EQ(ready(notification.value().native_handle()), 0);
}

TEST_F(EventNotificationTest, CoalescedLoanCommitAndAbort) {
	enable();
	auto notification = consumer->event_notification();
	ASSERT_TRUE(notification);
	ASSERT_TRUE(notification.value().drain());
	{ auto aborted = producer->loan(); ASSERT_TRUE(aborted); }
	EXPECT_EQ(ready(notification.value().native_handle()), 0);
	for (uint64_t value = 1; value <= 100; ++value) publish(value);
	ASSERT_TRUE(notification.value().drain());
	auto latest = consumer->try_read_latest();
	ASSERT_TRUE(latest);
	EXPECT_EQ(latest.value().sequence, 100u);
	auto loan = producer->loan();
	ASSERT_TRUE(loan);
	ASSERT_TRUE(edge_runtime::PayloadCodec<TestPayloadV1>::encode(
	        TestPayloadV1{0x5A000001u, 101, 0}, loan.value().data(), loan.value().size()));
	ASSERT_TRUE(loan.value().commit());
	ASSERT_EQ(ready(notification.value().native_handle(), 100), 1);
	EXPECT_TRUE(notification.value().drain());
	EXPECT_TRUE(consumer->try_loan_latest());
}

TEST_F(EventNotificationTest, SaturationAndInterruptedWritesKeepCommittedSuccess) {
	enable();
	auto notification = consumer->event_notification();
	ASSERT_TRUE(notification);
	ASSERT_TRUE(notification.value().drain());
	const uint64_t saturated = UINT64_MAX - 1;
	ASSERT_EQ(__real_write(server_event.load(), &saturated, sizeof(saturated)), 8);
	publish(1);
	EXPECT_EQ(producer->event_notification_error(), 0);
	EXPECT_TRUE(notification.value().drain());
	write_interrupts = 7;
	publish(2);
	EXPECT_EQ(write_interrupts, 0);
	read_interrupts = 7;
	EXPECT_TRUE(notification.value().drain());
	write_interrupts = 8;
	publish(3);
	EXPECT_EQ(write_interrupts, 0);
	EXPECT_EQ(producer->event_notification_error(), EINTR);
	EXPECT_EQ(ready(notification.value().native_handle(), 100), 1);
	auto latest = consumer->try_read_latest();
	ASSERT_TRUE(latest);
	EXPECT_EQ(latest.value().sequence, 3u);
}

TEST_F(EventNotificationTest, NotificationFailureDoesNotRollBackAndReregisters) {
	enable();
	auto old = consumer->event_notification();
	ASSERT_TRUE(old);
	ASSERT_TRUE(old.value().drain());
	write_error = EIO;
	publish(1);
	write_error = 0;
	EXPECT_EQ(producer->event_notification_error(), EIO);
	ASSERT_EQ(ready(old.value().native_handle(), 100), 1);
	EXPECT_FALSE(old.value().drain());
	auto latest = consumer->try_read_latest();
	ASSERT_TRUE(latest);
	EXPECT_EQ(latest.value().sequence, 1u);
	auto current = consumer->event_notification();
	ASSERT_TRUE(current);
	EXPECT_TRUE(current.value().drain());
	publish(2);
	EXPECT_EQ(ready(current.value().native_handle(), 100), 1);
	EXPECT_TRUE(current.value().drain());
}

TEST_F(EventNotificationTest, InitialSignalFailureIsDiagnosedAndRegistrationRecovers) {
	enable();
	initial_signal_error.store(EIO);
	EXPECT_FALSE(consumer->event_notification(30ms));
	EXPECT_EQ(producer->event_notification_error(), EIO);
	initial_signal_error.store(0);
	auto notification = consumer->event_notification();
	ASSERT_TRUE(notification);
	EXPECT_TRUE(notification.value().drain());
	EXPECT_TRUE(producer->disable_event_notifications());
	EXPECT_TRUE(producer->enable_event_notifications());
	EXPECT_EQ(producer->event_notification_error(), 0);
}

TEST_F(EventNotificationTest, OldRegistrationAndOldConsumerCannotFollowNewRole) {
	enable();
	auto first = consumer->event_notification();
	ASSERT_TRUE(first);
	auto second = consumer->event_notification();
	ASSERT_TRUE(second);
	ASSERT_EQ(ready(first.value().native_handle(), 100), 1);
	EXPECT_FALSE(first.value().drain());
	consumer.reset();
	EXPECT_FALSE(second.value().drain());
	auto opened = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(opened);
	consumer.emplace(std::move(opened.value()));
	auto third = consumer->event_notification();
	ASSERT_TRUE(third);
	EXPECT_TRUE(third.value().drain());
	publish(1);
	EXPECT_EQ(ready(third.value().native_handle(), 100), 1);
	EXPECT_FALSE(first.value().drain());
}

TEST_F(EventNotificationTest, ReplacementClosesOldFdBeforeNewReply) {
	enable();
	auto old = consumer->event_notification();
	ASSERT_TRUE(old);
	ASSERT_TRUE(old.value().drain());
	gate.arm(2);
	auto pending = std::async(std::launch::async, [&] { return consumer->event_notification(1500ms); });
	const bool arrived = gate.arrived();
	const int old_ready = arrived ? ready(old.value().native_handle(), 100) : -1;
	gate.open();
	auto current = pending.get();
	ASSERT_TRUE(arrived);
	ASSERT_TRUE(current);
	EXPECT_EQ(old_ready, 1);
	EXPECT_FALSE(old.value().drain());
	EXPECT_TRUE(current.value().drain());
}

TEST_F(EventNotificationTest, DisabledSidecarWakesAndReenableWorks) {
	enable();
	ASSERT_TRUE(producer->enable_event_notifications());
	auto notification = consumer->event_notification();
	ASSERT_TRUE(notification);
	ASSERT_TRUE(notification.value().drain());
	ASSERT_TRUE(producer->disable_event_notifications());
	EXPECT_EQ(ready(notification.value().native_handle(), 100), 1);
	EXPECT_FALSE(notification.value().drain());
	ASSERT_TRUE(producer->enable_event_notifications());
	auto current = consumer->event_notification();
	ASSERT_TRUE(current);
	EXPECT_TRUE(current.value().drain());
}

TEST_F(EventNotificationTest, DeferredProducerShutdownAfterLoanStillCommitsAndWakes) {
	enable();
	auto notification = consumer->event_notification();
	ASSERT_TRUE(notification);
	ASSERT_TRUE(notification.value().drain());
	auto loan = producer->loan();
	ASSERT_TRUE(loan);
	ASSERT_TRUE(edge_runtime::PayloadCodec<TestPayloadV1>::encode(
	        TestPayloadV1{0x5A000001u, 42, 0}, loan.value().data(), loan.value().size()));
	producer.reset();
	ASSERT_TRUE(loan.value().commit());
	EXPECT_EQ(ready(notification.value().native_handle(), 100), 1);
	auto stopped = notification.value().drain();
	EXPECT_FALSE(stopped);
	EXPECT_EQ(stopped.error().code, ErrorCode::kProducerOffline);
	auto sample = consumer->try_read_latest();
	ASSERT_TRUE(sample);
	EXPECT_EQ(sample.value().value.counter, 42u);
	auto replacement = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(replacement);
	producer.emplace(std::move(replacement.value()));
}

TEST_F(EventNotificationTest, RemovedInstanceCannotRestartSidecar) {
	enable();
	EXPECT_TRUE(producer->remove_if_owner());
	auto restarted = producer->enable_event_notifications();
	EXPECT_FALSE(restarted);
	EXPECT_EQ(restarted.error().code, ErrorCode::kStaleHandle);
	auto replacement = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(replacement);
	producer.emplace(std::move(replacement.value()));
}

TEST_F(EventNotificationTest, InterruptedDrainIsBoundedAndRetryable) {
	enable();
	auto notification = consumer->event_notification();
	ASSERT_TRUE(notification);
	read_interrupts = 8;
	auto interrupted = notification.value().drain();
	EXPECT_FALSE(interrupted);
	EXPECT_EQ(interrupted.error().errno_value, EINTR);
	EXPECT_EQ(read_interrupts, 0);
	EXPECT_TRUE(notification.value().drain());
}

TEST_F(EventNotificationTest, ActiveLoansRejectNotificationHandleReentry) {
	enable();
	auto notification = consumer->event_notification();
	ASSERT_TRUE(notification);
	{
		auto writing = producer->loan();
		ASSERT_TRUE(writing);
		EXPECT_EQ(producer->enable_event_notifications().error().code, ErrorCode::kConcurrentHandleUse);
		EXPECT_EQ(producer->disable_event_notifications().error().code, ErrorCode::kConcurrentHandleUse);
	}
	publish(1);
	{
		auto reading = consumer->try_loan_latest();
		ASSERT_TRUE(reading);
		EXPECT_EQ(consumer->event_notification().error().code, ErrorCode::kConcurrentHandleUse);
		EXPECT_EQ(notification.value().drain().error().code, ErrorCode::kConcurrentHandleUse);
	}
	EXPECT_TRUE(notification.value().drain());
}

TEST_F(EventNotificationTest, InvalidRequestsAndLateRepliesDoNotResetDeadline) {
	enable();
	for (int mutation : {1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16}) {
		request_mutation = mutation;
		const auto started = std::chrono::steady_clock::now();
		auto invalid = consumer->event_notification(30ms);
		EXPECT_FALSE(invalid) << mutation;
		EXPECT_LT(std::chrono::steady_clock::now() - started, 500ms);
	}
	request_mutation = 0;
	for (int mutation : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}) {
		reply_mutation = mutation;
		poll_interrupts = 100;
		const auto started = std::chrono::steady_clock::now();
		auto invalid = consumer->event_notification(30ms);
		EXPECT_FALSE(invalid) << mutation;
		EXPECT_LT(std::chrono::steady_clock::now() - started, 500ms);
	}
	reply_mutation = 0;
	auto current = consumer->event_notification();
	ASSERT_TRUE(current);
	EXPECT_TRUE(current.value().drain());
}

TEST_F(EventNotificationTest, PartialPeerHasBoundedBudgetAndStop) {
	enable();
	edge_runtime::detail::UniqueFd partial(::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
	const edge_runtime::detail::socket_io::IoBudget budget{
	        edge_runtime::detail::monotonic_deadline_ns(500'000'000)};
	ASSERT_TRUE(edge_runtime::detail::socket_io::connect_socket(
	        partial.get(), edge_runtime::detail::channel_socket_path(options.name) + ".notify", budget));
	const char first = 1;
	ASSERT_EQ(::send(partial.get(), &first, 1, MSG_NOSIGNAL), 1);
	auto legitimate = consumer->event_notification(500ms);
	ASSERT_TRUE(legitimate);
	EXPECT_TRUE(legitimate.value().drain());
	partial_seen.store(false);
	edge_runtime::detail::UniqueFd another(::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
	ASSERT_TRUE(edge_runtime::detail::socket_io::connect_socket(
	        another.get(), edge_runtime::detail::channel_socket_path(options.name) + ".notify", budget));
	ASSERT_EQ(::send(another.get(), &first, 1, MSG_NOSIGNAL), 1);
	const auto peer_deadline = std::chrono::steady_clock::now() + 500ms;
	while (!partial_seen.load() && std::chrono::steady_clock::now() < peer_deadline) {
		(void)::poll(nullptr, 0, 1);
	}
	ASSERT_TRUE(partial_seen.load());
	const auto started = std::chrono::steady_clock::now();
	EXPECT_TRUE(producer->disable_event_notifications());
	EXPECT_LT(std::chrono::steady_clock::now() - started, 500ms);
}

TEST_F(EventNotificationTest, ShortMissingWrongAndExtraDescriptorsFailClosed) {
	enable();
	for (int fault : {1, 2, 3, 4}) {
		reply_fault.store(fault);
		const auto started = std::chrono::steady_clock::now();
		auto invalid = consumer->event_notification(30ms);
		EXPECT_FALSE(invalid) << fault;
		EXPECT_LT(std::chrono::steady_clock::now() - started, 500ms);
	}
	reply_fault.store(0);
	auto valid = consumer->event_notification();
	ASSERT_TRUE(valid);
	EXPECT_TRUE(valid.value().drain());
}

TEST_F(EventNotificationTest, RepeatedRegistrationsAndMalformedRightsLeaveNoDescriptors) {
	const int before = descriptor_count();
	ASSERT_GT(before, 0);
	enable();
	for (int index = 0; index < 15; ++index) {
		{
			auto valid = consumer->event_notification();
			ASSERT_TRUE(valid);
			EXPECT_TRUE(valid.value().drain());
		}
		reply_fault.store(4);
		EXPECT_FALSE(consumer->event_notification(0ms));
		reply_fault.store(0);
	}
	ASSERT_TRUE(producer->disable_event_notifications());
	EXPECT_EQ(descriptor_count(), before);
}

TEST_F(EventNotificationTest, PreReadyAndInvalidOwnerCannotRegister) {
	using namespace edge_runtime::detail;
	enable();
	auto descriptor = shm_open_existing(channel_shm_name(options.name));
	ASSERT_TRUE(descriptor);
	uint64_t device = 0, inode = 0, size = 0;
	ASSERT_TRUE(shm_fstat_and_capture(descriptor.value(), &device, &inode, &size));
	auto mapping = mmap_region(descriptor.value(), size);
	ASSERT_TRUE(mapping);
	auto* base = static_cast<std::byte*>(mapping.value().get());
	auto* bootstrap = reinterpret_cast<BootstrapHeaderAbi*>(base);
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	for (auto* state : {&bootstrap->init_state, &header->init_state}) {
		RestoreWord<uint32_t> restore(state);
		shared_store_release(state, static_cast<uint32_t>(InitState::kInitializing));
		auto incomplete = consumer->event_notification(0ms);
		ASSERT_FALSE(incomplete);
		EXPECT_EQ(incomplete.error().code, ErrorCode::kStaleHandle);
	}
	for (auto* owner_pid : {&header->producer.pid, &header->consumer.pid}) {
		RestoreWord<uint64_t> restore(owner_pid);
		shared_store_release(owner_pid, uint64_t{0});
		EXPECT_FALSE(consumer->event_notification(0ms));
	}
	auto valid = consumer->event_notification();
	ASSERT_TRUE(valid);
	EXPECT_TRUE(valid.value().drain());
}

TEST_F(EventNotificationTest, ForkChildLocalDestructionKeepsParentRegistration) {
	enable();
	auto registered = consumer->event_notification();
	ASSERT_TRUE(registered);
	std::optional<edge_runtime::EventNotification> notification;
	notification.emplace(std::move(registered.value()));
	ASSERT_TRUE(notification->drain());
	const pid_t child = ::fork();
	ASSERT_GE(child, 0);
	if (child == 0) {
		notification.reset();
		_exit(0);
	}
	int status = 0;
	const auto deadline = std::chrono::steady_clock::now() + 1s;
	pid_t waited = 0;
	while (waited == 0 && std::chrono::steady_clock::now() < deadline) {
		waited = ::waitpid(child, &status, WNOHANG);
		if (waited == 0) (void)::poll(nullptr, 0, 1);
	}
	if (waited == 0) {
		(void)::kill(child, SIGKILL);
		while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {}
	}
	ASSERT_EQ(waited, child);
	ASSERT_TRUE(WIFEXITED(status));
	EXPECT_EQ(WEXITSTATUS(status), 0);
	publish(1);
	EXPECT_EQ(ready(notification->native_handle(), 100), 1);
	EXPECT_TRUE(notification->drain());
	EXPECT_TRUE(consumer->try_read_latest());
}

TEST_F(EventNotificationTest, PrivateSocketAndReplacementSentinelArePreserved) {
	enable();
	const auto path = edge_runtime::detail::channel_socket_path(options.name) + ".notify";
	struct stat socket_identity{};
	ASSERT_EQ(::lstat(path.c_str(), &socket_identity), 0);
	EXPECT_TRUE(S_ISSOCK(socket_identity.st_mode));
	EXPECT_EQ(socket_identity.st_mode & 0777, 0600u);
	const auto held = path + ".held";
	ASSERT_EQ(::rename(path.c_str(), held.c_str()), 0);
	OwnedPath owned_socket{held, socket_identity};
	edge_runtime::detail::UniqueFd sentinel(::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
	ASSERT_GE(sentinel.get(), 0);
	struct stat sentinel_identity{};
	ASSERT_EQ(::fstat(sentinel.get(), &sentinel_identity), 0);
	OwnedPath owned_sentinel{path, sentinel_identity};
	const char contents[] = "external-sentinel";
	ASSERT_EQ(::write(sentinel.get(), contents, sizeof(contents)), static_cast<ssize_t>(sizeof(contents)));
	EXPECT_TRUE(producer->disable_event_notifications());
	EXPECT_FALSE(producer->enable_event_notifications());
	struct stat preserved{};
	ASSERT_EQ(::lstat(path.c_str(), &preserved), 0);
	EXPECT_EQ(preserved.st_dev, sentinel_identity.st_dev);
	EXPECT_EQ(preserved.st_ino, sentinel_identity.st_ino);
	char observed[sizeof(contents)]{};
	ASSERT_EQ(::pread(sentinel.get(), observed, sizeof(observed), 0), static_cast<ssize_t>(sizeof(observed)));
	EXPECT_EQ(std::memcmp(contents, observed, sizeof(contents)), 0);
}
}
