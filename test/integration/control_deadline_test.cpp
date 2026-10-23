#include <fcntl.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <optional>
#include <thread>

#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/channel/control_operation.hpp"
#include "edge_runtime/sync/cancellation.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/utility/failpoint.hpp"
#include "self_ipcs/channel/consumer.hpp"
#include "self_ipcs/channel/control_context.hpp"
#include "self_ipcs/channel/producer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {
using namespace std::chrono_literals;
using namespace edge_runtime;
using ProducerType = Producer<TestPayloadV1>;
using ConsumerType = Consumer<TestPayloadV1>;
thread_local std::function<void()> lock_blocked;
thread_local std::function<void()> lock_acquired;
thread_local std::function<void(int)> sync_completed;
thread_local std::function<void(const char*)> checkpoint;
thread_local std::function<void()> futex_entering;
thread_local int sync_count = 0;
thread_local int poll_interrupts = 0;
thread_local std::function<void()> reply_received;
thread_local bool deny_cancel_event = false;
thread_local int cancel_write_interrupts = 0;
thread_local bool bypass_event_gate = false;

struct EventGate {
	std::mutex mutex;
	std::condition_variable condition;
	bool armed{false};
	bool entered{false};
	void arm() {
		std::lock_guard<std::mutex> guard(mutex);
		entered = false;
		armed = true;
	}
	void visit() {
		std::unique_lock<std::mutex> guard(mutex);
		if (!armed) return;
		entered = true;
		condition.notify_all();
		(void)condition.wait_for(guard, 3s, [&] { return !armed; });
	}
	bool wait() {
		std::unique_lock<std::mutex> guard(mutex);
		return condition.wait_for(guard, 2s, [&] { return entered; });
	}
	void release() {
		std::lock_guard<std::mutex> guard(mutex);
		armed = false;
		condition.notify_all();
	}
} event_gate;

ControlOperationContext budget(std::chrono::milliseconds timeout = 2s,
                               CancellationToken token = {}) {
	return ControlOperationContext::after(timeout, std::move(token)).value();
}

ChannelOptions options_for(const char* tag, Transport transport = Transport::kPosixShm) {
	ChannelOptions options;
	options.name = edge_test::unique_channel_name(tag);
	options.transport = transport;
	std::cout << "control-fixture name=" << options.name
	          << " transport=" << static_cast<int>(transport) << std::endl;
	return options;
}

class HeldLock {
       public:
	explicit HeldLock(const std::string& name, bool producer = false) {
		int ready[2]{-1, -1};
		int release[2]{-1, -1};
		if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, ready) != 0 ||
		    ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, release) != 0)
			return;
		const std::string path = detail::channel_lock_path(name);
		const std::string ready_text = std::to_string(ready[1]);
		const std::string release_text = std::to_string(release[0]);
		pid_ = ::fork();
		if (pid_ == 0) {
			(void)::fcntl(ready[1], F_SETFD, 0);
			(void)::fcntl(release[0], F_SETFD, 0);
			if (producer)
				::execl(CONTROL_LOCK_PEER, CONTROL_LOCK_PEER, "--producer",
				        name.c_str(), ready_text.c_str(), release_text.c_str(),
				        nullptr);
			else
				::execl(CONTROL_LOCK_PEER, CONTROL_LOCK_PEER, path.c_str(),
				        ready_text.c_str(), release_text.c_str(), nullptr);
			::_exit(127);
		}
		::close(ready[1]);
		::close(release[0]);
		release_ = release[1];
		struct pollfd descriptor {
			ready[0], POLLIN, 0
		};
		char status = 0;
		ready_ = ::poll(&descriptor, 1, 2000) > 0 && ::read(ready[0], &status, 1) == 1 &&
		         status == 'r';
		::close(ready[0]);
	}
	HeldLock(const HeldLock&) = delete;
	HeldLock& operator=(const HeldLock&) = delete;
	~HeldLock() { release(); }
	bool ready() const { return ready_; }
	void release() {
		if (pid_ <= 0) return;
		const char command = 'q';
		(void)::send(release_, &command, 1, MSG_NOSIGNAL);
		::close(release_);
		const uint64_t until = detail::monotonic_deadline_ns(2'000'000'000);
		int status = 0;
		while (::waitpid(pid_, &status, WNOHANG) == 0 &&
		       detail::remaining_time_ns(until) != 0)
			(void)::poll(nullptr, 0, 1);
		if (::waitpid(pid_, &status, WNOHANG) == 0) {
			(void)::kill(pid_, SIGKILL);
			(void)::waitpid(pid_, &status, 0);
			ADD_FAILURE() << "owned lock helper watchdog";
		}
		EXPECT_TRUE(WIFEXITED(status));
		EXPECT_EQ(WEXITSTATUS(status), 0);
		pid_ = -1;
	}

       private:
	pid_t pid_{-1};
	int release_{-1};
	bool ready_{false};
};

class ControlDeadline : public testing::Test {
       protected:
	void TearDown() override {
		event_gate.release();
		lock_blocked = {};
		lock_acquired = {};
		sync_completed = {};
		checkpoint = {};
		futex_entering = {};
		reply_received = {};
		sync_count = 0;
		poll_interrupts = 0;
		deny_cancel_event = false;
		cancel_write_interrupts = 0;
	}
};

TEST_F(ControlDeadline, ContextFeatureAndEarlyCancellation) {
	EXPECT_EQ(control_context_feature_version(), kControlContextFeatureVersion);
	EXPECT_FALSE(ControlOperationContext::after(-1ns));
	CancellationSource source;
	EXPECT_TRUE(source.request_cancel());
	EXPECT_FALSE(source.request_cancel());
	const auto context = budget(1s, source.token());
	EXPECT_EQ(context.check().error().code, ErrorCode::kCancelled);
	ControlReceipt receipt;
	const auto options = options_for("control_early");
	auto created = ProducerType::create(options, TestPayloadV1Schema(), context, receipt);
	ASSERT_FALSE(created);
	EXPECT_EQ(created.error().code, ErrorCode::kCancelled);
	EXPECT_FALSE(receipt.committed);
	EXPECT_FALSE(receipt.has_pending_cleanup());
	EXPECT_EQ(ControlOperationContext(0).check().error().code, ErrorCode::kTimeout);
}

TEST_F(ControlDeadline, CreateDeadlineUsesIndependentHeldLock) {
	const auto options = options_for("control_create");
	HeldLock holder(options.name);
	ASSERT_TRUE(holder.ready());
	ControlReceipt receipt;
	const auto context = budget(80ms);
	const uint64_t start = detail::monotonic_now_ns();
	auto created = ProducerType::create(options, TestPayloadV1Schema(), context, receipt);
	ASSERT_FALSE(created);
	EXPECT_EQ(created.error().code, ErrorCode::kTimeout);
	EXPECT_LT(detail::monotonic_now_ns() - start, 2'000'000'000u);
	EXPECT_FALSE(receipt.committed);
	holder.release();
	auto healthy = ProducerType::create(options, TestPayloadV1Schema(), budget(), receipt);
	ASSERT_TRUE(healthy);
	ASSERT_TRUE(healthy.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, HeldLockTimeoutsCoverOpenEnableRemoveReconnect) {
	const auto options = options_for("control_entries");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	HeldLock holder(options.name);
	ASSERT_TRUE(holder.ready());
	ControlReceipt receipt;
	auto opened = ConsumerType::open(options, TestPayloadV1Schema(), budget(40ms), receipt);
	ASSERT_FALSE(opened);
	EXPECT_EQ(opened.error().code, ErrorCode::kTimeout);
	auto enabled = producer.value().enable_event_notifications(budget(40ms), receipt);
	ASSERT_FALSE(enabled);
	EXPECT_EQ(enabled.error().code, ErrorCode::kTimeout);
	auto removed = producer.value().remove_if_owner(budget(40ms), receipt);
	ASSERT_FALSE(removed);
	EXPECT_EQ(removed.error().code, ErrorCode::kTimeout);
	auto reopened = consumer.value().reconnect(budget(40ms), receipt);
	ASSERT_FALSE(reopened);
	EXPECT_EQ(reopened.error().code, ErrorCode::kTimeout);
	holder.release();
	ASSERT_TRUE(consumer.value().close(budget(), receipt));
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, CancellationDuringLockWaitAndRepeatedRequests) {
	const auto options = options_for("control_cancel");
	HeldLock holder(options.name);
	ASSERT_TRUE(holder.ready());
	CancellationSource source;
	std::promise<void> waiting;
	std::once_flag once;
	lock_blocked = [&] { std::call_once(once, [&] { waiting.set_value(); }); };
	auto observed = waiting.get_future();
	std::thread canceller([&] {
		if (observed.wait_for(2s) != std::future_status::ready) {
			ADD_FAILURE() << "no lock attempt";
			return;
		}
		EXPECT_TRUE(source.request_cancel());
		EXPECT_FALSE(source.request_cancel());
	});
	ControlReceipt receipt;
	auto result = ProducerType::create(options, TestPayloadV1Schema(),
	                                   budget(3s, source.token()), receipt);
	canceller.join();
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, ErrorCode::kCancelled);
	EXPECT_FALSE(receipt.committed);
}

TEST_F(ControlDeadline, AcquiredLockBoundaryStillRejectsCancellation) {
	const auto options = options_for("control_boundary");
	CancellationSource source;
	lock_acquired = [&] { (void)source.request_cancel(); };
	ControlReceipt receipt;
	auto result = ProducerType::create(options, TestPayloadV1Schema(),
	                                   budget(2s, source.token()), receipt);
	lock_acquired = {};
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, ErrorCode::kCancelled);
	EXPECT_FALSE(receipt.committed);
	auto healthy = ProducerType::create(options, TestPayloadV1Schema(), budget(), receipt);
	ASSERT_TRUE(healthy);
	ASSERT_TRUE(healthy.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, EarlyAndWaitingCancellationCoverEveryLockEntry) {
	const auto options = options_for("control_cancel_entries");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	HeldLock holder(options.name);
	ASSERT_TRUE(holder.ready());
	for (int operation = 0; operation < 4; ++operation) {
		for (bool early : {true, false}) {
			SCOPED_TRACE(std::to_string(operation) + (early ? " early" : " waiting"));
			CancellationSource source;
			std::promise<void> waiting;
			auto observed = waiting.get_future();
			std::once_flag once;
			lock_blocked = [&] { std::call_once(once, [&] { waiting.set_value(); }); };
			if (early) (void)source.request_cancel();
			std::thread canceller;
			if (!early)
				canceller = std::thread([&] {
					if (observed.wait_for(2s) != std::future_status::ready) {
						ADD_FAILURE() << "missing lock attempt";
						return;
					}
					(void)source.request_cancel();
					(void)source.request_cancel();
				});
			ControlReceipt receipt;
			const auto context = budget(3s, source.token());
			Error actual;
			if (operation == 0) {
				auto result = ConsumerType::open(options, TestPayloadV1Schema(),
				                                 context, receipt);
				EXPECT_FALSE(result);
				actual = result.error();
			}
			if (operation == 1) {
				auto result = producer.value().enable_event_notifications(context,
				                                                          receipt);
				EXPECT_FALSE(result);
				actual = result.error();
			}
			if (operation == 2) {
				auto result = producer.value().remove_if_owner(context, receipt);
				EXPECT_FALSE(result);
				actual = result.error();
			}
			if (operation == 3) {
				auto result = consumer.value().reconnect(context, receipt);
				EXPECT_FALSE(result);
				actual = result.error();
			}
			if (canceller.joinable()) canceller.join();
			lock_blocked = {};
			EXPECT_EQ(actual.code, ErrorCode::kCancelled);
			EXPECT_FALSE(receipt.committed);
		}
	}
	holder.release();
	ASSERT_TRUE(producer.value().remove_if_owner());
}

TEST_F(ControlDeadline, BrokerAndLockUseOneBudgetAcrossIndependentProcesses) {
	const auto options = options_for("control_total", Transport::kMemfdFdPass);
	HeldLock producer(options.name, true);
	ASSERT_TRUE(producer.ready());
	HeldLock holder(options.name);
	ASSERT_TRUE(holder.ready());
	const auto context = budget(120ms);
	bool broker_seen = false;
	reply_received = [&] {
		broker_seen = true;
		EXPECT_EQ(detail::control_context()->deadline_ns(), context.deadline_ns());
		const uint64_t phase_end = context.deadline_ns() - 50'000'000;
		while (detail::remaining_time_ns(phase_end) != 0) (void)::poll(nullptr, 0, 1);
	};
	bool lock_seen = false;
	lock_blocked = [&] {
		lock_seen = true;
		EXPECT_TRUE(broker_seen);
		EXPECT_EQ(detail::control_context()->deadline_ns(), context.deadline_ns());
		EXPECT_LT(detail::remaining_time_ns(context.deadline_ns()), 55'000'000u);
	};
	ControlReceipt receipt;
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema(), context, receipt);
	reply_received = {};
	lock_blocked = {};
	EXPECT_TRUE(broker_seen);
	EXPECT_TRUE(lock_seen);
	EXPECT_FALSE(consumer);
	EXPECT_EQ(consumer.error().code, ErrorCode::kTimeout);
	EXPECT_TRUE(receipt.deadline_elapsed);
	holder.release();
	producer.release();
}

TEST_F(ControlDeadline, NotificationRetryAndDisableShareCancellationContract) {
	const auto options = options_for("control_notify_budget");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	ControlReceipt receipt;
	auto notification = consumer.value().event_notification(budget(40ms), receipt);
	ASSERT_FALSE(notification);
	EXPECT_EQ(notification.error().code, ErrorCode::kTimeout);
	CancellationSource source;
	(void)source.request_cancel();
	auto disabled =
	        producer.value().disable_event_notifications(budget(1s, source.token()), receipt);
	ASSERT_FALSE(disabled);
	EXPECT_EQ(disabled.error().code, ErrorCode::kCancelled);
	ASSERT_TRUE(producer.value().enable_event_notifications(budget(), receipt));
	notification = consumer.value().event_notification(budget(), receipt);
	ASSERT_TRUE(notification);
	EXPECT_EQ(consumer.value().close(budget(), receipt).value(), ControlCloseStatus::kClosed);
	EXPECT_EQ(producer.value().disable_event_notifications(budget(), receipt).value(),
	          ControlCloseStatus::kClosed);
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, ReconnectClosesOldWatchAndNeverReentersReusedFd) {
	const auto options = options_for("control_reconnect_watch");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	ControlReceipt receipt;
	ASSERT_TRUE(producer.value().enable_event_notifications(budget(), receipt));
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	auto notification = consumer.value().event_notification(budget(), receipt);
	ASSERT_TRUE(notification);
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
	auto replacement = ProducerType::create(options, TestPayloadV1Schema(), budget(), receipt);
	ASSERT_TRUE(replacement);
	auto reopened = consumer.value().reconnect(budget(), receipt);
	ASSERT_TRUE(reopened);
	EXPECT_NE(reopened.value().old_generation, reopened.value().new_generation);
	EXPECT_EQ(notification.value().native_handle(), -1);
	EXPECT_FALSE(notification.value().drain());
	ASSERT_TRUE(replacement.value().enable_event_notifications(budget(), receipt));
	auto new_watch = consumer.value().event_notification(budget(), receipt);
	ASSERT_TRUE(new_watch);
	EXPECT_EQ(consumer.value().close(budget(), receipt).value(), ControlCloseStatus::kClosed);
	ASSERT_TRUE(replacement.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, EintrDoesNotResetAbsoluteBudget) {
	const auto options = options_for("control_eintr");
	HeldLock holder(options.name);
	ASSERT_TRUE(holder.ready());
	poll_interrupts = 20;
	ControlReceipt receipt;
	const auto context = budget(60ms);
	auto created = ProducerType::create(options, TestPayloadV1Schema(), context, receipt);
	ASSERT_FALSE(created);
	EXPECT_EQ(created.error().code, ErrorCode::kTimeout);
	EXPECT_EQ(poll_interrupts, 0);
	EXPECT_TRUE(receipt.deadline_elapsed);
}

TEST_F(ControlDeadline, CancellationFdIsStickyAndTokenOwnsItsLifetime) {
	CancellationToken token;
	int descriptor = -1;
	{
		CancellationSource source;
		token = source.token();
		descriptor = detail::CancellationAccess::native_handle(token);
		ASSERT_GE(descriptor, 0);
		EXPECT_NE(::fcntl(descriptor, F_GETFD) & FD_CLOEXEC, 0);
		std::atomic<int> first_requests{0};
		std::thread first([&] {
			if (source.request_cancel()) ++first_requests;
		});
		std::thread second([&] {
			if (source.request_cancel()) ++first_requests;
		});
		first.join();
		second.join();
		EXPECT_EQ(first_requests.load(), 1);
	}
	struct pollfd ready {
		descriptor, POLLIN, 0
	};
	EXPECT_EQ(::poll(&ready, 1, 0), 1);
	EXPECT_EQ(::poll(&ready, 1, 0), 1);
	EXPECT_TRUE(token.is_cancelled());
}

TEST_F(ControlDeadline, CancellationSignalRetriesEintrWithoutDroppingWake) {
	CancellationSource source;
	const int descriptor = detail::CancellationAccess::native_handle(source.token());
	ASSERT_GE(descriptor, 0);
	cancel_write_interrupts = 6;
	EXPECT_TRUE(source.request_cancel());
	EXPECT_EQ(cancel_write_interrupts, 0);
	struct pollfd ready {
		descriptor, POLLIN, 0
	};
	EXPECT_EQ(::poll(&ready, 1, 0), 1);
	EXPECT_FALSE(source.request_cancel());
}

TEST_F(ControlDeadline, CancellationStillWorksWhenWakeFdCannotBeCreated) {
	const auto options = options_for("control_no_cancel_fd");
	HeldLock holder(options.name);
	ASSERT_TRUE(holder.ready());
	deny_cancel_event = true;
	CancellationSource source;
	std::promise<void> waiting;
	auto observed = waiting.get_future();
	std::once_flag once;
	lock_blocked = [&] { std::call_once(once, [&] { waiting.set_value(); }); };
	std::thread canceller([&] {
		if (observed.wait_for(2s) != std::future_status::ready) {
			ADD_FAILURE();
			return;
		}
		(void)source.request_cancel();
	});
	ControlReceipt receipt;
	auto result = ProducerType::create(options, TestPayloadV1Schema(),
	                                   budget(3s, source.token()), receipt);
	canceller.join();
	EXPECT_FALSE(result);
	EXPECT_EQ(result.error().code, ErrorCode::kCancelled);
}

TEST_F(ControlDeadline, UncommittedObjectCleanupHasUniqueMovableReceipt) {
	const auto options = options_for("control_pending");
	CancellationSource source;
	sync_count = 0;
	sync_completed = [&](int ordinal) {
		if (ordinal == 2) (void)source.request_cancel();
	};
	ControlReceipt original;
	auto created = ProducerType::create(options, TestPayloadV1Schema(),
	                                    budget(2s, source.token()), original);
	sync_completed = {};
	EXPECT_FALSE(created);
	EXPECT_FALSE(original.committed);
	EXPECT_TRUE(original.has_pending_cleanup());
	ControlReceipt receipt(std::move(original));
	EXPECT_FALSE(original.has_pending_cleanup());
	EXPECT_EQ(receipt.cleanup_owner, ControlCleanupOwner::kReceipt);
	auto forbidden = ProducerType::create(options, TestPayloadV1Schema(), budget(), receipt);
	EXPECT_FALSE(forbidden);
	EXPECT_EQ(forbidden.error().code, ErrorCode::kRecoveryBlocked);
	const std::string name = detail::channel_shm_name(options.name);
	struct stat before {};
	EXPECT_EQ(::stat(("/dev/shm" + name).c_str(), &before), 0);
	const auto finalized = receipt.finalize(budget());
	EXPECT_TRUE(finalized) << static_cast<int>(finalized.error().code);
	if (!finalized) std::abort();
	EXPECT_FALSE(receipt.has_pending_cleanup());
	EXPECT_NE(::stat(("/dev/shm" + name).c_str(), &before), 0);
	auto healthy = ProducerType::create(options, TestPayloadV1Schema(), budget(), receipt);
	ASSERT_TRUE(healthy);
	ASSERT_TRUE(healthy.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, CommittedCreateAndOpenDoNotPretendCancellationRolledBack) {
	const auto options = options_for("control_committed");
	CancellationSource source;
	checkpoint = [&](const char* id) {
		if (std::strcmp(id, "CONTROL_CREATE_COMMITTED") == 0) (void)source.request_cancel();
	};
	ControlReceipt receipt;
	auto producer = ProducerType::create(options, TestPayloadV1Schema(),
	                                     budget(2s, source.token()), receipt);
	checkpoint = {};
	ASSERT_TRUE(producer);
	EXPECT_TRUE(receipt.committed);
	EXPECT_TRUE(receipt.cancellation_observed);
	EXPECT_TRUE(receipt.has_pending_cleanup());
	auto completed = receipt.finalize(budget());
	EXPECT_TRUE(completed);
	if (!completed) std::abort();
	CancellationSource consumer_source;
	checkpoint = [&](const char* id) {
		if (std::strcmp(id, "C15") == 0) (void)consumer_source.request_cancel();
	};
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema(),
	                                   budget(2s, consumer_source.token()), receipt);
	checkpoint = {};
	ASSERT_TRUE(consumer);
	EXPECT_TRUE(receipt.committed);
	EXPECT_TRUE(receipt.cancellation_observed);
	ASSERT_TRUE(consumer.value().close(budget(), receipt));
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, CommittedRemovalRetainsJournalContinuation) {
	const auto options = options_for("control_remove_pending");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	CancellationSource source;
	sync_count = 0;
	sync_completed = [&](int ordinal) {
		if (ordinal == 2) (void)source.request_cancel();
	};
	ControlReceipt receipt;
	auto removed = producer.value().remove_if_owner(budget(2s, source.token()), receipt);
	sync_completed = {};
	EXPECT_TRUE(removed);
	EXPECT_TRUE(receipt.committed);
	EXPECT_TRUE(receipt.has_pending_cleanup());
	auto finalized = receipt.finalize(budget());
	EXPECT_TRUE(finalized);
	if (!finalized) std::abort();
	EXPECT_TRUE(receipt.committed);
	EXPECT_FALSE(receipt.has_pending_cleanup());
}

TEST_F(ControlDeadline, DeadlineDuringTransactionRetainsCleanupInsteadOfUnknownOwner) {
	const auto options = options_for("control_late_sync");
	const auto context = budget(40ms);
	sync_count = 0;
	sync_completed = [&](int ordinal) {
		if (ordinal != 2) return;
		while (detail::remaining_time_ns(context.deadline_ns()) != 0)
			(void)::poll(nullptr, 0, 1);
	};
	ControlReceipt receipt;
	auto created = ProducerType::create(options, TestPayloadV1Schema(), context, receipt);
	sync_completed = {};
	EXPECT_FALSE(created);
	EXPECT_EQ(created.error().code, ErrorCode::kTimeout);
	EXPECT_FALSE(receipt.committed);
	EXPECT_TRUE(receipt.deadline_elapsed);
	EXPECT_TRUE(receipt.has_pending_cleanup());
	auto finished = receipt.finalize(budget());
	EXPECT_TRUE(finished);
	if (!finished) std::abort();
	auto healthy = ProducerType::create(options, TestPayloadV1Schema(), budget(), receipt);
	ASSERT_TRUE(healthy);
	ASSERT_TRUE(healthy.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, RemovalCancellationBeforeUnlinkPreservesInstanceAndCanResume) {
	const auto options = options_for("control_remove_precommit");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	CancellationSource source;
	sync_count = 0;
	sync_completed = [&](int ordinal) {
		if (ordinal == 1) (void)source.request_cancel();
	};
	ControlReceipt receipt;
	auto removed = producer.value().remove_if_owner(budget(2s, source.token()), receipt);
	sync_completed = {};
	EXPECT_FALSE(removed);
	EXPECT_EQ(removed.error().code, ErrorCode::kCancelled);
	EXPECT_FALSE(receipt.committed);
	EXPECT_TRUE(receipt.has_pending_cleanup());
	auto finished = receipt.finalize(budget());
	EXPECT_TRUE(finished);
	if (!finished) std::abort();
	EXPECT_TRUE(producer.value().publish(TestPayloadV1{0x5A000001u, 1, 0}));
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, DisablePendingDoesNotPretendBlockedServerJoined) {
	const auto options = options_for("control_disable_pending");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	ControlReceipt receipt;
	ASSERT_TRUE(producer.value().enable_event_notifications(budget(), receipt));
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	event_gate.arm();
	std::thread registration([&] {
		bypass_event_gate = true;
		auto result = consumer.value().event_notification(1s);
		EXPECT_FALSE(result);
	});
	EXPECT_TRUE(event_gate.wait());
	auto disabled = producer.value().disable_event_notifications(budget(40ms), receipt);
	EXPECT_TRUE(disabled);
	if (disabled) {
		EXPECT_EQ(disabled.value(), ControlCloseStatus::kPending);
	}
	EXPECT_EQ(receipt.cleanup_owner, ControlCleanupOwner::kHandle);
	EXPECT_EQ(receipt.cleanup_error.code, ErrorCode::kTimeout);
	event_gate.release();
	registration.join();
	disabled = producer.value().disable_event_notifications(budget(), receipt);
	ASSERT_TRUE(disabled);
	EXPECT_EQ(disabled.value(), ControlCloseStatus::kClosed);
	EXPECT_EQ(consumer.value().close(budget(), receipt).value(), ControlCloseStatus::kClosed);
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, LateRegistrationAfterCancellationCannotReturnWatch) {
	const auto options = options_for("control_late_watch");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	ControlReceipt receipt;
	ASSERT_TRUE(producer.value().enable_event_notifications(budget(), receipt));
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	CancellationSource source;
	ControlReceipt registration_receipt;
	event_gate.arm();
	std::thread registration([&] {
		bypass_event_gate = true;
		auto result = consumer.value().event_notification(budget(1s, source.token()),
		                                                  registration_receipt);
		EXPECT_FALSE(result);
		EXPECT_EQ(result.error().code, ErrorCode::kCancelled);
	});
	EXPECT_TRUE(event_gate.wait());
	(void)source.request_cancel();
	registration.join();
	EXPECT_FALSE(registration_receipt.committed);
	event_gate.release();
	auto current = consumer.value().event_notification(budget(), receipt);
	ASSERT_TRUE(current);
	EXPECT_EQ(consumer.value().close(budget(), receipt).value(), ControlCloseStatus::kClosed);
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, ControlledProducerStopRejectsOutstandingCommitAndResumesClose) {
	const auto options = options_for("control_write_close");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto loan = producer.value().loan();
	ASSERT_TRUE(loan);
	ControlReceipt receipt;
	auto closed = producer.value().close(budget(), receipt);
	ASSERT_TRUE(closed);
	EXPECT_EQ(closed.value(), ControlCloseStatus::kPending);
	auto committed = loan.value().commit();
	EXPECT_FALSE(committed);
	EXPECT_EQ(committed.error().code, ErrorCode::kClosed);
	closed = producer.value().finalize(budget(), receipt);
	ASSERT_TRUE(closed);
	EXPECT_EQ(closed.value(), ControlCloseStatus::kClosed);
	auto replacement = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(replacement);
	ASSERT_TRUE(replacement.value().remove_if_owner());
}

TEST_F(ControlDeadline, MemfdClosePendingKeepsOriginalHandle) {
	const auto options = options_for("control_memfd_close", Transport::kMemfdFdPass);
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	HeldLock holder(options.name);
	ASSERT_TRUE(holder.ready());
	ControlReceipt receipt;
	auto closed = producer.value().close(budget(60ms), receipt);
	ASSERT_TRUE(closed);
	EXPECT_EQ(closed.value(), ControlCloseStatus::kPending);
	EXPECT_EQ(receipt.cleanup_owner, ControlCleanupOwner::kHandle);
	EXPECT_FALSE(receipt.finalize(budget()));
	holder.release();
	closed = producer.value().finalize(budget(), receipt);
	ASSERT_TRUE(closed);
	EXPECT_EQ(closed.value(), ControlCloseStatus::kClosed);
	EXPECT_FALSE(producer.value().publish(TestPayloadV1{}));
	EXPECT_FALSE(producer.value().status());
	EXPECT_EQ(producer.value().finalize(budget(), receipt).value(),
	          ControlCloseStatus::kClosed);
}

TEST_F(ControlDeadline, LoanPendingAndConsumerOwnsNotificationCloseBeforeFdReuse) {
	const auto options = options_for("control_watch");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	ControlReceipt receipt;
	ASSERT_TRUE(producer.value().enable_event_notifications(budget(), receipt));
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema(), budget(), receipt);
	ASSERT_TRUE(consumer);
	auto notification = consumer.value().event_notification(budget(), receipt);
	ASSERT_TRUE(notification);
	ASSERT_TRUE(producer.value().publish(TestPayloadV1{0x5A000001u, 1, 0}));
	auto loan = consumer.value().try_loan_latest();
	ASSERT_TRUE(loan);
	const int old_fd = notification.value().native_handle();
	auto closed = consumer.value().close(budget(), receipt);
	ASSERT_TRUE(closed);
	EXPECT_EQ(closed.value(), ControlCloseStatus::kPending);
	loan.value().release();
	closed = consumer.value().finalize(budget(), receipt);
	ASSERT_TRUE(closed);
	EXPECT_EQ(closed.value(), ControlCloseStatus::kClosed);
	EXPECT_EQ(notification.value().native_handle(), -1);
	const int replacement = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
	ASSERT_GE(replacement, 0);
	ASSERT_EQ(::dup2(replacement, old_fd), old_fd);
	EXPECT_FALSE(notification.value().drain());
	EXPECT_NE(::fcntl(old_fd, F_GETFD), -1);
	if (replacement != old_fd) ::close(replacement);
	::close(old_fd);
	ASSERT_TRUE(producer.value().disable_event_notifications(budget(), receipt));
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, RequestStopWakesActualFutexWait) {
	const auto options = options_for("control_wait_stop");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	std::promise<void> entering;
	auto observed = entering.get_future();
	std::thread waiter([&] {
		std::once_flag once;
		futex_entering = [&] { std::call_once(once, [&] { entering.set_value(); }); };
		auto sample = consumer.value().wait_latest(5s);
		EXPECT_FALSE(sample);
		EXPECT_EQ(sample.error().code, ErrorCode::kClosed);
		futex_entering = {};
	});
	EXPECT_EQ(observed.wait_for(2s), std::future_status::ready);
	consumer.value().request_stop();
	waiter.join();
	ControlReceipt receipt;
	EXPECT_EQ(consumer.value().finalize(budget(), receipt).value(),
	          ControlCloseStatus::kClosed);
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
}

TEST_F(ControlDeadline, StopInterruptsActiveControlButAllowsExplicitOwnerCleanup) {
	const auto options = options_for("control_stop_cleanup");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	HeldLock holder(options.name);
	ASSERT_TRUE(holder.ready());
	std::promise<void> waiting;
	auto observed = waiting.get_future();
	std::once_flag once;
	lock_blocked = [&] { std::call_once(once, [&] { waiting.set_value(); }); };
	std::thread stopper([&] {
		if (observed.wait_for(2s) != std::future_status::ready) {
			ADD_FAILURE();
			return;
		}
		producer.value().request_stop();
	});
	ControlReceipt receipt;
	auto enabled = producer.value().enable_event_notifications(budget(), receipt);
	stopper.join();
	lock_blocked = {};
	EXPECT_FALSE(enabled);
	EXPECT_EQ(enabled.error().code, ErrorCode::kClosed);
	holder.release();
	ASSERT_TRUE(producer.value().remove_if_owner(budget(), receipt));
	EXPECT_EQ(producer.value().finalize(budget(), receipt).value(),
	          ControlCloseStatus::kClosed);
}

TEST_F(ControlDeadline, DestructionCannotUndoControlledStopForOutstandingLoan) {
	const auto options = options_for("control_stop_destruct");
	auto created = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(created);
	std::optional<ProducerType> producer(std::move(created.value()));
	auto loan = producer->loan();
	ASSERT_TRUE(loan);
	producer->request_stop();
	producer.reset();
	auto committed = loan.value().commit();
	EXPECT_FALSE(committed);
	EXPECT_EQ(committed.error().code, ErrorCode::kClosed);
	auto replacement = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(replacement);
	ASSERT_TRUE(replacement.value().remove_if_owner());
}

TEST_F(ControlDeadline, InheritedControlHandlesCannotStopFinalizeOrRemoveParent) {
	const auto options = options_for("control_fork_identity");
	auto producer = ProducerType::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto consumer = ConsumerType::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	const pid_t child = ::fork();
	ASSERT_GE(child, 0);
	if (child == 0) {
		ControlReceipt receipt;
		producer.value().request_stop();
		consumer.value().request_stop();
		auto removed = producer.value().remove_if_owner(budget(), receipt);
		auto producer_close = producer.value().finalize(budget(), receipt);
		auto consumer_close = consumer.value().finalize(budget(), receipt);
		::_exit(!removed && removed.error().code == ErrorCode::kStaleHandle &&
		                        !producer_close &&
		                        producer_close.error().code == ErrorCode::kStaleHandle &&
		                        !consumer_close &&
		                        consumer_close.error().code == ErrorCode::kStaleHandle
		                ? 0
		                : 1);
	}
	int status = 0;
	ASSERT_EQ(::waitpid(child, &status, 0), child);
	ASSERT_TRUE(WIFEXITED(status));
	EXPECT_EQ(WEXITSTATUS(status), 0);
	ASSERT_TRUE(producer.value().publish(TestPayloadV1{0x5A000001u, 1, 0}));
	EXPECT_TRUE(consumer.value().try_read_latest());
	ASSERT_TRUE(producer.value().remove_if_owner());
}
}  // namespace

extern "C" int __real_flock(int, int);
extern "C" int __wrap_flock(int descriptor, int operation) {
	const int result = __real_flock(descriptor, operation);
	if ((operation & LOCK_EX) != 0) {
		if (result == 0 && lock_acquired) lock_acquired();
		if (result < 0 && lock_blocked) lock_blocked();
	}
	return result;
}
extern "C" int __real_fdatasync(int);
extern "C" int __wrap_fdatasync(int descriptor) {
	const int result = __real_fdatasync(descriptor);
	if (sync_completed) sync_completed(++sync_count);
	return result;
}
extern "C" int __real_poll(struct pollfd*, nfds_t, int);
extern "C" int __wrap_poll(struct pollfd* descriptors, nfds_t count, int timeout) {
	if (poll_interrupts > 0) {
		--poll_interrupts;
		errno = EINTR;
		return -1;
	}
	return __real_poll(descriptors, count, timeout);
}
extern "C" ssize_t __real_recvmsg(int, struct msghdr*, int);
extern "C" int __real_eventfd(unsigned int, int);
extern "C" ssize_t __real_write(int, const void*, size_t);
extern "C" ssize_t __wrap_write(int descriptor, const void* buffer, size_t size) {
	if (cancel_write_interrupts > 0) {
		--cancel_write_interrupts;
		errno = EINTR;
		return -1;
	}
	return __real_write(descriptor, buffer, size);
}
extern "C" int __wrap_eventfd(unsigned int value, int flags) {
	if (deny_cancel_event) {
		errno = EMFILE;
		return -1;
	}
	if (!bypass_event_gate) event_gate.visit();
	return __real_eventfd(value, flags);
}
extern "C" ssize_t __wrap_recvmsg(int descriptor, struct msghdr* message, int flags) {
	const ssize_t result = __real_recvmsg(descriptor, message, flags);
	if (result > 0 && reply_received) reply_received();
	return result;
}
extern "C" void __wrap__ZN12edge_runtime6detail17failpoint_triggerEPKNS0_15FailpointRecordE(
        const detail::FailpointRecord* record) {
	if (checkpoint) checkpoint(record->id);
}
extern "C" int __real__ZN12edge_runtime6detail10futex_waitEPKvjPK8timespec(const uint32_t*,
                                                                           uint32_t,
                                                                           const struct timespec*);
extern "C" int __wrap__ZN12edge_runtime6detail10futex_waitEPKvjPK8timespec(
        const uint32_t* address, uint32_t expected, const struct timespec* timeout) {
	if (futex_entering) futex_entering();
	return __real__ZN12edge_runtime6detail10futex_waitEPKvjPK8timespec(address, expected,
	                                                                   timeout);
}
