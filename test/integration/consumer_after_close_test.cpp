#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <iostream>
#include <string>
#include <tuple>

#include "self_ipcs/channel/producer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {
using namespace std::chrono_literals;
using namespace self_ipcs;

class ConsumerAfterClose : public testing::TestWithParam<std::tuple<Transport, bool, bool>> {
       protected:
	void check(const char* entry) {
		const auto [transport, legacy, finalize] = GetParam();
		ChannelOptions options;
		options.name = edge_test::unique_channel_name("consumer_after_close");
		options.transport = transport;
		options.heartbeat_interval = 1ms;
		std::cout << "fixture name=" << options.name << std::endl;
		auto producer = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
		ASSERT_TRUE(producer);
		const char* helper = legacy ? CONTROL_AFTER_CLOSE_LEGACY_PEER
		                            : CONTROL_AFTER_CLOSE_CANONICAL_PEER;
		const char* transport_text = transport == Transport::kPosixShm ? "shm" : "memfd";
		const char* close_text = finalize ? "finalize" : "close";
		const pid_t child = ::fork();
		if (child == 0) {
			::execl(helper, helper, options.name.c_str(), transport_text, close_text,
			        entry, nullptr);
			::_exit(127);
		}
		int status = 0;
		pid_t waited = -1;
		if (child > 0) {
			const auto deadline = std::chrono::steady_clock::now() + 3s;
			do {
				waited = ::waitpid(child, &status, WNOHANG);
				if (waited < 0 && errno == EINTR) waited = 0;
				if (waited != 0) break;
				(void)::poll(nullptr, 0, 1);
			} while (std::chrono::steady_clock::now() < deadline);
			if (waited == 0) {
				(void)::kill(child, SIGKILL);
				(void)::waitpid(child, &status, 0);
			}
		}
		ControlReceipt receipt;
		auto context = ControlOperationContext::after(2s);
		ASSERT_TRUE(context);
		EXPECT_TRUE(producer.value().remove_if_owner(context.value(), receipt));
		auto closed = producer.value().close(context.value(), receipt);
		ASSERT_TRUE(closed);
		EXPECT_EQ(closed.value(), ControlCloseStatus::kClosed);
		auto rejected = [](const auto& result) {
			return !result && result.error().code == ErrorCode::kClosed;
		};
		EXPECT_TRUE(rejected(producer.value().publish(TestPayloadV1{})));
		EXPECT_TRUE(rejected(producer.value().loan()));
		EXPECT_TRUE(rejected(producer.value().status()));
		EXPECT_TRUE(rejected(producer.value().heartbeat()));
		EXPECT_TRUE(rejected(producer.value().enable_event_notifications()));
		EXPECT_TRUE(rejected(producer.value().remove_if_owner()));
		EXPECT_TRUE(producer.value().disable_event_notifications());
		EXPECT_EQ(producer.value().finalize(context.value(), receipt).value(),
		          ControlCloseStatus::kClosed);
		ASSERT_GT(child, 0);
		ASSERT_EQ(waited, child) << "child did not finish before deadline";
		ASSERT_TRUE(WIFEXITED(status)) << "child signal=" << WTERMSIG(status);
		EXPECT_EQ(WEXITSTATUS(status), 0);
	}
};

TEST_P(ConsumerAfterClose, LegacyDefaultNotification) { check("default"); }
TEST_P(ConsumerAfterClose, LegacyBackendNotification) { check("backend"); }
TEST_P(ConsumerAfterClose, ControlledNotification) { check("controlled"); }

std::string case_name(const testing::TestParamInfo<ConsumerAfterClose::ParamType>& parameter) {
	const auto [transport, legacy, finalize] = parameter.param;
	return std::string(transport == Transport::kPosixShm ? "Shm" : "Memfd") +
	       (legacy ? "Legacy" : "Canonical") + (finalize ? "Finalize" : "Close");
}

INSTANTIATE_TEST_SUITE_P(Lifecycle, ConsumerAfterClose,
                         testing::Combine(testing::Values(Transport::kPosixShm,
                                                          Transport::kMemfdFdPass),
                                          testing::Bool(), testing::Bool()),
                         case_name);
}  // namespace
