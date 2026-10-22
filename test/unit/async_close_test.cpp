#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <chrono>
#include <optional>

#include "edge_runtime/channel/producer.hpp"
#include "self_ipcs/async/consumer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Async = edge_runtime::async::Consumer<TestPayloadV1>;
using Pending = boost::asio::awaitable<edge_runtime::Result<edge_runtime::Sample<TestPayloadV1>>>;

boost::asio::awaitable<bool> close_reopen(Async* consumer, edge_runtime::ChannelOptions options) {
	auto closed = co_await consumer->async_close();
	if (!closed) co_return false;
	auto again = co_await consumer->async_close();
	if (!again) co_return false;
	auto reopened = edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	co_return static_cast<bool>(reopened);
}

boost::asio::awaitable<bool> finish_read(Pending pending, int* completed) {
	auto result = co_await std::move(pending);
	++*completed;
	co_return !result && result.error().errno_value == ECANCELED;
}

boost::asio::awaitable<bool> finish_close(Async* consumer, const int* read_completed) {
	auto closed = co_await consumer->async_close();
	co_return closed&&* read_completed == 1;
}

class AsyncClose : public ::testing::Test {
       protected:
	void SetUp() override {
		options.name = edge_test::unique_channel_name("async_close");
		auto created = edge_runtime::Producer<TestPayloadV1>::create(options,
		                                                             TestPayloadV1Schema());
		ASSERT_TRUE(created);
		producer.emplace(std::move(created.value()));
		ASSERT_TRUE(producer->enable_event_notifications());
		auto opened =
		        edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
		ASSERT_TRUE(opened);
		auto adapted = Async::create(loop.get_executor(), std::move(opened.value()));
		ASSERT_TRUE(adapted);
		consumer.emplace(std::move(adapted.value()));
	}
	void TearDown() override {
		if (consumer) consumer->stop();
		loop.restart();
		loop.run();
		consumer.reset();
		if (producer) {
			EXPECT_TRUE(producer->remove_if_owner());
		}
	}
	auto completion(int* count) {
		return [count](std::exception_ptr error, bool success) {
			EXPECT_FALSE(error);
			EXPECT_TRUE(success);
			++*count;
		};
	}
	edge_runtime::ChannelOptions options;
	boost::asio::io_context loop;
	std::optional<edge_runtime::Producer<TestPayloadV1>> producer;
	std::optional<Async> consumer;
};

TEST_F(AsyncClose, IdleRepeatedCloseAllowsReopenOnRunningLoop) {
	int closed = 0;
	boost::asio::co_spawn(loop, close_reopen(&*consumer, options), completion(&closed));
	loop.run();
	EXPECT_EQ(closed, 1);
}

TEST_F(AsyncClose, ActiveReadAndMultipleClosersDrainBeforeCompletion) {
	int read_completed = 0;
	int tasks = 0;
	boost::asio::co_spawn(
	        loop, finish_read(consumer->async_wait_latest(Clock::now() + 1s), &read_completed),
	        completion(&tasks));
	loop.poll();
	loop.restart();
	for (int index = 0; index < 3; ++index)
		boost::asio::co_spawn(loop, finish_close(&*consumer, &read_completed),
		                      completion(&tasks));
	loop.run();
	EXPECT_EQ(read_completed, 1);
	EXPECT_EQ(tasks, 4);
	auto reopened = edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	EXPECT_TRUE(reopened);
}

TEST_F(AsyncClose, QueuedReadAndImmediateStopCompleteOnce) {
	int read_completed = 0;
	int tasks = 0;
	boost::asio::co_spawn(
	        loop, finish_read(consumer->async_wait_latest(Clock::now() + 1s), &read_completed),
	        completion(&tasks));
	consumer->close();
	boost::asio::co_spawn(loop, close_reopen(&*consumer, options), completion(&tasks));
	loop.run();
	EXPECT_EQ(read_completed, 1);
	EXPECT_EQ(tasks, 2);
}

TEST_F(AsyncClose, LazyReadSurvivesMoveAndWrapperDestruction) {
	int read_completed = 0;
	int tasks = 0;
	auto pending = consumer->async_wait_latest(Clock::now() + 1s);
	{
		Async moved = std::move(*consumer);
		consumer.reset();
	}
	boost::asio::co_spawn(loop, finish_read(std::move(pending), &read_completed),
	                      completion(&tasks));
	loop.run();
	EXPECT_EQ(tasks, 1);
	auto reopened = edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	EXPECT_TRUE(reopened);
}
}
