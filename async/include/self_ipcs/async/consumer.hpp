#ifndef SELF_IPCS_ASYNC_CONSUMER_HPP
#define SELF_IPCS_ASYNC_CONSUMER_HPP

#include <cerrno>
#include <chrono>
#include <functional>
#include <memory>
#include <new>
#include <optional>
#include <utility>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include "self_ipcs/channel/consumer.hpp"

namespace edge_runtime::async {

namespace detail {

class ConsumerState {
       public:
	using Clock = std::chrono::steady_clock;
	using Read = std::function<Result<void>()>;
	using Status = std::function<Result<ChannelStatus>()>;
	using Close = std::function<void()>;

	static Result<std::shared_ptr<ConsumerState>> create(
	        boost::asio::any_io_executor executor, std::shared_ptr<void> consumer_owner,
	        EventNotification notification, Close release_consumer);

	~ConsumerState();
	ConsumerState(const ConsumerState&) = delete;
	ConsumerState& operator=(const ConsumerState&) = delete;

	boost::asio::awaitable<Result<void>> wait_latest(Clock::time_point absolute_deadline,
	                                                Read read, Status status) const;
	void cancel() const;
	void stop() const;
	static boost::asio::awaitable<Result<void>> async_close_owned(
	        std::shared_ptr<ConsumerState> state);

       private:
	struct Impl;
	explicit ConsumerState(std::shared_ptr<Impl> impl);
	std::shared_ptr<Impl> impl_;
};

}

template <typename T>
class Consumer {
       public:
	using value_type = T;
	using Clock = std::chrono::steady_clock;

	static Result<Consumer> create(
	        boost::asio::any_io_executor executor, edge_runtime::Consumer<T>&& consumer,
	        std::chrono::milliseconds registration_timeout = std::chrono::milliseconds{1000}) {
		return create(std::move(executor), std::move(consumer),
		              NotificationBackend::kEventFd, registration_timeout);
	}

	static Result<Consumer> create(
	        boost::asio::any_io_executor executor, edge_runtime::Consumer<T>&& consumer,
	        NotificationBackend backend,
	        std::chrono::milliseconds registration_timeout = std::chrono::milliseconds{1000}) {
		if (!executor) {
			return make_error(ErrorCode::kInvalidOptions, "async::Consumer::create",
			                  "empty executor");
		}
		auto registered = consumer.event_notification(backend, registration_timeout);
		if (!registered) return registered.error();
		try {
			auto source = std::make_shared<Source>(std::move(consumer));
			auto state = detail::ConsumerState::create(
			        std::move(executor), source, std::move(registered.value()),
			        [source] { source->consumer.reset(); });
			if (!state) return state.error();
			return Consumer(std::move(source), std::move(state.value()));
		} catch (const std::bad_alloc&) {
			return make_errno_error(ErrorCode::kSystemError, ENOMEM,
			                        "async::Consumer::create", "allocation failed");
		}
	}

	Consumer(const Consumer&) = delete;
	Consumer& operator=(const Consumer&) = delete;
	Consumer(Consumer&&) noexcept = default;
	Consumer& operator=(Consumer&& other) noexcept {
		if (this != &other) {
			stop_on_destruction();
			source_ = std::move(other.source_);
			state_ = std::move(other.state_);
		}
		return *this;
	}
	~Consumer() { stop_on_destruction(); }

	boost::asio::awaitable<Result<Sample<T>>> async_wait_latest(
	        Clock::time_point absolute_deadline) const {
		return wait_owned(state_, source_, absolute_deadline);
	}

	void cancel() const {
		if (state_) state_->cancel();
	}
	void stop() const {
		if (state_) state_->stop();
	}
	void close() const { stop(); }
	boost::asio::awaitable<Result<void>> async_close() const {
		return detail::ConsumerState::async_close_owned(state_);
	}

       private:
	struct Source {
		explicit Source(edge_runtime::Consumer<T>&& input) : consumer(std::move(input)) {}
		std::optional<edge_runtime::Consumer<T>> consumer;
	};

	Consumer(std::shared_ptr<Source> source, std::shared_ptr<detail::ConsumerState> state)
	    : source_(std::move(source)), state_(std::move(state)) {}

	static boost::asio::awaitable<Result<Sample<T>>> wait_owned(
	        std::shared_ptr<detail::ConsumerState> state, std::shared_ptr<Source> source,
	        Clock::time_point absolute_deadline) {
		if (!state || !source) {
			co_return make_error(ErrorCode::kStaleHandle, "async::Consumer::async_wait_latest",
			                     "moved consumer");
		}
		auto sample = std::make_shared<std::optional<Sample<T>>>();
		auto read = [source, sample]() -> Result<void> {
			if (!source->consumer) {
				return make_error(ErrorCode::kStaleHandle, "async::Consumer::read", "closed source");
			}
			auto latest = source->consumer->try_read_latest();
			if (!latest) return latest.error();
			sample->emplace(std::move(latest.value()));
			return {};
		};
		auto status = [source]() -> Result<ChannelStatus> {
			if (!source->consumer) {
				return make_error(ErrorCode::kStaleHandle, "async::Consumer::status", "closed source");
			}
			return source->consumer->status();
		};
		auto result = co_await state->wait_latest(absolute_deadline, std::move(read),
		                                        std::move(status));
		if (!result) co_return result.error();
		co_return std::move(sample->value());
	}

	void stop_on_destruction() const noexcept {
		if (state_) {
			try {
				state_->stop();
			} catch (...) {
			}
		}
	}

	std::shared_ptr<Source> source_;
	std::shared_ptr<detail::ConsumerState> state_;
};

}

#endif
