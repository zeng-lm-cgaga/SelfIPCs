#include "self_ipcs/async/consumer.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <mutex>
#include <new>
#include <optional>

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>

namespace edge_runtime::async::detail {
namespace {

constexpr const char* kOperation = "async::Consumer::async_wait_latest";

Error canceled(const char* context) {
	return make_errno_error(ErrorCode::kSystemError, ECANCELED, kOperation, context);
}

Error timed_out() {
	return make_errno_error(ErrorCode::kTimeout, ETIMEDOUT, kOperation, "absolute deadline");
}

Error asio_error(const boost::system::error_code& error, const char* context) {
	return make_errno_error(ErrorCode::kSystemError, error.value(), kOperation, context);
}

}

struct ConsumerState::Impl : std::enable_shared_from_this<Impl> {
	using Completion = std::function<void(Result<void>)>;

	struct Operation {
		std::shared_ptr<Impl> owner;
		uint64_t generation = 0;
		Clock::time_point deadline;
		Read read;
		Status status;
		Completion completion;
		std::optional<Result<void>> winner;
		unsigned pending = 0;
		bool fd_pending = false;
		bool timer_pending = false;
	};

	Impl(boost::asio::any_io_executor executor, std::shared_ptr<void> owner,
	     EventNotification registered)
	    : consumer_owner(std::move(owner)), notification(std::move(registered)),
	      strand(boost::asio::make_strand(std::move(executor))), descriptor(strand), timer(strand) {}

	bool current(const std::shared_ptr<Operation>& operation) const {
		return active.lock() == operation && operation->generation == active_generation;
	}

	void close_resources() {
		boost::system::error_code ignored;
		descriptor.close(ignored);
		notification.reset();
		consumer_owner.reset();
	}

	void finish(const std::shared_ptr<Operation>& operation) {
		if (!current(operation) || !operation->winner || operation->pending != 0) return;
		auto completion = std::move(operation->completion);
		auto result = std::move(operation->winner.value());
		active.reset();
		{
			std::lock_guard<std::mutex> guard(admission_mutex);
			if (reserved.lock() == operation) reserved.reset();
		}
		if (stopped) close_resources();
		completion(std::move(result));
	}

	void choose(const std::shared_ptr<Operation>& operation, Result<void> result) {
		if (!current(operation) || operation->winner) return;
		operation->winner.emplace(std::move(result));
		boost::system::error_code ignored;
		if (operation->fd_pending) descriptor.cancel(ignored);
		if (operation->timer_pending) timer.cancel(ignored);
		finish(operation);
	}

	std::optional<Result<void>> probe(const std::shared_ptr<Operation>& operation) {
		auto drained = notification->drain();
		auto latest = operation->read();
		if (latest) return Result<void>{};
		auto status = operation->status();
		if (latest.error().code != ErrorCode::kNoNewSample &&
		    latest.error().code != ErrorCode::kReadContention) {
			return latest.error();
		}
		if (!drained) return drained.error();
		if (!status) return status.error();
		if (!status.value().ready) {
			return make_error(ErrorCode::kRecoveryBlocked, kOperation, "channel not ready");
		}
		if (!status.value().producer_alive) {
			return make_error(ErrorCode::kProducerOffline, kOperation, "producer offline");
		}
		return std::nullopt;
	}

	void on_descriptor(const std::shared_ptr<Operation>& operation,
	                   const boost::system::error_code& error) {
		operation->fd_pending = false;
		--operation->pending;
		if (!current(operation)) return;
		if (!operation->winner) {
			if (error) {
				choose(operation, asio_error(error, "descriptor wait"));
			} else if (Clock::now() >= operation->deadline) {
				choose(operation, timed_out());
			} else if (auto result = probe(operation)) {
				choose(operation, std::move(result.value()));
			} else {
				arm_descriptor(operation);
			}
		}
		finish(operation);
	}

	void on_timer(const std::shared_ptr<Operation>& operation,
	              const boost::system::error_code& error) {
		operation->timer_pending = false;
		--operation->pending;
		if (!current(operation)) return;
		if (!operation->winner) {
			choose(operation, error ? asio_error(error, "deadline wait") : timed_out());
		}
		finish(operation);
	}

	void arm_descriptor(const std::shared_ptr<Operation>& operation) {
		if (!current(operation) || operation->winner) return;
		if (Clock::now() >= operation->deadline) {
			choose(operation, timed_out());
			return;
		}
		operation->fd_pending = true;
		++operation->pending;
		try {
			descriptor.async_wait(
			        boost::asio::posix::stream_descriptor::wait_read,
			        boost::asio::bind_executor(
			                strand, [self = shared_from_this(), operation](
			                                const boost::system::error_code& error) {
				                self->on_descriptor(operation, error);
			                }));
		} catch (const boost::system::system_error& error) {
			operation->fd_pending = false;
			--operation->pending;
			choose(operation, asio_error(error.code(), "arm descriptor"));
		} catch (const std::bad_alloc&) {
			operation->fd_pending = false;
			--operation->pending;
			choose(operation, make_errno_error(ErrorCode::kSystemError, ENOMEM, kOperation,
			                                   "arm descriptor"));
		}
	}

	void arm_timer(const std::shared_ptr<Operation>& operation) {
		boost::system::error_code error;
		timer.expires_at(operation->deadline, error);
		if (error) {
			choose(operation, asio_error(error, "set deadline"));
			return;
		}
		operation->timer_pending = true;
		++operation->pending;
		try {
			timer.async_wait(boost::asio::bind_executor(
			        strand, [self = shared_from_this(), operation](
			                        const boost::system::error_code& completion_error) {
				        self->on_timer(operation, completion_error);
			        }));
		} catch (const boost::system::system_error& exception) {
			operation->timer_pending = false;
			--operation->pending;
			choose(operation, asio_error(exception.code(), "arm deadline"));
		} catch (const std::bad_alloc&) {
			operation->timer_pending = false;
			--operation->pending;
			choose(operation, make_errno_error(ErrorCode::kSystemError, ENOMEM, kOperation,
			                                   "arm deadline"));
		}
	}

	void begin(Clock::time_point deadline, Read read, Status status, Completion completion) {
		auto operation = std::make_shared<Operation>();
		operation->owner = shared_from_this();
		operation->deadline = deadline;
		operation->read = std::move(read);
		operation->status = std::move(status);
		operation->completion = std::move(completion);
		std::optional<Error> rejected;
		{
			std::lock_guard<std::mutex> guard(admission_mutex);
			if (stop_requested) {
				rejected = canceled("stop");
			} else if (!reserved.expired()) {
				rejected = make_error(ErrorCode::kConcurrentHandleUse, kOperation,
				                      "operation already active or queued");
			} else {
				operation->generation = ++next_generation;
				reserved = operation;
				try {
					boost::asio::post(strand, [operation] {
						operation->owner->start(operation);
					});
				} catch (...) {
					reserved.reset();
					throw;
				}
			}
		}
		if (rejected) operation->completion(rejected.value());
	}

	void start(const std::shared_ptr<Operation>& operation) {
		active = operation;
		active_generation = operation->generation;
		if (stopped) {
			choose(operation, canceled("stop"));
		} else if (Clock::now() >= operation->deadline) {
			choose(operation, timed_out());
		} else if (auto result = probe(operation)) {
			choose(operation, std::move(result.value()));
		} else {
			arm_timer(operation);
			arm_descriptor(operation);
		}
	}

	void post_cancel() {
		std::lock_guard<std::mutex> guard(admission_mutex);
		if (auto operation = reserved.lock()) {
			boost::asio::post(strand, [operation] {
				if (operation->owner->current(operation)) {
					operation->owner->choose(operation, canceled("cancel"));
				}
			});
		}
	}

	void post_stop() {
		std::lock_guard<std::mutex> guard(admission_mutex);
		stop_requested = true;
		boost::asio::post(strand, [self = shared_from_this()] { self->request_stop(); });
	}

	void request_stop() {
		stopped = true;
		if (auto operation = active.lock()) {
			choose(operation, canceled("stop"));
		} else {
			close_resources();
		}
	}

	std::shared_ptr<void> consumer_owner;
	std::optional<EventNotification> notification;
	boost::asio::strand<boost::asio::any_io_executor> strand;
	boost::asio::posix::stream_descriptor descriptor;
	boost::asio::steady_timer timer;
	std::weak_ptr<Operation> active;
	uint64_t active_generation = 0;
	bool stopped = false;
	std::mutex admission_mutex;
	std::weak_ptr<Operation> reserved;
	uint64_t next_generation = 0;
	bool stop_requested = false;
};

ConsumerState::ConsumerState(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

ConsumerState::~ConsumerState() = default;

Result<std::shared_ptr<ConsumerState>> ConsumerState::create(
        boost::asio::any_io_executor executor, std::shared_ptr<void> consumer_owner,
        EventNotification notification) {
	if (!executor || !consumer_owner || notification.native_handle() < 0) {
		return make_error(ErrorCode::kInvalidOptions, "async::Consumer::create",
		                  "invalid executor or notification owner");
	}
	try {
		auto impl = std::make_shared<Impl>(std::move(executor), std::move(consumer_owner),
		                                   std::move(notification));
		const int duplicate = ::fcntl(impl->notification->native_handle(), F_DUPFD_CLOEXEC, 0);
		if (duplicate < 0) {
			return make_errno_error(ErrorCode::kSystemError, errno, "async::Consumer::create",
			                        "duplicate notification epoll");
		}
		boost::system::error_code error;
		impl->descriptor.assign(duplicate, error);
		if (error) {
			(void)::close(duplicate);
			return make_errno_error(ErrorCode::kSystemError, error.value(),
			                        "async::Consumer::create", "assign notification epoll");
		}
		return std::shared_ptr<ConsumerState>(new ConsumerState(std::move(impl)));
	} catch (const boost::system::system_error& error) {
		return make_errno_error(ErrorCode::kSystemError, error.code().value(),
		                        "async::Consumer::create", "initialize executor resources");
	} catch (const std::bad_alloc&) {
		return make_errno_error(ErrorCode::kSystemError, ENOMEM, "async::Consumer::create",
		                        "allocation failed");
	}
}

boost::asio::awaitable<Result<void>> ConsumerState::wait_latest(
        Clock::time_point absolute_deadline, Read read, Status status) const {
	return boost::asio::async_initiate<decltype(boost::asio::use_awaitable), void(Result<void>)>(
	        [impl = impl_, absolute_deadline, read = std::move(read),
	         status = std::move(status)](auto handler) mutable {
		        auto completion_executor = boost::asio::get_associated_executor(handler);
		        auto completion = std::make_shared<decltype(handler)>(std::move(handler));
		        auto finish = [completion_executor, completion](Result<void> result) mutable {
			        boost::asio::post(
			                completion_executor,
			                [completion, result = std::move(result)]() mutable {
				                (*completion)(std::move(result));
			                });
		        };
		        impl->begin(absolute_deadline, std::move(read), std::move(status),
		                    std::move(finish));
	        },
	        boost::asio::use_awaitable);
}

void ConsumerState::cancel() const {
	impl_->post_cancel();
}

void ConsumerState::stop() const {
	impl_->post_stop();
}

}
