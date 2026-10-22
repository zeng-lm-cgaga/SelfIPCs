#include "async_consume_demo/api.hpp"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>

namespace async_consume_demo {

using Clock = std::chrono::steady_clock;
using AsyncConsumer = ipc::async::Consumer<Message>;
using PendingRead = boost::asio::awaitable<ipc::Result<ipc::Sample<Message>>>;
using namespace std::chrono_literals;

bool api_error(const char* operation, const ipc::Error& error) {
	std::fprintf(stderr, "%s: %s errno=%d\n", operation, ipc::to_string(error.code),
	             error.errno_value);
	return false;
}

class Descriptor {
       public:
	explicit Descriptor(int descriptor) : descriptor_(descriptor) {}
	~Descriptor() { close(); }
	Descriptor(const Descriptor&) = delete;
	Descriptor& operator=(const Descriptor&) = delete;
	int get() const { return descriptor_; }
	void close() {
		if (descriptor_ >= 0) (void)::close(std::exchange(descriptor_, -1));
	}
       private:
	int descriptor_;
};

bool transfer_token(int descriptor, char token, bool send) {
	const auto deadline = Clock::now() + 3s;
	while (Clock::now() < deadline) {
		pollfd event{descriptor, static_cast<short>(send ? POLLOUT : POLLIN), 0};
		const int ready = ::poll(&event, 1, 20);
		if (ready < 0 && errno == EINTR) continue;
		if (ready < 0) return false;
		if (ready == 0) continue;
		char received = token;
		const ssize_t count = send ? ::send(descriptor, &token, 1, MSG_NOSIGNAL)
		                           : ::recv(descriptor, &received, 1, 0);
		if (count == 1) return received == token;
		if (count < 0 && (errno == EINTR || errno == EAGAIN)) continue;
		return false;
	}
	return false;
}

class Child {
       public:
	explicit Child(pid_t pid) : pid_(pid) {}
	~Child() {
		if (pid_ > 0) {
			(void)::kill(pid_, SIGKILL);
			(void)wait();
		}
	}
	Child(const Child&) = delete;
	Child& operator=(const Child&) = delete;
	bool wait() {
		const auto deadline = Clock::now() + 3s;
		while (Clock::now() < deadline) {
			int status = 0;
			const pid_t result = ::waitpid(pid_, &status, WNOHANG);
			if (result == pid_) {
				pid_ = -1;
				return WIFEXITED(status) && WEXITSTATUS(status) == 0;
			}
			if (result < 0 && errno != EINTR) return false;
			(void)::poll(nullptr, 0, 5);
		}
		return false;
	}
       private:
	pid_t pid_;
};

class OwnedProducers {
       public:
	~OwnedProducers() {
		for (auto& producer : producers) {
			(void)producer.disable_event_notifications();
			(void)producer.remove_if_owner();
		}
	}
	std::vector<ipc::Producer<Message>> producers;
};

bool produce(const std::string& prefix, int descriptor, ipc::NotificationBackend backend) {
	OwnedProducers owned;
	for (int index = 0; index < 2; ++index) {
		ipc::ChannelOptions options;
		options.name = prefix + "_" + std::to_string(index);
		auto created = ipc::Producer<Message>::create(options, message_schema());
		if (!created) return api_error("producer create", created.error());
		owned.producers.push_back(std::move(created.value()));
		auto enabled = owned.producers.back().enable_event_notifications(backend);
		if (!enabled) return api_error("enable notifications", enabled.error());
	}
	if (!transfer_token(descriptor, 'R', true) || !transfer_token(descriptor, 'G', false)) {
		return false;
	}
	for (uint32_t value = 1; value <= 10; ++value) {
		for (auto& producer : owned.producers) {
			auto published = producer.publish(Message{value});
			if (!published) return api_error("producer publish", published.error());
		}
		(void)::poll(nullptr, 0, 5);
	}
	if (!transfer_token(descriptor, 'Q', false)) return false;
	for (auto& producer : owned.producers) {
		if (producer.event_notification_error() != 0) return false;
	}
	return true;
}

void require(bool condition, const char* message) {
	if (!condition) throw std::runtime_error(message);
}

boost::asio::awaitable<void> consume(std::shared_ptr<AsyncConsumer> consumer, int index,
                                     PendingRead first, std::string channel_name) {
	auto first_sample = co_await std::move(first);
	require(static_cast<bool>(first_sample), "lazy read after wrapper move failed");
	uint64_t last_sequence = first_sample.value().sequence;
	uint32_t last_value = first_sample.value().value.value;
	const auto data_deadline = Clock::now() + 2s;
	while (last_value < 10) {
		auto sample = co_await consumer->async_wait_latest(data_deadline);
		require(static_cast<bool>(sample), "async sample read failed");
		require(sample.value().sequence > last_sequence, "sequence did not advance");
		last_sequence = sample.value().sequence;
		last_value = sample.value().value.value;
	}
	auto expired = co_await consumer->async_wait_latest(Clock::now() + 100ms);
	require(!expired && expired.error().code == ipc::ErrorCode::kTimeout, "timeout expected");
	auto executor = co_await boost::asio::this_coro::executor;
	boost::asio::steady_timer cancellation(executor);
	cancellation.expires_after(5ms);
	cancellation.async_wait([consumer](const boost::system::error_code& error) {
		if (!error) consumer->cancel();
	});
	auto cancelled = co_await consumer->async_wait_latest(Clock::now() + 1s);
	require(!cancelled && cancelled.error().errno_value == ECANCELED, "cancel expected");
	boost::asio::steady_timer stopping(executor);
	stopping.expires_after(5ms);
	stopping.async_wait([consumer](const boost::system::error_code& error) {
		if (!error) consumer->close();
	});
	auto stopped = co_await consumer->async_wait_latest(Clock::now() + 1s);
	require(!stopped && stopped.error().errno_value == ECANCELED, "close expected");
	auto sticky = co_await consumer->async_wait_latest(Clock::now() + 1s);
	require(!sticky && sticky.error().errno_value == ECANCELED, "sticky stop expected");
	auto closed = co_await consumer->async_close();
	require(static_cast<bool>(closed), "close completion expected");
	auto closed_again = co_await consumer->async_close();
	require(static_cast<bool>(closed_again), "repeated close completion expected");
	ipc::ChannelOptions reopened_options;
	reopened_options.name = std::move(channel_name);
	auto reopened = ipc::Consumer<Message>::open(reopened_options, message_schema());
	require(static_cast<bool>(reopened), "reopen on running event loop expected");
	std::printf("channel=%d final_sequence=%llu value=%u timeout/cancel/rearm/close=OK\n",
	            index, static_cast<unsigned long long>(last_sequence), last_value);
}

bool run(ipc::NotificationBackend backend) {
	std::array<char, 4096> executable{};
	const ssize_t size = ::readlink("/proc/self/exe", executable.data(), executable.size() - 1);
	if (size <= 0 || static_cast<size_t>(size) >= executable.size() - 1) return false;
	const std::string prefix = "async_demo_" + std::to_string(::getpid()) + "_" +
	                           std::to_string(Clock::now().time_since_epoch().count());
	int sockets[2] = {-1, -1};
	if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, sockets) != 0) {
		return false;
	}
	Descriptor parent_socket(sockets[0]);
	Descriptor child_socket(sockets[1]);
	const std::string descriptor_argument = std::to_string(child_socket.get());
	const pid_t pid = ::fork();
	if (pid < 0) return false;
	if (pid == 0) {
		(void)::close(parent_socket.get());
		if (::fcntl(child_socket.get(), F_SETFD, 0) != 0) ::_exit(126);
		::execl(executable.data(), executable.data(), "--producer", prefix.c_str(),
		        descriptor_argument.c_str(),
		        backend == ipc::NotificationBackend::kDriver ? "driver" : "eventfd",
		        static_cast<char*>(nullptr));
		::_exit(127);
	}
	Child child(pid);
	child_socket.close();
	if (!transfer_token(parent_socket.get(), 'R', false)) return false;
	boost::asio::io_context loop;
	std::vector<std::shared_ptr<AsyncConsumer>> consumers;
	std::vector<PendingRead> first_reads;
	for (int index = 0; index < 2; ++index) {
		ipc::ChannelOptions options;
		options.name = prefix + "_" + std::to_string(index);
		auto opened = ipc::Consumer<Message>::open(options, message_schema());
		if (!opened) return api_error("consumer open", opened.error());
		auto adapted = AsyncConsumer::create(loop.get_executor(), std::move(opened.value()),
		                                     backend);
		if (!adapted) return api_error("async consumer create", adapted.error());
		first_reads.push_back(adapted.value().async_wait_latest(Clock::now() + 2s));
		consumers.push_back(std::make_shared<AsyncConsumer>(std::move(adapted.value())));
	}
	bool success = true;
	int completed = 0;
	boost::asio::steady_timer watchdog(loop);
	watchdog.expires_after(5s);
	watchdog.async_wait([&](const boost::system::error_code& error) {
		if (!error) {
			success = false;
			for (auto& consumer : consumers) consumer->stop();
		}
	});
	for (int index = 0; index < 2; ++index) {
		boost::asio::co_spawn(loop,
		                      consume(consumers[static_cast<size_t>(index)], index,
		                              std::move(first_reads[static_cast<size_t>(index)]),
		                              prefix + "_" + std::to_string(index)),
		                      [&](std::exception_ptr error) {
			                      if (error) {
				                      success = false;
				                      try {
					                      std::rethrow_exception(error);
				                      } catch (const std::exception& failure) {
					                      std::fprintf(stderr, "%s\n",
					                                   failure.what());
				                      }
			                      }
			                      if (++completed == 2) (void)watchdog.cancel();
		                      });
	}
	if (!transfer_token(parent_socket.get(), 'G', true)) return false;
	loop.run();
	for (int index = 0; index < 2; ++index) {
		ipc::ChannelOptions options;
		options.name = prefix + "_" + std::to_string(index);
		auto reopened = ipc::Consumer<Message>::open(options, message_schema());
		if (!reopened) {
			success = false;
			(void)api_error("reopen after async close while wrapper remains alive", reopened.error());
		}
	}
	const bool acknowledged = transfer_token(parent_socket.get(), 'Q', true);
	const bool child_ok = child.wait();
	std::printf(
	        "async demo backend=%s parent=%d producer=%d channels=2 io_threads=1 "
	        "completed=%d\n",
	        backend == ipc::NotificationBackend::kDriver ? "driver" : "eventfd",
	        static_cast<int>(::getpid()), static_cast<int>(pid), completed);
	return success && completed == 2 && acknowledged && child_ok;
}
}

int main(int argc, char** argv) {
	(void)::signal(SIGPIPE, SIG_IGN);
	auto backend = ipc::NotificationBackend::kEventFd;
	if (argc == 3 && std::string(argv[1]) == "--backend") {
		if (std::string(argv[2]) == "driver")
			backend = ipc::NotificationBackend::kDriver;
		else if (std::string(argv[2]) != "eventfd")
			return 2;
	}
	if ((argc == 4 || argc == 5) && std::string(argv[1]) == "--producer") {
		if (argc == 5) {
			if (std::string(argv[4]) == "driver")
				backend = ipc::NotificationBackend::kDriver;
			else if (std::string(argv[4]) != "eventfd")
				return 2;
		}
		char* end = nullptr;
		errno = 0;
		const long descriptor = std::strtol(argv[3], &end, 10);
		if (errno || end == argv[3] || *end || descriptor < 0 || descriptor > 1'000'000) return 2;
		const async_consume_demo::Descriptor owned(static_cast<int>(descriptor));
		return async_consume_demo::produce(argv[2], owned.get(), backend) ? 0 : 1;
	}
	if (argc != 1 && !(argc == 3 && std::string(argv[1]) == "--backend")) {
		std::fprintf(stderr, "usage: %s [--backend eventfd|driver]\n", argv[0]);
		return 2;
	}
	return async_consume_demo::run(backend) ? 0 : 1;
}
