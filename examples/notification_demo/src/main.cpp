#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <utility>

#include "notification_demo/api.hpp"

namespace notification_demo {
namespace {

using Clock = std::chrono::steady_clock;
using Consumer = ipc::Consumer<Message>;
using Producer = ipc::Producer<Message>;
using namespace std::chrono_literals;

class FileDescriptor {
       public:
	explicit FileDescriptor(int descriptor) : descriptor_(descriptor) {}
	~FileDescriptor() { close(); }
	FileDescriptor(const FileDescriptor&) = delete;
	FileDescriptor& operator=(const FileDescriptor&) = delete;
	int get() const { return descriptor_; }
	void close() {
		if (descriptor_ >= 0) (void)::close(std::exchange(descriptor_, -1));
	}

       private:
	int descriptor_;
};

bool api_error(const char* operation, const ipc::Error& error) {
	std::fprintf(stderr, "%s: %s errno=%d\n", operation, ipc::to_string(error.code),
	             error.errno_value);
	return false;
}

bool system_error(const char* operation) {
	std::perror(operation);
	return false;
}

class EpollWatch {
       public:
	EpollWatch(int poller, int descriptor, uint64_t tag)
	    : poller_(poller), descriptor_(descriptor) {
		epoll_event event{};
		event.events = EPOLLIN;
		event.data.u64 = tag;
		installed_ = ::epoll_ctl(poller_, EPOLL_CTL_ADD, descriptor_, &event) == 0;
		if (!installed_) (void)system_error("epoll ADD");
	}
	~EpollWatch() {
		if (installed_ && ::epoll_ctl(poller_, EPOLL_CTL_DEL, descriptor_, nullptr) != 0) {
			(void)system_error("epoll DEL before notification/fd destruction");
		}
	}
	EpollWatch(const EpollWatch&) = delete;
	EpollWatch& operator=(const EpollWatch&) = delete;
	bool installed() const { return installed_; }

       private:
	int poller_;
	int descriptor_;
	bool installed_{false};
};

class OwnedChannel {
       public:
	explicit OwnedChannel(Producer& producer) : producer_(producer) {}
	~OwnedChannel() {
		if (active_) (void)close();
	}
	OwnedChannel(const OwnedChannel&) = delete;
	OwnedChannel& operator=(const OwnedChannel&) = delete;
	bool close() {
		active_ = false;
		const auto disabled = producer_.disable_event_notifications();
		const auto removed = producer_.remove_if_owner();
		if (!disabled) (void)api_error("disable notifications", disabled.error());
		if (!removed) (void)api_error("remove own channel", removed.error());
		return disabled && removed;
	}

       private:
	Producer& producer_;
	bool active_{true};
};

class ChildProcess {
       public:
	explicit ChildProcess(pid_t pid) : pid_(pid) {}
	~ChildProcess() {
		if (pid_ <= 0) return;
		(void)::kill(pid_, SIGKILL);
		int status = 0;
		while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
		}
	}
	ChildProcess(const ChildProcess&) = delete;
	ChildProcess& operator=(const ChildProcess&) = delete;
	bool wait() {
		const auto deadline = Clock::now() + 3s;
		while (Clock::now() < deadline) {
			int status = 0;
			const pid_t result = ::waitpid(pid_, &status, WNOHANG);
			if (result == pid_) {
				pid_ = -1;
				return WIFEXITED(status) && WEXITSTATUS(status) == 0;
			}
			if (result < 0 && errno != EINTR) {
				if (errno == ECHILD) pid_ = -1;
				return system_error("waitpid");
			}
			(void)::poll(nullptr, 0, 10);
		}
		std::fprintf(stderr, "child exit timeout\n");
		return false;
	}

       private:
	pid_t pid_;
};

bool send_token(int descriptor, char token) {
	for (int attempt = 0; attempt < 8; ++attempt) {
		if (::write(descriptor, &token, 1) == 1) return true;
		if (errno != EINTR) break;
	}
	return system_error("write child status");
}

bool await_token(int descriptor, char expected) {
	const auto deadline = Clock::now() + 3s;
	while (Clock::now() < deadline) {
		const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
		        deadline - Clock::now());
		if (remaining.count() < 0) break;
		pollfd event{descriptor, POLLIN, 0};
		const int result = ::poll(&event, 1, static_cast<int>(remaining.count()) + 1);
		if (result < 0 && errno == EINTR) continue;
		if (result <= 0) break;
		char token = 0;
		const ssize_t received = ::read(descriptor, &token, 1);
		if (received < 0 && (errno == EINTR || errno == EAGAIN)) continue;
		if (received == 1 && token == expected) return true;
		break;
	}
	std::fprintf(stderr, "child status missing or invalid (expected %c)\n", expected);
	return false;
}

bool consume(const std::string& name, int status_fd, ipc::NotificationBackend backend) {
	ipc::ChannelOptions options;
	options.name = name;
	auto opened = Consumer::open(options, message_schema());
	if (!opened) return api_error("independent consumer open", opened.error());
	Consumer consumer = std::move(opened.value());
	auto registered = consumer.event_notification(backend, 1000ms);
	if (!registered) return api_error("register sidecar notification", registered.error());
	auto notification = std::move(registered.value());
	FileDescriptor poller(::epoll_create1(EPOLL_CLOEXEC));
	FileDescriptor timer(::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK));
	FileDescriptor stop(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK));
	if (poller.get() < 0 || timer.get() < 0 || stop.get() < 0) {
		return system_error("create application epoll/timerfd/stop eventfd");
	}
	itimerspec watchdog{};
	watchdog.it_value.tv_sec = 5;
	if (::timerfd_settime(timer.get(), 0, &watchdog, nullptr) != 0) {
		return system_error("arm watchdog timerfd");
	}
	EpollWatch notification_del_before_destruction(poller.get(), notification.native_handle(),
	                                               1);
	EpollWatch timer_watch(poller.get(), timer.get(), 2);
	EpollWatch stop_watch(poller.get(), stop.get(), 3);
	if (!notification_del_before_destruction.installed() || !timer_watch.installed() ||
	    !stop_watch.installed())
		return false;
	uint32_t expected = 1;
	uint64_t previous_sequence = 0;
	const auto deadline = Clock::now() + 6s;
	while (Clock::now() < deadline) {
		std::array<epoll_event, 3> events{};
		const int count = ::epoll_wait(poller.get(), events.data(), 3, 1000);
		if (count < 0 && errno == EINTR) continue;
		if (count < 0) return system_error("application epoll_wait");
		for (int index = 0; index < count; ++index) {
			const uint64_t tag = events[static_cast<size_t>(index)].data.u64;
			if (tag == 2) {
				std::fprintf(stderr, "consumer watchdog expired\n");
				return false;
			}
			if (tag == 3) return expected == 3 && send_token(status_fd, 'S');
			const auto drained = notification.drain();
			auto sample = consumer.try_read_latest();
			if (!sample) {
				if (!drained)
					return api_error(
					        "notification drain (re-register if stale/offline)",
					        drained.error());
				if (sample.error().code == ipc::ErrorCode::kNoNewSample) continue;
				return api_error("read latest after drain", sample.error());
			}
			if (sample.value().value.value != expected ||
			    sample.value().sequence <= previous_sequence)
				return false;
			previous_sequence = sample.value().sequence;
			std::printf("consumer pid=%d value=%u sequence=%llu (%s registration)\n",
			            static_cast<int>(::getpid()), expected,
			            static_cast<unsigned long long>(previous_sequence),
			            expected == 1 ? "published before" : "published after");
			++expected;
			if (expected == 2) {
				if (!send_token(status_fd, 'R')) return false;
			} else {
				const uint64_t signal = 1;
				if (::write(stop.get(), &signal, sizeof(signal)) !=
				    static_cast<ssize_t>(sizeof(signal)))
					return system_error("signal stop");
			}
			if (!drained)
				return api_error("notification drain after final sample",
				                 drained.error());
		}
	}
	std::fprintf(stderr, "consumer absolute deadline expired\n");
	return false;
}

bool produce(ipc::NotificationBackend backend) {
	std::array<char, 4096> executable{};
	const ssize_t length =
	        ::readlink("/proc/self/exe", executable.data(), executable.size() - 1);
	if (length <= 0 || static_cast<size_t>(length) >= executable.size() - 1) {
		return system_error("read executable path");
	}
	const std::string name = "notification_demo_" + std::to_string(::getpid()) + "_" +
	                         std::to_string(Clock::now().time_since_epoch().count());
	ipc::ChannelOptions options;
	options.name = name;
	auto created = Producer::create(options, message_schema());
	if (!created) return api_error("create own channel", created.error());
	Producer producer = std::move(created.value());
	OwnedChannel own_channel(producer);
	const auto enabled = producer.enable_event_notifications(backend);
	if (!enabled) return api_error("enable notifications", enabled.error());
	const auto initial = producer.publish(Message{1});
	if (!initial) return api_error("publish before registration", initial.error());
	int descriptors[2] = {-1, -1};
	if (::pipe2(descriptors, O_CLOEXEC | O_NONBLOCK) != 0) return system_error("status pipe");
	FileDescriptor status_read(descriptors[0]);
	FileDescriptor status_write(descriptors[1]);
	const std::string status_argument = std::to_string(status_write.get());
	const char* backend_name =
	        backend == ipc::NotificationBackend::kDriver ? "driver" : "eventfd";
	std::array<char*, 6> arguments{executable.data(),
	                               const_cast<char*>("--consumer"),
	                               const_cast<char*>(name.c_str()),
	                               const_cast<char*>(status_argument.c_str()),
	                               const_cast<char*>(backend_name),
	                               nullptr};
	const pid_t pid = ::fork();
	if (pid < 0) return system_error("fork");
	if (pid == 0) {
		(void)::close(status_read.get());
		if (::fcntl(status_write.get(), F_SETFD, 0) < 0) ::_exit(126);
		::execv(executable.data(), arguments.data());
		::_exit(127);
	}
	ChildProcess child(pid);
	status_write.close();
	if (!await_token(status_read.get(), 'R')) return false;
	const auto second = producer.publish(Message{2});
	if (!second) return api_error("publish after registration", second.error());
	if (!await_token(status_read.get(), 'S') || !child.wait()) return false;
	const int delivery_error = producer.event_notification_error();
	if (delivery_error != 0) {
		std::fprintf(stderr, "asynchronous notification delivery errno=%d\n",
		             delivery_error);
		return false;
	}
	if (!own_channel.close()) return false;
	std::printf(
	        "OK backend=%s parent=%d child=%d channel=%s; sidecar registration; epoll notification + "
	        "timerfd + stop eventfd; RAII DEL before notification destruction; own-channel "
	        "cleanup only\n",
	        backend_name, static_cast<int>(::getpid()), static_cast<int>(pid), name.c_str());
	return true;
}

}
}

int main(int argc, char** argv) {
	(void)::signal(SIGPIPE, SIG_IGN);
	if (argc == 5 && std::string(argv[1]) == "--consumer") {
		char* end = nullptr;
		errno = 0;
		const long descriptor = std::strtol(argv[3], &end, 10);
		if (errno != 0 || end == argv[3] || *end != '\0' || descriptor < 0 ||
		    descriptor > 1'000'000)
			return 2;
		const notification_demo::FileDescriptor status(static_cast<int>(descriptor));
		const std::string selected = argv[4];
		if (selected != "eventfd" && selected != "driver") return 2;
		const auto backend = selected == "driver" ? ipc::NotificationBackend::kDriver
		                                          : ipc::NotificationBackend::kEventFd;
		return notification_demo::consume(argv[2], status.get(), backend) ? 0 : 1;
	}
	ipc::NotificationBackend backend = ipc::NotificationBackend::kEventFd;
	if (argc == 3 && std::string(argv[1]) == "--backend" &&
	    (std::string(argv[2]) == "driver" || std::string(argv[2]) == "eventfd")) {
		if (std::string(argv[2]) == "driver") backend = ipc::NotificationBackend::kDriver;
	} else if (argc != 1) {
		std::fprintf(stderr, "usage: %s [--backend eventfd|driver]\n", argv[0]);
		return 2;
	}
	return notification_demo::produce(backend) ? 0 : 1;
}
