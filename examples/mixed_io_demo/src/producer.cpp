#include "mixed_io_demo/support.hpp"

#include <fcntl.h>
#include <linux/can.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>
#include <utility>

namespace mixed_io_demo {
namespace {

using namespace std::chrono_literals;

class ChannelOwner {
       public:
	explicit ChannelOwner(Producer& producer) : producer_(producer) {}
	~ChannelOwner() {
		if (active_) (void)remove();
	}
	ChannelOwner(const ChannelOwner&) = delete;
	ChannelOwner& operator=(const ChannelOwner&) = delete;
	bool remove() {
		active_ = false;
		const auto disabled = producer_.disable_event_notifications();
		const auto removed = producer_.remove_if_owner();
		if (!disabled) (void)api_error("disable notifications", disabled.error());
		if (!removed) (void)api_error("remove own channel", removed.error());
		std::printf("{\"event\":\"channel_cleanup\",\"disabled\":%s,\"removed\":%s,"
		            "\"control_journal_retained\":true}\n",
		            disabled ? "true" : "false", removed ? "true" : "false");
		return disabled && removed;
	}

       private:
	Producer& producer_;
	bool active_{true};
};

bool emit_can(int descriptor, uint32_t can_id, uint32_t& sequence, uint64_t& backpressure,
              uint32_t batch) {
	for (uint32_t index = 0; index < batch; ++index) {
		can_frame frame{};
		frame.can_id = can_id;
		frame.can_dlc = 8;
		const uint32_t next = sequence + 1;
		const uint32_t timestamp = static_cast<uint32_t>(monotonic_ns() / 1000000ULL);
		std::memcpy(frame.data, &next, sizeof(next));
		std::memcpy(frame.data + 4, &timestamp, sizeof(timestamp));
		const ssize_t sent = ::write(descriptor, &frame, sizeof(frame));
		if (sent == static_cast<ssize_t>(sizeof(frame))) {
			sequence = next;
			continue;
		}
		if (sent < 0 && (errno == EAGAIN || errno == ENOBUFS || errno == EINTR)) {
			++backpressure;
			break;
		}
		return system_error("write real CAN frame");
	}
	return true;
}

bool session(Options options) {
	options.channel = "mixio." + std::to_string(::getpid()) + "." + std::to_string(monotonic_ns());
	options.can_id = 0x400U + static_cast<uint32_t>(::getpid()) % 0x3ffU;
	Descriptor can(open_can(options, false));
	Descriptor stop(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
	Descriptor timer(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC));
	int pair[2] = {-1, -1};
	if (can.get() < 0 || stop.get() < 0 || timer.get() < 0) {
		return system_error("create producer descriptors");
	}
	if (::socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, pair) != 0) {
		return system_error("create exec handshake socketpair");
	}
	Descriptor parent_control(pair[0]);
	Descriptor child_control(pair[1]);
	const std::string duration = std::to_string(options.duration_ms);
	const std::string batch = std::to_string(options.can_batch);
	const std::string can_id = std::to_string(options.can_id);
	const std::string control_fd = std::to_string(child_control.get());
	const std::string stop_fd = std::to_string(stop.get());
	const pid_t pid = ::fork();
	if (pid < 0) return system_error("fork independent consumer");
	if (pid == 0) {
		if (::fcntl(child_control.get(), F_SETFD, 0) < 0 ||
		    ::fcntl(stop.get(), F_SETFD, 0) < 0) {
			::_exit(126);
		}
		::execl("/proc/self/exe", "mixed_io_demo", "--consumer", options.interface.c_str(),
		        options.channel.c_str(), duration.c_str(), batch.c_str(), can_id.c_str(),
		        control_fd.c_str(), stop_fd.c_str(), "exec", static_cast<char*>(nullptr));
		::_exit(127);
	}
	Child child(pid);
	child_control.close();
	ipc::ChannelOptions channel_options;
	channel_options.name = options.channel;
	auto created = Producer::create(channel_options, schema());
	if (!created) return api_error("create unique channel", created.error());
	Producer producer = std::move(created.value());
	ChannelOwner owner(producer);
	const auto enabled = producer.enable_event_notifications();
	if (!enabled) return api_error("enable event notifications", enabled.error());
	const auto startup_deadline = Clock::now() + 3s;
	if (!send_token(parent_control.get(), 'O', startup_deadline) ||
	    !await_token(parent_control.get(), 'R', startup_deadline) || !arm_timer(timer.get(), 10)) {
		return false;
	}
	std::printf("{\"event\":\"producer_ready\",\"pid\":%ld,\"consumer_pid\":%ld,"
	            "\"channel\":\"%s\",\"can_id\":%u,\"can_batch\":%u}\n",
	            static_cast<long>(::getpid()), static_cast<long>(pid), options.channel.c_str(),
	            options.can_id, options.can_batch);
	const uint64_t started = monotonic_ns();
	const uint64_t cpu_started = cpu_ns();
	const auto traffic_deadline = Clock::now() + std::chrono::milliseconds{options.duration_ms};
	uint64_t published = 0;
	uint32_t transmitted = 0;
	uint64_t backpressure = 0;
	while (Clock::now() < traffic_deadline) {
		pollfd events[2] = {{timer.get(), POLLIN, 0}, {parent_control.get(), POLLIN, 0}};
		const int ready = ::poll(events, 2, remaining_ms(traffic_deadline));
		if (ready < 0 && errno == EINTR) continue;
		if (ready < 0) return system_error("producer schedule poll");
		if (events[1].revents != 0) {
			std::fprintf(stderr, "consumer exited before final sample\n");
			return false;
		}
		if ((events[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) return false;
		if ((events[0].revents & POLLIN) == 0) continue;
		uint64_t expirations = 0;
		if (!read_counter(timer.get(), expirations)) return false;
		const auto committed = producer.publish(Message{published + 1, 0});
		if (!committed) return api_error("publish copied sample", committed.error());
		++published;
		if (!emit_can(can.get(), options.can_id, transmitted, backpressure, options.can_batch)) {
			return false;
		}
	}
	const auto final = producer.publish(Message{published + 1, 1});
	if (!final) return api_error("publish final copied sample", final.error());
	++published;
	if (!await_token(parent_control.get(), 'F', Clock::now() + 2s)) return false;
	const uint64_t requested = monotonic_ns();
	if (!write_counter(stop.get(), requested) ||
	    !await_token(parent_control.get(), 'D', Clock::now() + 2s) ||
	    !child.wait(Clock::now() + 2s)) {
		return false;
	}
	const bool notifications_ok = producer.event_notification_error() == 0;
	std::printf("{\"event\":\"producer_summary\",\"samples\":%llu,\"can_frames\":%u,"
	            "\"can_backpressure\":%llu,\"wall_ns\":%llu,\"cpu_ns\":%llu,"
	            "\"notification_errno\":%d}\n",
	            static_cast<unsigned long long>(published), transmitted,
	            static_cast<unsigned long long>(backpressure),
	            static_cast<unsigned long long>(monotonic_ns() - started),
	            static_cast<unsigned long long>(cpu_ns() - cpu_started),
	            producer.event_notification_error());
	return owner.remove() && notifications_ok;
}

}

bool run_producer(const Options& options) {
	return session(options);
}

}
