#include "mixed_io_demo/support.hpp"

#include <linux/can.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <utility>

namespace mixed_io_demo {
namespace {

using namespace std::chrono_literals;

struct Statistics {
	uint64_t samples{0};
	uint64_t sequence{0};
	uint64_t missed{0};
	uint64_t can_frames{0};
	uint64_t can_missed{0};
	uint32_t can_sequence{0};
	uint64_t timer_expirations{0};
	uint64_t wakeups{0};
	uint64_t max_age_ns{0};
	uint64_t max_can_age_ms{0};
	uint64_t max_sample_gap_ns{0};
	uint64_t max_can_gap_ns{0};
	uint64_t max_timer_gap_ns{0};
	uint64_t last_sample_ns{0};
	uint64_t last_can_ns{0};
	uint64_t last_timer_ns{0};
	uint64_t stop_delay_ns{0};
	bool final_seen{false};
};

void gap(uint64_t now, uint64_t& previous, uint64_t& maximum) {
	maximum = std::max(maximum, now - previous);
	previous = now;
}

bool read_can(int descriptor, const Options& options, Statistics& statistics) {
	uint32_t received = 0;
	for (uint32_t attempt = 0; attempt < kCanReadBatch; ++attempt) {
		can_frame frame{};
		const ssize_t count = ::read(descriptor, &frame, sizeof(frame));
		if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
		if (count < 0 && errno == EINTR) continue;
		if (count != static_cast<ssize_t>(sizeof(frame))) return system_error("read AF_CAN");
		if (frame.can_id != options.can_id || frame.can_dlc != 8) {
			std::fprintf(stderr, "unexpected CAN frame format\n");
			return false;
		}
		uint32_t sequence = 0;
		uint32_t timestamp = 0;
		std::memcpy(&sequence, frame.data, sizeof(sequence));
		std::memcpy(&timestamp, frame.data + 4, sizeof(timestamp));
		if (sequence <= statistics.can_sequence) {
			std::fprintf(stderr, "non-monotonic CAN sequence\n");
			return false;
		}
		const uint64_t now = monotonic_ns();
		const uint32_t age = static_cast<uint32_t>(now / 1000000ULL) - timestamp;
		statistics.max_can_age_ms = std::max(statistics.max_can_age_ms, uint64_t{age});
		statistics.can_missed += sequence - statistics.can_sequence - 1;
		statistics.can_sequence = sequence;
		++statistics.can_frames;
		++received;
		gap(now, statistics.last_can_ns, statistics.max_can_gap_ns);
	}
	std::printf("{\"event\":\"can_batch\",\"frames\":%u,\"sequence\":%u,\"limit\":%u}\n",
	            received, statistics.can_sequence, kCanReadBatch);
	return true;
}

bool consume(const Options& options, int control, int stop) {
	if (!await_token(control, 'O', Clock::now() + 3s)) return false;
	ipc::ChannelOptions channel_options;
	channel_options.name = options.channel;
	auto opened = Consumer::open(channel_options, schema());
	if (!opened) return api_error("independent consumer open after exec", opened.error());
	Consumer consumer = std::move(opened.value());
	auto registered = consumer.event_notification(1000ms);
	if (!registered) return api_error("register aggregate notification", registered.error());
	auto notification = std::move(registered.value());
	Descriptor can(open_can(options, true));
	Descriptor timer(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC));
	Descriptor poller(::epoll_create1(EPOLL_CLOEXEC));
	if (can.get() < 0 || timer.get() < 0 || poller.get() < 0) return false;
	if (!arm_timer(timer.get(), 25)) return false;
	Watch notification_watch(poller.get(), notification.native_handle(), kNotificationTag);
	Watch can_watch(poller.get(), can.get(), kCanTag);
	Watch timer_watch(poller.get(), timer.get(), kTimerTag);
	Watch stop_watch(poller.get(), stop, kStopTag);
	if (!notification_watch.installed() || !can_watch.installed() || !timer_watch.installed() ||
	    !stop_watch.installed()) return false;
	if (!send_token(control, 'R', Clock::now() + 1s)) return false;
	const uint64_t started = monotonic_ns();
	const uint64_t cpu_started = cpu_ns();
	Statistics statistics;
	statistics.last_sample_ns = started;
	statistics.last_can_ns = started;
	statistics.last_timer_ns = started;
	const auto deadline = Clock::now() + std::chrono::milliseconds{options.duration_ms} + 3s;
	bool stopped = false;
	bool terminal = false;
	while (Clock::now() < deadline && !stopped && !terminal) {
		std::array<epoll_event, 4> events{};
		const int ready = ::epoll_wait(poller.get(), events.data(),
		                               static_cast<int>(events.size()), remaining_ms(deadline));
		if (ready < 0 && errno == EINTR) continue;
		if (ready < 0) return system_error("consumer application epoll_wait");
		uint64_t mask = 0;
		for (int index = 0; index < ready; ++index) {
			const auto& event = events[static_cast<size_t>(index)];
			if ((event.events & (EPOLLERR | EPOLLHUP)) != 0 &&
			    event.data.u64 != kNotificationTag) {
				std::fprintf(stderr, "business descriptor epoll error/hangup\n");
				return false;
			}
			mask |= event.data.u64;
		}
		if (mask == 0) continue;
		++statistics.wakeups;
		if ((mask & kStopTag) != 0) {
			uint64_t requested = 0;
			if (!read_counter(stop, requested)) return false;
			const uint64_t observed = monotonic_ns();
			if (requested == 0 || requested > observed) return false;
			statistics.stop_delay_ns = observed - requested;
			stopped = true;
		}
		if ((mask & kNotificationTag) != 0) {
			const auto drained = notification.drain();
			auto sample = consumer.try_read_latest();
			if (sample) {
				const auto& value = sample.value();
				if (value.sequence <= statistics.sequence || value.value.sequence != value.sequence ||
				    value.receive_boot_ns < value.publish_boot_ns || value.value.final > 1) {
					std::fprintf(stderr, "invalid copied sample sequence/timestamp\n");
					return false;
				}
				statistics.sequence = value.sequence;
				statistics.missed += value.missed_samples;
				++statistics.samples;
				statistics.max_age_ns = std::max(statistics.max_age_ns,
				                                   value.receive_boot_ns - value.publish_boot_ns);
				gap(monotonic_ns(), statistics.last_sample_ns, statistics.max_sample_gap_ns);
				std::printf("{\"event\":\"sample\",\"sequence\":%llu,\"missed\":%llu,"
				            "\"age_ns\":%llu,\"final\":%s}\n",
				            static_cast<unsigned long long>(value.sequence),
				            static_cast<unsigned long long>(value.missed_samples),
				            static_cast<unsigned long long>(value.receive_boot_ns - value.publish_boot_ns),
				            value.value.final != 0 ? "true" : "false");
				if (value.value.final != 0 && !statistics.final_seen) {
					statistics.final_seen = true;
					if (!send_token(control, 'F', Clock::now() + 1s)) return false;
				}
			} else if (sample.error().code != ipc::ErrorCode::kNoNewSample &&
			           sample.error().code != ipc::ErrorCode::kProducerOffline &&
			           sample.error().code != ipc::ErrorCode::kStaleHandle) {
				return api_error("copy latest after notification drain", sample.error());
			}
			if (!drained) {
				const auto code = drained.error().code;
				if (code != ipc::ErrorCode::kProducerOffline && code != ipc::ErrorCode::kStaleHandle) {
					return api_error("notification drain", drained.error());
				}
				std::printf("{\"event\":\"notification_terminal_after_read\",\"status\":\"%s\"}\n",
				            ipc::to_string(code));
				if (!notification_watch.remove()) return false;
				terminal = true;
			}
		}
		if ((mask & kTimerTag) != 0) {
			uint64_t count = 0;
			if (!read_counter(timer.get(), count)) return false;
			statistics.timer_expirations += count;
			gap(monotonic_ns(), statistics.last_timer_ns, statistics.max_timer_gap_ns);
		}
		if ((mask & kCanTag) != 0 && !read_can(can.get(), options, statistics)) return false;
		if (stopped) {
			std::printf("{\"event\":\"stop_observed\",\"ready_mask\":%llu,\"delay_ns\":%llu}\n",
			            static_cast<unsigned long long>(mask),
			            static_cast<unsigned long long>(statistics.stop_delay_ns));
		}
	}
	const uint64_t ended = monotonic_ns();
	gap(ended, statistics.last_sample_ns, statistics.max_sample_gap_ns);
	gap(ended, statistics.last_can_ns, statistics.max_can_gap_ns);
	gap(ended, statistics.last_timer_ns, statistics.max_timer_gap_ns);
	const bool valid = stopped && !terminal && statistics.final_seen && statistics.samples > 0 &&
	                   statistics.can_frames > 0 && statistics.timer_expirations > 0;
	std::printf("{\"event\":\"consumer_summary\",\"pid\":%ld,\"samples\":%llu,"
	            "\"sequence\":%llu,\"missed_samples\":%llu,\"can_frames\":%llu,"
	            "\"can_missed\":%llu,\"timer_expirations\":%llu,\"wakeups\":%llu,"
	            "\"max_age_ns\":%llu,\"max_can_age_ms\":%llu,\"max_sample_gap_ns\":%llu,"
	            "\"max_can_gap_ns\":%llu,\"max_timer_gap_ns\":%llu,\"stop_delay_ns\":%llu,"
	            "\"wall_ns\":%llu,\"cpu_ns\":%llu,\"valid\":%s}\n",
	            static_cast<long>(::getpid()), static_cast<unsigned long long>(statistics.samples),
	            static_cast<unsigned long long>(statistics.sequence),
	            static_cast<unsigned long long>(statistics.missed),
	            static_cast<unsigned long long>(statistics.can_frames),
	            static_cast<unsigned long long>(statistics.can_missed),
	            static_cast<unsigned long long>(statistics.timer_expirations),
	            static_cast<unsigned long long>(statistics.wakeups),
	            static_cast<unsigned long long>(statistics.max_age_ns),
	            static_cast<unsigned long long>(statistics.max_can_age_ms),
	            static_cast<unsigned long long>(statistics.max_sample_gap_ns),
	            static_cast<unsigned long long>(statistics.max_can_gap_ns),
	            static_cast<unsigned long long>(statistics.max_timer_gap_ns),
	            static_cast<unsigned long long>(statistics.stop_delay_ns),
	            static_cast<unsigned long long>(ended - started),
	            static_cast<unsigned long long>(cpu_ns() - cpu_started), valid ? "true" : "false");
	const bool notification_removed = notification_watch.remove();
	const bool can_removed = can_watch.remove();
	const bool timer_removed = timer_watch.remove();
	const bool stop_removed = stop_watch.remove();
	return valid && notification_removed && can_removed && timer_removed && stop_removed;
}

}

bool run_consumer(const Options& options) {
	Descriptor control(options.control_fd);
	Descriptor stop(options.stop_fd);
	const bool result = consume(options, control.get(), stop.get());
	return result && send_token(control.get(), 'D', Clock::now() + 1s);
}

}
