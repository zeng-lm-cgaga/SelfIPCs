// 基准消费者可使用 futex 或有限轮询读取，并将原始延迟数据写入 CSV。
#include <poll.h>
#include <sched.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <utility>

#include "edge_runtime_tool/bench_notification.hpp"
#include "edge_runtime_tool/bench_payload.hpp"
#include "edge_runtime/channel/channel_options.hpp"
#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/channel/sample.hpp"
#include "edge_runtime/common/schema.hpp"
#include "edge_runtime_tool/tool_common.hpp"

namespace {

using edge_tool::arg_u64;
using edge_tool::arg_value;
using edge_tool::cpu_us_now;
using edge_tool::monotonic_raw_now_ns;

struct Args {
	std::string name;
	uint32_t payload = 64;
	bool use_wait = true;
	edge_tool::BenchNotification notification = edge_tool::BenchNotification::kFutex;
	std::string csv_path;
	uint64_t max_samples = 1000000;
	uint64_t max_time_ms = 60000;
	uint64_t idle_ms = 1500;
	uint64_t poll_bound = 1024;
	uint64_t open_retry_ms = 10000;
	bool checksum = true;
	int ready_fd = -1;
};

template <size_t N>
int run_consume_n(const Args& a, const edge_runtime::SchemaDescriptor& schema) {
	using ConsumerT = edge_runtime::Consumer<bench::BenchPayloadV1<N>>;
	edge_runtime::ChannelOptions opts;
	opts.name = a.name;
	opts.enable_payload_checksum = a.checksum;

	auto open_one = [&]() { return ConsumerT::open(opts, schema); };
	auto consumer = open_one();
	if (!consumer &&
	    (consumer.error().code == edge_runtime::ErrorCode::kNotFound ||
	     consumer.error().code == edge_runtime::ErrorCode::kInitializationIncomplete ||
	     consumer.error().code == edge_runtime::ErrorCode::kCorruptHeader) &&
	    a.open_retry_ms > 0) {
		const int64_t deadline_ms =
		        edge_tool::monotonic_ms_now() + static_cast<int64_t>(a.open_retry_ms);
		while (!consumer && edge_tool::monotonic_ms_now() < deadline_ms) {
			struct timespec ts {};
			ts.tv_sec = 0;
			ts.tv_nsec = 10 * 1000000L;
			::nanosleep(&ts, nullptr);
			consumer = open_one();
		}
	}
	if (!consumer) {
		const auto& e = consumer.error();
		std::printf("OPEN_FAIL code=%s errno=%d op=%s ctx=%s\n",
		            edge_runtime::to_string(e.code), e.errno_value, e.operation, e.context);
		std::fflush(stdout);
		return 2;
	}
	std::optional<edge_runtime::EventNotification> notification;
	if (a.notification == edge_tool::BenchNotification::kEventfd) {
		auto registered = consumer.value().event_notification(std::chrono::milliseconds(1000));
		if (!registered) {
			const auto& error = registered.error();
			std::printf("NOTIFICATION_REGISTER_FAIL code=%s errno=%d op=%s ctx=%s\n",
			            edge_runtime::to_string(error.code), error.errno_value,
			            error.operation, error.context);
			std::fflush(stdout);
			return 2;
		}
		notification.emplace(std::move(registered.value()));
	}
	FILE* csv = std::fopen(a.csv_path.c_str(), "w");
	if (csv == nullptr) {
		std::fprintf(stderr, "cannot open --csv %s\n", a.csv_path.c_str());
		return 2;
	}
	if (!edge_tool::bench_signal_ready(a.ready_fd)) {
		std::fprintf(stderr, "NOTIFICATION_READY_FAIL\n");
		std::fclose(csv);
		return 2;
	}
	std::printf("READY\n");
	std::fflush(stdout);

	const uint64_t start_cpu = cpu_us_now();
	const int64_t start_ms = edge_tool::monotonic_ms_now();
	const int64_t deadline_ms =
	        a.max_time_ms > 0 ? start_ms + static_cast<int64_t>(a.max_time_ms) : 0;
	uint64_t reads = 0;
	uint64_t torn = 0;
	uint64_t missed_total = 0;
	uint64_t last_seq = 0;
	uint64_t last_sample_raw = 0;
	uint64_t empty_polls = 0;
	const char* ended = "time";

	auto write_row = [&](const edge_runtime::Sample<bench::BenchPayloadV1<N>>& s,
	                     uint64_t receive_ns) {
		const uint64_t latency =
		        receive_ns >= s.value.publish_ns ? receive_ns - s.value.publish_ns : 0;
		std::fprintf(csv,
		             "%" PRIu64 ",%" PRIu64 ",%u,%" PRIu64 ",%" PRIu64 ",%" PRIu64
		             ",%" PRIu64 "\n",
		             s.sequence, s.generation, static_cast<unsigned>(N), s.value.publish_ns,
		             receive_ns, latency, s.missed_samples);
	};

	for (;;) {
		const int64_t now_ms = edge_tool::monotonic_ms_now();
		if (deadline_ms != 0 && now_ms >= deadline_ms) {
			ended = "time";
			break;
		}
		int64_t wait_ms =
		        deadline_ms != 0 ? std::min<int64_t>(200, deadline_ms - now_ms) : 200;
		auto snap = [&]() -> edge_runtime::Result<edge_runtime::Sample<bench::BenchPayloadV1<N>>> {
			if (notification) {
				auto drained = notification->drain();
				auto latest = consumer.value().try_read_latest();
				if (latest) return latest;
				if (!drained) return drained.error();
				return latest;
			}
			if (a.use_wait) {
				return consumer.value().wait_latest(std::chrono::milliseconds(wait_ms));
			}
			return consumer.value().try_read_latest();
		}();
		if (snap) {
			const uint64_t receive_ns = monotonic_raw_now_ns();
			empty_polls = 0;
			last_seq = snap.value().sequence;
			missed_total += snap.value().missed_samples;
			write_row(snap.value(), receive_ns);
			++reads;
			last_sample_raw = receive_ns;
			if (a.max_samples > 0 && reads >= a.max_samples) {
				ended = "samples";
				break;
			}
			continue;
		}
		const edge_runtime::ErrorCode ec = snap.error().code;
		if (ec == edge_runtime::ErrorCode::kNoNewSample ||
		    ec == edge_runtime::ErrorCode::kReadContention) {
			if (reads > 0 && a.idle_ms > 0 && last_sample_raw != 0) {
				const uint64_t idle_elapsed_ms =
				        (monotonic_raw_now_ns() - last_sample_raw) / 1000000ull;
				if (idle_elapsed_ms >= a.idle_ms) {
					ended = "idle";
					break;
				}
				wait_ms = std::min(
				        wait_ms,
				        static_cast<int64_t>(std::min<uint64_t>(200, a.idle_ms - idle_elapsed_ms)));
			}
			if (notification) {
				if (deadline_ms != 0) {
					wait_ms = std::min(wait_ms, deadline_ms - edge_tool::monotonic_ms_now());
					if (wait_ms <= 0) continue;
				}
				struct pollfd descriptor {notification->native_handle(), POLLIN, 0};
				const int ready = ::poll(&descriptor, 1, static_cast<int>(wait_ms));
				if ((ready < 0 && errno != EINTR) || (descriptor.revents & POLLNVAL) != 0) {
					std::printf("NOTIFICATION_POLL_FAIL errno=%d revents=%d\n",
					            ready < 0 ? errno : EBADF,
					            static_cast<int>(descriptor.revents));
					std::fflush(stdout);
					std::fclose(csv);
					return 3;
				}
			} else if (!a.use_wait && ++empty_polls >= a.poll_bound) {
				::sched_yield();
				empty_polls = 0;
			}
		} else if (a.use_wait &&
		           (ec == edge_runtime::ErrorCode::kDataStale ||
		            ec == edge_runtime::ErrorCode::kProducerOffline ||
		            ec == edge_runtime::ErrorCode::kRecoveryBlocked)) {
			ended = "idle";
			break;
		} else if (ec == edge_runtime::ErrorCode::kPayloadCorrupt ||
		           ec == edge_runtime::ErrorCode::kPayloadDecodeFailed) {
			++torn;
			break;
		} else {
			std::printf("READ_ERROR code=%s seq=%" PRIu64 "\n",
			            edge_runtime::to_string(ec), last_seq);
			std::fflush(stdout);
			std::fclose(csv);
			return 3;
		}
	}

	std::fclose(csv);
	const uint64_t end_cpu = cpu_us_now();
	const uint64_t elapsed_ms = static_cast<uint64_t>(edge_tool::monotonic_ms_now() - start_ms);
	std::printf("SUMMARY reads=%" PRIu64 " torn=%" PRIu64 " missed_total=%" PRIu64
	            " last_seq=%" PRIu64 " elapsed_ms=%" PRIu64 " cpu_us=%" PRIu64 " ended=%s\n",
	            reads, torn, missed_total, last_seq, elapsed_ms, end_cpu - start_cpu, ended);
	std::fflush(stdout);
	return torn > 0 ? 4 : 0;
}

}

int main(int argc, char** argv) {
	const char* name = edge_tool::arg_value(argc, argv, "--name");
	if (edge_tool::arg_flag(argc, argv, "--help") || name == nullptr || *name == '\0') {
		std::fprintf(stderr,
		             "usage: self_ipcs_shm_bench_consumer --name <name> --payload <bytes> "
		             "--csv <path> [--mode futex|poll] [--max-samples N] "
		             "[--max-time-ms T] [--idle-ms N] [--poll-bound N] "
		             "[--open-retry-ms N] [--notification futex|eventfd] [--ready-fd FD] [--checksum 1]\n");
		return edge_tool::arg_flag(argc, argv, "--help") ? 0 : 2;
	}
	Args a;
	a.name = name;
	a.payload = static_cast<uint32_t>(arg_u64(argc, argv, "--payload", 64));
	const char* mode = edge_tool::arg_value(argc, argv, "--mode");
	a.use_wait = mode == nullptr || std::string(mode) == "futex";
	a.notification = edge_tool::bench_notification_from_args(argc, argv);
	a.ready_fd = edge_tool::bench_ready_fd_from_args(argc, argv);
	if (!a.use_wait && a.notification == edge_tool::BenchNotification::kEventfd) {
		std::fprintf(stderr,
		             "--notification eventfd requires --mode futex (or the default mode)\n");
		return 2;
	}
	const char* csv = edge_tool::arg_value(argc, argv, "--csv");
	if (csv == nullptr || *csv == '\0') {
		std::fprintf(stderr, "self_ipcs_shm_bench_consumer: --csv is required\n");
		return 2;
	}
	a.csv_path = csv;
	a.max_samples = arg_u64(argc, argv, "--max-samples", 1000000);
	a.max_time_ms = arg_u64(argc, argv, "--max-time-ms", 60000);
	a.idle_ms = arg_u64(argc, argv, "--idle-ms", 1500);
	a.poll_bound = arg_u64(argc, argv, "--poll-bound", 1024);
	a.open_retry_ms = arg_u64(argc, argv, "--open-retry-ms", 10000);
	a.checksum = edge_tool::mandatory_payload_checksum(argc, argv);

	const edge_runtime::SchemaDescriptor schema{bench::bench_fingerprint(a.payload), 1,
	                                            "BenchPayloadV1"};

	switch (a.payload) {
		case 64:
			return run_consume_n<64>(a, schema);
		case 256:
			return run_consume_n<256>(a, schema);
		case 1024:
			return run_consume_n<1024>(a, schema);
		case 4096:
			return run_consume_n<4096>(a, schema);
		case 16384:
			return run_consume_n<16384>(a, schema);
		case 65536:
			return run_consume_n<65536>(a, schema);
		default:
			std::fprintf(
			        stderr,
			        "unsupported --payload %u (need 64|256|1024|4096|16384|65536)\n",
			        a.payload);
			return 2;
	}
}
