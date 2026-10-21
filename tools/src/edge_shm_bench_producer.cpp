// 基准生产者按指定载荷大小和速率发布样本，时间戳写入载荷而不是依赖日志时间。
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "edge_runtime_tool/bench_notification.hpp"
#include "edge_runtime_tool/notification_metrics.hpp"
#include "edge_runtime_tool/bench_payload.hpp"
#include "edge_runtime/channel/channel_options.hpp"
#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/channel/sample.hpp"
#include "edge_runtime/common/schema.hpp"
#include "edge_runtime_tool/tool_common.hpp"

namespace {

using edge_tool::arg_flag;
using edge_tool::arg_u64;
using edge_tool::arg_value;
using edge_tool::cpu_us_now;
using edge_tool::monotonic_raw_now_ns;

struct Args {
	std::string name;
	uint32_t payload = 64;
	uint64_t samples = 1000000;
	uint64_t max_time_ms = 60000;
	uint64_t rate_hz = 0;
	uint64_t wait_consumer_ms = 0;
	int ready_fd = -1;
	edge_tool::BenchNotification notification = edge_tool::BenchNotification::kFutex;
	bool checksum = true;
	std::string publish_csv;
};

template <size_t N>
int run_producer_n(const Args& a, const edge_runtime::SchemaDescriptor& schema) {
	using ProducerT = edge_runtime::Producer<bench::BenchPayloadV1<N>>;
	edge_runtime::ChannelOptions opts;
	opts.name = a.name;
	opts.enable_payload_checksum = a.checksum;

	auto p = ProducerT::create(opts, schema);
	if (!p) {
		const auto& e = p.error();
		std::printf("CREATE_FAIL code=%s errno=%d op=%s ctx=%s\n",
		            edge_runtime::to_string(e.code), e.errno_value, e.operation, e.context);
		std::fflush(stdout);
		return 2;
	}
	if (a.notification == edge_tool::BenchNotification::kEventfd) {
		auto enabled = p.value().enable_event_notifications();
		if (!enabled) {
			const auto& error = enabled.error();
			std::printf("NOTIFICATION_ENABLE_FAIL code=%s errno=%d op=%s ctx=%s\n",
			            edge_runtime::to_string(error.code), error.errno_value,
			            error.operation, error.context);
			std::fflush(stdout);
			return 2;
		}
	}
	std::printf("READY generation=%" PRIu64 "\n", p.value().generation());
	std::fflush(stdout);
	if (a.ready_fd >= 0) {
		if (!edge_tool::bench_wait_ready(a.ready_fd, a.wait_consumer_ms > 0 ? a.wait_consumer_ms : 5000)) {
			std::printf("WAIT_READY_FAIL code=TimeoutOrDisconnected\n");
			return 2;
		}
	} else if (a.wait_consumer_ms > 0) {
		const int64_t wait_deadline = edge_tool::monotonic_ms_now() +
		                              static_cast<int64_t>(a.wait_consumer_ms);
		for (;;) {
			auto status = p.value().status();
			if (!status) {
				std::printf("WAIT_CONSUMER_FAIL code=%s\n",
				            edge_runtime::to_string(status.error().code));
				return 2;
			}
			if (status.value().consumer_state == static_cast<uint32_t>(
			                                             edge_runtime::detail::EndpointState::kOnline)) {
				break;
			}
			if (edge_tool::monotonic_ms_now() >= wait_deadline) {
				std::printf("WAIT_CONSUMER_FAIL code=Timeout\n");
				return 2;
			}
			struct timespec ts {};
			ts.tv_nsec = 1000000L;
			::nanosleep(&ts, nullptr);
		}
	}

	FILE* publications = nullptr;
	if (!a.publish_csv.empty()) {
		publications = std::fopen(a.publish_csv.c_str(), "w");
		if (publications == nullptr) return 2;
		std::fputs("attempt,sequence,publish_ns,accepted\n", publications);
	}
	edge_tool::NotificationMetrics metrics(
	        "producer", edge_tool::bench_notification_name(a.notification), N, a.samples, a.rate_hz);
	const int64_t deadline_ms = a.max_time_ms > 0 ? edge_tool::monotonic_ms_now() +
	                                                        static_cast<int64_t>(a.max_time_ms)
	                                              : 0;
	const uint64_t start_cpu = cpu_us_now();

	edge_tool::RatePacer pacer(a.rate_hz);
	pacer.start();
	uint64_t published = 0;
	for (;;) {
		if (published > 0) pacer.wait_until_next();
		const uint64_t publish_ns = monotonic_raw_now_ns();
		bench::BenchPayloadV1<N> v{};
		v.counter = published;
		v.publish_ns = publish_ns;
		++metrics.attempted;
		auto r = p.value().publish(v);
		if (publications != nullptr) {
			std::fprintf(publications, "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%d\n",
			             metrics.attempted, published + 1, publish_ns, r ? 1 : 0);
		}
		if (!r) {
			if (publications != nullptr) std::fclose(publications);
			std::printf("PUBLISH_FAIL code=%s published=%" PRIu64 "\n",
			            edge_runtime::to_string(r.error().code), published);
			std::fflush(stdout);
			return 3;
		}
		++published;
		metrics.accepted = published;
		metrics.final_sequence = published;
		if (a.samples > 0 && published >= a.samples) break;
		if (deadline_ms != 0 && edge_tool::monotonic_ms_now() >= deadline_ms) break;
	}

	const uint64_t end_cpu = cpu_us_now();
	metrics.timeout = a.samples > published ? 1 : 0;
	metrics.finish();
	if (publications != nullptr && std::fclose(publications) != 0) return 3;
	if (a.notification == edge_tool::BenchNotification::kEventfd) {
		const int notification_error = p.value().event_notification_error();
		if (notification_error != 0) {
			std::printf("NOTIFICATION_DELIVERY_FAIL errno=%d published=%" PRIu64 "\n",
			            notification_error, published);
			std::fflush(stdout);
			return 3;
		}
	}
	std::printf("DONE published=%" PRIu64 " cpu_us=%" PRIu64 "\n", published,
	            end_cpu - start_cpu);
	std::fflush(stdout);
	return 0;
}

}

int main(int argc, char** argv) {
	const char* name = edge_tool::arg_value(argc, argv, "--name");
	if (edge_tool::arg_flag(argc, argv, "--help") || name == nullptr || *name == '\0') {
		std::fprintf(stderr,
		             "usage: self_ipcs_shm_bench_producer --name <name> --payload <bytes> "
		             "[--samples N] [--max-time-ms T] [--rate hz] "
		             "[--wait-consumer-ms N] [--notification futex|eventfd] [--ready-fd FD] "
		             "[--publish-csv path] [--checksum 1]\n");
		return edge_tool::arg_flag(argc, argv, "--help") ? 0 : 2;
	}
	Args a;
	a.name = name;
	a.payload = static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--payload", 64));
	a.samples = edge_tool::arg_u64(argc, argv, "--samples", 1000000);
	a.max_time_ms = edge_tool::arg_u64(argc, argv, "--max-time-ms", 60000);
	a.rate_hz = edge_tool::arg_u64(argc, argv, "--rate", 0);
	a.wait_consumer_ms = edge_tool::arg_u64(argc, argv, "--wait-consumer-ms", 0);
	a.notification = edge_tool::bench_notification_from_args(argc, argv);
	a.ready_fd = edge_tool::bench_ready_fd_from_args(argc, argv);
	a.checksum = edge_tool::mandatory_payload_checksum(argc, argv);
	if (const char* path = arg_value(argc, argv, "--publish-csv")) a.publish_csv = path;

	const edge_runtime::SchemaDescriptor schema{bench::bench_fingerprint(a.payload), 1,
	                                            "BenchPayloadV1"};

	switch (a.payload) {
		case 64:
			return run_producer_n<64>(a, schema);
		case 256:
			return run_producer_n<256>(a, schema);
		case 1024:
			return run_producer_n<1024>(a, schema);
		case 4096:
			return run_producer_n<4096>(a, schema);
		case 16384:
			return run_producer_n<16384>(a, schema);
		case 65536:
			return run_producer_n<65536>(a, schema);
		default:
			std::fprintf(
			        stderr,
			        "unsupported --payload %u (need 64|256|1024|4096|16384|65536)\n",
			        a.payload);
			return 2;
	}
}
