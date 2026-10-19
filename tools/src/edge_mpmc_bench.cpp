// Cross-process MPMC baseline benchmark. The parent creates the queue and fork/execs this
// executable for every producer and consumer; no thread-only measurement path is used.
#include <fcntl.h>
#include <linux/perf_event.h>
#include <poll.h>
#include <signal.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cinttypes>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>
#include <vector>

#include "edge_runtime/common/error.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime_tool/mpmc_bench_payload.hpp"
#include "edge_runtime_tool/tool_common.hpp"

namespace {

using edge_runtime::ErrorCode;

constexpr uint32_t kReportMagic = 0x45524D42u;  // "ERMB"
constexpr uint16_t kReportVersion = 2;
constexpr uint32_t kNoErrorCode = std::numeric_limits<uint32_t>::max();
constexpr uint32_t kMaxWorkers = 16;
constexpr uint64_t kMaxMessagesPerProducer = 10000000;
constexpr uint32_t kMaxRuns = 1000;

enum class ChildRole : uint16_t {
	kProducer = 1,
	kConsumer = 2,
};

enum class RetryPolicy : uint16_t {
	kYield = 1,
	kSpin = 2,
};

enum class WaitPolicy : uint16_t {
	kBusy = 1,
	kBlocking = 2,
};

const char* retry_policy_name(RetryPolicy policy) noexcept {
	return policy == RetryPolicy::kSpin ? "spin" : "yield";
}

bool parse_retry_policy(const char* value, RetryPolicy* policy) noexcept {
	if (value == nullptr || std::strcmp(value, "yield") == 0) {
		*policy = RetryPolicy::kYield;
		return true;
	}
	if (std::strcmp(value, "spin") == 0) {
		*policy = RetryPolicy::kSpin;
		return true;
	}
	return false;
}

bool parse_retry_policy_args(int argc, char** argv, RetryPolicy* policy) {
	if (parse_retry_policy(edge_tool::arg_value(argc, argv, "--retry-policy"), policy)) {
		return true;
	}
	std::fprintf(stderr, "--retry-policy must be yield or spin\n");
	return false;
}

const char* wait_policy_name(WaitPolicy policy) noexcept {
	return policy == WaitPolicy::kBlocking ? "blocking" : "busy";
}

bool parse_wait_policy(const char* value, WaitPolicy* policy) noexcept {
	if (value == nullptr || std::strcmp(value, "busy") == 0) {
		*policy = WaitPolicy::kBusy;
		return true;
	}
	if (std::strcmp(value, "blocking") == 0) {
		*policy = WaitPolicy::kBlocking;
		return true;
	}
	return false;
}

bool parse_wait_policy_args(int argc, char** argv, WaitPolicy* policy) {
	if (parse_wait_policy(edge_tool::arg_value(argc, argv, "--wait-policy"), policy)) {
		return true;
	}
	std::fprintf(stderr, "--wait-policy must be busy or blocking\n");
	return false;
}

void retry_after(RetryPolicy policy) noexcept {
	if (policy == RetryPolicy::kYield) (void)::sched_yield();
}

struct ReportHeader {
	uint32_t magic = kReportMagic;
	uint16_t version = kReportVersion;
	uint16_t role = 0;
	uint64_t published = 0;
	uint64_t delivered = 0;
	uint64_t queue_full = 0;
	uint64_t queue_empty = 0;
	uint64_t queue_contention = 0;
	uint64_t recovery = 0;
	uint64_t errors = 0;
	uint64_t invalid_payload = 0;
	uint32_t first_error_code = kNoErrorCode;
	uint32_t reserved = 0;
	uint64_t record_count = 0;
	uint64_t wait_calls = 0;
	uint64_t wait_timeouts = 0;
	uint32_t perf_available = 0;
	uint32_t reserved2 = 0;
	uint64_t perf_context_switches = 0;
};

struct DeliveryRecord {
	uint32_t producer = 0;
	uint32_t reserved = 0;
	uint64_t sequence = 0;
	uint64_t latency_ns = 0;
};

static_assert(std::is_trivially_copyable_v<ReportHeader>);
static_assert(std::is_trivially_copyable_v<DeliveryRecord>);

struct Config {
	size_t payload_size = 64;
	uint32_t producers = 1;
	uint32_t consumers = 1;
	uint64_t messages = 1000;
	uint32_t capacity = 64;
	uint32_t runs = 1;
	uint64_t timeout_ms = 30000;
	uint64_t test_consumer_hold_ms = 0;
	uint64_t test_consumer_delay_us = 0;
	bool test_ignore_term_consumer = false;
	bool test_consumer_stop = false;
	bool keep_worker_reports = false;
	RetryPolicy retry_policy = RetryPolicy::kYield;
	WaitPolicy wait_policy = WaitPolicy::kBusy;
	std::string out_dir;
	std::string ready_file;
	std::string test_retry_ready_file;
};

struct ChildProcess {
	pid_t pid = -1;
	ChildRole role = ChildRole::kProducer;
	std::string report_path;
	uint64_t expected_records = 0;
	bool waited = false;
	int status = 0;
	struct rusage usage {};
};

struct ParsedReport {
	ReportHeader header{};
	std::vector<DeliveryRecord> records;
	bool valid = false;
};

struct RunAggregate {
	uint64_t expected = 0;
	uint64_t published = 0;
	uint64_t delivered = 0;
	uint64_t queue_full = 0;
	uint64_t queue_empty = 0;
	uint64_t queue_contention = 0;
	uint64_t recovery = 0;
	uint64_t errors = 0;
	uint64_t wall_us = 0;
	uint64_t user_cpu_us = 0;
	uint64_t system_cpu_us = 0;
	uint64_t wait_calls = 0;
	uint64_t wait_timeouts = 0;
	uint64_t perf_context_switches = 0;
	bool perf_available = true;
	bool timed_out = false;
	bool ready_failed = false;
	bool child_kill_sent = false;
	bool correctness = false;
	std::vector<uint64_t> latency_ns;
};

volatile sig_atomic_t g_stop_requested = 0;

void stop_signal_handler(int) noexcept { g_stop_requested = 1; }

bool install_stop_signal_handler() noexcept {
	struct sigaction action {};
	action.sa_handler = stop_signal_handler;
	if (::sigemptyset(&action.sa_mask) != 0) return false;
	action.sa_flags = 0;
	return ::sigaction(SIGTERM, &action, nullptr) == 0 &&
	       ::sigaction(SIGINT, &action, nullptr) == 0;
}

uint64_t monotonic_ns() noexcept {
	return edge_tool::monotonic_raw_now_ns();
}

uint64_t rusage_user_us(const struct rusage& usage) noexcept {
	return static_cast<uint64_t>(usage.ru_utime.tv_sec) * 1000000ull +
	       static_cast<uint64_t>(usage.ru_utime.tv_usec);
}

uint64_t rusage_system_us(const struct rusage& usage) noexcept {
	return static_cast<uint64_t>(usage.ru_stime.tv_sec) * 1000000ull +
	       static_cast<uint64_t>(usage.ru_stime.tv_usec);
}

bool write_full(int fd, const void* data, size_t size) noexcept {
	const auto* bytes = static_cast<const unsigned char*>(data);
	while (size != 0) {
		const ssize_t written = ::write(fd, bytes, size);
		if (written > 0) {
			bytes += written;
			size -= static_cast<size_t>(written);
			continue;
		}
		if (written < 0 && errno == EINTR) continue;
		return false;
	}
	return true;
}

void report_ready(int fd, char value) noexcept {
	if (fd < 0) return;
	(void)write_full(fd, &value, sizeof(value));
	(void)::close(fd);
}

bool read_full(int fd, void* data, size_t size) noexcept {
	auto* bytes = static_cast<unsigned char*>(data);
	while (size != 0) {
		const ssize_t read_count = ::read(fd, bytes, size);
		if (read_count > 0) {
			bytes += read_count;
			size -= static_cast<size_t>(read_count);
			continue;
		}
		if (read_count < 0 && errno == EINTR) continue;
		return false;
	}
	return true;
}

bool mkdirs(const std::string& path) {
	if (path.empty()) return true;
	std::string current;
	for (size_t i = 0; i < path.size(); ++i) {
		current.push_back(path[i]);
		if (path[i] != '/' && i + 1 != path.size()) continue;
		if (current == "/" || current.empty()) continue;
		if (::mkdir(current.c_str(), 0755) != 0 && errno != EEXIST) return false;
	}
	return true;
}

std::string join_path(const std::string& directory, const std::string& file) {
	if (directory.empty()) return file;
	if (directory.back() == '/') return directory + file;
	return directory + "/" + file;
}

std::string self_executable() {
	char path[4096]{};
	const ssize_t length = ::readlink("/proc/self/exe", path, sizeof(path) - 1);
	if (length <= 0) return {};
	path[length] = '\0';
	return path;
}

std::string make_queue_name(uint32_t run_index) {
	return "er_mpmc_bench_" + std::to_string(static_cast<unsigned long>(::getpid())) + "_" +
	       std::to_string(run_index) + "_" +
	       std::to_string(static_cast<unsigned long long>(monotonic_ns()));
}

std::string make_report_path(const Config& config, uint32_t run_index, uint32_t child_index) {
	return (config.keep_worker_reports ? config.out_dir : "/tmp") + "/er_mpmc_bench_" +
	       std::to_string(static_cast<unsigned long>(::getpid())) + "_" +
	       std::to_string(run_index) + "_" + std::to_string(child_index) + ".report";
}

const char* child_role_name(ChildRole role) noexcept {
	return role == ChildRole::kProducer ? "producer" : "consumer";
}

void note_error(ReportHeader* header, ErrorCode code) noexcept {
	++header->errors;
	if (code == ErrorCode::kRecoveryBlocked) ++header->recovery;
	if (header->first_error_code == kNoErrorCode) {
		header->first_error_code = static_cast<uint32_t>(code);
	}
}

void note_worker_error(ReportHeader* header, const edge_runtime::Error& error,
				ChildRole role, const char* phase) noexcept {
	note_error(header, error.code);
	std::fprintf(stderr,
			"MPMC_WORKER_ERROR pid=%ld role=%s phase=%s code=%s operation=%s context=%s\n",
			static_cast<long>(::getpid()), child_role_name(role), phase,
			edge_runtime::to_string(error.code), error.operation == nullptr ? "" : error.operation,
			error.context);
	std::fflush(stderr);
}

class PerfCounter {
       public:
	PerfCounter() = default;
	PerfCounter(const PerfCounter&) = delete;
	PerfCounter& operator=(const PerfCounter&) = delete;

	void start() noexcept {
		struct perf_event_attr attr {};
		attr.type = PERF_TYPE_SOFTWARE;
		attr.size = static_cast<__u32>(sizeof(attr));
		attr.config = PERF_COUNT_SW_CONTEXT_SWITCHES;
		attr.disabled = 1;
		fd_ = static_cast<int>(::syscall(SYS_perf_event_open, &attr, 0, -1, -1, 0));
		if (fd_ < 0) return;
		if (::ioctl(fd_, PERF_EVENT_IOC_RESET, 0) != 0 ||
		    ::ioctl(fd_, PERF_EVENT_IOC_ENABLE, 0) != 0) {
			::close(fd_);
			fd_ = -1;
			return;
		}
		available_ = true;
	}

	void stop() noexcept {
		if (fd_ < 0) return;
		(void)::ioctl(fd_, PERF_EVENT_IOC_DISABLE, 0);
		uint64_t value = 0;
		if (::read(fd_, &value, sizeof(value)) == static_cast<ssize_t>(sizeof(value))) {
			value_ = value;
		} else {
			available_ = false;
		}
		::close(fd_);
		fd_ = -1;
	}

	bool available() const noexcept { return available_; }
	uint64_t value() const noexcept { return value_; }

       private:
	int fd_ = -1;
	bool available_ = false;
	uint64_t value_ = 0;
};

bool write_report(const std::string& path, ReportHeader header,
                  const std::vector<DeliveryRecord>& records) {
	header.record_count = static_cast<uint64_t>(records.size());
	const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
	if (fd < 0) return false;
	bool ok = write_full(fd, &header, sizeof(header));
	if (ok && !records.empty()) {
		ok = write_full(fd, records.data(), records.size() * sizeof(records[0]));
	}
	if (::close(fd) != 0) ok = false;
	return ok;
}

ParsedReport read_report(const std::string& path, ChildRole expected_role,
                         uint64_t expected_records) {
	ParsedReport report;
	const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
	if (fd < 0) return report;
	if (!read_full(fd, &report.header, sizeof(report.header))) {
		::close(fd);
		return report;
	}
	const uint16_t expected_role_value = static_cast<uint16_t>(expected_role);
	const bool role_shape_ok =
	        expected_role == ChildRole::kProducer
	                ? report.header.record_count == 0 && report.header.delivered == 0
	                : report.header.published == 0 &&
	                          report.header.record_count == report.header.delivered;
	if (report.header.magic != kReportMagic || report.header.version != kReportVersion ||
	    report.header.role != expected_role_value || report.header.reserved != 0 ||
	    report.header.reserved2 != 0 || report.header.perf_available > 1 ||
	    report.header.record_count > expected_records || !role_shape_ok ||
	    report.header.record_count > std::numeric_limits<size_t>::max() / sizeof(DeliveryRecord)) {
		::close(fd);
		return report;
	}
	report.records.resize(static_cast<size_t>(report.header.record_count));
	if (!report.records.empty() &&
	    !read_full(fd, report.records.data(), report.records.size() * sizeof(report.records[0]))) {
		::close(fd);
		return report;
	}
	struct stat file_stat {};
	bool size_ok =
	        ::fstat(fd, &file_stat) == 0 && S_ISREG(file_stat.st_mode) &&
	        static_cast<uint64_t>(file_stat.st_size) ==
	                sizeof(report.header) + report.header.record_count * sizeof(DeliveryRecord);
	if (size_ok) {
		for (const auto& record : report.records) {
			if (record.reserved != 0) {
				size_ok = false;
				break;
			}
		}
	}
	::close(fd);
	report.valid = size_ok;
	return report;
}

bool prepare_report_path(const std::string& path) noexcept {
	return ::unlink(path.c_str()) == 0 || errno == ENOENT;
}

template <size_t kSize>
int run_child_producer(int argc, char** argv) {
	if (!install_stop_signal_handler()) return 2;
	const char* name = edge_tool::arg_value(argc, argv, "--name");
	const char* report_path = edge_tool::arg_value(argc, argv, "--report-file");
	if (name == nullptr || report_path == nullptr) return 2;
	const int ready_fd = static_cast<int>(edge_tool::arg_u64(argc, argv, "--ready-fd", 0));
	const uint32_t capacity =
	        static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--capacity", 0));
	const uint32_t producer_id =
	        static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--producer-id", 0));
	const char* test_retry_ready_file = edge_tool::arg_value(argc, argv, "--test-retry-ready-file");
	const uint64_t messages = edge_tool::arg_u64(argc, argv, "--messages", 0);
	const uint64_t wait_timeout_ms = edge_tool::arg_u64(argc, argv, "--wait-timeout-ms", 30000);
	RetryPolicy retry_policy;
	if (!parse_retry_policy_args(argc, argv, &retry_policy)) return 2;
	WaitPolicy wait_policy;
	if (!parse_wait_policy_args(argc, argv, &wait_policy)) return 2;
	ReportHeader header;
	header.role = static_cast<uint16_t>(ChildRole::kProducer);
	PerfCounter perf;
	perf.start();
	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = capacity;
	auto queue = edge_runtime::MpmcQueue<edge_mpmc_bench::Payload<kSize>>::open(
	        options, edge_mpmc_bench::schema<kSize>());
	bool success = queue.has_value();
	if (!queue) {
		report_ready(ready_fd, 'E');
		note_worker_error(&header, queue.error(), ChildRole::kProducer, "open");
	}
	if (queue) {
		report_ready(ready_fd, 'R');
		for (uint64_t sequence = 0; sequence < messages && !g_stop_requested;) {
			const auto payload =
			        edge_mpmc_bench::make_payload<kSize>(producer_id, sequence, monotonic_ns());
			for (;;) {
				if (g_stop_requested) {
					success = false;
					break;
				}
				auto pushed = queue.value().try_push(payload);
				// Test-only rendezvous: the payload already exists and a real Full was observed.
				if (test_retry_ready_file != nullptr && !pushed &&
				    pushed.error().code == ErrorCode::kQueueFull) {
					FILE* ready = std::fopen(test_retry_ready_file, "wx");
					bool ok = ready != nullptr;
					if (ready != nullptr) {
						ok = std::fprintf(ready, "RETRY_READY pid=%ld sequence=%" PRIu64 "\n",
						                  static_cast<long>(::getpid()), sequence) >= 0;
						if (std::fclose(ready) != 0) ok = false;
					}
					if (!ok) {
						note_error(&header, ErrorCode::kSystemError);
						success = false;
						sequence = messages;
						break;
					}
					test_retry_ready_file = nullptr;
				}
				if (g_stop_requested) {
					success = false;
					break;
				}
				if (!pushed && wait_policy == WaitPolicy::kBlocking &&
				    (pushed.error().code == ErrorCode::kQueueFull ||
				     pushed.error().code == ErrorCode::kQueueContention)) {
					if (pushed.error().code == ErrorCode::kQueueFull) ++header.queue_full;
					if (pushed.error().code == ErrorCode::kQueueContention) ++header.queue_contention;
					++header.wait_calls;
					const auto deadline = std::chrono::steady_clock::now() +
					                      std::chrono::milliseconds(wait_timeout_ms);
					pushed = queue.value().wait_push_until(payload, deadline);
					if (!pushed && pushed.error().code == ErrorCode::kTimeout) {
						++header.wait_timeouts;
					}
				}
				if (pushed) {
					++sequence;
					++header.published;
					break;
				}
				if (wait_policy == WaitPolicy::kBlocking &&
				    pushed.error().code == ErrorCode::kTimeout) {
					note_worker_error(&header, pushed.error(), ChildRole::kProducer, "push");
					success = false;
					sequence = messages;
					break;
				}
				switch (pushed.error().code) {
				case ErrorCode::kQueueFull:
					++header.queue_full;
					retry_after(retry_policy);
					break;
				case ErrorCode::kQueueContention:
					++header.queue_contention;
					retry_after(retry_policy);
					break;
				default:
					note_worker_error(&header, pushed.error(), ChildRole::kProducer, "push");
					success = false;
					sequence = messages;
					break;
				}
				if (!success || sequence == messages) break;
			}
		}
	}
	perf.stop();
	header.perf_available = perf.available() ? 1u : 0u;
	header.perf_context_switches = perf.value();
	if (!write_report(report_path, header, {})) return 5;
	return success ? 0 : 3;
}

template <size_t kSize>
int run_child_consumer(int argc, char** argv) {
	if (!install_stop_signal_handler()) return 2;
	const char* name = edge_tool::arg_value(argc, argv, "--name");
	const char* report_path = edge_tool::arg_value(argc, argv, "--report-file");
	if (name == nullptr || report_path == nullptr) return 2;
	const int ready_fd = static_cast<int>(edge_tool::arg_u64(argc, argv, "--ready-fd", 0));
	const uint32_t capacity =
	        static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--capacity", 0));
	const uint32_t producer_count =
	        static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--producer-count", 0));
	const uint64_t messages = edge_tool::arg_u64(argc, argv, "--messages", 0);
	const uint64_t target = edge_tool::arg_u64(argc, argv, "--consumer-target", 0);
	const uint64_t wait_timeout_ms = edge_tool::arg_u64(argc, argv, "--wait-timeout-ms", 30000);
	const uint64_t test_consumer_hold_ms =
	        edge_tool::arg_u64(argc, argv, "--test-consumer-hold-ms", 0);
	const uint64_t test_consumer_delay_us =
	        edge_tool::arg_u64(argc, argv, "--test-consumer-delay-us", 0);
	const bool ignore_term = edge_tool::arg_flag(argc, argv, "--test-ignore-term-consumer");
	if (ignore_term && ::signal(SIGTERM, SIG_IGN) == SIG_ERR) return 2;
	RetryPolicy retry_policy;
	if (!parse_retry_policy_args(argc, argv, &retry_policy)) return 2;
	WaitPolicy wait_policy;
	if (!parse_wait_policy_args(argc, argv, &wait_policy)) return 2;
	ReportHeader header;
	header.role = static_cast<uint16_t>(ChildRole::kConsumer);
	std::vector<DeliveryRecord> records;
	records.reserve(static_cast<size_t>(target));
	PerfCounter perf;
	perf.start();
	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = capacity;
	auto queue = edge_runtime::MpmcQueue<edge_mpmc_bench::Payload<kSize>>::open(
	        options, edge_mpmc_bench::schema<kSize>());
	bool success = queue.has_value();
	if (!queue) {
		report_ready(ready_fd, 'E');
		note_worker_error(&header, queue.error(), ChildRole::kConsumer, "open");
	}
	if (queue) {
		report_ready(ready_fd, 'R');
		if (edge_tool::arg_flag(argc, argv, "--test-consumer-stop") && ::raise(SIGSTOP) != 0) {
			return 2;
		}
		if (test_consumer_hold_ms != 0) {
			::usleep(static_cast<useconds_t>(test_consumer_hold_ms * 1000ull));
		}
		for (uint64_t delivered = 0; delivered < target && !g_stop_requested;) {
			if (g_stop_requested) {
				success = false;
				break;
			}
			auto popped = queue.value().try_pop();
			if (g_stop_requested) {
				success = false;
				break;
			}
			if (!popped && wait_policy == WaitPolicy::kBlocking &&
			    (popped.error().code == ErrorCode::kQueueEmpty ||
			     popped.error().code == ErrorCode::kQueueContention)) {
				if (popped.error().code == ErrorCode::kQueueEmpty) ++header.queue_empty;
				if (popped.error().code == ErrorCode::kQueueContention) ++header.queue_contention;
				++header.wait_calls;
				const auto deadline = std::chrono::steady_clock::now() +
				                      std::chrono::milliseconds(wait_timeout_ms);
				popped = queue.value().wait_pop_until(deadline);
				if (!popped && popped.error().code == ErrorCode::kTimeout) {
					++header.wait_timeouts;
				}
			}
			if (popped) {
				const uint64_t receive_ns = monotonic_ns();
				uint64_t latency_ns = 0;
				if (!edge_mpmc_bench::valid_payload(popped.value(), producer_count, messages,
				                                    receive_ns, &latency_ns)) {
					++header.invalid_payload;
					note_error(&header, ErrorCode::kPayloadCorrupt);
					success = false;
					break;
				}
				records.push_back(
				        {popped.value().producer, 0, popped.value().sequence, latency_ns});
				++delivered;
				++header.delivered;
				if (test_consumer_delay_us != 0) {
					::usleep(static_cast<useconds_t>(test_consumer_delay_us));
				}
				continue;
			}
			if (wait_policy == WaitPolicy::kBlocking &&
			    popped.error().code == ErrorCode::kTimeout) {
				note_worker_error(&header, popped.error(), ChildRole::kConsumer, "pop");
				success = false;
				delivered = target;
				continue;
			}
			switch (popped.error().code) {
			case ErrorCode::kQueueEmpty:
				++header.queue_empty;
				retry_after(retry_policy);
				break;
			case ErrorCode::kQueueContention:
				++header.queue_contention;
				retry_after(retry_policy);
				break;
			default:
				note_worker_error(&header, popped.error(), ChildRole::kConsumer, "pop");
				success = false;
				delivered = target;
				break;
			}
		}
	}
	perf.stop();
	header.perf_available = perf.available() ? 1u : 0u;
	header.perf_context_switches = perf.value();
	if (!write_report(report_path, header, records)) return 5;
	return success ? 0 : 4;
}

template <size_t kSize>
int run_child(int argc, char** argv, bool producer) {
	return producer ? run_child_producer<kSize>(argc, argv)
	                : run_child_consumer<kSize>(argc, argv);
}

pid_t spawn_child(const std::string& executable, const std::vector<std::string>& arguments) {
	const pid_t pid = ::fork();
	if (pid != 0) return pid;
	std::vector<char*> child_argv;
	child_argv.reserve(arguments.size() + 1);
	for (const auto& argument : arguments) {
		child_argv.push_back(const_cast<char*>(argument.c_str()));
	}
	child_argv.push_back(nullptr);
	::execv(executable.c_str(), child_argv.data());
	::_exit(127);
}

bool wait_for_ready(int fd, size_t expected, uint64_t timeout_ms, bool* failed) {
	*failed = false;
	if (expected == 0) return true;
	size_t ready = 0;
	const uint64_t deadline = monotonic_ns() + timeout_ms * 1000000ull;
	while (ready < expected) {
		if (g_stop_requested != 0) return false;
		const uint64_t now = monotonic_ns();
		if (now >= deadline) return false;
		const uint64_t remaining_ms = (deadline - now + 999999ull) / 1000000ull;
		struct pollfd descriptor {fd, POLLIN | POLLHUP, 0};
		const int poll_result = ::poll(&descriptor, 1,
		                              static_cast<int>(std::min<uint64_t>(remaining_ms, 50)));
		if (poll_result < 0) {
			if (errno == EINTR) continue;
			return false;
		}
		if (poll_result == 0) continue;
		char values[32]{};
		const ssize_t count = ::read(fd, values, sizeof(values));
		if (count > 0) {
			for (ssize_t i = 0; i < count; ++i) {
				if (values[i] == 'R') {
					++ready;
				} else if (values[i] == 'E') {
					*failed = true;
				}
			}
			if (*failed) return false;
			continue;
		}
		if (count == 0) return false;
		if (errno != EINTR) return false;
	}
	return true;
}

bool write_ready_file(const Config& config, const std::string& resource_name,
	                      uint32_t run_index, const std::vector<ChildProcess>& children) {
	if (config.ready_file.empty()) return true;
	FILE* file = std::fopen(config.ready_file.c_str(), "w");
	if (file == nullptr) return false;
	bool ok = std::fprintf(file, "READY run=%" PRIu32 " resource=%s\n", run_index,
	                       resource_name.c_str()) >= 0;
	for (const auto& child : children) {
		if (!ok) break;
		ok = std::fprintf(file, "ROLE role=%s pid=%ld report=%s\n",
		                  child_role_name(child.role), static_cast<long>(child.pid),
		                  child.report_path.c_str()) >= 0;
	}
	if (std::fclose(file) != 0) ok = false;
	return ok;
}

bool reap_children(std::vector<ChildProcess>* children, uint64_t timeout_ms, bool* timed_out,
	                   bool* kill_sent) {
	const uint64_t normal_deadline = monotonic_ns() + timeout_ms * 1000000ull;
	uint64_t deadline = normal_deadline;
	bool stop_sent = false;
	*kill_sent = false;
	size_t remaining = children->size();
	while (remaining != 0) {
		for (auto& child : *children) {
			if (child.waited) continue;
			int status = 0;
			struct rusage usage {};
			const pid_t waited = ::wait4(child.pid, &status, WNOHANG, &usage);
			if (waited == child.pid) {
				child.status = status;
				child.usage = usage;
				child.waited = true;
				--remaining;
			} else if (waited < 0 && errno != EINTR) {
				child.waited = true;
				child.status = 127 << 8;
				--remaining;
			}
		}
		if (remaining == 0) return true;
		if (!stop_sent && (g_stop_requested != 0 || monotonic_ns() >= normal_deadline)) {
			*timed_out = true;
			stop_sent = true;
			deadline = monotonic_ns() + 1000ull * 1000000ull;
			for (auto& child : *children) {
				if (child.waited) continue;
				(void)::kill(child.pid, SIGCONT);
				(void)::kill(child.pid, SIGTERM);
			}
		}
		if (stop_sent && monotonic_ns() >= deadline) {
			for (auto& child : *children) {
				if (child.waited) continue;
				if (::kill(child.pid, SIGKILL) == 0) *kill_sent = true;
			}
			for (auto& child : *children) {
				if (child.waited) continue;
				int status = 0;
				struct rusage usage {};
				while (::wait4(child.pid, &status, 0, &usage) < 0 && errno == EINTR) {
				}
				child.status = status;
				child.usage = usage;
				child.waited = true;
			}
			return false;
		}
		::usleep(1000);
	}
	return true;
}

bool child_succeeded(const ChildProcess& child) noexcept {
	return child.waited && WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0;
}

template <size_t kSize>
bool remove_and_verify_queue(edge_runtime::MpmcQueue<edge_mpmc_bench::Payload<kSize>>* queue,
                             const edge_runtime::MpmcQueueOptions& options, uint32_t run_index,
                             bool stopped, bool child_kill_sent) {
	const auto removed = queue->remove_if_creator();
	auto reopened = edge_runtime::MpmcQueue<edge_mpmc_bench::Payload<kSize>>::open(
	        options, edge_mpmc_bench::schema<kSize>());
	const bool absent = !reopened && reopened.error().code == ErrorCode::kNotFound;
	std::printf("CLEANUP run=%" PRIu32
	            " resource=mpmc name=%s remove=%s reopen=%s stopped=%s child_kill=%s\n",
	            run_index, options.name.c_str(), removed ? "SUCCESS" : "FAIL",
	            reopened ? "FOUND" : edge_runtime::to_string(reopened.error().code),
	            stopped ? "true" : "false", child_kill_sent ? "true" : "false");
	std::fflush(stdout);
	return removed && absent;
}

uint64_t percentile_nearest_rank(const std::vector<uint64_t>& sorted, uint32_t numerator,
                                 uint32_t denominator) noexcept {
	if (sorted.empty()) return 0;
	const uint64_t count = static_cast<uint64_t>(sorted.size());
	uint64_t rank = (count * numerator + denominator - 1) / denominator;
	if (rank == 0) rank = 1;
	if (rank > count) rank = count;
	return sorted[static_cast<size_t>(rank - 1)];
}

bool write_latency_csv(const std::string& out_dir, uint32_t run_index, size_t payload_size,
                       uint32_t producers, uint32_t consumers, RetryPolicy retry_policy,
                       WaitPolicy wait_policy, const std::vector<DeliveryRecord>& records) {
	const std::string filename =
	        "mpmc_bench_" + std::to_string(static_cast<unsigned long>(::getpid())) +
	        "_run_" + std::to_string(run_index) + "_payload_" +
	        std::to_string(payload_size) + "_" + std::to_string(producers) + "P" +
	       std::to_string(consumers) + "C_" + retry_policy_name(retry_policy) + "_" +
	       wait_policy_name(wait_policy) + ".csv";
	const std::string path = join_path(out_dir, filename);
	FILE* file = std::fopen(path.c_str(), "w");
	if (file == nullptr) return false;
	bool ok = std::fprintf(file, "producer,sequence,latency_ns\n") >= 0;
	for (const auto& record : records) {
		if (!ok) break;
		ok = std::fprintf(file, "%" PRIu32 ",%" PRIu64 ",%" PRIu64 "\n", record.producer,
		                  record.sequence, record.latency_ns) >= 0;
	}
	if (std::fclose(file) != 0) ok = false;
	return ok;
}

void print_summary(uint32_t run_index, size_t payload_size, const Config& config,
                   const RunAggregate& aggregate) {
	std::vector<uint64_t> sorted = aggregate.latency_ns;
	std::sort(sorted.begin(), sorted.end());
	std::printf(
	        "RESULT run=%" PRIu32
	        " label=VM_ONLY retry_policy=%s wait_policy=%s payload_bytes=%zu producers=%" PRIu32
	        " consumers=%" PRIu32
	        " messages_per_producer=%" PRIu64 " capacity=%" PRIu32
	        " test_consumer_hold_ms=%" PRIu64 " test_consumer_delay_us=%" PRIu64
	        " expected=%" PRIu64
	        " published=%" PRIu64 " delivered=%" PRIu64 " queue_full=%" PRIu64
	        " queue_empty=%" PRIu64 " queue_contention=%" PRIu64 " recovery=%" PRIu64
	        " errors=%" PRIu64 " wait_calls=%" PRIu64 " wait_timeouts=%" PRIu64
	        " wall_us=%" PRIu64 " user_cpu_us=%" PRIu64
	        " system_cpu_us=%" PRIu64 " cpu_total_us=%" PRIu64 " p50_ns=",
	        run_index, retry_policy_name(config.retry_policy), wait_policy_name(config.wait_policy),
	        payload_size, config.producers,
	        config.consumers, config.messages,
	        config.capacity, config.test_consumer_hold_ms, config.test_consumer_delay_us,
	        aggregate.expected, aggregate.published,
	        aggregate.delivered, aggregate.queue_full, aggregate.queue_empty, aggregate.queue_contention,
	        aggregate.recovery, aggregate.errors, aggregate.wait_calls, aggregate.wait_timeouts,
	        aggregate.wall_us, aggregate.user_cpu_us,
	        aggregate.system_cpu_us, aggregate.user_cpu_us + aggregate.system_cpu_us);
	if (sorted.empty()) {
		std::printf("NA p95_ns=NA p99_ns=NA max_ns=NA ");
	} else {
		std::printf("%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 " max_ns=%" PRIu64 " ",
		            percentile_nearest_rank(sorted, 50, 100),
		            percentile_nearest_rank(sorted, 95, 100),
		            percentile_nearest_rank(sorted, 99, 100), sorted.back());
	}
	if (aggregate.perf_available) {
		std::printf("perf_context_switches=%" PRIu64 " ", aggregate.perf_context_switches);
	} else {
		std::printf("perf_context_switches=UNAVAILABLE ");
	}
	std::printf("spsc_missed_samples=NOT_APPLICABLE correctness=%s\n",
	            aggregate.correctness ? "PASS" : "FAIL");
	std::fflush(stdout);
}

template <size_t kSize>
int run_parent_once(const std::string& executable, const Config& config, uint32_t run_index) {
	if (config.keep_worker_reports && !mkdirs(config.out_dir)) return 2;
	using Queue = edge_runtime::MpmcQueue<edge_mpmc_bench::Payload<kSize>>;
	const std::string queue_name = make_queue_name(run_index);
	edge_runtime::MpmcQueueOptions options;
	options.name = queue_name;
	options.capacity = config.capacity;
	auto queue = Queue::create(options, edge_mpmc_bench::schema<kSize>());
	if (!queue) {
		std::fprintf(stderr, "CREATE_FAIL run=%" PRIu32 " code=%s ctx=%s\n", run_index,
		             edge_runtime::to_string(queue.error().code), queue.error().context);
		return 2;
	}
	RunAggregate aggregate;
	aggregate.expected = config.messages * static_cast<uint64_t>(config.producers);
	const uint64_t wall_start = monotonic_ns();
	std::vector<ChildProcess> children;
	children.reserve(static_cast<size_t>(config.producers + config.consumers));
	int ready_pipe[2] = {-1, -1};
	if (::pipe(ready_pipe) != 0) {
		std::fprintf(stderr, "READY_PIPE_FAIL run=%" PRIu32 " errno=%d\n", run_index, errno);
		(void)remove_and_verify_queue(&queue.value(), options, run_index, false, false);
		return 2;
	}
	const int read_flags = ::fcntl(ready_pipe[0], F_GETFD);
	if (read_flags >= 0) (void)::fcntl(ready_pipe[0], F_SETFD, read_flags | FD_CLOEXEC);

	const uint64_t total_consumers = static_cast<uint64_t>(config.consumers);
	const uint64_t base_target = aggregate.expected / total_consumers;
	const uint64_t remainder = aggregate.expected % total_consumers;
	for (uint32_t i = 0; i < config.consumers; ++i) {
		const uint64_t target = base_target + (static_cast<uint64_t>(i) < remainder ? 1 : 0);
		const std::string report = make_report_path(config, run_index, i);
		if (!prepare_report_path(report)) {
			std::fprintf(stderr, "REPORT_PATH_FAIL run=%" PRIu32 " role=consumer path=%s errno=%d\n",
			             run_index, report.c_str(), errno);
			++aggregate.errors;
			continue;
		}
		std::vector<std::string> args = {
		        executable, "--role", "consumer", "--payload-size", std::to_string(kSize),
		        "--name", queue_name, "--capacity", std::to_string(config.capacity),
		        "--producer-count", std::to_string(config.producers),
		        "--messages", std::to_string(config.messages), "--consumer-target",
		        std::to_string(target), "--retry-policy", retry_policy_name(config.retry_policy),
		        "--wait-policy", wait_policy_name(config.wait_policy), "--wait-timeout-ms",
		        std::to_string(config.timeout_ms), "--test-consumer-hold-ms",
		        std::to_string(config.test_consumer_hold_ms), "--test-consumer-delay-us",
		        std::to_string(config.test_consumer_delay_us), "--ready-fd",
		        std::to_string(ready_pipe[1]), "--report-file", report};
		if (config.test_ignore_term_consumer) args.push_back("--test-ignore-term-consumer");
		if (config.test_consumer_stop) args.push_back("--test-consumer-stop");
		const pid_t pid = spawn_child(executable, args);
		if (pid < 0) {
			std::fprintf(stderr, "FORK_FAIL run=%" PRIu32 " role=consumer errno=%d\n", run_index,
			             errno);
			++aggregate.errors;
			break;
		}
		children.push_back({pid, ChildRole::kConsumer, report, target});
	}
	for (uint32_t i = 0; i < config.producers; ++i) {
		const std::string report = make_report_path(config, run_index, config.consumers + i);
		if (!prepare_report_path(report)) {
			std::fprintf(stderr, "REPORT_PATH_FAIL run=%" PRIu32 " role=producer path=%s errno=%d\n",
			             run_index, report.c_str(), errno);
			++aggregate.errors;
			continue;
		}
		std::vector<std::string> args = {
		        executable, "--role", "producer", "--payload-size", std::to_string(kSize),
		        "--name", queue_name, "--capacity", std::to_string(config.capacity),
		        "--producer-id", std::to_string(i), "--messages",
		        std::to_string(config.messages), "--retry-policy",
		        retry_policy_name(config.retry_policy), "--wait-policy",
		        wait_policy_name(config.wait_policy), "--wait-timeout-ms",
		        std::to_string(config.timeout_ms), "--ready-fd", std::to_string(ready_pipe[1]),
		        "--report-file", report};
		if (!config.test_retry_ready_file.empty()) {
			args.push_back("--test-retry-ready-file");
			args.push_back(config.test_retry_ready_file);
		}
		const pid_t pid = spawn_child(executable, args);
		if (pid < 0) {
			std::fprintf(stderr, "FORK_FAIL run=%" PRIu32 " role=producer errno=%d\n", run_index,
			             errno);
			++aggregate.errors;
			break;
		}
		children.push_back({pid, ChildRole::kProducer, report, 0});
	}
	(void)::close(ready_pipe[1]);
	ready_pipe[1] = -1;
	bool ready_failed = false;
	const bool ready = wait_for_ready(ready_pipe[0], children.size(), config.timeout_ms, &ready_failed);
	(void)::close(ready_pipe[0]);
	ready_pipe[0] = -1;
	aggregate.ready_failed = !ready || ready_failed;
	if (ready && !aggregate.ready_failed &&
	    !write_ready_file(config, queue_name, run_index, children)) {
		std::fprintf(stderr, "READY_FILE_FAIL run=%" PRIu32 " path=%s errno=%d\n", run_index,
		             config.ready_file.c_str(), errno);
		aggregate.ready_failed = true;
	}
	if (aggregate.ready_failed) g_stop_requested = 1;
	bool timed_out = false;
	bool child_kill_sent = false;
	const bool all_reaped =
	        reap_children(&children, config.timeout_ms, &timed_out, &child_kill_sent);
	aggregate.timed_out = !all_reaped || timed_out || aggregate.ready_failed ||
	                      g_stop_requested != 0;
	aggregate.child_kill_sent = child_kill_sent;

	std::vector<ParsedReport> reports;
	reports.reserve(children.size());
	for (const auto& child : children) {
		aggregate.user_cpu_us += rusage_user_us(child.usage);
		aggregate.system_cpu_us += rusage_system_us(child.usage);
		if (config.keep_worker_reports) {
			std::printf("WORKER run=%" PRIu32 " pid=%ld role=%s status=%d user_cpu_us=%" PRIu64
			            " system_cpu_us=%" PRIu64 " report=%s\n",
			            run_index, static_cast<long>(child.pid), child_role_name(child.role),
			            child.status, rusage_user_us(child.usage), rusage_system_us(child.usage),
			            child.report_path.c_str());
		}
		if (!child_succeeded(child)) ++aggregate.errors;
		reports.push_back(read_report(child.report_path, child.role, child.expected_records));
	}
	for (size_t i = 0; i < children.size(); ++i) {
		const ParsedReport& report = reports[i];
		if (!report.valid) {
			++aggregate.errors;
			aggregate.perf_available = false;
			continue;
		}
		aggregate.published += report.header.published;
		aggregate.delivered += report.header.delivered;
		aggregate.queue_full += report.header.queue_full;
		aggregate.queue_empty += report.header.queue_empty;
		aggregate.queue_contention += report.header.queue_contention;
		aggregate.recovery += report.header.recovery;
		aggregate.errors += report.header.errors;
		aggregate.wait_calls += report.header.wait_calls;
		aggregate.wait_timeouts += report.header.wait_timeouts;
		if (report.header.perf_available == 0) {
			aggregate.perf_available = false;
		} else {
			aggregate.perf_context_switches += report.header.perf_context_switches;
		}
		if (children[i].role == ChildRole::kConsumer) {
			for (const auto& record : report.records) aggregate.latency_ns.push_back(record.latency_ns);
		}
	}
	if (aggregate.timed_out) ++aggregate.errors;
	aggregate.wall_us = (monotonic_ns() - wall_start) / 1000ull;

	std::vector<uint8_t> seen(static_cast<size_t>(aggregate.expected), 0);
	bool ids_valid = aggregate.delivered == aggregate.expected &&
	                 aggregate.published == aggregate.expected && !aggregate.timed_out;
	if (aggregate.latency_ns.size() != static_cast<size_t>(aggregate.expected)) {
		ids_valid = false;
	}
	for (size_t i = 0; i < children.size(); ++i) {
		if (children[i].role != ChildRole::kConsumer || !reports[i].valid) continue;
		for (const auto& record : reports[i].records) {
			if (record.producer >= config.producers || record.sequence >= config.messages) {
				ids_valid = false;
				continue;
			}
			const uint64_t index =
			        static_cast<uint64_t>(record.producer) * config.messages + record.sequence;
			if (index >= aggregate.expected || seen[static_cast<size_t>(index)] != 0) {
				ids_valid = false;
				continue;
			}
			seen[static_cast<size_t>(index)] = 1;
		}
	}
	if (ids_valid) {
		for (uint8_t value : seen) {
			if (value == 0) {
				ids_valid = false;
				break;
			}
		}
	}
	if (!config.out_dir.empty()) {
		if (!mkdirs(config.out_dir)) {
			std::fprintf(stderr, "OUTPUT_DIR_FAIL path=%s errno=%d\n", config.out_dir.c_str(),
			             errno);
			++aggregate.errors;
			ids_valid = false;
		} else {
			std::vector<DeliveryRecord> records;
			for (size_t i = 0; i < children.size(); ++i) {
				if (children[i].role == ChildRole::kConsumer && reports[i].valid) {
					records.insert(records.end(), reports[i].records.begin(),
					               reports[i].records.end());
				}
			}
			if (!write_latency_csv(config.out_dir, run_index, kSize, config.producers,
			                       config.consumers, config.retry_policy, config.wait_policy,
			                       records)) {
				std::fprintf(stderr, "LATENCY_OUTPUT_FAIL run=%" PRIu32 " errno=%d\n", run_index,
				             errno);
				++aggregate.errors;
				ids_valid = false;
			}
		}
	}
	if (!config.keep_worker_reports) {
		for (const auto& child : children) (void)::unlink(child.report_path.c_str());
	}
	if (!remove_and_verify_queue(&queue.value(), options, run_index,
	                             g_stop_requested != 0, child_kill_sent)) {
		std::fprintf(stderr, "REMOVE_FAIL run=%" PRIu32 "\n", run_index);
		ids_valid = false;
	}
	aggregate.correctness = ids_valid && aggregate.errors == 0;
	print_summary(run_index, kSize, config, aggregate);
	return aggregate.correctness ? 0 : 1;
}

template <size_t kSize>
int run_parent(const std::string& executable, const Config& config, uint32_t first_run) {
	int result = 0;
	for (uint32_t i = 0; i < config.runs; ++i) {
		if (run_parent_once<kSize>(executable, config, first_run + i) != 0) result = 1;
		if (g_stop_requested != 0) break;
	}
	return result;
}

bool parse_config(int argc, char** argv, Config* config) {
	const uint64_t payload_size = edge_tool::arg_u64(argc, argv, "--payload-size", 64);
	if (payload_size != 64 && payload_size != 1024 && payload_size != 4096) {
		std::fprintf(stderr, "--payload-size must be 64, 1024, or 4096\n");
		return false;
	}
	const uint64_t producers = edge_tool::arg_u64(argc, argv, "--producers", 1);
	const uint64_t consumers = edge_tool::arg_u64(argc, argv, "--consumers", 1);
	const uint64_t messages = edge_tool::arg_u64(argc, argv, "--messages", 1000);
	const uint64_t capacity = edge_tool::arg_u64(argc, argv, "--capacity", 64);
	const uint64_t runs = edge_tool::arg_u64(argc, argv, "--runs", 1);
	config->timeout_ms = edge_tool::arg_u64(argc, argv, "--timeout-ms", 30000);
	config->test_consumer_hold_ms =
	        edge_tool::arg_u64(argc, argv, "--test-consumer-hold-ms", 0);
	config->test_consumer_delay_us =
	        edge_tool::arg_u64(argc, argv, "--test-consumer-delay-us", 0);
	config->test_ignore_term_consumer =
	        edge_tool::arg_flag(argc, argv, "--test-ignore-term-consumer");
	config->test_consumer_stop = edge_tool::arg_flag(argc, argv, "--test-consumer-stop");
	if (!parse_retry_policy_args(argc, argv, &config->retry_policy)) return false;
	if (!parse_wait_policy_args(argc, argv, &config->wait_policy)) return false;
	const char* out_dir = edge_tool::arg_value(argc, argv, "--out-dir");
	const char* ready_file = edge_tool::arg_value(argc, argv, "--ready-file");
	const char* retry_ready_file = edge_tool::arg_value(argc, argv, "--test-retry-ready-file");
	config->keep_worker_reports = edge_tool::arg_flag(argc, argv, "--keep-worker-reports");
	if (config->keep_worker_reports && (out_dir == nullptr || *out_dir == '\0')) {
		std::fprintf(stderr, "--keep-worker-reports requires --out-dir\n");
		return false;
	}
	if (producers == 0 || producers > kMaxWorkers || consumers == 0 || consumers > kMaxWorkers ||
	    messages == 0 || messages > kMaxMessagesPerProducer || capacity == 0 || capacity > 4096 ||
	    runs == 0 || runs > kMaxRuns || config->timeout_ms == 0 ||
	    config->test_consumer_hold_ms > 10000 || config->test_consumer_delay_us > 1000000) {
		std::fprintf(stderr, "invalid benchmark bounds\n");
		return false;
	}
	config->payload_size = static_cast<size_t>(payload_size);
	config->producers = static_cast<uint32_t>(producers);
	config->consumers = static_cast<uint32_t>(consumers);
	config->messages = messages;
	config->capacity = static_cast<uint32_t>(capacity);
	config->runs = static_cast<uint32_t>(runs);
	config->out_dir = out_dir == nullptr ? "" : out_dir;
	config->ready_file = ready_file == nullptr ? "" : ready_file;
	config->test_retry_ready_file = retry_ready_file == nullptr ? "" : retry_ready_file;
	if (!config->test_retry_ready_file.empty() && producers != 1) {
		std::fprintf(stderr, "--test-retry-ready-file requires exactly one producer\n");
		return false;
	}
	if (messages > std::numeric_limits<uint64_t>::max() / producers) {
		std::fprintf(stderr, "expected message count overflows\n");
		return false;
	}
	return true;
}

void print_usage(const char* executable) {
	std::printf(
	        "usage: %s [--payload-size 64|1024|4096] [--producers N] [--consumers N] "
	        "[--messages N] [--capacity N] [--runs N] [--timeout-ms N] "
	        "[--test-consumer-hold-ms N] [--test-consumer-delay-us N] "
	        "[--test-ignore-term-consumer] [--retry-policy yield|spin] "
	        "[--test-consumer-stop] [--test-retry-ready-file FILE] "
	        "[--wait-policy busy|blocking] [--out-dir DIR] [--keep-worker-reports] "
	        "[--ready-file FILE]\n"
	        "       %s --smoke\n",
	        executable, executable);
}

int run_smoke(const std::string& executable) {
	const size_t sizes[] = {64, 1024, 4096};
	uint32_t run_index = 1;
	for (size_t payload_size : sizes) {
		for (uint32_t mode = 1; mode <= 2; ++mode) {
			Config config;
			config.payload_size = payload_size;
			config.producers = mode;
			config.consumers = mode;
			config.messages = mode == 1 ? 40 : 30;
			config.capacity = 8;
			config.timeout_ms = 10000;
			config.out_dir.clear();
			int status = 0;
			switch (payload_size) {
			case 64:
				status = run_parent<64>(executable, config, run_index);
				break;
			case 1024:
				status = run_parent<1024>(executable, config, run_index);
				break;
			default:
				status = run_parent<4096>(executable, config, run_index);
				break;
			}
			++run_index;
			if (status != 0) return status;
		}
	}
	return 0;
}

int run_as_child(int argc, char** argv, size_t payload_size) {
	const char* role_arg = edge_tool::arg_value(argc, argv, "--role");
	if (role_arg == nullptr) return 2;
	const bool producer = std::strcmp(role_arg, "producer") == 0;
	const bool consumer = std::strcmp(role_arg, "consumer") == 0;
	if (!producer && !consumer) return 2;
	switch (payload_size) {
	case 64:
		return run_child<64>(argc, argv, producer);
	case 1024:
		return run_child<1024>(argc, argv, producer);
	case 4096:
		return run_child<4096>(argc, argv, producer);
	default:
		return 2;
	}
}

}  // namespace

int main(int argc, char** argv) {
	if (!install_stop_signal_handler()) return 2;
	const char* role = edge_tool::arg_value(argc, argv, "--role");
	const uint64_t payload_size = edge_tool::arg_u64(argc, argv, "--payload-size", 64);
	if (role != nullptr) return run_as_child(argc, argv, static_cast<size_t>(payload_size));
	if (edge_tool::arg_flag(argc, argv, "--help")) {
		print_usage("self_ipcs_mpmc_bench");
		return 0;
	}
	const std::string executable = self_executable();
	if (executable.empty()) {
		std::fprintf(stderr, "cannot resolve /proc/self/exe\n");
		return 2;
	}
	if (edge_tool::arg_flag(argc, argv, "--smoke")) return run_smoke(executable);
	Config config;
	if (!parse_config(argc, argv, &config)) {
		print_usage("self_ipcs_mpmc_bench");
		return 2;
	}
	switch (config.payload_size) {
	case 64:
		return run_parent<64>(executable, config, 1);
	case 1024:
		return run_parent<1024>(executable, config, 1);
	case 4096:
		return run_parent<4096>(executable, config, 1);
	default:
		return 2;
	}
}
