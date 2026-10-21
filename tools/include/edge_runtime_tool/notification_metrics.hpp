#ifndef EDGE_RUNTIME_TOOL_NOTIFICATION_METRICS_HPP
#define EDGE_RUNTIME_TOOL_NOTIFICATION_METRICS_HPP

#include <dirent.h>
#include <sched.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cinttypes>
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "edge_runtime_tool/tool_common.hpp"

namespace edge_tool {

inline uint64_t notification_timeval_us(const timeval& value) {
	return static_cast<uint64_t>(value.tv_sec) * 1000000ull +
	       static_cast<uint64_t>(value.tv_usec);
}

inline uint64_t notification_realtime_ns() {
	timespec value{};
	if (::clock_gettime(CLOCK_REALTIME, &value) != 0) return 0;
	return static_cast<uint64_t>(value.tv_sec) * 1000000000ull +
	       static_cast<uint64_t>(value.tv_nsec);
}

inline void notification_marker(const char* marker, const char* role, uint64_t raw_ns,
                                uint64_t realtime_ns, const rusage& usage) {
	std::printf(
	        "%s {\"role\":\"%s\",\"pid\":%ld,\"monotonic_raw_ns\":%" PRIu64
	        ",\"realtime_ns\":%" PRIu64 ",\"user_us\":%" PRIu64
	        ",\"system_us\":%" PRIu64 ",\"voluntary_csw\":%ld,\"involuntary_csw\":%ld,"
	        "\"minor_faults\":%ld,\"major_faults\":%ld,\"maxrss_kb\":%ld,"
	        "\"input_blocks\":%ld,\"output_blocks\":%ld,\"signals\":%ld,"
	        "\"swaps\":%ld,\"messages_sent\":%ld,\"messages_received\":%ld}\n",
	        marker, role, static_cast<long>(::getpid()), raw_ns, realtime_ns,
	        notification_timeval_us(usage.ru_utime), notification_timeval_us(usage.ru_stime),
	        usage.ru_nvcsw, usage.ru_nivcsw, usage.ru_minflt, usage.ru_majflt, usage.ru_maxrss,
	        usage.ru_inblock, usage.ru_oublock, usage.ru_nsignals, usage.ru_nswap,
	        usage.ru_msgsnd, usage.ru_msgrcv);
	std::fflush(stdout);
}

inline std::string notification_threads() {
	std::string result = "[";
	DIR* directory = ::opendir("/proc/self/task");
	if (directory != nullptr) {
		while (const auto* entry = ::readdir(directory)) {
			char* end = nullptr;
			const long tid = std::strtol(entry->d_name, &end, 10);
			if (*end != '\0' || tid <= 0) continue;
			cpu_set_t mask;
			CPU_ZERO(&mask);
			if (::sched_getaffinity(static_cast<pid_t>(tid), sizeof(mask), &mask) != 0) continue;
			if (result.size() > 1) result += ",";
			result += "{\"tid\":" + std::to_string(tid) + ",\"cpus\":[";
			bool first = true;
			for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
				if (!CPU_ISSET(cpu, &mask)) continue;
				if (!first) result += ",";
				first = false;
				result += std::to_string(cpu);
			}
			result += "]}";
		}
		::closedir(directory);
	}
	return result + "]";
}

inline std::string notification_process_affinity() {
	cpu_set_t mask;
	CPU_ZERO(&mask);
	if (::sched_getaffinity(0, sizeof(mask), &mask) != 0) return "[]";
	std::string result = "[";
	for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
		if (!CPU_ISSET(cpu, &mask)) continue;
		if (result.size() > 1) result += ",";
		result += std::to_string(cpu);
	}
	return result + "]";
}

class NotificationMetrics {
       public:
	NotificationMetrics(const char* role, const char* notification, uint64_t payload,
	                    uint64_t target, uint64_t rate)
	    : role_(role), notification_(notification), payload_(payload), target_(target), rate_(rate),
	      threads_begin_(notification_threads()), affinity_begin_(notification_process_affinity()) {
		valid_ = ::getrusage(RUSAGE_SELF, &begin_) == 0;
		begin_ns_ = monotonic_raw_now_ns();
		begin_realtime_ns_ = notification_realtime_ns();
		notification_marker("WORK_BEGIN", role_, begin_ns_, begin_realtime_ns_, begin_);
	}
	~NotificationMetrics() { finish(); }
	NotificationMetrics(const NotificationMetrics&) = delete;
	NotificationMetrics& operator=(const NotificationMetrics&) = delete;
	void finish() {
		if (finished_) return;
		finished_ = true;
		const uint64_t end_ns = monotonic_raw_now_ns();
		const uint64_t end_realtime_ns = notification_realtime_ns();
		rusage end{};
		valid_ = (::getrusage(RUSAGE_SELF, &end) == 0) && valid_;
		notification_marker("WORK_END", role_, end_ns, end_realtime_ns, end);
		const std::string threads_end = notification_threads();
		const std::string affinity_end = notification_process_affinity();
		std::printf(
		        "WORK_METRICS {\"version\":1,\"role\":\"%s\",\"pid\":%ld,"
		        "\"notification\":\"%s\",\"payload_bytes\":%" PRIu64
		        ",\"target\":%" PRIu64 ",\"rate_hz\":%" PRIu64
		        ",\"clock\":\"CLOCK_MONOTONIC_RAW\",\"rusage_scope\":\"RUSAGE_SELF\","
		        "\"valid\":%s,\"begin_ns\":%" PRIu64 ",\"end_ns\":%" PRIu64
		        ",\"begin_realtime_ns\":%" PRIu64 ",\"end_realtime_ns\":%" PRIu64
		        ",\"user_us\":%" PRIu64 ",\"system_us\":%" PRIu64
		        ",\"voluntary_csw\":%ld,\"involuntary_csw\":%ld,"
		        "\"minor_faults\":%ld,\"major_faults\":%ld,\"input_blocks\":%ld,"
		        "\"output_blocks\":%ld,\"signals\":%ld,\"swaps\":%ld,"
		        "\"messages_sent\":%ld,\"messages_received\":%ld,\"process_maxrss_kb_at_end\":%ld,"
		        "\"attempted\":%" PRIu64 ",\"accepted\":%" PRIu64
		        ",\"consumed\":%" PRIu64 ",\"unique_consumed\":%" PRIu64
		        ",\"final_sequence\":%" PRIu64 ",\"missed\":%" PRIu64
		        ",\"duplicate\":%" PRIu64 ",\"torn\":%" PRIu64
		        ",\"timeout\":%" PRIu64 ",\"wait_timeouts\":%" PRIu64
		        ",\"thread_count_begin\":%zu,\"thread_count_end\":%zu,"
		        "\"process_allowed_cpus_begin\":%s,\"process_allowed_cpus_end\":%s,"
		        "\"threads_begin\":%s,\"threads_end\":%s}\n",
		        role_, static_cast<long>(::getpid()), notification_, payload_, target_, rate_,
		        valid_ ? "true" : "false", begin_ns_, end_ns, begin_realtime_ns_, end_realtime_ns,
		        notification_timeval_us(end.ru_utime) - notification_timeval_us(begin_.ru_utime),
		        notification_timeval_us(end.ru_stime) - notification_timeval_us(begin_.ru_stime),
		        end.ru_nvcsw - begin_.ru_nvcsw, end.ru_nivcsw - begin_.ru_nivcsw,
		        end.ru_minflt - begin_.ru_minflt, end.ru_majflt - begin_.ru_majflt,
		        end.ru_inblock - begin_.ru_inblock, end.ru_oublock - begin_.ru_oublock,
		        end.ru_nsignals - begin_.ru_nsignals, end.ru_nswap - begin_.ru_nswap,
		        end.ru_msgsnd - begin_.ru_msgsnd, end.ru_msgrcv - begin_.ru_msgrcv, end.ru_maxrss,
		        attempted, accepted, consumed, unique_consumed, final_sequence, missed,
		        duplicate, torn, timeout, wait_timeouts,
		        static_cast<size_t>(std::count(threads_begin_.begin(), threads_begin_.end(), '{')),
		        static_cast<size_t>(std::count(threads_end.begin(), threads_end.end(), '{')),
		        affinity_begin_.c_str(), affinity_end.c_str(),
		        threads_begin_.c_str(), threads_end.c_str());
		std::fflush(stdout);
	}
	uint64_t attempted = 0;
	uint64_t accepted = 0;
	uint64_t consumed = 0;
	uint64_t unique_consumed = 0;
	uint64_t final_sequence = 0;
	uint64_t missed = 0;
	uint64_t duplicate = 0;
	uint64_t torn = 0;
	uint64_t timeout = 0;
	uint64_t wait_timeouts = 0;

       private:
	const char* role_;
	const char* notification_;
	uint64_t payload_;
	uint64_t target_;
	uint64_t rate_;
	std::string threads_begin_;
	std::string affinity_begin_;
	rusage begin_{};
	uint64_t begin_ns_ = 0;
	uint64_t begin_realtime_ns_ = 0;
	bool valid_ = false;
	bool finished_ = false;
};

}

#endif
