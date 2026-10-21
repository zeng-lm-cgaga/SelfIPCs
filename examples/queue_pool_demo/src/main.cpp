#include <signal.h>
#include <unistd.h>

#include <charconv>
#include <cstdio>
#include <exception>
#include <string_view>
#include <system_error>

#include "queue_pool_demo/api.hpp"
#include "queue_pool_demo/pool_flow.hpp"
#include "queue_pool_demo/queue_flow.hpp"

namespace {

bool parse_positive(const char* argument, int* value) {
	const std::string_view text(argument);
	const auto parsed = std::from_chars(text.data(), text.data() + text.size(), *value);
	return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size() && *value > 0;
}

}  // namespace

int main(int argc, char** argv) {
	(void)std::setvbuf(stdout, nullptr, _IOLBF, 0);
	try {
		if (argc == 5 && (std::string_view(argv[1]) == "--queue-helper" ||
		                  std::string_view(argv[1]) == "--pool-helper")) {
			int descriptor = -1;
			int parent_pid = -1;
			if (!parse_positive(argv[3], &descriptor) ||
			    !parse_positive(argv[4], &parent_pid)) {
				return 2;
			}
			const int result = std::string_view(argv[1]) == "--pool-helper"
			                           ? queue_pool_demo::run_pool_helper(
			                                     argv[2], descriptor,
			                                     static_cast<pid_t>(parent_pid))
			                           : queue_pool_demo::run_queue_helper(
			                                     argv[2], descriptor,
			                                     static_cast<pid_t>(parent_pid));
			(void)::close(descriptor);
			return result;
		}
		if (argc != 1) {
			std::fprintf(stderr, "usage: %s\n", argv[0]);
			return 2;
		}
		struct sigaction child_exit {};
		child_exit.sa_handler = SIG_DFL;
		if (::sigemptyset(&child_exit.sa_mask) != 0 ||
		    ::sigaction(SIGCHLD, &child_exit, nullptr) != 0) {
			std::perror("sigaction(SIGCHLD)");
			return 1;
		}
		std::printf("queue_pool_demo: package=%s C++17 queue + Pool waits\n",
		            queue_pool_demo::kPackageName);
		return queue_pool_demo::run_queue_flow() && queue_pool_demo::run_pool_flow() ? 0
		                                                                             : 1;
	} catch (const std::exception& error) {
		std::fprintf(stderr, "queue_pool_demo: exception: %s\n", error.what());
		return 1;
	}
}
