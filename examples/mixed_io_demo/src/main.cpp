#include "mixed_io_demo/api.hpp"

#include <linux/can.h>
#include <net/if.h>

#include <charconv>
#include <climits>
#include <cstdio>
#include <cstring>
#include <string_view>

namespace mixed_io_demo {
namespace {

void help(const char* program) {
	std::printf(
	        "Usage: %s --interface <existing-can-interface> [--duration-ms 100..5000]\n"
	        "       [--can-batch 1..256]\n"
	        "A bounded fork/exec demo: parent publishes copied SHM samples and real CAN\n"
	        "frames; independent consumer uses one level-triggered epoll for aggregate\n"
	        "notification, AF_CAN, timerfd and inherited stop eventfd. No interface is\n"
	        "created or configured. Use vcan in an isolated user/network namespace.\n"
	        "Example: %s --interface vcan0 --duration-ms 300 --can-batch 8\n"
	        "Budgets: setup 3s, final sample ACK 2s, stop/reap 2s each; CAN read batch 16,\n"
	        "one notification drain/read and one timer/stop counter read per epoll turn.\n"
	        "JSON-line output is illustrative, not formal performance evidence.\n",
	        program, program);
}

bool number(const char* text, uint32_t& value) {
	const std::string_view input(text);
	const auto parsed = std::from_chars(input.data(), input.data() + input.size(), value);
	return parsed.ec == std::errc{} && parsed.ptr == input.data() + input.size();
}

}
}

int main(int argc, char** argv) {
	using namespace mixed_io_demo;
	(void)std::setvbuf(stdout, nullptr, _IOLBF, 0);
	if (argc == 2 && std::strcmp(argv[1], "--help") == 0) {
		help(argv[0]);
		return 0;
	}
	Options options;
	bool consumer = false;
	if (argc == 10 && std::strcmp(argv[1], "--consumer") == 0) {
		consumer = true;
		options.interface = argv[2];
		options.channel = argv[3];
		uint32_t control = 0;
		uint32_t stop = 0;
		if (!number(argv[4], options.duration_ms) || !number(argv[5], options.can_batch) ||
		    !number(argv[6], options.can_id) || !number(argv[7], control) ||
		    !number(argv[8], stop) || std::strcmp(argv[9], "exec") != 0 ||
		    control > INT_MAX || stop > INT_MAX || control == stop) {
			return 2;
		}
		options.control_fd = static_cast<int>(control);
		options.stop_fd = static_cast<int>(stop);
	} else {
		for (int index = 1; index < argc; index += 2) {
			if (index + 1 >= argc) {
				help(argv[0]);
				return 2;
			}
			const std::string_view argument(argv[index]);
			if (argument == "--interface") {
				options.interface = argv[index + 1];
			} else if (argument == "--duration-ms") {
				if (!number(argv[index + 1], options.duration_ms)) return 2;
			} else if (argument == "--can-batch") {
				if (!number(argv[index + 1], options.can_batch)) return 2;
			} else {
				help(argv[0]);
				return 2;
			}
		}
	}
	if (options.interface.empty() || options.interface.size() >= IFNAMSIZ ||
	    options.duration_ms < 100 || options.duration_ms > 5000 || options.can_batch < 1 ||
	    options.can_batch > 256 || options.can_id > CAN_SFF_MASK) {
		help(argv[0]);
		return 2;
	}
	return (consumer ? run_consumer(options) : run_producer(options)) ? 0 : 1;
}
