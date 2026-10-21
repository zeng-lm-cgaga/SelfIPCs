#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <string>

#include "self_ipcs/channel/consumer.hpp"
#include "self_ipcs/channel/producer.hpp"
#include "test_payload.hpp"

namespace {
using namespace std::chrono_literals;

bool report(const char* text) {
	return ::send(STDOUT_FILENO, text, std::strlen(text), MSG_NOSIGNAL) ==
	       static_cast<ssize_t>(std::strlen(text));
}

char command() {
	const auto deadline = std::chrono::steady_clock::now() + 10s;
	while (std::chrono::steady_clock::now() < deadline) {
		struct pollfd descriptor{STDIN_FILENO, POLLIN, 0};
		const int ready = ::poll(&descriptor, 1, 100);
		if (ready < 0 && errno == EINTR) continue;
		if (ready < 0) return 0;
		if (ready > 0) {
			char value = 0;
			if (::recv(STDIN_FILENO, &value, 1, MSG_DONTWAIT) == 1) return value;
			if (errno != EINTR && errno != EAGAIN) return 0;
		}
	}
	return 0;
}

int produce(const edge_runtime::ChannelOptions& options) {
	{
		auto created = self_ipcs::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
		if (!created) return 2;
		auto& producer = created.value();
		if (!producer.enable_event_notifications() ||
		    !producer.publish(TestPayloadV1{0x5A000001u, 1, 0}) || !report("READY\n")) return 3;
		const char next = command();
		if (next == 'p') {
			if (!producer.publish(TestPayloadV1{0x5A000001u, 2, 0}) || !report("PUBLISHED\n")) return 4;
			if (command() != 'q') return 5;
		} else if (next != 'q') return 6;
		if (!producer.remove_if_owner()) return 7;
	}
	return report("CLEAN\n") ? 0 : 8;
}

int consume(const edge_runtime::ChannelOptions& options) {
	{
		auto consumer = self_ipcs::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
		if (!consumer) return 10;
		auto notification = consumer.value().event_notification(1000ms);
		if (!notification || !notification.value().drain() ||
		    !consumer.value().try_read_latest() || !report("READY\n")) return 11;
		if (command() != 'q') return 12;
	}
	return report("CLEAN\n") ? 0 : 13;
}
}

int main(int argc, char** argv) {
	if (argc != 4) return 1;
	edge_runtime::ChannelOptions options;
	options.name = argv[2];
	if (std::string(argv[3]) == "memfd") options.transport = edge_runtime::Transport::kMemfdFdPass;
	return std::string(argv[1]) == "producer" ? produce(options) : consume(options);
}
