#include <fcntl.h>
#include <poll.h>
#include <sys/file.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>

#include "self_ipcs/channel/producer.hpp"
#include "test_payload.hpp"

int main(int argc, char** argv) {
	const bool producer_mode = argc == 5 && std::strcmp(argv[1], "--producer") == 0;
	if (argc != 4 && !producer_mode) return 2;
	const int offset = producer_mode ? 1 : 0;
	const int ready_fd = std::atoi(argv[2 + offset]);
	const int release_fd = std::atoi(argv[3 + offset]);
	if (producer_mode) {
		self_ipcs::ChannelOptions options;
		options.name = argv[2];
		options.transport = self_ipcs::Transport::kMemfdFdPass;
		auto producer =
		        self_ipcs::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
		if (!producer) return 10;
		const char ready = 'r';
		if (::write(ready_fd, &ready, 1) != 1) return 11;
		struct pollfd descriptor {
			release_fd, POLLIN, 0
		};
		if (::poll(&descriptor, 1, 8000) <= 0) return 12;
		char command = 0;
		if (::read(release_fd, &command, 1) != 1 || command != 'q') return 13;
		return producer.value().remove_if_owner() ? 0 : 14;
	}
	const int lock_fd = ::open(argv[1], O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
	if (lock_fd < 0 || ::flock(lock_fd, LOCK_EX | LOCK_NB) != 0) return 3;
	const char ready = 'r';
	if (::write(ready_fd, &ready, 1) != 1) return 4;
	struct pollfd descriptor {
		release_fd, POLLIN, 0
	};
	int result;
	do {
		result = ::poll(&descriptor, 1, 8000);
	} while (result < 0 && errno == EINTR);
	char release = 0;
	const bool healthy = result > 0 && ::read(release_fd, &release, 1) == 1 && release == 'q';
	if (::flock(lock_fd, LOCK_UN) != 0 || ::close(lock_fd) != 0) return 5;
	return healthy ? 0 : 6;
}
