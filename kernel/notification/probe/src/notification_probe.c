#define _GNU_SOURCE

#include "notification_probe.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "self_ipcs_notification.h"

enum peer_command {
	PEER_READY = 1,
	PEER_WRITE,
	PEER_READ,
	PEER_STOP,
	PEER_DELAY_WRITE,
	PEER_DELAY_STOP,
	PEER_SIGNAL,
	PEER_EXIT
};

struct peer_message {
	uint64_t command;
	uint64_t value;
	int64_t result;
	int32_t error;
	uint32_t reserved;
};

static volatile sig_atomic_t signal_seen;

static void signal_handler(int number) {
	(void)number;
	signal_seen = 1;
}

static int wait_for(int descriptor, short events, int timeout_ms) {
	struct pollfd watched = {.fd = descriptor, .events = events, .revents = 0};
	int result = poll(&watched, 1, timeout_ms);

	if (result <= 0) return -1;
	return watched.revents;
}

static int send_message(int control, const struct peer_message* message) {
	if (wait_for(control, POLLOUT, 2000) < 0) return -1;
	return send(control, message, sizeof(*message), MSG_NOSIGNAL) == sizeof(*message) ? 0 : -1;
}

static int receive_message(int control, struct peer_message* message) {
	if (wait_for(control, POLLIN, 2000) < 0) return -1;
	return recv(control, message, sizeof(*message), MSG_TRUNC) == sizeof(*message) ? 0 : -1;
}

static int send_descriptor(int control, int descriptor) {
	union {
		struct cmsghdr alignment;
		unsigned char bytes[CMSG_SPACE(sizeof(int))];
	} ancillary = {0};
	char marker = 'F';
	struct iovec payload = {.iov_base = &marker, .iov_len = 1};
	struct msghdr message = {0};
	struct cmsghdr* header;

	message.msg_iov = &payload;
	message.msg_iovlen = 1;
	message.msg_control = ancillary.bytes;
	message.msg_controllen = sizeof(ancillary.bytes);
	header = CMSG_FIRSTHDR(&message);
	header->cmsg_level = SOL_SOCKET;
	header->cmsg_type = SCM_RIGHTS;
	header->cmsg_len = CMSG_LEN(sizeof(descriptor));
	memcpy(CMSG_DATA(header), &descriptor, sizeof(descriptor));
	if (wait_for(control, POLLOUT, 2000) < 0) return -1;
	return sendmsg(control, &message, MSG_NOSIGNAL) == 1 ? 0 : -1;
}

static int receive_descriptor(int control) {
	union {
		struct cmsghdr alignment;
		unsigned char bytes[CMSG_SPACE(sizeof(int))];
	} ancillary = {0};
	char marker = 0;
	struct iovec payload = {.iov_base = &marker, .iov_len = 1};
	struct msghdr message = {0};
	struct cmsghdr* header;
	int descriptor = -1;

	message.msg_iov = &payload;
	message.msg_iovlen = 1;
	message.msg_control = ancillary.bytes;
	message.msg_controllen = sizeof(ancillary.bytes);
	if (wait_for(control, POLLIN, 2000) < 0 ||
	    recvmsg(control, &message, MSG_CMSG_CLOEXEC) != 1)
		return -1;
	header = CMSG_FIRSTHDR(&message);
	if (!header || header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS ||
	    header->cmsg_len != CMSG_LEN(sizeof(descriptor)))
		return -1;
	memcpy(&descriptor, CMSG_DATA(header), sizeof(descriptor));
	if (marker != 'F' || (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC))) {
		close(descriptor);
		return -1;
	}
	return descriptor;
}

static int request(int control, uint64_t command, uint64_t value, int64_t result,
                   uint64_t expected_value) {
	struct peer_message message = {.command = command, .value = value};

	if (send_message(control, &message) || receive_message(control, &message)) return -1;
	if (message.command != command || message.result != result || message.error) return -1;
	if (command == PEER_READ && message.value != expected_value) return -1;
	return 0;
}

int notification_probe_peer(int control) {
	struct peer_message message = {.command = PEER_READY};
	int descriptor;
	int outcome = 1;

	alarm(15);
	descriptor = receive_descriptor(control);
	if (descriptor < 0) goto cleanup;
	if (send_message(control, &message)) goto cleanup;
	for (;;) {
		if (receive_message(control, &message)) break;
		message.result = 0;
		message.error = 0;
		if (message.command == PEER_DELAY_WRITE || message.command == PEER_DELAY_STOP ||
		    message.command == PEER_SIGNAL) {
			struct timespec delay = {.tv_sec = 0, .tv_nsec = 100000000};

			if (nanosleep(&delay, NULL)) break;
		}
		errno = 0;
		switch (message.command) {
			case PEER_WRITE:
			case PEER_DELAY_WRITE:
				message.result =
				        write(descriptor, &message.value, sizeof(message.value));
				break;
			case PEER_READ:
				message.value = 0;
				message.result =
				        read(descriptor, &message.value, sizeof(message.value));
				break;
			case PEER_STOP:
			case PEER_DELAY_STOP:
				message.result =
				        ioctl(descriptor, SELF_IPCS_NOTIFICATION_STOP, 0UL);
				break;
			case PEER_SIGNAL:
				message.result = kill(getppid(), SIGUSR1);
				break;
			case PEER_EXIT:
				if (!send_message(control, &message)) outcome = 0;
				goto cleanup;
			default:
				goto cleanup;
		}
		if (message.result < 0) message.error = errno;
		if (send_message(control, &message)) break;
	}
cleanup:
	if (descriptor >= 0) close(descriptor);
	close(control);
	return outcome;
}

static pid_t start_peer(int control, int parent_control) {
	pid_t child = fork();

	if (child == 0) {
		char descriptor_text[32];

		close(parent_control);
		if (fcntl(control, F_SETFD, 0) < 0) _exit(125);
		if (snprintf(descriptor_text, sizeof(descriptor_text), "%d", control) < 0)
			_exit(125);
		execl("/proc/self/exe", "self_ipcs_notification_probe", "--peer", descriptor_text,
		      (char*)NULL);
		_exit(126);
	}
	return child;
}

static int finish_peer(pid_t child) {
	int status = 0;

	for (unsigned int attempt = 0; attempt < 200; ++attempt) {
		pid_t result = waitpid(child, &status, WNOHANG);
		struct timespec delay = {.tv_sec = 0, .tv_nsec = 10000000};

		if (result == child) return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : -1;
		if (result < 0 && errno != EINTR) return -1;
		(void)nanosleep(&delay, NULL);
	}
	(void)kill(child, SIGKILL);
	while (waitpid(child, &status, 0) < 0 && errno == EINTR) {
	}
	return -1;
}

#define REQUIRE(condition)                                                                         \
	do {                                                                                       \
		if (!(condition)) {                                                                \
			fprintf(stderr, "FAIL line %d: %s (errno=%d: %s)\n", __LINE__, #condition, \
			        errno, strerror(errno));                                           \
			goto cleanup;                                                              \
		}                                                                                  \
	} while (0)

int notification_probe_run(const char* device) {
	int original = -1;
	int duplicate = -1;
	int independent = -1;
	int poller = -1;
	struct epoll_event event = {.events = EPOLLIN, .data.u64 = 1};
	int control[2] = {-1, -1};
	pid_t child = -1;
	void* fault_page = MAP_FAILED;
	long page_bytes = sysconf(_SC_PAGESIZE);
	uint64_t counter = 0;
	unsigned char invalid_size[16] = {0};
	struct peer_message message = {0};
	struct sigaction action = {0};
	struct sigaction old_action = {0};
	bool signal_installed = false;
	int outcome = 1;
	struct self_ipcs_notification_info info = {0};

	alarm(20);
	original = open(device, O_RDWR | O_CLOEXEC | O_NONBLOCK);
	if (original < 0) {
		int open_error = errno;

		fprintf(stderr, "SKIP device not opened: %s: %s; no runtime pass\n", device,
		        strerror(open_error));
		alarm(0);
		return open_error == ENOENT || open_error == ENODEV || open_error == EACCES ||
		                       open_error == EPERM
		               ? 77
		               : 1;
	}
	REQUIRE(ioctl(original, SELF_IPCS_NOTIFICATION_GET_INFO, &info) == 0);
	REQUIRE(info.abi_version == SELF_IPCS_NOTIFICATION_ABI && info.struct_size == sizeof(info));
	REQUIRE(info.capabilities ==
	        (SELF_IPCS_NOTIFICATION_CAP_COUNTER | SELF_IPCS_NOTIFICATION_CAP_STOP));
	REQUIRE(info.reserved[0] == 0 && info.reserved[1] == 0);
	REQUIRE(ioctl(original, SELF_IPCS_NOTIFICATION_GET_INFO, (void*)1) == -1 &&
	        errno == EFAULT);
	independent = open(device, O_RDWR | O_CLOEXEC | O_NONBLOCK);
	REQUIRE(independent >= 0);
	duplicate = fcntl(original, F_DUPFD_CLOEXEC, 0);
	REQUIRE(duplicate >= 0);
	poller = epoll_create1(EPOLL_CLOEXEC);
	REQUIRE(poller >= 0);
	REQUIRE(epoll_ctl(poller, EPOLL_CTL_ADD, duplicate, &event) == 0);
	REQUIRE(read(original, &counter, sizeof(counter)) == -1 && errno == EAGAIN);
	REQUIRE(read(original, invalid_size, 7) == -1 && errno == EINVAL);
	REQUIRE(write(original, invalid_size, 9) == -1 && errno == EINVAL);
	REQUIRE(write(original, &counter, sizeof(counter)) == -1 && errno == EINVAL);
	REQUIRE(ioctl(original, _IO(0xB7, 0x7f), 0UL) == -1 && errno == ENOTTY);
	REQUIRE(lseek(original, 0, SEEK_SET) == -1 && errno == ESPIPE);
	counter = 2;
	REQUIRE(write(duplicate, &counter, sizeof(counter)) == sizeof(counter));
	counter = 3;
	REQUIRE(write(original, &counter, sizeof(counter)) == sizeof(counter));
	REQUIRE(epoll_wait(poller, &event, 1, 2000) == 1 && event.events == EPOLLIN);
	REQUIRE(read(duplicate, &counter, sizeof(counter)) == sizeof(counter) && counter == 5);
	REQUIRE(epoll_wait(poller, &event, 1, 0) == 0);
	REQUIRE(read(independent, &counter, sizeof(counter)) == -1 && errno == EAGAIN);
	counter = UINT64_MAX;
	REQUIRE(write(original, &counter, sizeof(counter)) == sizeof(counter));
	REQUIRE(wait_for(original, POLLIN | POLLOUT, 0) == POLLIN);
	counter = 1;
	REQUIRE(write(duplicate, &counter, sizeof(counter)) == -1 && errno == EOVERFLOW);
	REQUIRE(page_bytes > 0);
	fault_page = mmap(NULL, (size_t)page_bytes, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	REQUIRE(fault_page != MAP_FAILED);
	REQUIRE(read(original, fault_page, sizeof(counter)) == -1 && errno == EFAULT);
	REQUIRE(read(duplicate, &counter, sizeof(counter)) == sizeof(counter) &&
	        counter == UINT64_MAX);
	REQUIRE(write(original, fault_page, sizeof(counter)) == -1 && errno == EFAULT);
	REQUIRE(read(original, &counter, sizeof(counter)) == -1 && errno == EAGAIN);
	REQUIRE(socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, control) == 0);
	child = start_peer(control[1], control[0]);
	REQUIRE(child > 0);
	close(control[1]);
	control[1] = -1;
	REQUIRE(send_descriptor(control[0], duplicate) == 0);
	REQUIRE(receive_message(control[0], &message) == 0 && message.command == PEER_READY);
	close(original);
	original = -1;
	REQUIRE(request(control[0], PEER_WRITE, 7, 8, 0) == 0);
	REQUIRE(read(duplicate, &counter, sizeof(counter)) == sizeof(counter) && counter == 7);
	counter = 3;
	REQUIRE(write(duplicate, &counter, sizeof(counter)) == sizeof(counter));
	REQUIRE(request(control[0], PEER_READ, 0, 8, 3) == 0);
	message = (struct peer_message){.command = PEER_DELAY_WRITE, .value = 11};
	REQUIRE(send_message(control[0], &message) == 0);
	REQUIRE(wait_for(duplicate, POLLIN, 2000) == POLLIN);
	REQUIRE(receive_message(control[0], &message) == 0 && message.result == 8 &&
	        !message.error);
	REQUIRE(read(duplicate, &counter, sizeof(counter)) == sizeof(counter) && counter == 11);
	action.sa_handler = signal_handler;
	REQUIRE(sigemptyset(&action.sa_mask) == 0);
	REQUIRE(sigaction(SIGUSR1, &action, &old_action) == 0);
	signal_installed = true;
	REQUIRE(fcntl(independent, F_SETFL, 0) == 0);
	message = (struct peer_message){.command = PEER_SIGNAL};
	REQUIRE(send_message(control[0], &message) == 0);
	REQUIRE(read(independent, &counter, sizeof(counter)) == -1 && errno == EINTR &&
	        signal_seen);
	REQUIRE(receive_message(control[0], &message) == 0 && message.result == 0 &&
	        !message.error);
	REQUIRE(fcntl(independent, F_SETFL, O_NONBLOCK) == 0);
	counter = 9;
	REQUIRE(write(duplicate, &counter, sizeof(counter)) == sizeof(counter));
	REQUIRE(request(control[0], PEER_STOP, 0, 0, 0) == 0);
	REQUIRE(wait_for(duplicate, POLLIN | POLLOUT, 0) == POLLIN);
	REQUIRE(epoll_wait(poller, &event, 1, 2000) == 1 && event.events == EPOLLIN);
	REQUIRE(write(duplicate, &counter, sizeof(counter)) == -1 && errno == EPIPE);
	REQUIRE(ioctl(duplicate, SELF_IPCS_NOTIFICATION_STOP, 0UL) == 0);
	REQUIRE(read(duplicate, &counter, sizeof(counter)) == sizeof(counter) && counter == 9);
	REQUIRE(wait_for(duplicate, POLLIN | POLLOUT, 0) == POLLHUP);
	REQUIRE(epoll_wait(poller, &event, 1, 2000) == 1 && event.events == EPOLLHUP);
	REQUIRE(read(duplicate, &counter, sizeof(counter)) == 0);
	REQUIRE(epoll_ctl(poller, EPOLL_CTL_DEL, duplicate, NULL) == 0);
	close(poller);
	poller = -1;
	close(duplicate);
	duplicate = -1;
	REQUIRE(request(control[0], PEER_READ, 0, 0, 0) == 0);
	counter = 1;
	REQUIRE(write(independent, &counter, sizeof(counter)) == sizeof(counter));
	REQUIRE(read(independent, &counter, sizeof(counter)) == sizeof(counter) && counter == 1);
	REQUIRE(request(control[0], PEER_EXIT, 0, 0, 0) == 0);
	{
		int peer_result = finish_peer(child);

		child = -1;
		REQUIRE(peer_result == 0);
	}
	close(control[0]);
	control[0] = -1;
	REQUIRE(socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, control) == 0);
	child = start_peer(control[1], control[0]);
	REQUIRE(child > 0);
	close(control[1]);
	control[1] = -1;
	REQUIRE(send_descriptor(control[0], independent) == 0);
	REQUIRE(receive_message(control[0], &message) == 0 && message.command == PEER_READY);
	message = (struct peer_message){.command = PEER_DELAY_STOP};
	REQUIRE(send_message(control[0], &message) == 0);
	REQUIRE(wait_for(independent, POLLIN, 2000) == POLLHUP);
	REQUIRE(receive_message(control[0], &message) == 0 && message.result == 0 &&
	        !message.error);
	REQUIRE(read(independent, &counter, sizeof(counter)) == 0);
	REQUIRE(request(control[0], PEER_EXIT, 0, 0, 0) == 0);
	{
		int peer_result = finish_peer(child);

		child = -1;
		REQUIRE(peer_result == 0);
	}
	outcome = 0;
	puts("PASS device: dup/SCM_RIGHTS exec, isolation, EFAULT, overflow, EINTR, poll/epoll, sticky "
	     "stop/EOF");
cleanup:
	if (child > 0) {
		(void)kill(child, SIGKILL);
		(void)finish_peer(child);
	}
	if (signal_installed) (void)sigaction(SIGUSR1, &old_action, NULL);
	if (fault_page != MAP_FAILED) (void)munmap(fault_page, (size_t)page_bytes);
	if (poller >= 0) {
		if (duplicate >= 0) (void)epoll_ctl(poller, EPOLL_CTL_DEL, duplicate, NULL);
		close(poller);
	}
	if (original >= 0) close(original);
	if (duplicate >= 0) close(duplicate);
	if (independent >= 0) close(independent);
	if (control[0] >= 0) close(control[0]);
	if (control[1] >= 0) close(control[1]);
	alarm(0);
	return outcome;
}
