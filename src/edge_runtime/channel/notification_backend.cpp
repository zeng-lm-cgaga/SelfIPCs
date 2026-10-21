#include "edge_runtime/channel/notification_backend.hpp"

#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>

#include "self_ipcs_notification.h"

namespace edge_runtime::detail {
namespace {
Error failure(const char* context) noexcept {
	return make_errno_error(ErrorCode::kTransportFailed, errno, "EventNotification", context);
}
}

bool notification_backend_valid(NotificationBackend backend) noexcept {
	return backend == NotificationBackend::kEventFd || backend == NotificationBackend::kDriver;
}

bool notification_fd_valid(int fd, NotificationBackend backend) noexcept {
	const int flags = ::fcntl(fd, F_GETFL);
	const int descriptor_flags = ::fcntl(fd, F_GETFD);
	if (flags < 0 || (flags & O_NONBLOCK) == 0 || (flags & O_ACCMODE) != O_RDWR ||
	    descriptor_flags < 0 || (descriptor_flags & FD_CLOEXEC) == 0)
		return false;
	if (backend == NotificationBackend::kEventFd) {
		char path[64]{};
		char target[64]{};
		std::snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
		return ::readlink(path, target, sizeof(target) - 1) > 0 &&
		       std::strcmp(target, "anon_inode:[eventfd]") == 0;
	}
	if (backend != NotificationBackend::kDriver) return false;
	struct stat received {
	}, device{};
	if (::fstat(fd, &received) != 0 || !S_ISCHR(received.st_mode) ||
	    ::lstat("/dev/" SELF_IPCS_NOTIFICATION_DEVICE, &device) != 0 ||
	    !S_ISCHR(device.st_mode) || received.st_rdev != device.st_rdev)
		return false;
	struct self_ipcs_notification_info info {};
	bool queried = false;
	for (int attempt = 0; attempt < 8; ++attempt) {
		if (::ioctl(fd, SELF_IPCS_NOTIFICATION_GET_INFO, &info) == 0) {
			queried = true;
			break;
		}
		if (errno != EINTR) break;
	}
	return queried && info.abi_version == SELF_IPCS_NOTIFICATION_ABI &&
	       info.struct_size == sizeof(info) &&
	       info.capabilities ==
	               (SELF_IPCS_NOTIFICATION_CAP_COUNTER | SELF_IPCS_NOTIFICATION_CAP_STOP) &&
	       info.reserved[0] == 0 && info.reserved[1] == 0;
}

Result<UniqueFd> notification_open(NotificationBackend backend) noexcept {
	if (!notification_backend_valid(backend))
		return make_error(ErrorCode::kInvalidOptions, "EventNotification",
		                  "unknown backend");
	UniqueFd descriptor(backend == NotificationBackend::kEventFd
	                            ? ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)
	                            : ::open("/dev/" SELF_IPCS_NOTIFICATION_DEVICE,
	                                     O_RDWR | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW));
	if (descriptor.get() < 0) return failure("notification backend open failed");
	if (!notification_fd_valid(descriptor.get(), backend)) {
		errno = EPROTO;
		return failure("notification FD type/flags/UAPI mismatch");
	}
	return descriptor;
}

Result<void> notification_signal(int fd, NotificationBackend backend) noexcept {
	const uint64_t value = 1;
	for (int attempt = 0; attempt < 8; ++attempt) {
		const ssize_t count = ::write(fd, &value, sizeof(value));
		if (count == static_cast<ssize_t>(sizeof(value))) return Result<void>::ok();
		if (count < 0 && errno == EAGAIN && backend == NotificationBackend::kEventFd)
			return Result<void>::ok();
		if (count < 0 && (errno == EINTR ||
		                  (errno == EAGAIN && backend == NotificationBackend::kDriver)))
			continue;
		if (count >= 0) errno = EIO;
		return failure("notification write failed");
	}
	return failure("notification write retry budget exhausted");
}

Result<void> notification_drain(int fd, NotificationBackend backend) noexcept {
	uint64_t counter = 0;
	for (int attempt = 0; attempt < 8; ++attempt) {
		const ssize_t count = ::read(fd, &counter, sizeof(counter));
		if (count == static_cast<ssize_t>(sizeof(counter)) ||
		    (count < 0 && errno == EAGAIN))
			return Result<void>::ok();
		if (count < 0 && errno == EINTR) continue;
		if (count == 0 && backend == NotificationBackend::kDriver)
			return make_error(ErrorCode::kClosed, "EventNotification",
			                  "driver stopped and drained");
		if (count >= 0) errno = EIO;
		return failure("notification drain failed");
	}
	errno = EINTR;
	return failure("notification drain interrupted repeatedly");
}
}
