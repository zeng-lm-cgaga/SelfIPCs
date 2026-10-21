#ifndef EDGE_RUNTIME_CHANNEL_NOTIFICATION_BACKEND_HPP
#define EDGE_RUNTIME_CHANNEL_NOTIFICATION_BACKEND_HPP

#include "edge_runtime/channel/event_notification.hpp"
#include "edge_runtime/transport/shm_object.hpp"

namespace edge_runtime::detail {
bool notification_backend_valid(NotificationBackend backend) noexcept;
bool notification_fd_valid(int fd, NotificationBackend backend) noexcept;
Result<UniqueFd> notification_open(NotificationBackend backend) noexcept;
Result<void> notification_signal(int fd, NotificationBackend backend) noexcept;
Result<void> notification_drain(int fd, NotificationBackend backend) noexcept;
}
#endif
