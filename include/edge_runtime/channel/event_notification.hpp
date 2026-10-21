#ifndef EDGE_RUNTIME_CHANNEL_EVENT_NOTIFICATION_HPP
#define EDGE_RUNTIME_CHANNEL_EVENT_NOTIFICATION_HPP

#include <cstdint>
#include <memory>
#include <utility>

#include "edge_runtime/common/result.hpp"

namespace edge_runtime {
class EventNotification;
namespace detail {
struct ConsumerHandle;
struct EventNotificationState;
Result<EventNotification> consumer_event_notification_impl(
        const std::shared_ptr<ConsumerHandle>& handle, uint64_t timeout_ms);
}

class EventNotification {
       public:
	EventNotification(const EventNotification&) = delete;
	EventNotification& operator=(const EventNotification&) = delete;
	EventNotification(EventNotification&&) noexcept = default;
	EventNotification& operator=(EventNotification&&) noexcept = default;
	~EventNotification() = default;

	int native_handle() const noexcept;
	Result<void> drain() noexcept;

       private:
	explicit EventNotification(std::shared_ptr<detail::EventNotificationState> state) noexcept
	    : state_(std::move(state)) {}
	friend Result<EventNotification> detail::consumer_event_notification_impl(
	        const std::shared_ptr<detail::ConsumerHandle>&, uint64_t);
	std::shared_ptr<detail::EventNotificationState> state_;
};
}
#endif
