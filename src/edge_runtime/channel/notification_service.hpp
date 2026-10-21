#ifndef EDGE_RUNTIME_DETAIL_NOTIFICATION_SERVICE_HPP
#define EDGE_RUNTIME_DETAIL_NOTIFICATION_SERVICE_HPP

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/channel/event_notification.hpp"
#include "edge_runtime/process/process_identity.hpp"

namespace edge_runtime::detail {

struct ProducerHandle;
struct NotificationSession;

struct NotificationRole {
	uint64_t epoch{};
	ProcessIdentity identity{};
};

struct NotificationRecord {
	uint64_t magic{};
	uint32_t version{};
	uint32_t kind{};
	uint64_t generation{};
	uint64_t nonce_hi{};
	uint64_t nonce_lo{};
	NotificationRole producer{};
	NotificationRole consumer{};
	std::array<std::byte, 32> fingerprint{};
	uint64_t registration_hi{};
	uint64_t registration_lo{};
	uint64_t checksum{};
};
static_assert(sizeof(NotificationRecord) == 176, "notification sidecar record");
inline constexpr uint64_t kNotificationMagic = 0x314e464449504353ull;

uint64_t notification_checksum(NotificationRecord record) noexcept;
Result<void> eventfd_signal(int fd) noexcept;

class NotificationServer {
       public:
	static Result<std::shared_ptr<NotificationServer>> start(ProducerHandle& handle);
	~NotificationServer();
	void stop() noexcept;
	void notify() noexcept;
	int last_error() const noexcept { return last_error_.load(std::memory_order_relaxed); }

       private:
	NotificationServer() = default;
	void run() noexcept;
	void accept_peer(int fd) noexcept;
	void replace_session(std::shared_ptr<NotificationSession> session) noexcept;
	bool binding_current(const NotificationRecord& record) noexcept;
	ChannelHeaderAbi* header_{};
	BootstrapHeaderAbi* bootstrap_{};
	NotificationRecord owner_{};
	std::string path_;
	uint64_t socket_dev_{};
	uint64_t socket_ino_{};
	UniqueFd listener_;
	std::atomic<bool> stopping_{false};
	std::atomic<int> last_error_{0};
	std::thread thread_;
	std::shared_ptr<NotificationSession> session_;
};

}
#endif
