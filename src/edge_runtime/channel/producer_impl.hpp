#ifndef EDGE_RUNTIME_DETAIL_PRODUCER_IMPL_HPP
#define EDGE_RUNTIME_DETAIL_PRODUCER_IMPL_HPP

#include <sys/socket.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "edge_runtime/channel/channel_options.hpp"
#include "edge_runtime/channel/event_notification.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/channel/loan.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/channel/sample.hpp"
#include "edge_runtime/common/schema.hpp"

namespace edge_runtime::detail {

class NotificationServer;

// ProducerHandle 固定当前实例的名称、代数、文件身份和 Schema；发布前会再次核对这些身份。
struct ProducerHandle {
	ShmObject shm;
	std::string channel_name;
	uint64_t generation{0};
	uint64_t role_epoch{0};
	uint64_t instance_nonce_hi{0};
	uint64_t instance_nonce_lo{0};
	std::array<std::byte, 32> schema_fingerprint{};
	uint32_t schema_version{0};
	uint32_t payload_size{0};
	ProcessIdentity self{};
	std::atomic<bool> operation_in_use{false};
	std::atomic<bool> shutdown_pending{false};
	std::atomic<bool> shutdown_started{false};

	Transport transport{Transport::kPosixShm};
	uint32_t channel_hash{0};
	std::string socket_path;
	UniqueFd listen_fd;
	std::atomic<bool> serve_stop{false};
	std::thread server_thread;
	bool socket_unlinked{false};

	uint64_t heartbeat_interval_ns{0};
	std::shared_ptr<NotificationServer> notification_server;

	~ProducerHandle() {

		if (server_thread.joinable()) {
			serve_stop.store(true, std::memory_order_relaxed);
			if (listen_fd.get() >= 0) {
				(void)::shutdown(listen_fd.get(), SHUT_RDWR);
			}
			server_thread.join();
		}
	}
	ProducerHandle(const ProducerHandle&) = delete;
	ProducerHandle& operator=(const ProducerHandle&) = delete;
	ProducerHandle() = default;
};

Result<std::shared_ptr<ProducerHandle>> producer_create_impl(const ChannelOptions& options,
                                                             const SchemaDescriptor& schema,
                                                             uint32_t payload_size);

Result<PublishInfo> producer_publish_impl(const std::shared_ptr<ProducerHandle>& handle,
                                          const std::byte* encoded, uint32_t encoded_size) noexcept;

Result<ChannelStatus> producer_status_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

uint64_t producer_generation_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

void producer_shutdown_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

Result<void> producer_remove_if_owner_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

Result<void> producer_heartbeat_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;
Result<void> producer_enable_event_notifications_impl(const std::shared_ptr<ProducerHandle>& handle,
                                                      NotificationBackend backend);

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_PRODUCER_IMPL_HPP
