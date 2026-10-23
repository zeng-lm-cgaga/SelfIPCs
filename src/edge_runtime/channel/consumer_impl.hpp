#ifndef EDGE_RUNTIME_DETAIL_CONSUMER_IMPL_HPP
#define EDGE_RUNTIME_DETAIL_CONSUMER_IMPL_HPP

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "edge_runtime/channel/channel_options.hpp"
#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/sample.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/common/schema.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/transport/shm_object.hpp"

namespace edge_runtime::detail {
struct EventNotificationState;

// ConsumerHandle 保存映射身份和读取游标；operation_in_use 防止同一句柄重入。
struct ConsumerHandle {
	ShmObject shm;
	std::string channel_name;
	Transport transport{Transport::kPosixShm};
	uint64_t generation{0};
	uint64_t role_epoch{0};
	uint64_t instance_nonce_hi{0};
	uint64_t instance_nonce_lo{0};
	std::array<std::byte, 32> schema_fingerprint{};
	uint32_t schema_version{0};
	uint32_t payload_size{0};
	ProcessIdentity self{};
	uint64_t owner_pid{};

	uint64_t reconnect_timeout_ms{1000};
	uint64_t last_sequence{0};
	bool first_sample_in_generation{true};
	std::atomic<bool> operation_in_use{false};
	std::atomic<bool> shutdown_pending{false};
	std::atomic<bool> shutdown_started{false};
	std::atomic<bool> controlled_stop{false};
	std::atomic<bool> legacy_cleanup{false};
	CancellationSource control_stop;
	bool control_closed{false};
	std::vector<std::weak_ptr<EventNotificationState>> notifications;
};

void consumer_clear_notifications(ConsumerHandle& handle) noexcept;

Result<std::shared_ptr<ConsumerHandle>> consumer_open_impl(const ChannelOptions& options,
                                                           const SchemaDescriptor& schema,
                                                           uint32_t payload_size);

Result<ReadSnapshot> consumer_try_read_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                                   std::byte* encoded_out,
                                                   uint32_t encoded_cap) noexcept;

Result<ReadSnapshot> consumer_wait_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                               std::byte* encoded_out, uint32_t encoded_cap,
                                               uint64_t timeout_ns) noexcept;

Result<ReconnectInfo> consumer_reconnect_impl(
        const std::shared_ptr<ConsumerHandle>& handle) noexcept;

Result<ChannelStatus> consumer_status_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept;

void consumer_shutdown_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CONSUMER_IMPL_HPP
