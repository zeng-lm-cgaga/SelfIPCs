#ifndef EDGE_RUNTIME_PRODUCER_HPP
#define EDGE_RUNTIME_PRODUCER_HPP

#include <cstdint>
#include <memory>
#include <utility>

#include "edge_runtime/channel/channel_options.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/channel/loan.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/channel/sample.hpp"
#include "edge_runtime/common/schema.hpp"

namespace edge_runtime::detail {

struct ProducerHandle;
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
Result<void> producer_enable_event_notifications_impl(const std::shared_ptr<ProducerHandle>& handle);
Result<void> producer_disable_event_notifications_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;
int producer_event_notification_error_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

}  // 命名空间 edge_runtime::detail

namespace edge_runtime {

// 单生产者单消费者句柄。句柄独占映射，析构时释放资源；同一句柄不能并发调用。
template <typename T>
class Producer {
       public:
	using value_type = T;

	// 创建事务会校验控制锁、实例身份，并在确认旧实例失效后提交 READY 状态。
	static Result<Producer> create(const ChannelOptions& options,
	                               const SchemaDescriptor& schema) {
		detail::validate_payload_codec<T>();
		auto h = detail::producer_create_impl(options, schema,
		                                      PayloadCodec<T>::kEncodedSize);
		if (!h) return h.error();
		return Producer(std::move(h.value()));
	}

	// 编码失败时不触碰共享内存；成功后只发布完整的最新样本。
	Result<PublishInfo> publish(const T& value) noexcept {
		typename PayloadCodec<T>::EncodedBuffer encoded{};
		const uint32_t size = PayloadCodec<T>::kEncodedSize;
		if (!PayloadCodec<T>::encode(value, encoded.data(), encoded.size())) {
			return make_error(ErrorCode::kPayloadEncodeFailed, "Producer::publish",
			                  "encode failed");
		}
		return detail::producer_publish_impl(handle_, encoded.data(), size);
	}

	// 借用一个槽直接写入规范编码；只有 commit() 才会让 WRITING 对消费者可见。
	Result<WriteLoan> loan() noexcept { return detail::producer_loan_impl(handle_); }

	// 慢路径诊断：确认名称仍指向当前实例后读取通道状态。
	Result<ChannelStatus> status() const noexcept {
		return detail::producer_status_impl(handle_);
	}

	// 当前实例代数；实例被验证替换后递增。
	uint64_t generation() const noexcept { return detail::producer_generation_impl(handle_); }

	// 仅删除仍由当前句柄拥有的实例，析构函数不会隐式执行删除。
	Result<void> remove_if_owner() noexcept {
		return detail::producer_remove_if_owner_impl(handle_);
	}

	// 向共享状态写入应用进度；关闭心跳时该调用不改变数据。
	Result<void> heartbeat() noexcept {
		return detail::producer_heartbeat_impl(handle_);
	}

	Result<void> enable_event_notifications() {
		return detail::producer_enable_event_notifications_impl(handle_);
	}
	Result<void> disable_event_notifications() noexcept {
		return detail::producer_disable_event_notifications_impl(handle_);
	}
	int event_notification_error() const noexcept {
		return detail::producer_event_notification_error_impl(handle_);
	}

	Producer(const Producer&) = delete;
	Producer& operator=(const Producer&) = delete;
	Producer(Producer&& other) noexcept : handle_(std::move(other.handle_)) {}
	Producer& operator=(Producer&& other) noexcept {
		if (this == &other) return *this;
		if (handle_) detail::producer_shutdown_impl(handle_);
		handle_ = std::move(other.handle_);
		return *this;
	}

	// 尽力执行干净关闭，将 Producer 状态标记为 OFFLINE。
	~Producer() {
		if (handle_) detail::producer_shutdown_impl(handle_);
	}

       private:
	explicit Producer(std::shared_ptr<detail::ProducerHandle> h) : handle_(std::move(h)) {}

	std::shared_ptr<detail::ProducerHandle> handle_;
};

}  // 命名空间 edge_runtime

#endif  // EDGE_RUNTIME_PRODUCER_HPP
