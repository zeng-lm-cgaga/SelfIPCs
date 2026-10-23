#ifndef EDGE_RUNTIME_CONSUMER_HPP
#define EDGE_RUNTIME_CONSUMER_HPP

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "edge_runtime/channel/channel_options.hpp"
#include "edge_runtime/channel/control_context.hpp"
#include "edge_runtime/channel/event_notification.hpp"
#include "edge_runtime/channel/loan.hpp"
#include "edge_runtime/channel/sample.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/common/schema.hpp"

namespace edge_runtime::detail {
struct ConsumerHandle;
Result<std::shared_ptr<ConsumerHandle>> consumer_open_controlled(const ChannelOptions&,
                                                                 const SchemaDescriptor&, uint32_t,
                                                                 const ControlOperationContext&,
                                                                 ControlReceipt&);
Result<ReconnectInfo> consumer_reconnect_controlled(const std::shared_ptr<ConsumerHandle>&,
                                                    const ControlOperationContext&,
                                                    ControlReceipt&) noexcept;
void consumer_request_stop(const std::shared_ptr<ConsumerHandle>&) noexcept;
Result<ControlCloseStatus> consumer_finalize(const std::shared_ptr<ConsumerHandle>&,
                                             const ControlOperationContext&,
                                             ControlReceipt&) noexcept;

// 读取实现只复制编码字节和这些元数据，模板层再负责把本地字节解码为 T。
struct ReadSnapshot {
	uint32_t encoded_size{0};  // encoded_out 中实际有效的字节数
	bool checksum_ok{true};
	uint64_t sample_sequence{0};
	uint64_t publish_boot_ns{0};
	uint64_t receive_boot_ns{0};
	uint64_t generation{0};
	std::array<std::byte, 16> instance_nonce{};
	uint64_t missed_samples{0};
};

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

}  // namespace edge_runtime::detail

namespace edge_runtime {

// 单生产者单消费者句柄。句柄独占映射，析构时释放资源；同一句柄不能并发调用。
template <typename T>
class Consumer {
       public:
	using value_type = T;
	// 打开时校验共享内存头、控制锁和消费者身份；已有存活消费者会被拒绝。

	static Result<Consumer> open(const ChannelOptions& options,
	                             const SchemaDescriptor& schema) {
		detail::validate_payload_codec<T>();
		auto h = detail::consumer_open_impl(options, schema, PayloadCodec<T>::kEncodedSize);
		if (!h) return h.error();
		return Consumer(std::move(h.value()));
	}
	// 读取过程先冻结槽、校验校验和并释放槽，再只在本地副本上解码。

	Result<Sample<T>> try_read_latest() noexcept {
		typename PayloadCodec<T>::EncodedBuffer encoded{};
		auto snap = detail::consumer_try_read_latest_impl(
		        handle_, encoded.data(), static_cast<uint32_t>(encoded.size()));
		if (!snap) return snap.error();
		if (!snap.value().checksum_ok) {
			return make_error(ErrorCode::kPayloadCorrupt, "Consumer::try_read_latest",
			                  "checksum mismatch");
		}
		Sample<T> out;
		if (!PayloadCodec<T>::decode(encoded.data(), snap.value().encoded_size,
		                             &out.value)) {
			return make_error(ErrorCode::kPayloadDecodeFailed,
			                  "Consumer::try_read_latest", "decode failed");
		}
		out.generation = snap.value().generation;
		out.instance_nonce = snap.value().instance_nonce;
		out.sequence = snap.value().sample_sequence;
		out.publish_boot_ns = snap.value().publish_boot_ns;
		out.receive_boot_ns = snap.value().receive_boot_ns;
		out.missed_samples = snap.value().missed_samples;
		return Result<Sample<T>>(std::move(out));
	}
	// 借用编码数据时槽保持 READING，直到 ReadLoan 释放或析构后才能复用。

	Result<ReadLoan> try_loan_latest() noexcept {
		return detail::consumer_try_loan_latest_impl(handle_);
	}
	// 等待使用绝对 MONOTONIC 截止时间；超时再按 Producer 存活状态分类。
	// EAGAIN、EINTR 和伪唤醒不会重置截止时间，零超时只做一次有限探测。
	Result<Sample<T>> wait_latest(std::chrono::nanoseconds timeout) noexcept {
		detail::validate_payload_codec<T>();
		typename PayloadCodec<T>::EncodedBuffer encoded{};
		const int64_t count = timeout.count();
		const uint64_t timeout_ns = count > 0 ? static_cast<uint64_t>(count) : 0;
		auto snap = detail::consumer_wait_latest_impl(
		        handle_, encoded.data(), static_cast<uint32_t>(encoded.size()), timeout_ns);
		if (!snap) return snap.error();
		if (!snap.value().checksum_ok) {
			return make_error(ErrorCode::kPayloadCorrupt, "Consumer::wait_latest",
			                  "checksum mismatch");
		}
		Sample<T> out;
		if (!PayloadCodec<T>::decode(encoded.data(), snap.value().encoded_size,
		                             &out.value)) {
			return make_error(ErrorCode::kPayloadDecodeFailed, "Consumer::wait_latest",
			                  "decode failed");
		}
		out.generation = snap.value().generation;
		out.instance_nonce = snap.value().instance_nonce;
		out.sequence = snap.value().sample_sequence;
		out.publish_boot_ns = snap.value().publish_boot_ns;
		out.receive_boot_ns = snap.value().receive_boot_ns;
		out.missed_samples = snap.value().missed_samples;
		return Result<Sample<T>>(std::move(out));
	}
	Result<ReadLoan> wait_loan_latest(std::chrono::nanoseconds timeout) noexcept {
		const int64_t count = timeout.count();
		const uint64_t timeout_ns = count > 0 ? static_cast<uint64_t>(count) : 0;
		return detail::consumer_wait_loan_latest_impl(handle_, timeout_ns);
	}
	static Result<Consumer> open(const ChannelOptions& options, const SchemaDescriptor& schema,
	                             const ControlOperationContext& context,
	                             ControlReceipt& receipt) {
		detail::validate_payload_codec<T>();
		auto handle = detail::consumer_open_controlled(
		        options, schema, PayloadCodec<T>::kEncodedSize, context, receipt);
		if (!handle) return handle.error();
		return Consumer(std::move(handle.value()));
	}
	Result<ReconnectInfo> reconnect(const ControlOperationContext& context,
	                                ControlReceipt& receipt) noexcept {
		return detail::consumer_reconnect_controlled(handle_, context, receipt);
	}
	Result<EventNotification> event_notification(
	        const ControlOperationContext& context, ControlReceipt& receipt,
	        NotificationBackend backend = NotificationBackend::kEventFd) {
		return detail::consumer_notification_controlled(handle_, backend, context, receipt);
	}
	void request_stop() const noexcept { detail::consumer_request_stop(handle_); }
	Result<ControlCloseStatus> finalize(const ControlOperationContext& context,
	                                    ControlReceipt& receipt) noexcept {
		return detail::consumer_finalize(handle_, context, receipt);
	}
	Result<ControlCloseStatus> close(const ControlOperationContext& context,
	                                 ControlReceipt& receipt) noexcept {
		request_stop();
		return finalize(context, receipt);
	}

	// 针对已替换实例重新打开并更新句柄资源。
	Result<ReconnectInfo> reconnect() noexcept {
		return detail::consumer_reconnect_impl(handle_);
	}
	Result<EventNotification> event_notification(
	        std::chrono::milliseconds timeout = std::chrono::milliseconds{1000}) {
		if (timeout.count() < 0) {
			return make_error(ErrorCode::kInvalidOptions, "Consumer::event_notification",
			                  "negative timeout");
		}
		return detail::consumer_event_notification_impl(
		        handle_, static_cast<uint64_t>(timeout.count()));
	}
	Result<EventNotification> event_notification(
	        NotificationBackend backend,
	        std::chrono::milliseconds timeout = std::chrono::milliseconds{1000}) {
		if (timeout.count() < 0) {
			return make_error(ErrorCode::kInvalidOptions,
			                  "Consumer::event_notification", "negative timeout");
		}
		return detail::consumer_event_notification_impl(
		        handle_, static_cast<uint64_t>(timeout.count()), backend);
	}

	// 慢路径诊断：重新确认名称与实例绑定关系后读取通道状态。
	Result<ChannelStatus> status() const noexcept {
		return detail::consumer_status_impl(handle_);
	}

	Consumer(const Consumer&) = delete;
	Consumer& operator=(const Consumer&) = delete;
	Consumer(Consumer&& other) noexcept : handle_(std::move(other.handle_)) {}
	Consumer& operator=(Consumer&& other) noexcept {
		if (this == &other) return *this;
		if (handle_) detail::consumer_shutdown_impl(handle_);
		handle_ = std::move(other.handle_);
		return *this;
	}
	// 尽力执行干净关闭，将消费者状态标记为 OFFLINE，避免同进程重开被误判为占用。
	~Consumer() {
		if (handle_) detail::consumer_shutdown_impl(handle_);
	}

       private:
	explicit Consumer(std::shared_ptr<detail::ConsumerHandle> h) : handle_(std::move(h)) {}

	std::shared_ptr<detail::ConsumerHandle> handle_;
};

}  // 命名空间 edge_runtime

#endif  // EDGE_RUNTIME_CONSUMER_HPP
