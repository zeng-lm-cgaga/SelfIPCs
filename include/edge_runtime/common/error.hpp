#ifndef EDGE_RUNTIME_ERROR_HPP
#define EDGE_RUNTIME_ERROR_HPP

#include <cstdint>

namespace edge_runtime {

enum class ErrorCode : uint32_t {
	kInvalidName = 0,
	kInvalidOptions,
	kNotFound,
	kPermissionDenied,
	kAlreadyOwned,
	kConsumerAlreadyOwned,
	kInitializationIncomplete,
	kCorruptHeader,
	kAbiMismatch,
	kSchemaMismatch,
	kUnsupportedPlatform,
	kNoNewSample,
	kNoWritableSlot,
	kReadContention,
	kTimeout,
	kDataStale,
	kProducerOffline,
	kRecoveryBlocked,
	kStaleHandle,
	kPayloadCorrupt,
	kPayloadEncodeFailed,
	kPayloadDecodeFailed,
	kCorruptSlot,
	kNameRaceDetected,
	kClockAnomaly,
	kSequenceExhausted,
	kConcurrentHandleUse,
	kSystemError,

	// 新增错误码只能追加，数值顺序是跨进程稳定契约。
	kProducerStalled,
	kTransportFailed,

	// 监督器达到重启上限，后续不再自动拉起子进程。
	kSupervisionExhausted,

	// MPMC bounded queue 的非阻塞结果与并发门控状态。
	kQueueFull,
	kQueueEmpty,
	kQueueContention,

	// SharedBufferPool 固定块生命周期与 handle 校验结果。
	kBufferPoolFull,
	kBufferPoolContention,
	kBufferInvalidHandle,
	kBufferStateMismatch,
	kClosed,
	kCancelled,
};

// 固定大小错误载荷，热路径不分配内存也不抛异常。
struct Error {
	ErrorCode code{ErrorCode::kSystemError};
	int errno_value{0};
	const char* operation{nullptr};
	char context[64]{};

	constexpr Error() = default;
	Error(ErrorCode c, const char* op, const char* ctx = nullptr);
	Error(ErrorCode c, int saved_errno, const char* op, const char* ctx = nullptr);
};

const char* to_string(ErrorCode code) noexcept;

ErrorCode classify_errno(int errno_value) noexcept;

Error make_error(ErrorCode code, const char* operation, const char* context = nullptr) noexcept;

Error make_errno_error(int saved_errno, const char* operation,
                       const char* context = nullptr) noexcept;
Error make_errno_error(ErrorCode code, int saved_errno, const char* operation,
                       const char* context = nullptr) noexcept;

}  // 命名空间 edge_runtime

#endif  // EDGE_RUNTIME_ERROR_HPP
