#include "edge_runtime/common/error.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>

namespace edge_runtime {

namespace {

constexpr const char* kErrorNames[] = {
        "InvalidName",
        "InvalidOptions",
        "NotFound",
        "PermissionDenied",
        "AlreadyOwned",
        "ConsumerAlreadyOwned",
        "InitializationIncomplete",
        "CorruptHeader",
        "AbiMismatch",
        "SchemaMismatch",
        "UnsupportedPlatform",
        "NoNewSample",
        "NoWritableSlot",
        "ReadContention",
        "Timeout",
        "DataStale",
        "ProducerOffline",
        "RecoveryBlocked",
        "StaleHandle",
        "PayloadCorrupt",
        "PayloadEncodeFailed",
        "PayloadDecodeFailed",
        "CorruptSlot",
        "NameRaceDetected",
        "ClockAnomaly",
        "SequenceExhausted",
        "ConcurrentHandleUse",
        "SystemError",
        "ProducerStalled",
        "TransportFailed",
        "SupervisionExhausted",
	"QueueFull",
	"QueueEmpty",
	"QueueContention",
	"BufferPoolFull",
	"BufferPoolContention",
	"BufferInvalidHandle",
	"BufferStateMismatch",
	"Closed",
	"Cancelled",
};
// 错误码名称表必须与枚举保持一一对应，名称会出现在工具输出和日志中。
static_assert(sizeof(kErrorNames) / sizeof(kErrorNames[0]) ==
			static_cast<size_t>(ErrorCode::kCancelled) + 1u,
              "error name table must match the frozen ErrorCode enum");

}

Error::Error(ErrorCode c, const char* op, const char* ctx) : code(c), operation(op) {
	if (ctx != nullptr) {
		std::snprintf(context, sizeof(context), "%s", ctx);
	}
}

Error::Error(ErrorCode c, int saved_errno, const char* op, const char* ctx)
    : code(c), errno_value(saved_errno), operation(op) {
	if (ctx != nullptr) {
		std::snprintf(context, sizeof(context), "%s", ctx);
	}
}

const char* to_string(ErrorCode code) noexcept {
	const uint32_t index = static_cast<uint32_t>(code);
	if (index > static_cast<uint32_t>(ErrorCode::kCancelled)) return "UnknownError";
	return kErrorNames[index];
}

ErrorCode classify_errno(int errno_value) noexcept {
	switch (errno_value) {
		case ENOENT:
			return ErrorCode::kNotFound;
		case EEXIST:
			return ErrorCode::kAlreadyOwned;
		case EACCES:
		case EPERM:
			return ErrorCode::kPermissionDenied;
		default:
			return ErrorCode::kSystemError;
	}
}

Error make_error(ErrorCode code, const char* operation, const char* context) noexcept {
	return Error(code, operation, context);
}

Error make_errno_error(int saved_errno, const char* operation, const char* context) noexcept {
	return Error(classify_errno(saved_errno), saved_errno, operation, context);
}

Error make_errno_error(ErrorCode code, int saved_errno, const char* operation,
                       const char* context) noexcept {
	return Error(code, saved_errno, operation, context);
}

}
