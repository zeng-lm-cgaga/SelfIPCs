

#include "edge_runtime/common/error.hpp"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>

namespace {

using edge_runtime::classify_errno;
using edge_runtime::Error;
using edge_runtime::ErrorCode;
using edge_runtime::make_errno_error;
using edge_runtime::to_string;

TEST(ErrorModel, EveryCodeHasStableName) {
	EXPECT_STREQ(to_string(ErrorCode::kInvalidName), "InvalidName");
	EXPECT_STREQ(to_string(ErrorCode::kSystemError), "SystemError");

	EXPECT_STREQ(to_string(ErrorCode::kProducerStalled), "ProducerStalled");
	EXPECT_STREQ(to_string(ErrorCode::kTransportFailed), "TransportFailed");

	EXPECT_STREQ(to_string(ErrorCode::kSupervisionExhausted), "SupervisionExhausted");

	EXPECT_STREQ(to_string(ErrorCode::kClosed), "Closed");
	EXPECT_STREQ(to_string(ErrorCode::kCancelled), "Cancelled");
	for (uint32_t index = 0; index <= static_cast<uint32_t>(ErrorCode::kCancelled); ++index) {
		const char* name = to_string(static_cast<ErrorCode>(index));
		EXPECT_NE(name, nullptr);
		EXPECT_STRNE(name, "UnknownError");
	}

	EXPECT_STREQ(to_string(static_cast<ErrorCode>(0xFFFFu)), "UnknownError");
}

TEST(ErrorModel, ErrnoClassification) {
	EXPECT_EQ(classify_errno(ENOENT), ErrorCode::kNotFound);
	EXPECT_EQ(classify_errno(EEXIST), ErrorCode::kAlreadyOwned);
	EXPECT_EQ(classify_errno(EACCES), ErrorCode::kPermissionDenied);
	EXPECT_EQ(classify_errno(EPERM), ErrorCode::kPermissionDenied);
	EXPECT_EQ(classify_errno(EBADF), ErrorCode::kSystemError);
	EXPECT_EQ(classify_errno(0), ErrorCode::kSystemError);
}

TEST(ErrorModel, FixedContextNoAlloc) {
	Error e(ErrorCode::kPermissionDenied, "test_op", "the context");
	EXPECT_EQ(e.code, ErrorCode::kPermissionDenied);
	EXPECT_STREQ(e.operation, "test_op");
	EXPECT_STREQ(e.context, "the context");
	EXPECT_EQ(e.errno_value, 0);
}

TEST(ErrorModel, ContextTruncation) {
	std::string long_ctx(300, 'x');
	Error e(ErrorCode::kSystemError, "op2", long_ctx.c_str());
	const size_t len = std::strlen(e.context);
	EXPECT_GT(len, 0u);
	EXPECT_LT(len, 64u);
	EXPECT_EQ(e.context[len], '\0');
}

TEST(ErrorModel, NullContext) {
	Error e(ErrorCode::kNotFound, "op3");
	EXPECT_STREQ(e.context, "");
	EXPECT_STREQ(e.operation, "op3");
}

TEST(ErrorModel, SyscallErrorPreservesErrno) {
	const Error classified = make_errno_error(EACCES, "open", "permission denied");
	EXPECT_EQ(classified.code, ErrorCode::kPermissionDenied);
	EXPECT_EQ(classified.errno_value, EACCES);
	EXPECT_STREQ(classified.operation, "open");

	const Error domain = make_errno_error(ErrorCode::kTransportFailed, ECONNREFUSED,
	                                      "connect", "connection refused");
	EXPECT_EQ(domain.code, ErrorCode::kTransportFailed);
	EXPECT_EQ(domain.errno_value, ECONNREFUSED);
}

}
