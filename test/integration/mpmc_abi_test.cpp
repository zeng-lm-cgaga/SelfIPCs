#include <gtest/gtest.h>

#include <cstdio>
#include <string>
#include <vector>

#include <sys/mman.h>

#include "edge_runtime/common/error.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime/queue/mpmc_layout.hpp"
#include "mpmc_payload.hpp"
#include "retained_shm.hpp"
#include "test_util.hpp"

namespace {

using Queue = edge_runtime::MpmcQueue<MpmcTestPayload>;
using edge_runtime::ErrorCode;
using edge_runtime::MpmcQueueOptions;

std::string g_legacy_helper;

MpmcQueueOptions options_for(const std::string& name, uint32_t capacity) {
	MpmcQueueOptions options;
	options.name = name;
	options.capacity = capacity;
	return options;
}

TEST(MpmcAbi, LegacyBinaryRejectsV2Mapping) {
	const std::string name = edge_test::unique_channel_name("mpmc_legacy_reader");
	auto queue = Queue::create(options_for(name, 2), MpmcTestSchema());
	ASSERT_TRUE(queue);

	const auto result = edge_test::run_child_capture(
	        {g_legacy_helper, "--mode", "open", "--name", name}, 10000);
	ASSERT_FALSE(result.timed_out);
	EXPECT_EQ(result.exit_code, 0) << result.stdout_text;
	EXPECT_NE(result.stdout_text.find("LEGACY_REJECT code=AbiMismatch"), std::string::npos);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcAbi, V2BinaryRejectsLegacyMappingBeforeEpochRead) {
	const std::string name = edge_test::unique_channel_name("mpmc_legacy_object");
	const auto created = edge_test::run_child_capture(
	        {g_legacy_helper, "--mode", "create", "--name", name, "--capacity", "2",
	         "--payload-size", std::to_string(sizeof(MpmcTestPayload))},
	        10000);
	ASSERT_FALSE(created.timed_out);
	ASSERT_EQ(created.exit_code, 0) << created.stdout_text;

	const auto opened = Queue::open(options_for(name, 2), MpmcTestSchema());
	ASSERT_FALSE(opened);
	EXPECT_EQ(opened.error().code, ErrorCode::kAbiMismatch);
	EXPECT_TRUE(edge_test::retain_shm_object(edge_runtime::detail::mpmc_shm_name(name)));
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc != 2) {
		std::fprintf(stderr, "usage: mpmc_abi_test <legacy_helper>\n");
		return 2;
	}
	g_legacy_helper = argv[1];
	return RUN_ALL_TESTS();
}
