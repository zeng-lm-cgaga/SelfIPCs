

#include <gtest/gtest.h>

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/channel_abi.hpp"
#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/channel/control_lock.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"
#include "retained_shm.hpp"

namespace {

using edge_runtime::ChannelOptions;
using edge_runtime::Consumer;
using edge_runtime::ErrorCode;
using edge_runtime::Producer;
using edge_runtime::SchemaDescriptor;
namespace detail = edge_runtime::detail;

std::string g_prod;
std::string g_cons;

bool spawn_with_failpoint(edge_test::SpawnedChild& child, const std::vector<std::string>& argv,
                          const char* id) {
	::setenv("EDGE_FAILPOINT", id, 1);
	::setenv("EDGE_FAILPOINT_MODE", "stop", 1);
	const bool ok = child.spawn(argv);
	::unsetenv("EDGE_FAILPOINT");
	::unsetenv("EDGE_FAILPOINT_MODE");
	return ok;
}

void kill_and_reap(edge_test::SpawnedChild& child, std::string* out = nullptr) {
	child.kill(SIGKILL);
	std::string tmp;
	child.wait(5000, out != nullptr ? out : &tmp);
}

bool process_is_stopped(pid_t pid) {
	char path[64];
	std::snprintf(path, sizeof(path), "/proc/%ld/stat", static_cast<long>(pid));
	std::FILE* f = std::fopen(path, "r");
	if (f == nullptr) return false;
	char line[512];
	const size_t n = std::fread(line, 1, sizeof(line) - 1, f);
	std::fclose(f);
	if (n == 0) return false;
	line[n] = '\0';
	const char* rp = std::strrchr(line, ')');
	if (rp == nullptr) return false;
	const char* s = rp + 1;
	while (*s == ' ') ++s;
	return *s == 'T';
}

bool wait_stopped(pid_t pid, int timeout_ms) {
	const int64_t deadline = edge_test::monotonic_ms_now() + timeout_ms;
	while (edge_test::monotonic_ms_now() < deadline) {
		if (process_is_stopped(pid)) return true;
		struct timespec ts {};
		ts.tv_nsec = 20 * 1000 * 1000;
		::nanosleep(&ts, nullptr);
	}
	return process_is_stopped(pid);
}

bool contains(const std::string& text, const char* needle) {
	return text.find(needle) != std::string::npos;
}

void retain_shm(const std::string& channel_name) {
	EXPECT_TRUE(edge_test::retain_shm_object(detail::channel_shm_name(channel_name)));
}

TEST(Recovery, PreObjectNarrowAutoClean) {
	const std::string name = edge_test::unique_channel_name("rec_clean");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();

	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(victim, {g_prod, "--name", name, "--count", "0"}, "C01"));
	ASSERT_TRUE(wait_stopped(victim.pid(), 3000)) << "producer never stopped at C01";
	kill_and_reap(victim);

	auto p = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_TRUE(p) << edge_runtime::to_string(p.error().code) << " " << p.error().context;
	auto st = p.value().status();
	ASSERT_TRUE(st);
	EXPECT_EQ(st.value().generation, 1u);
	EXPECT_TRUE(st.value().ready);
	ASSERT_TRUE(p.value().remove_if_owner());
}

TEST(Recovery, PreObjectNonEmptyBlocks) {
	const std::string name = edge_test::unique_channel_name("rec_nonempty");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();

	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(victim, {g_prod, "--name", name, "--count", "0"}, "C01"));
	ASSERT_TRUE(wait_stopped(victim.pid(), 3000)) << "producer never stopped at C01";
	kill_and_reap(victim);

	const std::string shm = detail::channel_shm_name(name);
	auto fd = detail::shm_open_existing(shm);
	ASSERT_TRUE(fd) << edge_runtime::to_string(fd.error().code);
	ASSERT_TRUE(detail::shm_truncate(fd.value(), 832));
	uint64_t dev = 0, ino = 0;
	ASSERT_TRUE(detail::shm_fstat_and_capture(fd.value(), &dev, &ino, nullptr));

	auto p = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_FALSE(p) << "recovery must not unlink a non-empty pre-object";
	EXPECT_EQ(p.error().code, ErrorCode::kRecoveryBlocked);

	ASSERT_TRUE(edge_test::retain_shm_object(shm));
}

TEST(Recovery, PartialObjectAutoRecovers) {
	const std::string name = edge_test::unique_channel_name("rec_partial");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();

	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(victim, {g_prod, "--name", name, "--count", "0"}, "C02"));
	ASSERT_TRUE(wait_stopped(victim.pid(), 3000)) << "producer never stopped at C02";
	kill_and_reap(victim);

	auto p = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_TRUE(p) << edge_runtime::to_string(p.error().code) << " " << p.error().context;
	auto st = p.value().status();
	ASSERT_TRUE(st);
	EXPECT_EQ(st.value().generation, 1u);
	ASSERT_TRUE(p.value().remove_if_owner());
}

TEST(Recovery, ABANameReuseBlocks) {
	const std::string name = edge_test::unique_channel_name("rec_aba");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();

	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(victim, {g_prod, "--name", name, "--count", "0"}, "C02"));
	ASSERT_TRUE(wait_stopped(victim.pid(), 3000)) << "producer never stopped at C02";
	kill_and_reap(victim);

	const std::string shm = detail::channel_shm_name(name);
	auto fd0 = detail::shm_open_existing(shm);
	ASSERT_TRUE(fd0);
	uint64_t dev0 = 0, ino0 = 0;
	ASSERT_TRUE(detail::shm_fstat_and_capture(fd0.value(), &dev0, &ino0, nullptr));

	ASSERT_TRUE(edge_test::preserve_aba_creator(shm));
	ASSERT_TRUE(detail::shm_unlink_checked(shm, dev0, ino0));
	auto fd1 = detail::shm_open_create(shm);
	ASSERT_TRUE(fd1);
	ASSERT_TRUE(detail::shm_truncate(fd1.value(), 832));
	uint64_t dev1 = 0, ino1 = 0;
	ASSERT_TRUE(detail::shm_fstat_and_capture(fd1.value(), &dev1, &ino1, nullptr));
	EXPECT_NE(ino0, ino1);

	auto p = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_FALSE(p) << "recovery must not touch an object the journal does not own";
	EXPECT_EQ(p.error().code, ErrorCode::kNameRaceDetected);

	ASSERT_TRUE(edge_test::retain_shm_object(shm));
}

TEST(Recovery, GenerationDisagreementBlocks) {
	const std::string name = edge_test::unique_channel_name("rec_gen");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();

	uint64_t gen1 = 0;
	uint64_t nonce1_hi = 0;
	uint64_t nonce1_lo = 0;
	{
		auto p = Producer<TestPayloadV1>::create(opts, schema);
		ASSERT_TRUE(p);
		auto st = p.value().status();
		ASSERT_TRUE(st);
		gen1 = st.value().generation;
		nonce1_hi = st.value().instance_nonce_hi;
		nonce1_lo = st.value().instance_nonce_lo;
	}

	{
		auto lock = detail::ControlLock::acquire(detail::channel_lock_path(name));
		ASSERT_TRUE(lock);
		auto rec = detail::make_control_journal(name, detail::JournalState::kIdle, 0,
		                                        gen1 + 10, 0, 0, 0, 0,
		                                        detail::current_process_identity());
		ASSERT_TRUE(lock.value().write_journal(rec));
	}

	auto p2 = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_FALSE(p2) << "journal/segment generation disagreement must fail closed";
	EXPECT_EQ(p2.error().code, ErrorCode::kRecoveryBlocked);

	{
		auto lock = detail::ControlLock::acquire(detail::channel_lock_path(name));
		ASSERT_TRUE(lock);
		auto rec = detail::make_control_journal(name, detail::JournalState::kIdle, 0, gen1,
		                                        nonce1_hi, nonce1_lo, nonce1_hi, nonce1_lo,
		                                        detail::current_process_identity());
		ASSERT_TRUE(lock.value().write_journal(rec));
	}
	auto p3 = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_TRUE(p3) << edge_runtime::to_string(p3.error().code) << " " << p3.error().context;
	auto st3 = p3.value().status();
	ASSERT_TRUE(st3);
	EXPECT_EQ(st3.value().generation, gen1 + 1);
	ASSERT_TRUE(p3.value().remove_if_owner());
}

TEST(Recovery, DeadProducerReplacementIncrementsGeneration) {
	const std::string name = edge_test::unique_channel_name("rec_i10");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();

	edge_test::SpawnedChild a;
	ASSERT_TRUE(a.spawn({g_prod, "--name", name, "--count", "0", "--interval-us", "10000"}));
	std::string out_a;
	EXPECT_FALSE(a.wait(1500, &out_a))
	        << "producer A exited unexpectedly: " << out_a;
	EXPECT_TRUE(contains(out_a, "GENERATION 1")) << out_a;

	edge_test::SpawnedChild b;
	ASSERT_TRUE(b.spawn({g_prod, "--name", name, "--count", "5"}));
	std::string out_b;
	ASSERT_TRUE(b.wait(10000, &out_b)) << "producer B hung";
	EXPECT_TRUE(contains(out_b, "GENERATION 2")) << out_b;

	auto consumer = Consumer<TestPayloadV1>::open(opts, schema);
	ASSERT_TRUE(consumer) << edge_runtime::to_string(consumer.error().code) << " "
	                      << consumer.error().context;
	auto snap = consumer.value().try_read_latest();
	ASSERT_TRUE(snap) << edge_runtime::to_string(snap.error().code);
	EXPECT_EQ(snap.value().generation, 2u);
	EXPECT_GT(snap.value().sequence, 0u);

	retain_shm(name);
}

TEST(Recovery, DeadConsumerSlotReclaim) {
	const std::string name = edge_test::unique_channel_name("rec_i11");

	edge_test::SpawnedChild prod;
	ASSERT_TRUE(
	        prod.spawn({g_prod, "--name", name, "--count", "0", "--interval-us", "250000"}));
	struct timespec ts {};
	ts.tv_nsec = 450 * 1000 * 1000;
	::nanosleep(&ts, nullptr);

	edge_test::SpawnedChild c08;
	ASSERT_TRUE(spawn_with_failpoint(c08, {g_cons, "--name", name, "--reads", "1"}, "C08"));
	ASSERT_TRUE(wait_stopped(c08.pid(), 3000)) << "consumer never stopped at C08";
	kill_and_reap(c08);

	edge_test::SpawnedChild c2;
	ASSERT_TRUE(c2.spawn({g_cons, "--name", name, "--reads", "5", "--read-interval-ms", "50"}));
	std::string out_c2;
	ASSERT_TRUE(c2.wait(10000, &out_c2)) << "recovery consumer hung";
	EXPECT_TRUE(contains(out_c2, "SUMMARY reads=5 torn=0")) << out_c2;

	std::string out_prod;
	EXPECT_FALSE(prod.wait(2500, &out_prod))
	        << "producer wedged on a leaked READING slot: " << out_prod;
	EXPECT_FALSE(contains(out_prod, "PUBLISH_FAIL")) << out_prod;

	retain_shm(name);
}

}

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc < 3) {
		std::fprintf(stderr, "usage: recovery_test <producer_bin> <consumer_bin>\n");
		return 2;
	}
	g_prod = argv[1];
	g_cons = argv[2];
	return RUN_ALL_TESTS();
}
