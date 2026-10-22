#include "edge_runtime/buffer/shared_buffer_pool.hpp"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "edge_runtime/buffer/shared_buffer_pool_layout.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/queue/mpmc_layout.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "shared_buffer_pool_payload.hpp"
#include "test_util.hpp"

namespace {

using Queue = edge_runtime::MpmcQueue<edge_runtime::BufferHandle>;
using edge_runtime::BufferHandle;
using edge_runtime::ErrorCode;
using edge_runtime::SharedBufferPool;
using edge_runtime::SharedBufferPoolOptions;
using edge_runtime::detail::kSharedBufferPoolLegacyMagic;
using edge_runtime::detail::SharedBufferBlockHeaderAbi;
using edge_runtime::detail::SharedBufferPoolHeaderAbi;

std::string g_helper;

std::string pool_shm_name_for(const std::string& name) {
	return "/edgeruntime.bufferpool." + std::to_string(getuid()) + "." + name;
}

class ScopedObjectEvidence {
       public:
	explicit ScopedObjectEvidence(std::string name) : name_(std::move(name)) {}
	~ScopedObjectEvidence() {
		const int descriptor =
		        ::shm_open(name_.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW, 0);
		if (descriptor < 0) {
			std::printf("POOL_OBJECT_ABSENT name=%s errno=%d observer=%ld\n",
			            name_.c_str(), errno, static_cast<long>(::getpid()));
			return;
		}
		struct stat metadata {};
		if (::fstat(descriptor, &metadata) == 0) {
			std::printf(
			        "POOL_OBJECT_PRESERVED name=%s dev=%llu inode=%llu size=%lld "
			        "observer=%ld\n",
			        name_.c_str(), static_cast<unsigned long long>(metadata.st_dev),
			        static_cast<unsigned long long>(metadata.st_ino),
			        static_cast<long long>(metadata.st_size),
			        static_cast<long>(::getpid()));
		} else {
			std::printf("POOL_OBJECT_INSPECTION_FAILED name=%s errno=%d observer=%ld\n",
			            name_.c_str(), errno, static_cast<long>(::getpid()));
		}
		::close(descriptor);
	}

	ScopedObjectEvidence(const ScopedObjectEvidence&) = delete;
	ScopedObjectEvidence& operator=(const ScopedObjectEvidence&) = delete;

       private:
	std::string name_;
};

SharedBufferPoolOptions pool_options(const std::string& name) {
	SharedBufferPoolOptions options;
	options.name = name;
	options.block_size = 4096;
	options.block_count = 1;
	options.schema = SharedBufferPoolTestSchema();
	return options;
}

edge_runtime::MpmcQueueOptions queue_options(const std::string& name) {
	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = 4;
	return options;
}

std::vector<std::string> child_args(const std::string& mode, const std::string& pool_name) {
	return {g_helper, "--mode", mode, "--pool-name", pool_name};
}

std::vector<std::string> status_args(const std::string& pool_name) {
	return child_args("status", pool_name);
}

bool process_is_stopped(pid_t pid) {
	char path[64];
	std::snprintf(path, sizeof(path), "/proc/%ld/stat", static_cast<long>(pid));
	std::FILE* file = std::fopen(path, "r");
	if (file == nullptr) return false;
	char line[512];
	const size_t size = std::fread(line, 1, sizeof(line) - 1, file);
	std::fclose(file);
	if (size == 0) return false;
	line[size] = '\0';
	const char* close_paren = std::strrchr(line, ')');
	if (close_paren == nullptr) return false;
	const char* state = close_paren + 1;
	while (*state == ' ') ++state;
	return *state == 'T';
}

bool process_is_running(pid_t pid) {
	char path[64];
	std::snprintf(path, sizeof(path), "/proc/%ld/stat", static_cast<long>(pid));
	std::FILE* file = std::fopen(path, "r");
	if (file == nullptr) return false;
	char line[512];
	const size_t size = std::fread(line, 1, sizeof(line) - 1, file);
	std::fclose(file);
	if (size == 0) return false;
	line[size] = '\0';
	const char* close_paren = std::strrchr(line, ')');
	if (close_paren == nullptr) return false;
	const char* state = close_paren + 1;
	while (*state == ' ') ++state;
	return *state != '\0' && *state != 'Z';
}

bool wait_stopped(pid_t pid, int timeout_ms) {
	const int64_t deadline = edge_test::monotonic_ms_now() + timeout_ms;
	while (edge_test::monotonic_ms_now() < deadline) {
		if (process_is_stopped(pid)) return true;
		struct timespec delay {};
		delay.tv_nsec = 10 * 1000 * 1000L;
		::nanosleep(&delay, nullptr);
	}
	return process_is_stopped(pid);
}

std::string kill_and_reap(edge_test::SpawnedChild* child) {
	child->kill(SIGKILL);
	std::string output;
	(void)child->wait(5000, &output);
	return output;
}

bool spawn_with_failpoint(edge_test::SpawnedChild* child, const std::vector<std::string>& args,
                          const char* failpoint) {
	::setenv("EDGE_FAILPOINT", failpoint, 1);
	::setenv("EDGE_FAILPOINT_MODE", "stop", 1);
	const bool spawned = child->spawn(args);
	::unsetenv("EDGE_FAILPOINT");
	::unsetenv("EDGE_FAILPOINT_MODE");
	return spawned;
}

bool continue_and_reap(edge_test::SpawnedChild* child, std::string* output) {
	if (::kill(child->pid(), SIGCONT) != 0) return false;
	return child->wait(10000, output) && child->reaped();
}

bool wait_for_pipe_token(int fd, const char* token, int timeout_ms) {
	std::string received;
	const int64_t deadline = edge_test::monotonic_ms_now() + timeout_ms;
	for (;;) {
		if (received.find(token) != std::string::npos) return true;
		const int64_t remaining = deadline - edge_test::monotonic_ms_now();
		if (remaining <= 0) return false;
		struct pollfd descriptor {
			fd, POLLIN, 0
		};
		const int rc = ::poll(&descriptor, 1, static_cast<int>(remaining));
		if (rc < 0 && errno == EINTR) continue;
		if (rc <= 0) return false;
		char buffer[128];
		const ssize_t count = ::read(fd, buffer, sizeof(buffer));
		if (count > 0) {
			received.append(buffer, static_cast<size_t>(count));
			continue;
		}
		if (count < 0 && errno == EINTR) continue;
		return false;
	}
}

std::vector<std::string> crash_reader_args(const std::string& pool_name,
                                           const BufferHandle& handle) {
	auto args = child_args("crash-reader", pool_name);
	args.insert(args.end(),
	            {"--pool-generation", std::to_string(handle.pool_generation),
	             "--block-generation", std::to_string(handle.block_generation), "--nonce-hi",
	             std::to_string(handle.instance_nonce_hi), "--nonce-lo",
	             std::to_string(handle.instance_nonce_lo), "--offset",
	             std::to_string(handle.offset), "--block-id", std::to_string(handle.block_id),
	             "--length", std::to_string(handle.length)});
	return args;
}

std::vector<std::string> stale_reader_args(const std::string& pool_name,
                                           const BufferHandle& handle) {
	auto args = child_args("stale-reader", pool_name);
	args.insert(args.end(),
	            {"--pool-generation", std::to_string(handle.pool_generation),
	             "--block-generation", std::to_string(handle.block_generation), "--nonce-hi",
	             std::to_string(handle.instance_nonce_hi), "--nonce-lo",
	             std::to_string(handle.instance_nonce_lo), "--offset",
	             std::to_string(handle.offset), "--block-id", std::to_string(handle.block_id),
	             "--length", std::to_string(handle.length)});
	return args;
}

std::vector<std::string> recycle_args(const std::string& pool_name, const BufferHandle& handle) {
	auto args = child_args("recycle", pool_name);
	args.insert(args.end(),
	            {"--pool-generation", std::to_string(handle.pool_generation),
	             "--block-generation", std::to_string(handle.block_generation), "--nonce-hi",
	             std::to_string(handle.instance_nonce_hi), "--nonce-lo",
	             std::to_string(handle.instance_nonce_lo), "--offset",
	             std::to_string(handle.offset), "--block-id", std::to_string(handle.block_id),
	             "--length", std::to_string(handle.length)});
	return args;
}

TEST(SharedBufferPoolIntegration, QueueDescriptorChainAndBlockReuse) {
	const std::string pool_name = edge_test::unique_channel_name("buffer_pool_chain");
	const std::string queue_name = edge_test::unique_channel_name("buffer_handle_queue");
	ScopedObjectEvidence pool_cleanup(pool_shm_name_for(pool_name));
	ScopedObjectEvidence queue_cleanup(edge_runtime::detail::mpmc_shm_name(queue_name));

	auto queue = Queue::create(queue_options(queue_name), SharedBufferHandleQueueSchema());
	ASSERT_TRUE(queue) << edge_runtime::to_string(queue.error().code) << " "
	                   << queue.error().context;

	edge_test::SpawnedChild producer;
	edge_test::SpawnedChild consumer;
	auto producer_argv = child_args("producer", pool_name);
	producer_argv.insert(producer_argv.end(), {"--queue-name", queue_name, "--count", "8"});
	ASSERT_TRUE(producer.spawn(producer_argv));
	auto consumer_argv = child_args("consumer", pool_name);
	consumer_argv.insert(consumer_argv.end(), {"--queue-name", queue_name, "--count", "8"});
	ASSERT_TRUE(consumer.spawn(consumer_argv));

	std::string producer_output;
	std::string consumer_output;
	ASSERT_TRUE(producer.wait(30000, &producer_output)) << producer_output;
	ASSERT_TRUE(consumer.wait(30000, &consumer_output)) << consumer_output;
	EXPECT_EQ(producer.exit_code(), 0) << producer_output;
	EXPECT_EQ(consumer.exit_code(), 0) << consumer_output;
	EXPECT_NE(producer_output.find("PRODUCED count=8 generations=8"), std::string::npos);
	EXPECT_NE(consumer_output.find("CONSUMED count=8 generations=8"), std::string::npos);

	if (::testing::Test::HasFailure()) return;
	auto removed = queue.value().remove_if_creator();
	ASSERT_TRUE(removed) << edge_runtime::to_string(removed.error().code) << " "
	                     << removed.error().context;
}

TEST(SharedBufferPoolIntegration, StatusCountsObservedReadingRoleAfterPublish) {
	const std::string pool_name = edge_test::unique_channel_name("buffer_pool_status_reading");
	ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
	auto pool = SharedBufferPool::create(pool_options(pool_name));
	ASSERT_TRUE(pool);

	auto write = pool.value().try_acquire_write();
	ASSERT_TRUE(write);
	std::memset(write.value().data(), 0x4A, write.value().size());

	edge_test::SpawnedChild observer;
	ASSERT_TRUE(spawn_with_failpoint(&observer, status_args(pool_name),
	                                 "SHARED_BUFFER_POOL_OWNER_SNAPSHOT_AFTER_STATE1"));
	ASSERT_TRUE(wait_stopped(observer.pid(), 5000));

	auto published = write.value().publish();
	ASSERT_TRUE(published);
	edge_test::SpawnedChild reader;
	ASSERT_TRUE(reader.spawn(crash_reader_args(pool_name, published.value())));
	ASSERT_TRUE(wait_stopped(reader.pid(), 5000));

	std::string observer_output;
	ASSERT_TRUE(continue_and_reap(&observer, &observer_output)) << observer_output;
	EXPECT_EQ(observer.exit_code(), 0) << observer_output;
	EXPECT_NE(observer_output.find("STATUS_OK free=0 writing=0 published=0 reading=1"),
	          std::string::npos)
	        << observer_output;

	const std::string reader_output = kill_and_reap(&reader);
	EXPECT_TRUE(reader.reaped()) << reader_output;
	EXPECT_NE(reader_output.find("READY"), std::string::npos) << reader_output;
}

TEST(SharedBufferPoolIntegration, StatusCountsObservedWritingRoleAfterReadRelease) {
	const std::string pool_name = edge_test::unique_channel_name("buffer_pool_status_writing");
	ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
	auto pool = SharedBufferPool::create(pool_options(pool_name));
	ASSERT_TRUE(pool);

	auto write = pool.value().try_acquire_write();
	ASSERT_TRUE(write);
	std::memset(write.value().data(), 0x4B, write.value().size());
	auto published = write.value().publish();
	ASSERT_TRUE(published);
	auto read = pool.value().acquire_read(published.value());
	ASSERT_TRUE(read);

	edge_test::SpawnedChild observer;
	ASSERT_TRUE(spawn_with_failpoint(&observer, status_args(pool_name),
	                                 "SHARED_BUFFER_POOL_OWNER_SNAPSHOT_AFTER_STATE1"));
	ASSERT_TRUE(wait_stopped(observer.pid(), 5000));

	read.value().release();
	auto replacement = pool.value().try_acquire_write();
	ASSERT_TRUE(replacement);

	std::string observer_output;
	ASSERT_TRUE(continue_and_reap(&observer, &observer_output)) << observer_output;
	EXPECT_EQ(observer.exit_code(), 0) << observer_output;
	EXPECT_NE(observer_output.find("STATUS_OK free=0 writing=1 published=0 reading=0"),
	          std::string::npos)
	        << observer_output;
	replacement.value().abort();
	if (!::testing::Test::HasFailure()) {
		ASSERT_TRUE(pool.value().remove_if_creator());
	}
}

TEST(SharedBufferPoolIntegration, ChainProducerRetriesStatusContentionDuringReaderClaim) {
	const std::string pool_name =
	        edge_test::unique_channel_name("buffer_pool_chain_contention");
	const std::string queue_name =
	        edge_test::unique_channel_name("buffer_handle_chain_contention");
	ScopedObjectEvidence pool_cleanup(pool_shm_name_for(pool_name));
	ScopedObjectEvidence queue_cleanup(edge_runtime::detail::mpmc_shm_name(queue_name));

	auto queue = Queue::create(queue_options(queue_name), SharedBufferHandleQueueSchema());
	ASSERT_TRUE(queue) << edge_runtime::to_string(queue.error().code) << " "
	                   << queue.error().context;

	edge_test::SpawnedChild producer;
	int status_pipe[2] = {-1, -1};
	ASSERT_EQ(::pipe(status_pipe), 0);
	auto producer_args = child_args("producer", pool_name);
	producer_args.insert(producer_args.end(),
	                     {"--queue-name", queue_name, "--count", "1", "--status-event-fd",
	                      std::to_string(status_pipe[1])});
	ASSERT_TRUE(producer.spawn(producer_args));
	::close(status_pipe[1]);
	status_pipe[1] = -1;

	edge_test::SpawnedChild consumer;
	auto consumer_args = child_args("consumer", pool_name);
	consumer_args.insert(consumer_args.end(), {"--queue-name", queue_name, "--count", "1"});
	ASSERT_TRUE(spawn_with_failpoint(&consumer, consumer_args,
	                                 "SHARED_BUFFER_POOL_READ_OWNER_EPOCH_ODD"));
	ASSERT_TRUE(wait_stopped(consumer.pid(), 5000));
	ASSERT_TRUE(wait_for_pipe_token(status_pipe[0], "POOL_STATUS_CONTENTION\n", 30000));
	EXPECT_TRUE(process_is_running(producer.pid()));
	::close(status_pipe[0]);
	status_pipe[0] = -1;

	std::string consumer_output;
	ASSERT_TRUE(continue_and_reap(&consumer, &consumer_output)) << consumer_output;
	EXPECT_EQ(consumer.exit_code(), 0) << consumer_output;
	EXPECT_NE(consumer_output.find("CONSUMED count=1 generations=1"), std::string::npos)
	        << consumer_output;

	std::string producer_output;
	ASSERT_TRUE(producer.wait(30000, &producer_output)) << producer_output;
	EXPECT_EQ(producer.exit_code(), 0) << producer_output;
	EXPECT_NE(producer_output.find("PRODUCED count=1 generations=1"), std::string::npos)
	        << producer_output;
	EXPECT_EQ(producer_output.find("POOL_STATUS_FAIL"), std::string::npos) << producer_output;

	if (::testing::Test::HasFailure()) return;
	auto removed = queue.value().remove_if_creator();
	ASSERT_TRUE(removed) << edge_runtime::to_string(removed.error().code) << " "
	                     << removed.error().context;
}

TEST(SharedBufferPoolIntegration, SchemaAndAbiMismatchFailClosed) {
	const std::string schema_name = edge_test::unique_channel_name("buffer_pool_schema");
	ScopedObjectEvidence schema_cleanup(pool_shm_name_for(schema_name));
	auto schema_pool = SharedBufferPool::create(pool_options(schema_name));
	ASSERT_TRUE(schema_pool);

	auto mismatch =
	        edge_test::run_child_capture(child_args("open-mismatch", schema_name), 10000);
	ASSERT_FALSE(mismatch.timed_out);
	EXPECT_EQ(mismatch.exit_code, 3) << mismatch.stdout_text;
	EXPECT_NE(mismatch.stdout_text.find("OPEN_FAIL code=SchemaMismatch"), std::string::npos);
	if (::testing::Test::HasFailure()) return;
	ASSERT_TRUE(schema_pool.value().remove_if_creator());

	const std::string abi_name = edge_test::unique_channel_name("buffer_pool_abi");
	ScopedObjectEvidence abi_cleanup(pool_shm_name_for(abi_name));
	auto abi_pool = SharedBufferPool::create(pool_options(abi_name));
	ASSERT_TRUE(abi_pool);
	const auto shm_name = pool_shm_name_for(abi_name);
	auto fd = edge_runtime::detail::shm_open_existing(shm_name);
	ASSERT_TRUE(fd);
	auto mapping =
	        edge_runtime::detail::mmap_region(fd.value(), sizeof(SharedBufferPoolHeaderAbi));
	ASSERT_TRUE(mapping);
	auto* header = static_cast<SharedBufferPoolHeaderAbi*>(mapping.value().get());
	header->abi_major = 99;
	mapping.value().reset();
	fd.value().reset();

	auto abi = edge_test::run_child_capture(child_args("open", abi_name), 10000);
	ASSERT_FALSE(abi.timed_out);
	EXPECT_EQ(abi.exit_code, 3) << abi.stdout_text;
	EXPECT_NE(abi.stdout_text.find("OPEN_FAIL code=AbiMismatch"), std::string::npos);
	if (::testing::Test::HasFailure()) return;
	ASSERT_TRUE(abi_pool.value().remove_if_creator());

	const std::string legacy_name = edge_test::unique_channel_name("buffer_pool_legacy_magic");
	ScopedObjectEvidence legacy_cleanup(pool_shm_name_for(legacy_name));
	auto legacy_pool = SharedBufferPool::create(pool_options(legacy_name));
	ASSERT_TRUE(legacy_pool);
	auto legacy_fd = edge_runtime::detail::shm_open_existing(pool_shm_name_for(legacy_name));
	ASSERT_TRUE(legacy_fd);
	auto legacy_mapping = edge_runtime::detail::mmap_region(legacy_fd.value(),
	                                                        sizeof(SharedBufferPoolHeaderAbi));
	ASSERT_TRUE(legacy_mapping);
	auto* legacy_header = static_cast<SharedBufferPoolHeaderAbi*>(legacy_mapping.value().get());
	std::memcpy(legacy_header->magic, kSharedBufferPoolLegacyMagic,
	            sizeof(legacy_header->magic));
	legacy_mapping.value().reset();
	legacy_fd.value().reset();

	auto legacy = edge_test::run_child_capture(child_args("open", legacy_name), 10000);
	ASSERT_FALSE(legacy.timed_out);
	EXPECT_EQ(legacy.exit_code, 3) << legacy.stdout_text;
	EXPECT_NE(legacy.stdout_text.find("OPEN_FAIL code=AbiMismatch"), std::string::npos);
	if (::testing::Test::HasFailure()) return;
	ASSERT_TRUE(legacy_pool.value().remove_if_creator());
}

TEST(SharedBufferPoolIntegration, DeadWriterFailsClosed) {
	const std::string pool_name = edge_test::unique_channel_name("buffer_pool_dead_writer");
	ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
	auto pool = SharedBufferPool::create(pool_options(pool_name));
	ASSERT_TRUE(pool);

	edge_test::SpawnedChild victim;
	ASSERT_TRUE(victim.spawn(child_args("crash-writer", pool_name)));
	if (!wait_stopped(victim.pid(), 5000)) {
		const std::string output = kill_and_reap(&victim);
		FAIL() << "writer child did not stop: " << output;
	}
	const std::string output = kill_and_reap(&victim);
	EXPECT_NE(output.find("READY"), std::string::npos);

	auto blocked = pool.value().try_acquire_write();
	ASSERT_FALSE(blocked);
	EXPECT_EQ(blocked.error().code, ErrorCode::kRecoveryBlocked);
	auto remove = pool.value().remove_if_creator();
	ASSERT_FALSE(remove);
	EXPECT_EQ(remove.error().code, ErrorCode::kBufferPoolContention);
}

TEST(SharedBufferPoolIntegration, DeadReaderFailsClosed) {
	const std::string pool_name = edge_test::unique_channel_name("buffer_pool_dead_reader");
	ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
	auto pool = SharedBufferPool::create(pool_options(pool_name));
	ASSERT_TRUE(pool);
	auto write = pool.value().try_acquire_write();
	ASSERT_TRUE(write);
	std::memset(write.value().data(), 0x5A, write.value().size());
	auto published = write.value().publish();
	ASSERT_TRUE(published);

	edge_test::SpawnedChild victim;
	ASSERT_TRUE(victim.spawn(crash_reader_args(pool_name, published.value())));
	if (!wait_stopped(victim.pid(), 5000)) {
		const std::string output = kill_and_reap(&victim);
		FAIL() << "reader child did not stop: " << output;
	}
	const std::string output = kill_and_reap(&victim);
	EXPECT_NE(output.find("READY"), std::string::npos);

	auto blocked = pool.value().acquire_read(published.value());
	ASSERT_FALSE(blocked);
	EXPECT_EQ(blocked.error().code, ErrorCode::kRecoveryBlocked);
	auto remove = pool.value().remove_if_creator();
	ASSERT_FALSE(remove);
	EXPECT_EQ(remove.error().code, ErrorCode::kBufferPoolContention);
}

TEST(SharedBufferPoolIntegration, OwnerClaimWindowsAreContentionUntilCommit) {
	const char* writer_failpoints[] = {
	        "SHARED_BUFFER_POOL_WRITE_OWNER_EPOCH_ODD",
	        "SHARED_BUFFER_POOL_WRITE_OWNER_EPOCH_COMMITTED",
	};
	for (const char* failpoint : writer_failpoints) {
		const std::string pool_name =
		        edge_test::unique_channel_name("buffer_pool_writer_epoch");
		ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
		auto pool = SharedBufferPool::create(pool_options(pool_name));
		ASSERT_TRUE(pool);

		edge_test::SpawnedChild victim;
		ASSERT_TRUE(spawn_with_failpoint(&victim, child_args("crash-writer", pool_name),
		                                 failpoint));
		ASSERT_TRUE(wait_stopped(victim.pid(), 5000));

		const auto status = edge_test::run_child_capture(status_args(pool_name), 10000);
		ASSERT_FALSE(status.timed_out) << status.stdout_text;
		EXPECT_EQ(status.exit_code, 3) << status.stdout_text;
		EXPECT_NE(status.stdout_text.find("STATUS_FAIL code=BufferPoolContention"),
		          std::string::npos)
		        << status.stdout_text;
		const std::string output = kill_and_reap(&victim);
		EXPECT_TRUE(victim.reaped()) << output;
	}

	const char* reader_failpoints[] = {
	        "SHARED_BUFFER_POOL_READ_OWNER_EPOCH_ODD",
	        "SHARED_BUFFER_POOL_READ_OWNER_EPOCH_COMMITTED",
	};
	for (const char* failpoint : reader_failpoints) {
		const std::string pool_name =
		        edge_test::unique_channel_name("buffer_pool_reader_epoch");
		ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
		auto pool = SharedBufferPool::create(pool_options(pool_name));
		ASSERT_TRUE(pool);
		auto write = pool.value().try_acquire_write();
		ASSERT_TRUE(write);
		std::memset(write.value().data(), 0x7A, write.value().size());
		auto published = write.value().publish();
		ASSERT_TRUE(published);

		edge_test::SpawnedChild victim;
		ASSERT_TRUE(spawn_with_failpoint(
		        &victim, crash_reader_args(pool_name, published.value()), failpoint));
		ASSERT_TRUE(wait_stopped(victim.pid(), 5000));

		const auto status = edge_test::run_child_capture(status_args(pool_name), 10000);
		ASSERT_FALSE(status.timed_out) << status.stdout_text;
		EXPECT_EQ(status.exit_code, 3) << status.stdout_text;
		EXPECT_NE(status.stdout_text.find("STATUS_FAIL code=BufferPoolContention"),
		          std::string::npos)
		        << status.stdout_text;
		const std::string output = kill_and_reap(&victim);
		EXPECT_TRUE(victim.reaped()) << output;
	}
}

TEST(SharedBufferPoolIntegration, SnapshotAcceptsCompleteOwnerAfterInactiveReplacement) {
	const char* snapshot_failpoints[] = {
	        "SHARED_BUFFER_POOL_OWNER_SNAPSHOT_AFTER_STATE1",
	        "SHARED_BUFFER_POOL_OWNER_SNAPSHOT_AFTER_EPOCH1",
	};
	for (const char* failpoint : snapshot_failpoints) {
		const std::string pool_name =
		        edge_test::unique_channel_name("buffer_pool_owner_replace");
		ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
		auto pool = SharedBufferPool::create(pool_options(pool_name));
		ASSERT_TRUE(pool);
		{
			auto first = pool.value().try_acquire_write();
			ASSERT_TRUE(first);

			edge_test::SpawnedChild observer;
			ASSERT_TRUE(
			        spawn_with_failpoint(&observer, status_args(pool_name), failpoint));
			ASSERT_TRUE(wait_stopped(observer.pid(), 5000));

			first.value().abort();
			edge_test::SpawnedChild replacement;
			ASSERT_TRUE(replacement.spawn(child_args("crash-writer", pool_name)));
			ASSERT_TRUE(wait_stopped(replacement.pid(), 5000));

			std::string output;
			ASSERT_TRUE(continue_and_reap(&observer, &output)) << output;
			EXPECT_EQ(observer.exit_code(), 0) << output;
			EXPECT_NE(output.find("STATUS_OK free=0 writing=1"), std::string::npos)
			        << output;
			const std::string replacement_output = kill_and_reap(&replacement);
			EXPECT_TRUE(replacement.reaped()) << replacement_output;
			EXPECT_NE(replacement_output.find("READY"), std::string::npos)
			        << replacement_output;
		}
	}
}

TEST(SharedBufferPoolIntegration, SnapshotSeesOddReplacementAsContention) {
	const std::string pool_name =
	        edge_test::unique_channel_name("buffer_pool_owner_odd_replace");
	ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
	auto pool = SharedBufferPool::create(pool_options(pool_name));
	ASSERT_TRUE(pool);
	{
		auto first = pool.value().try_acquire_write();
		ASSERT_TRUE(first);

		edge_test::SpawnedChild observer;
		ASSERT_TRUE(spawn_with_failpoint(&observer, status_args(pool_name),
		                                 "SHARED_BUFFER_POOL_OWNER_SNAPSHOT_AFTER_STATE1"));
		ASSERT_TRUE(wait_stopped(observer.pid(), 5000));
		first.value().abort();

		edge_test::SpawnedChild replacement;
		ASSERT_TRUE(spawn_with_failpoint(&replacement,
		                                 child_args("crash-writer", pool_name),
		                                 "SHARED_BUFFER_POOL_WRITE_OWNER_EPOCH_ODD"));
		ASSERT_TRUE(wait_stopped(replacement.pid(), 5000));

		std::string output;
		ASSERT_TRUE(continue_and_reap(&observer, &output)) << output;
		EXPECT_EQ(observer.exit_code(), 3) << output;
		EXPECT_NE(output.find("STATUS_FAIL code=BufferPoolContention"), std::string::npos)
		        << output;
		const std::string replacement_output = kill_and_reap(&replacement);
		EXPECT_TRUE(replacement.reaped()) << replacement_output;
	}
}

TEST(SharedBufferPoolIntegration, WriterReservesReaderEpochAtBoundary) {
	const std::string pool_name = edge_test::unique_channel_name("buffer_pool_epoch_boundary");
	ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
	auto pool = SharedBufferPool::create(pool_options(pool_name));
	ASSERT_TRUE(pool);

	uint64_t mapping_size = 0;
	ASSERT_TRUE(
	        edge_runtime::detail::shared_buffer_pool_mapping_size_for(4096, 1, &mapping_size));
	auto fd = edge_runtime::detail::shm_open_existing(pool_shm_name_for(pool_name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(fd.value(), mapping_size);
	ASSERT_TRUE(mapping);
	auto* block = reinterpret_cast<SharedBufferBlockHeaderAbi*>(
	        static_cast<std::byte*>(mapping.value().get()) + sizeof(SharedBufferPoolHeaderAbi));
	edge_runtime::detail::shared_store_seq_cst(&block->owner_epoch, UINT64_MAX - 5u);
	mapping.value().reset();
	fd.value().reset();

	auto write = pool.value().try_acquire_write();
	ASSERT_TRUE(write) << edge_runtime::to_string(write.error().code) << " "
	                   << write.error().context;
	std::memset(write.value().data(), 0x6B, write.value().size());
	auto published = write.value().publish();
	ASSERT_TRUE(published);

	const auto reader = edge_test::run_child_capture(
	        stale_reader_args(pool_name, published.value()), 10000);
	ASSERT_FALSE(reader.timed_out) << reader.stdout_text;
	EXPECT_EQ(reader.exit_code, 0) << reader.stdout_text;
	EXPECT_NE(reader.stdout_text.find("READ_OK first_byte=0x6B"), std::string::npos)
	        << reader.stdout_text;

	auto exhausted = pool.value().try_acquire_write();
	ASSERT_FALSE(exhausted);
	EXPECT_EQ(exhausted.error().code, ErrorCode::kSequenceExhausted);
	auto status = pool.value().status();
	ASSERT_FALSE(status);
	EXPECT_EQ(status.error().code, ErrorCode::kRecoveryBlocked);
}

TEST(SharedBufferPoolIntegration, StaleReaderCannotClaimRecycledGeneration) {
	const std::string pool_name = edge_test::unique_channel_name("buffer_pool_generation_race");
	ScopedObjectEvidence cleanup(pool_shm_name_for(pool_name));
	auto pool = SharedBufferPool::create(pool_options(pool_name));
	ASSERT_TRUE(pool);

	auto write = pool.value().try_acquire_write();
	ASSERT_TRUE(write);
	std::memset(write.value().data(), 0x11, write.value().size());
	auto old_published = write.value().publish();
	ASSERT_TRUE(old_published);
	const BufferHandle old_handle = old_published.value();

	edge_test::SpawnedChild stale_reader;
	ASSERT_TRUE(spawn_with_failpoint(&stale_reader, stale_reader_args(pool_name, old_handle),
	                                 "SHARED_BUFFER_POOL_READ_GENERATION_CHECKED"));
	ASSERT_TRUE(wait_stopped(stale_reader.pid(), 5000));

	const auto recycled =
	        edge_test::run_child_capture(recycle_args(pool_name, old_handle), 10000);
	ASSERT_FALSE(recycled.timed_out) << recycled.stdout_text;
	ASSERT_EQ(recycled.exit_code, 0) << recycled.stdout_text;
	EXPECT_NE(recycled.stdout_text.find("RECYCLE_OK old_generation=1 new_generation=2"),
	          std::string::npos)
	        << recycled.stdout_text;

	ASSERT_EQ(::kill(stale_reader.pid(), SIGCONT), 0);
	std::string stale_output;
	ASSERT_TRUE(stale_reader.wait(10000, &stale_output)) << stale_output;
	EXPECT_EQ(stale_reader.exit_code(), 3) << stale_output;
	EXPECT_NE(stale_output.find("READ_ACQUIRE_FAIL code=BufferInvalidHandle"),
	          std::string::npos)
	        << stale_output;
	EXPECT_EQ(stale_output.find("READ_OK"), std::string::npos) << stale_output;

	BufferHandle new_handle = old_handle;
	new_handle.block_generation = old_handle.block_generation + 1;
	auto read = pool.value().acquire_read(new_handle);
	ASSERT_TRUE(read) << edge_runtime::to_string(read.error().code) << " "
	                  << read.error().context;
	EXPECT_EQ(std::to_integer<unsigned int>(read.value().data()[0]), 0x22u);
	read.value().release();

	auto status = pool.value().status();
	ASSERT_TRUE(status);
	EXPECT_EQ(status.value().free_blocks, 1u);
	EXPECT_EQ(status.value().published_blocks, 0u);
	EXPECT_EQ(status.value().reading_blocks, 0u);

	auto reuse = pool.value().try_acquire_write();
	ASSERT_TRUE(reuse) << edge_runtime::to_string(reuse.error().code) << " "
	                   << reuse.error().context;
	EXPECT_EQ(reuse.value().handle().block_generation, old_handle.block_generation + 2);
	reuse.value().abort();
	if (!::testing::Test::HasFailure()) {
		ASSERT_TRUE(pool.value().remove_if_creator());
	}
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc != 2) {
		std::fprintf(stderr, "usage: shared_buffer_pool_test <child_binary>\n");
		return 2;
	}
	g_helper = argv[1];
	return RUN_ALL_TESTS();
}
