#include "edge_runtime/channel/control_lock.hpp"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>

#include "edge_runtime/channel/control_operation.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/utility/checksum.hpp"

namespace edge_runtime::detail {

uint64_t journal_checksum(const ControlJournalV1& journal) noexcept {
	ControlJournalV1 tmp = journal;
	tmp.record_checksum = 0;
	return fnv1a64(reinterpret_cast<const std::byte*>(&tmp), sizeof(tmp));
}

uint32_t channel_name_hash(const char* name, size_t len) noexcept {
	uint32_t hash = 2166136261u;
	for (size_t i = 0; i < len; ++i) {
		hash ^= static_cast<uint32_t>(static_cast<unsigned char>(name[i]));
		hash *= 16777619u;
	}
	return hash;
}

// 控制锁目录使用当前用户私有权限，锁文件只保护创建、替换和删除事务。
Result<ControlLock> ControlLock::acquire(const std::string& lock_path) try {
	auto available = control_check();
	if (!available) return available.error();

	const size_t slash = lock_path.rfind('/');
	if (slash == std::string::npos || slash == 0) {
		return make_error(ErrorCode::kInvalidOptions, "control_lock_acquire",
		                  "lock path has no directory");
	}
	const std::string dir = lock_path.substr(0, slash);
	if (::mkdir(dir.c_str(), 0700) != 0 && errno != EEXIST) {
		const int e = errno;
		return make_errno_error(e, "control_lock_acquire", dir.c_str());
	}

	const int fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
	if (fd < 0) {
		const int e = errno;
		return make_errno_error(e, "control_lock_acquire", std::strerror(e));
	}
	UniqueFd owned(fd);

	struct stat st {};
	if (::fstat(fd, &st) != 0) {
		const int e = errno;
		return make_errno_error(e, "control_lock_acquire", std::strerror(e));
	}
	if (st.st_uid != geteuid()) {
		return make_error(ErrorCode::kPermissionDenied, "control_lock_acquire",
		                  "lock owner uid mismatch");
	}
	if (st.st_nlink != 1) {
		return make_error(ErrorCode::kPermissionDenied, "control_lock_acquire",
		                  "lock link count != 1");
	}
	if (!S_ISREG(st.st_mode))
		return make_error(ErrorCode::kPermissionDenied, "control_lock_acquire",
		                  "not a regular file");

	while (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
		const int saved = errno;
		if (saved != EWOULDBLOCK && saved != EAGAIN && saved != EINTR)
			return make_errno_error(saved, "control_lock_acquire", "flock");
		auto paused = control_pause(5'000'000);
		if (!paused) return paused.error();
	}
	available = control_check();
	if (!available) return available.error();
	struct stat named {};
	if (::lstat(lock_path.c_str(), &named) != 0 || named.st_dev != st.st_dev ||
	    named.st_ino != st.st_ino || named.st_nlink != 1)
		return make_error(ErrorCode::kNameRaceDetected, "control_lock_acquire",
		                  "control inode replaced during wait");

	ControlLock lock;
	lock.path_ = lock_path;
	lock.dev_ = static_cast<uint64_t>(st.st_dev);
	lock.ino_ = static_cast<uint64_t>(st.st_ino);
	lock.fd_ = std::move(owned);
	return Result<ControlLock>(std::move(lock));
} catch (...) {
	return make_errno_error(ENOMEM, "control_lock_acquire", "allocation");
}

Result<ControlJournalV1> ControlLock::read_journal() noexcept {
	ControlJournalV1 journal{};
	const size_t want = sizeof(journal);
	char* dst = reinterpret_cast<char*>(&journal);
	size_t got = 0;
	while (got < want) {
		auto available = control_check();
		if (!available) return available.error();
		const ssize_t n =
		        ::pread(fd_.get(), dst + got, want - got, static_cast<off_t>(got));
		if (n < 0) {
			if (errno == EINTR) continue;
			const int e = errno;
			return make_errno_error(e, "control_lock_read_journal", std::strerror(e));
		}
		if (n == 0) break;
		got += static_cast<size_t>(n);
	}
	if (got == 0) {
		return Result<ControlJournalV1>(journal);
	}
	if (got != want) {
		return make_error(ErrorCode::kRecoveryBlocked, "control_lock_read_journal",
		                  "partial journal record");
	}

	bool all_zero = true;
	const auto* bytes = reinterpret_cast<const unsigned char*>(&journal);
	for (size_t i = 0; i < sizeof(journal); ++i) {
		if (bytes[i] != 0) {
			all_zero = false;
			break;
		}
	}
	if (all_zero) {
		return Result<ControlJournalV1>(journal);
	}
	if (std::memcmp(journal.magic, kJournalMagic, 7) != 0) {
		return make_error(ErrorCode::kRecoveryBlocked, "control_lock_read_journal",
		                  "journal magic mismatch");
	}
	if (journal.version != kJournalVersion) {
		return make_error(ErrorCode::kRecoveryBlocked, "control_lock_read_journal",
		                  "journal version mismatch");
	}
	if (journal_checksum(journal) != journal.record_checksum) {
		return make_error(ErrorCode::kRecoveryBlocked, "control_lock_read_journal",
		                  "journal checksum mismatch");
	}
	last_journal_ = journal;
	return Result<ControlJournalV1>(journal);
}

Result<void> ControlLock::write_journal(const ControlJournalV1& journal) noexcept {
	attempted_journal_ = journal;
	auto available = control_check();
	if (!available) return available;
	if (journal.state != static_cast<uint32_t>(JournalState::kIdle) &&
	    !process_identity_valid({journal.creator.pid, journal.creator.proc_start_ticks,
	                             journal.creator.boot_id_hash_hi,
	                             journal.creator.boot_id_hash_lo})) {
		return make_error(ErrorCode::kRecoveryBlocked, "control_lock_write_journal",
		                  "invalid creator");
	}
	ControlJournalV1 rec = journal;
	rec.record_checksum = journal_checksum(rec);
	const char* src = reinterpret_cast<const char*>(&rec);
	const size_t want = sizeof(rec);
	size_t written = 0;
	while (written < want) {
		available = control_check();
		if (!available) return available;
		const ssize_t n = ::pwrite(fd_.get(), src + written, want - written,
		                           static_cast<off_t>(written));
		if (n < 0) {
			if (errno == EINTR) continue;
			const int e = errno;
			return make_errno_error(e, "control_lock_write_journal", std::strerror(e));
		}
		if (n == 0)
			return make_errno_error(EIO, "control_lock_write_journal", "zero write");
		written += static_cast<size_t>(n);
	}
	available = control_check();
	if (!available) return available;
	if (::fdatasync(fd_.get()) != 0) {
		const int e = errno;
		return make_errno_error(e, "control_lock_write_journal", "fdatasync");
	}
	last_journal_ = rec;
	return control_check();
}

void ControlLock::release() noexcept {
	if (fd_.get() >= 0) ::flock(fd_.get(), LOCK_UN);
	fd_.reset();
}

}
