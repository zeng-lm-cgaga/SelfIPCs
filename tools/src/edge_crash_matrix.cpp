// 崩溃矩阵通过独立进程和故障点覆盖创建、发布、恢复、传输及监督器失败路径。
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/channel_abi.hpp"
#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/channel/control_lock.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/channel/slot_protocol.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "edge_runtime_tool/tool_common.hpp"
#include "test_payload.hpp"

namespace {

using edge_runtime::ChannelOptions;
using edge_runtime::Consumer;
using edge_runtime::ErrorCode;
using edge_runtime::Producer;
using edge_runtime::SchemaDescriptor;
using edge_runtime::to_string;
namespace detail = edge_runtime::detail;

struct CommandResult {
	int exit_code = -1;
	std::string out;
};

int64_t monotonic_ms_now() {
	struct timespec ts {};
	::clock_gettime(CLOCK_MONOTONIC, &ts);
	return static_cast<int64_t>(ts.tv_sec) * 1000 + static_cast<int64_t>(ts.tv_nsec / 1000000);
}

uint64_t unix_secs_now() {
	struct timespec ts {};
	::clock_gettime(CLOCK_REALTIME, &ts);
	return static_cast<uint64_t>(ts.tv_sec);
}

bool contains(const std::string& text, const char* needle) {
	return text.find(needle) != std::string::npos;
}

uint64_t parse_after(const std::string& text, const char* key) {
	const size_t p = text.find(key);
	if (p == std::string::npos) return UINT64_MAX;
	return std::strtoull(text.c_str() + p + std::strlen(key), nullptr, 10);
}

std::vector<std::pair<const char*, const char*>> fp_env(const char* id, const char* count) {
	std::vector<std::pair<const char*, const char*>> env;
	env.emplace_back("EDGE_FAILPOINT", id);
	env.emplace_back("EDGE_FAILPOINT_MODE", "stop");
	if (count != nullptr) env.emplace_back("EDGE_FAILPOINT_COUNT", count);
	return env;
}

class CrashChild {
       public:
	bool spawn(const std::vector<std::string>& argv,
	           const std::vector<std::pair<const char*, const char*>>& env) {
		int pipefd[2] = {-1, -1};
		if (::pipe(pipefd) != 0) return false;
		const pid_t pid = ::fork();
		if (pid < 0) {
			::close(pipefd[0]);
			::close(pipefd[1]);
			return false;
		}
		if (pid == 0) {
			::close(pipefd[0]);
			if (::dup2(pipefd[1], STDOUT_FILENO) < 0) _exit(127);
			(void)::fcntl(pipefd[1], F_SETFL, O_NONBLOCK);
			for (const auto& kv : env) ::setenv(kv.first, kv.second, 1);
			std::vector<char*> args;
			args.reserve(argv.size() + 1);
			for (const std::string& a : argv) {
				args.push_back(const_cast<char*>(a.c_str()));
			}
			args.push_back(nullptr);
			::execv(args[0], args.data());
			std::fprintf(stderr, "execv %s: %s\n", args[0], std::strerror(errno));
			_exit(127);
		}
		pid_ = pid;
		pipe_ = pipefd[0];
		::close(pipefd[1]);
		(void)::fcntl(pipe_, F_SETFL, O_NONBLOCK);
		return true;
	}

	pid_t pid() const { return pid_; }
	bool reaped() const { return reaped_; }
	bool stopped() const { return stopped_; }
	const std::string& stdout_text() const { return out_; }

	bool wait_stop(int timeout_ms) {
		const int64_t deadline = monotonic_ms_now() + timeout_ms;
		while (true) {
			int status = 0;
			const pid_t rc = ::waitpid(pid_, &status, WUNTRACED | WNOHANG);
			if (rc == pid_) {
				if (WIFSTOPPED(status)) {
					stopped_ = true;
					drain();
					return true;
				}
				if (WIFEXITED(status) || WIFSIGNALED(status)) {
					reaped_ = true;
					drain();
					return false;
				}
			} else if (rc < 0 && errno != EINTR) {
				reaped_ = true;
				return false;
			}
			if (monotonic_ms_now() >= deadline) {
				kill_and_reap();
				return false;
			}
			struct timespec ts {};
			ts.tv_nsec = 5 * 1000 * 1000;
			::nanosleep(&ts, nullptr);
		}
	}

	int wait_exit(int timeout_ms) {
		const int64_t deadline = monotonic_ms_now() + timeout_ms;
		while (true) {
			int status = 0;
			const pid_t rc = ::waitpid(pid_, &status, WNOHANG);
			if (rc == pid_) {
				reaped_ = true;
				drain();
				if (WIFEXITED(status)) return WEXITSTATUS(status);
				return -1;
			}
			if (rc < 0 && errno != EINTR) {
				reaped_ = true;
				return -1;
			}
			if (monotonic_ms_now() >= deadline) {
				kill_and_reap();
				return -1;
			}
			struct timespec ts {};
			ts.tv_nsec = 5 * 1000 * 1000;
			::nanosleep(&ts, nullptr);
		}
	}

	void kill_and_reap() {
		if (!reaped_) ::kill(pid_, SIGKILL);
		if (!reaped_) ::waitpid(pid_, nullptr, 0);
		reaped_ = true;
		drain();
	}

	void send_signal(int sig) {
		if (!reaped_) ::kill(pid_, sig);
	}

       private:
	void drain() {
		if (pipe_ < 0) return;
		char buf[4096];
		while (true) {
			const ssize_t n = ::read(pipe_, buf, sizeof(buf));
			if (n > 0) {
				out_.append(buf, static_cast<size_t>(n));
				continue;
			}
			if (n == 0) {
				::close(pipe_);
				pipe_ = -1;
				return;
			}
			if (errno == EAGAIN || errno == EWOULDBLOCK) return;
			if (errno == EINTR) continue;
			::close(pipe_);
			pipe_ = -1;
			return;
		}
	}

	pid_t pid_ = -1;
	int pipe_ = -1;
	bool reaped_ = false;
	bool stopped_ = false;
	std::string out_;
};

class CaseDriver {
       public:
	std::string prod_bin;
	std::string cons_bin;
	std::string ctl_bin;
	std::string supervisor_bin;
	std::string run_dir;
	uint64_t name_seq = 0;
	int failures = 0;

	std::string channel_name(const char* tag) {
		char buf[96];
		std::snprintf(buf, sizeof(buf), "%s_%ld_%llu", tag, static_cast<long>(::getpid()),
		              static_cast<unsigned long long>(name_seq++));
		return buf;
	}

	void begin_case(const char* id, const char* description) {
		case_id_ = id;
		case_dir_ = run_dir + "/" + id;
		std::vector<std::string> mk{"/bin/mkdir", "-p", case_dir_};
		(void)run_command(mk);
		pass_ = true;
		write_evidence("killpoint.txt", std::string(id) + "\n" + description + "\n");
		commands_.clear();
	}

	bool ok() const { return pass_; }

	bool write_evidence(const std::string& rel, const std::string& content) {
		const std::string path = case_dir_ + "/" + rel;
		std::FILE* f = std::fopen(path.c_str(), "w");
		if (f == nullptr) return false;
		const bool written = std::fwrite(content.data(), 1, content.size(), f) == content.size();
		return std::fclose(f) == 0 && written;
	}

	void fail(const std::string& reason) {
		pass_ = false;
		failure_reason_ = reason;
	}

	void record_command(const std::string& line) { commands_.push_back(line); }

	CommandResult run_command(const std::vector<std::string>& argv) {
		record_command(join(argv));
		CommandResult res;
		int pipefd[2] = {-1, -1};
		if (::pipe(pipefd) != 0) return res;
		const pid_t pid = ::fork();
		if (pid < 0) {
			::close(pipefd[0]);
			::close(pipefd[1]);
			return res;
		}
		if (pid == 0) {
			::close(pipefd[0]);
			if (::dup2(pipefd[1], STDOUT_FILENO) < 0) _exit(127);
			std::vector<char*> args;
			args.reserve(argv.size() + 1);
			for (const std::string& a : argv) {
				args.push_back(const_cast<char*>(a.c_str()));
			}
			args.push_back(nullptr);
			::execv(args[0], args.data());
			std::fprintf(stderr, "execv %s: %s\n", args[0], std::strerror(errno));
			_exit(127);
		}
		::close(pipefd[1]);
		char buf[4096];
		while (true) {
			const ssize_t n = ::read(pipefd[0], buf, sizeof(buf));
			if (n <= 0) break;
			res.out.append(buf, static_cast<size_t>(n));
		}
		::close(pipefd[0]);
		int status = 0;
		::waitpid(pid, &status, 0);
		if (WIFEXITED(status)) res.exit_code = WEXITSTATUS(status);
		return res;
	}

	CommandResult ctl_inspect(const std::string& channel) {
		const CommandResult r = run_command({ctl_bin, "inspect", channel});
		write_evidence("header_dump.txt",
		               r.out + "EXIT " + std::to_string(r.exit_code) + "\n");
		return r;
	}

	void finish_case(const std::string& pids_line) {
		std::string cmd;
		for (const std::string& c : commands_) cmd += c + "\n";
		write_evidence("command.txt", cmd);
		write_evidence("pids.txt", pids_line);
		write_evidence("result.txt", ok() ? "PASS\n" : "FAIL " + failure_reason_ + "\n");
		if (!ok()) ++failures;
	}

	bool expect_contains(const std::string& text, const char* needle, const std::string& what) {
		if (contains(text, needle)) return true;
		fail(what + ": missing \"" + needle + "\" in:\n" + text);
		return false;
	}

       private:
	static std::string join(const std::vector<std::string>& argv) {
		std::string out;
		for (const std::string& a : argv) {
			if (!out.empty()) out += " ";
			out += a;
		}
		return out;
	}

	std::string case_id_;
	std::string case_dir_;
	bool pass_ = true;
	std::string failure_reason_;
	std::vector<std::string> commands_;
};

std::string gen_pids_line(const char* case_id, const CrashChild& c) {
	char buf[160];
	std::snprintf(buf, sizeof(buf),
	              "%s parent_pid=%ld crash_child_pid=%ld "
	              "stopped=%d\n",
	              case_id, static_cast<long>(::getpid()), static_cast<long>(c.pid()),
	              c.stopped() ? 1 : 0);
	return buf;
}

bool snapshot_object(CaseDriver& driver, const std::string& shm, const std::string& tag) {
	auto descriptor = detail::shm_open_existing(shm);
	if (!descriptor) return false;
	uint64_t device = 0, inode = 0, size = 0;
	if (!detail::shm_fstat_and_capture(descriptor.value(), &device, &inode, &size) ||
	    size > 1024 * 1024) {
		return false;
	}
	std::string bytes(static_cast<size_t>(size), '\0');
	size_t copied = 0;
	while (copied < bytes.size()) {
		const ssize_t count = ::pread(descriptor.value().get(), bytes.data() + copied,
		                              bytes.size() - copied, static_cast<off_t>(copied));
		if (count < 0 && errno == EINTR) continue;
		if (count <= 0) return false;
		copied += static_cast<size_t>(count);
	}
	const std::string identity = "name=" + shm + "\ndev=" + std::to_string(device) +
	                             "\nino=" + std::to_string(inode) +
	                             "\nsize=" + std::to_string(size) + "\n";
	return driver.write_evidence(tag + ".bin", bytes) &&
	       driver.write_evidence(tag + ".txt", identity);
}

struct HeaderView {
	detail::MappedRegion mm;
	detail::ChannelHeaderAbi* header = nullptr;
};

bool map_existing_header(const std::string& channel_name, HeaderView* view, std::string* err) {
	const std::string shm = detail::channel_shm_name(channel_name);
	auto fd = detail::shm_open_existing(shm);
	if (!fd) {
		*err = std::string("open: ") + to_string(fd.error().code);
		return false;
	}
	uint64_t dev = 0, ino = 0, size = 0;
	auto fst = detail::shm_fstat_and_capture(fd.value(), &dev, &ino, &size);
	if (!fst) {
		*err = "fstat failed";
		return false;
	}
	auto mm = detail::mmap_region(fd.value(), size);
	if (!mm) {
		*err = "mmap failed";
		return false;
	}
	view->mm = std::move(mm.value());
	auto* base = static_cast<std::byte*>(view->mm.get());
	view->header =
	        reinterpret_cast<detail::ChannelHeaderAbi*>(base + detail::kChannelHeaderOffset);
	return true;
}

}

static bool run_c01(CaseDriver& d) {
	d.begin_case("C01",
	             "creator dies right after shm_open: size-0 pre-object, "
	             "narrow condition auto-cleans");
	const std::string name = d.channel_name("c01");

	CrashChild victim;
	if (!victim.spawn({d.prod_bin, "--name", name, "--count", "0"}, fp_env("C01", nullptr)) ||
	    !victim.wait_stop(8000)) {
		d.fail("crash child never stopped at C01");
		d.finish_case(gen_pids_line("C01", victim));
		return d.ok();
	}
	victim.kill_and_reap();

	const CommandResult dump = d.ctl_inspect(name);
	d.expect_contains(dump.out, "JOURNAL state=creating_pre_object", "C01 header_dump");
	d.expect_contains(dump.out, "size=0", "C01 header_dump");

	const CommandResult rec = d.run_command({d.prod_bin, "--name", name, "--count", "3"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "GENERATION 1", "C01 recovery");
	d.expect_contains(rec.out, "DONE published=3", "C01 recovery");
	if (rec.exit_code != 0)
		d.fail("C01 recovery producer exit " + std::to_string(rec.exit_code));

	(void)d.run_command({d.ctl_bin, "remove", name});
	d.finish_case(gen_pids_line("C01", victim));
	return d.ok();
}

static bool run_c02(CaseDriver& d) {
	d.begin_case("C02",
	             "creator dies after bootstrap INITIALIZING write: partial "
	             "object, inode+nonce bound to journal, recovered (§9.1)");
	const std::string name = d.channel_name("c02");

	CrashChild victim;
	if (!victim.spawn({d.prod_bin, "--name", name, "--count", "0"}, fp_env("C02", nullptr)) ||
	    !victim.wait_stop(8000)) {
		d.fail("crash child never stopped at C02");
		d.finish_case(gen_pids_line("C02", victim));
		return d.ok();
	}
	victim.kill_and_reap();

	const CommandResult dump = d.ctl_inspect(name);
	d.expect_contains(dump.out, "JOURNAL state=creating_object", "C02 header_dump");
	d.expect_contains(dump.out, "HEADER absent (partial object)", "C02 header_dump");
	d.expect_contains(dump.out, "init=initializing", "C02 header_dump");

	const CommandResult rec = d.run_command({d.prod_bin, "--name", name, "--count", "3"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "GENERATION 1", "C02 recovery");
	d.expect_contains(rec.out, "DONE published=3", "C02 recovery");
	if (rec.exit_code != 0)
		d.fail("C02 recovery producer exit " + std::to_string(rec.exit_code));

	(void)d.run_command({d.ctl_bin, "remove", name});
	d.finish_case(gen_pids_line("C02", victim));
	return d.ok();
}

static bool run_c_publish(CaseDriver& d, const char* id, const char* fp, const char* description,
                          bool expect_ticketed, bool loaned = false) {
	d.begin_case(id, description);
	const std::string name = d.channel_name(id);

	CrashChild victim;
	std::vector<std::string> victim_args = {
	        d.prod_bin, "--name", name, "--count", "0", "--interval-us", "50000"};
	if (loaned) victim_args.emplace_back("--loaned");
	if (!victim.spawn(victim_args, fp_env(fp, "4")) ||
	    !victim.wait_stop(8000)) {
		d.fail("crash child never stopped");
		d.finish_case(gen_pids_line(id, victim));
		return d.ok();
	}

	const CommandResult dump = d.ctl_inspect(name);
	const char* expect_ticket = expect_ticketed ? "TICKET seq=4" : "TICKET seq=3";
	d.expect_contains(dump.out, expect_ticket, "publish-crash header_dump");

	victim.kill_and_reap();

	const CommandResult rec = d.run_command(
	        {d.cons_bin, "--name", name, "--reads", "1", "--read-timeout-ms", "15000"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "torn=0", "publish-crash recovery");
	const uint64_t last_seq = parse_after(rec.out, "last_seq=");
	const uint64_t want = expect_ticketed ? 4u : 3u;
	if (last_seq != want) {
		d.fail("publish-crash recovery read last_seq=" +
		       (last_seq == UINT64_MAX ? std::string("?") : std::to_string(last_seq)) +
		       " want " + std::to_string(want) + "\n" + rec.out);
	}
	if (rec.exit_code != 0)
		d.fail("publish-crash recovery consumer exit " + std::to_string(rec.exit_code));

	(void)d.run_command({d.ctl_bin, "remove", name});
	d.finish_case(gen_pids_line(id, victim));
	return d.ok();
}

static bool run_c_consumer(CaseDriver& d, const char* id, const char* fp, const char* description,
                           const char* expect_slot, bool loaned = false) {
	d.begin_case(id, description);
	const std::string name = d.channel_name(id);

	CrashChild prod;
	if (!prod.spawn({d.prod_bin, "--name", name, "--count", "0", "--interval-us", "250000"},
	                {})) {
		d.fail("producer spawn failed");
		d.finish_case(gen_pids_line(id, prod));
		return d.ok();
	}
	struct timespec ts {};
	ts.tv_nsec = 450 * 1000 * 1000;
	::nanosleep(&ts, nullptr);

	CrashChild victim;
	std::vector<std::string> victim_args = {d.cons_bin, "--name", name, "--reads", "1"};
	if (loaned) victim_args.emplace_back("--loaned");
	if (!victim.spawn(victim_args, fp_env(fp, nullptr)) ||
	    !victim.wait_stop(8000)) {
		d.fail("crash consumer never stopped");
		prod.kill_and_reap();
		d.finish_case(gen_pids_line(id, victim));
		return d.ok();
	}
	victim.kill_and_reap();

	const CommandResult dump = d.ctl_inspect(name);
	if (expect_slot == nullptr) {
		if (contains(dump.out, "state=reading")) {
			d.fail("consumer-crash: leaked reading slot present after release:\n" +
			       dump.out);
		}
	} else {
		d.expect_contains(dump.out, expect_slot, "consumer-crash header_dump");
	}

	const CommandResult rec =
	        d.run_command({d.cons_bin, "--name", name, "--reads", "5", "--read-interval-ms",
	                       "100", "--read-timeout-ms", "15000"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "SUMMARY reads=5 torn=0", "consumer-crash recovery");

	if (prod.reaped()) {
		d.fail("consumer-crash: producer exited early:\n" + prod.stdout_text());
	} else {
		prod.kill_and_reap();
	}

	(void)d.run_command({d.ctl_bin, "remove", name});
	d.finish_case(gen_pids_line(id, victim));
	return d.ok();
}

static bool run_c10(CaseDriver& d) {
	d.begin_case("C10",
	             "old producer dead + two new producers: the control lock "
	             "serializes recovery, generation advances exactly once");
	const std::string name = d.channel_name("c10");

	CrashChild old_prod;
	if (!old_prod.spawn({d.prod_bin, "--name", name, "--count", "0", "--interval-us", "100000"},
	                    {})) {
		d.fail("old producer spawn failed");
		d.finish_case(gen_pids_line("C10", old_prod));
		return d.ok();
	}
	struct timespec ts {};
	ts.tv_nsec = 400 * 1000 * 1000;
	::nanosleep(&ts, nullptr);
	old_prod.kill_and_reap();

	CrashChild a;
	if (!a.spawn({d.prod_bin, "--name", name, "--count", "0"}, fp_env("C10", nullptr)) ||
	    !a.wait_stop(8000)) {
		d.fail("recovery A never stopped at C10");
		a.kill_and_reap();
		d.finish_case(gen_pids_line("C10", a));
		return d.ok();
	}

	CrashChild b;
	if (!b.spawn({d.prod_bin, "--name", name, "--count", "3"}, {})) {
		d.fail("recovery B spawn failed");
		a.kill_and_reap();
		d.finish_case(gen_pids_line("C10", a));
		return d.ok();
	}
	ts.tv_nsec = 200 * 1000 * 1000;
	::nanosleep(&ts, nullptr);

	a.kill_and_reap();
	const int bcode = b.wait_exit(20000);
	d.write_evidence("recovery_result.txt",
	                 b.stdout_text() + "EXIT " + std::to_string(bcode) + "\n");
	d.expect_contains(b.stdout_text(), "GENERATION 2", "C10 recovery");
	d.expect_contains(b.stdout_text(), "DONE published=3", "C10 recovery");
	if (bcode != 0) d.fail("C10 recovery producer exit " + std::to_string(bcode));

	(void)d.run_command({d.ctl_bin, "remove", name});
	d.finish_case(gen_pids_line("C10", a));
	return d.ok();
}

static bool run_c11(CaseDriver& d) {
	d.begin_case("C11",
	             "PID-reuse fixture: a live pid claimed with a WRONG "
	             "starttime must read as kPidReused, so replacement "
	             "proceeds at generation+1");
	const std::string name = d.channel_name("c11");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();
	std::string ev;

	uint64_t gen1 = 0;
	{
		auto p1 = Producer<TestPayloadV1>::create(opts, schema);
		if (!p1) {
			d.fail("gen-1 create failed: " + std::string(to_string(p1.error().code)));
			d.write_evidence("recovery_result.txt", ev);
			d.finish_case("C11 parent_pid=" + std::to_string(::getpid()) + "\n");
			return d.ok();
		}
		auto st = p1.value().status();
		if (!st) {
			d.fail("gen-1 status failed");
		} else {
			gen1 = st.value().generation;
			ev += "gen1=" + std::to_string(gen1) + "\n";
		}
	}

	{
		HeaderView view;
		std::string err;
		if (!map_existing_header(name, &view, &err)) {
			d.fail("C11 identity patch: " + err);
		} else {
			detail::ProcessIdentityAbi forged{};
			forged.pid = static_cast<uint64_t>(::getpid());
			forged.proc_start_ticks = 1;
			forged.boot_id_hash_hi = 0x1111111111111111ull;
			forged.boot_id_hash_lo = 0x2222222222222222ull;
			auto wr = detail::identity_snapshot_write(&view.header->producer, forged);
			if (!wr) {
				d.fail("C11 identity patch write failed");
			} else {
				detail::shared_store_release(
				        &view.header->producer_state,
				        static_cast<uint32_t>(detail::EndpointState::kOnline));
				ev += "patched pid=" + std::to_string(::getpid()) +
				      " start=1 state=online epoch=" + std::to_string(wr.value()) +
				      "\n";
			}
		}
	}

	{
		auto p2 = Producer<TestPayloadV1>::create(opts, schema);
		if (!p2) {
			d.fail("C11 replacement create failed: " +
			       std::string(to_string(p2.error().code)) + " " + p2.error().context);
		} else {
			auto st = p2.value().status();
			if (!st) {
				d.fail("C11 replacement status failed");
			} else {
				const uint64_t gen2 = st.value().generation;
				ev += "gen2=" + std::to_string(gen2) + "\n";
				if (gen2 != gen1 + 1) {
					d.fail("C11 replacement did not advance generation: gen1=" +
					       std::to_string(gen1) +
					       " gen2=" + std::to_string(gen2));
				}
			}
			(void)p2.value().remove_if_owner();
		}
	}

	d.write_evidence("recovery_result.txt", ev);
	d.finish_case("C11 parent_pid=" + std::to_string(::getpid()) + "\n");
	return d.ok();
}

static bool run_c12(CaseDriver& d) {
	d.begin_case("C12",
	             "identity-epoch corruption fixture: an odd role_epoch "
	             "makes identity_snapshot_read fail, so recovery fails "
	             "closed with RecoveryBlocked (C12)");
	const std::string name = d.channel_name("c12");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();
	std::string ev;

	uint64_t gen1 = 0;
	{
		auto p1 = Producer<TestPayloadV1>::create(opts, schema);
		if (!p1) {
			d.fail("gen-1 create failed: " + std::string(to_string(p1.error().code)));
			d.write_evidence("recovery_result.txt", ev);
			d.finish_case("C12 parent_pid=" + std::to_string(::getpid()) + "\n");
			return d.ok();
		}
		auto st = p1.value().status();
		if (!st) {
			d.fail("gen-1 status failed");
		} else {
			gen1 = st.value().generation;
			ev += "gen1=" + std::to_string(gen1) + "\n";
		}
	}

	{
		HeaderView view;
		std::string err;
		if (!map_existing_header(name, &view, &err)) {
			d.fail("C12 identity patch: " + err);
		} else {
			detail::shared_store_release(&view.header->producer.role_epoch,
			                             uint64_t{1});
			ev += "corrupted role_epoch=1 (odd)\n";
		}
	}

	{
		auto p2 = Producer<TestPayloadV1>::create(opts, schema);
		if (p2) {
			d.fail("C12 create must fail closed on an unstable identity");
		} else {
			const std::string code = to_string(p2.error().code);
			ev += "create blocked code=" + code + " ctx=" + p2.error().context + "\n";
			if (p2.error().code != ErrorCode::kRecoveryBlocked) {
				d.fail("C12 expected kRecoveryBlocked, got " + code);
			}
		}
	}

	if (!snapshot_object(d, detail::channel_shm_name(name), "retained-object")) {
		d.fail("C12 failed to preserve object snapshot");
	}

	d.write_evidence("recovery_result.txt", ev);
	d.finish_case("C12 parent_pid=" + std::to_string(::getpid()) + "\n");
	return d.ok();
}

static bool run_c13(CaseDriver& d) {
	d.begin_case("C13",
	             "name/inode ABA: after a creator crash the object under "
	             "the name is replaced; recovery must refuse to touch the "
	             "stranger's object (kNameRaceDetected)");
	const std::string name = d.channel_name("c13");
	std::string ev;

	CrashChild victim;
	if (!victim.spawn({d.prod_bin, "--name", name, "--count", "0"}, fp_env("C02", nullptr)) ||
	    !victim.wait_stop(8000)) {
		d.fail("crash child never stopped at C02");
		d.finish_case(gen_pids_line("C13", victim));
		return d.ok();
	}
	victim.kill_and_reap();

	const CommandResult dump = d.ctl_inspect(name);
	d.expect_contains(dump.out, "JOURNAL state=creating_object", "C13 header_dump");
	d.expect_contains(dump.out, "HEADER absent (partial object)", "C13 header_dump");

	const std::string shm = detail::channel_shm_name(name);
	auto fd0 = detail::shm_open_existing(shm);
	if (!fd0) {
		d.fail("C13 open of creator object failed");
		d.finish_case(gen_pids_line("C13", victim));
		return d.ok();
	}
	uint64_t dev0 = 0, ino0 = 0;
	if (!detail::shm_fstat_and_capture(fd0.value(), &dev0, &ino0, nullptr)) {
		d.fail("C13 fstat of creator object failed");
		d.finish_case(gen_pids_line("C13", victim));
		return d.ok();
	}

	const std::string retained_creator = shm + ".creator-retained";
	if (::link(("/dev/shm" + shm).c_str(), ("/dev/shm" + retained_creator).c_str()) != 0 ||
	    !snapshot_object(d, retained_creator, "creator-before-aba")) {
		d.fail("C13 failed to preserve creator inode before intentional ABA replacement");
		d.finish_case(gen_pids_line("C13", victim));
		return d.ok();
	}
	if (!detail::shm_unlink_checked(shm, dev0, ino0)) {
		d.fail("C13 unlink of creator object failed");
		d.finish_case(gen_pids_line("C13", victim));
		return d.ok();
	}
	auto fd1 = detail::shm_open_create(shm);
	if (!fd1) {
		d.fail("C13 replacement object create failed");
		d.finish_case(gen_pids_line("C13", victim));
		return d.ok();
	}
	if (!detail::shm_truncate(fd1.value(), 832)) {
		d.fail("C13 replacement object truncate failed");
		d.finish_case(gen_pids_line("C13", victim));
		return d.ok();
	}
	uint64_t dev1 = 0, ino1 = 0, size1 = 0;
	if (!detail::shm_fstat_and_capture(fd1.value(), &dev1, &ino1, &size1)) {
		d.fail("C13 fstat of replacement object failed");
		d.finish_case(gen_pids_line("C13", victim));
		return d.ok();
	}
	ev += "aba: creator_ino=" + std::to_string(ino0) +
	      " replacement_ino=" + std::to_string(ino1) + "\n";

	ChannelOptions opts;
	opts.name = name;
	auto p = Producer<TestPayloadV1>::create(opts, TestPayloadV1Schema());
	if (p) {
		d.fail("C13 recovery must not touch the stranger's object");
	} else {
		const std::string code = to_string(p.error().code);
		ev += "create blocked code=" + code + " ctx=" + p.error().context + "\n";
		if (p.error().code != ErrorCode::kNameRaceDetected) {
			d.fail("C13 expected kNameRaceDetected, got " + code);
		}
	}

	{
		auto fd2 = detail::shm_open_existing(shm);
		if (!fd2) {
			d.fail("C13 stranger object disappeared");
		} else {
			uint64_t dev2 = 0, ino2 = 0, size2 = 0;
			if (detail::shm_fstat_and_capture(fd2.value(), &dev2, &ino2, &size2)) {
				if (dev2 != dev1 || ino2 != ino1 || size2 != size1) {
					d.fail("C13 stranger object altered");
				}
				ev += "stranger_untouched dev=" + std::to_string(dev2) +
				      " ino=" + std::to_string(ino2) +
				      " size=" + std::to_string(size2) + "\n";
			}
		}
	}

	if (!snapshot_object(d, shm, "retained-object")) {
		d.fail("C13 failed to preserve replacement object snapshot");
	}

	d.write_evidence("recovery_result.txt", ev);
	d.finish_case(gen_pids_line("C13", victim));
	return d.ok();
}

static bool run_c14(CaseDriver& d) {
	d.begin_case("C14",
	             "fd producer dies in serve_loop after one hand-out: consumer "
	             "bounded-retry -> ProducerOffline, stale socket probe -> "
	             "successor rebinds gen+1");
	const std::string name = d.channel_name("c14");

	CrashChild victim;
	if (!victim.spawn({d.prod_bin, "--name", name, "--transport", "fd",
	                   "--interval-us", "50000"},
	                  fp_env("C14", nullptr))) {
		d.fail("C14 victim spawn failed");
		d.finish_case(gen_pids_line("C14", victim));
		return d.ok();
	}

	const CommandResult first = d.run_command(
	        {d.cons_bin, "--name", name, "--transport", "fd", "--reads", "1",
	         "--read-interval-ms", "20", "--open-retry-ms", "8000"});
	d.expect_contains(first.out, "SUMMARY reads=1", "C14 first consumer");
	d.expect_contains(first.out, "torn=0", "C14 first consumer");

	if (!victim.wait_stop(8000)) {
		d.fail("C14 victim never stopped at C14");
		d.finish_case(gen_pids_line("C14", victim));
		return d.ok();
	}
	victim.kill_and_reap();

	const CommandResult dump = d.ctl_inspect(name);
	d.expect_contains(dump.out, "transport=fd_pass", "C14 header_dump");
	d.expect_contains(dump.out, "broker_failed", "C14 header_dump");

	const CommandResult second = d.run_command(
	        {d.cons_bin, "--name", name, "--transport", "fd", "--reads", "1",
	         "--open-retry-ms", "3000"});
	d.expect_contains(second.out, "code=ProducerOffline", "C14 second consumer");

	const CommandResult rec = d.run_command(
	        {d.prod_bin, "--name", name, "--transport", "fd", "--count", "2"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "GENERATION 2", "C14 recovery");
	d.expect_contains(rec.out, "DONE published=2", "C14 recovery");
	if (rec.exit_code != 0)
		d.fail("C14 recovery producer exit " + std::to_string(rec.exit_code));

	d.finish_case(gen_pids_line("C14", victim));
	return d.ok();
}

static bool run_c15(CaseDriver& d) {
	d.begin_case("C15",
	             "fd consumer dies after identity registration: dead identity "
	             "reclaimed by the next open, producer keeps publishing");
	const std::string name = d.channel_name("c15");

	CrashChild prod;
	if (!prod.spawn({d.prod_bin, "--name", name, "--transport", "fd",
	                 "--interval-us", "20000"},
	                {})) {
		d.fail("C15 producer spawn failed");
		d.finish_case(gen_pids_line("C15", prod));
		return d.ok();
	}
	struct timespec wait_ts {};
	wait_ts.tv_nsec = 400 * 1000 * 1000;
	::nanosleep(&wait_ts, nullptr);

	CrashChild victim;
	if (!victim.spawn({d.cons_bin, "--name", name, "--transport", "fd", "--reads", "1"},
	                  fp_env("C15", nullptr)) ||
	    !victim.wait_stop(8000)) {
		d.fail("C15 victim never stopped at C15");
		d.finish_case(gen_pids_line("C15", victim));
		return d.ok();
	}
	victim.kill_and_reap();

	const CommandResult dump = d.ctl_inspect(name);
	d.expect_contains(dump.out, "consumer=online", "C15 header_dump");

	const CommandResult rec = d.run_command(
	        {d.cons_bin, "--name", name, "--transport", "fd", "--reads", "2",
	         "--read-interval-ms", "20", "--open-retry-ms", "5000"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "SUMMARY reads=2", "C15 recovery");
	d.expect_contains(rec.out, "torn=0", "C15 recovery");
	if (rec.exit_code != 0) d.fail("C15 recovery consumer exit " + std::to_string(rec.exit_code));

	prod.kill_and_reap();
	d.finish_case(gen_pids_line("C15", victim));
	return d.ok();
}

static bool run_c16(CaseDriver& d) {
	d.begin_case("C16",
	             "fd creator dies after memfd_create: object gone with creator, "
	             "journal reconciles, successor creates gen 1");
	const std::string name = d.channel_name("c16");

	CrashChild victim;
	if (!victim.spawn({d.prod_bin, "--name", name, "--transport", "fd", "--count", "0"},
	                  fp_env("C16", nullptr)) ||
	    !victim.wait_stop(8000)) {
		d.fail("C16 victim never stopped at C16");
		d.finish_case(gen_pids_line("C16", victim));
		return d.ok();
	}
	victim.kill_and_reap();

	const CommandResult dump = d.ctl_inspect(name);
	d.expect_contains(dump.out, "JOURNAL state=creating_pre_object", "C16 header_dump");
	d.expect_contains(dump.out, "transport=fd_pass", "C16 header_dump");

	const CommandResult rec = d.run_command(
	        {d.prod_bin, "--name", name, "--transport", "fd", "--count", "3"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "GENERATION 1", "C16 recovery");
	d.expect_contains(rec.out, "DONE published=3", "C16 recovery");
	if (rec.exit_code != 0)
		d.fail("C16 recovery producer exit " + std::to_string(rec.exit_code));

	const CommandResult rm = d.run_command({d.ctl_bin, "remove", name});
	d.expect_contains(rm.out, "REMOVED fd_pass", "C16 remove");

	d.finish_case(gen_pids_line("C16", victim));
	return d.ok();
}

static bool run_c17(CaseDriver& d) {
	d.begin_case("C17",
	             "stale broker socket at the path: create probes it dead, unlinks "
	             "and rebinds (no ghost instance, no AlreadyOwned)");
	const std::string name = d.channel_name("c17");
	const std::string sock = detail::channel_socket_path(name);

	{
		auto lock = detail::ControlLock::acquire(detail::channel_lock_path(name));
		if (!lock) {
			d.fail("C17 prepare control directory failed: " +
			       std::string(to_string(lock.error().code)));
			d.finish_case(gen_pids_line("C17", CrashChild{}));
			return d.ok();
		}
	}

	{
		const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
		if (fd < 0) {
			d.fail("C17 stage socket failed");
			d.finish_case(gen_pids_line("C17", CrashChild{}));
			return d.ok();
		}
		struct sockaddr_un addr {};
		addr.sun_family = AF_UNIX;
		if (sock.size() >= sizeof(addr.sun_path)) {
			::close(fd);
			d.fail("C17 socket path too long");
			d.finish_case(gen_pids_line("C17", CrashChild{}));
			return d.ok();
		}
		std::memcpy(addr.sun_path, sock.c_str(), sock.size() + 1);
		if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr),
		           static_cast<socklen_t>(sizeof(addr))) != 0) {
			::close(fd);
			d.fail("C17 stage bind failed");
			d.finish_case(gen_pids_line("C17", CrashChild{}));
			return d.ok();
		}
		::close(fd);
	}

	const CommandResult rec = d.run_command(
	        {d.prod_bin, "--name", name, "--transport", "fd", "--count", "2"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "GENERATION 1", "C17 create");
	d.expect_contains(rec.out, "DONE published=2", "C17 create");
	if (rec.exit_code != 0) d.fail("C17 create exit " + std::to_string(rec.exit_code));

	d.finish_case(gen_pids_line("C17", CrashChild{}));
	return d.ok();
}

static bool run_c18(CaseDriver& d) {
	d.begin_case("C18",
	             "producer frozen after one heartbeat: consumer wait classifies "
	             "ProducerStalled (observation, no takeover)");
	const std::string name = d.channel_name("c18");

	CrashChild victim;
	if (!victim.spawn({d.prod_bin, "--name", name, "--heartbeat-interval-us", "100000",
	                   "--heartbeat-only"},
	                  fp_env("C18", nullptr)) ||
	    !victim.wait_stop(8000)) {
		d.fail("C18 victim never stopped at C18");
		d.finish_case(gen_pids_line("C18", victim));
		return d.ok();
	}

	const CommandResult dump = d.ctl_inspect(name);
	d.expect_contains(dump.out, "abi=1.1", "C18 header_dump");
	d.expect_contains(dump.out, "producer=online", "C18 header_dump");

	const CommandResult rec = d.run_command(
	        {d.cons_bin, "--name", name, "--use-wait-ms", "2000",
	         "--read-timeout-ms", "0", "--open-retry-ms", "5000"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "last_error=ProducerStalled", "C18 consumer");
	d.expect_contains(rec.out, "timed_out=1", "C18 consumer");

	const CommandResult takeover = d.run_command(
	        {d.prod_bin, "--name", name, "--count", "1"});
	d.expect_contains(takeover.out, "code=AlreadyOwned", "C18 no takeover");

	victim.kill_and_reap();
	d.finish_case(gen_pids_line("C18", victim));
	return d.ok();
}

static bool run_c19(CaseDriver& d) {
	d.begin_case("C19",
	             "supervisor stall takeover: frozen heartbeat producer -> "
	             "STALL_DETECTED -> KILLED -> RESTART gen+1, consumer reads");
	if (d.supervisor_bin.empty()) {
		d.fail("C19 needs --supervisor <bin>");
		d.finish_case("C19 supervisor_bin_unset\n");
		return d.ok();
	}
	const std::string name = d.channel_name("c19");

	CrashChild sup;
	if (!sup.spawn({d.supervisor_bin, "--name", name, "--producer-argv",
	                d.prod_bin + " --name " + name +
	                        " --heartbeat-only --heartbeat-interval-us 100000",
	                "--watch-interval-ms", "100", "--stall-grace-ms", "500",
	                "--initial-delay-ms", "200"},
	               {})) {
		d.fail("C19 supervisor spawn failed");
		d.finish_case(gen_pids_line("C19", sup));
		return d.ok();
	}
	struct timespec wait_ts {};
	wait_ts.tv_sec = 1;
	wait_ts.tv_nsec = 500 * 1000 * 1000;
	::nanosleep(&wait_ts, nullptr);

	const std::string stop_cmd =
	        "/usr/bin/pkill -STOP -f \"^" + d.prod_bin + " --name " + name + " \"";
	(void)d.run_command({"/bin/sh", "-c", stop_cmd});

	wait_ts.tv_sec = 3;
	wait_ts.tv_nsec = 0;
	::nanosleep(&wait_ts, nullptr);

	const CommandResult dump = d.ctl_inspect(name);
	d.expect_contains(dump.out, "gen=2", "C19 header_dump");
	const CommandResult rec = d.run_command(
	        {d.cons_bin, "--name", name, "--reads", "1", "--read-interval-ms", "20",
	         "--open-retry-ms", "8000", "--read-timeout-ms", "2000"});
	d.write_evidence("recovery_result.txt",
	                 rec.out + "EXIT " + std::to_string(rec.exit_code) + "\n");
	d.expect_contains(rec.out, "READY", "C19 recovery");
	if (rec.exit_code != 0)
		d.fail("C19 recovery consumer exit " + std::to_string(rec.exit_code));

	sup.send_signal(SIGTERM);
	const int sup_rc = sup.wait_exit(15000);
	d.write_evidence("supervisor_result.txt",
	                 sup.stdout_text() + "EXIT " + std::to_string(sup_rc) + "\n");
	d.expect_contains(sup.stdout_text(), "STALL_DETECTED", "C19 supervisor");
	d.expect_contains(sup.stdout_text(), "KILLED sig=9", "C19 supervisor");
	d.expect_contains(sup.stdout_text(), "RESTART attempt=1", "C19 supervisor");
	d.expect_contains(sup.stdout_text(), "gen=2", "C19 supervisor");
	d.expect_contains(sup.stdout_text(), "STOPPED", "C19 supervisor");

	d.finish_case(gen_pids_line("C19", sup));
	return d.ok();
}

static bool run_c20(CaseDriver& d) {
	d.begin_case("C20",
	             "supervisor crash loop: /bin/false child -> RESTART x3 -> "
	             "GAVE_UP, non-zero exit");
	if (d.supervisor_bin.empty()) {
		d.fail("C20 needs --supervisor <bin>");
		d.finish_case("C20 supervisor_bin_unset\n");
		return d.ok();
	}
	const std::string name = d.channel_name("c20");

	CrashChild sup;
	if (!sup.spawn({d.supervisor_bin, "--name", name, "--producer-argv", "/bin/false",
	                "--watch-interval-ms", "100", "--max-restarts", "3",
	                "--max-delay-ms", "200", "--initial-delay-ms", "50",
	                "--create-timeout-ms", "1000"},
	               {})) {
		d.fail("C20 supervisor spawn failed");
		d.finish_case(gen_pids_line("C20", sup));
		return d.ok();
	}
	const int rc = sup.wait_exit(30000);
	d.write_evidence("supervisor_result.txt",
	                 sup.stdout_text() + "EXIT " + std::to_string(rc) + "\n");
	if (rc != 3) d.fail("C20 expected supervisor exit 3 (GAVE_UP), got " +
	                    std::to_string(rc));
	d.expect_contains(sup.stdout_text(), "GAVE_UP", "C20 supervisor");
	d.expect_contains(sup.stdout_text(), "GAVE_UP attempts=4 restarts=3", "C20 supervisor");
	d.expect_contains(sup.stdout_text(), "RESTART attempt=1 delay=50000000ns", "C20 supervisor");
	d.expect_contains(sup.stdout_text(), "RESTART attempt=2 delay=100000000ns", "C20 supervisor");
	d.expect_contains(sup.stdout_text(), "RESTART attempt=3 delay=200000000ns", "C20 supervisor");
	if (contains(sup.stdout_text(), "RESTART attempt=4")) {
		d.fail("C20 restarted past the cap");
	}

	d.finish_case(gen_pids_line("C20", sup));
	return d.ok();
}

static bool run_c21(CaseDriver& d) {
	d.begin_case("C21",
	             "supervisor clean exit: --count 2 child finishes -> "
	             "CLEAN_EXIT, no restart, exit 0");
	if (d.supervisor_bin.empty()) {
		d.fail("C21 needs --supervisor <bin>");
		d.finish_case("C21 supervisor_bin_unset\n");
		return d.ok();
	}
	const std::string name = d.channel_name("c21");

	CrashChild sup;
	if (!sup.spawn({d.supervisor_bin, "--name", name, "--producer-argv",
	                d.prod_bin + " --name " + name + " --count 2 --interval-us 20000",
	                "--watch-interval-ms", "100"},
	               {})) {
		d.fail("C21 supervisor spawn failed");
		d.finish_case(gen_pids_line("C21", sup));
		return d.ok();
	}
	const int rc = sup.wait_exit(20000);
	d.write_evidence("supervisor_result.txt",
	                 sup.stdout_text() + "EXIT " + std::to_string(rc) + "\n");
	if (rc != 0) d.fail("C21 expected supervisor exit 0, got " + std::to_string(rc));
	d.expect_contains(sup.stdout_text(), "SUPERVISED", "C21 supervisor");
	d.expect_contains(sup.stdout_text(), "CLEAN_EXIT", "C21 supervisor");
	if (contains(sup.stdout_text(), "RESTART")) {
		d.fail("C21 restarted after a clean exit");
	}

	d.finish_case(gen_pids_line("C21", sup));
	return d.ok();
}

struct CaseDef {
	const char* id;
	const char* desc;
	bool (*fn)(CaseDriver&);
};

static const CaseDef kCases[] = {
        {"C01", "creator dies after shm_open (size-0 pre-object, auto-clean)", run_c01},
        {"C02", "creator dies after bootstrap write (partial object, recovered)", run_c02},
        {"C03", "producer dies after claim (WRITING, invisible)",
         [](CaseDriver& d) {
	         return run_c_publish(d, "C03", "C03",
	                              "producer killed after slot->WRITING; the WRITING "
	                              "sample is invisible, the old current stays readable",
	                              false);
         }},
        {"C04", "producer dies mid payload copy (WRITING, invisible)",
         [](CaseDriver& d) {
	         return run_c_publish(d, "C04", "C04",
	                              "producer killed mid payload copy; torn-free read of "
	                              "the old current",
	                              false);
         }},
        {"C05", "producer dies after publish, before ticket (unpublished invisible)",
         [](CaseDriver& d) {
	         return run_c_publish(d, "C05", "C05",
	                              "slot PUBLISHED but ticket uncommitted: the sample is "
	                              "invisible to readers",
	                              false);
         }},
        {"C06", "producer dies after ticket, before wake (ticketed visible)",
         [](CaseDriver& d) {
	         return run_c_publish(d, "C06", "C06",
	                              "ticket committed: the ticketed sample is visible "
	                              "even though the producer never woke waiters",
	                              true);
         }},
        {"C07", "consumer dies at READING_CLAIMING (epoch unset, reclaimed)",
         [](CaseDriver& d) {
	         return run_c_consumer(d, "C07", "C07",
	                               "consumer killed between claim and epoch write; the "
	                               "epoch-0 slot is reclaimed on open",
	                               "state=reading_claiming ");
         }},
        {"C08", "consumer dies at READING (epoch set, reclaimed)",
         [](CaseDriver& d) {
	         return run_c_consumer(d, "C08", "C08",
	                               "consumer killed while READING; the slot is "
	                               "reclaimed by role epoch on open",
	                               "state=reading ");
         }},
        {"C09", "consumer dies after release (nothing leaked)",
         [](CaseDriver& d) {
	         return run_c_consumer(d, "C09", "C09",
	                               "consumer killed after release_read: the slot is "
	                               "back to PUBLISHED, no leak survives",
	                               nullptr);
         }},
        {"C10", "old producer dead + two new producers (lock serializes, gen+1)", run_c10},
        {"C11", "PID-reuse fixture (live pid + wrong starttime -> kPidReused, gen+1)", run_c11},
        {"C12", "identity-epoch corruption (odd epoch -> RecoveryBlocked, closed)", run_c12},
        {"C13", "name/inode ABA (replaced object -> kNameRaceDetected)", run_c13},
        {"C14", "fd producer dies after one fd hand-out (stale socket -> successor rebinds)", run_c14},
        {"C15", "fd consumer dies after identity registration (role reclaimed)", run_c15},
        {"C16", "fd creator dies after memfd_create (object dies with creator)", run_c16},
        {"C17", "stale broker socket (probe-dead -> unlink -> rebind)", run_c17},
        {"C18", "heartbeat producer frozen (ProducerStalled, no takeover)", run_c18},
        {"C19", "supervisor stall takeover (STALL_DETECTED -> KILLED -> RESTART gen+1)", run_c19},
        {"C20", "supervisor crash loop (RESTART x3 -> GAVE_UP)", run_c20},
        {"C21", "supervisor clean exit (CLEAN_EXIT, no restart)", run_c21},
        {"C22", "loaned producer dies while holding WRITING (invisible)",
         [](CaseDriver& d) {
	         return run_c_publish(d, "C22", "C22",
	                              "write-loan owner killed while WRITING; the borrowed "
	                              "bytes stay invisible and the old ticket is readable",
	                              false, true);
         }},
        {"C23", "loaned consumer dies while holding READING (reclaimed)",
         [](CaseDriver& d) {
	         return run_c_consumer(d, "C23", "C23",
	                               "read-loan owner killed while READING; a new consumer "
	                               "reclaims the slot by role epoch",
	                               "state=reading ", true);
         }},
};

static const char* kSmokeCases[] = {"C01", "C03", "C08", "C16", "C22", "C23"};

const char* arg_value(int argc, char** argv, const char* flag) {
	for (int i = 1; i + 1 < argc; ++i) {
		if (std::strcmp(argv[i], flag) == 0) return argv[i + 1];
	}
	return nullptr;
}

bool arg_has(int argc, char** argv, const char* flag) {
	for (int i = 1; i < argc; ++i) {
		if (std::strcmp(argv[i], flag) == 0) return true;
	}
	return false;
}

int main(int argc, char** argv) {
	if (arg_has(argc, argv, "--list")) {
		std::printf("self_ipcs_crash_matrix case table:\n");
		for (const CaseDef& c : kCases) {
			std::printf("  %-4s  %s\n", c.id, c.desc);
		}
		std::printf("smoke subset:");
		for (const char* id : kSmokeCases) std::printf(" %s", id);
		std::printf("\n");
		return 0;
	}

	const std::string prod = arg_value(argc, argv, "--producer") != nullptr
	                                 ? arg_value(argc, argv, "--producer")
	                                 : edge_tool::sibling_executable("self_ipcs_shm_producer");
	const std::string cons = arg_value(argc, argv, "--consumer") != nullptr
	                                 ? arg_value(argc, argv, "--consumer")
	                                 : edge_tool::sibling_executable("self_ipcs_shm_consumer");
	const std::string ctl = arg_value(argc, argv, "--ctl") != nullptr
	                                ? arg_value(argc, argv, "--ctl")
	                                : edge_tool::sibling_executable("self_ipcs_shm_ctl");
	const std::string supervisor =
	        arg_value(argc, argv, "--supervisor") != nullptr
	                ? arg_value(argc, argv, "--supervisor")
	                : edge_tool::sibling_executable("self_ipcs_shm_supervisor");
	const char* out_dir = arg_value(argc, argv, "--out-dir");
	const char* seed = arg_value(argc, argv, "--seed");
	const bool smoke = arg_has(argc, argv, "--smoke");
	const char* only = arg_value(argc, argv, "--only");

	if (arg_has(argc, argv, "--help") || prod.empty() || cons.empty() || ctl.empty()) {
		std::fprintf(stderr,
		             "usage: self_ipcs_crash_matrix [--producer <bin>] [--consumer <bin>] "
		             "[--ctl <bin>] [--supervisor <bin>] [--smoke] [--only C01,C03] "
		             "[--seed N] [--out-dir P]\n");
		return arg_has(argc, argv, "--help") ? 0 : 2;
	}

	std::vector<const CaseDef*> selected;
	if (only != nullptr) {
		const char* p = only;
		while (*p != '\0') {
			const char* comma = std::strchr(p, ',');
			const size_t len = comma ? static_cast<size_t>(comma - p) : std::strlen(p);
			for (const CaseDef& c : kCases) {
				if (std::strlen(c.id) == len && std::strncmp(c.id, p, len) == 0) {
					selected.push_back(&c);
				}
			}
			if (comma == nullptr) break;
			p = comma + 1;
		}
	} else if (smoke) {
		for (const char* id : kSmokeCases) {
			for (const CaseDef& c : kCases) {
				if (std::strcmp(c.id, id) == 0) selected.push_back(&c);
			}
		}
	} else {
		for (const CaseDef& c : kCases) selected.push_back(&c);
	}
	if (selected.empty()) {
		std::fprintf(stderr, "no cases selected\n");
		return 2;
	}

	char run_id[128];
	std::snprintf(run_id, sizeof(run_id), "run_%llu_%ld_s%s",
	              static_cast<unsigned long long>(unix_secs_now()),
	              static_cast<long>(::getpid()), seed != nullptr ? seed : "0");
	const std::string run_dir =
	        std::string(out_dir != nullptr ? out_dir : "evidence/crash") + "/" + run_id;
	{
		std::vector<std::string> mk{"/bin/mkdir", "-p", run_dir};
		std::vector<char*> args;
		for (const std::string& a : mk) args.push_back(const_cast<char*>(a.c_str()));
		args.push_back(nullptr);
		const pid_t mk_pid = ::fork();
		if (mk_pid == 0) {
			::execv(args[0], args.data());
			_exit(127);
		}
		int st = 0;
		if (mk_pid > 0) ::waitpid(mk_pid, &st, 0);
	}

	CaseDriver d;
	d.prod_bin = prod;
	d.cons_bin = cons;
	d.ctl_bin = ctl;
	d.supervisor_bin = supervisor;
	d.run_dir = run_dir;

	std::printf("self_ipcs_crash_matrix run=%s cases=%zu\n", run_id, selected.size());
	std::fflush(stdout);
	for (const CaseDef* c : selected) {
		const bool ok = c->fn(d);
		std::printf("CASE %s %s\n", c->id, ok ? "PASS" : "FAIL");
		std::fflush(stdout);
	}
	std::printf("SUMMARY total=%zu failed=%d evidence=%s\n", selected.size(), d.failures,
	            run_dir.c_str());
	std::fflush(stdout);
	return d.failures == 0 ? 0 : 1;
}
