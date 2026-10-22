#ifndef SELF_IPCS_QUEUE_POOL_DEMO_PROCESS_HPP
#define SELF_IPCS_QUEUE_POOL_DEMO_PROCESS_HPP

#include <sys/types.h>

#include <chrono>
#include <string>

namespace queue_pool_demo {

using Clock = std::chrono::steady_clock;

bool send_token(int descriptor, char token, Clock::time_point deadline);
bool expect_token(int descriptor, char expected, Clock::time_point deadline);
std::string unique_name();

class HelperProcess {
       public:
	HelperProcess() = default;
	~HelperProcess();
	HelperProcess(const HelperProcess&) = delete;
	HelperProcess& operator=(const HelperProcess&) = delete;

	bool spawn(const std::string& name, const char* mode = "--queue-helper");
	bool send(char token);
	bool expect(char token);
	bool wait_for_success(Clock::time_point deadline);
	bool stop();
	bool running() const noexcept { return pid_ > 0; }
	pid_t pid() const noexcept { return pid_; }
	int control_descriptor() const noexcept { return descriptor_; }

       private:
	bool reap_until(Clock::time_point deadline);
	void close_socket() noexcept;
	void signal_group(int signal) const noexcept;

	int descriptor_{-1};
	pid_t pid_{-1};
	int status_{0};
};

}  // namespace queue_pool_demo

#endif
