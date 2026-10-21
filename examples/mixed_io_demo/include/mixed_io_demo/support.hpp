#ifndef SELFIPCS_MIXED_IO_DEMO_SUPPORT_HPP
#define SELFIPCS_MIXED_IO_DEMO_SUPPORT_HPP

#include <sys/types.h>

#include <chrono>
#include <cstdint>

#include "mixed_io_demo/api.hpp"

namespace mixed_io_demo {

using Clock = std::chrono::steady_clock;
using Deadline = Clock::time_point;
using Producer = ipc::Producer<Message>;
using Consumer = ipc::Consumer<Message>;
inline constexpr uint32_t kCanReadBatch = 16;
inline constexpr uint64_t kNotificationTag = 1;
inline constexpr uint64_t kCanTag = 2;
inline constexpr uint64_t kTimerTag = 4;
inline constexpr uint64_t kStopTag = 8;

class Descriptor {
       public:
	explicit Descriptor(int descriptor = -1) : descriptor_(descriptor) {}
	~Descriptor();
	Descriptor(const Descriptor&) = delete;
	Descriptor& operator=(const Descriptor&) = delete;
	int get() const { return descriptor_; }
	void close();

       private:
	int descriptor_;
};

class Watch {
       public:
	Watch(int poller, int descriptor, uint64_t tag);
	~Watch();
	Watch(const Watch&) = delete;
	Watch& operator=(const Watch&) = delete;
	bool installed() const { return installed_; }
	bool remove();

       private:
	int poller_;
	int descriptor_;
	bool installed_{false};
};

class Child {
       public:
	explicit Child(pid_t pid) : pid_(pid) {}
	~Child();
	Child(const Child&) = delete;
	Child& operator=(const Child&) = delete;
	bool wait(Deadline deadline);

       private:
	pid_t pid_;
};

uint64_t monotonic_ns();
uint64_t cpu_ns();
bool system_error(const char* operation);
bool api_error(const char* operation, const ipc::Error& error);
int remaining_ms(Deadline deadline);
bool send_token(int descriptor, char token, Deadline deadline);
bool await_token(int descriptor, char expected, Deadline deadline);
int open_can(const Options& options, bool receive);
bool arm_timer(int descriptor, uint32_t interval_ms);
bool read_counter(int descriptor, uint64_t& count);
bool write_counter(int descriptor, uint64_t count);

}

#endif
