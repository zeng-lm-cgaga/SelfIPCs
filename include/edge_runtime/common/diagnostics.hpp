#ifndef EDGE_RUNTIME_COMMON_DIAGNOSTICS_HPP
#define EDGE_RUNTIME_COMMON_DIAGNOSTICS_HPP

#include <cstdint>

namespace edge_runtime {

struct DiagnosticsSnapshot {
	bool enabled{false};
	uint32_t capacity{0};
	uint64_t push_attempts{0};
	uint64_t pop_attempts{0};
	uint64_t acquire_attempts{0};
	uint64_t push_successes{0};
	uint64_t pop_successes{0};
	uint64_t acquire_successes{0};
	uint64_t full{0};
	uint64_t empty{0};
	uint64_t contention{0};
	uint64_t wait_calls{0};
	uint64_t waits{0};
	uint64_t timeouts{0};
	uint64_t cancelled{0};
	uint64_t notifications{0};
	uint64_t close_calls{0};
	uint64_t batch_calls{0};
};

}  // namespace edge_runtime

#endif
