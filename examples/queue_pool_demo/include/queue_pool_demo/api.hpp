#ifndef SELF_IPCS_QUEUE_POOL_DEMO_API_HPP
#define SELF_IPCS_QUEUE_POOL_DEMO_API_HPP

#if defined(SELFIPCS_DEMO_USE_LEGACY)
#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/common/cancellation.hpp"
#include "edge_runtime/common/diagnostics.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/common/schema.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#else
#include "self_ipcs/buffer/shared_buffer_pool.hpp"
#include "self_ipcs/common/cancellation.hpp"
#include "self_ipcs/common/diagnostics.hpp"
#include "self_ipcs/common/error.hpp"
#include "self_ipcs/common/schema.hpp"
#include "self_ipcs/queue/mpmc_queue.hpp"
#endif

namespace queue_pool_demo {

#if defined(SELFIPCS_DEMO_USE_LEGACY)
namespace ipc = edge_runtime;
inline constexpr const char* kPackageName = "EdgeRuntime";
#else
namespace ipc = self_ipcs;
inline constexpr const char* kPackageName = "SelfIPCs";
#endif

}  // namespace queue_pool_demo

#endif
