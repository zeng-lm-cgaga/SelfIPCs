#ifndef SELF_IPCS_QUEUE_POOL_DEMO_QUEUE_FLOW_HPP
#define SELF_IPCS_QUEUE_POOL_DEMO_QUEUE_FLOW_HPP

#include <sys/types.h>

#include <string>

namespace queue_pool_demo {

bool run_queue_flow();
int run_queue_helper(const std::string& name, int descriptor, pid_t parent_pid);

}  // namespace queue_pool_demo

#endif
