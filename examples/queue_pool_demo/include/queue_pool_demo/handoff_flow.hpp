#ifndef SELF_IPCS_QUEUE_POOL_DEMO_HANDOFF_FLOW_HPP
#define SELF_IPCS_QUEUE_POOL_DEMO_HANDOFF_FLOW_HPP

#include <string>

namespace queue_pool_demo {
bool run_handoff_flow();
int run_handoff_helper(const std::string& name, int descriptor, bool producer);
}

#endif
