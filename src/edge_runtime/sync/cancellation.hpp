#ifndef EDGE_RUNTIME_SYNC_CANCELLATION_HPP
#define EDGE_RUNTIME_SYNC_CANCELLATION_HPP

#include <cstdint>

#include "edge_runtime/common/cancellation.hpp"

namespace edge_runtime::detail {

class CancellationRegistration {
       public:
	CancellationRegistration(const CancellationToken& token, uint32_t* epoch) noexcept;
	~CancellationRegistration();
	CancellationRegistration(const CancellationRegistration&) = delete;
	CancellationRegistration& operator=(const CancellationRegistration&) = delete;

       private:
	friend struct CancellationState;
	std::shared_ptr<CancellationState> state_;
	uint32_t* epoch_;
	CancellationRegistration* previous_{nullptr};
	CancellationRegistration* next_{nullptr};
};

}

#endif
