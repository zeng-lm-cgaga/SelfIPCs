#ifndef EDGE_RUNTIME_CHANNEL_CONTROL_OPERATION_HPP
#define EDGE_RUNTIME_CHANNEL_CONTROL_OPERATION_HPP

#include <cstdint>
#include <string>

#include "edge_runtime/channel/control_context.hpp"
#include "edge_runtime/channel/control_lock.hpp"

namespace edge_runtime::detail {
struct ControlRecovery {
	uint64_t owner_pid{};
	std::string lock_path;
	std::string shm_name;
	std::string socket_path;
	uint64_t socket_dev{};
	uint64_t socket_ino{};
	ControlJournalV1 expected{};
	ControlJournalV1 alternate{};
	ControlJournalV1 reset{};
	uint64_t lock_dev{};
	uint64_t lock_ino{};
	uint64_t dev{};
	uint64_t ino{};
	bool pending{false};
	bool remove_object{false};
};

class ControlScope {
       public:
	ControlScope(const ControlOperationContext& context, ControlReceipt& receipt,
	             CancellationToken stop = {}) noexcept;
	~ControlScope();
	ControlScope(const ControlScope&) = delete;
	ControlScope& operator=(const ControlScope&) = delete;
	void committed() noexcept;
	void pending() noexcept;

       private:
	ControlScope* previous_;
	const ControlOperationContext& context_;
	ControlReceipt& receipt_;
	CancellationToken stop_;
	friend Result<void> control_check() noexcept;
	friend Result<void> control_begin_commit() noexcept;
	friend const ControlOperationContext* control_context() noexcept;
	friend int control_stop_handle() noexcept;
	friend Result<void> control_prepare_cleanup(const std::string&, const std::string&);
	friend void control_retain_cleanup(const ControlLock&, const ControlJournalV1&,
	                                   const ControlJournalV1&, bool, uint64_t,
	                                   uint64_t) noexcept;
	friend void control_handle_pending(const Error&) noexcept;
	friend void control_retain_socket(uint64_t, uint64_t) noexcept;
};

Result<void> control_check() noexcept;
Result<void> control_begin_commit() noexcept;
const ControlOperationContext* control_context() noexcept;
void control_committed() noexcept;
void control_handle_pending(
        const Error& error = make_error(ErrorCode::kConcurrentHandleUse, "control finalize",
                                        "owner operation still active")) noexcept;
void control_retain_socket(uint64_t dev, uint64_t ino) noexcept;
int control_stop_handle() noexcept;
Result<void> control_prepare_cleanup(const std::string& path, const std::string& shm_name);
void control_retain_cleanup(const ControlLock& lock, const ControlJournalV1& expected,
                            const ControlJournalV1& reset, bool remove_object, uint64_t dev,
                            uint64_t ino) noexcept;
Result<void> control_pause(uint64_t maximum_ns) noexcept;

}  // namespace edge_runtime::detail
#endif
