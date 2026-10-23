#ifndef EDGE_RUNTIME_TRANSPORT_SOCKET_IO_HPP
#define EDGE_RUNTIME_TRANSPORT_SOCKET_IO_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include "edge_runtime/common/cancellation.hpp"
#include "edge_runtime/transport/shm_object.hpp"

namespace edge_runtime::detail::socket_io {

struct IoBudget {
	uint64_t deadline_ns;
	const std::atomic<bool>* stop = nullptr;
	const CancellationToken* cancellation = nullptr;
};

bool budget_available(const IoBudget& budget) noexcept;
bool wait_socket(int fd, short events, const IoBudget& budget) noexcept;
bool send_full(int fd, const void* buffer, size_t size, const IoBudget& budget) noexcept;
bool recv_full(int fd, void* buffer, size_t size, const IoBudget& budget) noexcept;
bool peer_is_same_uid(int fd) noexcept;
bool connect_socket(int fd, const std::string& path, const IoBudget& budget) noexcept;
bool send_fd(int fd, const void* buffer, size_t size, int payload_fd,
             const IoBudget& budget) noexcept;
bool receive_fd(int fd, void* buffer, size_t size, UniqueFd* payload_fd,
                const IoBudget& budget) noexcept;

}
#endif
