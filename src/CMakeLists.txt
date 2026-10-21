add_library(self_ipcs STATIC
  edge_runtime/common/error.cpp
  edge_runtime/channel/channel_abi.cpp
  edge_runtime/channel/channel_layout.cpp
  edge_runtime/channel/channel_observer.cpp
  edge_runtime/channel/consumer_impl.cpp
  edge_runtime/channel/event_notification.cpp
  edge_runtime/channel/control_lock.cpp
  edge_runtime/channel/loan.cpp
  edge_runtime/channel/producer_impl.cpp
  edge_runtime/channel/slot_protocol.cpp
  edge_runtime/queue/mpmc_queue.cpp
  edge_runtime/buffer/shared_buffer_pool.cpp
  edge_runtime/process/process_identity.cpp
  edge_runtime/process/process_spawn.cpp
  edge_runtime/process/supervisor_impl.cpp
  edge_runtime/transport/fd_broker.cpp
  edge_runtime/transport/socket_io.cpp
  edge_runtime/transport/shm_object.cpp
  edge_runtime/sync/clock.cpp
  edge_runtime/sync/futex.cpp
  edge_runtime/sync/cancellation.cpp
  edge_runtime/utility/checksum.cpp
  edge_runtime/utility/failpoint.cpp)
add_library(SelfIPCs::self_ipcs ALIAS self_ipcs)
add_library(EdgeRuntime::edge_runtime ALIAS self_ipcs)
add_library(edge_runtime INTERFACE ${CMAKE_CURRENT_LIST_FILE})
target_link_libraries(edge_runtime INTERFACE self_ipcs)
add_dependencies(edge_runtime self_ipcs)

target_compile_features(self_ipcs PUBLIC cxx_std_17)
target_include_directories(
  self_ipcs
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

selfipcs_apply_project_options(self_ipcs)
target_link_libraries(self_ipcs PUBLIC Threads::Threads PRIVATE rt)
