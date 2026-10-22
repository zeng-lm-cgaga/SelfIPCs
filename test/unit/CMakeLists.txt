
function(edgeruntime_unit_test name)
  add_executable(${name} ${name}.cpp)
  edgeruntime_apply_project_options(${name})
  target_link_libraries(
    ${name} PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
  target_include_directories(
    ${name} PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
      ${CMAKE_SOURCE_DIR}/src)
  gtest_discover_tests(${name} ${ARGN})
endfunction()

# 单元测试覆盖 ABI、错误分类、进程身份、槽协议、传输和监督策略。
edgeruntime_unit_test(abi_layout_test)        # U01-U04：ABI 布局与命名
edgeruntime_unit_test(error_test)             # U08：错误码与 errno 分类
edgeruntime_unit_test(process_identity_test)  # U06/U11/U13：pidfd 与启动时间
edgeruntime_unit_test(control_journal_test)   # U11/U13：控制日志往返
edgeruntime_unit_test(checksum_test)          # FNV-1a 固定校验值
edgeruntime_unit_test(schema_contract_test)   # PayloadCodec 接口与 noexcept
edgeruntime_unit_test(slot_protocol_test)     # U07/U09/U14：槽状态机
edgeruntime_unit_test(futex_test)             # U10：等待、唤醒和超时
edgeruntime_unit_test(clock_test)             # 时间截止和剩余时间计算
edgeruntime_unit_test(failpoint_test)         # 故障点注册、模式和命中次数
edgeruntime_unit_test(fd_broker_test)         # fd 传输记录布局与校验和
edgeruntime_unit_test(channel_observer_test)  # 只读观察和停滞分类
edgeruntime_unit_test(liveness_watch_test)    # 长驻 pidfd 与 CLOEXEC
edgeruntime_unit_test(process_spawn_test)     # posix_spawn 与非阻塞输出管道
edgeruntime_unit_test(producer_concurrency_test)  # 同一句柄并发调用约束
edgeruntime_unit_test(loan_test)                  # 借用槽所有权与延迟关闭
edgeruntime_unit_test(supervisor_policy_test)     # 重启上限、退避和参数边界
edgeruntime_unit_test(mpmc_layout_test)           # MPMC 共享内存 ABI 和映射边界
edgeruntime_unit_test(queue_feature_test PROPERTIES TIMEOUT 10)
edgeruntime_unit_test(pool_wait_feature_test PROPERTIES TIMEOUT 10)
edgeruntime_unit_test(pool_mapping_lifetime_test PROPERTIES TIMEOUT 8)
target_link_options(pool_mapping_lifetime_test PRIVATE
  "LINKER:--wrap=_ZN12edge_runtime6detail10futex_waitEPKvjPK8timespec"
  "LINKER:--wrap=_ZN12edge_runtime6detail14probe_livenessEmm")
edgeruntime_unit_test(pool_queue_test PROPERTIES TIMEOUT 8)
target_link_options(pool_queue_test PRIVATE
  "LINKER:--wrap=_ZN12edge_runtime6detail17failpoint_triggerEPKNS0_15FailpointRecordE")
if(TARGET SelfIPCs::async)
  edgeruntime_unit_test(async_close_test PROPERTIES TIMEOUT 8 LABELS async)
  target_link_libraries(async_close_test PRIVATE SelfIPCs::async)
endif()
edgeruntime_unit_test(shared_buffer_pool_test)    # 固定块池 ABI、状态和 handle 校验
edgeruntime_unit_test(owner_snapshot_test)        # MPMC/Pool 共享 owner tuple 与 epoch 边界
edgeruntime_unit_test(identity_failure_test)
target_link_options(identity_failure_test PRIVATE
  "-Wl,--wrap=open" "-Wl,--wrap=poll" "-Wl,--wrap=read")
edgeruntime_unit_test(creation_failure_test)
target_sources(creation_failure_test PRIVATE ../fixtures/creation_faults.cpp)
target_link_options(creation_failure_test PRIVATE
  "-Wl,--wrap=fstat" "-Wl,--wrap=shm_open" "-Wl,--wrap=statx")
edgeruntime_unit_test(channel_options_test)
edgeruntime_unit_test(event_notification_test PROPERTIES TIMEOUT 15)
edgeruntime_unit_test(notification_bench_driver_test PROPERTIES TIMEOUT 15)
target_sources(notification_bench_driver_test PRIVATE ../../tools/src/bench_child.cpp)
target_include_directories(notification_bench_driver_test PRIVATE ${CMAKE_SOURCE_DIR}/tools/include)
target_link_options(event_notification_test PRIVATE
  "LINKER:--wrap=eventfd,--wrap=write,--wrap=read,--wrap=sendmsg,--wrap=recvmsg,--wrap=send,--wrap=recv,--wrap=poll")

add_executable(broker_observer_peer broker_observer_peer.cpp broker_observer_fixture.cpp
  broker_observer_syscall_fixture.cpp)
edgeruntime_apply_project_options(broker_observer_peer)
target_link_libraries(broker_observer_peer PRIVATE edge_runtime Threads::Threads rt)
target_include_directories(broker_observer_peer PRIVATE ../fixtures ${CMAKE_SOURCE_DIR}/src)
target_compile_definitions(broker_observer_peer PRIVATE
  BROKER_OBSERVER_PEER="$<TARGET_FILE:broker_observer_peer>")
target_link_options(broker_observer_peer PRIVATE
  "LINKER:--wrap=connect,--wrap=send,--wrap=recvmsg,--wrap=sendmsg")

foreach(name IN ITEMS fd_broker_deadline_test channel_observer_ready_test)
  edgeruntime_unit_test(${name})
  target_sources(${name} PRIVATE broker_observer_fixture.cpp)
  target_compile_definitions(${name} PRIVATE
    BROKER_OBSERVER_PEER="$<TARGET_FILE:broker_observer_peer>")
  add_dependencies(${name} broker_observer_peer)
endforeach()
