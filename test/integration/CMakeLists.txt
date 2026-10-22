# 集成测试必须通过独立可执行文件和 fork/exec 验证进程边界行为。

find_package(Python3 QUIET COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
  add_test(NAME cmake_option_compatibility
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/cmake_options_test.py
            --cmake ${CMAKE_COMMAND})
  set_tests_properties(cmake_option_compatibility PROPERTIES TIMEOUT 120 LABELS packaging
    ENVIRONMENT PYTHONDONTWRITEBYTECODE=1)
  set(_selfipcs_compat_build_type ${CMAKE_BUILD_TYPE})
  if(NOT _selfipcs_compat_build_type)
    set(_selfipcs_compat_build_type Release)
  endif()
  add_test(NAME cmake_build_target_compatibility
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/cmake_build_targets_test.py
            --cmake ${CMAKE_COMMAND} --build-type ${_selfipcs_compat_build_type}
            --sanitizers ${SELFIPCS_ENABLE_SANITIZERS} --failpoints ${SELFIPCS_ENABLE_FAILPOINTS})
  set_tests_properties(cmake_build_target_compatibility PROPERTIES TIMEOUT 180 LABELS packaging
    ENVIRONMENT PYTHONDONTWRITEBYTECODE=1)
  unset(_selfipcs_compat_build_type)
endif()

add_executable(notification_peer helpers/notification_peer.cpp)
edgeruntime_apply_project_options(notification_peer)
target_link_libraries(notification_peer PRIVATE self_ipcs)
target_include_directories(notification_peer PRIVATE ../fixtures)
add_executable(notification_cross_exec_test event_notification_test.cpp)
edgeruntime_apply_project_options(notification_cross_exec_test)
target_link_libraries(notification_cross_exec_test PRIVATE self_ipcs GTest::gtest_main)
target_include_directories(notification_cross_exec_test PRIVATE ../fixtures ${CMAKE_SOURCE_DIR}/src)
target_compile_definitions(notification_cross_exec_test PRIVATE
  NOTIFICATION_PEER="$<TARGET_FILE:notification_peer>")
add_dependencies(notification_cross_exec_test notification_peer)
gtest_discover_tests(notification_cross_exec_test PROPERTIES TIMEOUT 30 LABELS integration)

add_executable(channel_options_cli_test channel_options_cli_test.cpp)
edgeruntime_apply_project_options(channel_options_cli_test)
target_link_libraries(channel_options_cli_test PRIVATE GTest::gtest)
target_include_directories(channel_options_cli_test PRIVATE ../fixtures)
add_test(NAME channel_options_cli
  COMMAND channel_options_cli_test
    $<TARGET_FILE:edge_shm_producer> $<TARGET_FILE:edge_shm_consumer>
    $<TARGET_FILE:edge_shm_bench_producer> $<TARGET_FILE:edge_shm_bench_consumer>)
set_tests_properties(channel_options_cli PROPERTIES TIMEOUT 45 LABELS integration)

add_executable(edge_shm_reopen_helper helpers/reopen_child.cpp)
edgeruntime_apply_project_options(edge_shm_reopen_helper)
target_link_libraries(
  edge_shm_reopen_helper PRIVATE edge_runtime Threads::Threads rt)
target_include_directories(
  edge_shm_reopen_helper PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_executable(reopen_test reopen_test.cpp)
edgeruntime_apply_project_options(reopen_test)
target_link_libraries(
  reopen_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  reopen_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME reopen_cross_exec
  COMMAND reopen_test $<TARGET_FILE:edge_shm_reopen_helper>)
set_tests_properties(reopen_cross_exec PROPERTIES TIMEOUT 30 LABELS integration)

add_executable(pattern_test pattern_test.cpp)
edgeruntime_apply_project_options(pattern_test)
target_link_libraries(
  pattern_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  pattern_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME pattern_er2
  COMMAND pattern_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(
  pattern_er2 PROPERTIES TIMEOUT 300 LABELS integration)

add_executable(wait_test wait_test.cpp)
edgeruntime_apply_project_options(wait_test)
target_link_libraries(
  wait_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  wait_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME wait_er3
  COMMAND wait_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(wait_er3 PROPERTIES TIMEOUT 90 LABELS integration)

add_executable(recovery_test recovery_test.cpp)
edgeruntime_apply_project_options(recovery_test)
target_link_libraries(
  recovery_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  recovery_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME recovery_er4
  COMMAND recovery_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(recovery_er4 PROPERTIES TIMEOUT 90 LABELS integration)

add_executable(fd_transport_test fd_transport_test.cpp)
edgeruntime_apply_project_options(fd_transport_test)
target_link_libraries(
  fd_transport_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  fd_transport_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME fd_transport_er10
  COMMAND fd_transport_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>
          $<TARGET_FILE:edge_shm_ctl>)
set_tests_properties(fd_transport_er10 PROPERTIES TIMEOUT 120 LABELS integration)

add_executable(heartbeat_test heartbeat_test.cpp)
edgeruntime_apply_project_options(heartbeat_test)
target_link_libraries(
  heartbeat_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  heartbeat_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME heartbeat_er11
  COMMAND heartbeat_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(heartbeat_er11 PROPERTIES TIMEOUT 90 LABELS integration)

add_executable(supervisor_test supervisor_test.cpp)
edgeruntime_apply_project_options(supervisor_test)
target_link_libraries(
  supervisor_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  supervisor_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME supervisor_er13
  COMMAND supervisor_test
          $<TARGET_FILE:edge_shm_supervisor>
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(supervisor_er13 PROPERTIES TIMEOUT 150 LABELS integration)

add_executable(edge_shm_loan_helper helpers/loan_child.cpp)
edgeruntime_apply_project_options(edge_shm_loan_helper)
target_link_libraries(
  edge_shm_loan_helper PRIVATE edge_runtime Threads::Threads rt)
target_include_directories(
  edge_shm_loan_helper PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_executable(loan_integration_test loan_test.cpp)
edgeruntime_apply_project_options(loan_integration_test)
target_link_libraries(
  loan_integration_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  loan_integration_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME loan_er15
  COMMAND loan_integration_test $<TARGET_FILE:edge_shm_loan_helper>)
set_tests_properties(loan_er15 PROPERTIES TIMEOUT 30 LABELS integration)

add_executable(edge_mpmc_child helpers/mpmc_child.cpp)
edgeruntime_apply_project_options(edge_mpmc_child)
target_link_libraries(
  edge_mpmc_child PRIVATE edge_runtime Threads::Threads rt)
target_include_directories(
  edge_mpmc_child PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_executable(mpmc_queue_test mpmc_queue_test.cpp)
edgeruntime_apply_project_options(mpmc_queue_test)
target_link_libraries(
  mpmc_queue_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  mpmc_queue_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME mpmc_queue_er16
  COMMAND mpmc_queue_test $<TARGET_FILE:edge_mpmc_child>)
set_tests_properties(mpmc_queue_er16 PROPERTIES TIMEOUT 180 LABELS integration)

add_executable(edge_mpmc_legacy_child helpers/mpmc_legacy_child.cpp)
edgeruntime_apply_project_options(edge_mpmc_legacy_child)

add_executable(mpmc_abi_test mpmc_abi_test.cpp)
edgeruntime_apply_project_options(mpmc_abi_test)
target_link_libraries(
  mpmc_abi_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  mpmc_abi_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME mpmc_abi_er19
  COMMAND mpmc_abi_test $<TARGET_FILE:edge_mpmc_legacy_child>)
set_tests_properties(mpmc_abi_er19 PROPERTIES TIMEOUT 60 LABELS integration)

add_executable(mpmc_owner_snapshot_test mpmc_owner_snapshot_test.cpp)
edgeruntime_apply_project_options(mpmc_owner_snapshot_test)
target_link_libraries(
	mpmc_owner_snapshot_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
	mpmc_owner_snapshot_test PRIVATE
	${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
	${CMAKE_SOURCE_DIR}/src)

add_test(
	NAME mpmc_owner_er20
	COMMAND mpmc_owner_snapshot_test $<TARGET_FILE:edge_mpmc_child>)
set_tests_properties(mpmc_owner_er20 PROPERTIES TIMEOUT 180 LABELS integration)

add_executable(edge_mpmc_wait_child helpers/mpmc_wait_child.cpp)
edgeruntime_apply_project_options(edge_mpmc_wait_child)
target_link_libraries(
  edge_mpmc_wait_child PRIVATE edge_runtime Threads::Threads rt)
target_include_directories(
  edge_mpmc_wait_child PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_executable(mpmc_wait_test mpmc_wait_test.cpp)
edgeruntime_apply_project_options(mpmc_wait_test)
target_link_libraries(
  mpmc_wait_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  mpmc_wait_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME mpmc_wait_er18
  COMMAND mpmc_wait_test $<TARGET_FILE:edge_mpmc_wait_child>)
set_tests_properties(mpmc_wait_er18 PROPERTIES TIMEOUT 180 LABELS integration)

add_executable(edge_shared_buffer_pool_child helpers/shared_buffer_pool_child.cpp)
edgeruntime_apply_project_options(edge_shared_buffer_pool_child)
target_link_libraries(
  edge_shared_buffer_pool_child PRIVATE edge_runtime Threads::Threads rt)
target_include_directories(
  edge_shared_buffer_pool_child PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_executable(shared_buffer_pool_integration_test shared_buffer_pool_test.cpp)
edgeruntime_apply_project_options(shared_buffer_pool_integration_test)
target_link_libraries(
  shared_buffer_pool_integration_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  shared_buffer_pool_integration_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
    ${CMAKE_SOURCE_DIR}/src)

add_test(
  NAME shared_buffer_pool_er17
  COMMAND shared_buffer_pool_integration_test $<TARGET_FILE:edge_shared_buffer_pool_child>)
set_tests_properties(shared_buffer_pool_er17 PROPERTIES TIMEOUT 120 LABELS integration)
gtest_add_tests(
  TARGET shared_buffer_pool_integration_test
  SOURCES shared_buffer_pool_test.cpp
  EXTRA_ARGS $<TARGET_FILE:edge_shared_buffer_pool_child>
  TEST_LIST shared_buffer_pool_cases)
set_tests_properties(${shared_buffer_pool_cases} PROPERTIES TIMEOUT 120 LABELS integration)

add_executable(
	consume_demo_cleanup_test
	consume_demo_cleanup_test.cpp
	${CMAKE_SOURCE_DIR}/examples/consume_demo/src/common.cpp
	${CMAKE_SOURCE_DIR}/examples/consume_demo/src/pool_flow.cpp)
edgeruntime_apply_project_options(consume_demo_cleanup_test)
target_link_options(consume_demo_cleanup_test PRIVATE LINKER:--wrap=shm_open)
target_link_libraries(
	consume_demo_cleanup_test PRIVATE edge_runtime Threads::Threads rt)
target_include_directories(
	consume_demo_cleanup_test PRIVATE
	${CMAKE_SOURCE_DIR}/examples/consume_demo/include
	${CMAKE_CURRENT_SOURCE_DIR}/../fixtures
	${CMAKE_SOURCE_DIR}/src)

add_test(NAME consume_demo_cleanup_er20 COMMAND consume_demo_cleanup_test)
set_tests_properties(consume_demo_cleanup_er20 PROPERTIES TIMEOUT 60 LABELS integration)
