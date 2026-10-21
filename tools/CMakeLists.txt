# 工具用于运行诊断、故障恢复和跨进程性能采集，不属于公共 API。
function(selfipcs_tool name output_name)
  if(ARGC GREATER 2)
    set(source ${ARGV2})
  else()
    set(source src/${name}.cpp)
  endif()
  add_executable(${name} ${source})
  set_target_properties(${name} PROPERTIES OUTPUT_NAME ${output_name})
  cmake_language(EVAL CODE "
    cmake_language(
      DEFER DIRECTORY \"${CMAKE_SOURCE_DIR}\"
      CALL add_custom_target ${output_name} DEPENDS ${name})
  ")
  selfipcs_apply_project_options(${name})
  target_link_libraries(${name} PRIVATE SelfIPCs::self_ipcs Threads::Threads rt)
  target_include_directories(
    ${name} PRIVATE
      ${CMAKE_SOURCE_DIR}/test/fixtures
      ${CMAKE_SOURCE_DIR}/src
      ${CMAKE_CURRENT_SOURCE_DIR}/include
      ${CMAKE_CURRENT_SOURCE_DIR})

  add_custom_command(
    TARGET ${name} POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E rm -f $<TARGET_FILE_DIR:${name}>/${name}
    COMMAND ${CMAKE_COMMAND} -E create_symlink
            $<TARGET_FILE_NAME:${name}> $<TARGET_FILE_DIR:${name}>/${name}
    VERBATIM)
  set_property(
    TARGET ${name} APPEND PROPERTY ADDITIONAL_CLEAN_FILES
    $<TARGET_FILE_DIR:${name}>/${name})

  install(TARGETS ${name} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  if(IS_ABSOLUTE "${CMAKE_INSTALL_BINDIR}")
    set(alias_directory "\$ENV{DESTDIR}${CMAKE_INSTALL_BINDIR}")
  else()
    set(alias_directory "\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}")
  endif()
  install(CODE "
    set(_selfipcs_tool_directory \"${alias_directory}\")
    file(REMOVE \"\${_selfipcs_tool_directory}/${name}\")
    file(CREATE_LINK \"${output_name}\" \"\${_selfipcs_tool_directory}/${name}\" SYMBOLIC)
  ")

  if(BUILD_TESTING)
    add_test(NAME ${output_name}_help COMMAND ${name} --help)
    add_test(
      NAME ${output_name}_legacy_help
      COMMAND $<TARGET_FILE_DIR:${name}>/${name} --help)
    set_tests_properties(
      ${output_name}_help ${output_name}_legacy_help
      PROPERTIES TIMEOUT 10 LABELS cli WORKING_DIRECTORY /
      PASS_REGULAR_EXPRESSION "${output_name}")
  endif()
endfunction()

selfipcs_tool(edge_runtime_platform_probe self_ipcs_platform_probe src/platform_probe.cpp)
selfipcs_tool(edge_shm_producer self_ipcs_shm_producer)
selfipcs_tool(edge_shm_consumer self_ipcs_shm_consumer)

selfipcs_tool(edge_shm_ctl self_ipcs_shm_ctl)
selfipcs_tool(edge_shm_supervisor self_ipcs_shm_supervisor)

selfipcs_tool(edge_crash_matrix self_ipcs_crash_matrix)

selfipcs_tool(edge_shm_bench self_ipcs_shm_bench)
target_sources(edge_shm_bench PRIVATE src/bench_child.cpp)
selfipcs_tool(edge_shm_bench_producer self_ipcs_shm_bench_producer)
selfipcs_tool(edge_shm_bench_consumer self_ipcs_shm_bench_consumer)
selfipcs_tool(edge_shm_bench_sock self_ipcs_shm_bench_sock)
selfipcs_tool(edge_mpmc_bench self_ipcs_mpmc_bench)
selfipcs_tool(edge_buffer_pool_bench self_ipcs_buffer_pool_bench)
selfipcs_tool(edge_mpmc_handshake_helper self_ipcs_mpmc_handshake_helper)
selfipcs_tool(edge_buffer_pool_handshake_helper self_ipcs_buffer_pool_handshake_helper)
selfipcs_tool(edge_benchmark_watchdog_helper self_ipcs_benchmark_watchdog_helper)

if(BUILD_TESTING)
  find_package(Python3 QUIET COMPONENTS Interpreter)

  add_test(NAME platform_contract COMMAND edge_runtime_platform_probe)
  set_tests_properties(platform_contract PROPERTIES TIMEOUT 10)

  add_test(
    NAME self_ipcs_cli_canonical_parent_legacy_child
    COMMAND ${CMAKE_COMMAND} -DSUPERVISOR=$<TARGET_FILE:edge_shm_supervisor>
            -DPRODUCER=$<TARGET_FILE_DIR:edge_shm_producer>/edge_shm_producer
            -P ${CMAKE_SOURCE_DIR}/test/integration/cli_alias_test.cmake)
  add_test(
    NAME self_ipcs_cli_legacy_parent_canonical_child
    COMMAND ${CMAKE_COMMAND}
            -DSUPERVISOR=$<TARGET_FILE_DIR:edge_shm_supervisor>/edge_shm_supervisor
            -DPRODUCER=$<TARGET_FILE:edge_shm_producer>
            -P ${CMAKE_SOURCE_DIR}/test/integration/cli_alias_test.cmake)
  set_tests_properties(
    self_ipcs_cli_canonical_parent_legacy_child self_ipcs_cli_legacy_parent_canonical_child
    PROPERTIES TIMEOUT 15 LABELS cli WORKING_DIRECTORY /
    PASS_REGULAR_EXPRESSION "HELP_CHILD_EXECUTED")

  add_test(
    NAME self_ipcs_cli_legacy_default_children
    COMMAND $<TARGET_FILE_DIR:edge_crash_matrix>/edge_crash_matrix --only C17
            --out-dir ${CMAKE_BINARY_DIR}/metadata/tools/cli-compat/default-children)
  set_tests_properties(
    self_ipcs_cli_legacy_default_children PROPERTIES TIMEOUT 30 LABELS cli
    WORKING_DIRECTORY / PASS_REGULAR_EXPRESSION "CASE C17 PASS")

  add_test(
    NAME self_ipcs_cli_legacy_mpmc_forkexec
    COMMAND $<TARGET_FILE_DIR:edge_mpmc_bench>/edge_mpmc_bench
            --payload-size 64 --producers 2 --consumers 2 --messages 8 --capacity 4
            --timeout-ms 10000 --out-dir ${CMAKE_BINARY_DIR}/metadata/tools/cli-compat/mpmc)
  add_test(
    NAME self_ipcs_cli_legacy_buffer_pool_forkexec
    COMMAND $<TARGET_FILE_DIR:edge_buffer_pool_bench>/edge_buffer_pool_bench
            --block-size 4096 --block-count 2 --producers 2 --consumers 2 --messages 8
            --capacity 4 --timeout-ms 10000
            --out-dir ${CMAKE_BINARY_DIR}/metadata/tools/cli-compat/buffer-pool)
  set_tests_properties(
    self_ipcs_cli_legacy_mpmc_forkexec self_ipcs_cli_legacy_buffer_pool_forkexec
    PROPERTIES TIMEOUT 30 LABELS cli WORKING_DIRECTORY /
    PASS_REGULAR_EXPRESSION "correctness=PASS")

  add_test(
    NAME crash_matrix_smoke
    COMMAND edge_crash_matrix --smoke
            --producer $<TARGET_FILE:edge_shm_producer>
            --consumer $<TARGET_FILE:edge_shm_consumer>
            --ctl $<TARGET_FILE:edge_shm_ctl>
            --out-dir ${CMAKE_BINARY_DIR}/metadata/tools/crash)
  set_tests_properties(crash_matrix_smoke PROPERTIES TIMEOUT 240 LABELS crash)

  add_test(
    NAME crash_matrix
    COMMAND edge_crash_matrix
            --producer $<TARGET_FILE:edge_shm_producer>
            --consumer $<TARGET_FILE:edge_shm_consumer>
            --ctl $<TARGET_FILE:edge_shm_ctl>
            --out-dir ${CMAKE_BINARY_DIR}/metadata/tools/crash)
  set_tests_properties(
    crash_matrix PROPERTIES TIMEOUT 900 LABELS crash DISABLED TRUE)

  add_test(
    NAME bench_smoke
    COMMAND edge_shm_bench --smoke
            --producer $<TARGET_FILE:edge_shm_bench_producer>
            --consumer $<TARGET_FILE:edge_shm_bench_consumer>
            --sock $<TARGET_FILE:edge_shm_bench_sock>
            --ctl $<TARGET_FILE:edge_shm_ctl>
            --out-dir ${CMAKE_BINARY_DIR}/metadata/tools/performance)
  set_tests_properties(
    bench_smoke PROPERTIES TIMEOUT 300 LABELS benchmark)

  add_test(NAME mpmc_bench_smoke COMMAND edge_mpmc_bench --smoke)
  set_tests_properties(mpmc_bench_smoke PROPERTIES TIMEOUT 120 LABELS benchmark)

  add_test(
    NAME mpmc_bench_spin_smoke
    COMMAND edge_mpmc_bench --retry-policy spin --payload-size 64 --producers 2
            --consumers 2 --messages 8 --capacity 4 --timeout-ms 10000)
  set_tests_properties(mpmc_bench_spin_smoke PROPERTIES TIMEOUT 30 LABELS benchmark)

  add_test(
    NAME mpmc_wait_bench_smoke
    COMMAND edge_mpmc_bench --retry-policy yield --wait-policy blocking --payload-size 64
            --producers 2 --consumers 2 --messages 8 --capacity 4 --timeout-ms 10000)
  set_tests_properties(mpmc_wait_bench_smoke PROPERTIES TIMEOUT 30 LABELS benchmark)

  add_test(NAME buffer_pool_bench_smoke COMMAND edge_buffer_pool_bench --smoke)
  set_tests_properties(buffer_pool_bench_smoke PROPERTIES TIMEOUT 120 LABELS benchmark)

  if(Python3_Interpreter_FOUND)
    add_test(NAME software_notification_management_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_software_notification_management.py)
    set_tests_properties(software_notification_management_regression PROPERTIES
      TIMEOUT 15 LABELS integration ENVIRONMENT PYTHONDONTWRITEBYTECODE=1)

    add_test(
      NAME notification_matrix_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_notification_matrix.py
              --binary-dir $<TARGET_FILE_DIR:edge_shm_bench>
              --output-root ${CMAKE_BINARY_DIR})
    set_tests_properties(notification_matrix_regression PROPERTIES
      TIMEOUT 60 LABELS benchmark ENVIRONMENT PYTHONDONTWRITEBYTECODE=1)

    add_test(
      NAME benchmark_worker_deadline_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_bench_worker_deadline.py
              --driver $<TARGET_FILE:edge_shm_bench> --output-root ${CMAKE_BINARY_DIR})
    set_tests_properties(benchmark_worker_deadline_regression PROPERTIES
      TIMEOUT 40 LABELS benchmark ENVIRONMENT PYTHONDONTWRITEBYTECODE=1)

    add_test(
      NAME event_notification_bench_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_event_notification_bench.py
              --driver $<TARGET_FILE:edge_shm_bench>
              --producer $<TARGET_FILE:edge_shm_bench_producer>
              --consumer $<TARGET_FILE:edge_shm_bench_consumer>
              --output-root ${CMAKE_BINARY_DIR})
    set_tests_properties(event_notification_bench_regression PROPERTIES
      TIMEOUT 60 LABELS benchmark ENVIRONMENT PYTHONDONTWRITEBYTECODE=1)

    add_test(
      NAME formal_benchmark_metadata_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_formal_bench.py)
    set_tests_properties(
      formal_benchmark_metadata_regression PROPERTIES TIMEOUT 15 LABELS benchmark
      ENVIRONMENT PYTHONDONTWRITEBYTECODE=1)

    add_test(
      NAME benchmark_pressure_watchdog_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_pressure_watchdog.py
              --mpmc-binary $<TARGET_FILE:edge_mpmc_bench>
              --pool-binary $<TARGET_FILE:edge_buffer_pool_bench>
              --owner-binary $<TARGET_FILE:edge_benchmark_watchdog_helper>
              --output-root /tmp)
    set_tests_properties(
      benchmark_pressure_watchdog_regression PROPERTIES TIMEOUT 45 LABELS benchmark)

    add_test(
      NAME mpmc_payload_retry_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_mpmc_payload_retry.py
              --binary $<TARGET_FILE:edge_mpmc_bench> --output-root /tmp)
    set_tests_properties(mpmc_payload_retry_regression PROPERTIES TIMEOUT 45 LABELS benchmark)

    add_test(
      NAME benchmark_handshake_timing_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_handshake_timing.py
              --mpmc-binary $<TARGET_FILE:edge_mpmc_handshake_helper>
              --pool-binary $<TARGET_FILE:edge_buffer_pool_handshake_helper>
              --output-root /tmp)
    set_tests_properties(
      benchmark_handshake_timing_regression PROPERTIES TIMEOUT 45 LABELS benchmark)
  endif()
endif()
