# 测试按单元测试和真实跨进程集成测试分开构建。

find_package(Threads REQUIRED)
find_package(GTest REQUIRED)
include(GoogleTest)

add_subdirectory(unit)

add_subdirectory(integration)

add_executable(pool_liveness_race_test integration/pool_liveness_race_test.cpp)
edgeruntime_apply_project_options(pool_liveness_race_test)
target_link_libraries(pool_liveness_race_test PRIVATE edge_runtime GTest::gtest Threads::Threads rt)
target_include_directories(pool_liveness_race_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_options(pool_liveness_race_test PRIVATE
  "LINKER:--wrap=_ZN12edge_runtime6detail14probe_livenessEmm")
gtest_discover_tests(pool_liveness_race_test PROPERTIES TIMEOUT 12 LABELS integration)
