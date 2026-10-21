cmake_minimum_required(VERSION 3.22)
project(selfipcs_queue_pool_demo VERSION 0.1.0 LANGUAGES CXX)

option(SELFIPCS_DEMO_USE_LEGACY "Use the legacy EdgeRuntime installed package" OFF)

if(SELFIPCS_DEMO_USE_LEGACY)
  find_package(EdgeRuntime CONFIG REQUIRED)
  set(_queue_pool_demo_target EdgeRuntime::edge_runtime)
else()
  find_package(SelfIPCs CONFIG REQUIRED)
  set(_queue_pool_demo_target SelfIPCs::self_ipcs)
endif()

add_executable(queue_pool_demo
  src/main.cpp
  src/pool_flow.cpp
  src/process.cpp
  src/queue_flow.cpp)
target_include_directories(queue_pool_demo PRIVATE include)
target_compile_features(queue_pool_demo PRIVATE cxx_std_17)
set_target_properties(queue_pool_demo PROPERTIES CXX_EXTENSIONS OFF)
target_link_libraries(queue_pool_demo PRIVATE ${_queue_pool_demo_target})
if(SELFIPCS_DEMO_USE_LEGACY)
  target_compile_definitions(queue_pool_demo PRIVATE SELFIPCS_DEMO_USE_LEGACY=1)
endif()
target_compile_options(queue_pool_demo PRIVATE
  -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
