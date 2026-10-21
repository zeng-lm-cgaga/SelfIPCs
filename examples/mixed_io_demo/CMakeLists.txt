cmake_minimum_required(VERSION 3.22)
project(selfipcs_mixed_io_demo VERSION 0.1.0 LANGUAGES CXX)

option(SELFIPCS_DEMO_USE_LEGACY "Use the legacy EdgeRuntime package and headers" OFF)

if(SELFIPCS_DEMO_USE_LEGACY)
  find_package(EdgeRuntime CONFIG REQUIRED)
  set(_mixed_io_target EdgeRuntime::edge_runtime)
else()
  if(NOT TARGET SelfIPCs::self_ipcs)
    find_package(SelfIPCs CONFIG REQUIRED)
  endif()
  set(_mixed_io_target SelfIPCs::self_ipcs)
endif()

add_executable(mixed_io_demo
  src/main.cpp
  src/support.cpp
  src/producer.cpp
  src/consumer.cpp)
target_include_directories(mixed_io_demo PRIVATE include)
target_compile_features(mixed_io_demo PRIVATE cxx_std_17)
set_target_properties(mixed_io_demo PROPERTIES CXX_EXTENSIONS OFF)
target_compile_options(mixed_io_demo PRIVATE
  -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
target_link_libraries(mixed_io_demo PRIVATE ${_mixed_io_target})
if(SELFIPCS_DEMO_USE_LEGACY)
  target_compile_definitions(mixed_io_demo PRIVATE SELFIPCS_DEMO_USE_LEGACY=1)
endif()
