cmake_minimum_required(VERSION 3.22)
project(selfipcs_notification_demo VERSION 0.1.0 LANGUAGES CXX)

option(SELFIPCS_DEMO_USE_LEGACY "Use the legacy EdgeRuntime package and headers" OFF)
option(SELFIPCS_DEMO_ENABLE_SANITIZERS "Consume an ASan/UBSan-instrumented installation" OFF)

if(SELFIPCS_DEMO_USE_LEGACY)
  find_package(EdgeRuntime CONFIG REQUIRED)
  set(_notification_demo_target EdgeRuntime::edge_runtime)
else()
  find_package(SelfIPCs CONFIG REQUIRED)
  set(_notification_demo_target SelfIPCs::self_ipcs)
endif()

add_executable(notification_demo src/main.cpp)
target_include_directories(notification_demo PRIVATE include)
target_compile_features(notification_demo PRIVATE cxx_std_17)
set_target_properties(notification_demo PROPERTIES CXX_EXTENSIONS OFF)
target_link_libraries(notification_demo PRIVATE ${_notification_demo_target})
if(SELFIPCS_DEMO_USE_LEGACY)
  target_compile_definitions(notification_demo PRIVATE SELFIPCS_DEMO_USE_LEGACY=1)
endif()
target_compile_options(notification_demo PRIVATE
  -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
if(SELFIPCS_DEMO_ENABLE_SANITIZERS)
  target_compile_options(notification_demo PRIVATE
    -fno-omit-frame-pointer -fsanitize=address,undefined)
  target_link_options(notification_demo PRIVATE
    -fno-omit-frame-pointer -fsanitize=address,undefined)
endif()

enable_testing()
add_test(NAME installed_notification_demo COMMAND notification_demo)
set_tests_properties(installed_notification_demo PROPERTIES TIMEOUT 15)
