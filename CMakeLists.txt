# SelfIPCs 提供 Linux/C++17 最新值通道、有界 MPMC 队列和共享缓冲池描述符链。
cmake_minimum_required(VERSION 3.22)

project(
  SelfIPCs
  VERSION 0.5.1
  DESCRIPTION "Linux C++17 IPC: latest-value channels, bounded MPMC queues and shared buffer pools"
  LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

find_package(Threads REQUIRED)

function(selfipcs_option suffix description default_value)
  set(current SELFIPCS_${suffix})
  set(legacy EDGERUNTIME_${suffix})
  set(previous _SELFIPCS_${suffix}_RESOLVED)
  foreach(name IN ITEMS ${current} ${legacy} ${previous})
    if(DEFINED ${name})
      if(${name})
        set(${name}_normalized ON)
      else()
        set(${name}_normalized OFF)
      endif()
    endif()
  endforeach()
  set(value ${default_value})
  if(DEFINED ${current} AND DEFINED ${legacy})
    if(NOT ${current}_normalized STREQUAL ${legacy}_normalized)
      if(DEFINED ${previous} AND ${current}_normalized STREQUAL ${previous}_normalized)
        set(value ${${legacy}_normalized})
      elseif(DEFINED ${previous} AND ${legacy}_normalized STREQUAL ${previous}_normalized)
        set(value ${${current}_normalized})
      else()
        message(FATAL_ERROR "Conflicting options: ${current} and ${legacy}")
      endif()
    else()
      set(value ${${current}_normalized})
    endif()
  elseif(DEFINED ${current})
    set(value ${${current}_normalized})
  elseif(DEFINED ${legacy})
    set(value ${${legacy}_normalized})
  endif()
  set(${current} ${value} CACHE BOOL "${description}" FORCE)
  set(${legacy} ${value} CACHE BOOL "Compatibility alias for ${current}" FORCE)
  set(${previous} ${value} CACHE INTERNAL "Last resolved ${current}" FORCE)
endfunction()

selfipcs_option(WARNINGS_AS_ERRORS "Treat compiler warnings as errors" ON)
selfipcs_option(ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
selfipcs_option(BUILD_ASYNC "Build the optional C++20 Boost.Asio consumer component" OFF)

set(_selfipcs_failpoints_default OFF)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(_selfipcs_failpoints_default ON)
endif()
selfipcs_option(ENABLE_FAILPOINTS "Enable compile-time failpoints" ${_selfipcs_failpoints_default})
unset(_selfipcs_failpoints_default)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  set(SELFIPCS_PRIVATE_COMPILE_OPTIONS -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
  if(SELFIPCS_WARNINGS_AS_ERRORS)
    list(APPEND SELFIPCS_PRIVATE_COMPILE_OPTIONS -Werror)
  endif()
else()
  message(FATAL_ERROR "SelfIPCs currently supports GCC or Clang on Linux only")
endif()

function(selfipcs_apply_project_options target)
  target_compile_options(${target} PRIVATE ${SELFIPCS_PRIVATE_COMPILE_OPTIONS})
  if(SELFIPCS_ENABLE_SANITIZERS)
    target_compile_options(
      ${target} PRIVATE -fno-omit-frame-pointer -fsanitize=address,undefined)
    target_link_options(
      ${target} PRIVATE -fno-omit-frame-pointer -fsanitize=address,undefined)
  endif()
  if(SELFIPCS_ENABLE_FAILPOINTS)
    target_compile_definitions(${target} PRIVATE EDGERUNTIME_ENABLE_FAILPOINTS=1)
  endif()
endfunction()

function(edgeruntime_apply_project_options target)
  selfipcs_apply_project_options(${target})
endfunction()

add_subdirectory(src)
if(SELFIPCS_BUILD_ASYNC)
  add_subdirectory(async)
endif()
add_subdirectory(tools)
if(BUILD_TESTING)
  add_subdirectory(test)
endif()

install(
  TARGETS self_ipcs
  EXPORT SelfIPCsTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(
  DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  FILES_MATCHING
    PATTERN "*.h"
    PATTERN "*.hpp"
    PATTERN "detail" EXCLUDE)
install(
  EXPORT SelfIPCsTargets
  FILE SelfIPCsTargets.cmake
  NAMESPACE SelfIPCs::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/SelfIPCs)
install(FILES kernel/notification/include/uapi/self_ipcs_notification.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/self_ipcs/notification
  RENAME driver_uapi.h)

configure_package_config_file(
  cmake/SelfIPCsConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/SelfIPCsConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/SelfIPCs)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/SelfIPCsConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/SelfIPCsConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/SelfIPCsConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/SelfIPCs)

configure_package_config_file(
  cmake/EdgeRuntimeConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/EdgeRuntimeConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/EdgeRuntime)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/EdgeRuntimeConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

install(
  FILES
    ${CMAKE_CURRENT_BINARY_DIR}/EdgeRuntimeConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/EdgeRuntimeConfigVersion.cmake
    cmake/EdgeRuntimeTargets.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/EdgeRuntime)
