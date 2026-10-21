find_package(Boost 1.74 REQUIRED COMPONENTS system)

add_library(self_ipcs_async STATIC src/consumer.cpp)
add_library(SelfIPCs::async ALIAS self_ipcs_async)
set_target_properties(self_ipcs_async PROPERTIES EXPORT_NAME async CXX_EXTENSIONS OFF)
target_compile_features(self_ipcs_async PUBLIC cxx_std_20)
target_include_directories(self_ipcs_async PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(self_ipcs_async PUBLIC SelfIPCs::self_ipcs Boost::system)
selfipcs_apply_project_options(self_ipcs_async)

install(TARGETS self_ipcs_async EXPORT SelfIPCsAsyncTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  FILES_MATCHING PATTERN "*.hpp")
install(EXPORT SelfIPCsAsyncTargets FILE SelfIPCsAsyncTargets.cmake
  NAMESPACE SelfIPCs:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/SelfIPCs)

if(BUILD_TESTING)
  add_subdirectory(${PROJECT_SOURCE_DIR}/examples/async_consume_demo
    ${CMAKE_CURRENT_BINARY_DIR}/consume_demo)
endif()
