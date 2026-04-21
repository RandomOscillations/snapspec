cmake_minimum_required(VERSION 3.16)
project(snapspec_blockstore VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
option(SNAPSPEC_BUILD_TESTS "Build C++ tests" ON)

# Block store library
add_library(blockstore
    src/blockstore/row.cpp
    src/blockstore/cow.cpp
    src/blockstore/fullcopy.cpp
)
target_include_directories(blockstore PUBLIC src/)
set_target_properties(blockstore PROPERTIES POSITION_INDEPENDENT_CODE ON)

# pybind11
find_package(pybind11 QUIET)
if(pybind11_FOUND)
    pybind11_add_module(_blockstore src/bindings.cpp)
    target_link_libraries(_blockstore PRIVATE blockstore)
else()
    message(WARNING "pybind11 not found — Python bindings will not be built")
endif()

if(SNAPSPEC_BUILD_TESTS)
    # Google Test
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0
    )
    FetchContent_MakeAvailable(googletest)

    enable_testing()

    add_executable(blockstore_tests
        src/tests/row_test.cpp
        src/tests/cow_test.cpp
        src/tests/fullcopy_test.cpp
    )
    target_link_libraries(blockstore_tests PRIVATE blockstore GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(blockstore_tests)
endif()
