# CMakeList.txt : CMake project for microsched, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project ("microsched")

# Add source to this project's executable.
add_executable (microsched "sched.cpp" "sched.h" "tests.cpp")

target_include_directories(microsched PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# TODO: Add tests and install targets if needed.
