cmake_minimum_required(VERSION 3.19)
project(rofl_db)

set(CMAKE_CXX_STANDARD 20)

include_directories(include)
file(GLOB SOURCES "src/*.cpp")
add_library(rofl_db ${SOURCES})

add_executable(test test.cpp)
target_link_libraries(test LINK_PUBLIC rofl_db)
