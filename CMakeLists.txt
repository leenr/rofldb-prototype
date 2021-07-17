cmake_minimum_required(VERSION 3.19)
project(rofl_db)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS  "-Wno-unknown-pragmas")
set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG -Wall -Og")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -Wall -O3")

include_directories(include)
file(GLOB SOURCES "src/*.cpp")

add_library(rofl_db ${SOURCES})
add_executable(test test.cpp)
target_link_libraries(test LINK_PUBLIC rofl_db)
