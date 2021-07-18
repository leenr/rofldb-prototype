cmake_minimum_required(VERSION 3.19)
project(rofl_db)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS  "-Wno-unknown-pragmas")
#set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG -Wall -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -Wall -O3")

#add_link_options(-static)

include_directories(include)
include_directories(benchmark/sqlite/ext/lsm1)
file(GLOB SOURCES "src/*.cpp")
file(GLOB LSM_SOURCES "benchmark/sqlite/ext/lsm1/*.c")

add_library(rofl_db ${SOURCES})
add_executable(test test.cpp)
add_executable(benchmark benchmark/benchmark.cpp ${LSM_SOURCES})
target_link_libraries(test LINK_PUBLIC rofl_db)
target_link_libraries(benchmark LINK_PUBLIC rofl_db)
