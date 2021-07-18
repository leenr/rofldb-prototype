cmake_minimum_required(VERSION 3.19)
project(rofl_db)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS  "-Wno-unknown-pragmas")
#set(CMAKE_CXX_FLAGS_DEBUG "-DDEBUG -Wall -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -Wall -O3")

add_link_options(-static)

include_directories(include)
file(GLOB SOURCES "src/*.cpp")
file(GLOB LSM1_SOURCES "benchmark/sqlite/ext/lsm1/*.c")

add_library(lsm1 ${LSM1_SOURCES})
add_library(lmdb benchmark/lmdb/libraries/liblmdb/mdb.c benchmark/lmdb/libraries/liblmdb/midl.c)
add_library(rofl_db ${SOURCES})

add_executable(test test.cpp)
add_executable(benchmark benchmark/benchmark.cpp)

target_link_libraries(test LINK_PUBLIC rofl_db)
target_link_libraries(lmdb LINK_PUBLIC pthread)
target_link_libraries(benchmark LINK_PUBLIC rofl_db lsm1 lmdb)
