cmake_minimum_required(VERSION 3.5 FATAL_ERROR)
project(BDCI19
    LANGUAGES C CXX)

set(CMAKE_VERBOSE_MAKEFILE ON)

find_package(Threads REQUIRED)
link_libraries(Threads::Threads)

if (MAKE_FASTEST)
    add_definitions(-DMAKE_FASTEST=1)
endif()

add_compile_options(
    -flto -Ofast
    #-march=broadwell -mtune=broadwell
    -march=native -mtune=native
    -std=gnu++17 -Wall -Wextra -Winline)

link_libraries(
    -flto -Ofast
    #-march=broadwell -mtune=broadwell
    -march=native -mtune=native
    -static-libgcc -static-libstdc++)

add_executable(BDCI19
    sourcecode/create_index.cpp
    sourcecode/use_index.cpp
    sourcecode/main.cpp)
