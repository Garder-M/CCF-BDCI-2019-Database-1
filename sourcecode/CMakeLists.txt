cmake_minimum_required(VERSION 3.9.0 FATAL_ERROR)
project(BDCI19
    LANGUAGES C CXX)

set(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

if (MAKE_FASTEST)
    message(STATUS "MAKE_FASTEST is ON")
    add_definitions(-DMAKE_FASTEST)
endif()

add_compile_options(
    -flto -fopenmp -Ofast
    -Wall -Wextra -Wno-unused-parameter)

link_libraries(
    -flto -fopenmp -Ofast
    -pthread #[[-Wl,--whole-archive -lpthread -Wl,--no-whole-archive -static]]
    -static-libgcc -static-libstdc++)

add_executable(BDCI19
    main.cpp create_index.cpp #[[use_index.cpp]] use_index.cpp)
    #main.cpp create_index.cpp #[[use_index.cpp]] naive_use_index.cpp)
