cmake_minimum_required(VERSION 3.9.0 FATAL_ERROR)

set(CMAKE_CXX_FLAGS_RELEASE "")  # default: -O3 -DNDEBUG
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g")  # default: -O2 -g -DNDEBUG

project(BDCI19
    LANGUAGES C CXX
    VERSION 0.2.0)


set(CMAKE_VERBOSE_MAKEFILE ON)


file(GLOB_RECURSE CPP_SRC_FILES
    CONFIGURE_DEPENDS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    src/*.cpp)
message(STATUS "CPP_SRC_FILES: ${CPP_SRC_FILES}")


set(CPP_FLAGS "")
string(APPEND CPP_FLAGS " -std=gnu++17 -Ofast")
string(APPEND CPP_FLAGS " -flto -mtune=broadwell -march=broadwell")
string(APPEND CPP_FLAGS " -funsafe-loop-optimizations -fstack-protector-explicit -funroll-loops")
string(APPEND CPP_FLAGS " -Wunsafe-loop-optimizations -Wall -Wextra -Winline -Wformat=2")
set_source_files_properties(${CPP_SRC_FILES} PROPERTIES COMPILE_FLAGS "${CPP_FLAGS}")


find_package(Threads REQUIRED)
link_libraries(
    -static-libgcc -static-libstdc++
    Threads::Threads)


#
# Executable: BDCI19
#
add_executable(BDCI19
    src/create_index.cpp
    src/use_index.cpp
    src/main.cpp)

if (MAKE_FASTEST)
    target_compile_definitions(BDCI19 PUBLIC MAKE_FASTEST=1)
endif()


#
# Executable: debug_preload
#
add_executable(debug_preload EXCLUDE_FROM_ALL
    src/debug/debug_preload.cpp)

#
# Executable: debug_sysv_perf
#
add_executable(debug_sysv_perf EXCLUDE_FROM_ALL
    src/debug/debug_sysv_perf.cpp)
