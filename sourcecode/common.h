#if !defined(_BDCI19_COMMON_H_INCLUDED_)
#define _BDCI19_COMMON_H_INCLUDED_

#include <atomic>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <condition_variable>
#include <fcntl.h>
#include <mutex>
#include <pthread.h>
#include <sched.h>
#include <semaphore.h>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <thread>
#include <tuple>
#include <type_traits>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

typedef __int128_t  int128_t;
typedef __uint128_t uint128_t;


#include "macros.h"

#include "date.h"
#include "spin_lock.h"
#include "done_event.h"
#include "sync_barrier.h"
#include "queue.h"
#include "mapper.h"
//#include "unloader.h"



//==============================================================================
// Global variables
//==============================================================================
inline int g_customer_fd, g_orders_fd, g_lineitem_fd;
inline uint64_t g_customer_file_size, g_orders_file_size, g_lineitem_file_size;

inline bool g_is_creating_index;
inline int g_index_directory_fd;

inline uint32_t g_query_count;
inline const char* const* g_argv_queries;

inline uint32_t g_loader_thread_count;
inline uint32_t g_worker_thread_count;
inline std::vector<std::thread> g_worker_threads;
inline std::vector<std::thread> g_loader_threads;
inline sync_barrier g_worker_sync_barrier;
inline sync_barrier g_loader_sync_barrier;



//==============================================================================
// Functions in create_index.cpp
//==============================================================================
void main_thread_create_index() noexcept;
void fn_worker_thread_create_index(const uint32_t tid) noexcept;
void create_index_initialize() noexcept;


//==============================================================================
// Functions in use_index.cpp / naive_use_index.cpp
//==============================================================================
void main_thread_use_index() noexcept;
void fn_worker_thread_use_index(const uint32_t tid) noexcept;
void use_index_initialize() noexcept;
void fn_loader_thread_use_index(const uint32_t tid) noexcept;


#endif  // !defined(_BDCI19_COMMON_H_INCLUDED_)
