#if !defined(_BDCI19_COMMON_H_INCLUDED_)
#define _BDCI19_COMMON_H_INCLUDED_

//==============================================================================
// Standard C++ / System Headers
//==============================================================================
#include <atomic>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <chrono>
#include <cstdint>
#include <cinttypes>
#include <climits>
#include <vector>
#include <algorithm>
#include <functional>
#include <sys/sysctl.h>
#include <sys/syscall.h>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <unistd.h>
#include <thread>
#include <pthread.h>
#include <mutex>
#include <shared_mutex>


//==============================================================================
// My Headers
//==============================================================================
#include "config.h"

#include "macros.h"
#include "spin_lock.h"
#include "date.h"
#include "sync_barrier.h"
#include "queue.h"
#include "mapper.h"
#include "done_event.h"


//==============================================================================
// Structures
//==============================================================================
struct load_file_context
{
    int fd = -1;
    uint64_t file_size = 0;
    std::atomic_uint64_t shared_offset { 0 };
};


//==============================================================================
// Global Variables
//==============================================================================
inline int g_index_directory_fd = -1;

inline load_file_context g_customer_file;
inline load_file_context g_orders_file;
inline load_file_context g_lineitem_file;

inline std::vector<std::thread> g_worker_threads;
inline std::vector<std::thread> g_loader_threads;
inline sync_barrier g_worker_sync_barrier;
inline sync_barrier g_loader_sync_barrier;

inline std::vector<std::thread> g_loader_threads;
inline sync_barrier g_loader_sync_barrier;

inline uint32_t g_query_count = 0;
inline const char* const* g_argv_queries = nullptr;

[[maybe_unused]] inline struct {
    uint32_t partial_index_count;
    uint32_t max_shipdate_orderdate_diff;
    uint32_t max_bucket_actual_size;
    uint32_t max_bucket_actual_size_up_aligned;
} g_meta;


//==============================================================================
// Functions in create_index.cpp
//==============================================================================
void fn_loader_thread_create_index(const uint32_t tid) noexcept;
void fn_worker_thread_create_index(const uint32_t tid) noexcept;
void create_index_initialize() noexcept;


//==============================================================================
// Functions in use_index.cpp
//==============================================================================
void fn_loader_thread_use_index(const uint32_t tid) noexcept;
void fn_worker_thread_use_index(const uint32_t tid) noexcept;
void use_index_initialize() noexcept;
void fn_loader_thread_use_index(const uint32_t tid) noexcept;


//==============================================================================
// Very common routines
//==============================================================================
FORCEINLINE void open_file_read(
    /*in*/ const char *const path,
    /*out*/ load_file_context *const ctx) noexcept
{
    ASSERT(ctx->fd == -1, "fd should be initialized to -1 to prevent bugs");
    ctx->fd = C_CALL(open(path, O_RDONLY | O_CLOEXEC));

    struct stat64 st;
    C_CALL(fstat64(ctx->fd, &st));
    ctx->file_size = st.st_size;

    ctx->shared_offset = 0;  // Clear the shared offset

    DEBUG("open_file_read() %s: fd = %d, size = %lu", path, ctx->fd, ctx->file_size);
}

FORCEINLINE void openat_file_read(
    /*in*/ const char *const path,
    /*out*/ int *const fd,
    /*out*/ uint64_t *const file_size) noexcept
{
    ASSERT(g_index_directory_fd >= 0);
    ASSERT(*fd == -1, "fd should be initialized to -1 to prevent bugs");
    *fd = C_CALL(openat(g_index_directory_fd, path, O_RDONLY | O_CLOEXEC));

    struct stat64 st;
    C_CALL(fstat64(*fd, &st));
    *file_size = st.st_size;

    DEBUG("openat_file_read() %s: fd = %d, size = %lu", path, *fd, *file_size);
}

FORCEINLINE void openat_file_write(
    /*in*/ const char *const path,
    /*out*/ int *const fd,
    /*in*/ const uint64_t file_size,
    /*in*/ const bool allocate_disk) noexcept
{
    ASSERT(g_index_directory_fd >= 0);
    *fd = C_CALL(openat(
        g_index_directory_fd,
        path,
        O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
        0666));

    if (allocate_disk) {
        C_CALL(fallocate64(*fd, /*mode*/0, /*offset*/0, /*len*/file_size));
    }
    else {
        C_CALL(ftruncate64(*fd, file_size));
    }

    DEBUG("openat_file_write() %s: fd = %d, size = %lu", path, *fd, file_size);
}

FORCEINLINE void pin_thread_to_cpu_core(const uint32_t core) noexcept
{
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(core, &cpu_set);
    PTHREAD_CALL_NO_PANIC(pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set));
}

FORCEINLINE void set_thread_fifo_scheduler(const uint32_t nice_from_max_priority) noexcept
{
    static const uint32_t __max_priority = C_CALL(sched_get_priority_max(SCHED_FIFO));
    ASSERT(nice_from_max_priority < __max_priority);

    sched_param param { };
    param.sched_priority = (int)(__max_priority - nice_from_max_priority);
    PTHREAD_CALL_NO_PANIC(pthread_setschedparam(pthread_self(), SCHED_FIFO, &param));
}

#endif  // !defined(_BDCI19_COMMON_H_INCLUDED_)
