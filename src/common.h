#if !defined(_BDCI19_COMMON_H_INCLUDED_)
#define _BDCI19_COMMON_H_INCLUDED_


//==============================================================================
// Standard C++ / System Headers
//==============================================================================
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <chrono>
#include <cstdint>
#include <cinttypes>
#include <climits>
#include <functional>
#include <linux/futex.h>
#include <mutex>
#include <pthread.h>
#include <random>
//#include <semaphore.h>  // use custom semaphore
#include <shared_mutex>
#include <sys/sysctl.h>
#include <sys/syscall.h>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/resource.h>
#include <sys/shm.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <mmintrin.h>
#include <immintrin.h>


typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

typedef uint32_t index32_t;
typedef uint64_t index64_t;

#if !defined(PAGE_SIZE)
#define PAGE_SIZE   (4096)
#endif


#include "config.h"
#include "macros.h"

#include "str.h"
#include "fs.h"
#include "futex.h"
#include "sem.h"
#include "mm.h"
#include "mapper.h"
#include "spin_lock.h"
#include "sync_barrier.h"
#include "date.h"
#include "queue.h"


//==============================================================================
// Structures
//==============================================================================
template<typename T>
struct posix_shm_t
{
public:
    T* ptr = nullptr;
    uint64_t size_in_byte = 0;
    int shmid = -1;

public:
    __always_inline
    bool init_fixed(const key_t shmkey, uint64_t size, bool try_create) noexcept
    {
        ASSERT(shmid < 0);
        ASSERT(ptr == nullptr);
        ASSERT(size_in_byte == 0);

        shmid = shmget(
            shmkey,
            size,
            0666 | SHM_HUGETLB | SHM_HUGE_2MB | (try_create ? IPC_CREAT : 0));
        if (shmid < 0) {
            WARN("shmget(shmkey=%d) failed. errno = %d (%s)", shmkey, errno, strerror(errno));
            return false;
        }
        INFO("shmkey %d -> shmid %d", shmkey, shmid);

        size_in_byte = size;
        ptr = (T*)mmap_reserve_space(size);
        return true;
    }

    __always_inline
    void attach_fixed(const bool do_remove) noexcept
    {
        ASSERT(ptr != nullptr);
        ASSERT(shmid >= 0);
        void* const tmp_ptr = shmat(shmid, ptr, 0);
        CHECK(tmp_ptr != (void*)-1, "shmat(shmid=0x%x) failed. errno = %d (%s)", shmid, errno, strerror(errno));

        if (do_remove) {
            C_CALL(shmctl(shmid, IPC_RMID, nullptr));
        }

        DEBUG("shmat(shmid=0x%x): %p", shmid, tmp_ptr);
        ASSERT(ptr == tmp_ptr);
    }

    __always_inline
    void detach() noexcept
    {
        ASSERT(ptr != nullptr);
        C_CALL(shmdt(ptr));

        DEBUG("shmdt(shmid=0x%x): %p", shmid, ptr);
        ptr = nullptr;
    }
};

//==============================================================================
// Global Constants
//==============================================================================
#define ACTION_DROP_PAGE_CACHE      "drop_page_cache"

#define SHMKEY_TXT_CUSTOMER         ((key_t)0x19491001)
#define SHMKEY_TXT_ORDERS           ((key_t)0x19491002)
#define SHMKEY_TXT_LINEITEM         ((key_t)0x19491003)
#define SHMKEY_CUSTKEY_TO_MKTID     ((key_t)0x19491004)
#define SHMKEY_ORDERKEY_TO_ORDER    ((key_t)0x19491005)
#define SHMKEY_ORDERKEY_TO_CUSTKEY  ((key_t)0x19491006)

//==============================================================================
// Global Variables
//==============================================================================
struct shared_information_t {
public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(shared_information_t);
    shared_information_t() noexcept = default;

public:
    std::atomic_uint64_t customer_file_shared_offset { 0 };
    std::atomic_uint64_t orders_file_shared_offset { 0 };
    std::atomic_uint64_t lineitem_file_shared_offset { 0 };

    std::atomic_uint64_t orderkey_custkey_shared_counter { 0 };

    sync_barrier loader_sync_barrier { };
    sync_barrier worker_sync_barrier { };

    volatile uint8_t mktid_count = 0;
    struct {
        uint8_t length;
        char name[12];
    } all_mktsegments[8] { };  // only used in create_index
    pthread_mutex<process_shared> all_mktsegments_insert_mutex { };  // only used in create_index

    uint32_t total_buckets = 0;
    uint32_t buckets_per_holder = 0;

    std::atomic_uint64_t next_truncate_holder_major_id { 0 };
    std::atomic_uint64_t next_truncate_holder_minor_id { 0 };

    uint32_t total_plates = 0;
    std::atomic_uint32_t pretopn_plate_id_shared_counter { 0 };

    pthread_mutex<process_shared> meta_update_mutex { };
    struct {
        uint32_t max_shipdate_orderdate_diff = 0;
        uint64_t max_bucket_size_major = 0;
        uint64_t max_bucket_size_minor = 0;
    } meta { };

    bool sched_fifo_failed { false };
#if ENABLE_ASSERTION
    std::atomic_uint64_t customer_file_loaded_parts { 0 };
#endif
};

inline shared_information_t* g_shared = nullptr;
inline bool g_use_multi_process = false;
inline uint32_t g_active_cpu_cores = 0;  // number of CPU cores
inline uint32_t g_total_process_count = 0;  // process or thread count
inline uint32_t g_id = 0;

#if ENABLE_SHM_CACHE_TXT
inline posix_shm_t<char> g_customer_shm { };
inline posix_shm_t<char> g_orders_shm { };
inline posix_shm_t<char> g_lineitem_shm { };
#endif

inline load_file_context g_customer_file { };
inline load_file_context g_orders_file { };
inline load_file_context g_lineitem_file { };

inline int g_index_directory_fd = -1;
inline bool g_is_creating_index = false;
inline bool g_is_preparing_page_cache = true;

inline uint32_t g_query_count = 0;
inline const char* const* g_argv_queries = nullptr;

constexpr const uint32_t BUCKETS_PER_MKTID = __div_up((MAX_TABLE_DATE - MIN_TABLE_DATE + 1), CONFIG_ORDERDATES_PER_BUCKET);

inline load_file_context g_endoffset_file_major { };
inline load_file_context g_endoffset_file_minor { };
inline int g_holder_files_major_fd[CONFIG_INDEX_HOLDER_COUNT] { };
inline int g_holder_files_minor_fd[CONFIG_INDEX_HOLDER_COUNT] { };


static_assert(CONFIG_TOPN_DATES_PER_PLATE % CONFIG_ORDERDATES_PER_BUCKET == 0);
constexpr const uint32_t BUCKETS_PER_PLATE = CONFIG_TOPN_DATES_PER_PLATE / CONFIG_ORDERDATES_PER_BUCKET;
constexpr const uint32_t PLATES_PER_MKTID = __div_up(BUCKETS_PER_MKTID, BUCKETS_PER_PLATE);

inline load_file_context g_pretopn_file { };
inline load_file_context g_pretopn_count_file { };

inline uint64_t* g_pretopn_start_ptr = nullptr;  // [g_shared->total_plates][CONFIG_EXPECT_MAX_TOPN]
inline uint32_t* g_pretopn_count_start_ptr = nullptr;  // [g_shared->total_plates]


//==============================================================================
// Functions in create_index.cpp
//==============================================================================
void fn_loader_thread_create_index(const uint32_t tid) noexcept;
void fn_worker_thread_create_index(const uint32_t tid) noexcept;
void fn_unloader_thread_create_index() noexcept;
void create_index_initialize_before_fork() noexcept;
void create_index_initialize_after_fork() noexcept;


//==============================================================================
// Functions in use_index.cpp
//==============================================================================
void fn_loader_thread_use_index(const uint32_t tid) noexcept;
void fn_worker_thread_use_index(const uint32_t tid) noexcept;
void fn_unloader_thread_use_index() noexcept;
void use_index_initialize_before_fork() noexcept;
void use_index_initialize_after_fork() noexcept;


//==============================================================================
// Very common routines
//==============================================================================
__always_inline
uint32_t calc_bucket_index(const uint8_t mktid, const date_t orderdate) noexcept
{
    ASSERT(g_shared->mktid_count > 0);
    ASSERT(mktid < g_shared->mktid_count);
    ASSERT(orderdate >= MIN_TABLE_DATE);
    ASSERT(orderdate <= MAX_TABLE_DATE);

    return (uint32_t)(mktid - 0) * BUCKETS_PER_MKTID + (orderdate - MIN_TABLE_DATE) / CONFIG_ORDERDATES_PER_BUCKET;
}

__always_inline
date_t calc_bucket_base_orderdate(const date_t orderdate) noexcept
{
    return __align_down((orderdate - MIN_TABLE_DATE), CONFIG_ORDERDATES_PER_BUCKET) + MIN_TABLE_DATE;
}

__always_inline
uint32_t calc_bucket_mktid(const uint32_t bucket_id) noexcept
{
    return bucket_id / BUCKETS_PER_MKTID + 0;
}

__always_inline
date_t calc_bucket_base_orderdate_by_bucket_id(const uint32_t bucket_id) noexcept
{
    return (bucket_id % BUCKETS_PER_MKTID) * CONFIG_ORDERDATES_PER_BUCKET + MIN_TABLE_DATE;
}

__always_inline
uint32_t calc_plate_id(const uint32_t bucket_id) noexcept
{
    const uint32_t mktid = bucket_id / BUCKETS_PER_MKTID;
    const uint32_t bucket_id_in_mkt = bucket_id % BUCKETS_PER_MKTID;
    return mktid * PLATES_PER_MKTID + (bucket_id_in_mkt / BUCKETS_PER_PLATE);
}

__always_inline
date_t calc_plate_base_orderdate_by_plate_id(const uint32_t plate_id) noexcept
{
    const uint32_t plate_id_in_mkt = plate_id % PLATES_PER_MKTID;
    return plate_id_in_mkt * CONFIG_TOPN_DATES_PER_PLATE + MIN_TABLE_DATE;
}

__always_inline
date_t calc_plate_base_bucket_id_by_plate_id(const uint32_t plate_id) noexcept
{
    const uint32_t mktid = plate_id / PLATES_PER_MKTID;
    const uint32_t plate_id_in_mkt = plate_id % PLATES_PER_MKTID;

    return (uint32_t)(mktid - 0) * BUCKETS_PER_MKTID + plate_id_in_mkt * BUCKETS_PER_PLATE;
}

__always_inline
void pin_thread_to_cpu_core(const uint32_t core) noexcept
{
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(core, &cpu_set);
    PTHREAD_CALL_NO_PANIC(pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set));
}

__always_inline
void set_thread_fifo_scheduler(const uint32_t nice_from_max_priority) noexcept
{
    static const uint32_t __max_priority = C_CALL(sched_get_priority_max(SCHED_FIFO));
    ASSERT(nice_from_max_priority < __max_priority);

    ASSERT(g_shared != nullptr);
    if (g_shared->sched_fifo_failed) {
        return;
    }

    sched_param param { };
    param.sched_priority = (int)(__max_priority - nice_from_max_priority);
    const int err = PTHREAD_CALL_NO_PANIC(pthread_setschedparam(pthread_self(), SCHED_FIFO, &param));
    if (err != 0) {
        g_shared->sched_fifo_failed = true;
    }
}


#endif  // !defined(_BDCI19_COMMON_H_INCLUDED_)
