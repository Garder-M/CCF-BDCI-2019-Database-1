#if !defined(_BDCI19_CONFIG_H_INCLUDED_)
#define _BDCI19_CONFIG_H_INCLUDED_

#if !defined(MAKE_FASTEST)
//#define MAKE_FASTEST
#endif

#if defined(MAKE_FASTEST)
#define ENABLE_ASSERTION                    0
#define ENABLE_LOGGING_TRACE                0
#define ENABLE_LOGGING_DEBUG                0
#define ENABLE_LOGGING_INFO                 0
#else  // !defined(MAKE_FASTEST)
#define ENABLE_ASSERTION                    1
#define ENABLE_LOGGING_TRACE                1
#define ENABLE_LOGGING_DEBUG                1
#define ENABLE_LOGGING_INFO                 1
#endif

#define ENABLE_CPU_HANDLE_MINOR_WORKLOAD    1

// Do we use spin_lock in bounded_bag<T> and mpmc_queue<T>?
//  0 - Use mutex
//  1 - Use spin_lock
#define ENABLE_QUEUE_USE_SPIN_LOCK          0


// Do we pin worker and loader threads to its corresponding CPU core?
//  0 - Don't pin
//  1 - Pin
#define ENABLE_PIN_THREAD_TO_CPU            1


// Do we try to use SCHED_FIFO for better performance?
//  1 - Try to set scheduler to SCHED_FIFO (fall back to not changed (SCHED_OTHER) if failed)
//  0 - Don't try to set scheduler to SCHED_FIFO
#define ENABLE_ATTEMPT_SCHED_FIFO           0
#if ENABLE_ATTEMPT_SCHED_FIFO
#define CONFIG_SCHED_FIFO_LOADER_NICE       (0)
#define CONFIG_SCHED_FIFO_WORKER_NICE       (1)
#define CONFIG_SCHED_FIFO_PWRITE_NICE       (CONFIG_SCHED_FIFO_WORKER_NICE)
#define CONFIG_SCHED_FIFO_UNLOADER_NICE     (CONFIG_SCHED_FIFO_LOADER_NICE)
#endif


// Number of 2MB-hugepages used by program
#define CONFIG_EXTRA_HUGE_PAGES             (2560)  // 5120 MB


// How many byte per mmap() when loading original text files?
// Note, to deal with unaligned line-breaks, we have to overlap a little between each call
#define CONFIG_PART_OVERLAPPED_SIZE         (4096U)

#define CONFIG_CUSTOMER_PART_BODY_SIZE      (1048576U * 8 - CONFIG_PART_OVERLAPPED_SIZE)
#define CONFIG_ORDERS_PART_BODY_SIZE        (1048576U * 16 - CONFIG_PART_OVERLAPPED_SIZE)
#define CONFIG_LINEITEM_PART_BODY_SIZE      (1048576U * 16 - CONFIG_PART_OVERLAPPED_SIZE)

// Number of buffers when we load the three text files
#define CONFIG_LOAD_TXT_BUFFER_COUNT        (16)


// How many orderdates are saved in a same bucket?
//  Must be one of: 1, 2, 4
#define CONFIG_ORDERDATES_PER_BUCKET        (4)
static_assert(CONFIG_ORDERDATES_PER_BUCKET == 4);  // TODO: adjust CONFIG_INDEX_TLS_BUFFER_SIZE_XXX if < 4

// How many index files do we use?
// This is to accelerate buffered I/O (reduce lock contention in vfs_read)
#define CONFIG_INDEX_HOLDER_COUNT           (32)

// Do we use mid-level index?
// This largely increase create_index time, also largely decrease use_index time
#define ENABLE_MID_INDEX                    1


// How large is a single buffer for each bucket? How many buffer for all workers do we need?
// These buffers will be flushed to index file once they are full
#if ENABLE_MID_INDEX
// This is the size for (item_count == 6,7)
#define CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR   (CONFIG_ORDERDATES_PER_BUCKET * 1048576U * 20)  // Tune factor as necessary
#define CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR      (4096U * 3)  // Tune factor as necessary
// This is the size for (item_count == 4,5)
#define CONFIG_INDEX_SPARSE_BUCKET_SIZE_MID     (CONFIG_ORDERDATES_PER_BUCKET * 1048576U * 20)  // Tune factor as necessary
#define CONFIG_INDEX_TLS_BUFFER_SIZE_MID        (4096U * 3)  // Tune factor as necessary
// This is the size for (item_count == 1,2,3)
#define CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR   (CONFIG_ORDERDATES_PER_BUCKET * 1048576U * 10)  // Tune factor as necessary
#define CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR      (4096U * 1)  // Tune factor as necessary

#else  // !ENABLE_MID_INDEX
// This is the size for (item_count == 4,5,6,7)
#define CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR   (CONFIG_ORDERDATES_PER_BUCKET * 1048576U * 30)  // Tune factor as necessary
#define CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR      (4096U * 5)  // Tune factor as necessary
// This is the size for (item_count == 1,2,3)
#define CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR   (CONFIG_ORDERDATES_PER_BUCKET * 1048576U * 10)  // Tune factor as necessary
#define CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR      (4096U * 2)  // Tune factor as necessary
#endif

// How many extra buffers for each bucket?
// This saves pwrite() syscall a lot!
#define CONFIG_INDEX_EXTRA_BUFFER_COUNT         (26)
static_assert(CONFIG_INDEX_EXTRA_BUFFER_COUNT > 0);


// Number of maximum pretopn limit
// Calculate top-N in advance every several days
//  >0 - Calculate top-N every these days in advance (when query!)
#define CONFIG_TOPN_DATES_PER_PLATE         (32)
static_assert(CONFIG_TOPN_DATES_PER_PLATE > 0);
static_assert(CONFIG_TOPN_DATES_PER_PLATE <= 64, "Max 6 bits for orderdate_diff in a plate");
static_assert(CONFIG_TOPN_DATES_PER_PLATE % CONFIG_ORDERDATES_PER_BUCKET == 0);

// "N" in top-N when pre-calculating
// If this threshold is exceeded, we can't make use of the pretopn index any more (fallback to normal index scan)
#define CONFIG_EXPECT_MAX_TOPN              (102400U)  // According to problem description: 10000
static_assert((sizeof(uint64_t) * CONFIG_EXPECT_MAX_TOPN) % PAGE_SIZE == 0);

#endif  // !defined(_BDCI19_CONFIG_H_INCLUDED_)
