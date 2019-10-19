#if !defined(_BDCI19_CONFIG_H_INCLUDED_)
#define _BDCI19_CONFIG_H_INCLUDED_

#if !defined(MAKE_FASTEST)
//#define MAKE_FASTEST
#endif

#if !defined(MAKE_FASTEST)
#define ENABLE_ASSERTION                    0
#define ENABLE_LOGGING_DEBUG                0
#define ENABLE_LOGGING_INFO                 0
#endif

// Do we pin worker and loader threads to its corresponding CPU core?
//  0 - Don't pin
//  1 - Pin
#define ENABLE_PIN_THREAD_TO_CPU            0

// Do we use spin_lock in bounded_bag<T> and mpmc_queue<T>?
//  0 - Use std::mutex
//  1 - Use spin_lock
#define ENABLE_QUEUE_USE_SPIN_LOCK          1

// Do we pread() rather than mmap() in mmap_file_overlapped?
//  0 - Use mmap()
//  1 - Use pread()
#define ENABLE_LOAD_FILE_USE_PREAD          0  // Suggest to be 0 (except on sccc)

// Do we try to use hugetlb for better mmap() performance?
//  1 - Try to allocate huge pages (fall back to normal pages if failed)
//  0 - Don't try to allocate huge pages
// TODO: implement this and set to 1
#define ENABLE_ATTEMPT_HUGETLB              0

// Do we try to use SCHED_FIFO for better performance?
//  1 - Try to set scheduler to SCHED_FIFO (fall back to not changed (SCHED_OTHER) if failed)
//  0 - Don't try to set scheduler to SCHED_FIFO
// TODO: implement this and set to 1
#define ENABLE_ATTEMPT_SCHED_FIFO           0

// Number of buffers when we load the 3 text files
// Generally, 2 * loader_thread_count is fine
#define CONFIG_LOAD_BUFFER_COUNT            (32)

//======== Parameters for storing index "items_xxx" files ========
// How much (sparse) space is reserved for each bucket (for each worker thread)?
// A very large value is OK, as the file is sparse anyway
#define CONFIG_INDEX_SPARSE_SIZE_PER_BUCKET (65536 * 8)  // Tune factor as necessary

// How large is a single buffer for each bucket? How many buffer for each worker do we need?
// These buffers will be flushed to index file once they are full
#define CONFIG_INDEX_TLS_BUFFER_SIZE        (4096 * 1)  // Tune factor as necessary
#define CONFIG_INDEX_TLS_BUFFER_COUNT       (2 * 12800)

// When calling mmap_allocate_parallel, how large does each mmap() allocates?
// It must be inside [MIN_STEP_SIZE, MAX_STEP_SIZE]
#define CONFIG_MMAP_MAX_STEP_SIZE           (1048576U * 16)
#define CONFIG_MMAP_MIN_STEP_SIZE           (1024U * 64)
#define CONFIG_MMAP_MAX_STEP_SIZE_HUGETLB   (1048576U * 8)
#define CONFIG_MMAP_MIN_STEP_SIZE_HUGETLB   (1048576U * 2)

// How many byte per mmap() when loading original text files?
// Note, to deal with unaligned line-breaks, we have to overlap a little between each call
#define CONFIG_PART_OVERLAPPED_SIZE         (4096U)
#define CONFIG_CUSTOMER_PART_BODY_SIZE      (1048576U * 2 - CONFIG_PART_OVERLAPPED_SIZE)
#define CONFIG_ORDERS_PART_BODY_SIZE        (1048576U * 8 - CONFIG_PART_OVERLAPPED_SIZE)
#define CONFIG_LINEITEM_PART_BODY_SIZE      (1048576U * 16 - CONFIG_PART_OVERLAPPED_SIZE)


// Calculate top-N in advance every several days
//   0 - Don't calculate top-N in advance
//  >0 - Calculate top-N every these days in advance
#define CONFIG_TOPN_DATES_PER_PLATE         (8)
static_assert(CONFIG_TOPN_DATES_PER_PLATE <= 64, "Max 6 bits for orderdate_diff in a plate");

// Number of buffers when we load built indices (when pre-calculating top-N)
// Generally, 2 * loader_thread_count is fine
#define CONFIG_PRETOPN_BUFFER_COUNT         (32)

// Do we use pread() or mmap() to access those partial indices? (when pre-calculating top-N)
//  0 - Use mmap()
//  1 - Use pread()
#define CONFIG_PRETOPN_LOAD_INDEX_USE_PREAD 1  // Suggest to be 1

// "N" in top-N when pre-calculating
// If this threshold is exceeded, we can't make use of the pretopn index any more (fallback to normal index scan)
#define CONFIG_EXPECT_MAX_TOPN              (10000)  // According to problem description: 10000

#endif  // !defined(_BDCI19_CONFIG_H_INCLUDED_)
