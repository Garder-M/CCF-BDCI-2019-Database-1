#include "common.h"

//==============================================================================
// Structures
//==============================================================================
struct mapped_file_part_overlapped_t
{
    uint64_t file_offset;  // TODO: optimize this out to save memory
    uint32_t desired_size;
    uint32_t map_size;

    [[nodiscard]] FORCEINLINE bool is_first() const noexcept { return file_offset == 0; }
    [[nodiscard]] FORCEINLINE bool is_last() const noexcept { return (desired_size == map_size); }
};

struct pretopn_desc_t
{
    uint32_t plate_id;
    date_t from_orderdate;
    date_t to_orderdate;
    uint8_t mktid;

    int32_t topn_count;
    uint64_t topn_values[CONFIG_EXPECT_MAX_TOPN];
};


struct order_t
{
    std::make_unsigned_t<date_t> orderdate : 12;
    uint8_t mktid : 4;
};
static_assert(sizeof(order_t) == 2);

struct worker_pwrite_store_items_context
{
    struct pwrite_buffer_t {
        void* buffer;  // length: CONFIG_INDEX_TLS_BUFFER_SIZE
        uint64_t offset_in_file;
    };

    int items_fd;
    spsc_bounded_bag<void*, CONFIG_INDEX_TLS_BUFFER_COUNT> buffers;
    spsc_queue<pwrite_buffer_t, CONFIG_INDEX_TLS_BUFFER_COUNT> pwrite_buffers;
    iovec* buckets_buffer_ptr;  // [g_bucket_count]
    uint64_t* buckets_offset_in_file;  // [g_bucket_count]

    uint32_t max_orderdate_shipdate_diff = 0;
    uint32_t max_bucket_actual_size = 0;
};


//==============================================================================
// Global Variables
//==============================================================================
namespace
{
    constexpr const uint32_t MAPPING_BUFFER_SIZE = std::max({
        CONFIG_CUSTOMER_PART_BODY_SIZE,
        CONFIG_ORDERS_PART_BODY_SIZE,
        CONFIG_LINEITEM_PART_BODY_SIZE
    }) + CONFIG_PART_OVERLAPPED_SIZE;
    void* g_mapping_buffer_start_ptr = nullptr;
    bounded_bag<size_t, CONFIG_LOAD_BUFFER_COUNT> g_mapping_buffers { };
    mapped_file_part_overlapped_t g_overlapped_parts[CONFIG_LOAD_BUFFER_COUNT];

    mpmc_queue<size_t, CONFIG_LOAD_BUFFER_COUNT> g_customer_mapping_queue { };
    mpmc_queue<size_t, CONFIG_LOAD_BUFFER_COUNT> g_orders_mapping_queue { };
    mpmc_queue<size_t, CONFIG_LOAD_BUFFER_COUNT> g_lineitem_mapping_queue { };

    uint32_t g_max_custkey = 0;
    std::string g_all_mktsegments[256] { };
    std::mutex g_all_mktsegments_insert_mutex { };
    uint8_t g_mktid_count = 0;
    std::atomic_size_t g_custkey_to_mktid_shared_offset { 0 };
    uint8_t* g_custkey_to_mktid = nullptr;  // [g_max_custkey+1]

    uint32_t g_max_orderkey = 0;
    order_t* g_orderkey_to_order = nullptr;  // [g_max_orderkey+1]
    std::atomic_size_t g_orderkey_to_order_shared_offset { 0 };

    uint32_t g_total_buckets = 0;

    worker_pwrite_store_items_context* g_pwrite_store_items_context;  // [worker_thread_count]

#if CONFIG_TOPN_DATES_PER_PLATE > 0
    done_event g_write_meta_done;

    constexpr const uint32_t PLATES_PER_MKTID = (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) / CONFIG_TOPN_DATES_PER_PLATE + 1;
    void* g_pretopn_buffer_start_ptr = nullptr;
    bounded_bag<uint32_t, CONFIG_PRETOPN_BUFFER_COUNT> g_pretopn_buffers { };
    pretopn_desc_t g_pretopn_desc[CONFIG_PRETOPN_BUFFER_COUNT];
    mpmc_queue<uint32_t, CONFIG_PRETOPN_BUFFER_COUNT> g_pretopn_queue { };
    int g_pretopn_fd = -1;
    int g_pretopn_count_fd = -1;
    uint32_t* g_pretopn_count_ptr = nullptr;
#endif
}



FORCEINLINE size_t calc_bucket_index(const uint8_t mktid, const date_t orderdate) noexcept
{
    ASSERT(mktid < g_mktid_count);
    ASSERT(orderdate >= MIN_TABLE_DATE);
    ASSERT(orderdate <= MAX_TABLE_DATE);

    return (size_t)(mktid - 0) * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) + (orderdate - MIN_TABLE_DATE);
}


#if CONFIG_TOPN_DATES_PER_PLATE > 0
FORCEINLINE uint32_t calc_topn_plate_index(const uint8_t mktid, const date_t orderdate) noexcept
{
    ASSERT(orderdate >= MIN_TABLE_DATE);
    ASSERT(orderdate <= MAX_TABLE_DATE);

    return (uint32_t)(mktid - 0) * PLATES_PER_MKTID + (uint32_t)(orderdate - MIN_TABLE_DATE) / CONFIG_TOPN_DATES_PER_PLATE;
}
#endif

template<uint32_t _PartBodySize, uint32_t _PartOverlap>
FORCEINLINE void mmap_file_overlapped(
    /*in*/ sync_barrier& barrier,
    /*inout*/ std::atomic_size_t& shared_offset,  // it should be 0!
    /*in*/ [[maybe_unused]] const uint32_t tid,
    /*in*/ const size_t size,
    /*in*/ const int fd,
    /*inout*/ mpmc_queue<size_t, CONFIG_LOAD_BUFFER_COUNT>& queue,
    /*in*/ const uint32_t max_consumer_threads)
{
    DEBUG("[loader:%u] mmap_file_overlapped() starts: fd=%d", tid, fd);

#if ENABLE_ASSERTION
    barrier.run_once_and_sync([&]() {
        ASSERT(queue.capacity() > 0);
        ASSERT(shared_offset.load() == 0);
    });
#endif

    while (true) {
        const uint64_t off = shared_offset.fetch_add(_PartBodySize);
        if (off >= size) {
            if (off >= size + (barrier.thread_count() - 1) * _PartBodySize) {
                queue.mark_push_finish(max_consumer_threads);
            }
            break;
        }

        size_t part_index;
        g_mapping_buffers.take(&part_index);
        ASSERT(part_index < CONFIG_LOAD_BUFFER_COUNT);

        mapped_file_part_overlapped_t& part = g_overlapped_parts[part_index];
        part.file_offset = off;
        if (UNLIKELY(off + _PartBodySize + _PartOverlap >= size)) {  // this is last
            part.desired_size = size - off;
            part.map_size = size - off;
        }
        else {
            part.desired_size = _PartBodySize;
            part.map_size = _PartBodySize + _PartOverlap;
        }

        void* const ptr = (void*)((uintptr_t)g_mapping_buffer_start_ptr + part_index * MAPPING_BUFFER_SIZE);
#if ENABLE_LOAD_FILE_USE_PREAD
        const ssize_t read_cnt = C_CALL(pread(fd, ptr, part.map_size, off));
        CHECK(read_cnt == part.map_size);
#else
        void* const return_ptr = mmap(
            ptr,
            part.map_size,
            PROT_READ,
            MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
            fd,
            off);
        CHECK(return_ptr != MAP_FAILED);
        ASSERT(return_ptr == ptr);
#endif

        //DEBUG("[loader:%u] mmap_file_overlapped(): fd=%d mapped to %p (size=%lu, offset=%lu, _DesiredSize=%u, map_size=%u, part_index=%lu)",
        //    tid, fd, ptr, size, off, part.desired_size, part.map_size, part_index);
        queue.push(part_index);

        //C_CALL(sched_yield());
    }

    DEBUG("[loader:%u] mmap_file_overlapped() done: fd=%d", tid, fd);
}


void fn_loader_thread_create_index(const uint32_t tid) noexcept
{
    DEBUG("[loader:%u] fn_loader_thread_create_index() starts", tid);

    // Load customer file to memory
    {
        mmap_allocate_parallel(
            (void**)&g_custkey_to_mktid,
            g_loader_sync_barrier,
            g_custkey_to_mktid_shared_offset,
            tid,
            sizeof(g_custkey_to_mktid[0]) * (g_max_custkey + 1));

        mmap_file_overlapped<CONFIG_CUSTOMER_PART_BODY_SIZE, CONFIG_PART_OVERLAPPED_SIZE>(
            g_loader_sync_barrier,
            g_customer_file.shared_offset,
            tid,
            g_customer_file.file_size,
            g_customer_file.fd,
            g_customer_mapping_queue,
            g_worker_sync_barrier.thread_count());
    }


    // Load orders file to memory
    {
        mmap_allocate_parallel(
            (void**)&g_orderkey_to_order,
            g_loader_sync_barrier,
            g_orderkey_to_order_shared_offset,
            tid,
            sizeof(g_orderkey_to_order[0]) * (g_max_orderkey + 1));

        mmap_file_overlapped<CONFIG_ORDERS_PART_BODY_SIZE, CONFIG_PART_OVERLAPPED_SIZE>(
            g_loader_sync_barrier,
            g_orders_file.shared_offset,
            tid,
            g_orders_file.file_size,
            g_orders_file.fd,
            g_orders_mapping_queue,
            g_worker_sync_barrier.thread_count());
    }


    // Load lineitem file to memory
    {
        static std::atomic_uint32_t __curr_worker_tid { 0 };

        static void* __buffers_start_ptr;  // must be static!
        static std::atomic_uint64_t __tmp_shared_offset { 0 };  // must be static!
        mmap_allocate_parallel(
            &__buffers_start_ptr,
            g_loader_sync_barrier,
            __tmp_shared_offset,
            tid,
            (uint64_t)g_worker_sync_barrier.thread_count() * CONFIG_INDEX_TLS_BUFFER_COUNT * CONFIG_INDEX_TLS_BUFFER_SIZE);

        while (true) {
            const uint32_t worker_tid = __curr_worker_tid++;
            if (worker_tid >= g_worker_sync_barrier.thread_count()) break;
            auto& ctx = g_pwrite_store_items_context[worker_tid];

            char items_file_name[32];
            snprintf(items_file_name, std::size(items_file_name), "items_%u", worker_tid);
            const uint64_t sparse_size = (uint64_t)g_total_buckets * CONFIG_INDEX_SPARSE_SIZE_PER_BUCKET;
            openat_file_write(items_file_name, &ctx.items_fd, sparse_size, false);

            CHECK(CONFIG_INDEX_TLS_BUFFER_COUNT >= g_total_buckets);
            const uintptr_t curr_worker_start_ptr = (uintptr_t)__buffers_start_ptr + (uint64_t)worker_tid * CONFIG_INDEX_TLS_BUFFER_COUNT * CONFIG_INDEX_TLS_BUFFER_SIZE;
            ctx.buffers.init([&](const size_t idx) -> void* {
                ASSERT(idx < CONFIG_INDEX_TLS_BUFFER_COUNT - g_total_buckets);
                return (void*)(curr_worker_start_ptr + (g_total_buckets + idx) * CONFIG_INDEX_TLS_BUFFER_SIZE);
            }, CONFIG_INDEX_TLS_BUFFER_COUNT - g_total_buckets);
            //ctx.pwrite_buffers.init(...);
            INFO("[loader:%u] worker #%u: items_start_ptr: %p", tid, worker_tid, (void*)curr_worker_start_ptr);

            ctx.buckets_buffer_ptr = new iovec[g_total_buckets];
            for (uint32_t bucket = 0; bucket < g_total_buckets; ++bucket) {
                uintptr_t bucket_ptr = (uintptr_t)curr_worker_start_ptr + bucket * CONFIG_INDEX_TLS_BUFFER_SIZE;
                ctx.buckets_buffer_ptr[bucket].iov_base = (void*)bucket_ptr;
                ctx.buckets_buffer_ptr[bucket].iov_len = 0;
            }

            ctx.buckets_offset_in_file = new uint64_t[g_total_buckets];
            for (uint32_t bucket = 0; bucket < g_total_buckets; ++bucket) {
                uintptr_t bucket_offset_in_file = (uint64_t)bucket * CONFIG_INDEX_SPARSE_SIZE_PER_BUCKET;
                ctx.buckets_offset_in_file[bucket] = bucket_offset_in_file;
            }
        }

        g_loader_sync_barrier.sync();


        mmap_file_overlapped<CONFIG_LINEITEM_PART_BODY_SIZE, CONFIG_PART_OVERLAPPED_SIZE>(
            g_loader_sync_barrier,
            g_lineitem_file.shared_offset,
            tid,
            g_lineitem_file.file_size,
            g_lineitem_file.fd,
            g_lineitem_mapping_queue,
            g_worker_sync_barrier.thread_count());
    }


    // Save mktsegment to index
    {
        if (tid == 0) {
            std::string buf;
            buf.reserve(64);
            buf += (char)g_mktid_count;
            for (uint8_t mktid = 0; mktid < g_mktid_count; ++mktid) {
                buf += (char)g_all_mktsegments[mktid].length();
                buf += g_all_mktsegments[mktid];
            }

            const int fd = C_CALL(openat(
                g_index_directory_fd,
                "mktsegment",
                O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
                0666));

            size_t cnt = C_CALL(write(fd, buf.data(), buf.length()));
            CHECK(cnt == buf.length());

            C_CALL(close(fd));
        }
    }


#if CONFIG_TOPN_DATES_PER_PLATE > 0
    // Load built indices for pre calculating top-N
    {
        g_loader_sync_barrier.run_once_and_sync([]() {
            openat_file_write(
                "pretopn",
                &g_pretopn_fd,
                sizeof(uint64_t) * CONFIG_EXPECT_MAX_TOPN * g_mktid_count * PLATES_PER_MKTID,
                false);
            INFO("g_pretopn_fd: %d", g_pretopn_fd);

            openat_file_write(
                "pretopn_count",
                &g_pretopn_count_fd,
                sizeof(uint32_t) * g_mktid_count * PLATES_PER_MKTID,
                true);
            INFO("g_pretopn_count_fd: %d", g_pretopn_count_fd);

            g_pretopn_count_ptr = (uint32_t*)my_mmap(
                sizeof(uint32_t) * g_mktid_count * PLATES_PER_MKTID,
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_POPULATE,
                g_pretopn_count_fd,
                0);
            INFO("g_pretopn_count_ptr: %p", g_pretopn_count_ptr);
        });

        g_write_meta_done.wait_done();
        ASSERT(g_meta.max_bucket_actual_size_up_aligned > 0);

        const uint64_t pretopn_buffer_total_size = (uint64_t)g_meta.max_bucket_actual_size_up_aligned * CONFIG_TOPN_DATES_PER_PLATE * g_worker_sync_barrier.thread_count() * CONFIG_PRETOPN_BUFFER_COUNT;
#if CONFIG_PRETOPN_LOAD_INDEX_USE_PREAD
        static std::atomic_size_t __shared_offset { 0 };
        mmap_allocate_parallel(
            &g_pretopn_buffer_start_ptr,
            g_loader_sync_barrier,
            __shared_offset,
            tid,
            pretopn_buffer_total_size);
#else
        g_loader_sync_barrier.run_once_and_sync([&]() {
            g_pretopn_buffer_start_ptr = my_mmap(
                pretopn_buffer_total_size,
                PROT_READ,
                MAP_PRIVATE | MAP_ANONYMOUS,  // no MAP_POPULATE
                -1,
                0);
            //C_CALL(munmap(g_pretopn_buffer_start_ptr, pretopn_buffer_total_size));
        });
#endif
        if (tid == 0) {
            INFO("g_pretopn_buffer_start_ptr: %p", g_pretopn_buffer_start_ptr);
            INFO("pretopn_buffer_total_size: 0x%lx, g_pretopn_buffer_start_ptr+pretopn_buffer_total_size: %p",
                pretopn_buffer_total_size, (void*)((uintptr_t)g_pretopn_buffer_start_ptr + pretopn_buffer_total_size));
        }
        ASSERT(g_pretopn_buffer_start_ptr != nullptr);

        static std::atomic_uint32_t __plate_id { 0 };
        const uint32_t max_plate_id = g_mktid_count * PLATES_PER_MKTID;
        while (true) {
            const uint32_t plate_id = __plate_id++;
            if (plate_id >= max_plate_id) break;

            uint32_t idx;
            g_pretopn_buffers.take(&idx);
            ASSERT(idx < CONFIG_PRETOPN_BUFFER_COUNT);

            const date_t from_orderdate = MIN_TABLE_DATE + (plate_id % PLATES_PER_MKTID) * CONFIG_TOPN_DATES_PER_PLATE;
            const date_t to_orderdate = std::min<date_t>(from_orderdate + CONFIG_TOPN_DATES_PER_PLATE - 1, MAX_TABLE_DATE);
            g_pretopn_desc[idx].plate_id = plate_id;
            g_pretopn_desc[idx].mktid = 0 + (plate_id / PLATES_PER_MKTID);
            ASSERT(g_pretopn_desc[idx].mktid < g_mktid_count);
            g_pretopn_desc[idx].from_orderdate = from_orderdate;
            g_pretopn_desc[idx].to_orderdate = to_orderdate;
            g_pretopn_desc[idx].topn_count = 0;

            void* const dst_ptr = (void*)((uintptr_t)g_pretopn_buffer_start_ptr +
                                          (uint64_t)g_meta.max_bucket_actual_size_up_aligned * CONFIG_TOPN_DATES_PER_PLATE * g_worker_sync_barrier.thread_count() * idx);

            for (uint32_t worker_tid = 0; worker_tid < g_worker_sync_barrier.thread_count(); ++worker_tid) {
                const auto& ctx = g_pwrite_store_items_context[worker_tid];
                void* const dst_ptr_worker = (void*)((uintptr_t)dst_ptr +
                                                     (uint64_t)g_meta.max_bucket_actual_size_up_aligned * CONFIG_TOPN_DATES_PER_PLATE * worker_tid);
                for (date_t orderdate = from_orderdate; orderdate <= to_orderdate; ++orderdate) {
                    void* const dst_ptr_orderdate = (void*)((uintptr_t)dst_ptr_worker +
                                                            (uint64_t)g_meta.max_bucket_actual_size_up_aligned * (orderdate - from_orderdate));
                    const uint32_t bucket = calc_bucket_index(g_pretopn_desc[idx].mktid, orderdate);
                    const uint64_t bucket_start_offset_in_file = (uint64_t)bucket * CONFIG_INDEX_SPARSE_SIZE_PER_BUCKET;
                    const uint32_t bucket_size = ctx.buckets_offset_in_file[bucket] - bucket_start_offset_in_file;
                    ASSERT(bucket_size <= g_meta.max_bucket_actual_size_up_aligned);
                    if (bucket_size == 0) continue;

#if CONFIG_PRETOPN_LOAD_INDEX_USE_PREAD
                    const size_t cnt = C_CALL(pread(ctx.items_fd, dst_ptr_orderdate, bucket_size, bucket_start_offset_in_file));
                    CHECK(cnt == bucket_size);
#else
                    ASSERT(bucket_start_offset_in_file % 4096 == 0);
                    void* const dummy = mmap64(
                        dst_ptr_orderdate,
                        bucket_size,
                        PROT_READ,
                        MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                        ctx.items_fd,
                        bucket_start_offset_in_file);
                    CHECK(dummy != MAP_FAILED, "mmap() failed. errno: %d(%s). dst_ptr_orderdate=%p,bucket_start_offset_in_file=0x%lx,bucket_size=0x%x",
                        errno, strerror(errno), dst_ptr_orderdate, bucket_start_offset_in_file, bucket_size);
                    ASSERT(dummy == dst_ptr_orderdate);
#endif
                }
            }

            g_pretopn_queue.push(idx);
        }
        g_loader_sync_barrier.sync_and_run_once([]() {
            g_pretopn_queue.mark_push_finish(g_worker_sync_barrier.thread_count());
        });
    }
#endif  // CONFIG_TOPN_DATES_PER_PLATE > 0

    DEBUG("[loader:%u] fn_loader_thread_create_index() done", tid);
}



void worker_load_customer_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
    size_t part_index;
    while (g_customer_mapping_queue.pop(&part_index)) {
        ASSERT(part_index < CONFIG_LOAD_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_overlapped_parts[part_index];
        const char* p = (const char*)((uintptr_t)g_mapping_buffer_start_ptr + part_index * MAPPING_BUFFER_SIZE);
        const char* end = p + part.desired_size;

        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;
        }
        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;
        }
        DEBUG("[%u] load customer: [%p, %p)", tid, p, end);

        while (p < end) {
            uint32_t custkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                custkey = custkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(custkey <= g_max_custkey, "Unexpected custkey (too large): %u", custkey);

            const char* const mktsegment_start = p;
            while (*p != '\n') {
                ++p;
            }

            std::string_view mktsegment_view(mktsegment_start, p - mktsegment_start);
            ++p;  // skip '\n'

            const auto try_find_mktsegment = [&]() -> bool {
                for (int8_t i = g_mktid_count - 1; i >= 0; --i) {
                    if (g_all_mktsegments[i][0] != mktsegment_view[0]) continue;
                    if (g_all_mktsegments[i] == mktsegment_view) {
                        g_custkey_to_mktid[custkey] = (uint8_t)i;
                        return true;
                    }
                }
                return false;
            };

            if (!try_find_mktsegment()) {
                std::unique_lock<std::mutex> lock(g_all_mktsegments_insert_mutex);
                if (!try_find_mktsegment()) {
                    DEBUG("custkey = %u, & = %p", custkey, &g_custkey_to_mktid[custkey]);
                    g_custkey_to_mktid[custkey] = g_mktid_count;
                    g_all_mktsegments[g_mktid_count] = mktsegment_view;
                    DEBUG("found new mktsegment: %s -> %u", g_all_mktsegments[g_mktid_count].c_str(), g_mktid_count);
                    ++g_mktid_count;
                    g_total_buckets = g_mktid_count * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1);
                }
            }
        }

        g_mapping_buffers.return_back(part_index);
    }

    DEBUG("[%u] done worker_load_customer_multi_part()", tid);
}



void worker_load_orders_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
    size_t part_index;
    while (g_orders_mapping_queue.pop(&part_index)) {
        ASSERT(part_index < CONFIG_LOAD_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_overlapped_parts[part_index];
        const char* p = (const char*)((uintptr_t)g_mapping_buffer_start_ptr + part_index * MAPPING_BUFFER_SIZE);
        const char* end = p + part.desired_size;

        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;
        }
        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;
        }

        DEBUG("[%u] load orders: [%p, %p)", tid, p, end);
        while (p < end) {
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);

            uint32_t custkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                custkey = custkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            const uint8_t mktid = g_custkey_to_mktid[custkey];
            ASSERT(mktid < g_mktid_count, "Expect mktid < g_mktid_count (%u < %u)", mktid, g_mktid_count);

            const date_t orderdate = date_from_string(p);
            ASSERT(orderdate >= MIN_TABLE_DATE);
            ASSERT(orderdate <= MAX_TABLE_DATE);
            p += 10;  // skip 'yyyy-MM-dd'

            // Save to orderkey_to_order
            ASSERT(mktid < (1 << 4), "Boom! mktid %u too large", mktid);
            ASSERT(orderdate < (1 << 12), "BUG: orderdate too large?");
            g_orderkey_to_order[orderkey] = { (std::make_unsigned_t<date_t>)(orderdate), mktid };

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'
        }

        g_mapping_buffers.return_back(part_index);
    }

    DEBUG("[%u] done worker_load_orders_multi_part()", tid);
}



FORCEINLINE const char* skip_one_orderkey_in_lineitem(const char* p) noexcept
{
    uint32_t first_orderkey = (uint32_t)-1;
    while (true) {
        const char* const saved_p = p;
        uint32_t orderkey = 0;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            orderkey = orderkey * 10 + (*p - '0');
            ++p;
        }
        //++p;  // skip '|' (not necessary)
        ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);

        if (first_orderkey == (uint32_t)-1) {
            first_orderkey = orderkey;
        }
        else {
            ASSERT(orderkey >= first_orderkey, "lineitem not sorted? orderkey=%u, first_orderkey=%u", orderkey, first_orderkey);
            if (orderkey > first_orderkey) return saved_p;
        }

        while (*p != '\n') ++p;
        ++p;  // skip '\n'
    }
}


void worker_load_lineitem_multi_part(const uint32_t tid) noexcept
{
    // Prepare file mapping
    auto& ctx = g_pwrite_store_items_context[tid];
    uint32_t max_orderdate_shipdate_diff = 0;

    std::thread thread_pwrite([&]() {
        worker_pwrite_store_items_context::pwrite_buffer_t pwrite_buffer;
        while (ctx.pwrite_buffers.pop(&pwrite_buffer)) {
            //DEBUG("[pwrite:%u] pwrite: %p (length=%u), offset_in_file=0x%lx",
            //    tid, pwrite_buffer.buffer, CONFIG_INDEX_TLS_BUFFER_SIZE, pwrite_buffer.offset_in_file);
            ssize_t cnt = C_CALL(pwrite(
                ctx.items_fd,
                pwrite_buffer.buffer,
                CONFIG_INDEX_TLS_BUFFER_SIZE,
                pwrite_buffer.offset_in_file));
            CHECK(cnt == CONFIG_INDEX_TLS_BUFFER_SIZE);
            ctx.buffers.return_back(pwrite_buffer.buffer);
        }
    });

    const auto maybe_submit_for_pwrite = [&ctx](
        /*inout*/iovec& vec,
        /*inout*/uint64_t& bucket_offset_in_file)
        __attribute__((always_inline)) noexcept
    {
        if (LIKELY(vec.iov_len < CONFIG_INDEX_TLS_BUFFER_SIZE)) {
            return;
        }
        ASSERT(vec.iov_len == CONFIG_INDEX_TLS_BUFFER_SIZE, "vec.iov_len = %lu", vec.iov_len);
        //DEBUG("[%u] queue for pwrite: %p (length=%u), offset_in_file=0x%lx",
        //    tid, vec.iov_base, CONFIG_INDEX_TLS_BUFFER_SIZE, bucket_offset_in_file);

        ctx.pwrite_buffers.push(
            worker_pwrite_store_items_context::pwrite_buffer_t {
                vec.iov_base,
                bucket_offset_in_file });
        ctx.buffers.take(&vec.iov_base);
        vec.iov_len = 0;
        bucket_offset_in_file += CONFIG_INDEX_TLS_BUFFER_SIZE;
    };


    size_t part_index;
    while (g_lineitem_mapping_queue.pop(&part_index)) {
        ASSERT(part_index < CONFIG_LOAD_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_overlapped_parts[part_index];
        const char* p = (const char*)((uintptr_t)g_mapping_buffer_start_ptr + part_index * MAPPING_BUFFER_SIZE);
        const char* valid_end = p + part.desired_size;

        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;

            p = skip_one_orderkey_in_lineitem(p);
        }
        const char* const valid_start = p;

        if (!part.is_last()) {
            while (*valid_end != '\n') ++valid_end;
            ++valid_end;

            valid_end = skip_one_orderkey_in_lineitem(valid_end);
        }

        DEBUG("[%u] load lineitem: [%p, %p)", tid, p, valid_end);

        // Detect first orderkey
        uint32_t last_orderkey = 0;
        uint32_t last_bucket;
        {
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                last_orderkey = last_orderkey * 10 + (*p - '0');
                ++p;
            }
            const order_t& order = g_orderkey_to_order[last_orderkey];
            last_bucket = calc_bucket_index(order.mktid, order.orderdate);
        }

        p = valid_start;
        while (true) {
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);

            const order_t& order = g_orderkey_to_order[orderkey];

            const uint8_t mktid = order.mktid;
            const date_t orderdate = order.orderdate;
            ASSERT(mktid < g_mktid_count, "BUG: expect mktid < g_mktid_count (%u < %u)", mktid, g_mktid_count);

            uint32_t expend_cent = 0;
            while (*p != '.') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                expend_cent = expend_cent * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '.'
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                expend_cent = expend_cent * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(expend_cent < (1 << 24), "Boom! expend_cent too large");

            const date_t shipdate = date_from_string(p);
            p += 10;  // skip 'yyyy-MM-dd'
            ASSERT(shipdate > orderdate, "Expect shipdate > orderdate");
            ASSERT((shipdate - orderdate) < (1 << 7), "shipdate - orderdate Boom! shipdate=%d, orderdate=%d", (int32_t)shipdate, (int32_t)orderdate);
            ASSERT(shipdate >= MIN_TABLE_DATE, "Expect shipdate >= MIN_TABLE_DATE");
            ASSERT(shipdate <= MAX_TABLE_DATE, "Expect shipdate <= MAX_TABLE_DATE");
            if (UNLIKELY(shipdate - orderdate) > max_orderdate_shipdate_diff) {
                max_orderdate_shipdate_diff = shipdate - orderdate;
            }

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'

            const uint32_t bucket = calc_bucket_index(mktid, orderdate);

            if (orderkey != last_orderkey) {
                iovec& last_vec = ctx.buckets_buffer_ptr[last_bucket];
                *(uint32_t*)((uintptr_t)last_vec.iov_base + last_vec.iov_len) = last_orderkey | 0x80000000U;
                last_vec.iov_len += sizeof(uint32_t);
                maybe_submit_for_pwrite(last_vec, ctx.buckets_offset_in_file[last_bucket]);

                last_orderkey = orderkey;
                last_bucket = bucket;
            }

            ASSERT(ctx.buckets_buffer_ptr != nullptr);
            iovec& vec = ctx.buckets_buffer_ptr[bucket];
            ASSERT(vec.iov_base != nullptr);

            *(uint32_t*)((uintptr_t)vec.iov_base + vec.iov_len) = (uint32_t)(shipdate - orderdate) << 24 | expend_cent;
            vec.iov_len += sizeof(uint32_t);
            maybe_submit_for_pwrite(vec, ctx.buckets_offset_in_file[bucket]);

            if (UNLIKELY(p >= valid_end)) {
                ASSERT(p == valid_end);

                *(uint32_t*)((uintptr_t)vec.iov_base + vec.iov_len) = orderkey | 0x80000000U;
                vec.iov_len += sizeof(uint32_t);
                maybe_submit_for_pwrite(vec, ctx.buckets_offset_in_file[bucket]);

                break;
            }
        }

        g_mapping_buffers.return_back(part_index);
    }

    // Update my max_orderdate_shipdate_diff
    DEBUG("[%u] max_orderdate_shipdate_diff: %u", tid, max_orderdate_shipdate_diff);
    ctx.max_orderdate_shipdate_diff = max_orderdate_shipdate_diff;
    INFO("[%u] ctx.max_orderdate_shipdate_diff: %u", tid, max_orderdate_shipdate_diff);

    INFO("[%u] worker_load_lineitem_multi_part() now pwrite tails", tid);

    // For each bucket, write remaining buffer (if exists) to file
    for (uint32_t bucket = 0; bucket < g_total_buckets; ++bucket) {
        const iovec& vec = ctx.buckets_buffer_ptr[bucket];
        if (vec.iov_len != 0) {
            ctx.pwrite_buffers.push(
                worker_pwrite_store_items_context::pwrite_buffer_t {
                    vec.iov_base,
                    ctx.buckets_offset_in_file[bucket]
                });
            ctx.buckets_offset_in_file[bucket] += vec.iov_len;
        }

        const uint64_t bucket_start_offset_in_file = (uint64_t)bucket * CONFIG_INDEX_SPARSE_SIZE_PER_BUCKET;
        ASSERT((ctx.buckets_offset_in_file[bucket] - bucket_start_offset_in_file) < UINT32_MAX);
        const uint32_t bucket_actual_size = (uint32_t)(ctx.buckets_offset_in_file[bucket] - bucket_start_offset_in_file);
        if (bucket_actual_size > ctx.max_bucket_actual_size) {
            ctx.max_bucket_actual_size = bucket_actual_size;
        }
    }
    INFO("[%u] ctx.max_bucket_actual_size: 0x%x", tid, ctx.max_bucket_actual_size);

    ctx.pwrite_buffers.mark_push_finish(/*1*/);

    // Write endoffset to index directory
    {
        char filename[32];
        snprintf(filename, std::size(filename), "endoffset_%u", tid);
        const int fd = C_CALL(openat(
            g_index_directory_fd,
            filename,
            O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
            0666));

        size_t cnt = C_CALL(write(fd, ctx.buckets_offset_in_file, sizeof(uint64_t) * g_total_buckets));
        CHECK(cnt == sizeof(uint64_t) * g_total_buckets);

        C_CALL(close(fd));
    }

    thread_pwrite.join();

    INFO("[%u] done worker_load_lineitem_multi_part()", tid);
}


#if CONFIG_TOPN_DATES_PER_PLATE > 0

void worker_calc_pretopn_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
    spsc_queue<uint32_t, CONFIG_PRETOPN_BUFFER_COUNT> pwrite_queue;
    std::thread thread_pwrite([&]() {
        ASSERT(g_pretopn_fd != -1);

        uint32_t desc_index;
        while (pwrite_queue.pop(&desc_index)) {
            ASSERT(g_pretopn_desc[desc_index].plate_id < g_mktid_count * PLATES_PER_MKTID);

            g_pretopn_count_ptr[g_pretopn_desc[desc_index].plate_id] = g_pretopn_desc[desc_index].topn_count;
            if (g_pretopn_desc[desc_index].topn_count > 0) {
                const size_t cnt = C_CALL(pwrite(
                    g_pretopn_fd,
                    g_pretopn_desc[desc_index].topn_values,
                    sizeof(uint64_t) * g_pretopn_desc[desc_index].topn_count,
                    sizeof(uint64_t) * CONFIG_EXPECT_MAX_TOPN * g_pretopn_desc[desc_index].plate_id));
                CHECK(cnt == sizeof(uint64_t) * g_pretopn_desc[desc_index].topn_count);

                C_CALL(sched_yield());
            }

            g_pretopn_buffers.return_back(desc_index);
        }
    });


    uint32_t desc_index;
    while (g_pretopn_queue.pop(&desc_index)) {
        auto& desc = g_pretopn_desc[desc_index];
        ASSERT(desc.topn_count == 0);

        const date_t from_orderdate = desc.from_orderdate;
        ASSERT(from_orderdate >= MIN_TABLE_DATE);
        ASSERT(from_orderdate <= MAX_TABLE_DATE);

        const date_t to_orderdate = desc.to_orderdate;
        ASSERT(to_orderdate >= MIN_TABLE_DATE);
        ASSERT(to_orderdate <= MAX_TABLE_DATE);
        ASSERT(to_orderdate >= from_orderdate);
        ASSERT(to_orderdate < from_orderdate + CONFIG_TOPN_DATES_PER_PLATE);

        const uint32_t mktid = desc.mktid;
        ASSERT(mktid < g_mktid_count);

        [[maybe_unused]] const uint32_t plate_id = desc.plate_id;
        ASSERT(plate_id < PLATES_PER_MKTID * g_mktid_count);

        void* const src_ptr = (void*)((uintptr_t)g_pretopn_buffer_start_ptr +
                                      (uint64_t)g_meta.max_bucket_actual_size_up_aligned * CONFIG_TOPN_DATES_PER_PLATE * g_worker_sync_barrier.thread_count() * desc_index);

        for (uint32_t worker_tid = 0; worker_tid < g_worker_sync_barrier.thread_count(); ++worker_tid) {
            const auto& ctx = g_pwrite_store_items_context[worker_tid];
            void* const src_ptr_worker = (void*)((uintptr_t)src_ptr +
                                                 (uint64_t)g_meta.max_bucket_actual_size_up_aligned * CONFIG_TOPN_DATES_PER_PLATE * worker_tid);
            for (date_t orderdate = from_orderdate; orderdate <= to_orderdate; ++orderdate) {
                void* const src_ptr_orderdate = (void*)((uintptr_t)src_ptr_worker +
                                                        (uint64_t)g_meta.max_bucket_actual_size_up_aligned * (orderdate - from_orderdate));
                const uint32_t bucket = calc_bucket_index(mktid, orderdate);
                const uint64_t bucket_start_offset_in_file = (uint64_t)bucket * CONFIG_INDEX_SPARSE_SIZE_PER_BUCKET;
                const uint32_t bucket_size = ctx.buckets_offset_in_file[bucket] - bucket_start_offset_in_file;
                ASSERT(bucket_size <= g_meta.max_bucket_actual_size_up_aligned);

                // Load from [src_ptr_orderdate, src_ptr_orderdate+bucket_size)
                const uint32_t* p = (uint32_t*)src_ptr_orderdate;
                const uint32_t* const end = (uint32_t*)((uintptr_t)src_ptr_orderdate + bucket_size);

                uint32_t total_expend_cent = 0;
                const auto maybe_update_topn = [&](const uint32_t orderkey) {
                    ASSERT(total_expend_cent > 0);
                    ASSERT(orderkey < (1 << 30), "orderkey Boom!");

                    if (desc.topn_count < CONFIG_EXPECT_MAX_TOPN) {
                        desc.topn_values[desc.topn_count++] = (uint64_t)total_expend_cent << 36 | (uint64_t)orderkey << 6 | (uint32_t)(orderdate - from_orderdate);
                        if (desc.topn_count == CONFIG_EXPECT_MAX_TOPN) {
                            std::make_heap(desc.topn_values, desc.topn_values + CONFIG_EXPECT_MAX_TOPN, std::greater<>());
                        }
                    }
                    else {
                        if (UNLIKELY(total_expend_cent > (desc.topn_values[0] >> 36))) {
                            std::pop_heap(desc.topn_values, desc.topn_values + CONFIG_EXPECT_MAX_TOPN, std::greater<>());
                            desc.topn_values[CONFIG_EXPECT_MAX_TOPN - 1] = (uint64_t)total_expend_cent << 36 | (uint64_t)orderkey << 6 | (uint32_t)(orderdate - from_orderdate);
                            std::push_heap(desc.topn_values, desc.topn_values + CONFIG_EXPECT_MAX_TOPN, std::greater<>());
                        }
                    }
                };

                while (p < end) {
                    const uint32_t value = *(p++);
                    if (value & 0x80000000) {  // This is orderkey
                        const uint32_t orderkey = value & ~0x80000000;
                        maybe_update_topn(orderkey);
                        total_expend_cent = 0;
                    }
                    else {
                        //const date_t shipdate = orderdate + (value >> 24);
                        const uint32_t expend_cent = value & 0x00FFFFFF;
                        ASSERT(expend_cent > 0);
                        total_expend_cent += expend_cent;
                    }
                }
            }
        }

        std::sort(desc.topn_values, desc.topn_values + desc.topn_count, std::greater<>());

#if ENABLE_LOGGING_DEBUG
        const uint64_t value = desc.topn_values[0];
        const uint32_t expend_cent = value >> 36;
        const uint32_t orderkey = (value >> 6) & ((1U << 30) - 1);
        const date_t orderdate = from_orderdate + (value & 0b111111U);
        const auto tuple = date_get_ymd(orderdate);
        DEBUG("plate %u: N=%u, top=(orderkey=%u,orderdate=%u-%02u-%02u,expend_cent=%u)",
             plate_id, desc.topn_count, orderkey, std::get<0>(tuple), std::get<1>(tuple), std::get<2>(tuple), expend_cent);
#endif

        pwrite_queue.push(desc_index);
    }

    pwrite_queue.mark_push_finish();

    thread_pwrite.join();

    INFO("[%u] done worker_calc_pretopn_multi_part()", tid);
}

#endif  // CONFIG_TOPN_DATES_PER_PLATE > 0


void fn_worker_thread_create_index(const uint32_t tid) noexcept
{
    DEBUG("[%u] fn_worker_thread_create_index() starts", tid);

    // Parse loaded customer table
    {
        worker_load_customer_multi_part(tid);
        g_worker_sync_barrier.sync();

        if (tid == 0) {
            INFO("g_total_buckets: %u", g_total_buckets);
        }
    }

    // Parse loaded orders table
    {
        worker_load_orders_multi_part(tid);
        g_worker_sync_barrier.sync();
    }

    // Parse loaded lineitem table
    {
        worker_load_lineitem_multi_part(tid);
        g_worker_sync_barrier.sync();
    }

    // Save meta to index directory
    g_worker_sync_barrier.run_once_and_sync([]() {
        g_meta.partial_index_count = g_worker_sync_barrier.thread_count();
        g_meta.max_shipdate_orderdate_diff = 0;
        g_meta.max_bucket_actual_size = 0;
        for (uint32_t worker_tid = 0; worker_tid < g_worker_sync_barrier.thread_count(); ++worker_tid) {
            if (g_pwrite_store_items_context[worker_tid].max_orderdate_shipdate_diff > g_meta.max_shipdate_orderdate_diff) {
                g_meta.max_shipdate_orderdate_diff = g_pwrite_store_items_context[worker_tid].max_orderdate_shipdate_diff;
            }
            if (g_pwrite_store_items_context[worker_tid].max_bucket_actual_size > g_meta.max_bucket_actual_size) {
                g_meta.max_bucket_actual_size = g_pwrite_store_items_context[worker_tid].max_bucket_actual_size;
            }
        }
        INFO("g_meta.max_shipdate_orderdate_diff: %u", g_meta.max_shipdate_orderdate_diff);
        INFO("g_meta.max_bucket_actual_size: 0x%x", g_meta.max_bucket_actual_size);
        g_meta.max_bucket_actual_size_up_aligned = (uint32_t)(uintptr_t)MMAP_ALIGN_UP(g_meta.max_bucket_actual_size, 4096);
        INFO("g_meta.max_bucket_actual_size_up_aligned: 0x%x", g_meta.max_bucket_actual_size_up_aligned);  // up-align to 4K

        const int fd = C_CALL(openat(
            g_index_directory_fd,
            "meta",
            O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
            0666));

        size_t cnt = C_CALL(write(fd, &g_meta, sizeof(g_meta)));
        CHECK(cnt == sizeof(g_meta));

        C_CALL(close(fd));

#if CONFIG_TOPN_DATES_PER_PLATE > 0
        g_write_meta_done.mark_done();
#endif
    });


#if CONFIG_TOPN_DATES_PER_PLATE > 0
    // Ready to load all index again, and build topn in advance
    {
        worker_calc_pretopn_multi_part(tid);
    }
#endif

    DEBUG("[%u] fn_worker_thread_create_index() done", tid);
}


void create_index_initialize() noexcept
{
    // Calculate g_max_custkey
    {
        char buffer[256];
        C_CALL(pread(
            g_customer_file.fd,
            buffer,
            std::size(buffer),
            g_customer_file.file_size - std::size(buffer)));
        const char* p = &buffer[std::size(buffer) - 1];
        while (*(p - 1) != '\n') --p;
        g_max_custkey = 0;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9');
            g_max_custkey = g_max_custkey * 10 + (*p - '0');
            ++p;
        }
        DEBUG("g_max_custkey: %u", g_max_custkey);
    }

    // Calculate g_max_orderkey
    {
        char buffer[256];
        C_CALL(pread(
            g_orders_file.fd,
            buffer,
            std::size(buffer),
            g_orders_file.file_size - std::size(buffer)));
        const char* p = &buffer[std::size(buffer) - 1];
        while (*(p - 1) != '\n') --p;
        g_max_orderkey = 0;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9');
            g_max_orderkey = g_max_orderkey * 10 + (*p - '0');
            ++p;
        }
        DEBUG("g_max_orderkey: %u", g_max_orderkey);
    }

    // Initialize g_mapping_buffers
    {
        DEBUG("CONFIG_LOAD_BUFFER_COUNT: %u", CONFIG_LOAD_BUFFER_COUNT);

        DEBUG("MAPPING_BUFFER_SIZE: 0x%x (%u)", MAPPING_BUFFER_SIZE, MAPPING_BUFFER_SIZE);
        ASSERT(MAPPING_BUFFER_SIZE % sysconf(_SC_PAGE_SIZE) == 0);

        g_mapping_buffer_start_ptr = my_mmap(
            (size_t)MAPPING_BUFFER_SIZE * CONFIG_LOAD_BUFFER_COUNT,
            PROT_READ | PROT_WRITE,  // TODO!!!
            MAP_PRIVATE | MAP_ANONYMOUS | (ENABLE_LOAD_FILE_USE_PREAD ? MAP_POPULATE : 0),
            -1,
            0);
        CHECK(g_mapping_buffer_start_ptr != MAP_FAILED, "mmap() failed");
        DEBUG("g_mapping_buffer_start_ptr: %p", g_mapping_buffer_start_ptr);

        g_mapping_buffers.init([](const size_t idx) -> size_t {
            ASSERT(idx < CONFIG_LOAD_BUFFER_COUNT);
            return idx;
        }, CONFIG_LOAD_BUFFER_COUNT);

#if CONFIG_TOPN_DATES_PER_PLATE > 0
        g_pretopn_buffers.init([&](const uint32_t idx) {
            ASSERT(idx < CONFIG_PRETOPN_BUFFER_COUNT);
            return idx;
        }, CONFIG_PRETOPN_BUFFER_COUNT);
#endif
    }

    // Initialize g_mmap_store_items_context
    {
        g_pwrite_store_items_context = new worker_pwrite_store_items_context[g_worker_sync_barrier.thread_count()];
        CHECK(g_pwrite_store_items_context != nullptr);
    }
}
