#include "common.h"

//==============================================================================
// Structures
//==============================================================================
struct mapped_file_part_overlapped_t
{
    uint64_t file_offset;
    uint32_t desired_size;
    uint32_t map_size;
    int fd;

    [[nodiscard]] __always_inline bool is_first() const noexcept { return file_offset == 0; }
    [[nodiscard]] __always_inline bool is_last() const noexcept { return (desired_size == map_size); }
};

struct order_t
{
    std::make_unsigned_t<date_t> orderdate : 12;
    uint8_t mktid : 4;
};
static_assert(sizeof(order_t) == 2);


//==============================================================================
// Global Variables
//==============================================================================
namespace
{
    constexpr const uint32_t TXT_MAPPING_BUFFER_SIZE = std::max({
        CONFIG_CUSTOMER_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE,
        CONFIG_ORDERS_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE,
        CONFIG_LINEITEM_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE
    });

    void* g_txt_mapping_buffer_start_ptr = nullptr;  // [CONFIG_LOAD_TXT_BUFFER_COUNT][TXT_MAPPING_BUFFER_SIZE]
    mapped_file_part_overlapped_t g_txt_mapping_buffers[CONFIG_LOAD_TXT_BUFFER_COUNT];

    spsc_bounded_bag<index32_t, CONFIG_LOAD_TXT_BUFFER_COUNT> g_txt_mapping_bag { };

    spsc_queue<index32_t, CONFIG_LOAD_TXT_BUFFER_COUNT> g_customer_mapping_queue { };
    spsc_queue<index32_t, CONFIG_LOAD_TXT_BUFFER_COUNT> g_orders_mapping_queue { };
    spsc_queue<index32_t, CONFIG_LOAD_TXT_BUFFER_COUNT> g_lineitem_mapping_queue { };

    uint32_t g_max_custkey = 0;
    uint32_t g_max_orderkey = 0;

    posix_shm_t<uint8_t> g_custkey_to_mktid { };
    posix_shm_t<order_t> g_orderkey_to_order { };
    posix_shm_t<uint32_t> g_orderkey_to_custkey { };

    void* g_items_buffer_major_start_ptr = nullptr;  // [index_tls_buffer_count][CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR]
    void* g_items_buffer_minor_start_ptr = nullptr;  // [index_tls_buffer_count][CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR]

    std::atomic_uint64_t* g_buckets_endoffset_major = nullptr;  // [g_shared->total_buckets]
    std::atomic_uint64_t* g_buckets_endoffset_minor = nullptr;  // [g_shared->total_buckets]
}



template<uint32_t _PartBodySize, uint32_t _PartOverlap, bool _ClearPageCache, size_t _LoadBufferCount>
static void load_file_overlapped(
    /*in*/ [[maybe_unused]] sync_barrier& barrier,
    /*inout*/ std::atomic_size_t& shared_offset,  // it should be 0!
    /*in*/ [[maybe_unused]] const uint32_t tid,
    /*in*/ const size_t size,
    /*in*/ const int fd,
    /*inout*/ spsc_queue<index32_t, _LoadBufferCount>& queue,
    const uint64_t max_clear_cache_offset)
{
    DEBUG("[loader:%u] load_file_overlapped() starts: fd=%d", tid, fd);

    ASSERT(fd > 0);

#if ENABLE_ASSERTION
    barrier.run_once_and_sync([&]() {
        ASSERT(queue.capacity() > 0);
        ASSERT(shared_offset.load() == 0);
    });
#endif

    while (true) {
        const uint64_t off = shared_offset.fetch_add(_PartBodySize);
        if (off >= size) {
            queue.mark_push_finish(/*max_consumer_threads=1*/);
            break;
        }

        index32_t part_index;
        g_txt_mapping_bag.take(&part_index);
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);

        [[maybe_unused]] uint64_t clear_cache_file_offset = 0;
        [[maybe_unused]] uint64_t clear_cache_size = 0;
        if constexpr (_ClearPageCache) {
            if (part.fd == fd && off < max_clear_cache_offset) {
                clear_cache_file_offset = part.file_offset;
                clear_cache_size = part.desired_size;
            }
            part.fd = fd;
        }

        part.file_offset = off;
        if (__unlikely(off + _PartBodySize + _PartOverlap >= size)) {  // this is last
            part.desired_size = size - off;
            part.map_size = size - off;
        }
        else {
            part.desired_size = _PartBodySize;
            part.map_size = _PartBodySize + _PartOverlap;
        }

        void* const return_ptr = mmap(
            ptr,
            part.map_size,
            PROT_READ,
            MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
            fd,
            off);
        CHECK(return_ptr != MAP_FAILED);
        ASSERT(return_ptr == ptr);

        TRACE("[loader:%u] load_file_overlapped(): fd=%d mapped to %p (size=%lu, offset=%lu, _DesiredSize=%u, map_size=%u)",
            tid, fd, ptr, size, off, part.desired_size, part.map_size);
        queue.push(part_index);

        if constexpr (_ClearPageCache) {
            if (clear_cache_size > 0) {
                __fadvice_dont_need(fd, clear_cache_file_offset, clear_cache_size);
            }
        }

        //C_CALL(sched_yield());
    }

    DEBUG("[loader:%u] load_file_overlapped() done: fd=%d", tid, fd);
}


template<char _Delimiter>
__always_inline
uint32_t __parse_u32(const char* s) noexcept
{
    uint32_t result = 0;
    do {
        ASSERT(*s >= '0' && *s <= '9', "Unexpected char: %c", *s);
        result = result * 10 + (*s - '0');
        ++s;
    } while (*s != _Delimiter);
    return result;
}

__always_inline
constexpr uint32_t __u32_length(uint32_t n) noexcept
{
    uint32_t len = 0;
    do {
        ++len;
        n /= 10;
    } while (n > 0);
    return len;
}
static_assert(__u32_length(0) == 1);
static_assert(__u32_length(1) == 1);
static_assert(__u32_length(2) == 1);
static_assert(__u32_length(9) == 1);
static_assert(__u32_length(10) == 2);
static_assert(__u32_length(99) == 2);
static_assert(__u32_length(100) == 3);
static_assert(__u32_length(999) == 3);
static_assert(__u32_length(1000) == 4);


void fn_loader_thread_create_index([[maybe_unused]] const uint32_t tid) noexcept
{
    DEBUG("[loader:%u] fn_loader_thread_create_index() starts", tid);

    // Load customer file to memory
    {
#if ENABLE_SHM_CACHE_TXT
        // Do nothing
        ASSERT(g_customer_shm.shmid >= 0);
        ASSERT(g_customer_shm.ptr != nullptr);

#else
        load_file_overlapped<
                CONFIG_CUSTOMER_PART_BODY_SIZE,
                CONFIG_PART_OVERLAPPED_SIZE,
                /*_ClearPageCache*/false>(
            g_shared->loader_sync_barrier,
            g_shared->customer_file_shared_offset,
            tid,
            g_customer_file.file_size,
            g_customer_file.fd,
            g_customer_mapping_queue,
            /*dummy*/ 0);
#endif
    }


    // Load orders file to memory
    {
#if ENABLE_SHM_CACHE_TXT
        // Do nothing
        ASSERT(g_orders_shm.shmid >= 0);
        ASSERT(g_orders_shm.ptr != nullptr);

#else
        // TODO: adjust this!
        const uint64_t clear_size = g_orders_file.file_size;
//        const uint64_t clear_size = std::min<uint64_t>(
//            g_orders_file.file_size,
//            sizeof(uint32_t) * g_max_orderkey * 6);
        //TODO: const uint32_t free_mem = mem_get_free_bytes();
        // Make use of free memory to reduce page cache clearing?

        load_file_overlapped<
            CONFIG_ORDERS_PART_BODY_SIZE,
            CONFIG_PART_OVERLAPPED_SIZE,
            /*_ClearPageCache*/true>(
            g_shared->loader_sync_barrier,
            g_shared->orders_file_shared_offset,
            tid,
            g_orders_file.file_size,
            g_orders_file.fd,
            g_orders_mapping_queue,
            clear_size);
#endif
    }


    // Load lineitem file to memory
    {
#if ENABLE_SHM_CACHE_TXT
        // Do nothing
        ASSERT(g_lineitem_shm.shmid >= 0);
        ASSERT(g_lineitem_shm.ptr != nullptr);

#else
        load_file_overlapped<
            CONFIG_LINEITEM_PART_BODY_SIZE,
            CONFIG_PART_OVERLAPPED_SIZE,
            /*_ClearPageCache*/false>(
            g_shared->loader_sync_barrier,
            g_shared->lineitem_file_shared_offset,
            tid,
            g_lineitem_file.file_size,
            g_lineitem_file.fd,
            g_lineitem_mapping_queue,
            /*dummy*/ 0);
#endif
    }


    // Save mktsegment to index
    {
        if (tid == 0) {
            ASSERT(g_shared->mktid_count > 0);

            std::string buf;
            buf.reserve(64);
            buf += (char)g_shared->mktid_count;
            for (uint8_t mktid = 0; mktid < g_shared->mktid_count; ++mktid) {
                buf += (char)g_shared->all_mktsegments[mktid].length;
                buf += std::string_view(g_shared->all_mktsegments[mktid].name, g_shared->all_mktsegments[mktid].length);
            }

            const int fd = C_CALL(openat(
                g_index_directory_fd,
                "mktsegment",
                O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
                0666));
            size_t cnt = C_CALL(pwrite(fd, buf.data(), buf.length(), 0));
            CHECK(cnt == buf.length());

            C_CALL(close(fd));
        }
    }
}



static void worker_load_customer_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
#if ENABLE_SHM_CACHE_TXT
    std::atomic_uint64_t& shared_offset = g_shared->customer_file_shared_offset;
    #if ENABLE_ASSERTION
    g_shared->worker_sync_barrier.run_once_and_sync([&]() {
        ASSERT(shared_offset.load() == 0);
    });
    #endif

    ASSERT(g_customer_shm.shmid >= 0);

    ASSERT(g_customer_shm.size_in_byte == g_customer_file.file_size);
    const uint64_t file_size = g_customer_shm.size_in_byte;

    void* const base_ptr = g_customer_shm.ptr;
    ASSERT(base_ptr != nullptr);

#endif

    while (true) {

#if ENABLE_SHM_CACHE_TXT
        const uint64_t off = shared_offset.fetch_add(CONFIG_CUSTOMER_PART_BODY_SIZE);
        if (off >= file_size) break;

        mapped_file_part_overlapped_t part; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
        part.file_offset = off;
        if (__unlikely(off + CONFIG_CUSTOMER_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE >= file_size)) {  // this is last
            part.desired_size = file_size - off;
            part.map_size = file_size - off;
        }
        else {
            part.desired_size = CONFIG_CUSTOMER_PART_BODY_SIZE;
            part.map_size = CONFIG_CUSTOMER_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE;
        }

        void* const ptr = (void*)((uintptr_t)base_ptr + off);

#else  // !ENABLE_SHM_CACHE_TXT
        index32_t part_index;
        if (!g_customer_mapping_queue.pop(&part_index)) break;
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
#endif
        const char* p = (const char*)ptr;
        const char* end = p + part.desired_size;

        // Get beginning custkey -> last_custkey
        uint32_t last_custkey;
        uint32_t last_custkey_length;
        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;  // skip '\n'

            last_custkey = __parse_u32<'|'>(p);
            if (last_custkey % 3 == 0) {
                while (*p != '\n') ++p;
                ++p;  // skip '\n'
                ++last_custkey;
            }
            else if (last_custkey % 3 == 2) {
                while (*p != '\n') ++p;
                ++p;  // skip '\n'
                ++last_custkey;

                while (*p != '\n') ++p;
                ++p;  // skip '\n'
                ++last_custkey;
            }
            else {  // last_custkey % 3 == 1
                // Do nothing
            }

            last_custkey_length = __u32_length(last_custkey);
        }
        else {  // part.is_first()
            last_custkey = 1;
            last_custkey_length = 1;
        }
        ASSERT(last_custkey_length > 0);

        ASSERT(last_custkey % 3 == 1);
        ASSERT(last_custkey > 0);
        ASSERT(last_custkey <= g_max_custkey, "Unexpected last_custkey (too large): %u", last_custkey);
        --last_custkey;

        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;  // skip '\n'

            const uint32_t to_custkey = __parse_u32<'|'>(end);
            if (to_custkey % 3 == 1) {
                // Do nothing
            }
            else if (to_custkey % 3 == 2) {
                while (*end != '\n') ++end;
                ++end;  // skip '\n'
                while (*end != '\n') ++end;
                ++end;  // skip '\n'
            }
            else {  // to_custkey % 3 == 0
                while (*end != '\n') ++end;
                ++end;  // skip '\n'
            }

#if ENABLE_ASSERTION
            const uint32_t new_to_custkey = __parse_u32<'|'>(end);
            ASSERT(new_to_custkey % 3 == 1);
#endif
        }

        TRACE("[%u] load customer: [%p, %p)", tid, p, end);
#if ENABLE_ASSERTION
        ++g_shared->customer_file_loaded_parts;
#endif

        uint32_t write_offset = last_custkey / 3;
        while (p < end) {
            ASSERT(last_custkey % 3 == 0);
            uint8_t write_value = 0;

            #pragma GCC unroll 3
            for (uint32_t inner = 0; inner < 3; ++inner) {
                if (__unlikely(p >= end)) {
                    ASSERT(p == end);
                    break;
                }

                const uint32_t custkey = ++last_custkey;
                ASSERT(custkey >= 1);
                ASSERT(custkey <= g_max_custkey, "Unexpected custkey (too large): %u", custkey);
#if ENABLE_ASSERTION
                const char* tmp_p = p;
                uint32_t tmp_custkey = 0;
                while (*tmp_p != '|') {
                    ASSERT(*tmp_p >= '0' && *tmp_p <= '9', "Unexpected char: %c", *tmp_p);
                    tmp_custkey = tmp_custkey * 10 + (*tmp_p - '0');
                    ++tmp_p;
                }
                ASSERT(custkey == tmp_custkey);
#endif
                p += last_custkey_length;
                if (__unlikely(*p != '|')) {
                    ++p;
                    ASSERT(*p == '|', "custkey: %u, *p = %c", custkey, *p);
                    ++last_custkey_length;
                }
                ++p;  // skip '|'

                const char* const mktsegment_start = p;
                while (*p != '\n') {
                    ++p;
                }
                const char* const mktsegment_end = p;
                ++p;  // skip '\n'

                //std::string_view mktsegment_view(mktsegment_start, mktsegment_end - mktsegment_start);

                const auto try_find_mktsegment = [&]() -> bool {
                    for (int8_t mktid = g_shared->mktid_count - 1; mktid >= 0; --mktid) {
                        if (g_shared->all_mktsegments[mktid].name[0] != *mktsegment_start) continue;
                        //if (std::string_view(g_shared->all_mktsegments[mktid].name, g_shared->all_mktsegments[mktid].length) == mktsegment_view) {
                            ASSERT(mktid < (1 << 4));
                            //write_value |= mktid << (inner * 4);
                            write_value = write_value * 5 + mktid;
                            return true;
                        //}
                    }
                    return false;
                };

                if (!try_find_mktsegment()) {
                    std::unique_lock<decltype(g_shared->all_mktsegments_insert_mutex)> lock(
                        g_shared->all_mktsegments_insert_mutex);
                    if (!try_find_mktsegment()) {
                        const uint32_t new_mktid = g_shared->mktid_count++;
                        std::atomic_thread_fence(std::memory_order_seq_cst);
                        ASSERT(new_mktid < (1 << 4));
                        //write_value |= new_mktid << (inner * 4);
                        write_value = write_value * 5 + new_mktid;

                        g_shared->all_mktsegments[new_mktid].length = (uint8_t)(mktsegment_end - mktsegment_start);
                        strncpy(
                            g_shared->all_mktsegments[new_mktid].name,
                            mktsegment_start,
                            mktsegment_end - mktsegment_start);
                        INFO("found new mktsegment: custkey = %u, %.*s -> %u",
                             custkey,
                             (int)g_shared->all_mktsegments[new_mktid].length,
                             g_shared->all_mktsegments[new_mktid].name,
                             new_mktid);

                        g_shared->total_buckets = g_shared->mktid_count * BUCKETS_PER_MKTID;
                        g_shared->buckets_per_holder = __div_up(g_shared->total_buckets, CONFIG_INDEX_HOLDER_COUNT);
                        g_shared->total_plates = g_shared->mktid_count * PLATES_PER_MKTID;
                    }
                }
            }

            //ASSERT(last_custkey % 3 == 0);
            ASSERT(write_offset == (last_custkey - 1) / 3);
            g_custkey_to_mktid.ptr[write_offset++] = write_value;
        }

#if ENABLE_SHM_CACHE_TXT
#else  // !ENABLE_SHM_CACHE_TXT
        g_txt_mapping_bag.return_back(part_index);
#endif  // ENABLE_SHM_CACHE_TXT
    }

    INFO("[%u] done worker_load_customer_multi_part()", tid);
}


static void worker_load_orders_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
    [[maybe_unused]] const uint8_t mktid_count = g_shared->mktid_count;

#if ENABLE_SHM_CACHE_TXT
    std::atomic_uint64_t& shared_offset = g_shared->orders_file_shared_offset;
    #if ENABLE_ASSERTION
    g_shared->worker_sync_barrier.run_once_and_sync([&]() {
        ASSERT(shared_offset.load() == 0);
    });
    #endif

    ASSERT(g_orders_shm.shmid >= 0);

    void* const base_ptr = g_orders_shm.ptr;
    ASSERT(base_ptr != nullptr);

    ASSERT(g_orders_shm.size_in_byte == g_orders_file.file_size);

    const uint64_t file_size = g_orders_shm.size_in_byte;
#endif

    while (true) {

#if ENABLE_SHM_CACHE_TXT
        const uint64_t off = shared_offset.fetch_add(CONFIG_ORDERS_PART_BODY_SIZE);
        if (off >= file_size) break;

        mapped_file_part_overlapped_t part; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
        part.file_offset = off;
        if (__unlikely(off + CONFIG_ORDERS_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE >= file_size)) {  // this is last
            part.desired_size = file_size - off;
            part.map_size = file_size - off;
        }
        else {
            part.desired_size = CONFIG_ORDERS_PART_BODY_SIZE;
            part.map_size = CONFIG_ORDERS_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE;
        }

        void* const ptr = (void*)((uintptr_t)base_ptr + off);

#else  // !ENABLE_SHM_CACHE_TXT
        index32_t part_index;
        if (!g_orders_mapping_queue.pop(&part_index)) break;
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
#endif  // ENABLE_SHM_CACHE_TXT

        const char* p = (const char*)ptr;
        const char* end = p + part.desired_size;

        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;
        }
        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;
        }

        TRACE("[%u] load orders: [%p, %p)", tid, p, end);

        // Get beginning orderkey -> last_orderkey
        uint32_t last_orderkey = 0;
        const char* tmp_p = p;
        while (*tmp_p != '|') {
            ASSERT(*tmp_p >= '0' && *tmp_p <= '9', "Unexpected char: %c", *tmp_p);
            last_orderkey = last_orderkey * 10 + (*tmp_p - '0');
            ++tmp_p;
        }
        ASSERT((last_orderkey & 0b11000) == 0);
        last_orderkey = (last_orderkey & 0b111) | ((last_orderkey >> 2) & ~0b1);
        ASSERT(last_orderkey > 0);
        ASSERT(last_orderkey <= g_max_orderkey, "Unexpected last_orderkey (too large): %u", last_orderkey);

        uint32_t last_orderkey_length = (tmp_p - p);
        ASSERT(last_orderkey_length > 0);

        --last_orderkey;

        while (p < end) {
            const uint32_t orderkey = ++last_orderkey;
            ASSERT(orderkey >= 1);
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);
#if ENABLE_ASSERTION
            const char* tmp_p = p;
            uint32_t tmp_orderkey = 0;
            while (*tmp_p != '|') {
                ASSERT(*tmp_p >= '0' && *tmp_p <= '9', "Unexpected char: %c", *tmp_p);
                tmp_orderkey = tmp_orderkey * 10 + (*tmp_p - '0');
                ++tmp_p;
            }
            ASSERT((tmp_orderkey & 0b11000) == 0);
            tmp_orderkey = (tmp_orderkey & 0b111) | ((tmp_orderkey >> 2) & ~0b1);
            ASSERT(orderkey == tmp_orderkey);
#endif
            p += last_orderkey_length;
            if (__unlikely(*p != '|')) {
                ++p;
                ASSERT(*p == '|', "orderkey: %u, *p = %c", orderkey, *p);
                ++last_orderkey_length;
            }
            ++p;  // skip '|'

            uint32_t custkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                custkey = custkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(custkey >= 1);
            ASSERT(custkey <= g_max_custkey, "Unexpected custkey (too large): %u", custkey);
            ASSERT(g_orderkey_to_custkey.ptr[orderkey] == 0);
            g_orderkey_to_custkey.ptr[orderkey] = custkey;

            const date_t orderdate = date_from_string<true>(p);
            ASSERT(orderdate >= MIN_TABLE_DATE);
            ASSERT(orderdate <= MAX_TABLE_DATE);
            p += 10;  // skip 'yyyy-MM-dd'

            // Save to orderkey_to_order
            ASSERT(orderdate < (1 << 12), "BUG: orderdate too large?");
#if ENABLE_ASSERTION
            g_orderkey_to_order.ptr[orderkey].orderdate = (std::make_unsigned_t<date_t>)(orderdate);
            g_orderkey_to_order.ptr[orderkey].mktid = mktid_count;  // Set a posion value
#else
            g_orderkey_to_order.ptr[orderkey] = { (std::make_unsigned_t<date_t>)(orderdate), /*dummy*/0 };
#endif

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'
        }

#if ENABLE_ASSERTION
        if (part.is_last()) {
            ASSERT(last_orderkey == g_max_orderkey);
        }
#endif

#if ENABLE_SHM_CACHE_TXT
#else
        g_txt_mapping_bag.return_back(part_index);
#endif
    }

    INFO("[%u] done worker_load_orders_multi_part()", tid);
}


static void worker_load_orders_custkey_from_orderkey(
    /*in*/ [[maybe_unused]] const uint32_t tid,
    /*inout*/ std::atomic_uint64_t& shared_counter) noexcept
{
#if ENABLE_ASSERTION
    g_shared->worker_sync_barrier.run_once_and_sync([&]() {
        ASSERT(shared_counter.load() == 0);
    });
#endif

    static constexpr const uint32_t STEP_SIZE = 1048576;
    while (true) {
        const uint32_t from_orderkey = std::max<uint32_t>(shared_counter.fetch_add(STEP_SIZE), 1);
        if (from_orderkey > g_max_orderkey) break;
        const uint32_t to_orderkey = std::min<uint32_t>(from_orderkey + STEP_SIZE - 1, g_max_orderkey);

        //TODO: #pragma GCC unroll 4
        for (uint32_t orderkey = from_orderkey; orderkey <= to_orderkey; ++orderkey) {
            const uint32_t custkey = g_orderkey_to_custkey.ptr[orderkey];
            ASSERT(custkey >= 1);
            ASSERT(custkey <= g_max_custkey);

            const uint8_t mktid_embed = g_custkey_to_mktid.ptr[(custkey - 1) / 3];

            uint8_t mktid;
            if (custkey % 3 == 1) {
                mktid = mktid_embed / 25;
            }
            else if (custkey % 3 == 2) {
                mktid = (mktid_embed / 5) % 5;
            }
            else {  // custkey % 3 == 0
                mktid = mktid_embed % 5;
            }
            ASSERT(mktid < 5, "Boom! mktid %u too large", mktid);
            ASSERT(mktid < g_shared->mktid_count,
                "Expect mktid < g_shared->mktid_count (%u < %u)", mktid, g_shared->mktid_count);

            g_orderkey_to_order.ptr[orderkey].mktid = mktid;
        }
    }

    INFO("[%u] done worker_load_orders_custkey_from_orderkey()", tid);
}


void debug_check_orders()
{
    INFO("[%u] debug_check_orders() starts", g_id);

    const uint8_t mktid_count = g_shared->mktid_count;

    for (uint32_t orderkey = 1; orderkey <= g_max_orderkey; ++orderkey) {
        const order_t order = g_orderkey_to_order.ptr[orderkey];
        CHECK(order.orderdate >= MIN_TABLE_DATE);
        CHECK(order.orderdate <= MAX_TABLE_DATE);

        CHECK(order.mktid < mktid_count, "orderkey: %u, mktid: %u", orderkey, order.mktid);
    }

    INFO("[%u] done debug_check_orders()", g_id);
}




__always_inline
const char* skip_one_orderkey_in_lineitem(const char* p) noexcept
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
        ASSERT((orderkey & 0b11000) == 0);
        orderkey = (orderkey & 0b111) | ((orderkey >> 2) & ~0b1);
        ASSERT(orderkey > 0);
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
    ASSERT(g_orderkey_to_order.ptr != nullptr);
    const order_t* const orderkey_to_order = g_orderkey_to_order.ptr;

    ASSERT(g_shared->mktid_count > 0);
    const uint32_t mktid_count = g_shared->mktid_count;

    uint32_t max_orderdate_shipdate_diff = 0;

    //
    // Allocate for g_items_buffer_major_start_ptr, g_items_buffer_minor_start_ptr
    //
    {
        ASSERT(g_shared->total_buckets > 0);
        const uint64_t index_tls_buffer_count =
            (uint64_t)g_shared->mktid_count *
            __div_up(MAX_TABLE_DATE - MIN_TABLE_DATE + 1, CONFIG_ORDERDATES_PER_BUCKET);
        DEBUG("index_tls_buffer_count: %lu", index_tls_buffer_count);

        g_items_buffer_major_start_ptr = mmap_allocate_page2m(index_tls_buffer_count * CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR);
        ASSERT(g_items_buffer_major_start_ptr != nullptr);
        DEBUG("[%u] g_items_buffer_major_start_ptr: %p", tid, g_items_buffer_major_start_ptr);

        // Allocate for g_items_buffer_minor_start_ptr
        g_items_buffer_minor_start_ptr = mmap_allocate_page2m(index_tls_buffer_count * CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR);
        ASSERT(g_items_buffer_minor_start_ptr != nullptr);
        DEBUG("[%u] g_items_buffer_minor_start_ptr: %p", tid, g_items_buffer_minor_start_ptr);
    }


    // Truncate for g_endoffset_file
    {
        ASSERT(g_shared->total_buckets > 0);

        ASSERT(g_endoffset_file_major.fd > 0);
        ASSERT(g_endoffset_file_minor.fd > 0);

        g_endoffset_file_major.file_size = sizeof(uint64_t) * g_shared->total_buckets;
        g_endoffset_file_minor.file_size = sizeof(uint64_t) * g_shared->total_buckets;
        ASSERT(g_endoffset_file_major.file_size > 0);
        ASSERT(g_endoffset_file_minor.file_size > 0);

        g_shared->worker_sync_barrier.run_once_and_sync([]() {
            C_CALL(ftruncate(g_endoffset_file_major.fd, g_endoffset_file_major.file_size));
            C_CALL(ftruncate(g_endoffset_file_minor.fd, g_endoffset_file_minor.file_size));
        });

        g_buckets_endoffset_major = (std::atomic_uint64_t*)my_mmap(
            g_endoffset_file_major.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_endoffset_file_major.fd,
            0);
        DEBUG("[%u] g_buckets_endoffset_major: %p", tid, g_buckets_endoffset_major);

        g_buckets_endoffset_minor = (std::atomic_uint64_t*)my_mmap(
            g_endoffset_file_minor.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_endoffset_file_minor.fd,
            0);
        DEBUG("[%u] g_buckets_endoffset_minor: %p", tid, g_buckets_endoffset_minor);
    }

    //
    // Truncate for holder files
    //
    {
        ASSERT(g_shared->buckets_per_holder > 0);

        const uint64_t holder_major_file_size = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * g_shared->buckets_per_holder;
        while (true) {
            const uint32_t holder_id = g_shared->next_truncate_holder_major_id++;
            if (holder_id >= CONFIG_INDEX_HOLDER_COUNT) break;

            ASSERT(g_holder_files_major_fd[holder_id] > 0);
            C_CALL(ftruncate(g_holder_files_major_fd[holder_id], holder_major_file_size));

            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;
            const uint32_t end_bucket_id = (holder_id + 1) * g_shared->buckets_per_holder;
            for (uint32_t bucket_id = begin_bucket_id; bucket_id < end_bucket_id; ++bucket_id) {
                g_buckets_endoffset_major[bucket_id] = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - begin_bucket_id);
            }
        }

        const uint64_t holder_minor_file_size = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * g_shared->buckets_per_holder;
        while (true) {
            const uint32_t holder_id = g_shared->next_truncate_holder_minor_id++;
            if (holder_id >= CONFIG_INDEX_HOLDER_COUNT) break;

            ASSERT(g_holder_files_minor_fd[holder_id] > 0);
            C_CALL(ftruncate(g_holder_files_minor_fd[holder_id], holder_minor_file_size));

            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;
            const uint32_t end_bucket_id = (holder_id + 1) * g_shared->buckets_per_holder;
            for (uint32_t bucket_id = begin_bucket_id; bucket_id < end_bucket_id; ++bucket_id) {
                g_buckets_endoffset_minor[bucket_id] = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - begin_bucket_id);
            }
        }
    }


#define _CALC_START_PTR_MAJOR(_BufferIndex) \
    ((void*)((uintptr_t)g_items_buffer_major_start_ptr + (size_t)(_BufferIndex) * CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR))
#define _CALC_START_PTR_MINOR(_BufferIndex) \
    ((void*)((uintptr_t)g_items_buffer_minor_start_ptr + (size_t)(_BufferIndex) * CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR))

    iovec* const bucket_data_major = new iovec[g_shared->total_buckets];
    for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
        bucket_data_major[bucket_id].iov_len = 0;
        bucket_data_major[bucket_id].iov_base = _CALC_START_PTR_MAJOR(bucket_id);
    }
    iovec* const bucket_data_minor = new iovec[g_shared->total_buckets];
    for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
        bucket_data_minor[bucket_id].iov_len = 0;
        bucket_data_minor[bucket_id].iov_base = _CALC_START_PTR_MINOR(bucket_id);
    }


    const auto maybe_submit_for_pwrite_major = [&](/*inout*/ iovec& vec, /*in*/ uint32_t bucket_id) {
        //ASSERT(bucket_id < index_tls_buffer_count, "bucket_id: %u", bucket_id);
        ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR);
        ASSERT(vec.iov_len > 0);

        // Do pwrite
        const uint64_t file_offset = g_buckets_endoffset_major[bucket_id].fetch_add(vec.iov_len);
        // If not final writing:
        //  ASSERT(vec.iov_len == CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR);
        //  ASSERT(file_offset % CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR == 0, "bucket_id: %u, file_offset: %lu", bucket_id, file_offset);
        const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
        const size_t cnt = C_CALL(pwrite(
            g_holder_files_major_fd[holder_id],
            vec.iov_base,
            vec.iov_len,
            file_offset));
        CHECK(cnt == vec.iov_len);

        // Fetch new last_data
        vec.iov_len = 0;
    };

    const auto maybe_submit_for_pwrite_minor = [&](/*inout*/ iovec& vec, /*in*/ uint32_t bucket_id) {
        //ASSERT(bucket_id < index_tls_buffer_count, "bucket_id: %u", bucket_id);
        ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR);
        ASSERT(vec.iov_len > 0);

        // Do pwrite
        const uint64_t file_offset = g_buckets_endoffset_minor[bucket_id].fetch_add(vec.iov_len);
        const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
        const size_t cnt = C_CALL(pwrite(
            g_holder_files_minor_fd[holder_id],
            vec.iov_base,
            vec.iov_len,
            file_offset));
        CHECK(cnt == vec.iov_len);

        // Fetch new last_data
        vec.iov_len = 0;
    };

    static constexpr const uint32_t COUNT_BASE = 8;
    uint32_t last_items[COUNT_BASE + 8];  // start index: COUNT_BASE
    for (uint32_t i = 0; i < COUNT_BASE; ++i) {
        last_items[i] = (/*dummy*/0 << 24) | 0x00000000;
    }

    
#if ENABLE_SHM_CACHE_TXT
    std::atomic_uint64_t& shared_offset = g_shared->lineitem_file_shared_offset;
    #if ENABLE_ASSERTION
    g_shared->worker_sync_barrier.run_once_and_sync([&]() {
        ASSERT(shared_offset.load() == 0);
    });
    #endif

    ASSERT(g_lineitem_shm.shmid >= 0);

    ASSERT(g_lineitem_shm.size_in_byte == g_lineitem_file.file_size);
    const uint64_t file_size = g_lineitem_shm.size_in_byte;

    void* const base_ptr = g_lineitem_shm.ptr;
    ASSERT(base_ptr != nullptr);
#endif  // ENABLE_SHM_CACHE_TXT

    while (true) {

#if ENABLE_SHM_CACHE_TXT
        const uint64_t off = shared_offset.fetch_add(CONFIG_LINEITEM_PART_BODY_SIZE);
        if (off >= file_size) break;

        mapped_file_part_overlapped_t part; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
        part.file_offset = off;
        if (__unlikely(off + CONFIG_LINEITEM_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE >= file_size)) {  // this is last
            part.desired_size = file_size - off;
            part.map_size = file_size - off;
        }
        else {
            part.desired_size = CONFIG_LINEITEM_PART_BODY_SIZE;
            part.map_size = CONFIG_LINEITEM_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE;
        }

        void* const ptr = (void*)((uintptr_t)base_ptr + off);

#else  // !ENABLE_SHM_CACHE_TXT
        index32_t part_index;
        if (!g_lineitem_mapping_queue.pop(&part_index)) break;
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
#endif
        const char* p = (const char*)ptr;
        const char* valid_end = p + part.desired_size;

        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;

            p = skip_one_orderkey_in_lineitem(p);
        }

        if (!part.is_last()) {
            while (*valid_end != '\n') ++valid_end;
            ++valid_end;

            valid_end = skip_one_orderkey_in_lineitem(valid_end);
        }

        TRACE("[%u] load lineitem: [%p, %p)", tid, p, valid_end);

        uint32_t last_item_count = COUNT_BASE;  // 8 heading zeros
        uint32_t last_orderkey;
        uint32_t last_bucket_id;
        date_t last_orderdate;
        date_t last_bucket_base_orderdate;


        const auto append_current_order_to_index = [&]() {
            ASSERT(last_item_count >= COUNT_BASE + 1);
            ASSERT(last_item_count <= COUNT_BASE + 7);
            last_items[last_item_count++] = (last_orderdate - last_bucket_base_orderdate) << 30 | last_orderkey;

            if (last_item_count >= COUNT_BASE + 5) {  // 8,7,6,5
                ASSERT(last_item_count <= COUNT_BASE + 8);

                iovec& vec = bucket_data_major[last_bucket_id];
                ASSERT(vec.iov_len < CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR);
                ASSERT(vec.iov_base == _CALC_START_PTR_MAJOR(last_bucket_id));
                static_assert(CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR % (8 * sizeof(uint32_t)) == 0);

                memcpy(
                    (void*)((uintptr_t)vec.iov_base + vec.iov_len),
                    last_items + last_item_count - 8,
                    8 * sizeof(uint32_t));
                vec.iov_len += 8 * sizeof(uint32_t);

                ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR);
                if (vec.iov_len == CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR) {
                    maybe_submit_for_pwrite_major(vec, last_bucket_id);
                }
            }
            else {  // 4,3,2
                ASSERT(last_item_count < COUNT_BASE + 5);
                ASSERT(last_item_count >= COUNT_BASE + 2);

                iovec& vec = bucket_data_minor[last_bucket_id];
                ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR);
                ASSERT(vec.iov_base == _CALC_START_PTR_MINOR(last_bucket_id));
                static_assert(CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR % (4 * sizeof(uint32_t)) == 0);

                memcpy(
                    (void*)((uintptr_t)vec.iov_base + vec.iov_len),
                    last_items + last_item_count - 4,
                    4 * sizeof(uint32_t));
                vec.iov_len += 4 * sizeof(uint32_t);

                ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR);
                if (vec.iov_len == CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR) {
                    maybe_submit_for_pwrite_minor(vec, last_bucket_id);
                }
            }
        };


        // Get first item
        {
            last_orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                last_orderkey = last_orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT((last_orderkey & 0b11000) == 0);
            last_orderkey = (last_orderkey & 0b111) | ((last_orderkey >> 2) & ~0b1);
            ASSERT(last_orderkey > 0);
            ASSERT(last_orderkey < (1 << 30));
            ASSERT(last_orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", last_orderkey);

            ASSERT(orderkey_to_order != nullptr);
            const order_t last_order = orderkey_to_order[last_orderkey];
            last_orderdate = last_order.orderdate;
            const uint8_t mktid = last_order.mktid;

            ASSERT(mktid < mktid_count, "BUG: expect mktid < mktid_count (%u < %u)", mktid, mktid_count);

            last_bucket_base_orderdate = calc_bucket_base_orderdate(last_orderdate);
            ASSERT(last_orderdate >= last_bucket_base_orderdate);
            ASSERT(last_orderdate < last_bucket_base_orderdate + CONFIG_ORDERDATES_PER_BUCKET);
            //last_orderkey_with_orderdate_diff = last_orderkey | (uint32_t)(last_orderdate - last_bucket_base_orderdate) << 30;

            last_bucket_id = calc_bucket_index(mktid, last_orderdate);

            uint32_t expend_cent = 0;
            while (*p != '.') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                expend_cent = expend_cent * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '.'
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            expend_cent = expend_cent * 10 + (*(p++) - '0');
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            expend_cent = expend_cent * 10 + (*(p++) - '0');
            ASSERT(*p == '|');
            ++p;  // skip '|'
            ASSERT(expend_cent < (1 << 24), "Boom! expend_cent too large");

            const date_t shipdate = date_from_string<true>(p);
            p += 10;  // skip 'yyyy-MM-dd'
            ASSERT(shipdate > last_orderdate, "Expect shipdate > orderdate");
            ASSERT((shipdate - last_orderdate) < (1 << 7),
                "shipdate - orderdate Boom! shipdate=%d, orderdate=%d", (int32_t)shipdate, (int32_t)last_orderdate);
            ASSERT((shipdate - last_bucket_base_orderdate) < (1 << 7),
                "shipdate - orderdate Boom! shipdate=%d, base_orderdate=%d", (int32_t)shipdate, (int32_t)last_bucket_base_orderdate);
            ASSERT(shipdate >= MIN_TABLE_DATE, "Expect shipdate >= MIN_TABLE_DATE");
            ASSERT(shipdate <= MAX_TABLE_DATE, "Expect shipdate <= MAX_TABLE_DATE");
            if (__unlikely(shipdate - last_orderdate) > max_orderdate_shipdate_diff) {
                max_orderdate_shipdate_diff = shipdate - last_orderdate;
            }

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'

            ASSERT(last_orderdate - last_bucket_base_orderdate < CONFIG_ORDERDATES_PER_BUCKET);
            last_items[COUNT_BASE + 0] = ((uint32_t)(shipdate - last_bucket_base_orderdate) << 24) | expend_cent;
            last_item_count = COUNT_BASE + 1;
        }


        while (true) {
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT((orderkey & 0b11000) == 0);
            orderkey = (orderkey & 0b111) | ((orderkey >> 2) & ~0b1);
            ASSERT(orderkey > 0);
            ASSERT(orderkey < (1 << 30));
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);


            uint32_t expend_cent = 0;
            while (*p != '.') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                expend_cent = expend_cent * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '.'
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            expend_cent = expend_cent * 10 + (*(p++) - '0');
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            expend_cent = expend_cent * 10 + (*(p++) - '0');
            ASSERT(*p == '|');
            ++p;  // skip '|'
            ASSERT(expend_cent < (1 << 24), "Boom! expend_cent too large");

            const date_t shipdate = date_from_string<true>(p);
            p += 10;  // skip 'yyyy-MM-dd'

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'

            if (orderkey == last_orderkey) {
                ASSERT(shipdate > last_orderdate, "Expect shipdate > orderdate");
                ASSERT((shipdate - last_orderdate) < (1 << 7),
                    "shipdate - orderdate Boom! shipdate=%d, orderdate=%d", (int32_t)shipdate, (int32_t)last_orderdate);
                ASSERT((shipdate - last_bucket_base_orderdate) < (1 << 7),
                    "shipdate - orderdate Boom! shipdate=%d, base_orderdate=%d", (int32_t)shipdate, (int32_t)last_bucket_base_orderdate);
                ASSERT(shipdate >= MIN_TABLE_DATE, "Expect shipdate >= MIN_TABLE_DATE");
                ASSERT(shipdate <= MAX_TABLE_DATE, "Expect shipdate <= MAX_TABLE_DATE");

                if (__unlikely(shipdate - last_orderdate) > max_orderdate_shipdate_diff) {
                    max_orderdate_shipdate_diff = shipdate - last_orderdate;
                }

                ASSERT(last_item_count >= COUNT_BASE + 1);
                ASSERT(last_item_count < COUNT_BASE + 7);
                last_items[last_item_count++] = ((uint32_t)(shipdate - last_bucket_base_orderdate) << 24) | expend_cent;
            }
            else {  // orderkey != last_orderkey
                // Save current items to index buffer
                append_current_order_to_index();

                ASSERT(orderkey == last_orderkey + 1);
                last_orderkey = orderkey;

                ASSERT(orderkey_to_order != nullptr);
                const order_t last_order = orderkey_to_order[last_orderkey];
                last_orderdate = last_order.orderdate;

                const uint8_t mktid = last_order.mktid;
                ASSERT(mktid < mktid_count, "BUG: expect mktid < mktid_count (%u < %u)", mktid, mktid_count);

                last_bucket_base_orderdate = calc_bucket_base_orderdate(last_orderdate);
                ASSERT(last_orderdate >= last_bucket_base_orderdate);
                ASSERT(last_orderdate < last_bucket_base_orderdate + CONFIG_ORDERDATES_PER_BUCKET);
                //last_orderkey_with_orderdate_diff = last_orderkey | (uint32_t)(last_orderdate - last_bucket_base_orderdate) << 30;

                last_bucket_id = calc_bucket_index(mktid, last_orderdate);

                ASSERT(shipdate > last_orderdate, "Expect shipdate > orderdate");
                ASSERT((shipdate - last_orderdate) < (1 << 7),
                    "shipdate - orderdate Boom! shipdate=%d, orderdate=%d", (int32_t)shipdate, (int32_t)last_orderdate);
                ASSERT((shipdate - last_bucket_base_orderdate) < (1 << 7),
                    "shipdate - orderdate Boom! shipdate=%d, base_orderdate=%d", (int32_t)shipdate, (int32_t)last_bucket_base_orderdate);
                ASSERT(shipdate >= MIN_TABLE_DATE, "Expect shipdate >= MIN_TABLE_DATE");
                ASSERT(shipdate <= MAX_TABLE_DATE, "Expect shipdate <= MAX_TABLE_DATE");
                if (__unlikely(shipdate - last_orderdate) > max_orderdate_shipdate_diff) {
                    max_orderdate_shipdate_diff = shipdate - last_orderdate;
                }

                ASSERT(last_orderdate - last_bucket_base_orderdate < CONFIG_ORDERDATES_PER_BUCKET);
                last_items[COUNT_BASE + 0] = ((uint32_t)(shipdate - last_bucket_base_orderdate) << 24) | expend_cent;
                last_item_count = COUNT_BASE + 1;
            }


            if (__unlikely(p >= valid_end)) {
                ASSERT(p == valid_end);

                // Save current items to index buffer
                append_current_order_to_index();

                break;
            }
        }

#if ENABLE_SHM_CACHE_TXT
#else
        g_txt_mapping_bag.return_back(part_index);
#endif
    }

    // For each bucket, write remaining buffer (if exists) to file
    uint32_t bucket_ids_to_shuffle[g_shared->total_buckets];
    for (uint32_t i = 0; i < g_shared->total_buckets; ++i) {
        bucket_ids_to_shuffle[i] = i;
    }
    std::mt19937 g(tid);
    std::shuffle(bucket_ids_to_shuffle, bucket_ids_to_shuffle + g_shared->total_buckets, g);
    for (uint32_t i = 0; i < g_shared->total_buckets; ++i) {
        const uint32_t bucket_id = bucket_ids_to_shuffle[i];
        iovec& vec_major = bucket_data_major[bucket_id];
        if (vec_major.iov_len > 0) {
            maybe_submit_for_pwrite_major(vec_major, bucket_id);
        }

        iovec& vec_minor = bucket_data_minor[bucket_id];
        if (vec_minor.iov_len > 0) {
            maybe_submit_for_pwrite_minor(vec_minor, bucket_id);
        }
    }

#if ENABLE_ASSERTION
    for (uint32_t i = 0; i < g_shared->total_buckets; ++i) {
        const uint32_t bucket_id = bucket_ids_to_shuffle[i];
        //TODO: ASSERT(bucket_data_major[bucket_id].iov_len == 0);
        ASSERT(bucket_data_minor[bucket_id].iov_len == 0);
    }
#endif


    // Update my max_orderdate_shipdate_diff
    {
        DEBUG("[%u] max_orderdate_shipdate_diff: %u", tid, max_orderdate_shipdate_diff);
        if (*(volatile uint32_t*)&g_shared->meta.max_shipdate_orderdate_diff < max_orderdate_shipdate_diff) {
            std::unique_lock<decltype(g_shared->meta_update_mutex)> lock(g_shared->meta_update_mutex);
            if (*(volatile uint32_t*)&g_shared->meta.max_shipdate_orderdate_diff < max_orderdate_shipdate_diff) {
                g_shared->meta.max_shipdate_orderdate_diff = max_orderdate_shipdate_diff;
            }
        }
    }

    INFO("[%u] done worker_load_lineitem_multi_part()", tid);
}


static void worker_compute_pretopn_for_plate_major(
    /*in*/ const date_t bucket_base_orderdate_minus_plate_base_orderdate,
    /*in*/ const void* const bucket_ptr_major,
    /*in*/ const uint64_t bucket_size_major,
    /*inout*/ uint32_t& topn_count,
    /*inout*/ uint64_t* const topn_ptr) noexcept
{
    ASSERT(bucket_base_orderdate_minus_plate_base_orderdate >= 0);
    ASSERT(bucket_base_orderdate_minus_plate_base_orderdate % CONFIG_ORDERDATES_PER_BUCKET == 0);
    ASSERT((uint32_t)bucket_base_orderdate_minus_plate_base_orderdate < CONFIG_ORDERDATES_PER_BUCKET * BUCKETS_PER_PLATE);
    ASSERT(bucket_ptr_major != nullptr);
    ASSERT((uintptr_t)bucket_ptr_major % PAGE_SIZE == 0);
    ASSERT(bucket_size_major > 0);
    ASSERT(bucket_size_major % (sizeof(uint32_t) * 8) == 0);
    ASSERT(topn_count <= CONFIG_EXPECT_MAX_TOPN);
    ASSERT(topn_ptr != nullptr);
    ASSERT((uintptr_t)topn_ptr % PAGE_SIZE == 0);

#if ENABLE_ASSERTION
    {
        const uint32_t* p = (const uint32_t*)bucket_ptr_major;
        const uint32_t* const end = (const uint32_t*)((uintptr_t)p + bucket_size_major);
        while (p < end) {
            const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
            p += 8;

            ASSERT(orderkey > 0, "bucket_id=%%u, bucket_size_major=%lu, offset=%lu",
            /*bucket_id,*/ bucket_size_major, (end - p) * sizeof(uint32_t));
        }
    }
#endif

    const __m256i expend_mask = _mm256_set_epi32(
        0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
        0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);

    const uint32_t* p = (const uint32_t*)bucket_ptr_major;
    const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr_major + bucket_size_major);
    const uint32_t* const end_align32 = p + __align_down(bucket_size_major / sizeof(uint32_t), 32);
    ASSERT((uintptr_t)p % 32 == 0);
    ASSERT((uintptr_t)end_align32 % 32 == 0);
    ASSERT(end_align32 <= end);


#define _CHECK_RESULT(N) \
        do { \
            ASSERT(orderkey##N > 0, ""); \
            ASSERT(orderkey##N < (1U << 30)); \
            ASSERT(orderkey##N <= g_max_orderkey, "orderkey" #N " too large: %u", orderkey##N); \
            ASSERT(total_expend_cent##N > 0, "orderkey" #N ": %u", orderkey##N); \
            ASSERT(total_expend_cent##N < (1U << 28)); \
            ASSERT(plate_orderdate_diff##N >= 0); \
            ASSERT(plate_orderdate_diff##N < (1 << 6)); \
            const uint64_t value = (uint64_t)(total_expend_cent##N) << 36 | (uint64_t)(orderkey##N) << 6 | (plate_orderdate_diff##N); \
            \
            if (topn_count < CONFIG_EXPECT_MAX_TOPN) { \
                topn_ptr[topn_count++] = value; \
                if (__unlikely(topn_count == CONFIG_EXPECT_MAX_TOPN)) { \
                    std::make_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>()); \
                } \
            } \
            else { \
                if (value > topn_ptr[0]) { \
                    std::pop_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>()); \
                    topn_ptr[CONFIG_EXPECT_MAX_TOPN-1] = value; \
                    std::push_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>()); \
                } \
            } \
        } while(false)

    while (p < end_align32) {
        const uint32_t orderkey1 = *(p + 7) & ~0xC0000000U;
        ASSERT(orderkey1 > 0);
        const uint32_t bucket_orderdate_diff1 = *(p + 7) >> 30;
        const date_t plate_orderdate_diff1 = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff1;
        const __m256i items1 = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 8;

        const uint32_t orderkey2 = *(p + 7) & ~0xC0000000U;
        ASSERT(orderkey2 > 0);
        const uint32_t bucket_orderdate_diff2 = *(p + 7) >> 30;
        const date_t plate_orderdate_diff2 = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff2;
        const __m256i items2 = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 8;

        const uint32_t orderkey3 = *(p + 7) & ~0xC0000000U;
        ASSERT(orderkey3 > 0);
        const uint32_t bucket_orderdate_diff3 = *(p + 7) >> 30;
        const date_t plate_orderdate_diff3 = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff3;
        const __m256i items3 = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 8;

        const uint32_t orderkey4 = *(p + 7) & ~0xC0000000U;
        ASSERT(orderkey4 > 0);
        const uint32_t bucket_orderdate_diff4 = *(p + 7) >> 30;
        const date_t plate_orderdate_diff4 = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff4;
        const __m256i items4 = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 8;

        // See https://stackoverflow.com/questions/9775538/fastest-way-to-do-horizontal-vector-sum-with-avx-instructions
        const __m256i tmp1 = _mm256_hadd_epi32(items1, items2);
        const __m256i tmp2 = _mm256_hadd_epi32(items3, items4);
        const __m256i tmp3 = _mm256_hadd_epi32(tmp1, tmp2);
        const __m128i tmp3lo = _mm256_castsi256_si128(tmp3);
        const __m128i tmp3hi = _mm256_extracti128_si256(tmp3, 1);
        const __m128i sum = _mm_add_epi32(tmp3hi, tmp3lo);

        const uint32_t total_expend_cent1 = _mm_extract_epi32(sum, 0);
        const uint32_t total_expend_cent2 = _mm_extract_epi32(sum, 1);
        const uint32_t total_expend_cent3 = _mm_extract_epi32(sum, 2);
        const uint32_t total_expend_cent4 = _mm_extract_epi32(sum, 3);

        _CHECK_RESULT(1);
        _CHECK_RESULT(2);
        _CHECK_RESULT(3);
        _CHECK_RESULT(4);
    }

    ASSERT(p == end_align32);
    while (p < end) {
        const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
        ASSERT(orderkey > 0);
        const uint32_t bucket_orderdate_diff = *(p + 7) >> 30;
        const date_t plate_orderdate_diff = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff;
        const __m256i items = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 8;

        __m256i sum = _mm256_hadd_epi32(items, items);
        sum = _mm256_hadd_epi32(sum, sum);
        const uint32_t total_expend_cent = _mm256_extract_epi32(sum, 0) + _mm256_extract_epi32(sum, 4);
        ASSERT(total_expend_cent > 0);

        _CHECK_RESULT();

#undef _CHECK_RESULT
    }
}



static void worker_compute_pretopn_for_plate_minor(
    /*in*/ const date_t bucket_base_orderdate_minus_plate_base_orderdate,
    /*in*/ const void* const bucket_ptr_minor,
    /*in*/ const uint64_t bucket_size_minor,
    /*inout*/ uint32_t& topn_count,
    /*inout*/ uint64_t* const topn_ptr) noexcept
{
    ASSERT(bucket_base_orderdate_minus_plate_base_orderdate >= 0);
    ASSERT(bucket_base_orderdate_minus_plate_base_orderdate % CONFIG_ORDERDATES_PER_BUCKET == 0);
    ASSERT((uint32_t)bucket_base_orderdate_minus_plate_base_orderdate < CONFIG_ORDERDATES_PER_BUCKET * BUCKETS_PER_PLATE);
    ASSERT(bucket_ptr_minor != nullptr);
    ASSERT((uintptr_t)bucket_ptr_minor % PAGE_SIZE == 0);
    ASSERT(bucket_size_minor > 0);
    ASSERT(bucket_size_minor % (sizeof(uint32_t) * 4) == 0);
    ASSERT(topn_count <= CONFIG_EXPECT_MAX_TOPN);
    ASSERT(topn_ptr != nullptr);
    ASSERT((uintptr_t)topn_ptr % PAGE_SIZE == 0);


    //
    // Naive implementation
    //
    {
        const uint32_t* p = (const uint32_t*)bucket_ptr_minor;
        const uint32_t* const end = (const uint32_t*)((uintptr_t)p + bucket_size_minor);
        while (p < end) {
            const uint32_t total_expend_cent = (p[0] & 0x00FFFFFF) + (p[1] & 0x00FFFFFF) + (p[2] & 0x00FFFFFF);
            const uint32_t orderkey = *(p + 3) & ~0xC0000000U;
            const uint32_t bucket_orderdate_diff = *(p + 3) >> 30;
            const date_t plate_orderdate_diff = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff;
            p += 4;

            const uint64_t value = (uint64_t)(total_expend_cent) << 36 | (uint64_t)(orderkey) << 6 | (plate_orderdate_diff);
           
            if (topn_count < CONFIG_EXPECT_MAX_TOPN) {
                topn_ptr[topn_count++] = value;
                if (__unlikely(topn_count == CONFIG_EXPECT_MAX_TOPN)) {
                    std::make_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>());
                }
            }
            else {
                if (value > topn_ptr[0]) {
                    std::pop_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>());
                    topn_ptr[CONFIG_EXPECT_MAX_TOPN-1] = value;
                    std::push_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>());
                }
            }
        }
    }
    return;

#if ENABLE_ASSERTION
    {
        const uint32_t* p = (const uint32_t*)bucket_ptr_minor;
        const uint32_t* const end = (const uint32_t*)((uintptr_t)p + bucket_size_minor);
        while (p < end) {
            const uint32_t orderkey = *(p + 3) & ~0xC0000000U;
            p += 4;

            ASSERT(orderkey > 0, "bucket_id=%%u, bucket_size_minor=%lu, offset=%lu",
                /*bucket_id,*/ bucket_size_minor, (end - p) * sizeof(uint32_t));
        }
    }
#endif

    const __m256i expend_mask = _mm256_set_epi32(
        0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
        0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);

    const uint32_t* p = (const uint32_t*)bucket_ptr_minor;
    const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr_minor + bucket_size_minor);
    const uint32_t* const end_align32 = p + __align_down(bucket_size_minor / sizeof(uint32_t), 32);
    ASSERT((uintptr_t)p % 32 == 0);
    ASSERT((uintptr_t)end_align32 % 32 == 0);
    ASSERT(end_align32 <= end);


#define _CHECK_RESULT(N) \
        do { \
            ASSERT(orderkey##N > 0, ""); \
            ASSERT(orderkey##N < (1U << 30)); \
            ASSERT(orderkey##N <= g_max_orderkey, "orderkey" #N " too large: %u", orderkey##N); \
            ASSERT(total_expend_cent##N > 0, "orderkey" #N ": %u", orderkey##N); \
            ASSERT(total_expend_cent##N < (1U << 28)); \
            ASSERT(plate_orderdate_diff##N >= 0); \
            ASSERT(plate_orderdate_diff##N < (1 << 6)); \
            const uint64_t value = (uint64_t)(total_expend_cent##N) << 36 | (uint64_t)(orderkey##N) << 6 | (plate_orderdate_diff##N); \
            \
            if (topn_count < CONFIG_EXPECT_MAX_TOPN) { \
                topn_ptr[topn_count++] = value; \
                if (__unlikely(topn_count == CONFIG_EXPECT_MAX_TOPN)) { \
                    std::make_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>()); \
                } \
            } \
            else { \
                if (value > topn_ptr[0]) { \
                    std::pop_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>()); \
                    topn_ptr[CONFIG_EXPECT_MAX_TOPN-1] = value; \
                    std::push_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>()); \
                } \
            } \
        } while(false)

    while (p < end_align32) {
        const uint32_t orderkey1 = *(p + 3) & ~0xC0000000U;
        ASSERT(orderkey1 > 0);
        const uint32_t bucket_orderdate_diff1 = *(p + 3) >> 30;
        const date_t plate_orderdate_diff1 = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff1;
        const __m256i items1 = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 4;

        const uint32_t orderkey2 = *(p + 3) & ~0xC0000000U;
        ASSERT(orderkey2 > 0);
        const uint32_t bucket_orderdate_diff2 = *(p + 3) >> 30;
        const date_t plate_orderdate_diff2 = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff2;
        const __m256i items2 = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 4;

        const uint32_t orderkey3 = *(p + 3) & ~0xC0000000U;
        ASSERT(orderkey3 > 0);
        const uint32_t bucket_orderdate_diff3 = *(p + 3) >> 30;
        const date_t plate_orderdate_diff3 = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff3;
        const __m256i items3 = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 4;

        const uint32_t orderkey4 = *(p + 3) & ~0xC0000000U;
        ASSERT(orderkey4 > 0);
        const uint32_t bucket_orderdate_diff4 = *(p + 3) >> 30;
        const date_t plate_orderdate_diff4 = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff4;
        const __m256i items4 = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 4;

        // See https://stackoverflow.com/questions/9775538/fastest-way-to-do-horizontal-vector-sum-with-avx-instructions
        const __m256i tmp1 = _mm256_hadd_epi32(items1, items2);
        const __m256i tmp2 = _mm256_hadd_epi32(items3, items4);
        const __m256i tmp3 = _mm256_hadd_epi32(tmp1, tmp2);
        const __m128i tmp3lo = _mm256_castsi256_si128(tmp3);
        const __m128i tmp3hi = _mm256_extracti128_si256(tmp3, 1);
        const __m128i sum = _mm_add_epi32(tmp3hi, tmp3lo);

        const uint32_t total_expend_cent1 = _mm_extract_epi32(sum, 0);
        const uint32_t total_expend_cent2 = _mm_extract_epi32(sum, 1);
        const uint32_t total_expend_cent3 = _mm_extract_epi32(sum, 2);
        const uint32_t total_expend_cent4 = _mm_extract_epi32(sum, 3);

        _CHECK_RESULT(1);
        _CHECK_RESULT(2);
        _CHECK_RESULT(3);
        _CHECK_RESULT(4);
    }

    ASSERT(p == end_align32);
    while (p < end) {
        const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
        ASSERT(orderkey > 0);
        const uint32_t bucket_orderdate_diff = *(p + 7) >> 30;
        const date_t plate_orderdate_diff = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff;
        const __m256i items = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        p += 8;

        __m256i sum = _mm256_hadd_epi32(items, items);
        sum = _mm256_hadd_epi32(sum, sum);
        const uint32_t total_expend_cent = _mm256_extract_epi32(sum, 0) + _mm256_extract_epi32(sum, 4);
        ASSERT(total_expend_cent > 0);

        _CHECK_RESULT();

#undef _CHECK_RESULT
    }
}


static void worker_compute_pretopn([[maybe_unused]] const uint32_t tid) noexcept
{
    // Truncate for pretopn, pretopn_count
    {
        ASSERT(g_shared->total_plates > 0);

        ASSERT(g_pretopn_file.fd > 0);
        ASSERT(g_pretopn_count_file.fd > 0);

        static_assert(sizeof(uint64_t) * CONFIG_EXPECT_MAX_TOPN % PAGE_SIZE == 0);
        g_pretopn_file.file_size = sizeof(uint64_t) * CONFIG_EXPECT_MAX_TOPN * g_shared->total_plates;
        g_pretopn_count_file.file_size = sizeof(uint32_t) * g_shared->total_plates;
        ASSERT(g_pretopn_file.file_size > 0);
        ASSERT(g_pretopn_count_file.file_size > 0);

        g_shared->worker_sync_barrier.run_once_and_sync([]() {
            C_CALL(ftruncate(g_pretopn_file.fd, g_pretopn_file.file_size));
            C_CALL(ftruncate(g_pretopn_count_file.fd, g_pretopn_count_file.file_size));

            INFO("g_pretopn_file.file_size: %lu", g_pretopn_file.file_size);
            INFO("g_pretopn_count_file.file_size: %lu", g_pretopn_count_file.file_size);

            ASSERT(g_shared->pretopn_plate_id_shared_counter.load() == 0);
        });

        g_pretopn_start_ptr = (uint64_t*)my_mmap(
            g_pretopn_file.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_pretopn_file.fd,
            0);
        DEBUG("[%u] g_pretopn_start_ptr: %p", tid, g_pretopn_start_ptr);

        g_pretopn_count_start_ptr = (uint32_t*)my_mmap(
            g_pretopn_count_file.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_pretopn_count_file.fd,
            0);
        DEBUG("[%u] g_pretopn_count_start_ptr: %p", tid, g_pretopn_count_start_ptr);
    }


    void* const bucket_ptr_major = mmap_reserve_space(CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR);
    void* const bucket_ptr_minor = mmap_reserve_space(CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR);

    // For each bucket, write remaining buffer (if exists) to file
    uint32_t plate_ids_to_shuffle[g_shared->total_plates];
    for (uint32_t i = 0; i < g_shared->total_plates; ++i) {
        plate_ids_to_shuffle[i] = i;
    }
    std::mt19937 g(0xdeedbeef); // NOLINT(cert-msc32-c,cert-msc51-cpp)
    std::shuffle(plate_ids_to_shuffle, plate_ids_to_shuffle + g_shared->total_plates, g);

    while (true) {
        const uint32_t plate_id_index = g_shared->pretopn_plate_id_shared_counter++;
        if (__unlikely(plate_id_index >= g_shared->total_plates)) break;
        const uint32_t plate_id = plate_ids_to_shuffle[plate_id_index];
        TRACE("pretopn for plate_id: %u", plate_id);

        const uint32_t plate_base_bucket_id = calc_plate_base_bucket_id_by_plate_id(plate_id);
        const uint32_t base_mktid = calc_bucket_mktid(plate_base_bucket_id);

        const date_t plate_base_orderdate = calc_plate_base_orderdate_by_plate_id(plate_id);
        ASSERT((plate_base_orderdate - MIN_TABLE_DATE) % CONFIG_TOPN_DATES_PER_PLATE == 0);

        uint32_t& topn_count = g_pretopn_count_start_ptr[plate_id];
        uint64_t* topn_ptr = g_pretopn_start_ptr + (uint64_t)plate_id * CONFIG_EXPECT_MAX_TOPN;


        //
        // Scan major buckets
        //
        for (uint32_t bucket_id = plate_base_bucket_id; bucket_id < plate_base_bucket_id + BUCKETS_PER_PLATE; ++bucket_id) {
            if (calc_bucket_mktid(bucket_id) != base_mktid) break;
            TRACE("pretopn for major bucket_id: %u", bucket_id);

#if ENABLE_ASSERTION
            ASSERT(calc_plate_id(bucket_id) == plate_id);
#endif

            const date_t bucket_base_orderdate = calc_bucket_base_orderdate_by_bucket_id(bucket_id);
            ASSERT(bucket_base_orderdate >= plate_base_orderdate);
            ASSERT(bucket_base_orderdate < plate_base_orderdate + CONFIG_TOPN_DATES_PER_PLATE);
            ASSERT((bucket_base_orderdate - plate_base_orderdate) % CONFIG_ORDERDATES_PER_BUCKET == 0);

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - begin_bucket_id);
            const uint64_t bucket_size_major = g_buckets_endoffset_major[bucket_id] - bucket_start_offset_major;
            if (__unlikely(bucket_size_major == 0)) continue;

            void* const mapped_ptr = mmap(
                bucket_ptr_major,
                bucket_size_major,
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_major_fd[holder_id],
                bucket_start_offset_major);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == bucket_ptr_major);

#if ENABLE_ASSERTION
            const uint32_t* p = (uint32_t*)bucket_ptr_major;
            const uint32_t* end = (uint32_t*)((uintptr_t)p + bucket_size_major);
            while (p < end) {
                const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
                p += 8;

                ASSERT(orderkey > 0, "bucket_id=%u, bucket_size_major=%lu, offset=%lu",
                    bucket_id, bucket_size_major, (end - p) * sizeof(uint32_t));
            }
#endif

            worker_compute_pretopn_for_plate_major(
                (bucket_base_orderdate - plate_base_orderdate),
                (const uint32_t *) bucket_ptr_major,
                bucket_size_major,
                topn_count,
                topn_ptr);
        }


        //
        // Scan minor buckets
        //
        for (uint32_t bucket_id = plate_base_bucket_id; bucket_id < plate_base_bucket_id + BUCKETS_PER_PLATE; ++bucket_id) {
            if (calc_bucket_mktid(bucket_id) != base_mktid) break;
            TRACE("pretopn for minor bucket_id: %u", bucket_id);

#if ENABLE_ASSERTION
            ASSERT(calc_plate_id(bucket_id) == plate_id);
#endif

            const date_t bucket_base_orderdate = calc_bucket_base_orderdate_by_bucket_id(bucket_id);
            ASSERT(bucket_base_orderdate >= plate_base_orderdate);
            ASSERT(bucket_base_orderdate < plate_base_orderdate + CONFIG_TOPN_DATES_PER_PLATE);
            ASSERT((bucket_base_orderdate - plate_base_orderdate) % CONFIG_ORDERDATES_PER_BUCKET == 0);

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_minor = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - begin_bucket_id);
            const uint64_t bucket_size_minor = g_buckets_endoffset_minor[bucket_id] - bucket_start_offset_minor;
            if (__unlikely(bucket_size_minor == 0)) continue;

            void* const mapped_ptr = mmap(
                bucket_ptr_minor,
                bucket_size_minor,
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_minor_fd[holder_id],
                bucket_start_offset_minor);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == bucket_ptr_minor);

#if ENABLE_ASSERTION
            const uint32_t* p = (uint32_t*)bucket_ptr_minor;
            const uint32_t* end = (uint32_t*)((uintptr_t)p + bucket_size_minor);
            while (p < end) {
                const uint32_t orderkey = *(p + 3) & ~0xC0000000U;
                p += 4;

                ASSERT(orderkey > 0, "bucket_id=%u, bucket_size_minor=%lu, offset=%lu",
                    bucket_id, bucket_size_minor, (end - p) * sizeof(uint32_t));
            }
#endif

            TRACE("build pretopn: scan minor bucket: %u", bucket_id);
            worker_compute_pretopn_for_plate_minor(
                (bucket_base_orderdate - plate_base_orderdate),
                (const uint32_t *)bucket_ptr_minor,
                bucket_size_minor,
                topn_count,
                topn_ptr);
        }

        std::sort(topn_ptr, topn_ptr + topn_count, std::greater<>());
    }


    C_CALL(munmap(bucket_ptr_minor, CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR));
    mmap_return_space(bucket_ptr_minor, CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR);

    C_CALL(munmap(bucket_ptr_major, CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR));
    mmap_return_space(bucket_ptr_major, CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR);

    INFO("[%u] done worker_compute_pretopn()", tid);
}


void fn_worker_thread_create_index(const uint32_t tid) noexcept
{
    DEBUG("[%u] fn_worker_thread_create_index() starts", tid);

    // Parse loaded customer table
    {
        worker_load_customer_multi_part(tid);
        g_shared->worker_sync_barrier.sync();
        if (tid == 0) {
            INFO("g_shared->total_buckets: %u", g_shared->total_buckets);
            INFO("g_shared->buckets_per_holder: %u", g_shared->buckets_per_holder);

#if ENABLE_ASSERTION
            INFO("g_shared->customer_file_loaded_parts: %lu", g_shared->customer_file_loaded_parts.load());
#endif
        }
    }


    // Parse loaded orders table
    {
        worker_load_orders_multi_part(tid);
#if ENABLE_SHM_CACHE_TXT
        ASSERT(g_orders_shm.ptr != nullptr);
        g_orders_shm.detach();  // then g_orders_shm should have been removed
        ASSERT(g_orders_shm.ptr == nullptr);

        g_shared->worker_sync_barrier.sync_and_run_once([]() {
            // Release huge pages for orders txt
            const uint64_t require_nr_2mb =
                __div_up(g_customer_file.file_size, 1024 * 1024 * 2) +
                __div_up(g_lineitem_file.file_size, 1024 * 1024 * 2) +
                CONFIG_EXTRA_HUGE_PAGES;

            const bool success = mem_set_nr_hugepages_2048kB(require_nr_2mb);
            CHECK(success, "Can't adjust nr_hugepages to %lu", require_nr_2mb);
            INFO("adjusted nr_hugepages to %lu", require_nr_2mb);

            #if ENABLE_ASSERTION
            const uint64_t actual_nr_2mb = mem_get_nr_hugepages_2048kB();
            ASSERT(actual_nr_2mb == require_nr_2mb,
                "Expect actual_nr_2mb == require_nr_2mb: %lu == %lu", actual_nr_2mb, require_nr_2mb);
            #endif

            //system("free -h");
        });

#else  // !ENABLE_SHM_CACHE_TXT
        g_shared->worker_sync_barrier.sync_and_run_once([]() {
            ASSERT(g_orders_file.fd > 0);
            ASSERT(g_orders_file.file_size > 0);

            /*
            uint64_t clear_size = std::min<uint64_t>(
                g_orders_file.file_size,
                sizeof(uint32_t) * g_max_orderkey * 6);
            //TODO: const uint32_t free_mem = mem_get_free_bytes();
            // Make use of free memory to reduce page cache clearing?

            __fadvice_dont_need(g_orders_file.fd, 0, clear_size);
            INFO("__fadvice_dont_need page cache for orders file (clear_size: %lu)", clear_size);
             */

            //system("free -h");
        });
#endif  // ENABLE_SHM_CACHE_TXT

        worker_load_orders_custkey_from_orderkey(tid, g_shared->orderkey_custkey_shared_counter);
        g_shared->worker_sync_barrier.sync();

#if ENABLE_ASSERTION
        if (tid == 0) {
            debug_check_orders();
        }
#endif
    }

    // We must do shmdt()
    // This is to save space for g_items_buffer_{1/2}_start_ptr
    {
        // Detach g_custkey_to_mktid
        g_custkey_to_mktid.detach();

        // Detach g_orderkey_to_custkey
        g_orderkey_to_custkey.detach();

        g_shared->worker_sync_barrier.sync();  // This sync is necessary
    }


    // Parse loaded lineitem table
    {
        worker_load_lineitem_multi_part(tid);

        g_shared->worker_sync_barrier.sync_and_run_once([]() {
            uint64_t max_bucket_size_major = 0;
            uint64_t max_bucket_size_minor = 0;

            for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
                const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
                const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

                const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - begin_bucket_id);
                const uint64_t bucket_size_major = g_buckets_endoffset_major[bucket_id] - bucket_start_offset_major;
                if (max_bucket_size_major < bucket_size_major) {
                    max_bucket_size_major = bucket_size_major;
                }

                const uintptr_t bucket_start_offset_minor = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - begin_bucket_id);
                const uint64_t bucket_size_minor = g_buckets_endoffset_minor[bucket_id] - bucket_start_offset_minor;
                if (max_bucket_size_minor < bucket_size_minor) {
                    max_bucket_size_minor = bucket_size_minor;
                }
            }

            INFO("max_bucket_size_major: %lu", max_bucket_size_major);
            INFO("max_bucket_size_minor: %lu", max_bucket_size_minor);

            ASSERT(max_bucket_size_major < CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR);
            ASSERT(max_bucket_size_minor < CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR);

            g_shared->meta.max_bucket_size_major = max_bucket_size_major;
            g_shared->meta.max_bucket_size_minor = max_bucket_size_minor;


            //
            // Save meta to file
            //
            {
                const int fd = C_CALL(openat(
                    g_index_directory_fd,
                    "meta",
                    O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
                    0666));
                const size_t cnt = C_CALL(pwrite(
                    fd,
                    &g_shared->meta,
                    sizeof(g_shared->meta),
                    0));
                CHECK(cnt == sizeof(g_shared->meta));
                C_CALL(close(fd));
            }
        });
    }


    // Create pretopn
    {
        worker_compute_pretopn(tid);
        g_shared->worker_sync_barrier.sync();
    }


    //
    // Test query
    //
#if ENABLE_ASSERTION
    if (tid == 0) {
        constexpr const uint32_t TOPN = 5;
        std::vector<query_result_t> results;
        results.reserve(TOPN);

        uint32_t mktid;
        for (mktid = 0; mktid < g_shared->mktid_count; ++mktid) {
            if (std::string_view(g_shared->all_mktsegments[mktid].name, g_shared->all_mktsegments[mktid].length) == "BUILDING") {
                break;
            }
        }
        CHECK(mktid < g_shared->mktid_count);

        const date_t q_orderdate = date_from_string<false>("1998-08-02");
        const date_t q_shipdate = date_from_string<false>("1992-01-02");

        uint32_t* const ptr = (uint32_t*)mmap_reserve_space(1048576 * 64);
        for (date_t scan_orderdate = MIN_TABLE_DATE; scan_orderdate < q_orderdate; ++scan_orderdate) {
            const date_t base_orderdate = calc_bucket_base_orderdate(scan_orderdate);
            if (scan_orderdate != base_orderdate) continue;

            const uint32_t bucket_id = calc_bucket_index(mktid, base_orderdate);
            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - begin_bucket_id);
            const uint64_t bucket_size_major = g_buckets_endoffset_major[bucket_id] - bucket_start_offset_major;
            if (bucket_size_major == 0) continue;

            void* const mapped_ptr = mmap(
                ptr,
                bucket_size_major,
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_major_fd[holder_id],
                bucket_start_offset_major);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == ptr);

            ASSERT(bucket_size_major % (8 * sizeof(uint32_t)) == 0);

            const __m256i expend_mask = _mm256_set_epi32(
                0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
                0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);
            __m256i greater_than_value;
            if (base_orderdate > q_shipdate) {
                greater_than_value = _mm256_set1_epi32(0);  // TODO: dummy. remove!
            }
            else if (q_shipdate - base_orderdate >= 128) {
                greater_than_value = _mm256_set1_epi32(0x7FFFFFFF);  // TODO: dummy. remove!
            }
            else {  // base_orderdate <= q_shipdate < base_orderdate + 128
                greater_than_value = _mm256_set1_epi32((q_shipdate - base_orderdate) << 24 | 0x00FFFFFF);  // TODO: dummy. remove!
            }

            uint32_t* p = ptr;
            const uint32_t* const end = (uint32_t*)((uintptr_t)ptr + bucket_size_major);
            const uint32_t* const end_align32 = p + __align_down(bucket_size_major / sizeof(uint32_t), 32);
            while (p < end_align32) {
                const uint32_t orderdate_diff1 = *(p + 7) >> 30;
                const date_t orderdate1 = base_orderdate + orderdate_diff1;
                const uint32_t orderkey1 = *(p + 7) & ~0xC0000000U;
                __m256i items1 = _mm256_load_si256((__m256i*)p);
                p += 8;
                const __m256i gt_mask1 = _mm256_cmpgt_epi32(items1, greater_than_value);
                items1 = _mm256_and_si256(items1, gt_mask1);
                items1 = _mm256_and_si256(items1, expend_mask);

                const uint32_t orderdate_diff2 = *(p + 7) >> 30;
                const date_t orderdate2 = base_orderdate + orderdate_diff2;
                const uint32_t orderkey2 = *(p + 7) & ~0xC0000000U;
                __m256i items2 = _mm256_load_si256((__m256i*)p);
                p += 8;
                const __m256i gt_mask2 = _mm256_cmpgt_epi32(items2, greater_than_value);
                items2 = _mm256_and_si256(items2, gt_mask2);
                items2 = _mm256_and_si256(items2, expend_mask);

                const uint32_t orderdate_diff3 = *(p + 7) >> 30;
                const date_t orderdate3 = base_orderdate + orderdate_diff3;
                const uint32_t orderkey3 = *(p + 7) & ~0xC0000000U;
                __m256i items3 = _mm256_load_si256((__m256i*)p);
                p += 8;
                const __m256i gt_mask3 = _mm256_cmpgt_epi32(items3, greater_than_value);
                items3 = _mm256_and_si256(items3, gt_mask3);
                items3 = _mm256_and_si256(items3, expend_mask);

                const uint32_t orderdate_diff4 = *(p + 7) >> 30;
                const date_t orderdate4 = base_orderdate + orderdate_diff4;
                const uint32_t orderkey4 = *(p + 7) & ~0xC0000000U;
                __m256i items4 = _mm256_load_si256((__m256i*)p);
                p += 8;
                const __m256i gt_mask4 = _mm256_cmpgt_epi32(items4, greater_than_value);
                items4 = _mm256_and_si256(items4, gt_mask4);
                items4 = _mm256_and_si256(items4, expend_mask);

                // TODO: looks for better way!
                // See https://stackoverflow.com/questions/9775538/fastest-way-to-do-horizontal-vector-sum-with-avx-instructions
                const __m256i tmp1 = _mm256_hadd_epi32(items1, items2);
                const __m256i tmp2 = _mm256_hadd_epi32(items3, items4);
                const __m256i tmp3 = _mm256_hadd_epi32(tmp1, tmp2);
                const __m128i tmp3lo = _mm256_castsi256_si128(tmp3);
                const __m128i tmp3hi = _mm256_extracti128_si256(tmp3, 1);
                const __m128i sum = _mm_add_epi32(tmp3hi, tmp3lo);

                const uint32_t total_expend_cent1 = _mm_extract_epi32(sum, 0);
                const uint32_t total_expend_cent2 = _mm_extract_epi32(sum, 1);
                const uint32_t total_expend_cent3 = _mm_extract_epi32(sum, 2);
                const uint32_t total_expend_cent4 = _mm_extract_epi32(sum, 3);

#define _CHECK_RESULT(N) \
                if (total_expend_cent##N > 0) { \
                    query_result_t tmp; \
                    tmp.orderdate = orderdate##N; \
                    tmp.orderkey = orderkey##N; \
                    tmp.total_expend_cent = total_expend_cent##N; \
                    \
                    if (results.size() < TOPN) { \
                        results.emplace_back(std::move(tmp)); \
                        if (__unlikely(results.size() == TOPN)) { \
                            std::make_heap(results.begin(), results.end(), std::greater<>()); \
                        } \
                    } \
                    else { \
                        if (tmp > *results.begin()) { \
                            std::pop_heap(results.begin(), results.end(), std::greater<>()); \
                            *results.rbegin() = tmp; \
                            std::push_heap(results.begin(), results.end(), std::greater<>()); \
                        } \
                    } \
                }

                _CHECK_RESULT(1)
                _CHECK_RESULT(2)
                _CHECK_RESULT(3)
                _CHECK_RESULT(4)
#undef _CHECK_RESULT
            }


            while (p < end) {
                const uint32_t orderdate_diff = *(p + 7) >> 30;
                const date_t orderdate = base_orderdate + orderdate_diff;
                const uint32_t orderkey = *(p + 7) & ~0xC0000000U;

                __m256i items = _mm256_load_si256((__m256i*)p);
                p += 8;

                const __m256i gt_mask = _mm256_cmpgt_epi32(items, greater_than_value);
                if (_mm256_testz_si256(gt_mask, gt_mask)) continue;

                items = _mm256_and_si256(items, gt_mask);
                items = _mm256_and_si256(items, expend_mask);

                // TODO: looks for better way!
                // See https://stackoverflow.com/questions/9775538/fastest-way-to-do-horizontal-vector-sum-with-avx-instructions
                __m256i sum = _mm256_hadd_epi32(items, items);
                sum = _mm256_hadd_epi32(sum, sum);
                const uint32_t total_expend_cent = _mm256_extract_epi32(sum, 0) + _mm256_extract_epi32(sum, 4);
                ASSERT(total_expend_cent > 0);

                query_result_t tmp;
                tmp.orderdate = orderdate;
                tmp.orderkey = orderkey;
                tmp.total_expend_cent = total_expend_cent;

                if (results.size() < TOPN) {
                    results.emplace_back(std::move(tmp));
                    if (__unlikely(results.size() == TOPN)) {
                        std::make_heap(results.begin(), results.end(), std::greater<>());
                    }
                }
                else {
                    if (tmp > *results.begin()) {
                        std::pop_heap(results.begin(), results.end(), std::greater<>());
                        *results.rbegin() = tmp;
                        std::push_heap(results.begin(), results.end(), std::greater<>());
                    }
                }
            }
//
//            uint32_t* p = ptr;
//            uint32_t* end = (uint32_t*)((uintptr_t)ptr + bucket_size_major);
//            while (p < end) {
//                const uint32_t orderdate_diff = *(p + 7) >> 30;
//                const date_t orderdate = base_orderdate + orderdate_diff;
//                const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
//
//                __m256i items = _mm256_load_si256((__m256i*)p);
//                p += 8;
//
//                const __m256i gt_mask = _mm256_cmpgt_epi32(items, greater_than_value);
//                if (_mm256_testz_si256(gt_mask, gt_mask)) continue;
//
//                items = _mm256_and_si256(items, gt_mask);
//                items = _mm256_and_si256(items, expend_mask);
//
//                // TODO: looks for better way!
//                // See https://stackoverflow.com/questions/9775538/fastest-way-to-do-horizontal-vector-sum-with-avx-instructions
//                __m256i sum = _mm256_hadd_epi32(items, items);
//                sum = _mm256_hadd_epi32(sum, sum);
//                const uint32_t total_expend_cent = _mm256_extract_epi32(sum, 0) + _mm256_extract_epi32(sum, 4);
//                ASSERT(total_expend_cent > 0);
//
//                query_result_t tmp;
//                tmp.orderdate = orderdate;
//                tmp.orderkey = orderkey;
//                tmp.total_expend_cent = total_expend_cent;
//
//                if (results.size() < TOPN) {
//                    results.emplace_back(std::move(tmp));
//                    if (__unlikely(results.size() == TOPN)) {
//                        std::make_heap(results.begin(), results.end(), std::greater<>());
//                    }
//                }
//                else {
//                    if (tmp > *results.begin()) {
//                        std::pop_heap(results.begin(), results.end(), std::greater<>());
//                        *results.rbegin() = tmp;
//                        std::push_heap(results.begin(), results.end(), std::greater<>());
//                    }
//                }
//            }
        }

        std::sort(results.begin(), results.end(), std::greater<>());
        for (const auto& r : results) {
            const uint32_t print_orderkey = ((r.orderkey & ~0b111) << 2) | (r.orderkey & 0b111);
            const uint32_t year = date_get_year(r.orderdate);
            const uint32_t month = date_get_month(r.orderdate);
            const uint32_t day = date_get_day(r.orderdate);
            INFO("%u|%04u-%02u-%02u|%u.%02u", print_orderkey, year, month, day, r.total_expend_cent / 100, r.total_expend_cent % 100);
        }
    }
#endif  // ENABLE_ASSERTION

}


void fn_unloader_thread_create_index() noexcept
{

}

void create_index_initialize_before_fork() noexcept
{
    //
    // Open holder files & endoffset file
    //
    {
        for (uint32_t holder_id = 0; holder_id < CONFIG_INDEX_HOLDER_COUNT; ++holder_id) {
            char filename[32];

            snprintf(filename, std::size(filename), "holder_major_%04u", holder_id);
            g_holder_files_major_fd[holder_id] = C_CALL(openat(
                g_index_directory_fd,
                filename,
                O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
                0666));

            snprintf(filename, std::size(filename), "holder_minor_%04u", holder_id);
            g_holder_files_minor_fd[holder_id] = C_CALL(openat(
                g_index_directory_fd,
                filename,
                O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
                0666));
        }

        g_endoffset_file_major.fd = C_CALL(openat(
            g_index_directory_fd,
            "endoffset_major",
            O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
            0666));

        g_endoffset_file_minor.fd = C_CALL(openat(
            g_index_directory_fd,
            "endoffset_minor",
            O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
            0666));
    }


    //
    // Open pretopn file & pretopn_count
    //
    {
        g_pretopn_file.fd = C_CALL(openat(
            g_index_directory_fd,
            "pretopn",
            O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
            0666));

        g_pretopn_count_file.fd = C_CALL(openat(
            g_index_directory_fd,
            "pretopn_count",
            O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
            0666));
    }


    //
    // Calculate g_max_custkey
    //
#if ENABLE_SHM_CACHE_TXT
    PANIC("TODO! Get g_max_custkey");

#else
    {
        char buffer[PAGE_SIZE];
        C_CALL(pread(
            g_customer_file.fd,
            buffer,
            std::size(buffer),
            g_customer_file.file_size - PAGE_SIZE));
        const char* p = &buffer[std::size(buffer) - 1];
        while (*(p - 1) != '\n') --p;
        g_max_custkey = 0;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9');
            g_max_custkey = g_max_custkey * 10 + (*p - '0');
            ++p;
        }
        INFO("g_max_custkey: %u", g_max_custkey);
    }
#endif


    //
    // Calculate g_max_orderkey
    //
#if ENABLE_SHM_CACHE_TXT
    PANIC("TODO! Get g_max_orderkey");

#else
    {
        char buffer[PAGE_SIZE];
        C_CALL(pread(
            g_orders_file.fd,
            buffer,
            std::size(buffer),
            g_orders_file.file_size - PAGE_SIZE));
        const char* p = &buffer[std::size(buffer) - 1];
        while (*(p - 1) != '\n') --p;
        g_max_orderkey = 0;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9');
            g_max_orderkey = g_max_orderkey * 10 + (*p - '0');
            ++p;
        }
        ASSERT((g_max_orderkey & 0b11000) == 0);
        g_max_orderkey = (g_max_orderkey & 0b111) | ((g_max_orderkey >> 2) & ~0b1);
        INFO("g_max_orderkey: %u", g_max_orderkey);
    }
#endif


    // Initialize g_customer_shm, g_orders_shm, g_lineitem_shm
    if (g_is_preparing_page_cache) {
#if ENABLE_SHM_CACHE_TXT
        PANIC("TODO!");
#else  // !ENABLE_SHM_CACHE_TXT
        // Do nothing
#endif  // ENABLE_SHM_CACHE_TXT
    }
    else {
#if ENABLE_SHM_CACHE_TXT
        ASSERT(g_customer_file.file_size > 0);
        ASSERT(g_customer_shm.size_in_byte == g_customer_file.file_size);
        ASSERT(g_customer_shm.shmid >= 0);
        ASSERT(g_customer_shm.ptr != nullptr);
        INFO("g_customer_shm: shmid=%d, ptr=%p, size0x%lx", g_customer_shm.shmid, g_customer_shm.ptr, g_customer_shm.size_in_byte);

        ASSERT(g_orders_file.file_size > 0);
        ASSERT(g_orders_shm.size_in_byte == g_orders_file.file_size);
        ASSERT(g_orders_shm.shmid >= 0);
        ASSERT(g_orders_shm.ptr != nullptr);
        INFO("g_orders_shm: shmid=%d, ptr=%p, size0x%lx", g_orders_shm.shmid, g_orders_shm.ptr, g_orders_shm.size_in_byte);

        ASSERT(g_lineitem_file.file_size > 0);
        ASSERT(g_lineitem_shm.size_in_byte == g_lineitem_file.file_size);
        ASSERT(g_lineitem_shm.shmid >= 0);
        ASSERT(g_lineitem_shm.ptr != nullptr);
        INFO("g_lineitem_shm: shmid=%d, ptr=%p, size0x%lx", g_lineitem_shm.shmid, g_lineitem_shm.ptr, g_lineitem_shm.size_in_byte);
        
#else  // !ENABLE_SHM_CACHE_TXT
        // Do nothing
#endif  // ENABLE_SHM_CACHE_TXT
    }

    // Initialize g_custkey_to_mktid, g_orderkey_to_order
    {
        const bool success = g_custkey_to_mktid.init_fixed(
            SHMKEY_CUSTKEY_TO_MKTID,
            sizeof(g_custkey_to_mktid.ptr[0]) * (g_max_custkey / 3 + 1),
            true);
        CHECK(success);
        INFO("g_custkey_to_mktid: %p (size: %lu)", g_custkey_to_mktid.ptr, g_custkey_to_mktid.size_in_byte);
    }

    // Initialize g_orderkey_to_order
    {
        const bool success = g_orderkey_to_order.init_fixed(
            SHMKEY_ORDERKEY_TO_ORDER,
            sizeof(g_orderkey_to_order.ptr[0]) * (g_max_orderkey + 1),
            true);
        CHECK(success);
        INFO("g_orderkey_to_order: %p (size: %lu)", g_orderkey_to_order.ptr, g_orderkey_to_order.size_in_byte);
    }

    // Initialize g_orderkey_to_custkey
    {
        const bool success = g_orderkey_to_custkey.init_fixed(
            SHMKEY_ORDERKEY_TO_CUSTKEY,
            sizeof(g_orderkey_to_custkey.ptr[0]) * (g_max_orderkey + 1),
            true);
        CHECK(success);
        INFO("g_orderkey_to_custkey: %p (size: %lu)", g_orderkey_to_custkey.ptr, g_orderkey_to_custkey.size_in_byte);
    }
}

void create_index_initialize_after_fork() noexcept
{
    // Attach g_customer_shmat_ptr, g_orders_shmat_ptr, g_lineitem_shmat_ptr
    if (g_is_preparing_page_cache) {
#if ENABLE_SHM_CACHE_TXT
        PANIC("TODO!");
#else  // !ENABLE_SHM_CACHE_TXT
        // Do nothing
#endif // ENABLE_SHM_CACHE_TXT
    }
    else {
#if ENABLE_SHM_CACHE_TXT
        g_customer_shm.attach_fixed(false);
        g_orders_shm.attach_fixed((g_id == 0));
        g_lineitem_shm.attach_fixed(false);
#else  // !ENABLE_SHM_CACHE_TXT
        // Do nothing
#endif // ENABLE_SHM_CACHE_TXT
    }

    // Initialize g_custkey_to_mktid
    g_custkey_to_mktid.attach_fixed((g_id == 0));
#if ENABLE_ASSERTION
    memset(g_custkey_to_mktid.ptr, 0x00, g_custkey_to_mktid.size_in_byte);
#endif

    // Initialize g_orderkey_to_order
    g_orderkey_to_order.attach_fixed((g_id == 0));
#if ENABLE_ASSERTION
    memset(g_orderkey_to_order.ptr, 0x00, g_orderkey_to_order.size_in_byte);
#endif

    // Initialize g_orderkey_to_custkey
    g_orderkey_to_custkey.attach_fixed((g_id == 0));
#if ENABLE_ASSERTION
    memset(g_orderkey_to_custkey.ptr, 0x00, g_orderkey_to_custkey.size_in_byte);
#endif


    // Initialize g_mapping_buffers
    {
        DEBUG("CONFIG_LOAD_TXT_BUFFER_COUNT: %u", CONFIG_LOAD_TXT_BUFFER_COUNT);

        DEBUG("TXT_MAPPING_BUFFER_SIZE: 0x%x (%u)", TXT_MAPPING_BUFFER_SIZE, TXT_MAPPING_BUFFER_SIZE);
        ASSERT(TXT_MAPPING_BUFFER_SIZE % PAGE_SIZE == 0);

        g_txt_mapping_buffer_start_ptr = mmap_reserve_space((size_t)TXT_MAPPING_BUFFER_SIZE * CONFIG_LOAD_TXT_BUFFER_COUNT);
        DEBUG("g_txt_mapping_buffer_start_ptr: %p", g_txt_mapping_buffer_start_ptr);

        static_assert(g_txt_mapping_bag.capacity() == CONFIG_LOAD_TXT_BUFFER_COUNT);
        g_txt_mapping_bag.init([](const size_t idx) -> index32_t {
            ASSERT(idx < CONFIG_LOAD_TXT_BUFFER_COUNT);
            return idx;
        }, CONFIG_LOAD_TXT_BUFFER_COUNT);
    }
}
