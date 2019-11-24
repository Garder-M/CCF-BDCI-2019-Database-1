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
    posix_shm_t<void> g_items_buffer { };

//    void* g_items_buffer_major_start_ptr = nullptr;  // [total_buckets][CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR]
//#if ENABLE_MID_INDEX
//    void* g_items_buffer_mid_start_ptr = nullptr;  // [total_buckets][CONFIG_INDEX_TLS_BUFFER_SIZE_MID]
//#endif
//    void* g_items_buffer_minor_start_ptr = nullptr;  // [total_buckets][CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR]

    std::atomic_uint64_t* g_buckets_endoffset_major = nullptr;  // [g_shared->total_buckets]
#if ENABLE_MID_INDEX
    std::atomic_uint64_t* g_buckets_endoffset_mid = nullptr;  // [g_shared->total_buckets]
#endif
    std::atomic_uint64_t* g_buckets_endoffset_minor = nullptr;  // [g_shared->total_buckets]

    std::uint64_t g_orders_file_max_clear_cache_offset = 0;
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
    }


    // Load orders file to memory
    {
        // TODO: adjust this!
        g_orders_file_max_clear_cache_offset = __align_down(
            g_orders_file.file_size * 3 / 5,
            PAGE_SIZE);
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
            g_orders_file_max_clear_cache_offset);
    }


    // Load lineitem file to memory
    {
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
    while (true) {
        index32_t part_index;
        if (!g_customer_mapping_queue.pop(&part_index)) break;
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
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

            //TODO: #pragma GCC unroll 3
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

        g_txt_mapping_bag.return_back(part_index);
    }

    INFO("[%u] done worker_load_customer_multi_part()", tid);
}


static void worker_load_orders_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
    [[maybe_unused]] const uint8_t mktid_count = g_shared->mktid_count;

    while (true) {

        index32_t part_index;
        if (!g_orders_mapping_queue.pop(&part_index)) break;
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);

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

        g_txt_mapping_bag.return_back(part_index);
    }

    DEBUG("[%u] now unmap! worker_load_orders_multi_part()", tid);

    //
    // Unmap mapped orders texts to allow for clearing page cache
    //
    // NOTE:
    //  We MUST NOT do this in worker threads!
    //  This is a bug! Worker threads here may unmap the "part" which has been mapped to lineitem by loader threads
    //  Then this causes segfault when worker threads later access those unmapped "part"s
    //
    /*
    g_txt_mapping_bag.unsafe_for_each([&](index32_t& part_index) {
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];
        if (__likely(part.fd == g_orders_file.fd)) {
            void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
            C_CALL(munmap(ptr, TXT_MAPPING_BUFFER_SIZE));
            INFO("[%u] unmap orders for part_index=%u", tid, part_index);
        }
    });
    */

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


void worker_load_lineitem_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
    ASSERT(g_orderkey_to_order.ptr != nullptr);
    const order_t* const orderkey_to_order = g_orderkey_to_order.ptr;

    ASSERT(g_shared->mktid_count > 0);
    const uint32_t mktid_count = g_shared->mktid_count;

    uint32_t max_orderdate_shipdate_diff = 0;

    struct iovec_expend_t {
        void* iov_base;
        uint32_t iov_len32;
        uint32_t max_total_expend_cent;
    };
    static_assert(sizeof(iovec_expend_t) == sizeof(iovec));

    //
    // Allocate for g_items_buffer_major_start_ptr, g_items_buffer_mid_start_ptr, g_items_buffer_minor_start_ptr
    //
    ASSERT(g_shared->total_buckets > 0);
    const uint32_t total_buckets = g_shared->total_buckets;

    const uint64_t tls_buffer_size_major = (uint64_t)total_buckets * CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR;
#if ENABLE_MID_INDEX
    const uint64_t tls_buffer_size_mid = (uint64_t)total_buckets * CONFIG_INDEX_TLS_BUFFER_SIZE_MID;
#endif
    const uint64_t tls_buffer_size_minor = (uint64_t)total_buckets * CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR;
    //INFO("tls_buffer_size_major: %lu", tls_buffer_size_major);
    //INFO("tls_buffer_size_mid: %lu", tls_buffer_size_mid);
    //INFO("tls_buffer_size_minor: %lu", tls_buffer_size_minor);

    const uint64_t tls_iovec_size_major = sizeof(iovec) * g_shared->total_buckets;
#if ENABLE_MID_INDEX
    const uint64_t tls_iovec_size_mid = sizeof(iovec_expend_t) * g_shared->total_buckets;
#endif
    const uint64_t tls_iovec_size_minor = sizeof(iovec_expend_t) * g_shared->total_buckets;
    //INFO("tls_iovec_size_major: %lu", tls_iovec_size_major);
    //INFO("tls_iovec_size_mid: %lu", tls_iovec_size_mid);
    //INFO("tls_iovec_size_minor: %lu", tls_iovec_size_minor);

#if ENABLE_MID_INDEX
    const uint64_t tls_total_buffer_size =
        tls_buffer_size_major + tls_buffer_size_mid + tls_buffer_size_minor +
        tls_iovec_size_major + tls_iovec_size_mid + tls_iovec_size_minor;
#else
    const uint64_t tls_total_buffer_size =
        tls_buffer_size_major + tls_buffer_size_minor +
        tls_iovec_size_major + tls_iovec_size_minor;
#endif

    struct saved_buffer_t {
        index32_t index;
        uint32_t iov_len;
    };
    struct extra_buffer_t {
        mpmc_queue<saved_buffer_t, CONFIG_INDEX_EXTRA_BUFFER_COUNT> major_queue { };
        bounded_bag<index32_t, CONFIG_INDEX_EXTRA_BUFFER_COUNT> major_bag { };
#if ENABLE_MID_INDEX
        mpmc_queue<saved_buffer_t, CONFIG_INDEX_EXTRA_BUFFER_COUNT> mid_queue { };
        bounded_bag<index32_t, CONFIG_INDEX_EXTRA_BUFFER_COUNT> mid_bag { };
#endif
        mpmc_queue<saved_buffer_t, CONFIG_INDEX_EXTRA_BUFFER_COUNT> minor_queue { };
        bounded_bag<index32_t, CONFIG_INDEX_EXTRA_BUFFER_COUNT> minor_bag { };

        DISABLE_COPY_MOVE_CONSTRUCTOR(extra_buffer_t);
        extra_buffer_t() noexcept {
            major_bag.init(
                [](const size_t idx) noexcept -> index32_t { return g_total_process_count + idx; },
                CONFIG_INDEX_EXTRA_BUFFER_COUNT);
#if ENABLE_MID_INDEX
            mid_bag.init(
                [](const size_t idx) noexcept -> index32_t { return g_total_process_count + idx; },
                CONFIG_INDEX_EXTRA_BUFFER_COUNT);
#endif
            minor_bag.init(
                [](const size_t idx) noexcept -> index32_t { return g_total_process_count + idx; },
                CONFIG_INDEX_EXTRA_BUFFER_COUNT);
        }
    };

    g_items_buffer.init_fixed(
        SHMKEY_ITEMS_BUFFER,
        tls_total_buffer_size * (g_total_process_count + CONFIG_INDEX_EXTRA_BUFFER_COUNT) + sizeof(extra_buffer_t) * total_buckets,
        true);
    ASSERT(g_items_buffer.ptr != nullptr);
    DEBUG("[%u] g_items_buffer.ptr: %p", tid, g_items_buffer.ptr);
    DEBUG("[%u] g_items_buffer.size_in_byte: %lu", tid, g_items_buffer.size_in_byte);
    DEBUG("[%u] g_items_buffer.shmid: %u", tid, g_items_buffer.shmid);

    g_shared->worker_sync_barrier.sync();  // this sync is necessary!
    g_items_buffer.attach_fixed((g_id == 0));

    extra_buffer_t* const extra_buffers = (extra_buffer_t*)((uintptr_t)g_items_buffer.ptr + tls_total_buffer_size * (g_total_process_count + CONFIG_INDEX_EXTRA_BUFFER_COUNT));
    g_shared->worker_sync_barrier.run_once_and_sync([&]() {
        for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
            new (&extra_buffers[bucket_id]) extra_buffer_t;
        }
    });  // this sync is necessary!


    struct {
        uintptr_t buffer_start_ptr;
        void* items_buffer_major_start_ptr;
#if ENABLE_MID_INDEX
        void* items_buffer_mid_start_ptr;
#endif
        void* items_buffer_minor_start_ptr;
        iovec* bucket_data_major;
#if ENABLE_MID_INDEX
        iovec_expend_t* bucket_data_mid;
#endif
        iovec_expend_t* bucket_data_minor;
    } iovec_contexts[g_total_process_count + CONFIG_INDEX_EXTRA_BUFFER_COUNT];

    for (uint32_t id = 0; id < g_total_process_count + CONFIG_INDEX_EXTRA_BUFFER_COUNT; ++id) {
        auto& ctx = iovec_contexts[id];
        ctx.buffer_start_ptr = (uintptr_t)g_items_buffer.ptr + id * tls_total_buffer_size;

        uint64_t tmp_offset = 0;
        ctx.items_buffer_major_start_ptr = (void*)(ctx.buffer_start_ptr + tmp_offset);
        tmp_offset += tls_buffer_size_major;

#if ENABLE_MID_INDEX
        ctx.items_buffer_mid_start_ptr = (void*)(ctx.buffer_start_ptr + tmp_offset);
        tmp_offset += tls_buffer_size_mid;
#endif

        ctx.items_buffer_minor_start_ptr = (void*)(ctx.buffer_start_ptr + tmp_offset);
        tmp_offset += tls_buffer_size_minor;

        ctx.bucket_data_major = (iovec*)(ctx.buffer_start_ptr + tmp_offset);
        tmp_offset += tls_iovec_size_major;

#if ENABLE_MID_INDEX
        ctx.bucket_data_mid = (iovec_expend_t*)(ctx.buffer_start_ptr + tmp_offset);
        tmp_offset += tls_iovec_size_mid;
#endif

        ctx.bucket_data_minor = (iovec_expend_t*)(ctx.buffer_start_ptr + tmp_offset);
        tmp_offset += tls_iovec_size_minor;

        ASSERT(tmp_offset == tls_total_buffer_size);
    }

    //const uintptr_t tls_buffer_start_ptr = iovec_contexts[g_id].buffer_start_ptr;
//    g_items_buffer_major_start_ptr = iovec_contexts[g_id].items_buffer_major_start_ptr;
//#if ENABLE_MID_INDEX
//    g_items_buffer_mid_start_ptr = iovec_contexts[g_id].items_buffer_mid_start_ptr;
//#endif
//    g_items_buffer_minor_start_ptr = iovec_contexts[g_id].items_buffer_minor_start_ptr;

    iovec* const bucket_data_major = iovec_contexts[g_id].bucket_data_major;
    uint32_t bucket_buffer_id_major[total_buckets];
    for (uint32_t bucket_id = 0; bucket_id < total_buckets; ++bucket_id) {
        bucket_buffer_id_major[bucket_id] = g_id;
    }
#if ENABLE_MID_INDEX
    iovec_expend_t* const bucket_data_mid = iovec_contexts[g_id].bucket_data_mid;
    uint32_t bucket_buffer_id_mid[total_buckets];
    for (uint32_t bucket_id = 0; bucket_id < total_buckets; ++bucket_id) {
        bucket_buffer_id_mid[bucket_id] = g_id;
    }
#endif
    iovec_expend_t* const bucket_data_minor = iovec_contexts[g_id].bucket_data_minor;
    uint32_t bucket_buffer_id_minor[total_buckets];
    for (uint32_t bucket_id = 0; bucket_id < total_buckets; ++bucket_id) {
        bucket_buffer_id_minor[bucket_id] = g_id;
    }

#define _CALC_START_PTR_MAJOR(_BufferId, _BucketId_) \
    ((void*)((uintptr_t)iovec_contexts[(_BufferId)].items_buffer_major_start_ptr + (size_t)(_BucketId_) * CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR))
#define _CALC_START_PTR_MID(_BufferId, _BucketId_) \
    ((void*)((uintptr_t)iovec_contexts[(_BufferId)].items_buffer_mid_start_ptr + (size_t)(_BucketId_) * CONFIG_INDEX_TLS_BUFFER_SIZE_MID))
#define _CALC_START_PTR_MINOR(_BufferId, _BucketId_) \
    ((void*)((uintptr_t)iovec_contexts[(_BufferId)].items_buffer_minor_start_ptr + (size_t)(_BucketId_) * CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR))


    ASSERT(bucket_data_major != nullptr);
    for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
        bucket_data_major[bucket_id].iov_len = 0;
        bucket_data_major[bucket_id].iov_base = _CALC_START_PTR_MAJOR(g_id, bucket_id);
    }
#if ENABLE_MID_INDEX
    ASSERT(bucket_data_mid != nullptr);
    for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
        bucket_data_mid[bucket_id].max_total_expend_cent = 0;
        bucket_data_mid[bucket_id].iov_len32 = 0;
        bucket_data_mid[bucket_id].iov_base = _CALC_START_PTR_MID(g_id, bucket_id);
    }
#endif
    ASSERT(bucket_data_minor != nullptr);
    for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
        bucket_data_minor[bucket_id].max_total_expend_cent = 0;
        bucket_data_minor[bucket_id].iov_len32 = 0;
        bucket_data_minor[bucket_id].iov_base = _CALC_START_PTR_MINOR(g_id, bucket_id);
    }


    //
    // Open and map only_xxx
    //
    {
#if ENABLE_MID_INDEX
        ASSERT(g_only_mid_max_expend_file.fd > 0);
        g_only_mid_max_expend_file.file_size = sizeof(uint32_t) * total_buckets;
        ASSERT(g_only_mid_max_expend_file.file_size > 0);
#endif
        ASSERT(g_only_minor_max_expend_file.fd > 0);
        g_only_minor_max_expend_file.file_size = sizeof(uint32_t) * total_buckets;
        ASSERT(g_only_minor_max_expend_file.file_size > 0);

        g_shared->worker_sync_barrier.run_once_and_sync([]() {
#if ENABLE_MID_INDEX
            C_CALL(ftruncate(g_only_mid_max_expend_file.fd, g_only_mid_max_expend_file.file_size));
            INFO("g_only_mid_max_expend_file.file_size: %lu", g_only_mid_max_expend_file.file_size);
#endif
            C_CALL(ftruncate(g_only_minor_max_expend_file.fd, g_only_minor_max_expend_file.file_size));
            INFO("g_only_minor_max_expend_file.file_size: %lu", g_only_minor_max_expend_file.file_size);
        });

#if ENABLE_MID_INDEX
        g_only_mid_max_expend_start_ptr = (uint32_t*)my_mmap(
            g_only_mid_max_expend_file.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_only_mid_max_expend_file.fd,
            0);
        DEBUG("[%u] g_only_mid_max_expend_start_ptr: %p", tid, g_only_mid_max_expend_start_ptr);
#endif

        g_only_minor_max_expend_start_ptr = (uint32_t*)my_mmap(
            g_only_minor_max_expend_file.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_only_minor_max_expend_file.fd,
            0);
        DEBUG("[%u] g_only_minor_max_expend_start_ptr: %p", tid, g_only_minor_max_expend_start_ptr);
    }


    // Truncate for g_endoffset_file
    {
        ASSERT(g_shared->total_buckets > 0);

        ASSERT(g_endoffset_file_major.fd > 0);
#if ENABLE_MID_INDEX
        ASSERT(g_endoffset_file_mid.fd > 0);
#endif
        ASSERT(g_endoffset_file_minor.fd > 0);

        g_endoffset_file_major.file_size = sizeof(uint64_t) * g_shared->total_buckets;
#if ENABLE_MID_INDEX
        g_endoffset_file_mid.file_size = sizeof(uint64_t) * g_shared->total_buckets;
#endif
        g_endoffset_file_minor.file_size = sizeof(uint64_t) * g_shared->total_buckets;
        ASSERT(g_endoffset_file_major.file_size > 0);
#if ENABLE_MID_INDEX
        ASSERT(g_endoffset_file_mid.file_size > 0);
#endif
        ASSERT(g_endoffset_file_minor.file_size > 0);

        g_shared->worker_sync_barrier.run_once_and_sync([]() {
            C_CALL(ftruncate(g_endoffset_file_major.fd, g_endoffset_file_major.file_size));
#if ENABLE_MID_INDEX
            C_CALL(ftruncate(g_endoffset_file_mid.fd, g_endoffset_file_mid.file_size));
#endif
            C_CALL(ftruncate(g_endoffset_file_minor.fd, g_endoffset_file_minor.file_size));
        });

        g_buckets_endoffset_major = (std::atomic_uint64_t*)my_mmap(
            g_endoffset_file_major.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_endoffset_file_major.fd,
            0);
        DEBUG("[%u] g_buckets_endoffset_major: %p", tid, g_buckets_endoffset_major);

#if ENABLE_MID_INDEX
        g_buckets_endoffset_mid = (std::atomic_uint64_t*)my_mmap(
            g_endoffset_file_mid.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_endoffset_file_mid.fd,
            0);
        DEBUG("[%u] g_buckets_endoffset_mid: %p", tid, g_buckets_endoffset_mid);
#endif

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

#if ENABLE_MID_INDEX
        const uint64_t holder_mid_file_size = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MID * g_shared->buckets_per_holder;
        while (true) {
            const uint32_t holder_id = g_shared->next_truncate_holder_mid_id++;
            if (holder_id >= CONFIG_INDEX_HOLDER_COUNT) break;

            ASSERT(g_holder_files_mid_fd[holder_id] > 0);
            C_CALL(ftruncate(g_holder_files_mid_fd[holder_id], holder_mid_file_size));

            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;
            const uint32_t end_bucket_id = (holder_id + 1) * g_shared->buckets_per_holder;
            for (uint32_t bucket_id = begin_bucket_id; bucket_id < end_bucket_id; ++bucket_id) {
                g_buckets_endoffset_mid[bucket_id] = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MID * (bucket_id - begin_bucket_id);
            }
        }
#endif

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

        // This is necessary
        g_shared->worker_sync_barrier.sync();
    }



    const auto maybe_submit_for_pwrite_major = [&](/*inout*/ iovec& vec, /*in*/ uint32_t bucket_id) {
        //ASSERT(bucket_id < total_buckets, "bucket_id: %u", bucket_id);
        ASSERT(vec.iov_len == CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR);
        ASSERT(vec.iov_len <= UINT32_MAX);
        ASSERT(vec.iov_len > 0);
        ASSERT(vec.iov_base == _CALC_START_PTR_MAJOR(bucket_buffer_id_major[bucket_id], bucket_id));

        extra_buffer_t& extra = extra_buffers[bucket_id];
        uint32_t new_bucket_buffer_id_major;

        if (extra.major_bag.try_take(&new_bucket_buffer_id_major)) {
            // Push current vec into queue
            extra.major_queue.push({bucket_buffer_id_major[bucket_id], (uint32_t)vec.iov_len});

            //INFO("[%u] <1> bucket_id=%u,bucket_buffer_id_major=%u,new=%u,iov_base=%p,new_iov_base=%p",
            //    g_id, bucket_id, bucket_buffer_id_major[bucket_id], new_bucket_buffer_id_major, vec.iov_base,
            //    _CALC_START_PTR_MAJOR(new_bucket_buffer_id_major, bucket_id));

            bucket_buffer_id_major[bucket_id] = new_bucket_buffer_id_major;
            vec.iov_base = _CALC_START_PTR_MAJOR(new_bucket_buffer_id_major, bucket_id);
            vec.iov_len = 0;
        }
        else {  // extra's bag is empty... now we do pwritev
            iovec vecs[CONFIG_INDEX_EXTRA_BUFFER_COUNT + 1];
            uint32_t buffer_ids[CONFIG_INDEX_EXTRA_BUFFER_COUNT + 1];
            vecs[0] = vec;
            // buffer_ids[0] = dummy_not_used;
            uint32_t vec_count = 1;
            uint64_t vec_total_len = vec.iov_len;

            saved_buffer_t saved;
            while (extra.major_queue.try_pop(&saved)) {
                ASSERT(saved.iov_len > 0);
                vecs[vec_count].iov_base = _CALC_START_PTR_MAJOR(saved.index, bucket_id);
                vecs[vec_count].iov_len = saved.iov_len;
                buffer_ids[vec_count] = saved.index;
                ++vec_count;
                vec_total_len += saved.iov_len;
            }

            const uint64_t file_offset = g_buckets_endoffset_major[bucket_id].fetch_add(vec_total_len);
            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            const size_t cnt = C_CALL(pwritev(
                g_holder_files_major_fd[holder_id],
                vecs,
                vec_count,
                file_offset));
            CHECK(cnt == vec_total_len);

            //INFO("[%u] <2> pwrite! current: bucket_buffer_id_major=%u,iov_base=%p",
            //     g_id, bucket_buffer_id_major, vec.iov_base);

            extra.major_bag.return_back_many(&buffer_ids[1], vec_count - 1);

            // We just keep using "vec", but clear its iov_len
            // bucket_buffer_id_major not changed
            // vec.iov_base not changed
            vec.iov_len = 0;
        }
    };

#if ENABLE_MID_INDEX
    const auto maybe_submit_for_pwrite_mid = [&](/*inout*/ iovec_expend_t& vec, /*in*/ uint32_t bucket_id) {
        //ASSERT(bucket_id < total_buckets, "bucket_id: %u", bucket_id);
        ASSERT(vec.iov_len32 == CONFIG_INDEX_TLS_BUFFER_SIZE_MID);
        ASSERT(vec.iov_len32 <= UINT32_MAX);
        ASSERT(vec.iov_len32 > 0);
        ASSERT(vec.iov_base == _CALC_START_PTR_MID(bucket_buffer_id_mid[bucket_id], bucket_id));

        extra_buffer_t& extra = extra_buffers[bucket_id];
        uint32_t new_bucket_buffer_id_mid;

        if (extra.mid_bag.try_take(&new_bucket_buffer_id_mid)) {
            // Push current vec into queue
            extra.mid_queue.push({bucket_buffer_id_mid[bucket_id], (uint32_t)vec.iov_len32});

            //INFO("[%u] <1> bucket_id=%u,bucket_buffer_id_mid=%u,new=%u,iov_base=%p,new_iov_base=%p",
            //    g_id, bucket_id, bucket_buffer_id_mid[bucket_id], new_bucket_buffer_id_mid, vec.iov_base,
            //    _CALC_START_PTR_MID(new_bucket_buffer_id_mid, bucket_id));

            bucket_buffer_id_mid[bucket_id] = new_bucket_buffer_id_mid;
            vec.iov_base = _CALC_START_PTR_MID(new_bucket_buffer_id_mid, bucket_id);
            vec.iov_len32 = 0;
        }
        else {  // extra's bag is empty... now we do pwritev
            iovec vecs[CONFIG_INDEX_EXTRA_BUFFER_COUNT + 1];
            uint32_t buffer_ids[CONFIG_INDEX_EXTRA_BUFFER_COUNT + 1];
            vecs[0] = { vec.iov_base, vec.iov_len32 };
            // buffer_ids[0] = dummy_not_used;
            uint32_t vec_count = 1;
            uint64_t vec_total_len = vec.iov_len32;

            saved_buffer_t saved;
            while (extra.mid_queue.try_pop(&saved)) {
                ASSERT(saved.iov_len > 0);
                vecs[vec_count].iov_base = _CALC_START_PTR_MID(saved.index, bucket_id);
                vecs[vec_count].iov_len = saved.iov_len;
                buffer_ids[vec_count] = saved.index;
                ++vec_count;
                vec_total_len += saved.iov_len;
            }

            const uint64_t file_offset = g_buckets_endoffset_mid[bucket_id].fetch_add(vec_total_len);
            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            const size_t cnt = C_CALL(pwritev(
                g_holder_files_mid_fd[holder_id],
                vecs,
                vec_count,
                file_offset));
            CHECK(cnt == vec_total_len);

            //INFO("[%u] <2> pwrite! current: bucket_buffer_id_mid=%u,iov_base=%p",
            //     g_id, bucket_buffer_id_mid, vec.iov_base);

            extra.mid_bag.return_back_many(&buffer_ids[1], vec_count - 1);

            // We just keep using "vec", but clear its iov_len
            // bucket_buffer_id_mid not changed
            // vec.iov_base not changed
            vec.iov_len32 = 0;
        }
    };
#endif

    const auto maybe_submit_for_pwrite_minor = [&](/*inout*/ iovec_expend_t& vec, /*in*/ uint32_t bucket_id) {
        //ASSERT(bucket_id < total_buckets, "bucket_id: %u", bucket_id);
        ASSERT(vec.iov_len32 == CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR);
        ASSERT(vec.iov_len32 <= UINT32_MAX);
        ASSERT(vec.iov_len32 > 0);
        ASSERT(vec.iov_base == _CALC_START_PTR_MINOR(bucket_buffer_id_minor[bucket_id], bucket_id));

        extra_buffer_t& extra = extra_buffers[bucket_id];
        uint32_t new_bucket_buffer_id_minor;

        if (extra.minor_bag.try_take(&new_bucket_buffer_id_minor)) {
            // Push current vec into queue
            extra.minor_queue.push({bucket_buffer_id_minor[bucket_id], (uint32_t)vec.iov_len32});

            bucket_buffer_id_minor[bucket_id] = new_bucket_buffer_id_minor;
            vec.iov_base = _CALC_START_PTR_MINOR(new_bucket_buffer_id_minor, bucket_id);
            vec.iov_len32 = 0;
        }
        else {  // extra's bag is empty... now we do pwritev
            iovec vecs[CONFIG_INDEX_EXTRA_BUFFER_COUNT + 1];
            uint32_t buffer_ids[CONFIG_INDEX_EXTRA_BUFFER_COUNT + 1];
            vecs[0] = { vec.iov_base, vec.iov_len32 };
            // buffer_ids[0] = dummy_not_used;
            uint32_t vec_count = 1;
            uint64_t vec_total_len = vec.iov_len32;

            saved_buffer_t saved;
            while (extra.minor_queue.try_pop(&saved)) {
                ASSERT(saved.iov_len > 0);
                vecs[vec_count].iov_base = _CALC_START_PTR_MINOR(saved.index, bucket_id);
                vecs[vec_count].iov_len = saved.iov_len;
                buffer_ids[vec_count] = saved.index;
                ++vec_count;
                vec_total_len += saved.iov_len;
            }

            const uint64_t file_offset = g_buckets_endoffset_minor[bucket_id].fetch_add(vec_total_len);
            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            const size_t cnt = C_CALL(pwritev(
                g_holder_files_minor_fd[holder_id],
                vecs,
                vec_count,
                file_offset));
            CHECK(cnt == vec_total_len);

            extra.minor_bag.return_back_many(&buffer_ids[1], vec_count - 1);

            // We just keep using "vec", but clear its iov_len
            // bucket_buffer_id_minor not changed
            // vec.iov_base not changed
            vec.iov_len32 = 0;
        }
    };
    

    static constexpr const uint32_t COUNT_BASE = 8;
    uint32_t last_items[COUNT_BASE + 8];  // start index: COUNT_BASE
    for (uint32_t i = 0; i < COUNT_BASE; ++i) {
        last_items[i] = (/*dummy*/0 << 24) | 0x00000000;
    }


    while (true) {
        index32_t part_index;
        if (!g_lineitem_mapping_queue.pop(&part_index)) break;
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
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
        uint32_t last_total_expend_cent;


        const auto append_current_order_to_index = [&]() {
            ASSERT(last_item_count >= COUNT_BASE + 1);
            ASSERT(last_item_count <= COUNT_BASE + 7);

            ASSERT(last_total_expend_cent > 0);

#if ENABLE_MID_INDEX
            if (last_item_count >= COUNT_BASE + 6) {  // 7,6
                ASSERT(last_item_count <= COUNT_BASE + 7);
                last_items[last_item_count++] = (last_orderdate - last_bucket_base_orderdate) << 30 | last_orderkey;

                iovec& vec = bucket_data_major[last_bucket_id];
                ASSERT(vec.iov_len < CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR);
                ASSERT(vec.iov_base == _CALC_START_PTR_MAJOR(bucket_buffer_id_major[last_bucket_id], last_bucket_id));
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
            else if (last_item_count >= COUNT_BASE + 4) {  // 5,4
                ASSERT(last_item_count <= COUNT_BASE + 5);
                last_items[last_item_count++] = last_total_expend_cent;
                last_items[last_item_count++] = (last_orderdate - last_bucket_base_orderdate) << 30 | last_orderkey;

                iovec_expend_t& vec = bucket_data_mid[last_bucket_id];
                if (vec.max_total_expend_cent < last_total_expend_cent) {
                    vec.max_total_expend_cent = last_total_expend_cent;
                }
                ASSERT(vec.iov_len32 < CONFIG_INDEX_TLS_BUFFER_SIZE_MID);
                ASSERT(vec.iov_base == _CALC_START_PTR_MID(bucket_buffer_id_mid[last_bucket_id], last_bucket_id));
                static_assert(CONFIG_INDEX_TLS_BUFFER_SIZE_MID % (8 * sizeof(uint32_t)) == 0);

                memcpy(
                    (void*)((uintptr_t)vec.iov_base + vec.iov_len32),
                    last_items + last_item_count - 8,
                    8 * sizeof(uint32_t));
                vec.iov_len32 += 8 * sizeof(uint32_t);

                ASSERT(vec.iov_len32 <= CONFIG_INDEX_TLS_BUFFER_SIZE_MID);
                if (vec.iov_len32 == CONFIG_INDEX_TLS_BUFFER_SIZE_MID) {
                    maybe_submit_for_pwrite_mid(vec, last_bucket_id);
                }
            }
#else  // !ENABLE_MID_INDEX
            if (last_item_count >= COUNT_BASE + 4) {  // 7,6,5,4
                ASSERT(last_item_count <= COUNT_BASE + 7);
                last_items[last_item_count++] = (last_orderdate - last_bucket_base_orderdate) << 30 | last_orderkey;

                iovec& vec = bucket_data_major[last_bucket_id];
                ASSERT(vec.iov_len < CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR);
                ASSERT(vec.iov_base == _CALC_START_PTR_MAJOR(bucket_buffer_id_major[last_bucket_id], last_bucket_id),
                    "[%u] last_bucket_id=%u,bucket_buffer_id_major=%u,vec.iov_len=%lu,vec.iov_base=%p,_CALC_START_PTR_MAJOR=%p",
                    tid, last_bucket_id, bucket_buffer_id_major[last_bucket_id], vec.iov_len, vec.iov_base,
                    _CALC_START_PTR_MAJOR(bucket_buffer_id_major[last_bucket_id], last_bucket_id));
                static_assert(CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR % (8 * sizeof(uint32_t)) == 0);

                memcpy(
                    (void*)((uintptr_t)vec.iov_base + vec.iov_len),
                    last_items + last_item_count - 8,
                    8 * sizeof(uint32_t));
                vec.iov_len += 8 * sizeof(uint32_t);

                ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR);
                if (vec.iov_len == CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR) {
                    //INFO("[%u] BEFORE: last_bucket_id=%u,bucket_buffer_id_major=%u,vec.iov_len=%lu,vec.iov_base=%p,_CALC_START_PTR_MAJOR=%p",
                    //    tid, last_bucket_id, bucket_buffer_id_major[last_bucket_id], vec.iov_len, vec.iov_base,
                    //    _CALC_START_PTR_MAJOR(bucket_buffer_id_major[last_bucket_id], last_bucket_id));
                    maybe_submit_for_pwrite_major(vec, last_bucket_id);
                    //INFO("[%u] AFTER: last_bucket_id=%u,bucket_buffer_id_major=%u,vec.iov_len=%lu,vec.iov_base=%p,_CALC_START_PTR_MAJOR=%p",
                    //     tid, last_bucket_id, bucket_buffer_id_major[last_bucket_id], vec.iov_len, vec.iov_base,
                    //     _CALC_START_PTR_MAJOR(bucket_buffer_id_major[last_bucket_id], last_bucket_id));
                }
            }
#endif  // ENABLE_MID_INDEX

            else {  // 3,2,1
                ASSERT(last_item_count <= COUNT_BASE + 3);
                ASSERT(last_item_count >= COUNT_BASE + 1);
                last_items[last_item_count++] = (last_orderdate - last_bucket_base_orderdate) << 30 | last_orderkey;

                iovec_expend_t& vec = bucket_data_minor[last_bucket_id];
                if (vec.max_total_expend_cent < last_total_expend_cent) {
                    vec.max_total_expend_cent = last_total_expend_cent;
                }
                ASSERT(vec.iov_len32 <= CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR);
                ASSERT(vec.iov_base == _CALC_START_PTR_MINOR(bucket_buffer_id_minor[last_bucket_id], last_bucket_id));
                static_assert(CONFIG_INDEX_TLS_BUFFER_SIZE_MAJOR % (4 * sizeof(uint32_t)) == 0);

                memcpy(
                    (void*)((uintptr_t)vec.iov_base + vec.iov_len32),
                    last_items + last_item_count - 4,
                    4 * sizeof(uint32_t));
                vec.iov_len32 += 4 * sizeof(uint32_t);

                ASSERT(vec.iov_len32 <= CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR);
                if (vec.iov_len32 == CONFIG_INDEX_TLS_BUFFER_SIZE_MINOR) {
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
            last_total_expend_cent = expend_cent;
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
                last_total_expend_cent += expend_cent;
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
                last_total_expend_cent = expend_cent;
            }


            if (__unlikely(p >= valid_end)) {
                ASSERT(p == valid_end);

                // Save current items to index buffer
                append_current_order_to_index();

                break;
            }
        }

        g_txt_mapping_bag.return_back(part_index);
    }


    //
    // For each bucket, write remaining buffer (if exists) to file
    //
    INFO("[%u] write remaining buffer (if exists) to file", tid);
    uint32_t bucket_ids_to_shuffle[g_shared->total_buckets];
    for (uint32_t i = 0; i < g_shared->total_buckets; ++i) {
        bucket_ids_to_shuffle[i] = i;
    }
    std::mt19937 g(0x23336666); // NOLINT(cert-msc32-c,cert-msc51-cpp)
    std::shuffle(bucket_ids_to_shuffle, bucket_ids_to_shuffle + g_shared->total_buckets, g);

    g_shared->worker_sync_barrier.sync();  // this sync is necessary!
    while (true) {
        const uint32_t bucket_id_index = g_shared->write_tail_bucket_id_shared_counter++;
        if (bucket_id_index >= g_shared->total_buckets) break;
        const uint32_t bucket_id = bucket_ids_to_shuffle[bucket_id_index];

        iovec vecs[g_total_process_count + CONFIG_INDEX_EXTRA_BUFFER_COUNT];
        {
            uint32_t vec_count = 0;
            uint64_t vec_total_len = 0;
            for (uint32_t id = 0; id < g_total_process_count; ++id) {
                const iovec& vec_major = iovec_contexts[id].bucket_data_major[bucket_id];
                if (vec_major.iov_len > 0) {
                    vec_total_len += vec_major.iov_len;
                    vecs[vec_count++] = vec_major;
                }
            }

            saved_buffer_t saved;
            while (extra_buffers[bucket_id].major_queue.try_pop(&saved)) {
                ASSERT(saved.iov_len > 0);
                vec_total_len += saved.iov_len;
                vecs[vec_count].iov_base = _CALC_START_PTR_MAJOR(saved.index, bucket_id);
                vecs[vec_count].iov_len = saved.iov_len;
                ++vec_count;
            }

            if (__likely(vec_count > 0)) {
                const uint64_t file_offset = g_buckets_endoffset_major[bucket_id].fetch_add(vec_total_len);
                const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
                const size_t cnt = C_CALL(pwritev(
                    g_holder_files_major_fd[holder_id],
                    vecs,
                    vec_count,
                    file_offset));
                CHECK(cnt == vec_total_len);
            }
        }
#if ENABLE_MID_INDEX
        {
            uint32_t vec_count = 0;
            uint64_t vec_total_len = 0;
            for (uint32_t id = 0; id < g_total_process_count; ++id) {
                const iovec_expend_t& vec_mid = iovec_contexts[id].bucket_data_mid[bucket_id];
                if (vec_mid.iov_len32 > 0) {
                    vec_total_len += vec_mid.iov_len32;
                    vecs[vec_count++] = { vec_mid.iov_base, vec_mid.iov_len32 };
                }
            }

            saved_buffer_t saved;
            while (extra_buffers[bucket_id].mid_queue.try_pop(&saved)) {
                ASSERT(saved.iov_len > 0);
                vec_total_len += saved.iov_len;
                vecs[vec_count].iov_base = _CALC_START_PTR_MID(saved.index, bucket_id);
                vecs[vec_count].iov_len = saved.iov_len;
                ++vec_count;
            }
            
            if (__likely(vec_count > 0)) {
                const uint64_t file_offset = g_buckets_endoffset_mid[bucket_id].fetch_add(vec_total_len);
                const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
                const size_t cnt = C_CALL(pwritev(
                    g_holder_files_mid_fd[holder_id],
                    vecs,
                    vec_count,
                    file_offset));
                CHECK(cnt == vec_total_len);
            }
        }
#endif
        {
            uint32_t vec_count = 0;
            uint64_t vec_total_len = 0;
            for (uint32_t id = 0; id < g_total_process_count; ++id) {
                const iovec_expend_t& vec_minor = iovec_contexts[id].bucket_data_minor[bucket_id];
                if (vec_minor.iov_len32 > 0) {
                    vec_total_len += vec_minor.iov_len32;
                    vecs[vec_count++] = { vec_minor.iov_base, vec_minor.iov_len32 };
                }
            }

            saved_buffer_t saved;
            while (extra_buffers[bucket_id].minor_queue.try_pop(&saved)) {
                ASSERT(saved.iov_len > 0);
                vec_total_len += saved.iov_len;
                vecs[vec_count].iov_base = _CALC_START_PTR_MINOR(saved.index, bucket_id);
                vecs[vec_count].iov_len = saved.iov_len;
                ++vec_count;
            }
            
            if (__likely(vec_count > 0)) {
                const uint64_t file_offset = g_buckets_endoffset_minor[bucket_id].fetch_add(vec_total_len);
                const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
                const size_t cnt = C_CALL(pwritev(
                    g_holder_files_minor_fd[holder_id],
                    vecs,
                    vec_count,
                    file_offset));
                CHECK(cnt == vec_total_len);
            }
        }
    }


    //
    // Update only_mid_max_expend, only_minor_max_expend
    //
    {
        while (true) {
            const uint32_t bucket_id = g_shared->only_xxx_max_expend_cent_bucket_id_shared_counter++;
            if (bucket_id >= total_buckets) break;

#if ENABLE_MID_INDEX
            ASSERT(g_only_mid_max_expend_start_ptr != nullptr);
            uint32_t mid_max_expend_cent = 0;
            for (uint32_t id = 0; id < g_total_process_count; ++id) {
                const uint32_t tmp = iovec_contexts[id].bucket_data_mid[bucket_id].max_total_expend_cent;
                if (mid_max_expend_cent < tmp) {
                    mid_max_expend_cent = tmp;
                }
            }
            g_only_mid_max_expend_start_ptr[bucket_id] = mid_max_expend_cent;
#endif

            ASSERT(g_only_minor_max_expend_start_ptr != nullptr);
            uint32_t minor_max_expend_cent = 0;
            for (uint32_t id = 0; id < g_total_process_count; ++id) {
                const uint32_t tmp = iovec_contexts[id].bucket_data_minor[bucket_id].max_total_expend_cent;
                if (minor_max_expend_cent < tmp) {
                    minor_max_expend_cent = tmp;
                }
            }
            g_only_minor_max_expend_start_ptr[bucket_id] = minor_max_expend_cent;
        }

        g_shared->worker_sync_barrier.sync();  // this sync is necessary!
    }


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
            ASSERT(total_expend_cent##N > 0); \
            ASSERT(total_expend_cent##N < (1U << 28)); \
            if (topn_count < CONFIG_EXPECT_MAX_TOPN) { \
                const uint32_t orderkey##N = *(p + 7) & ~0xC0000000U; \
                const uint32_t bucket_orderdate_diff##N = *(p + 7) >> 30; \
                const date_t plate_orderdate_diff##N = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff##N; \
                \
                ASSERT(orderkey##N > 0); \
                ASSERT(orderkey##N < (1U << 30)); \
                ASSERT(orderkey##N <= g_max_orderkey, "orderkey" #N " too large: %u", orderkey##N); \
                ASSERT(plate_orderdate_diff##N >= 0); \
                ASSERT(plate_orderdate_diff##N < (1 << 6)); \
                const uint64_t value = (uint64_t)(total_expend_cent##N) << 36 | (uint64_t)(orderkey##N) << 6 | (plate_orderdate_diff##N); \
                \
                topn_ptr[topn_count++] = value; \
                if (__unlikely(topn_count == CONFIG_EXPECT_MAX_TOPN)) { \
                    std::make_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>()); \
                } \
            } \
            else { \
                if (total_expend_cent##N >= (uint32_t)(topn_ptr[0] >> 36)) { \
                    const uint32_t orderkey##N = *(p + 7) & ~0xC0000000U; \
                    const uint32_t bucket_orderdate_diff##N = *(p + 7) >> 30; \
                    const date_t plate_orderdate_diff##N = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff##N; \
                    \
                    ASSERT(orderkey##N > 0); \
                    ASSERT(orderkey##N < (1U << 30)); \
                    ASSERT(orderkey##N <= g_max_orderkey, "orderkey" #N " too large: %u", orderkey##N); \
                    ASSERT(plate_orderdate_diff##N >= 0); \
                    ASSERT(plate_orderdate_diff##N < (1 << 6)); \
                    const uint64_t value = (uint64_t)(total_expend_cent##N) << 36 | (uint64_t)(orderkey##N) << 6 | (plate_orderdate_diff##N); \
                    \
                    if (value > topn_ptr[0]) { \
                        modify_heap(topn_ptr, CONFIG_EXPECT_MAX_TOPN, value, std::greater<>()); \
                    } \
                } \
            } \
        } while(false)

    while (p < end_align32) {
        const __m256i items1 = _mm256_and_si256(_mm256_load_si256((__m256i*)p), expend_mask);
        const __m256i items2 = _mm256_and_si256(_mm256_load_si256((__m256i*)p + 1), expend_mask);
        const __m256i items3 = _mm256_and_si256(_mm256_load_si256((__m256i*)p + 2), expend_mask);
        const __m256i items4 = _mm256_and_si256(_mm256_load_si256((__m256i*)p + 3), expend_mask);

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
        p += 8;

        _CHECK_RESULT(2);
        p += 8;

        _CHECK_RESULT(3);
        p += 8;

        _CHECK_RESULT(4);
        p += 8;
#undef _CHECK_RESULT
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

        {
            ASSERT(orderkey > 0, "");
            ASSERT(orderkey < (1U << 30));
            ASSERT(orderkey <= g_max_orderkey, "orderkey too large: %u", orderkey);
            ASSERT(total_expend_cent > 0, "orderkey: %u", orderkey);
            ASSERT(total_expend_cent < (1U << 28));
            ASSERT(plate_orderdate_diff >= 0);
            ASSERT(plate_orderdate_diff < (1 << 6));
            const uint64_t value = (uint64_t)(total_expend_cent) << 36 | (uint64_t)(orderkey) << 6 | (plate_orderdate_diff);

            if (__unlikely(topn_count < CONFIG_EXPECT_MAX_TOPN)) {
                topn_ptr[topn_count++] = value;
                if (__unlikely(topn_count == CONFIG_EXPECT_MAX_TOPN)) {
                    std::make_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>());
                }
            }
            else {
                if (value > topn_ptr[0]) {
                    modify_heap(topn_ptr, CONFIG_EXPECT_MAX_TOPN, value, std::greater<>());
                }
            }
        }
    }
}


#if ENABLE_MID_INDEX
static void worker_compute_pretopn_for_plate_mid(
    /*in*/ const date_t bucket_base_orderdate_minus_plate_base_orderdate,
    /*in*/ const void* const bucket_ptr_mid,
    /*in*/ const uint64_t bucket_size_mid,
    /*inout*/ uint32_t& topn_count,
    /*inout*/ uint64_t* const topn_ptr) noexcept
{
    ASSERT(bucket_base_orderdate_minus_plate_base_orderdate >= 0);
    ASSERT(bucket_base_orderdate_minus_plate_base_orderdate % CONFIG_ORDERDATES_PER_BUCKET == 0);
    ASSERT((uint32_t)bucket_base_orderdate_minus_plate_base_orderdate < CONFIG_ORDERDATES_PER_BUCKET * BUCKETS_PER_PLATE);
    ASSERT(bucket_ptr_mid != nullptr);
    ASSERT((uintptr_t)bucket_ptr_mid % PAGE_SIZE == 0);
    ASSERT(bucket_size_mid > 0);
    ASSERT(bucket_size_mid % (sizeof(uint32_t) * 8) == 0);
    ASSERT(topn_count <= CONFIG_EXPECT_MAX_TOPN);
    ASSERT(topn_ptr != nullptr);
    ASSERT((uintptr_t)topn_ptr % PAGE_SIZE == 0);

#if ENABLE_ASSERTION
    {
        const uint32_t* p = (const uint32_t*)bucket_ptr_mid;
        const uint32_t* const end = (const uint32_t*)((uintptr_t)p + bucket_size_mid);
        while (p < end) {
            const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
            p += 8;

            ASSERT(orderkey > 0, "bucket_id=%%u, bucket_size_mid=%lu, offset=%lu",
            /*bucket_id,*/ bucket_size_mid, (end - p) * sizeof(uint32_t));
        }
    }
#endif

    const uint32_t* p = (const uint32_t*)bucket_ptr_mid;
    const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr_mid + bucket_size_mid);
    ASSERT((uintptr_t)p % 32 == 0);

    uint32_t curr_min_expend_cent;
    if (__unlikely(topn_count < CONFIG_EXPECT_MAX_TOPN)) {
        curr_min_expend_cent = 0;
    }
    else {
        ASSERT(topn_count == CONFIG_EXPECT_MAX_TOPN);
        curr_min_expend_cent = (uint32_t)(topn_ptr[0] >> 36);
    }


    while (p < end) {

        const uint32_t total_expend_cent = p[6];
        ASSERT(total_expend_cent > 0);
        ASSERT(total_expend_cent < (1U << 28));
        if (__likely(total_expend_cent < curr_min_expend_cent)) {
            p += 8;
            continue;
        }

        const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
        ASSERT(orderkey > 0);
        ASSERT(orderkey < (1U << 30));
        ASSERT(orderkey <= g_max_orderkey, "orderkey too large: %u", orderkey);

        const uint32_t bucket_orderdate_diff = *(p + 7) >> 30;
        const date_t plate_orderdate_diff = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff;
        ASSERT(plate_orderdate_diff >= 0);
        ASSERT(plate_orderdate_diff < (1 << 6));

        p += 8;

        const uint64_t value = (uint64_t)(total_expend_cent) << 36 | (uint64_t)(orderkey) << 6 | (plate_orderdate_diff);

        if (__unlikely(topn_count < CONFIG_EXPECT_MAX_TOPN)) {
            topn_ptr[topn_count++] = value;
            if (__unlikely(topn_count == CONFIG_EXPECT_MAX_TOPN)) {
                std::make_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>());
                curr_min_expend_cent = (uint32_t)(topn_ptr[0] >> 36);
            }
        }
        else {
            if (__likely(value > topn_ptr[0])) {
                //INFO("Wow!!!!!! mid bucket gets into pretopn!");
                modify_heap(topn_ptr, CONFIG_EXPECT_MAX_TOPN, value, std::greater<>());
                curr_min_expend_cent = (uint32_t)(topn_ptr[0] >> 36);
            }
        }
    }
}

#endif  // ENABLE_MID_INDEX


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
//    {
//        const uint32_t* p = (const uint32_t*)bucket_ptr_minor;
//        const uint32_t* const end = (const uint32_t*)((uintptr_t)p + bucket_size_minor);
//        while (p < end) {
//            const uint32_t total_expend_cent = (p[0] & 0x00FFFFFF) + (p[1] & 0x00FFFFFF) + (p[2] & 0x00FFFFFF);
//            const uint32_t orderkey = *(p + 3) & ~0xC0000000U;
//            const uint32_t bucket_orderdate_diff = *(p + 3) >> 30;
//            const date_t plate_orderdate_diff = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff;
//            p += 4;
//
//            const uint64_t value = (uint64_t)(total_expend_cent) << 36 | (uint64_t)(orderkey) << 6 | (plate_orderdate_diff);
//
//            if (topn_count < CONFIG_EXPECT_MAX_TOPN) {
//                topn_ptr[topn_count++] = value;
//                if (__unlikely(topn_count == CONFIG_EXPECT_MAX_TOPN)) {
//                    std::make_heap(topn_ptr, topn_ptr + CONFIG_EXPECT_MAX_TOPN, std::greater<>());
//                }
//            }
//            else {
//                if (value > topn_ptr[0]) {
//                    modify_heap(topn_ptr, CONFIG_EXPECT_MAX_TOPN, value, std::greater<>());
//                }
//            }
//        }
//    }

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

    ASSERT(topn_count > 0);  // we should have done worker_compute_pretopn_for_plate_major()

    __m256i curr_min_expend_cent;
    uint32_t curr_min_expend_cent_i32;
    if (__unlikely(topn_count < CONFIG_EXPECT_MAX_TOPN)) {
        curr_min_expend_cent_i32 = 0;
        curr_min_expend_cent = _mm256_setzero_si256();
    }
    else {
        ASSERT(topn_count == CONFIG_EXPECT_MAX_TOPN);
        curr_min_expend_cent_i32 = (uint32_t)(topn_ptr[0] >> 36);
        curr_min_expend_cent = _mm256_set1_epi32((int)curr_min_expend_cent_i32);
    }

    const __m256i expend_mask = _mm256_set_epi32(
        0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
        0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);

    ASSERT(bucket_size_minor % (4 * sizeof(uint32_t)) == 0);
    const uint32_t* p = (const uint32_t*)bucket_ptr_minor;
    const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr_minor + bucket_size_minor);
    const uint32_t* const end_align32 = p + __align_down(bucket_size_minor / sizeof(uint32_t), 32);
    while (p < end_align32) {
        // Do a quick check!
        __m256i items12 = _mm256_load_si256((__m256i*)p);
        items12 = _mm256_and_si256(items12, expend_mask);

        __m256i items34 = _mm256_load_si256((__m256i*)p + 1);
        items34 = _mm256_and_si256(items34, expend_mask);

        __m256i items56 = _mm256_load_si256((__m256i*)p + 2);
        items56 = _mm256_and_si256(items56, expend_mask);

        __m256i items78 = _mm256_load_si256((__m256i*)p + 3);
        items78 = _mm256_and_si256(items78, expend_mask);

        const __m256i sum = _mm256_hadd_epi32(
            _mm256_hadd_epi32(items12, items34),
            _mm256_hadd_epi32(items56, items78));

        const __m256i curr_min_gt_sum = _mm256_cmpgt_epi32(curr_min_expend_cent, sum);
        if (__likely(_mm256_movemask_epi8(curr_min_gt_sum) == (int)0xFFFFFFFF)) {
            p += 32;
            continue;
        }

        //
        // NOTE:
        // minor-order to sum:
        //  1       2       3       4       5       6       7       8
        //  |       |       |       |       |       |       |       |
        //  0       4       1       5       2       6       3       7
        //
        const uint32_t total_expend_cent1 = _mm256_extract_epi32(sum, 0);
        const uint32_t total_expend_cent2 = _mm256_extract_epi32(sum, 4);
        const uint32_t total_expend_cent3 = _mm256_extract_epi32(sum, 1);
        const uint32_t total_expend_cent4 = _mm256_extract_epi32(sum, 5);
        const uint32_t total_expend_cent5 = _mm256_extract_epi32(sum, 2);
        const uint32_t total_expend_cent6 = _mm256_extract_epi32(sum, 6);
        const uint32_t total_expend_cent7 = _mm256_extract_epi32(sum, 3);
        const uint32_t total_expend_cent8 = _mm256_extract_epi32(sum, 7);

#define _CHECK_ONE_ORDER(N) \
        do { \
            const uint32_t bucket_orderdate_diff##N = *(p + 3) >> 30; \
            const date_t plate_orderdate_diff##N = bucket_base_orderdate_minus_plate_base_orderdate + bucket_orderdate_diff##N; \
            const uint32_t orderkey##N = *(p + 3) & ~0xC0000000U; \
            \
            ASSERT(total_expend_cent##N > 0); \
            const uint64_t value = (uint64_t)(total_expend_cent##N) << 36 | (uint64_t)(orderkey##N) << 6 | (plate_orderdate_diff##N); \
            \
            if (topn_count < CONFIG_EXPECT_MAX_TOPN) { \
                topn_ptr[topn_count++] = value; \
                if (__unlikely(topn_count == CONFIG_EXPECT_MAX_TOPN)) { \
                    std::make_heap(topn_ptr, topn_ptr + topn_count, std::greater<>()); \
                    curr_min_expend_cent_i32 = (uint32_t)(topn_ptr[0] >> 36); \
                    curr_min_expend_cent = _mm256_set1_epi32(curr_min_expend_cent_i32); \
                } \
            } \
            else { \
                ASSERT(topn_count > 0); \
                ASSERT(topn_count == CONFIG_EXPECT_MAX_TOPN); \
                ASSERT(value > topn_ptr[0]); \
                \
                modify_heap(topn_ptr, CONFIG_EXPECT_MAX_TOPN, value, std::greater<>()); \
                \
                /* curr_min_expend_cent hopefully improves performance */ \
                curr_min_expend_cent_i32 = (uint32_t)(topn_ptr[0] >> 36); \
                curr_min_expend_cent = _mm256_set1_epi32(curr_min_expend_cent_i32); \
            } \
        } while(false)


        if (total_expend_cent1 > curr_min_expend_cent_i32) _CHECK_ONE_ORDER(1);
        p += 4;

        if (total_expend_cent2 > curr_min_expend_cent_i32) _CHECK_ONE_ORDER(2);
        p += 4;

        if (total_expend_cent3 > curr_min_expend_cent_i32) _CHECK_ONE_ORDER(3);
        p += 4;

        if (total_expend_cent4 > curr_min_expend_cent_i32) _CHECK_ONE_ORDER(4);
        p += 4;

        if (total_expend_cent5 > curr_min_expend_cent_i32) _CHECK_ONE_ORDER(5);
        p += 4;

        if (total_expend_cent6 > curr_min_expend_cent_i32) _CHECK_ONE_ORDER(6);
        p += 4;

        if (total_expend_cent7 > curr_min_expend_cent_i32) _CHECK_ONE_ORDER(7);
        p += 4;

        if (total_expend_cent8 > curr_min_expend_cent_i32) _CHECK_ONE_ORDER(8);
        p += 4;
    }

    while (p < end) {
        const uint32_t total_expend_cent = (p[0] & 0x00FFFFFF) + (p[1] & 0x00FFFFFF) + (p[2] & 0x00FFFFFF);

        if (total_expend_cent > curr_min_expend_cent_i32) {
            _CHECK_ONE_ORDER();
        }
        p += 4;
    }
#undef _DECLARE_ONE_ORDER
#undef _CHECK_RESULT
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
#if ENABLE_MID_INDEX
    void* const bucket_ptr_mid = mmap_reserve_space(CONFIG_INDEX_SPARSE_BUCKET_SIZE_MID);
#endif
    void* const bucket_ptr_minor = mmap_reserve_space(CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR);

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


#if ENABLE_MID_INDEX
        //
        // Scan mid buckets
        //
        for (uint32_t bucket_id = plate_base_bucket_id; bucket_id < plate_base_bucket_id + BUCKETS_PER_PLATE; ++bucket_id) {
            if (calc_bucket_mktid(bucket_id) != base_mktid) break;
            TRACE("pretopn for mid bucket_id: %u", bucket_id);

            // Quick check!
            ASSERT(topn_count <= CONFIG_EXPECT_MAX_TOPN);
            if (__likely(topn_count == CONFIG_EXPECT_MAX_TOPN)) {
                if (g_only_mid_max_expend_start_ptr[bucket_id] < (uint32_t)(topn_ptr[0] >> 36)) {
                    continue;
                }
            }

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

            const uintptr_t bucket_start_offset_mid = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MID * (bucket_id - begin_bucket_id);
            const uint64_t bucket_size_mid = g_buckets_endoffset_mid[bucket_id] - bucket_start_offset_mid;
            if (__unlikely(bucket_size_mid == 0)) continue;

            void* const mapped_ptr = mmap(
                bucket_ptr_mid,
                bucket_size_mid,
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_mid_fd[holder_id],
                bucket_start_offset_mid);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == bucket_ptr_mid);

#if ENABLE_ASSERTION
            const uint32_t* p = (uint32_t*)bucket_ptr_mid;
            const uint32_t* end = (uint32_t*)((uintptr_t)p + bucket_size_mid);
            while (p < end) {
                const uint32_t orderkey = *(p + 3) & ~0xC0000000U;
                p += 4;

                ASSERT(orderkey > 0, "bucket_id=%u, bucket_size_mid=%lu, offset=%lu",
                    bucket_id, bucket_size_mid, (end - p) * sizeof(uint32_t));
            }
#endif

            TRACE("build pretopn: scan mid bucket: %u", bucket_id);

            worker_compute_pretopn_for_plate_mid(
                (bucket_base_orderdate - plate_base_orderdate),
                (const uint32_t *)bucket_ptr_mid,
                bucket_size_mid,
                topn_count,
                topn_ptr);
        }
#endif  // ENABLE_MID_INDEX


        //
        // Scan minor buckets
        //
        for (uint32_t bucket_id = plate_base_bucket_id; bucket_id < plate_base_bucket_id + BUCKETS_PER_PLATE; ++bucket_id) {
            if (calc_bucket_mktid(bucket_id) != base_mktid) break;
            TRACE("pretopn for minor bucket_id: %u", bucket_id);

            // Quick check!
            ASSERT(topn_count <= CONFIG_EXPECT_MAX_TOPN);
            if (__likely(topn_count == CONFIG_EXPECT_MAX_TOPN)) {
                if (__likely(g_only_minor_max_expend_start_ptr[bucket_id] < (uint32_t)(topn_ptr[0] >> 36))) {
                    continue;
                }
            }

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


    // Return space in reverse order
    C_CALL(munmap(bucket_ptr_minor, CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR));
    mmap_return_space(bucket_ptr_minor, CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR);

#if ENABLE_MID_INDEX
    C_CALL(munmap(bucket_ptr_mid, CONFIG_INDEX_SPARSE_BUCKET_SIZE_MID));
    mmap_return_space(bucket_ptr_mid, CONFIG_INDEX_SPARSE_BUCKET_SIZE_MID);
#endif

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

        g_shared->worker_sync_barrier.sync_and_run_once([]() {
            ASSERT(g_orders_file.fd > 0);
            ASSERT(g_orders_file.file_size > 0);

            ASSERT(g_orders_file_max_clear_cache_offset > 0);
            constexpr const uint64_t INSTANT_FREE_SIZE = 1024ULL * 1024 * 1024 * 3;
            __fadvice_dont_need(
                g_orders_file.fd,
                g_orders_file_max_clear_cache_offset,
                INSTANT_FREE_SIZE);
            INFO("__fadvice_dont_need page cache for orders file: done beginnings");

            std::thread([&]() {
                constexpr const uint64_t STEP_FREE_SIZE = 1024ULL * 1024 * 32;
                uint64_t pos = __align_down(g_orders_file.file_size, STEP_FREE_SIZE);
                const uint64_t curr_freed_pos = g_orders_file_max_clear_cache_offset + INSTANT_FREE_SIZE;
                while (pos >= curr_freed_pos) {
                    __fadvice_dont_need(
                        g_orders_file.fd,
                        pos,
                        STEP_FREE_SIZE);
                    pos -= STEP_FREE_SIZE;

                    // This is interesting!
                    // Give some time for loaders to load lineitem texts
                    std::this_thread::sleep_for(std::chrono::microseconds(3000));
                    //std::this_thread::yield();
                }
                INFO("__fadvice_dont_need page cache for orders file: done all (background thread done)");
            }).detach();

            //system("free -h");
        });

        worker_load_orders_custkey_from_orderkey(tid, g_shared->orderkey_custkey_shared_counter);
        g_shared->worker_sync_barrier.sync();

#if ENABLE_ASSERTION
        if (tid == 0) {
            debug_check_orders();
        }
#endif
    }

    // We must do shmdt()
    // This is to save space for g_items_buffer_{major/mid/minor}_start_ptr
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
#if ENABLE_MID_INDEX
            uint64_t max_bucket_size_mid = 0;
#endif
            uint64_t max_bucket_size_minor = 0;

            for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
                const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
                const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

                const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - begin_bucket_id);
                const uint64_t bucket_size_major = g_buckets_endoffset_major[bucket_id] - bucket_start_offset_major;
                if (max_bucket_size_major < bucket_size_major) {
                    max_bucket_size_major = bucket_size_major;
                }

#if ENABLE_MID_INDEX
                const uintptr_t bucket_start_offset_mid = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MID * (bucket_id - begin_bucket_id);
                const uint64_t bucket_size_mid = g_buckets_endoffset_mid[bucket_id] - bucket_start_offset_mid;
                if (max_bucket_size_mid < bucket_size_mid) {
                    max_bucket_size_mid = bucket_size_mid;
                }
#endif
                
                const uintptr_t bucket_start_offset_minor = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - begin_bucket_id);
                const uint64_t bucket_size_minor = g_buckets_endoffset_minor[bucket_id] - bucket_start_offset_minor;
                if (max_bucket_size_minor < bucket_size_minor) {
                    max_bucket_size_minor = bucket_size_minor;
                }
            }

            INFO("max_bucket_size_major: %lu", max_bucket_size_major);
            ASSERT(max_bucket_size_major < CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR);
            g_shared->meta.max_bucket_size_major = max_bucket_size_major;

#if ENABLE_MID_INDEX
            INFO("max_bucket_size_mid: %lu", max_bucket_size_mid);
            ASSERT(max_bucket_size_mid < CONFIG_INDEX_SPARSE_BUCKET_SIZE_MID);
            g_shared->meta.max_bucket_size_mid = max_bucket_size_mid;
#endif

            INFO("max_bucket_size_minor: %lu", max_bucket_size_minor);
            ASSERT(max_bucket_size_minor < CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR);
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

#if ENABLE_MID_INDEX
            snprintf(filename, std::size(filename), "holder_mid_%04u", holder_id);
            g_holder_files_mid_fd[holder_id] = C_CALL(openat(
                g_index_directory_fd,
                filename,
                O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
                0666));
#endif

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

#if ENABLE_MID_INDEX
        g_endoffset_file_mid.fd = C_CALL(openat(
            g_index_directory_fd,
            "endoffset_mid",
            O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
            0666));
#endif

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

#if ENABLE_MID_INDEX
        g_only_mid_max_expend_file.fd = C_CALL(openat(
            g_index_directory_fd,
            "only_mid_max_expend",
            O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
            0666));
#endif

        g_only_minor_max_expend_file.fd = C_CALL(openat(
            g_index_directory_fd,
            "only_minor_max_expend",
            O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
            0666));
    }


    //
    // Calculate g_max_custkey
    //
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


    //
    // Calculate g_max_orderkey
    //
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
