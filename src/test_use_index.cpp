#include "common.h"

struct query_t
{
    uint8_t q_mktid;
    date_t q_orderdate;
    date_t q_shipdate;
    uint32_t q_topn;

    date_t d_scan_begin;
    date_t d_scan_end;

    date_t d_pretopn_begin;
    date_t d_pretopn_end;
    date_t d_shared_pretopn_begin;
    date_t d_shared_pretopn_end;

    bool is_unknown_mktsegment;
    query_result_t* result = nullptr;
    // std::string output;
    // uint32_t* item_buffer = nullptr;
    // uint32_t* item_size_buffer = nullptr;
};

#define MAX_SHIP_ORDER_DATE_OFFSET 128
#define MAX_PARTIAL_COUNT 30
namespace
{
    // int g_mktsegment_fd = -1;
    // uint64_t g_mktsegment_file_size = 0;
    // uint8_t g_mktid_count = 0;
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };

    query_t* g_queries = nullptr;
    uint32_t* g_tasks_to_query = nullptr;
    done_event* g_queries_done = nullptr;

    uint64_t* g_pretopn_ptr = nullptr;
    uint32_t* g_pretopn_count_ptr = nullptr;
    uint32_t* g_shared_pretopn_q_index_buffer = nullptr;
    int* g_holder_major_fds = nullptr;
    int* g_holder_minor_fds = nullptr;
    uint64_t* g_endoffset_major_ptr = nullptr;
    uint64_t* g_endoffset_minor_ptr = nullptr;
    
    // constexpr const uint32_t max_date_item_count = MAX_SHIP_ORDER_DATE_OFFSET * MAX_PARTIAL_COUNT;
    constexpr const uint32_t max_date_item_count = MAX_SHIP_ORDER_DATE_OFFSET + 2 * CONFIG_TOPN_DATES_PER_PLATE;
    typedef spsc_queue<date_item_range_t, max_date_item_count> date_item_range_queue;
    date_item_range_queue* g_di_range_queues_of_query = nullptr;

    constexpr const uint32_t PLATES_PER_MKTID = (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) / CONFIG_TOPN_DATES_PER_PLATE + 1;


    done_event* g_shared_pretopn_queries_done = nullptr;
    done_event* g_pretopn_queries_done = nullptr;

    uint32_t g_pretopn_queries = nullptr;

    date_range_t* g_shared_pretopn_d_ranges;
}

__always_inline
void parse_queries() noexcept
{
    ASSERT(g_argv_queries != nullptr);
    ASSERT(g_query_count > 0);

    
}

void use_index_initialize_before_fork() noexcept
{
    //
    // Load meta to g_shared
    //
    {
        ASSERT(g_shared != nullptr);
        const int fd = C_CALL(openat(
            g_index_directory_fd,
            "meta",
            O_RDONLY | O_CLOEXEC));
        const size_t cnt = C_CALL(pread(fd, &g_shared->meta, sizeof(g_shared->meta), 0));
        CHECK(cnt == sizeof(g_shared->meta));
        C_CALL(close(fd));

        INFO("meta.max_shipdate_orderdate_diff: %u", g_shared->meta.max_shipdate_orderdate_diff);
        INFO("meta.max_bucket_size_major: %lu", g_shared->meta.max_bucket_size_major);
        INFO("meta.max_bucket_size_minor: %lu", g_shared->meta.max_bucket_size_minor);
    }


    //
    // Load mktsegments to g_shared
    //
    {
        ASSERT(g_shared != nullptr);
        load_file_context ctx;
        __openat_file_read(g_index_directory_fd, "mktsegment", &ctx);

        char buffer[ctx.file_size];
        const size_t cnt = C_CALL(pread(ctx.fd, buffer, ctx.file_size, 0));
        CHECK(cnt == ctx.file_size);
        C_CALL(close(ctx.fd));

        uintptr_t p = (uintptr_t)buffer;
        g_shared->mktid_count = *(uint8_t*)p;
        p += sizeof(uint8_t);
        INFO("g_shared->mktid_count: %u", g_shared->mktid_count);
        ASSERT(g_shared->mktid_count < (1 << 3));
        for (uint8_t mktid = 0; mktid < g_shared->mktid_count; ++mktid) {
            const uint8_t len = *(uint8_t*)p;
            p += sizeof(uint8_t);
            g_shared->all_mktsegments[mktid].length = len;
            memcpy(g_shared->all_mktsegments[mktid].name, (const void*)p, len);
            p += len;

            INFO("mktsegment: %u -> %.*s", mktid, (int)len, g_shared->all_mktsegments[mktid].name);
            g_mktsegment_to_mktid[std::string(g_shared->all_mktsegments[mktid].name, len)] = mktid;
        }
        ASSERT(p == (uintptr_t)buffer + ctx.file_size);

        g_shared->total_buckets = g_shared->mktid_count * BUCKETS_PER_MKTID;
        INFO("g_shared->total_buckets: %u", g_shared->total_buckets);

        g_shared->buckets_per_holder = __div_up(g_shared->total_buckets, CONFIG_INDEX_HOLDER_COUNT);
        INFO("g_shared->buckets_per_holder: %u", g_shared->buckets_per_holder);

        g_shared->total_plates = g_shared->mktid_count * PLATES_PER_MKTID;
        INFO("g_shared->total_plates: %u", g_shared->total_plates);
    }

    //
    // Load pretopn and pretopn_count
    //
    {
        int pretopn_fd = -1, pretopn_count_fd = -1;
        uint64_t pretopn_size = 0, pretopn_count_size = 0;
        load_file_context pretopn_ctx, pretopn_count_ctx;
        __openat_file_read(g_index_directory_fd, "pretopn", &pretopn_ctx);
        __openat_file_read(g_index_directory_fd, "pretopn_count", &pretopn_count_ctx);

        // ASSERT(pretopn_size == sizeof(uint64_t) * CONFIG_EXPECT_MAX_TOPN * g_mktid_count * PLATES_PER_MKTID);
        uint64_t* reserve_g_pretopn_ptr = (uint64_t*)mmap_reserve_space(pretopn_ctx.file_size);
        g_pretopn_ptr = (uint64_t*)mmap(
            reserve_g_pretopn_ptr,
            pretopn_ctx.file_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE | MAP_FIXED,
            pretopn_ctx.fd,
            0);
        ASSERT(reserve_g_pretopn_ptr == g_pretopn_ptr);
        INFO("g_pretopn_ptr: %p", g_pretopn_ptr);

        // ASSERT(pretopn_count_size == sizeof(uint32_t) * g_mktid_count * PLATES_PER_MKTID);
        uint64_t* reserve_g_pretopn_count_ptr = (uint64_t*)mmap_reserve_space(pretopn_ctx.file_size);
        g_pretopn_count_ptr = (uint64_t*)mmap(
            reserve_g_pretopn_count_ptr,
            pretopn_ctx.file_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE | MAP_FIXED,
            pretopn_ctx.fd,
            0);
        ASSERT(reserve_g_pretopn_count_ptr == g_pretopn_count_ptr);
        INFO("g_pretopn_count_ptr: %p", g_pretopn_count_ptr);
    }


    //
    // Parse queries
    //
    {
        parse_queries();
    }
}

void use_index_initialize_after_fork() noexcept
{

}

void fn_loader_thread_use_index(const uint32_t tid) noexcept
{

}

void fn_worker_thread_use_index(const uint32_t tid) noexcept
{

}

void fn_unloader_thread_use_index() noexcept
{

}


