#include "common.h"

#define offsetof(s,m)   (size_t)&reinterpret_cast<const volatile char&>((((s *)0)->m))

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

struct date_range_t
{
    date_t d_begin;
    date_t d_end;
    uint16_t q_index_begin;
    uint16_t q_index_end;
};

// #define MAX_SHIP_ORDER_DATE_OFFSET 128
// #define MAX_PARTIAL_COUNT 30
namespace
{
    shared_buffer_packer g_buffer_packer;
    
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };

    query_t* g_queries = nullptr;
    std::vector<uint32_t> g_tasks_to_query { };
    std::vector<uint32_t> g_pretopn_queries { };
    
    done_event* g_queries_done = nullptr;
    done_event* g_shared_pretopn_queries_done = nullptr;
    done_event* g_pretopn_queries_done = nullptr;

    uint64_t* g_pretopn_ptr = nullptr;
    uint32_t* g_pretopn_count_ptr = nullptr;
    uint32_t* g_shared_pretopn_q_index_buffer = nullptr;
    int* g_holder_major_fds = nullptr;
    int* g_holder_minor_fds = nullptr;
    uint64_t* g_endoffset_major_ptr = nullptr;
    uint64_t* g_endoffset_minor_ptr = nullptr;
    
    // // constexpr const uint32_t max_date_item_count = MAX_SHIP_ORDER_DATE_OFFSET * MAX_PARTIAL_COUNT;
    // constexpr const uint32_t max_date_item_count = MAX_SHIP_ORDER_DATE_OFFSET + 2 * CONFIG_TOPN_DATES_PER_PLATE;
    // typedef spsc_queue<date_item_range_t, max_date_item_count> date_item_range_queue;
    // date_item_range_queue* g_di_range_queues_of_query = nullptr;

    // constexpr const uint32_t PLATES_PER_MKTID = (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) / CONFIG_TOPN_DATES_PER_PLATE + 1;

    std::vector<date_range_t> g_shared_pretopn_d_ranges { };
}

__always_inline
void parse_queries() noexcept
{
    ASSERT(g_argv_queries != nullptr);
    ASSERT(g_query_count > 0);

    std::vector<std::vector<uint32_t> > pretopn_queries_by_mktid;
    std::vector<std::vector<date_t> > pretopn_dates_by_mktid;
    std::vector<std::vector<date_range_t> > pretopn_d_ranges_by_mktid;

    pretopn_queries_by_mktid.resize(g_shared->mktid_count);
    pretopn_dates_by_mktid.resize(g_shared->mktid_count);
    pretopn_d_ranges_by_mktid.resize(g_shared->mktid_count);

    // g_queries_done = new done_event[g_query_count];
    // g_pretopn_queries_done = new done_event[g_query_count];
    // g_shared_pretopn_queries_done = new done_event[g_query_count];
    const uint64_t shm_tag_g_queries = g_buffer_packer.register_external((uintptr_t)&g_queries, sizeof(query_t) * g_query_count);
    g_buffer_packer.register_external((uintptr_t)&g_queries_done, sizeof(done_event) * g_query_count);
    g_buffer_packer.register_external((uintptr_t)&g_pretopn_queries_done, sizeof(done_event) * g_query_count);
    g_buffer_packer.register_external((uintptr_t)&g_shared_pretopn_queries_done, sizeof(done_event) * g_query_count);
    ASSERT(g_queries != nullptr);
    ASSERT(g_queries_done != nullptr);
    ASSERT(g_pretopn_queries_done != nullptr);
    ASSERT(g_shared_pretopn_queries_done != nullptr);

    // g_tasks_to_query.reserve(g_query_count);
    g_tasks_to_query.resize(g_query_count);
    // g_di_range_queues_of_query.resize(g_query_count);
    for (uint32_t i = 0; i < g_query_count; ++i) g_tasks_to_query[i] = i;

    std::vector<std::vector<uint32_t> > pretopn_ranges_of_query;
    pretopn_ranges_of_query.resize(g_query_count);

    bool *pretopn_shared_flag = nullptr;
    pretopn_shared_flag = new bool[g_query_count];
    memset(pretopn_shared_flag, 0, g_query_count * sizeof(bool));

    uint32_t max_date_ranges_count = 0;
    for (uint32_t q = 0; q < g_query_count; ++q) {
        query_t& query = g_queries[q];
        const auto it = g_mktsegment_to_mktid.find(g_argv_queries[4 * q + 0]);
        if (__unlikely(it == g_mktsegment_to_mktid.end())) {
            query.is_unknown_mktsegment = true;
            DEBUG("query #%u: unknown mktsegment: %s", q, g_argv_queries[4 * q + 0]);
            continue;
        }
        query.q_mktid = it->second;
        query.q_orderdate = date_from_string<false>(g_argv_queries[4 * q + 1]);
        query.q_shipdate = date_from_string<false>(g_argv_queries[4 * q + 2]);
        query.q_topn = (uint32_t)std::strtoul(g_argv_queries[4 * q + 3], nullptr, 10);
        DEBUG("query #%u: q_mktid=%u,q_orderdate=%u,q_shipdate=%u,q_topn=%u",
                q, query.q_mktid, query.q_orderdate, query.q_shipdate, query.q_topn);

        g_buffer_packer.register_internal(
            shm_tag_g_queries + q * sizeof(query_t) + offsetof(query_t, result),
            query.q_topn * sizeof(query_result_t)
        );
        ASSERT(query.result != nullptr);

        query.d_scan_begin = std::min<date_t>(std::max<date_t>(
            query.q_shipdate - (g_shared->meta.max_shipdate_orderdate_diff - 1),
            MIN_TABLE_DATE), MAX_TABLE_DATE);
        query.d_scan_end = std::max<date_t>(std::min<date_t>(query.q_orderdate, 
            MAX_TABLE_DATE + 1), MIN_TABLE_DATE);
        const uint8_t mktid = query.q_mktid;
        const date_t q_shipdate = query.q_shipdate;
        const date_t q_orderdate = query.d_scan_end;
        if (q_shipdate < q_orderdate) {
            date_t d_pretopn_begin = ((q_shipdate + CONFIG_TOPN_DATES_PER_PLATE) / 
                CONFIG_TOPN_DATES_PER_PLATE) * CONFIG_TOPN_DATES_PER_PLATE;
            date_t d_pretopn_end = (q_orderdate / 
                CONFIG_TOPN_DATES_PER_PLATE) * CONFIG_TOPN_DATES_PER_PLATE;
            ASSERT(d_pretopn_begin > q_shipdate);
            ASSERT(d_pretopn_end <= q_orderdate);
            if (d_pretopn_begin >= q_orderdate || d_pretopn_end <= q_shipdate || 
                d_pretopn_begin == d_pretopn_end || query.q_topn > CONFIG_EXPECT_MAX_TOPN ||
                query.q_topn <= 0) {
                d_pretopn_begin = q_orderdate;
                d_pretopn_end = q_orderdate;
                pretopn_shared_flag[q] = true; // mark as already shared to avoid checking
                // mark skip pretopn
                // g_tasks_to_query.push_back(q);
                g_pretopn_queries_done[q].mark_done();
            }
            else {
                pretopn_queries_by_mktid[mktid].push_back(q);
                pretopn_dates_by_mktid[mktid].push_back(d_pretopn_begin);
                pretopn_dates_by_mktid[mktid].push_back(d_pretopn_end);
                max_date_ranges_count += 2;
            }
            query.d_pretopn_begin = d_pretopn_begin;
            query.d_pretopn_end = d_pretopn_end;
        }
        else {
            query.d_pretopn_begin = q_orderdate;
            query.d_pretopn_end = q_orderdate;
            pretopn_shared_flag[q] = true; // mark as already shared to avoid checking
            // mark skip pretopn
            // g_tasks_to_query.push_back(q);
            g_pretopn_queries_done[q].mark_done();
        }
        DEBUG("qid%u scan[%u,%u) pretopn[%u,%u)", 
            q,
            query.d_scan_begin,query.d_scan_end, 
            query.d_pretopn_begin,query.d_pretopn_end);
    }
    DEBUG("pretopn query(s) size %u", g_pretopn_queries.size());

    uint32_t* q_index_buffer = nullptr;
    q_index_buffer = new uint32_t[g_query_count * max_date_ranges_count];
    g_shared_pretopn_q_index_buffer = new uint32_t[g_query_count * max_date_ranges_count];
    uint32_t buffer_l = 0, buffer_r = 0;
    uint32_t shared_buffer_l = 0, shared_buffer_r = 0;
    bool* mktid_date_sorted_flag = nullptr;
    mktid_date_sorted_flag = new bool[g_shared->mktid_count];
    memset(mktid_date_sorted_flag, 0, g_shared->mktid_count * sizeof(bool));
    for (uint32_t qid = 0; qid < g_query_count; ++qid) {
        if (pretopn_shared_flag[qid]) continue;
        const uint8_t mktid = g_queries[qid].q_mktid;
        if (!mktid_date_sorted_flag[mktid]) {
            std::sort(
                pretopn_dates_by_mktid[mktid].begin(),
                pretopn_dates_by_mktid[mktid].end(),
                std::less<date_t>()
            );
            mktid_date_sorted_flag[mktid] = true;
        }
        const size_t size = pretopn_dates_by_mktid[mktid].size();
        date_range_t d_range;
        for (size_t i = 1, d_range_count = 0; i < size; ++i) {
            const date_t d_begin = pretopn_dates_by_mktid[mktid][i - 1];
            const date_t d_end   = pretopn_dates_by_mktid[mktid][i];
            if (d_end <= d_begin) continue;
            d_range.d_begin = d_begin;
            d_range.d_end = d_end;
            for (const uint32_t qid : pretopn_queries_by_mktid[mktid]) {
                if (d_begin >= g_queries[qid].d_pretopn_begin && 
                    d_end <= g_queries[qid].d_pretopn_end) {
                    q_index_buffer[buffer_r] = qid;
                    ++buffer_r;
                    pretopn_ranges_of_query[qid].push_back(d_range_count);
                }
            }
            d_range.q_index_begin = buffer_l;
            d_range.q_index_end = buffer_r;
            buffer_l = buffer_r;
            pretopn_d_ranges_by_mktid[mktid].push_back(d_range);
            ++d_range_count;
        }
        for (const uint32_t qid : pretopn_queries_by_mktid[mktid]) {
            if (pretopn_shared_flag[qid]) continue;
            uint32_t max_unshared_count = 0;
            uint32_t max_unshared_d_range_id = 0;
            ASSERT(pretopn_ranges_of_query[qid].size() > 0);
            for (const uint32_t d_range_id : pretopn_ranges_of_query[qid]) {
                uint32_t unshared_count = 0;
                uint32_t l = pretopn_d_ranges_by_mktid[mktid][d_range_id].q_index_begin;
                const uint32_t r = pretopn_d_ranges_by_mktid[mktid][d_range_id].q_index_end;
                for (; l < r; ++l) {
                    // unshared_count += pretopn_shared_flag[q_index_buffer[l]] ? 0 : 1; 
                    unshared_count += !(pretopn_shared_flag[q_index_buffer[l]]);
                }
                if (unshared_count > max_unshared_count) {
                    max_unshared_count = unshared_count;
                    max_unshared_d_range_id = d_range_id;
                }
            }
            ASSERT(max_unshared_count > 0);
            const date_range_t& d_range = pretopn_d_ranges_by_mktid[mktid][max_unshared_d_range_id];
            uint32_t l = d_range.q_index_begin;
            const uint32_t r = d_range.q_index_end;
            date_range_t shared_d_range;
            shared_d_range.d_begin = d_range.d_begin;
            shared_d_range.d_end = d_range.d_end;
            for (; l < r; ++l) {
                const uint32_t to_share_qid = q_index_buffer[l];
                if (!pretopn_shared_flag[to_share_qid]) {
                    pretopn_shared_flag[to_share_qid] = true;
                    query_t& query = g_queries[to_share_qid];
                    query.d_shared_pretopn_begin = d_range.d_begin;
                    query.d_shared_pretopn_end = d_range.d_end;
                    ASSERT(query.d_pretopn_begin <= query.d_shared_pretopn_begin);
                    ASSERT(query.d_pretopn_end >= query.d_shared_pretopn_end);
                    g_shared_pretopn_q_index_buffer[shared_buffer_r] = to_share_qid;
                    // g_tasks_to_query.push_back(to_share_qid);
                    g_pretopn_queries.push_back(to_share_qid);
                    ++shared_buffer_r;
                }
            }
            ASSERT(shared_buffer_r - shared_buffer_l == max_unshared_count);
            shared_d_range.q_index_begin = shared_buffer_l;
            shared_d_range.q_index_end = shared_buffer_r;
            g_shared_pretopn_d_ranges.push_back(shared_d_range);
            shared_buffer_l = shared_buffer_r;
        }
    }
    ASSERT(g_tasks_to_query.size() == g_query_count);

    if (q_index_buffer != nullptr) delete [] q_index_buffer;
    if (pretopn_shared_flag != nullptr) delete [] pretopn_shared_flag;
    if (mktid_date_sorted_flag != nullptr) delete [] mktid_date_sorted_flag;
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
        load_file_context pretopn_ctx, pretopn_count_ctx;
        __openat_file_read(g_index_directory_fd, "pretopn", &pretopn_ctx);
        __openat_file_read(g_index_directory_fd, "pretopn_count", &pretopn_count_ctx);

        // ASSERT(pretopn_size == sizeof(uint64_t) * CONFIG_EXPECT_MAX_TOPN * g_shared->mktid_count * PLATES_PER_MKTID);
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

        // ASSERT(pretopn_count_size == sizeof(uint32_t) * g_shared->mktid_count * PLATES_PER_MKTID);
        uint32_t* reserve_g_pretopn_count_ptr = (uint32_t*)mmap_reserve_space(pretopn_ctx.file_size);
        g_pretopn_count_ptr = (uint32_t*)mmap(
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
    // open fds of holder_major_XXXX and holder_minor_XXXX; load endoffset_major and endoffset_minor
    //
    {
        // CONFIG_INDEX_HOLDER_COUNT
        g_holder_major_fds = new int[CONFIG_INDEX_HOLDER_COUNT];
        g_holder_minor_fds = new int[CONFIG_INDEX_HOLDER_COUNT];
        load_file_context ctx;
        char filename[50];
        for (uint32_t i = 0; i < CONFIG_INDEX_HOLDER_COUNT; ++i) {
            sprintf(filename, "holder_major_%04u", i);
            __openat_file_read(g_index_directory_fd, filename, &ctx);
            g_holder_major_fds[i] = ctx.fd;
            DEBUG("open %s with fd %d", filename, g_holder_major_fds[i]);
            sprintf(filename, "holder_minor_%04u", i);
            __openat_file_read(g_index_directory_fd, filename, &ctx);
            g_holder_minor_fds[i] = ctx.fd;
            DEBUG("open %s with fd %d", filename, g_holder_minor_fds[i]);
        }

        __openat_file_read(g_index_directory_fd, "endoffset_major", &ctx);
        uint64_t* reserve_g_endoffset_major_ptr = (uint64_t*)mmap_reserve_space(ctx.file_size);
        g_endoffset_major_ptr = (uint64_t*)mmap(
            reserve_g_endoffset_major_ptr,
            ctx.file_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE | MAP_FIXED,
            ctx.fd,
            0);
        ASSERT(reserve_g_endoffset_major_ptr == g_endoffset_major_ptr);
        INFO("g_endoffset_major_ptr: %p", g_endoffset_major_ptr);

        __openat_file_read(g_index_directory_fd, "endoffset_minor", &ctx);
        uint64_t* reserve_g_endoffset_minor_ptr = (uint64_t*)mmap_reserve_space(ctx.file_size);
        g_endoffset_minor_ptr = (uint64_t*)mmap(
            reserve_g_endoffset_minor_ptr,
            ctx.file_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE | MAP_FIXED,
            ctx.fd,
            0);
        ASSERT(reserve_g_endoffset_minor_ptr == g_endoffset_minor_ptr);
        INFO("g_endoffset_minor_ptr: %p", g_endoffset_minor_ptr);
    }

    //
    // Parse queries and arrange the query plan
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


