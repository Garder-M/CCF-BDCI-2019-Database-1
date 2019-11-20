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

    date_t d_exact_pretopn_begin;;
    date_t d_exact_pretopn_end;

    bool is_unknown_mktsegment;
    query_result_t* result = nullptr;
    uint32_t result_size;
    char* output = nullptr;
    uint32_t output_size;
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

struct workload_info_t
{
    date_t orderdate;
    uint8_t type;   // 0: check both shipdate & orderdate; 1: only check shipdate; 2: no check
    uint16_t index;
};

namespace
{
    // writen by main process, read by child processes
    // can safely use std::structure as well as new/malloc
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };
    std::vector<uint32_t> g_tasks_to_query { };
    std::vector<uint32_t> g_pretopn_queries { };

    uint64_t* g_pretopn_ptr = nullptr;
    uint32_t* g_pretopn_count_ptr = nullptr;
    uint32_t* g_shared_pretopn_q_index_buffer = nullptr;
    int* g_holder_major_fds = nullptr;
    int* g_holder_minor_fds = nullptr;
    uint64_t* g_endoffset_major_ptr = nullptr;
    uint64_t* g_endoffset_minor_ptr = nullptr;
    
    std::vector<date_range_t> g_shared_pretopn_d_ranges { };

    // read and writen by all processes
    // should be registered into g_buffer_packer
    shared_buffer_packer g_buffer_packer;

    query_t* g_queries = nullptr;
    
    done_event* g_queries_done = nullptr;
    done_event* g_shared_pretopn_queries_done = nullptr;
    done_event* g_pretopn_queries_done = nullptr;

    // read and writen by different threads within a single process
    // can safely use std::structure as well as new/malloc
    uint32_t g_curr_working_qid;
    const constexpr uint16_t max_workload_size = 16;
    typedef spsc_queue<workload_info_t, max_workload_size> workload_info_queue;
    typedef spsc_bounded_bag<uint16_t, max_workload_size> workload_index_bag;
    
    process_shared_semaphore g_start_working_sem;
    process_shared_semaphore g_finish_working_sem;

    workload_info_queue g_major_workload_info_queue;
    workload_index_bag g_major_workload_index_bag;
    uint32_t* g_major_workload_mmap_base_ptr = nullptr;
    uint64_t g_major_workload_mmap_size;

#if ENABLE_CPU_HANDLE_MINOR_WORKLOAD
    workload_info_queue g_minor_workload_info_queue;
    workload_index_bag g_minor_workload_index_bag;
    uint32_t* g_minor_workload_mmap_base_ptr = nullptr;
    uint64_t g_minor_workload_mmap_size;
#endif

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
        INFO("query #%u: q_mktid=%u,q_orderdate=%u,q_shipdate=%u,q_topn=%u",
                q, query.q_mktid, query.q_orderdate, query.q_shipdate, query.q_topn);

        g_buffer_packer.register_internal(
            shm_tag_g_queries + q * sizeof(query_t) + __field_offset(query_t, result),
            query.q_topn * sizeof(query_result_t)
        );
        ASSERT(query.result != nullptr);
        query.result_size = 0;
        g_buffer_packer.register_internal(
            shm_tag_g_queries + q * sizeof(query_t) + __field_offset(query_t, output),
            (query.q_topn + 1) * 40
        ); // max line length: ~32, reserved to 40 for safety
        ASSERT(query.output != nullptr);
        query.output_size = 0;

        query.d_scan_begin = std::min<date_t>(std::max<date_t>(
            query.q_shipdate - (g_shared->meta.max_shipdate_orderdate_diff - 1),
            MIN_TABLE_DATE), MAX_TABLE_DATE);
        query.d_scan_end = std::max<date_t>(std::min<date_t>(query.q_orderdate, 
            MAX_TABLE_DATE + 1), MIN_TABLE_DATE);
        const uint8_t mktid = query.q_mktid;
        const date_t q_shipdate = query.q_shipdate;
        const date_t q_orderdate = query.d_scan_end;
        if (q_shipdate < q_orderdate) {
            date_t d_pretopn_begin = (((q_shipdate - MIN_TABLE_DATE) + CONFIG_TOPN_DATES_PER_PLATE) / 
                CONFIG_TOPN_DATES_PER_PLATE) * CONFIG_TOPN_DATES_PER_PLATE + MIN_TABLE_DATE;
            date_t d_pretopn_end = ((q_orderdate - MIN_TABLE_DATE) / 
                CONFIG_TOPN_DATES_PER_PLATE) * CONFIG_TOPN_DATES_PER_PLATE + MIN_TABLE_DATE;
            ASSERT(d_pretopn_begin > q_shipdate);
            ASSERT(d_pretopn_end <= q_orderdate);
            date_t d_exact_pretopn_begin = q_shipdate + 1;
            date_t d_exact_pretopn_end = q_orderdate;
            ASSERT(d_exact_pretopn_begin <= d_exact_pretopn_end);
            ASSERT(d_exact_pretopn_begin <= d_pretopn_begin);
            ASSERT(d_exact_pretopn_end >= d_pretopn_end);
            if (d_pretopn_begin >= q_orderdate || d_pretopn_end <= q_shipdate || 
                d_pretopn_begin == d_pretopn_end || query.q_topn > CONFIG_EXPECT_MAX_TOPN ||
                query.q_topn <= 0) {
                // d_pretopn_begin = q_orderdate;
                // d_pretopn_end = q_orderdate;
                // d_exact_pretopn_begin = q_orderdate;
                // d_exact_pretopn_end = q_orderdate;
                d_pretopn_begin = d_exact_pretopn_end;
                d_pretopn_end = d_exact_pretopn_end;
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
            query.d_exact_pretopn_begin = d_exact_pretopn_begin;
            query.d_exact_pretopn_end = d_exact_pretopn_end;
        }
        else {
            query.d_pretopn_begin = q_orderdate;
            query.d_pretopn_end = q_orderdate;
            query.d_exact_pretopn_begin = q_orderdate;
            query.d_exact_pretopn_end = q_orderdate;
            pretopn_shared_flag[q] = true; // mark as already shared to avoid checking
            // mark skip pretopn
            // g_tasks_to_query.push_back(q);
            g_pretopn_queries_done[q].mark_done();
        }
        INFO("qid%u scan[%u,%u) pretopn[%u,%u)", 
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
        g_holder_major_fds = new int[CONFIG_INDEX_HOLDER_COUNT];
        g_holder_minor_fds = new int[CONFIG_INDEX_HOLDER_COUNT];
        load_file_context ctx;
        char filename[50];
        for (uint32_t i = 0; i < CONFIG_INDEX_HOLDER_COUNT; ++i) {
            sprintf(filename, "holder_major_%04u", i);
            __openat_file_read(g_index_directory_fd, filename, &ctx);
            g_holder_major_fds[i] = ctx.fd;
            ctx.fd = -1;
            DEBUG("open %s with fd %d", filename, g_holder_major_fds[i]);
            sprintf(filename, "holder_minor_%04u", i);
            __openat_file_read(g_index_directory_fd, filename, &ctx);
            g_holder_minor_fds[i] = ctx.fd;
            ctx.fd = -1;
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
        ctx.fd = -1;

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
    g_major_workload_index_bag.init([](const size_t idx) -> uint16_t {
        ASSERT(idx < max_workload_size);
        return idx;
    }, max_workload_size);

    g_major_workload_mmap_size = (g_shared->meta.max_bucket_size_major + 4096 - 1) / 4096 * 4096;
    g_major_workload_mmap_base_ptr = (uint32_t*)mmap_reserve_space(g_major_workload_mmap_size * max_workload_size);

#if ENABLE_CPU_HANDLE_MINOR_WORKLOAD
    g_minor_workload_index_bag.init([](const size_t idx) -> uint16_t {
        ASSERT(idx < max_workload_size);
        return idx;
    }, max_workload_size);

    g_minor_workload_mmap_size = (g_shared->meta.max_bucket_size_minor + 4096 - 1) / 4096 * 4096;
    g_minor_workload_mmap_base_ptr = (uint32_t*)mmap_reserve_space(g_minor_workload_mmap_size * max_workload_size);
#endif
}

void fn_pretopn_thread_use_index(const uint32_t tid) noexcept
{
    const auto scan_plate = [&](const uint32_t plate_id, const date_t from_orderdate, query_t& query) {
        ASSERT(plate_id < g_shared->mktid_count * PLATES_PER_MKTID);
        const uint64_t* const plate_ptr = &g_pretopn_ptr[plate_id * CONFIG_EXPECT_MAX_TOPN];
        const uint32_t count = g_pretopn_count_ptr[plate_id];
        // DEBUG("pretopn%u scan_plate plate_id=%u,from_orderdate=%u,count=%u", 
            // tid, plate_id, from_orderdate, count);

        for (uint32_t i = 0; i < count; ++i) {
            const uint64_t value = plate_ptr[i];

            query_result_t tmp;
            tmp.total_expend_cent = value >> 36;
            tmp.orderkey = (value >> 6) & ((1U << 30) - 1);
            tmp.orderdate = from_orderdate + (value & 0b111111U);
            // tmp.orderkey = value & ((1U << 30) - 1);
            // tmp.orderdate = from_orderdate + ((value >> 30) & 0b111111U);

            if (query.result_size < query.q_topn) {
                query.result[query.result_size++] = tmp;
                if (__unlikely(query.result_size == query.q_topn)) {
                    std::make_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                }
            }
            else {
                if (tmp > query.result[0]) {
                    std::pop_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                    query.result[query.result_size - 1] = tmp;
                    std::push_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                }
                else {
                    // plate is ordered (descending)
                    break;
                }
            }
        }
    };

    // static std::atomic_uint32_t __shared_pretopn_d_ranges_curr { 0 };
    while (true) {
        // const uint32_t task_id = __shared_pretopn_d_ranges_curr++;
        const uint32_t task_id = g_shared->shared_pretopn_d_ranges_curr++;
        if (task_id >= g_shared_pretopn_d_ranges.size()) break;
        const date_range_t& d_range = g_shared_pretopn_d_ranges[task_id];
        uint32_t max_q_topn = 0;
        uint32_t max_q_topn_qid = 0;
        ASSERT(d_range.d_begin < d_range.d_end);
        ASSERT(d_range.q_index_begin < d_range.q_index_end);
        DEBUG("pretopn%u fetch shared_q_range[%u,%u) shared by %u query(s), offset[%u,%u)", 
            tid, d_range.d_begin, d_range.d_end, d_range.q_index_end-d_range.q_index_begin,
            d_range.q_index_begin, d_range.q_index_end);
        for (uint32_t l = d_range.q_index_begin; l < d_range.q_index_end; ++l) {
            const uint32_t qid = g_shared_pretopn_q_index_buffer[l];
            const query_t& query = g_queries[qid];
            ASSERT(d_range.d_begin == query.d_shared_pretopn_begin);
            ASSERT(d_range.d_end == query.d_shared_pretopn_end);
            if (query.q_topn >= max_q_topn) {
                max_q_topn = query.q_topn;
                max_q_topn_qid = qid;
            }
        }
        query_t& ref_query = g_queries[max_q_topn_qid];
        const uint8_t mktid = ref_query.q_mktid;
        for (date_t orderdate = d_range.d_begin; orderdate < d_range.d_end; orderdate += CONFIG_TOPN_DATES_PER_PLATE) {
            const uint32_t plate_id = calc_topn_plate_index(mktid, orderdate);
            scan_plate(plate_id, orderdate, ref_query);
        }
        query_result_t* ref_result = ref_query.result;
        uint32_t ref_result_size = ref_query.result_size;
        uint32_t left_query_count = d_range.q_index_end - d_range.q_index_begin - 1;
        for (uint32_t l = d_range.q_index_begin; l < d_range.q_index_end && left_query_count > 0; ++l) {
            const uint32_t qid = g_shared_pretopn_q_index_buffer[l];
            if (qid == max_q_topn_qid) continue;
            query_t& query = g_queries[qid];
            if (query.q_topn >= ref_result_size) {
                memcpy(query.result, ref_result, query.q_topn * sizeof(query_result_t));
                query.result_size = ref_result_size;
                g_shared_pretopn_queries_done[qid].mark_done();
                --left_query_count;
            }
        }
        const bool heap_reordered = (left_query_count > 0) ? true : false;
        for (uint32_t l = d_range.q_index_begin; l < d_range.q_index_end && left_query_count > 0; ++l) {
            const uint32_t qid = g_shared_pretopn_q_index_buffer[l];
            if (qid == max_q_topn_qid) continue;
            query_t& query = g_queries[qid];
            if (query.q_topn < ref_result_size) {
                std::nth_element(&ref_result[0], &ref_result[query.q_topn], 
                    &ref_result[ref_result_size], std::greater<>());
                memcpy(query.result, ref_result, query.q_topn * sizeof(query_result_t));
                query.result_size = query.q_topn;
                std::make_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                g_shared_pretopn_queries_done[qid].mark_done();
                --left_query_count;
            }
        }
        ASSERT(left_query_count == 0);
        if (heap_reordered && ref_result_size >= ref_query.q_topn) {
            std::make_heap(&ref_result[0], &ref_result[ref_result_size], std::greater<>());
        }
        DEBUG("pretopn%u finished shared_q_range[%u,%u) shared by %u query(s), ref_qid=%u", 
            tid, d_range.d_begin, d_range.d_end, d_range.q_index_end-d_range.q_index_begin, max_q_topn_qid);
        if (ref_result_size < ref_query.q_topn) {
            DEBUG("warning: pretopn%u finished shared_q_range[%u,%u), ref_qid%u result unfilled(%u<%u)",
                tid, d_range.d_begin, d_range.d_end, max_q_topn_qid, ref_result_size, ref_query.q_topn);
        }
        g_shared_pretopn_queries_done[max_q_topn_qid].mark_done();
    }

    // static std::atomic_uint32_t __pretopn_queries_curr { 0 };
    while (true) {
        // const uint32_t task_id = __pretopn_queries_curr++;
        const uint32_t task_id = g_shared->pretopn_queries_curr++;
        if (task_id >= g_pretopn_queries.size()) break;
        const uint32_t qid = g_pretopn_queries[task_id];
        g_shared_pretopn_queries_done[qid].wait_done();
        DEBUG("pretopn%u starts query%u pretopn, pretopn_task_id %u", 
            tid, qid, task_id);
        query_t& query = g_queries[qid];
        ASSERT(query.d_pretopn_begin <= query.d_shared_pretopn_begin);
        ASSERT(query.d_shared_pretopn_begin < query.d_shared_pretopn_end);
        ASSERT(query.d_shared_pretopn_end <= query.d_pretopn_end);
        for (date_t orderdate = query.d_pretopn_begin; orderdate < query.d_shared_pretopn_begin; 
                orderdate += CONFIG_TOPN_DATES_PER_PLATE) {
            const uint32_t plate_id = calc_topn_plate_index(query.q_mktid, orderdate);
            scan_plate(plate_id, orderdate, query);
        }
        for (date_t orderdate = query.d_shared_pretopn_end; orderdate < query.d_pretopn_end; 
                orderdate += CONFIG_TOPN_DATES_PER_PLATE) {
            const uint32_t plate_id = calc_topn_plate_index(query.q_mktid, orderdate);
            scan_plate(plate_id, orderdate, query);
        }
        DEBUG("pretopn%u finishes query%u pretopn, pretopn_task_id %u", 
            tid, qid, task_id);
        g_pretopn_queries_done[qid].mark_done();
    }
}

void fn_loader_thread_use_index(const uint32_t tid) noexcept
{
    // static std::atomic_uint32_t __g_queries_curr { 0 };
    struct mmap_date_range_t {
        date_t d_aligned_begin;
        date_t d_aligned_end;
        uint8_t type;
    };
    const constexpr uint32_t mmap_date_range_size = 20;
    mmap_date_range_t mmap_date_ranges[mmap_date_range_size];
    uint32_t global_major_workload_count = 0;
    while (true) {
        // const uint32_t task_id = __g_queries_curr++;
        const uint32_t task_id = g_shared->queries_curr++;
        if (task_id >= g_query_count) {
            g_curr_working_qid = -1;
            g_start_working_sem.post();
            break;
        }
        const uint32_t qid = g_tasks_to_query[task_id];
        g_curr_working_qid = qid;
        size_t queue_head, queue_tail;
        // g_major_workload_info_queue.report(&queue_head, &queue_tail);
        DEBUG("[#%u] query%u fetched, major_queue init status head=%llu, tail=%llu", 
                tid, qid, 
                queue_head, queue_tail
        );
        g_major_workload_info_queue.reinit();
#if ENABLE_CPU_HANDLE_MINOR_WORKLOAD
        g_minor_workload_info_queue.reinit();
#endif
        g_start_working_sem.post();
        query_t& query = g_queries[qid];

        uint32_t major_workload_count = 0;
        const auto mmap_date_range_major_workload = [&](const date_t d_aligned_begin, const date_t d_aligned_end, const uint8_t type) {
            uint16_t workload_index;
            workload_info_t workload_info;
            const auto ymd_begin = date_get_ymd(d_aligned_begin);
            const auto valid_d_aligned_end = std::min<date_t>(d_aligned_end, MAX_TABLE_DATE);
            const auto ymd_end = date_get_ymd(valid_d_aligned_end);
            DEBUG("[#%u] query%u mmap major d_range[%u<%u-%u-%u>,%u<%u-%u-%u>) with type%d", 
                tid, qid, 
                d_aligned_begin, std::get<0>(ymd_begin), std::get<1>(ymd_begin), std::get<2>(ymd_begin),
                valid_d_aligned_end, std::get<0>(ymd_end), std::get<1>(ymd_end), std::get<2>(ymd_end), type
            );
            for (date_t orderdate = d_aligned_begin; orderdate < d_aligned_end; orderdate += CONFIG_ORDERDATES_PER_BUCKET) {
                const uint32_t bucket_id = calc_bucket_index(query.q_mktid, orderdate);
                const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
                const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

                const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - begin_bucket_id);
                const uint64_t bucket_size_major = g_endoffset_major_ptr[bucket_id] - bucket_start_offset_major;
                if (bucket_size_major == 0) continue;

                TRACE("[#%u] query%u want to take %uth(g%u) mmap workload from major_bag",
                    tid, qid,
                    major_workload_count, global_major_workload_count
                );
                g_major_workload_index_bag.take(&workload_index);
                uint32_t* const ptr = (uint32_t*)((uintptr_t)g_major_workload_mmap_base_ptr + (uintptr_t)(workload_index * g_major_workload_mmap_size));
                uint32_t* mapped_ptr = (uint32_t*)mmap(
                    ptr,
                    bucket_size_major,
                    PROT_READ,
                    MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                    g_holder_major_fds[holder_id],
                    bucket_start_offset_major
                );
                CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
                ASSERT(mapped_ptr == ptr);
                workload_info.orderdate = orderdate;
                workload_info.type = type;
                workload_info.index = workload_index;
                g_major_workload_info_queue.push(workload_info);
                TRACE("[#%u] query%u pushed %uth(g%u) mmap workload to major_queue, <%u,%u,%u>",
                    tid, qid,
                    major_workload_count, global_major_workload_count,
                    workload_info.orderdate, workload_info.type, workload_info.index
                );
                ++major_workload_count;
                ++global_major_workload_count;
            }
        };

#if ENABLE_CPU_HANDLE_MINOR_WORKLOAD
        const auto mmap_date_range_minor_workload = [&](const date_t d_aligned_begin, const date_t d_aligned_end, const uint8_t type) {
            uint16_t workload_index;
            workload_info_t workload_info;
            const auto ymd_begin = date_get_ymd(d_aligned_begin);
            const auto valid_d_aligned_end = std::min<date_t>(d_aligned_end, MAX_TABLE_DATE);
            const auto ymd_end = date_get_ymd(valid_d_aligned_end);
            TRACE("[#%u] query%u mmap minor d_range[%u<%u-%u-%u>,%u<%u-%u-%u>) with type%d", 
                tid, qid, 
                d_aligned_begin, std::get<0>(ymd_begin), std::get<1>(ymd_begin), std::get<2>(ymd_begin),
                valid_d_aligned_end, std::get<0>(ymd_end), std::get<1>(ymd_end), std::get<2>(ymd_end), type
            );
            for (date_t orderdate = d_aligned_begin; orderdate < d_aligned_end; orderdate += CONFIG_ORDERDATES_PER_BUCKET) {
                const uint32_t bucket_id = calc_bucket_index(query.q_mktid, orderdate);
                const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
                const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

                const uintptr_t bucket_start_offset_minor = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - begin_bucket_id);
                const uint64_t bucket_size_minor = g_endoffset_minor_ptr[bucket_id] - bucket_start_offset_minor;
                if (bucket_size_minor == 0) continue;

                g_minor_workload_index_bag.take(&workload_index);
                uint32_t* const ptr = (uint32_t*)((uintptr_t)g_minor_workload_mmap_base_ptr + (uintptr_t)(workload_index * g_minor_workload_mmap_size));
                uint32_t* mapped_ptr = (uint32_t*)mmap(
                    ptr,
                    bucket_size_minor,
                    PROT_READ,
                    MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                    g_holder_minor_fds[holder_id],
                    bucket_start_offset_minor
                );
                CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
                ASSERT(mapped_ptr == ptr);
                workload_info.orderdate = orderdate;
                workload_info.type = type;
                workload_info.index = workload_index;
                g_minor_workload_info_queue.push(workload_info);
            }
        };
#endif

        uint32_t mmap_date_range_count = 0;
        const auto add_mmap_plan = [&](const date_t d_aligned_begin, const date_t d_aligned_end, const uint8_t type) {
            ASSERT(mmap_date_range_count < mmap_date_range_size);
            if (d_aligned_end <= d_aligned_begin) return;
            mmap_date_ranges[mmap_date_range_count].d_aligned_begin = d_aligned_begin;
            mmap_date_ranges[mmap_date_range_count].d_aligned_end = d_aligned_end;
            mmap_date_ranges[mmap_date_range_count].type = type;
            ++mmap_date_range_count;
            DEBUG("[#%u] query%u add mmap range [%u,%u) with type%u", 
                tid, qid, d_aligned_begin, d_aligned_end, type);
        };

        const auto implement_mmap_plan = [&]() {
            ASSERT(mmap_date_range_count <= mmap_date_range_size);
            for (uint32_t i = 0; i < mmap_date_range_count; ++i) {
                mmap_date_range_major_workload(
                    mmap_date_ranges[i].d_aligned_begin,
                    mmap_date_ranges[i].d_aligned_end,
                    mmap_date_ranges[i].type
                );
            }
            g_major_workload_info_queue.mark_push_finish();
#if ENABLE_CPU_HANDLE_MINOR_WORKLOAD
            for (uint32_t i = 0; i < mmap_date_range_count; ++i) {
                mmap_date_range_minor_workload(
                    mmap_date_ranges[i].d_aligned_begin,
                    mmap_date_ranges[i].d_aligned_end,
                    mmap_date_ranges[i].type
                );
            }
            g_minor_workload_info_queue.mark_push_finish();
#endif
        };

        ASSERT(query.d_scan_begin <= query.d_exact_pretopn_begin);
        ASSERT(query.d_exact_pretopn_begin <= query.d_pretopn_begin);
        ASSERT(query.d_pretopn_begin <= query.d_pretopn_end);
        ASSERT(query.d_pretopn_end <= query.d_exact_pretopn_end);
        ASSERT(query.d_exact_pretopn_end <= query.d_scan_end);
        if (query.d_scan_begin >= query.d_scan_end || query.q_topn <= 0) {
            implement_mmap_plan();
            // g_queries_done[qid].wait_done();
            g_finish_working_sem.wait();
            continue;
        }
        date_t base_orderdate = calc_bucket_base_orderdate(query.d_scan_begin);
        // now base_orderdate <= query.d_scan_begin
        if (base_orderdate < query.d_scan_begin) {
            date_t end_orderdate = base_orderdate + CONFIG_ORDERDATES_PER_BUCKET;
            add_mmap_plan(base_orderdate, end_orderdate, 0);
            base_orderdate = end_orderdate;
            if (base_orderdate >= query.d_scan_end) {
                implement_mmap_plan();
                // g_queries_done[qid].wait_done();
                g_finish_working_sem.wait();
                continue;
            }
        }
        // now base_orderdate >= query.d_scan_begin
        if (__likely(base_orderdate < query.d_exact_pretopn_begin)) {
            date_t end_orderdate = calc_bucket_base_orderdate(query.d_exact_pretopn_begin);
            add_mmap_plan(base_orderdate, end_orderdate, 1);
            base_orderdate = end_orderdate;
            if (end_orderdate < query.d_exact_pretopn_begin) {
                end_orderdate += CONFIG_ORDERDATES_PER_BUCKET;
                if (end_orderdate > query.d_scan_end) {
                    add_mmap_plan(base_orderdate, end_orderdate, 0); 
                    implement_mmap_plan();
                    // g_queries_done[qid].wait_done();
                    g_finish_working_sem.wait();
                    continue;
                }
                else {
                    add_mmap_plan(base_orderdate, end_orderdate, 1);
                    base_orderdate = end_orderdate;
                    if (end_orderdate == query.d_scan_end) {
                        implement_mmap_plan();
                        // g_queries_done[qid].wait_done();
                        g_finish_working_sem.wait();
                        continue;
                    }
                }
            }
        }
        // now base_orderdate >= query.d_exact_pretopn_begin
        if (base_orderdate < query.d_pretopn_begin) {
            date_t end_orderdate = calc_bucket_base_orderdate(query.d_pretopn_begin);
            add_mmap_plan(base_orderdate, end_orderdate, 2);
            base_orderdate = end_orderdate;
            if (end_orderdate != query.d_pretopn_begin) { // not aligned, no actual pretopn
                end_orderdate += CONFIG_ORDERDATES_PER_BUCKET;
                ASSERT(end_orderdate > query.d_pretopn_end);
                if (end_orderdate <= query.d_exact_pretopn_end) {
                    add_mmap_plan(base_orderdate, end_orderdate, 2);
                    base_orderdate = end_orderdate;
                }
                else if (end_orderdate > query.d_scan_end) {
                    add_mmap_plan(base_orderdate, end_orderdate, 0);
                    implement_mmap_plan();
                    // g_queries_done[qid].wait_done();
                    g_finish_working_sem.wait();
                    continue;
                }
                else if (end_orderdate == query.d_scan_end) {
                    add_mmap_plan(base_orderdate, end_orderdate, 1);
                    implement_mmap_plan();
                    // g_queries_done[qid].wait_done();
                    g_finish_working_sem.wait();
                    continue;
                }

            }
        }
        if (base_orderdate <= query.d_pretopn_end) {
            base_orderdate = query.d_pretopn_end;
            // ASSERT((base_orderdate - MIN_TABLE_DATE) % CONFIG_ORDERDATES_PER_BUCKET == 0);
        }
        // now base_orderdate >= query.d_pretopn_end
        if (base_orderdate < query.d_exact_pretopn_end) {
            date_t end_orderdate = calc_bucket_base_orderdate(query.d_exact_pretopn_end);
            add_mmap_plan(base_orderdate, end_orderdate, 2);
            base_orderdate = end_orderdate;
        }
        // now base_orderdate >= calc_bucket_base_orderdate(query.d_exact_pretopn_end)
        if (base_orderdate < query.d_scan_end) {
            date_t end_orderdate = calc_bucket_base_orderdate(query.d_scan_end);
            add_mmap_plan(base_orderdate, end_orderdate, 1);
            base_orderdate = end_orderdate;
            if (base_orderdate < query.d_scan_end) {
                end_orderdate = base_orderdate + CONFIG_ORDERDATES_PER_BUCKET;
                add_mmap_plan(base_orderdate, end_orderdate, 0);
                base_orderdate = end_orderdate;
            }
        }
        ASSERT(base_orderdate >= query.d_scan_end);
        implement_mmap_plan();
        // g_queries_done[qid].wait_done();
        g_finish_working_sem.wait();
        continue;
    }

    INFO("loader %u finished work and begin to sync", tid);

    g_shared->loader_sync_barrier.sync_and_run_once([]() {
        for (uint32_t qid = 0; qid < g_query_count; ++qid) {
            TRACE("outputer begin to wait for query%u", qid);
            g_queries_done[qid].wait_done();

            // print query
            const query_t& query = g_queries[qid];
#if defined(MAKE_FASTEST)
            const size_t cnt = fwrite(query.output, sizeof(char), query.output_size, stdout);
            CHECK(cnt == query.output_size);
#endif
            DEBUG("query%u result length%u, done", qid, query.output_size);
        }
        C_CALL(fflush(stdout));
    });

}

void fn_worker_thread_use_index(const uint32_t tid) noexcept
{
    uint32_t global_major_workload_count = 0;
    while (true) {
        g_start_working_sem.wait();

        const uint32_t qid = g_curr_working_qid;
        if (qid == -1) break;
        query_t& query = g_queries[qid];
        g_pretopn_queries_done[qid].wait_done();

        const date_t q_shipdate = query.q_shipdate;
        uint32_t* p;
        uint32_t* end;
        date_t base_orderdate;

        uint64_t bucket_size_major;

        const auto scan_major_workload_type0 = [&]() {
            DEBUG("[#%u] query%u scan major type0 orderdate %u, q_orderdate %u, q_shipdate %u",
                tid, qid, base_orderdate, query.q_orderdate, q_shipdate
            );
            // ASSERT(q_shipdate >= base_orderdate);
            // ASSERT((q_shipdate - base_orderdate) < (1 << 8));
            const __m256i expend_mask = _mm256_set_epi32(
                0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
                0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);
            __m256i greater_than_value;
            if (q_shipdate >= base_orderdate) {
                greater_than_value = _mm256_set1_epi32(((q_shipdate - base_orderdate) << 24) | 0x00ffffff);
            }
            else {
                greater_than_value = _mm256_set1_epi32(0);
            }

//             uint32_t count = 0;
//             __m256i items1, items2, items3, items4;
//             uint32_t orderkey1, orderkey2, orderkey3, orderkey4;
//             date_t orderdate1, orderdate2, orderdate3, orderdate4;
//             while (p < end) {
//                 while (p < end) {
//                     const uint32_t orderdate_diff1 = *(p + 7) >> 30;
//                     orderdate1 = base_orderdate + orderdate_diff1;
//                     if (orderdate1 < query.d_scan_begin || orderdate1 >= query.d_scan_end) {
//                         p += 8;
//                         continue;
//                     }
//                     else {
//                         orderkey1 = *(p + 7) & ~0xC0000000U;
//                         items1 = _mm256_load_si256((__m256i*)p);
//                         p += 8;
//                         const __m256i gt_mask1 = _mm256_cmpgt_epi32(items1, greater_than_value);
//                         if (_mm256_testz_si256(gt_mask1, gt_mask1)) continue;
//                         items1 = _mm256_and_si256(items1, gt_mask1);
//                         items1 = _mm256_and_si256(items1, expend_mask);
//                         ++count;
//                         break;
//                     }
//                 }
//                 if (p >= end) break;

//                 while (p < end) {
//                     const uint32_t orderdate_diff2 = *(p + 7) >> 30;
//                     orderdate2 = base_orderdate + orderdate_diff2;
//                     if (orderdate2 < query.d_scan_begin || orderdate2 >= query.d_scan_end) {
//                         p += 8;
//                         continue;
//                     }
//                     else {
//                         orderkey2 = *(p + 7) & ~0xC0000000U;
//                         items2 = _mm256_load_si256((__m256i*)p);
//                         p += 8;
//                         const __m256i gt_mask2 = _mm256_cmpgt_epi32(items2, greater_than_value);
//                         if (_mm256_testz_si256(gt_mask2, gt_mask2)) continue;
//                         items2 = _mm256_and_si256(items2, gt_mask2);
//                         items2 = _mm256_and_si256(items2, expend_mask);
//                         ++count;
//                         break;
//                     }
//                 }
//                 if (p >= end) break;

//                 while (p < end) {
//                     const uint32_t orderdate_diff3 = *(p + 7) >> 30;
//                     orderdate3 = base_orderdate + orderdate_diff3;
//                     if (orderdate3 < query.d_scan_begin || orderdate3 >= query.d_scan_end) {
//                         p += 8;
//                         continue;
//                     }
//                     else {
//                         orderkey3 = *(p + 7) & ~0xC0000000U;
//                         items3 = _mm256_load_si256((__m256i*)p);
//                         p += 8;
//                         const __m256i gt_mask3 = _mm256_cmpgt_epi32(items3, greater_than_value);
//                         if (_mm256_testz_si256(gt_mask3, gt_mask3)) continue;
//                         items3 = _mm256_and_si256(items3, gt_mask3);
//                         items3 = _mm256_and_si256(items3, expend_mask);
//                         ++count;
//                         break;
//                     }
//                 }
//                 if (p >= end) break;

//                 while (p < end) {
//                     const uint32_t orderdate_diff4 = *(p + 7) >> 30;
//                     orderdate4 = base_orderdate + orderdate_diff4;
//                     if (orderdate4 < query.d_scan_begin || orderdate4 >= query.d_scan_end) {
//                         p += 8;
//                         continue;
//                     }
//                     else {
//                         orderkey4 = *(p + 7) & ~0xC0000000U;
//                         items4 = _mm256_load_si256((__m256i*)p);
//                         p += 8;
//                         const __m256i gt_mask4 = _mm256_cmpgt_epi32(items4, greater_than_value);
//                         if (_mm256_testz_si256(gt_mask4, gt_mask4)) continue;
//                         items4 = _mm256_and_si256(items4, gt_mask4);
//                         items4 = _mm256_and_si256(items4, expend_mask);
//                         ++count;
//                         break;
//                     }
//                 }
//                 // if (p >= end) break;
//                 if (count % 4 != 0) {
//                     break;
//                 }

//                 // TODO: looks for better way!
//                 // See https://stackoverflow.com/questions/9775538/fastest-way-to-do-horizontal-vector-sum-with-avx-instructions
//                 const __m256i tmp1 = _mm256_hadd_epi32(items1, items2);
//                 const __m256i tmp2 = _mm256_hadd_epi32(items3, items4);
//                 const __m256i tmp3 = _mm256_hadd_epi32(tmp1, tmp2);
//                 const __m128i tmp3lo = _mm256_castsi256_si128(tmp3);
//                 const __m128i tmp3hi = _mm256_extracti128_si256(tmp3, 1);
//                 const __m128i sum = _mm_add_epi32(tmp3hi, tmp3lo);

//                 const uint32_t total_expend_cent1 = _mm_extract_epi32(sum, 0);
//                 const uint32_t total_expend_cent2 = _mm_extract_epi32(sum, 1);
//                 const uint32_t total_expend_cent3 = _mm_extract_epi32(sum, 2);
//                 const uint32_t total_expend_cent4 = _mm_extract_epi32(sum, 3);

// #define _CHECK_RESULT(N) \
//                 /* ASSERT(total_expend_cent##N > 0); */ \
//                 if (__likely(total_expend_cent##N > 0)) { \
//                     query_result_t tmp; \
//                     tmp.orderdate = orderdate##N; \
//                     tmp.orderkey = orderkey##N; \
//                     tmp.total_expend_cent = total_expend_cent##N; \
//                     \
//                     if (query.result_size < query.q_topn) { \
//                         query.result[query.result_size++] = tmp; \
//                         if (__unlikely(query.result_size == query.q_topn)) { \
//                             std::make_heap(&query.result[0], &query.result[query.result_size], std::greater<>()); \
//                         } \
//                     } \
//                     else { \
//                         if (tmp > query.result[0]) { \
//                             std::pop_heap(&query.result[0], &query.result[query.result_size], std::greater<>()); \
//                             query.result[query.result_size - 1] = tmp; \
//                             std::push_heap(&query.result[0], &query.result[query.result_size], std::greater<>()); \
//                         } \
//                     } \
//                 }

//                 _CHECK_RESULT(1)
//                 _CHECK_RESULT(2)
//                 _CHECK_RESULT(3)
//                 _CHECK_RESULT(4)
// #undef _CHECK_RESULT
//             }
//             ASSERT(p >= end);

// #define _CHECK_ITEM_RESULT(N) {\
//                 __m256i sum = _mm256_hadd_epi32(items##N, items##N); \
//                 sum = _mm256_hadd_epi32(sum, sum); \
//                 const uint32_t total_expend_cent = _mm256_extract_epi32(sum, 0) + _mm256_extract_epi32(sum, 4); \
//                 /* ASSERT(total_expend_cent > 0); */ \
//                 if (__likely(total_expend_cent > 0)) { \
//                     query_result_t tmp; \
//                     tmp.orderdate = orderdate##N; \
//                     tmp.orderkey = orderkey##N; \
//                     tmp.total_expend_cent = total_expend_cent; \
//                     \
//                     if (query.result_size < query.q_topn) { \
//                         query.result[query.result_size++] = tmp; \
//                         if (__unlikely(query.result_size == query.q_topn)) { \
//                             std::make_heap(&query.result[0], &query.result[query.result_size], std::greater<>()); \
//                         } \
//                     } \
//                     else { \
//                         if (tmp > query.result[0]) { \
//                             std::pop_heap(&query.result[0], &query.result[query.result_size], std::greater<>()); \
//                             query.result[query.result_size - 1] = tmp; \
//                             std::push_heap(&query.result[0], &query.result[query.result_size], std::greater<>()); \
//                         } \
//                     } \
//                 } \
//             }

//             switch (count % 4) {
//             case 3: 
//                 _CHECK_ITEM_RESULT(3)
//             case 2:
//                 _CHECK_ITEM_RESULT(2)
//             case 1:
//                 _CHECK_ITEM_RESULT(1)
//             }
// #undef _CHECK_ITEM_RESULT
            while (p < end) {
                const uint32_t orderdate_diff = *(p + 7) >> 30;
                const date_t orderdate = base_orderdate + orderdate_diff;
                if (orderdate >= query.q_orderdate) {
                    p += 8;
                    continue;
                }

                const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
                __m256i items = _mm256_load_si256((__m256i*)p);
                p += 8;
                const __m256i gt_mask = _mm256_cmpgt_epi32(items, greater_than_value);
                //if (_mm256_testz_si256(gt_mask, gt_mask)) continue;
                items = _mm256_and_si256(items, gt_mask);
                items = _mm256_and_si256(items, expend_mask);

                __m256i sum = _mm256_hadd_epi32(items, items);
                sum = _mm256_hadd_epi32(sum, sum);
                const uint32_t total_expend_cent = _mm256_extract_epi32(sum, 0) + _mm256_extract_epi32(sum, 4);

                if (total_expend_cent > 0) {
                    query_result_t tmp;
                    tmp.orderdate = orderdate;
                    tmp.orderkey = orderkey;
                    tmp.total_expend_cent = total_expend_cent;

                    if (query.result_size < query.q_topn) {
                        query.result[query.result_size++] = tmp;
                        if (__unlikely(query.result_size == query.q_topn)) {
                            std::make_heap(query.result, query.result + query.result_size, std::greater<>());
                        }
                    }
                    else {
                        ASSERT(query.result_size > 0);
                        ASSERT(query.result_size == query.q_topn);
                        if (tmp > query.result[0]) {
                            std::pop_heap(query.result, query.result + query.result_size, std::greater<>());
                            query.result[query.result_size-1] = tmp;
                            std::push_heap(query.result, query.result + query.result_size, std::greater<>());
                        }
                    }
                }
            }

        };


        const auto scan_major_workload_type12 = [&](uint8_t type) {
            ASSERT(type == 1 || type == 2);
            const __m256i expend_mask = _mm256_set_epi32(
                0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
                0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);
            __m256i greater_than_value;
            if (type == 1) {
                if (q_shipdate >= base_orderdate) {
                    greater_than_value = _mm256_set1_epi32(((q_shipdate - base_orderdate) << 24) | 0x00ffffff);
                }
                else {
                    greater_than_value = _mm256_set1_epi32(0);
                }
                // greater_than_value = _mm256_set1_epi32(((q_shipdate - base_orderdate) << 24) | 0x00ffffff);
            }
            const uint32_t* const end_align32 = p + __align_down(bucket_size_major / sizeof(uint32_t), 32);
            while (p < end_align32) {
                const uint32_t orderdate_diff1 = *(p + 7) >> 30;
                const date_t orderdate1 = base_orderdate + orderdate_diff1;
                const uint32_t orderkey1 = *(p + 7) & ~0xC0000000U;
                __m256i items1 = _mm256_load_si256((__m256i*)p);
                p += 8;
                if (type == 1) {
                    const __m256i gt_mask1 = _mm256_cmpgt_epi32(items1, greater_than_value);
                    items1 = _mm256_and_si256(items1, gt_mask1);
                }
                items1 = _mm256_and_si256(items1, expend_mask);

                const uint32_t orderdate_diff2 = *(p + 7) >> 30;
                const date_t orderdate2 = base_orderdate + orderdate_diff2;
                const uint32_t orderkey2 = *(p + 7) & ~0xC0000000U;
                __m256i items2 = _mm256_load_si256((__m256i*)p);
                p += 8;
                if (type == 1) {
                    const __m256i gt_mask2 = _mm256_cmpgt_epi32(items2, greater_than_value);
                    items2 = _mm256_and_si256(items2, gt_mask2);
                }
                items2 = _mm256_and_si256(items2, expend_mask);

                const uint32_t orderdate_diff3 = *(p + 7) >> 30;
                const date_t orderdate3 = base_orderdate + orderdate_diff3;
                const uint32_t orderkey3 = *(p + 7) & ~0xC0000000U;
                __m256i items3 = _mm256_load_si256((__m256i*)p);
                p += 8;
                if (type == 1) {
                    const __m256i gt_mask3 = _mm256_cmpgt_epi32(items3, greater_than_value);
                    items3 = _mm256_and_si256(items3, gt_mask3);
                }
                items3 = _mm256_and_si256(items3, expend_mask);

                const uint32_t orderdate_diff4 = *(p + 7) >> 30;
                const date_t orderdate4 = base_orderdate + orderdate_diff4;
                const uint32_t orderkey4 = *(p + 7) & ~0xC0000000U;
                __m256i items4 = _mm256_load_si256((__m256i*)p);
                p += 8;
                if (type == 1) {
                    const __m256i gt_mask4 = _mm256_cmpgt_epi32(items4, greater_than_value);
                    items4 = _mm256_and_si256(items4, gt_mask4);
                }
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
                    if (query.result_size < query.q_topn) { \
                        query.result[query.result_size++] = tmp; \
                        if (__unlikely(query.result_size == query.q_topn)) { \
                            std::make_heap(&query.result[0], &query.result[query.result_size], std::greater<>()); \
                        } \
                    } \
                    else { \
                        if (tmp > query.result[0]) { \
                            std::pop_heap(&query.result[0], &query.result[query.result_size], std::greater<>()); \
                            query.result[query.result_size - 1] = tmp; \
                            std::push_heap(&query.result[0], &query.result[query.result_size], std::greater<>()); \
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

                if (type == 1) {
                    const __m256i gt_mask = _mm256_cmpgt_epi32(items, greater_than_value);
                    if (_mm256_testz_si256(gt_mask, gt_mask)) continue;

                    items = _mm256_and_si256(items, gt_mask);
                }
                items = _mm256_and_si256(items, expend_mask);

                // TODO: looks for better way!
                // See https://stackoverflow.com/questions/9775538/fastest-way-to-do-horizontal-vector-sum-with-avx-instructions
                __m256i sum = _mm256_hadd_epi32(items, items);
                sum = _mm256_hadd_epi32(sum, sum);
                const uint32_t total_expend_cent = _mm256_extract_epi32(sum, 0) + _mm256_extract_epi32(sum, 4);
                // ASSERT(total_expend_cent > 0);
                if(__likely(total_expend_cent > 0)) {
                    query_result_t tmp;
                    tmp.orderdate = orderdate;
                    tmp.orderkey = orderkey;
                    tmp.total_expend_cent = total_expend_cent;

                    if (query.result_size < query.q_topn) {
                        query.result[query.result_size++] = tmp;
                        if (__unlikely(query.result_size == query.q_topn)) {
                            std::make_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                        }
                    }
                    else {
                        if (tmp > query.result[0]) {
                            std::pop_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                            query.result[query.result_size - 1] = tmp;
                            std::push_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                        }
                    }
                }
            }
        };

#if ENABLE_CPU_HANDLE_MINOR_WORKLOAD
        uint64_t bucket_size_minor;

        const auto scan_minor_workload_type012 = [&]() {
            while (p < end) {
                const uint32_t orderdate_diff = *(p + 3) >> 30;
                const date_t orderdate = base_orderdate + orderdate_diff;
                if (orderdate >= query.q_orderdate) {
                    p += 4;
                    continue;
                }
                const uint32_t orderkey = *(p + 3) & ~0xC0000000U;
                uint32_t total_expend_cent = 0;

                const uint32_t value1 = *p;
                const date_t shipdate1 = base_orderdate + (value1 >> 24);
                if (shipdate1 > query.q_shipdate) {
                    total_expend_cent += (value1 & 0x00FFFFFF);
                }

                const uint32_t value2 = *(p + 1);
                const date_t shipdate2 = base_orderdate + (value2 >> 24);
                if (shipdate2 > query.q_shipdate) {
                    total_expend_cent += (value2 & 0x00FFFFFF);
                }

                const uint32_t value3 = *(p + 2);
                const date_t shipdate3 = base_orderdate + (value3 >> 24);
                if (shipdate3 > query.q_shipdate) {
                    total_expend_cent += (value3 & 0x00FFFFFF);
                }

                p += 4;

                query_result_t tmp;
                tmp.orderdate = orderdate;
                tmp.orderkey = orderkey;
                tmp.total_expend_cent = total_expend_cent;

                if (query.result_size < query.q_topn) {
                    query.result[query.result_size++] = tmp;
                    if (__unlikely(query.result_size == query.q_topn)) {
                        std::make_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                    }
                }
                else {
                    if (tmp > query.result[0]) {
                        std::pop_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                        query.result[query.result_size - 1] = tmp;
                        std::push_heap(&query.result[0], &query.result[query.result_size], std::greater<>());
                    }
                }
            }
        };
#endif

        workload_info_t workload_info;
        uint32_t major_workload_count = 0;
        while (g_major_workload_info_queue.pop(&workload_info)) {
            TRACE("[#%u] query%u poped %uth(g%u) mmap workload from major_queue, <%u,%u,%u>",
                tid, qid,
                major_workload_count, global_major_workload_count,
                workload_info.orderdate, workload_info.type, workload_info.index
            );
            base_orderdate = workload_info.orderdate;
            const uint32_t bucket_id = calc_bucket_index(query.q_mktid, base_orderdate);
            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - begin_bucket_id);
            bucket_size_major = g_endoffset_major_ptr[bucket_id] - bucket_start_offset_major;
            p = (uint32_t*)((uintptr_t)g_major_workload_mmap_base_ptr + (uintptr_t)(workload_info.index * g_major_workload_mmap_size));
            end = (uint32_t*)((uintptr_t)p + (uintptr_t)bucket_size_major);
            ASSERT(bucket_size_major % (8 * sizeof(uint32_t)) == 0);

            switch(workload_info.type) {
            case 0:
                scan_major_workload_type0();
                break;
            case 1:
                // break;
            case 2:
                scan_major_workload_type12(workload_info.type);
                break;
            default:
                ASSERT(workload_info.type <= 2);
            }
            // if (workload_info.type == 0) {
            //     scan_major_workload_type0();
            // }
            // else {
            //     scan_major_workload_type12(workload_info.type);
            // }

            g_major_workload_index_bag.return_back(workload_info.index);
            TRACE("[#%u] query%u returned back %uth(g%u) mmap workload to major_bag",
                tid, qid,
                major_workload_count, global_major_workload_count
            );
            ++major_workload_count;
            ++global_major_workload_count;
        }
#if ENABLE_CPU_HANDLE_MINOR_WORKLOAD
        while (g_minor_workload_info_queue.pop(&workload_info)) {
            base_orderdate = workload_info.orderdate;
            const uint32_t bucket_id = calc_bucket_index(query.q_mktid, base_orderdate);
            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_minor = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - begin_bucket_id);
            bucket_size_minor = g_endoffset_minor_ptr[bucket_id] - bucket_start_offset_minor;
            p = (uint32_t*)((uintptr_t)g_minor_workload_mmap_base_ptr + (uintptr_t)(workload_info.index * g_minor_workload_mmap_size));
            end = (uint32_t*)((uintptr_t)p + (uintptr_t)bucket_size_minor);
            ASSERT(bucket_size_minor % (8 * sizeof(uint32_t)) == 0);

            switch(workload_info.type) {
            case 0:
                // scan_minor_workload_type0();
                // break;
            case 1:
                // break;
            case 2:
                scan_minor_workload_type012();
                break;
            default:
                ASSERT(workload_info.type <= 2);
            }

            g_minor_workload_index_bag.return_back(workload_info.index);
        }
#endif
        std::sort(&query.result[0], &query.result[query.result_size], std::greater<>());

        //
        // print query to string
        //
        std::string output;
        const auto append_u32 = [&](const uint32_t n) noexcept {
            ASSERT(n > 0);
            output += std::to_string(n);  // TODO: implement it!
        };
        const auto append_u32_width2 = [&](const uint32_t n) noexcept {
            ASSERT(n <= 99);
            output += (char)('0' + n / 10);
            output += (char)('0' + n % 10);
        };
        const auto append_u32_width4 = [&](const uint32_t n) noexcept {
            ASSERT(n <= 9999);
            output += (char)('0' + (n       ) / 1000);
            output += (char)('0' + (n % 1000) / 100);
            output += (char)('0' + (n % 100 ) / 10);
            output += (char)('0' + (n % 10 )  / 1);
        };
        output.reserve((size_t)(query.q_topn + 1) * 40);  // max line length: ~32, reserved to 40
        output += "l_orderkey|o_orderdate|revenue\n";
        for (uint32_t i = 0; i < query.result_size; ++i) {
            //printf("%u|%u-%02u-%02u|%u.%02u\n",
            //       line.orderkey,
            //       std::get<0>(ymd), std::get<1>(ymd), std::get<2>(ymd),
            //       line.total_expend_cent / 100, line.total_expend_cent % 100);
            const query_result_t& line = query.result[i];
            const auto ymd = date_get_ymd(line.orderdate);
            append_u32(((line.orderkey & ~0b111) << 2) | (line.orderkey & 0b111));
            output += '|';
            append_u32_width4(std::get<0>(ymd));
            output += '-';
            append_u32_width2(std::get<1>(ymd));
            output += '-';
            append_u32_width2(std::get<2>(ymd));
            output += '|';
            append_u32(line.total_expend_cent / 100);
            output += '.';
            append_u32_width2(line.total_expend_cent % 100);
            output += '\n';
        }
        
        query.output_size = output.size();
        CHECK(query.output_size <= (query.q_topn + 1) * 40);
        memcpy(query.output, output.c_str(), query.output_size);

        INFO("[#%u] query #%u done", tid, qid);
        g_queries_done[qid].mark_done();
        g_finish_working_sem.post();
    }

}

void fn_unloader_thread_use_index() noexcept
{

}


