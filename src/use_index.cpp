#include "common.h"


//==============================================================================
// Structures
//==============================================================================
struct query_context_t
{
    process_shared_mutex wrlock { };
    uint32_t wrlock_try_lock_fail_count = 0;

    uint32_t q_topn = 0;
    date_t q_orderdate = MIN_TABLE_DATE - 1;
    date_t q_shipdate = MIN_TABLE_DATE - 1;
    uint8_t q_mktid = 0;
    bool is_bad_query = false;

    uint32_t remaining_pretopn_plates = 0;
    uint32_t remaining_major_buckets = 0;
    uint32_t remaining_minor_buckets = 0;

    //
    // Queries are processed in 6 bucket ranges:
    //
    //  [check_orderdate_check_shipdate_begin_bucket_id, only_check_shipdate_begin_bucket_id)
    //      Should check orderdate < q_orderdate && shipdate > q_shipdate
    //
    //  [only_check_shipdate_begin_bucket_id, nocheck_head_begin_bucket_id)
    //      Should check shipdate > q_shipdate
    //
    //  [nocheck_head_begin_bucket_id, pretopn_begin_bucket_id)
    //      No check - scan through major/minor index
    //
    //  [pretopn_begin_bucket_id, nocheck_tail_begin_bucket_id)
    //      No check - make use of pretopn index
    //
    //  [nocheck_tail_begin_bucket_id, only_check_orderdate_begin_bucket_id)
    //      No check - scan through major/minor index
    //
    //  [only_check_orderdate_begin_bucket_id, only_check_orderdate_end_bucket_id)
    //      Should check orderdate < q_orderdate
    //
    uint32_t check_orderdate_check_shipdate_begin_bucket_id;
    uint32_t only_check_shipdate_begin_bucket_id;
    uint32_t nocheck_head_begin_bucket_id;
    uint32_t pretopn_begin_bucket_id;
    uint32_t nocheck_tail_begin_bucket_id;
    uint32_t only_check_orderdate_begin_bucket_id;
    uint32_t only_check_orderdate_end_bucket_id;

    uint32_t current_topn = 0;
    query_result_t results[0];  // actually: [limit_topn]
};
static_assert(sizeof(query_context_t) == 72);


//==============================================================================
// Global Variables
//==============================================================================
namespace
{
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };

    posix_shm_t<void> g_query_context_shm { };
    query_context_t** g_query_contexts = nullptr;  // [g_query_count]
}


__always_inline
void parse_queries() noexcept
{
    ASSERT(g_argv_queries != nullptr);
    ASSERT(g_query_count > 0);

    g_query_contexts = (query_context_t**)malloc(sizeof(query_context_t*) * g_query_count);
    CHECK(g_query_contexts != nullptr, "malloc() failed");

    uint64_t curr_size = 0;
    for (uint32_t query_id = 0; query_id < g_query_count; ++query_id) {
        g_query_contexts[query_id] = (query_context_t*)curr_size;  // tricky here!

        const uint32_t q_topn = (uint32_t)std::strtoul(g_argv_queries[4 * query_id + 3], nullptr, 10);
        curr_size += sizeof(query_context_t);
        curr_size += q_topn * sizeof(query_result_t);
        curr_size = __align_up(curr_size, 64);  // align to CPU cache line
    }

    const bool success = g_query_context_shm.init_fixed(SHMKEY_QUERY_CONTEXT, curr_size, true);
    CHECK(success);

    INFO("g_query_context_shm.shmid: %d", g_query_context_shm.shmid);
    INFO("g_query_context_shm.size_in_byte: %lu", g_query_context_shm.size_in_byte);
    INFO("g_query_context_shm.ptr: %p", g_query_context_shm.ptr);

    // Because g_query_context_shm.size_in_byte is expected to be small here
    // We choose to shmat BEFORE fork() for (a bit) better performance
    g_query_context_shm.attach_fixed(true);

    for (uint32_t query_id = 0; query_id < g_query_count; ++query_id) {
        g_query_contexts[query_id] = (query_context_t*)((uintptr_t)g_query_context_shm.ptr + (uintptr_t)g_query_contexts[query_id]);  // tricky here!
        query_context_t* const ctx = g_query_contexts[query_id];

        new (ctx) query_context_t;
        const auto it = g_mktsegment_to_mktid.find(g_argv_queries[4 * query_id + 0]);
        if (__unlikely(it == g_mktsegment_to_mktid.end())) {
            ctx->is_bad_query = true;
            INFO("query #%u: unknown mktsegment: %s", query_id, g_argv_queries[4 * query_id + 0]);
            continue;
        }

        ctx->q_mktid = it->second;
        ctx->q_orderdate = date_from_string</*_Unchecked*/false>(g_argv_queries[4 * query_id + 1]);
        ctx->q_shipdate = date_from_string</*_Unchecked*/false>(g_argv_queries[4 * query_id + 2]);
        ctx->q_topn = (uint32_t)std::strtoul(g_argv_queries[4 * query_id + 3], nullptr, 10);

        INFO("query #%u: q_mktid=%u,q_orderdate=%u,q_shipdate=%u,q_topn=%u",
              query_id, ctx->q_mktid, ctx->q_orderdate, ctx->q_shipdate, ctx->q_topn);

        ctx->current_topn = 0;
    }
}


__always_inline
void parse_query_scan_range() noexcept
{
    ASSERT(g_shared != nullptr);
    ASSERT(g_shared->meta.max_shipdate_orderdate_diff > 0);

    while (true) {
        const uint32_t query_id = g_shared->use_index.parse_query_id_shared_counter++;
        if (query_id >= g_query_count) break;

        query_context_t* const ctx = g_query_contexts[query_id];
        ASSERT(ctx != nullptr);
        if (__unlikely(ctx->is_bad_query)) continue;

        const date_t scan_begin_orderdate = std::max<date_t>(
            ctx->q_shipdate - (g_shared->meta.max_shipdate_orderdate_diff - 1),
            MIN_TABLE_DATE);  // inclusive
        ASSERT(scan_begin_orderdate >= MIN_TABLE_DATE);
        ASSERT(scan_begin_orderdate <= MAX_TABLE_DATE);

        const date_t scan_end_orderdate = std::max<date_t>(
            std::min<date_t>(ctx->q_orderdate, MAX_TABLE_DATE + 1),
            MIN_TABLE_DATE);  // exclusive
        ASSERT(scan_end_orderdate >= MIN_TABLE_DATE);
        ASSERT(scan_end_orderdate <= MAX_TABLE_DATE + 1);

        DEBUG("query #%u: scan_begin_orderdate=%d, scan_end_orderdate=%d", query_id, scan_begin_orderdate, scan_end_orderdate);
        if (__unlikely(scan_end_orderdate <= scan_begin_orderdate)) {
            INFO("query #%u: scan_end_orderdate <= scan_begin_orderdate (skipped)", query_id);
            ctx->is_bad_query = true;
            continue;
        }

        const date_t scan_begin_nocheck_orderdate = std::min<date_t>(
            std::max<date_t>(ctx->q_shipdate, MIN_TABLE_DATE),
            MAX_TABLE_DATE);
        ASSERT(scan_begin_nocheck_orderdate >= MIN_TABLE_DATE);
        ASSERT(scan_begin_nocheck_orderdate <= MAX_TABLE_DATE);
        ASSERT(scan_begin_nocheck_orderdate >= scan_begin_orderdate);

        //const date_t scan_end_nocheck_orderdate = scan_end_orderdate;
        //ASSERT(scan_end_nocheck_orderdate >= MIN_TABLE_DATE);
        //ASSERT(scan_end_nocheck_orderdate <= MAX_TABLE_DATE);

        [[maybe_unused]] const uint32_t curr_mktid_min_bucket_id = calc_bucket_index(ctx->q_mktid, MIN_TABLE_DATE);
        [[maybe_unused]] const uint32_t curr_mktid_max_bucket_id = calc_bucket_index(ctx->q_mktid, MAX_TABLE_DATE);

        const auto __calc_bucket_index_saturate = [&](const date_t orderdate) {
            ASSERT(orderdate >= MIN_TABLE_DATE);
            ASSERT(orderdate <= MAX_TABLE_DATE + CONFIG_ORDERDATES_PER_BUCKET);
            if (orderdate > MAX_TABLE_DATE) {
                return curr_mktid_max_bucket_id;
            }
            else {
                return calc_bucket_index(ctx->q_mktid, orderdate);
            }
        };

        ctx->only_check_orderdate_end_bucket_id = __calc_bucket_index_saturate(
            scan_end_orderdate + (CONFIG_ORDERDATES_PER_BUCKET - 1));
        ASSERT(ctx->only_check_orderdate_end_bucket_id >= curr_mktid_min_bucket_id);
        ASSERT(ctx->only_check_orderdate_end_bucket_id <= curr_mktid_max_bucket_id);

        ctx->check_orderdate_check_shipdate_begin_bucket_id = ctx->only_check_orderdate_end_bucket_id;
        ctx->only_check_shipdate_begin_bucket_id = ctx->only_check_orderdate_end_bucket_id;
        ctx->nocheck_head_begin_bucket_id = ctx->only_check_orderdate_end_bucket_id;
        ctx->pretopn_begin_bucket_id = ctx->only_check_orderdate_end_bucket_id;
        ctx->nocheck_tail_begin_bucket_id = ctx->only_check_orderdate_end_bucket_id;
        ctx->only_check_orderdate_begin_bucket_id = ctx->only_check_orderdate_end_bucket_id;

        do {
            ctx->check_orderdate_check_shipdate_begin_bucket_id = calc_bucket_index(
                ctx->q_mktid,
                scan_begin_orderdate);
            ASSERT(ctx->check_orderdate_check_shipdate_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->check_orderdate_check_shipdate_begin_bucket_id <= curr_mktid_max_bucket_id);
            if (ctx->check_orderdate_check_shipdate_begin_bucket_id >= ctx->only_check_orderdate_end_bucket_id) {
                ASSERT(ctx->check_orderdate_check_shipdate_begin_bucket_id == ctx->only_check_orderdate_end_bucket_id);
                break;
            }


            ctx->only_check_shipdate_begin_bucket_id = __calc_bucket_index_saturate(
                scan_begin_orderdate + (CONFIG_ORDERDATES_PER_BUCKET - 1));
            ASSERT(ctx->only_check_shipdate_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->only_check_shipdate_begin_bucket_id <= curr_mktid_max_bucket_id);
            ASSERT(ctx->only_check_shipdate_begin_bucket_id >= ctx->check_orderdate_check_shipdate_begin_bucket_id);
            ASSERT(ctx->only_check_shipdate_begin_bucket_id <= ctx->check_orderdate_check_shipdate_begin_bucket_id + 1);
            if (ctx->only_check_shipdate_begin_bucket_id >= ctx->only_check_orderdate_end_bucket_id) {
                ASSERT(ctx->only_check_shipdate_begin_bucket_id == ctx->only_check_orderdate_end_bucket_id);
                break;
            }


            ctx->nocheck_head_begin_bucket_id = __calc_bucket_index_saturate(
                scan_begin_nocheck_orderdate + (CONFIG_ORDERDATES_PER_BUCKET - 1));
            ASSERT(ctx->nocheck_head_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->nocheck_head_begin_bucket_id <= curr_mktid_max_bucket_id);
            ASSERT(ctx->nocheck_head_begin_bucket_id >= ctx->only_check_shipdate_begin_bucket_id);
            ASSERT(ctx->nocheck_head_begin_bucket_id <= ctx->only_check_shipdate_begin_bucket_id + __div_up(g_shared->meta.max_shipdate_orderdate_diff, CONFIG_ORDERDATES_PER_BUCKET));
            if (ctx->nocheck_head_begin_bucket_id >= ctx->only_check_orderdate_end_bucket_id) {
                ASSERT(ctx->nocheck_head_begin_bucket_id == ctx->only_check_orderdate_end_bucket_id);
                break;
            }


            uint32_t pretopn_begin_plate_id;
            const uint32_t nocheck_head_plate_id = calc_plate_id(ctx->nocheck_head_begin_bucket_id);
            const uint32_t nocheck_head_plate_base_bucket_id = calc_plate_base_bucket_id_by_plate_id(nocheck_head_plate_id);
            ASSERT(ctx->nocheck_head_begin_bucket_id >= nocheck_head_plate_base_bucket_id);
            ASSERT(ctx->nocheck_head_begin_bucket_id < nocheck_head_plate_base_bucket_id + BUCKETS_PER_PLATE);
            if (ctx->nocheck_head_begin_bucket_id == nocheck_head_plate_base_bucket_id) {
                ctx->pretopn_begin_bucket_id = ctx->nocheck_head_begin_bucket_id;
                pretopn_begin_plate_id = nocheck_head_plate_id;
            }
            else {
                ASSERT(ctx->nocheck_head_begin_bucket_id > nocheck_head_plate_base_bucket_id);
                if (nocheck_head_plate_base_bucket_id + BUCKETS_PER_PLATE >= ctx->only_check_orderdate_end_bucket_id) {
                    break;
                }
                ctx->pretopn_begin_bucket_id = nocheck_head_plate_base_bucket_id + BUCKETS_PER_PLATE;
                pretopn_begin_plate_id = nocheck_head_plate_id + 1;
            }
            ASSERT(ctx->pretopn_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->pretopn_begin_bucket_id <= curr_mktid_max_bucket_id);
            ASSERT(ctx->pretopn_begin_bucket_id >= ctx->nocheck_head_begin_bucket_id);
            ASSERT(ctx->pretopn_begin_bucket_id < ctx->nocheck_head_begin_bucket_id + BUCKETS_PER_PLATE);
            if (ctx->pretopn_begin_bucket_id >= ctx->only_check_orderdate_end_bucket_id) {
                ASSERT(ctx->pretopn_begin_bucket_id == ctx->only_check_orderdate_end_bucket_id,
                    "Expect %u == %u", ctx->pretopn_begin_bucket_id, ctx->only_check_orderdate_end_bucket_id);
                break;
            }

            const date_t pretopn_begin_orderdate = calc_plate_base_orderdate_by_plate_id(pretopn_begin_plate_id);
            ASSERT(pretopn_begin_orderdate <= scan_end_orderdate);
            const uint32_t num_plates = __div_down(scan_end_orderdate - pretopn_begin_orderdate, CONFIG_TOPN_DATES_PER_PLATE);
            ctx->nocheck_tail_begin_bucket_id = ctx->pretopn_begin_bucket_id + num_plates * BUCKETS_PER_PLATE;
            ASSERT(ctx->nocheck_tail_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->nocheck_tail_begin_bucket_id <= curr_mktid_max_bucket_id);
            ASSERT(ctx->nocheck_tail_begin_bucket_id >= ctx->pretopn_begin_bucket_id);
            if (ctx->nocheck_tail_begin_bucket_id >= ctx->only_check_orderdate_end_bucket_id) {
                ASSERT(ctx->nocheck_tail_begin_bucket_id == ctx->only_check_orderdate_end_bucket_id);
                break;
            }

            ctx->only_check_orderdate_begin_bucket_id = __calc_bucket_index_saturate(
                scan_end_orderdate);
            ASSERT(ctx->only_check_orderdate_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->only_check_orderdate_begin_bucket_id <= curr_mktid_max_bucket_id);
            ASSERT(ctx->only_check_orderdate_begin_bucket_id >= ctx->nocheck_tail_begin_bucket_id);
            ASSERT(ctx->only_check_orderdate_begin_bucket_id <= ctx->only_check_orderdate_end_bucket_id);
            ASSERT(ctx->only_check_orderdate_begin_bucket_id + 1 >= ctx->only_check_orderdate_end_bucket_id);
        } while(false);


        // Important here!
        // Special check for queries whose q_topn > CONFIG_EXPECT_MAX_TOPN
        // In this case, we should disable pretopn
        if (__unlikely(ctx->q_topn > CONFIG_EXPECT_MAX_TOPN)) {
            DEBUG("query #%u: q_topn > CONFIG_EXPECT_MAX_TOPN (%u > %u)", query_id, ctx->q_topn, CONFIG_EXPECT_MAX_TOPN);
            ctx->pretopn_begin_bucket_id = ctx->nocheck_head_begin_bucket_id;
            ctx->nocheck_tail_begin_bucket_id = ctx->nocheck_head_begin_bucket_id;
        }


#if ENABLE_LOGGING_DEBUG
        {
            std::lock_guard<decltype(g_shared->use_index.parse_query_id_logging_mutex)> _lock(g_shared->use_index.parse_query_id_logging_mutex);

            DEBUG("query #%u:", query_id);
            DEBUG("    check_orderdate_check_shipdate_begin_bucket_id: %u", ctx->check_orderdate_check_shipdate_begin_bucket_id);
            DEBUG("    only_check_shipdate_begin_bucket_id: %u", ctx->only_check_shipdate_begin_bucket_id);
            DEBUG("    nocheck_head_begin_bucket_id: %u", ctx->nocheck_head_begin_bucket_id);
            DEBUG("    pretopn_begin_bucket_id: %u", ctx->pretopn_begin_bucket_id);
            DEBUG("    nocheck_tail_begin_bucket_id: %u", ctx->nocheck_tail_begin_bucket_id);
            DEBUG("    only_check_orderdate_begin_bucket_id: %u", ctx->only_check_orderdate_begin_bucket_id);
            DEBUG("    only_check_orderdate_end_bucket_id: %u", ctx->only_check_orderdate_end_bucket_id);
        }
#endif

        //
        // Add to task queue accordingly
        //

    }
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
    // Parse queries
    //
    {
        parse_queries();
    }

}

void use_index_initialize_after_fork() noexcept
{
    // Parse query range
    {
        ASSERT(g_query_contexts != nullptr);

        parse_query_scan_range();
    }
}

