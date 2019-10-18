#include "common.h"

struct query_result_t
{
    date_t orderdate;
    uint32_t orderkey;
    uint32_t total_expend_cent;

    query_result_t() noexcept = default;
    query_result_t(const uint32_t cent, const uint32_t key, const date_t date) noexcept 
        : total_expend_cent(cent), orderkey(key), orderdate(date) { }

    FORCEINLINE bool operator >(const query_result_t& other) const noexcept {
        if (total_expend_cent > other.total_expend_cent) return true;
        if (total_expend_cent < other.total_expend_cent) return false;
        return (orderkey > other.orderkey);
    }
};

struct query_t
{
    uint32_t q_index;
    date_t q_orderdate;
    date_t q_shipdate;
    uint32_t q_topn;
    uint8_t q_mktid;

    std::vector<query_result_t> results[MAX_WORKER_THREADS];
    std::vector<query_result_t> final_result;
};

#define SHIP_ORDER_DATE_OFFSET 121
struct date_range_t
{
    date_t d_begin;
    date_t d_end;     // date range [begin, end); assume begin < end
    std::vector<uint32_t> q_indexs;
    // TODO: copy constructor and operator=() for q_indexs; or replace it completely with pointer(requires extra q_num parameter)
};

/*
struct item_t
{
    uint32_t _value;

    // item_t() noexcept = default;
    // item_t(uint32_t value) noexcept { _value = value; }
    // FORCEINLINE item_t& operator=(uint32_t value) noexcept { _value = value; return *this; }
    // FORCEINLINE item_t& operator=(item_t& right) noexcept { _value = right._value; return *this; }
    FORCEINLINE uint32_t order_tag() const noexcept { return (uint32_t)(_value & 0x80000000) >> 31; }
    FORCEINLINE uint32_t shipdate_offset() const noexcept { return (uint32_t)(_value & 0x7f000000) >> 24; }
    FORCEINLINE uint32_t cent() const noexcept { return _value & 0xffffff; }
};
*/

struct item_t
{
    uint64_t _value;

    // item_t() noexcept = default;
    // item_t(uint32_t value) noexcept { _value = value; }
    // FORCEINLINE item_t& operator=(uint32_t value) noexcept { _value = value; return *this; }
    // FORCEINLINE item_t& operator=(item_t& right) noexcept { _value = right._value; return *this; }
    FORCEINLINE uint32_t orderkey() const noexcept { return (uint32_t)_value; }
    FORCEINLINE uint32_t shipdate_offset() const noexcept { return (uint32_t)(_value >> 56); }
    FORCEINLINE uint32_t cent() const noexcept { return (uint32_t)(_value >> 32) & 0xffffff; }
};
static_assert(sizeof(item_t) == 8);

struct item_range_t
{
    const item_t* i_begin;
    const item_t* i_end;
    const item_t* i_aligned_begin;
    const uint32_t* p_date_offset;
    date_t d_begin;
    date_t d_end;     // date range [begin, end); assume begin < end
    const std::vector<uint32_t>* p_q_indexs;
    uint8_t q_mktid;
};

namespace
{
    int g_mktsegment_fd = -1;
    uint64_t g_mktsegment_file_size = 0;
    uint8_t g_mktid_count = 0;
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };
    std::unordered_set<uint8_t> g_mktids { };

    int g_prefixsum_fd = -1;
    uint64_t g_prefixsum_file_size = 0;

    int g_items_fd = -1;
    uint64_t g_items_file_size = 0;

    uint32_t* g_index_prefixsum = nullptr;

    std::vector<query_t> g_queries_by_mktid[256];
    std::vector<std::pair<uint8_t /*mktid*/, uint32_t /*bucket_index*/> > g_all_queries;
    std::vector<date_range_t> g_date_ranges_by_mktid[256];
    constexpr uint64_t g_item_range_step_size = 1048576 * 16 - 4096;
    uint64_t g_max_item_ranges_count = 0;
    std::atomic_uint32_t g_date_range_offsets_by_mktid[256];
    mpmc_queue<item_range_t> g_item_range_queue;

    std::atomic_uint32_t g_synthesis_query_count { 0 };
    
    done_event* g_queries_done = nullptr;
}

FORCEINLINE size_t calc_bucket_index(const uint8_t mktid, const date_t orderdate) noexcept
{
    ASSERT(mktid < g_mktid_count, "BUG");
    ASSERT(orderdate >= MIN_TABLE_DATE, "BUG");
    ASSERT(orderdate <= MAX_TABLE_DATE, "BUG");

    return (size_t)(mktid - 0) * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) + (orderdate - MIN_TABLE_DATE);
}

void load_queries()
{
    std::vector<date_t> dates_by_mktid[256];
    g_queries_done = new done_event[g_query_count];
    g_all_queries.resize(g_query_count);
    for (uint32_t qid = 0; qid < g_query_count; ++qid) {
        const auto it = g_mktsegment_to_mktid.find(g_argv_queries[4 * qid + 0]);
        ASSERT(it != g_mktsegment_to_mktid.end(), "TODO: deal with unknown mktsegment in query");  // TODO

        const uint8_t mktid = it->second;
        g_mktids.insert(mktid);
        const size_t bucket_index = g_queries_by_mktid[mktid].size();
        g_queries_by_mktid[mktid].resize(bucket_index + 1);  // usually O(1)
        query_t& query = g_queries_by_mktid[mktid][bucket_index];
        query.q_index = qid;
        query.q_mktid = mktid;
        query.q_orderdate = date_from_string(g_argv_queries[4 * qid + 1]);
        query.q_shipdate = date_from_string(g_argv_queries[4 * qid + 2]);
        query.q_topn = (uint32_t)std::strtoul(g_argv_queries[4 * qid + 3], nullptr, 10);

        DEBUG("query #%u: q_mktid=%u,q_orderdate=%u,q_shipdate=%u,q_topn=%u",
            qid, query.q_mktid, query.q_orderdate, query.q_shipdate, query.q_topn);

        query.final_result.reserve(query.q_topn);

        // insert the involved date_range endpoints here
        const date_t d_begin = date_add_bounded_to_max_table_date(date_subtract_bounded_to_min_table_date(query.q_shipdate, SHIP_ORDER_DATE_OFFSET - 1), 0);
        const date_t d_end = date_add_bounded_to_max_table_date(date_subtract_bounded_to_min_table_date(query.q_orderdate, 0), 0);
        if (d_begin < d_end) {
            dates_by_mktid[mktid].push_back(d_begin);
            dates_by_mktid[mktid].push_back(d_end);
        }
        // TODO: else is invalid query, left the query_result empty and skip the query
        // another invalid condition: q_shipdate < q_orderdate
        // TODO: the static topn range must be considered if possible
    }

    for (auto mktid : g_mktids) {
        std::sort(
            g_queries_by_mktid[mktid].begin(),
            g_queries_by_mktid[mktid].end(),
            [](const query_t& a, const query_t& b) noexcept {
                if (a.q_orderdate > b.q_orderdate) return true;
                if (a.q_orderdate < b.q_orderdate) return false;
                return (a.q_index < b.q_index);
            });
        // TRACE("------------------------------------------------");
        // TRACE("total queries on mktid#%u: %" PRIu64, mktid, (uint64_t)g_queries_by_mktid[mktid].size());
    
        for (size_t bucket_index = 0; bucket_index < g_queries_by_mktid[mktid].size(); ++bucket_index) {
            query_t& query = g_queries_by_mktid[mktid][bucket_index];
            // TRACE("    query#%u: q_mktsegment=%u, q_orderdate=%04u-%02u-%02u, q_shipdate=%04u-%02u-%02u, q_topn=%u",
            //     query.q_index, query.q_mktsegment,
            //     query.q_orderdate.year(), query.q_orderdate.month(), query.q_orderdate.day(),
            //     query.q_shipdate.year(), query.q_shipdate.month(), query.q_shipdate.day(),
            //     query.q_topn);

            // TODO: Reserve for results?
            //for (uint32_t t = 0; t < MAX_WORKER_THREADS; ++t) {
            //    query.results[t].reserve(query.q_topn);
            //}

            // Fill g_all_queries
            g_all_queries[query.q_index] = std::make_pair((uint8_t)mktid, (uint32_t)bucket_index);
        }

        std::sort(
            dates_by_mktid[mktid].begin(),
            dates_by_mktid[mktid].end(),
            std::less<date_t>()
        );
        const size_t size = dates_by_mktid[mktid].size();
        g_date_ranges_by_mktid[mktid].resize(size + 1);
        for (size_t i = 1; i < size; ++i) {
            const date_t d_begin = dates_by_mktid[mktid][i - 1];
            const date_t d_end   = dates_by_mktid[mktid][i];
            g_date_ranges_by_mktid[mktid][i].d_begin = d_begin;
            g_date_ranges_by_mktid[mktid][i].d_end   = d_end;
            DEBUG("add date range [%u, %u)", d_begin, d_end);
            for (const query_t& query : g_queries_by_mktid[mktid]) {
                if (date_subtract_bounded_to_min_table_date(query.q_orderdate, 0) >= d_end && date_subtract_bounded_to_min_table_date(query.q_shipdate, SHIP_ORDER_DATE_OFFSET - 1) <= d_begin) {
                    g_date_ranges_by_mktid[mktid][i].q_indexs.push_back(query.q_index);
                    g_max_item_ranges_count += (g_index_prefixsum[calc_bucket_index(mktid, d_end)] - g_index_prefixsum[calc_bucket_index(mktid, d_begin)]) / g_item_range_step_size + 1;
                    DEBUG("add query %u to date range [%u, %u)", query.q_index, d_begin, d_end);
                }
            }
        }
    }

}

void fn_loader_thread_use_index(const uint32_t tid) noexcept
{
    DEBUG("[%u] loader started", tid);

    const uint64_t item_page_align_mask = ~((uint64_t)getpagesize() / sizeof(item_t) - 1);


    for (auto mktid : g_mktids) {
        while (true) {
            const uint32_t off = g_date_range_offsets_by_mktid[mktid].fetch_add(1);
            if (off >= g_date_ranges_by_mktid[mktid].size()) {
                break;
            }
            const date_range_t d_range = g_date_ranges_by_mktid[mktid][off];
            // // mmap the item_range corresponding to the d_range with several threads
            // const begin_offset = g_index_prefixsum[calc_bucket_index(mktid, d_range.d_begin)];
            // const end_offset   = g_index_prefixsum[calc_bucket_index(mktid, d_range.d_end)];
            // const item_size = end_offset - begin_offset;
            // const item_t* const p_item_range = (const item_t*)mmap_parallel(
            //     item_size * sizeof(item_t),
            //     item_size / g_item_range_step_size,
            //     PROT_READ,
            //     MAP_PRIVATE,
            //     g_items_fd,
            //     /*unsupported...*/begin_offset * sizeof(item_t)
            // );
            item_range_t i_range;
            for (date_t l = d_range.d_begin, r = d_range.d_begin; r < d_range.d_end; r = r + 1) {
                bool flag = false;
                if (UNLIKELY(r + 1 >= d_range.d_end)) {
                    r = d_range.d_end;
                    flag = true;
                }
                const uint64_t l_offset = g_index_prefixsum[calc_bucket_index(mktid, l)];
                const uint64_t l_aligned_offset = l_offset & item_page_align_mask;
                ASSERT(l_offset >= l_aligned_offset, "BUG... l_offset < l_aligned_offset");
                const uint64_t r_offset = g_index_prefixsum[calc_bucket_index(mktid, r)];
                uint64_t size = r_offset - l_offset;
                if (size >= g_item_range_step_size) {
                    flag = true;
                }
                if (flag) {
                    size = r_offset - l_aligned_offset;
                    DEBUG("map date range [%u, %u) to item_range [%u, %u), file [%.2f, %.2f)MB", l, r, l_offset, r_offset, (double)l_offset*sizeof(item_t)/1024/1024, (double)r_offset*sizeof(item_t)/1024/1024);
                    i_range.i_aligned_begin = (const item_t*)my_mmap(
                    // i_range.i_begin = (const item_t*)mmap(NULL,
                        size * sizeof(item_t),
                        PROT_READ,
                        MAP_PRIVATE | MAP_POPULATE,
                        // MAP_PRIVATE,
                        g_items_fd,
                        l_aligned_offset * sizeof(item_t)
                    );
                    // if (UNLIKELY(i_range.i_aligned_begin == MAP_FAILED)) {
                    //     PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
                    // }
                    i_range.i_begin = i_range.i_aligned_begin + (l_offset - l_aligned_offset);
                    i_range.i_end = i_range.i_aligned_begin + size;
                    // i_range.map_size = size;
                    i_range.p_date_offset = &g_index_prefixsum[calc_bucket_index(mktid, l)];
                    i_range.d_begin = l;
                    i_range.d_end = r;
                    i_range.p_q_indexs = &(d_range.q_indexs);
                    i_range.q_mktid = mktid;
                    g_item_range_queue.push(i_range);
                    // g_mapped_queues_by_mktid[mktid].push(i_range);
                    DEBUG("loader%u push i_range, date[%u,%u){ref[%u,%u)}, i_range[%p,%p), q_index p%p size%u", tid, i_range.d_begin,i_range.d_end, l,r, i_range.i_begin,i_range.i_end, i_range.p_q_indexs, (i_range.p_q_indexs)->size());
                    l = r;
                }
            }
        }
    }

    g_loader_sync_barrier.sync();
    if (tid == 0) {
        g_item_range_queue.mark_push_finish();
    }

    DEBUG("[%u] loader done", tid);
}

void worker_load_multi_part(const uint32_t tid) {
    item_range_t i_range;
    while (g_item_range_queue.pop(&i_range)) {
        ASSERT(i_range.i_begin != nullptr, "[%u] BUG: pop: i_range.i_begin == nullptr", tid);
        ASSERT(i_range.i_end   != nullptr, "[%u] BUG: pop: i_range.i_end == nullptr", tid);
        ASSERT(i_range.p_q_indexs != nullptr, "[%u] BUG: pop: i_range.p_q_indexs == nullptr", tid);
        DEBUG("worker%u fetch i_range, date[%u,%u), i_range[%p,%p), q_indexes p%p size%u", tid, i_range.d_begin,i_range.d_end, i_range.i_begin,i_range.i_end, i_range.p_q_indexs, (i_range.p_q_indexs)->size());
    
        item_t curr_items[32];
        // adjust the head curr_orderdate to the precise orderdate, skipping bubble orderdate
        date_t curr_orderdate = i_range.d_begin;
        const item_t* p = i_range.i_begin;
        const uint32_t* p_date_offset = i_range.p_date_offset + 1;
        while (*p_date_offset - *(i_range.p_date_offset) <= (uint32_t)(p - i_range.i_begin)) {
            ++p_date_offset;
            curr_orderdate = curr_orderdate + 1;
            // if (curr_orderdate >= i_range.d_end) {
            //     // goto PASS;
            //     p = i_range.i_end;
            //     break;
            // }
            ASSERT(curr_orderdate < i_range.d_end, "curr_orderdate >= i_range.d_end");
        }

        const uint8_t mktid = i_range.q_mktid;
        uint32_t curr_item_count = 0;
        uint32_t curr_orderkey = (*p).orderkey();
        const auto query_by_same_orderkey = [&]() {
            ASSERT(curr_item_count > 0, "BUG... curr_item_count is 0");
            ASSERT(curr_item_count < 32, "BUG... curr_item_count is greater than 32");
            const std::vector<uint32_t>& q_indexs = *(i_range.p_q_indexs);
            for (uint32_t q_index : q_indexs) {
                ASSERT(mktid == g_all_queries[q_index].first, "BUG... mktid mismatch");
                query_t& query = g_queries_by_mktid[mktid][g_all_queries[q_index].second];
                ASSERT(curr_orderdate < query.q_orderdate, "BUG... curr_orderdate >= query.q_orderdate, tid = %d", tid);
                TRACE("q_index=%u, curr_orderdate=%u, q_shipdate=%u", q_index, curr_orderdate, query.q_shipdate);
                uint32_t total_expend_cent = 0;
                bool matched = false;
                for (uint32_t i = 0; i < curr_item_count; ++i) {
                    if (curr_orderdate + curr_items[i].shipdate_offset() > query.q_shipdate) {
                        total_expend_cent += curr_items[i].cent();
                        matched = true;
                    }
                }

                if (matched) {
                    TRACE("worker%u matched token (%u,%u,%u) to query %u(topn=%u)", tid, total_expend_cent, curr_orderkey, curr_orderdate, q_index, query.q_topn);
                    ASSERT(tid < MAX_WORKER_THREADS, "tid >= MAX_WORKER_THREADS");
                    std::vector<query_result_t>& result = query.results[tid];
                    if (result.size() < query.q_topn) {
                        result.emplace_back(total_expend_cent, curr_orderkey, curr_orderdate);
                        if (result.size() == query.q_topn) {
                            std::make_heap(result.begin(), result.end(), std::greater<query_result_t>());
                        }
                    }
                    else {
                        if (UNLIKELY(total_expend_cent > result.begin()->total_expend_cent)) {
                            std::pop_heap(result.begin(), result.end(), std::greater<query_result_t>());
                            *result.rbegin() = query_result_t(total_expend_cent, curr_orderkey, curr_orderdate);
                            std::push_heap(result.begin(), result.end(), std::greater<query_result_t>());
                        }
                    }
                }
            }
        };

        for (; p < i_range.i_end; ++p) {
            if ((*p).orderkey() != curr_orderkey) {
                query_by_same_orderkey();
                // update the curr_orderdate for the new orderkey
                while (UNLIKELY(*p_date_offset - *(i_range.p_date_offset) <= (uint32_t)(p - i_range.i_begin))) {
                    ++p_date_offset;
                    curr_orderdate = curr_orderdate + 1;
                    // if (curr_orderdate >= i_range.d_end) {
                    //     // goto PASS;
                    //     break;
                    // }
                    ASSERT(curr_orderdate < i_range.d_end, "curr_orderdate >= i_range.d_end");
                }
                // ++curr_fake_orderkey;
                curr_orderkey = (*p).orderkey();
                curr_item_count = 0;
            }
            curr_items[curr_item_count] = *p;
            ++curr_item_count;
        }
        query_by_same_orderkey();

// PASS:
        C_CALL(munmap((void*)i_range.i_aligned_begin, (size_t)((i_range.i_end - i_range.i_aligned_begin) * sizeof(item_t))));
    }
}

void worker_final_synthesis(const uint32_t /*tid*/) noexcept
{
    while (true) {
        const uint32_t query_idx = g_synthesis_query_count++;
        if (query_idx >= g_query_count) return;

        const uint8_t mktid = g_all_queries[query_idx].first;
        const uint32_t bucket_index = g_all_queries[query_idx].second;
        query_t& query = g_queries_by_mktid[mktid][bucket_index];
        auto& final_result = query.final_result;

        for (uint32_t t = 0; t < g_worker_thread_count; ++t) {
            for (const query_result_t& result : query.results[t]) {
                if (final_result.size() < query.q_topn) {
                    final_result.emplace_back(result);
                    if (final_result.size() == query.q_topn) {
                        std::make_heap(final_result.begin(), final_result.end(), std::greater<query_result_t>());
                    }
                }
                else {
                    if (UNLIKELY(result > *final_result.begin())) {
                        std::pop_heap(final_result.begin(), final_result.end(), std::greater<query_result_t>());
                        *final_result.rbegin() = result;
                        std::push_heap(final_result.begin(), final_result.end(), std::greater<query_result_t>());
                    }
                }
            }
        }

        std::sort(final_result.begin(), final_result.end(), std::greater<query_result_t>());
        g_queries_done[query_idx].mark_done();
        // // TODO: translate the fake_orderkey to the real_orderkey, according to the mktsegment and recorded orderkey file

    }
}

void use_index_initialize() noexcept
{
    // Open index files
    {
        const auto &open_file = [](const char *const path, int *const fd, uint64_t *const file_size) noexcept {
            *fd = C_CALL(openat(g_index_directory_fd, path, O_RDONLY | O_CLOEXEC));
            struct stat64 st;
            C_CALL(fstat64(*fd, &st));
            *file_size = st.st_size;
            TRACE("openat %s: fd = %d, size = %lu", path, *fd, *file_size);
        };

        open_file("mktsegment", &g_mktsegment_fd, &g_mktsegment_file_size);
        open_file("prefixsum", &g_prefixsum_fd, &g_prefixsum_file_size);
        open_file("items", &g_items_fd, &g_items_file_size);
    }

    // Load index files: load mktsegment
    {
        const void* ptr = mmap_parallel(g_mktsegment_file_size, 1, PROT_READ, MAP_PRIVATE, g_mktsegment_fd);
        const char* p = (const char*)ptr;
        g_mktid_count = *(uint8_t*)(p++);
        for (uint8_t mktid = 0; mktid < g_mktid_count; ++mktid) {
            const uint8_t length = *(uint8_t*)(p++);
            g_mktsegment_to_mktid[std::string(p, length)] = mktid;
            DEBUG("loaded mktsegment: %.*s -> %u", length, p, mktid);
            p += length;
        }
    }

    // Load index files: load prefixsum
    {
        ASSERT(g_prefixsum_file_size == sizeof(uint32_t) * g_mktid_count * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1),
            "BUG: broken index?");

        g_index_prefixsum = (uint32_t*)mmap_parallel(
            g_prefixsum_file_size,
            1,
            PROT_READ,
            MAP_PRIVATE,
            g_prefixsum_fd);
        DEBUG("g_index_prefixsum: %p", g_index_prefixsum);
    }

    // Load queries
    {
        load_queries();
    }

    g_item_range_queue.init(g_max_item_ranges_count, g_worker_thread_count);
}

void fn_worker_thread_use_index(const uint32_t tid) noexcept
{
    DEBUG("[%u] worker started", tid);

    //g_workers_barrier_external.sync();  // sync.ext@0
    {
        // [[maybe_unused]] timer tmr;
        // DEBUG("[%u] worker_load_multi_part(): starts", tid);
        worker_load_multi_part(tid);
        // DEBUG("[%u] worker_load_multi_part(): %.3lf msec", tid, tmr.elapsed_msec());
    }
    DEBUG("worker%u syncing", tid);
    g_worker_sync_barrier.sync();  // sync
    
    {
        // [[maybe_unused]] timer tmr;
        // DEBUG("[%u] worker final synthesis: starts", tid);
        worker_final_synthesis(tid);
        // DEBUG("[%u] worker final synthesis: %.3lf msec", tid, tmr.elapsed_msec());
    }

    DEBUG("[%u] worker done", tid);
}

void main_thread_use_index() noexcept
{
    for (uint32_t query_idx = 0; query_idx < g_query_count; ++query_idx) {
        g_queries_done[query_idx].wait_done();

        // print query
        query_t& query = g_queries_by_mktid[g_all_queries[query_idx].first][g_all_queries[query_idx].second];
        printf("l_orderkey|o_orderdate|revenue\n");
        for (const query_result_t& line : query.final_result) {
            const auto ymd = date_get_ymd(line.orderdate);
            printf("%u|%u-%02u-%02u|%u.%02u\n",
                line.orderkey,
                std::get<0>(ymd), std::get<1>(ymd), std::get<2>(ymd),
                line.total_expend_cent / 100, line.total_expend_cent % 100);
        }
    }
    delete [] g_queries_done;
}