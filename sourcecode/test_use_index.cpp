#include "common.h"

struct query_result_t
{
    date_t orderdate;
    uint32_t orderkey;
    uint32_t total_expend_cent;

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

    std::vector<query_result_t> result;
};

#define SHIP_ORDER_DATE_OFFSET 128
struct date_range_t
{
    date_t d_begin;
    date_t d_end;     // date range [begin, end); assume begin < end
    std::vector<uint32_t> q_indexs;
    // TODO: copy constructor and operator=() for q_indexs; or replace it completely with pointer(requires extra q_num parameter)
};

struct item_t
{

};

struct item_range_t
{
    const item_t* i_begin;
    const item_t* i_end;
    const uint32_t* p_date_offset;
    date_t d_begin;
    date_t d_end;     // date range [begin, end); assume begin < end
    std::vector<uint32_t>& q_indexs;
    uint8_t q_mktid;
}

namespace
{
    int g_mktsegment_fd = -1;
    uint64_t g_mktsegment_file_size = 0;
    uint8_t g_mktid_count = 0;
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };
    std::set<uint8_t> g_mktids { };

    int g_prefixsum_fd = -1;
    uint64_t g_prefixsum_file_size = 0;

    int g_items_fd = -1;
    uint64_t g_items_file_size = 0;

    uint64_t* g_index_items = nullptr;
    uint32_t* g_index_prefixsum = nullptr;

    // std::vector<query_t> g_queries { };
    // done_event* g_queries_done = nullptr;
    // std::atomic_uint32_t g_queries_curr { 0 };
    std::vector<query_t> g_queries_by_mktid[256];
    std::pair<uint32_t /*mktid*/, uint32_t /*bucket_index*/> g_all_queries[256];
    std::vector<date_range_t> g_date_ranges_by_mktid[256];
    static std::atomic_uint32_t g_date_range_offsets_by_mktid[256];
    mpmc_queue_t<item_range_t> g_item_range_queue;

    // require to support in main.cpp or common.h
    threads_barrier_t g_loaders_barrier;
}

void load_queries()
{
    std::vector<date_t> dates_by_mktid[256];
    g_queries.resize(g_query_count);
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

        query.result.reserve(query.q_topn);

        // insert the involved date_range endpoints here
        const date_t d_begin = date_subtract_bounded_to_min_table_date(query.q_shipdate, SHIP_ORDER_DATE_OFFSET - 1);
        const date_t d_end = date_subtract_bounded_to_min_table_date(query.q_orderdate, 0);
        if (d_begin < d_end) {
            dates_by_mktid[mktid].append(d_begin);
            dates_by_mktid[mktid].append(d_end);
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
            // TRACE("    query#%u: q_mktsegment=%u, q_orderdate=%04u-%02u-%02u, q_shipdate=%04u-%02u-%02u, q_limit=%u",
            //     query.q_index, query.q_mktsegment,
            //     query.q_orderdate.year(), query.q_orderdate.month(), query.q_orderdate.day(),
            //     query.q_shipdate.year(), query.q_shipdate.month(), query.q_shipdate.day(),
            //     query.q_limit);

            // TODO: Reserve for results?
            //for (uint32_t t = 0; t < MAX_WORKER_THREADS; ++t) {
            //    query.results[t].reserve(query.q_limit);
            //}

            // Fill g_all_queries
            g_all_queries[query.q_index] = std::make_pair((uint32_t)mktid, (uint32_t)bucket_index);
        }

        std::sort(
            dates_by_mktid[mktid].begin(),
            dates_by_mktid[mktid].end(),
            std::greater<date_t>()
        );
        const size_t size = dates_by_mktid[mktid].size();
        g_date_ranges_by_mktid.resize(size + 1);
        for (size_t i = 1; i < size; ++i) {
            const date_t d_begin = dates_by_mktid[mktid][i - 1];
            const date_t d_end   = dates_by_mktid[mktid][i];
            g_date_ranges_by_mktid[mktid].d_begin = d_begin;
            g_date_ranges_by_mktid[mktid].d_end   = d_end;
            for (size_t j = 0; j < g_queries_by_mktid[mktid].size(); ++j) {
                if (date_subtract_bounded_to_min_table_date(g_queries_by_mktid[mktid][j].q_orderdate, 0) >= d_end && 
                    date_subtract_bounded_to_min_table_date(g_queries_by_mktid[mktid][j].q_shipdate, SHIP_ORDER_DATE_OFFSET - 1) <= d_begin) {
                    g_date_ranges_by_mktid[mktid].q_indexs.append(g_queries_by_mktid[mktid][j].q_index);
                }
            }
        }
    }

}

void fn_loader_thread(const uint32_t tid) noexcept
{
    DEBUG("[%u] loader started", tid);
    constexpr uint64_t step_size = 1048576 * 16 - 4096;

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
            //     item_size / step_size,
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
                const uint32_t l_offset = g_index_prefixsum[calc_bucket_index(mktid, l)];
                const uint32_t r_offset = g_index_prefixsum[calc_bucket_index(mktid, r)];
                const uint64_t size = r_offset - l_offset;
                if (size >= step_size) {
                    flag = true;
                }
                if (flag) {
                    i_range.i_begin = (const item_t*)my_mmap(
                        size * sizeof(item_t),
                        PROT_READ,
                        MAP_PRIVATE | MAP_POPULATE,
                        g_items_fd,
                        l_offset * sizeof(item_t)
                    );
                    if (UNLIKELY(i_range.i_begin == MAP_FAILED)) {
                        PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
                    }
                    i_range.i_end = i_range.i_begin + size;
                    // i_range.map_size = size;
                    i_range.p_date_offset = &g_date_offsets_by_mktid[mktid][l];
                    i_range.d_begin = l;
                    i_range.d_end = r;
                    i_range.q_indexs = d_range.q_indexs;
                    i_range.q_mktid = mktid;
                    g_item_range_queue.push(i_range);
                    // g_mapped_queues_by_mktid[mktid].push(i_range);
                    l = r;
                }
            }
        }
    }

    g_loaders_barrier.sync();
    if (tid == 0) {
        g_item_range_queue.mark_finish_push();
    }

    DEBUG("[%u] loader done", tid);
}

void fn_worker_thread(const uint32_t tid) noexcept
{

}

void main_thread_use_index() noexcept
{

}