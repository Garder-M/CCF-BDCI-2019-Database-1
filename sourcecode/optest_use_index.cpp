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
    uint8_t q_mktid;
    date_t q_orderdate;
    date_t q_shipdate;
    uint32_t q_topn;

    date_t d_scan_begin;
    date_t d_scan_end;

#if CONFIG_TOPN_DATES_PER_PLATE > 0
    date_t d_pretopn_begin;
    date_t d_pretopn_end;
    date_t d_shared_pretopn_begin;
    date_t d_shared_pretopn_end;
#endif

    bool is_unknown_mktsegment;
    std::vector<query_result_t> result;
};

struct partial_index_t
{
    int items_fd = -1;
    uint64_t items_file_size = 0;
    void* items_ptr = nullptr;

    int endoffset_fd = -1;
    uint64_t endoffset_file_size = 0;
    uint64_t* endoffset_ptr = nullptr;
};


#if CONFIG_TOPN_DATES_PER_PLATE > 0
struct date_range_t
{
    date_t d_begin;
    date_t d_end;
    uint16_t q_index_begin;
    uint16_t q_index_end;
};
#endif

namespace
{
    int g_mktsegment_fd = -1;
    uint64_t g_mktsegment_file_size = 0;
    uint8_t g_mktid_count = 0;
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };

    std::vector<query_t> g_queries { };
    done_event* g_queries_done = nullptr;
    std::atomic_uint32_t g_queries_curr { 0 };

    partial_index_t* g_partial_indices = nullptr;
    done_event g_partial_index_loaded { };

#if CONFIG_TOPN_DATES_PER_PLATE > 0
    constexpr const uint32_t PLATES_PER_MKTID = (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) / CONFIG_TOPN_DATES_PER_PLATE + 1;

    uint64_t* g_pretopn_ptr = nullptr;
    uint32_t* g_pretopn_count_ptr = nullptr;
    uint32_t* g_shared_pretopn_q_index_buffer = nullptr;

    done_event* g_shared_pretopn_queries_done = nullptr;
    done_event* g_pretopn_queries_done = nullptr;

    std::vector<uint32_t> g_pretopn_queries { };
    std::atomic_uint32_t g_pretopn_queries_curr { 0 };

    std::vector<date_range_t> g_shared_pretopn_d_ranges { };
    std::atomic_uint32_t g_shared_pretopn_d_ranges_curr { 0 };
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
    ASSERT((orderdate - MAX_TABLE_DATE) % CONFIG_TOPN_DATES_PER_PLATE == 0);

    return (uint32_t)(mktid - 0) * PLATES_PER_MKTID + (uint32_t)(orderdate - MIN_TABLE_DATE) / CONFIG_TOPN_DATES_PER_PLATE;
}
#endif

void use_index_initialize() noexcept
{
    // Open index files
    {
        {
            int count_fd = -1;
            uint64_t count_file_size;
            openat_file_read("meta", &count_fd, &count_file_size);
            const size_t cnt = C_CALL(read(count_fd, &g_meta, sizeof(g_meta)));
            CHECK(cnt == sizeof(g_meta));
            C_CALL(close(count_fd));

            INFO("g_meta.partial_index_count: %u", g_meta.partial_index_count);
            INFO("g_meta.max_shipdate_orderdate_diff: %u", g_meta.max_shipdate_orderdate_diff);
            g_partial_indices = new partial_index_t[g_meta.partial_index_count];
        }

        openat_file_read("mktsegment", &g_mktsegment_fd, &g_mktsegment_file_size);
    }

    // Load index files: load mktsegment
    {
        char buffer[g_mktsegment_file_size];
        const size_t cnt = C_CALL(read(g_mktsegment_fd, buffer, g_mktsegment_file_size));
        CHECK(cnt == g_mktsegment_file_size);

        const char* p = (const char*)buffer;
        g_mktid_count = *(uint8_t*)(p++);
        for (uint8_t mktid = 0; mktid < g_mktid_count; ++mktid) {
            const uint8_t length = *(uint8_t*)(p++);
            INFO("length: %u", length);
            g_mktsegment_to_mktid[std::string(p, length)] = mktid;
            INFO("loaded mktsegment: %.*s -> %u", length, p, mktid);
            p += length;
        }
    }

#if CONFIG_TOPN_DATES_PER_PLATE > 0
    // Load index files: load pretopn
    {
        int pretopn_fd = -1, pretopn_count_fd = -1;
        uint64_t pretopn_size = 0, pretopn_count_size = 0;
        openat_file_read("pretopn", &pretopn_fd, &pretopn_size);
        openat_file_read("pretopn_count", &pretopn_count_fd, &pretopn_count_size);

        ASSERT(pretopn_size == sizeof(uint64_t) * CONFIG_EXPECT_MAX_TOPN * g_mktid_count * PLATES_PER_MKTID);
        g_pretopn_ptr = (uint64_t*)my_mmap(
            pretopn_size,
            PROT_READ,
            MAP_PRIVATE, // MAP_POPULATE ?
            pretopn_fd,
            0);
        INFO("g_pretopn_ptr: %p", g_pretopn_ptr);

        ASSERT(pretopn_count_size == sizeof(uint32_t) * g_mktid_count * PLATES_PER_MKTID);
        g_pretopn_count_ptr = (uint32_t*)my_mmap(
            pretopn_count_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE,
            pretopn_count_fd,
            0);
        INFO("g_pretopn_count_ptr: %p", g_pretopn_count_ptr);
    }
    // Load queries
    // prepare the (shared) pretopn plan & scan plan
    {
        std::vector<std::vector<uint32_t> > pretopn_queries_by_mktid;
        std::vector<std::vector<date_t> pretopn_dates_by_mktid;
        std::vector<std::vector<date_range_t> > pretopn_d_ranges_by_mktid;

        pretopn_queries_by_mktid.resize(g_mktid_count);
        pretopn_dates_by_mktid.resize(g_mktid_count);
        pretopn_d_ranges_by_mktid.resize(g_mktid_count);

        std::vector<std::vector<uint32_t> > pretopn_ranges_of_query;
        pretopn_ranges_of_query.resize(g_query_count);

        bool *pretopn_shared_flag = nullptr;
        pretopn_shared_flag = new bool[g_query_count];
        memset(pretopn_shared_flag, 0, g_query_count * sizeof(bool));

        g_queries_done = new done_event[g_query_count];
        g_pretopn_queries_done = new done_event[g_query_count];
        g_shared_pretopn_queries_done = new done_event[g_query_count];

        uint32_t max_date_ranges_count = 0;
        g_queries.resize(g_query_count);
        for (uint32_t q = 0; q < g_query_count; ++q) {
            query_t& query = g_queries[q];

            const auto it = g_mktsegment_to_mktid.find(g_argv_queries[4 * q + 0]);
            if (UNLIKELY(it == g_mktsegment_to_mktid.end())) {
                query.is_unknown_mktsegment = true;
                DEBUG("query #%u: unknown mktsegment: %s", q, g_argv_queries[4 * q + 0]);
                continue;
            }

            query.q_mktid = it->second;
            query.q_orderdate = date_from_string(g_argv_queries[4 * q + 1]);
            query.q_shipdate = date_from_string(g_argv_queries[4 * q + 2]);
            query.q_topn = (uint32_t)std::strtoul(g_argv_queries[4 * q + 3], nullptr, 10);

            DEBUG("query #%u: q_mktid=%u,q_orderdate=%u,q_shipdate=%u,q_topn=%u",
                  q, query.q_mktid, query.q_orderdate, query.q_shipdate, query.q_topn);

            query.result.reserve(query.q_topn);

            query.d_scan_begin = std::min<date_t>(std::max<date_t>(
                query.q_shipdate - (g_meta.max_shipdate_orderdate_diff - 1),
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
                    d_pretopn_begin == d_pretopn_end || query.q_topn > CONFIG_EXPECT_MAX_TOPN) {
                    d_pretopn_begin = q_orderdate;
                    d_pretopn_end = q_orderdate;
                    pretopn_shared_flag[q] = true; // mark as already shared to avoid checking
                    // mark skip pretopn
                    g_pretopn_queries_done[q].mark_done();
                }
                else {
                    pretopn_queries_by_mktid[mktid].push_back(q);
                    pretopn_dates_by_mktid[mktid].push_back(d_pretopn_begin);
                    pretopn_dates_by_mktid[mktid].push_back(d_pretopn_end);
                    g_pretopn_queries.push_back(q);
                    max_date_ranges_count += 2;
                }
                query.d_pretopn_begin = d_pretopn_begin;
                query.d_pretopn_end = d_pretopn_end;
            }
            else {
                pretopn_shared_flag[q] = true; // mark as already shared to avoid checking
                // mark skip pretopn
                g_pretopn_queries_done[q].mark_done();
            }
        }

        uint32_t* q_index_buffer = nullptr;
        q_index_buffer = new uint32_t[g_query_count * max_date_ranges_count];
        g_shared_pretopn_q_index_buffer = new uint32_t[g_query_count * max_date_ranges_count];
        uint32_t buffer_l = 0, buffer_r = 0;
        uint32_t shared_buffer_l = 0, shared_buffer_r = 0;
        for (uint8_t mktid = 0; mktid < g_mktid_count; ++mktid) {
            if (pretopn_queries_by_mktid[mktid].size() <= 0) continue;
            std::sort(
                pretopn_dates_by_mktid[mktid].begin(),
                pretopn_dates_by_mktid[mktid].end(),
                std::less<date_t>()
            );
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
        // TODO: create threads to implement the topN plan

        if (q_index_buffer != nullptr) delete [] q_index_buffer;
        if (pretopn_shared_flag != nullptr) delete [] pretopn_shared_flag;
    }

#else
    // Load queries
    {
        g_queries_done = new done_event[g_query_count];

        g_queries.resize(g_query_count);
        for (uint32_t q = 0; q < g_query_count; ++q) {
            query_t& query = g_queries[q];

            const auto it = g_mktsegment_to_mktid.find(g_argv_queries[4 * q + 0]);
            if (UNLIKELY(it == g_mktsegment_to_mktid.end())) {
                query.is_unknown_mktsegment = true;
                DEBUG("query #%u: unknown mktsegment: %s", q, g_argv_queries[4 * q + 0]);
                continue;
            }

            query.q_mktid = it->second;
            query.q_orderdate = date_from_string(g_argv_queries[4 * q + 1]);
            query.q_shipdate = date_from_string(g_argv_queries[4 * q + 2]);
            query.q_topn = (uint32_t)std::strtoul(g_argv_queries[4 * q + 3], nullptr, 10);

            DEBUG("query #%u: q_mktid=%u,q_orderdate=%u,q_shipdate=%u,q_topn=%u",
                  q, query.q_mktid, query.q_orderdate, query.q_shipdate, query.q_topn);

            query.result.reserve(query.q_topn);
            query.d_scan_begin = std::min<date_t>(std::max<date_t>(
                query.q_shipdate - (g_meta.max_shipdate_orderdate_diff - 1),
                MIN_TABLE_DATE), MAX_TABLE_DATE);
            query.d_scan_end = std::max<date_t>(std::min<date_t>(query.q_orderdate, 
                MAX_TABLE_DATE + 1), MIN_TABLE_DATE);
        }
    }
#endif
}

#if CONFIG_TOPN_DATES_PER_PLATE > 0
void fn_pretopn_thread_use_index([[maybe_unused]] const uint32_t tid) noexcept
{
    const auto scan_plate = [&](const uint32_t plate_id, const date_t from_orderdate, query_t& query) {
        ASSERT(plate_id < g_mktid_count * PLATES_PER_MKTID);
        const uint64_t* const plate_ptr = &g_pretopn_ptr[plate_id * CONFIG_EXPECT_MAX_TOPN];
        const uint32_t count = g_pretopn_count_ptr[plate_id];
        //DEBUG("plate_id: %u, count: %u", plate_id, count);

        for (uint32_t i = 0; i < count; ++i) {
            const uint64_t value = plate_ptr[i];

            query_result_t tmp;
            tmp.total_expend_cent = value >> 36;
            tmp.orderkey = (value >> 6) & ((1U << 30) - 1);
            tmp.orderdate = from_orderdate + (value & 0b111111U);

            if (query.result.size() < query.q_topn) {
                query.result.emplace_back(std::move(tmp));
                if (query.result.size() == query.q_topn) {
                    std::make_heap(query.result.begin(), query.result.end(), std::greater<>());
                }
            }
            else {
                if (tmp > *query.result.begin()) {
                    std::pop_heap(query.result.begin(), query.result.end(), std::greater<>());
                    *query.result.rbegin() = tmp;
                    std::push_heap(query.result.begin(), query.result.end(), std::greater<>());
                }
                else {
                    // plate is ordered (descending)
                    break;
                }
            }
        }
    };

    while (true) {
        const uint32_t task_id = g_shared_pretopn_d_ranges_curr++;
        if (task_id >= g_shared_pretopn_d_ranges.size()) break;
        const date_range_t& d_range = g_shared_pretopn_d_ranges[task_id];
        uint32_t max_q_topn = 0;
        uint32_t max_q_topn_qid = 0;
        ASSERT(d_range.d_begin < d_range.d_end);
        ASSERT(d_range.q_index_begin < d_range.q_index_end);
        for (uint32_t l = d_range.q_index_begin; l < d_range.q_index_end; ++l) {
            const uint32_t qid = g_shared_pretopn_q_index_buffer[l];
            const query_t& query = g_queries[qid];
            ASSERT(d_range.d_begin == query.d_shared_pretopn_begin);
            ASSERT(d_range.d_end == query.d_shared_pretopn_end);
            max_q_topn = (query.q_topn > max_q_topn) ? query.q_topn : max_q_topn;
            max_q_topn_qid = (query.q_topn > max_q_topn) ? qid 
                                                         : max_q_topn_qid;
        }
        query_t& ref_query = g_queries[max_q_topn_qid];
        const uint8_t mktid = ref_query.q_mktid;
        for (date_t orderdate = d_range.d_begin; orderdate < d_range.d_end; orderdate += CONFIG_TOPN_DATES_PER_PLATE) {
            const uint32_t plate_id = calc_topn_plate_index(mktid, orderdate);
            scan_plate(plate_id, orderdate, ref_query);
        }
        std::vector<query_result_t>& ref_result = ref_query.result;
        uint32_t left_query_count = d_range.q_index_end - d_range.q_index_begin - 1;
        for (uint32_t l = d_range.q_index_begin; l < d_range.q_index_end && left_query_count > 0; ++l) {
            const uint32_t qid = g_shared_pretopn_q_index_buffer[l];
            if (qid == max_q_topn_qid) continue;
            query_t& query = g_queries[qid];
            if (query.q_topn >= ref_result.size()) {
                query.result.insert(query.result.end(), ref_result.begin(), ref_result.end());
                g_shared_pretopn_queries_done[qid].mark_done();
                --left_query_count;
            }
        }
        const bool heap_reordered = (left_query_count > 0) ? true : false;
        for (uint32_t l = d_range.q_index_begin; l < d_range.q_index_end && left_query_count > 0; ++l) {
            const uint32_t qid = g_shared_pretopn_q_index_buffer[l];
            if (qid == max_q_topn_qid) continue;
            query_t& query = g_queries[qid];
            if (query.q_topn < ref_result.size()) {
                std::nth_element(ref_result.begin(), ref_result.begin() + query.q_topn, 
                    ref_result.end(), std::greater<>());
                query.result.insert(query.result.end(), ref_result.begin(), ref_result.begin() + query.q_topn);
                std::make_heap(query.result.begin(), query.result.end(), std::greater<>());
                g_shared_pretopn_queries_done[qid].mark_done();
                --left_query_count;
            }
        }
        ASSERT(left_query_count == 0);
        if (heap_reordered) {
            std::make_heap(ref_result.begin(), ref_result.end(), std::greater<>());
        }
        g_shared_pretopn_queries_done[max_q_topn_qid].mark_done();
    }

    while (true) {
        const uint32_t task_id = g_pretopn_queries_curr++;
        if (task_id >= g_pretopn_queries.size()) break;
        const uint32_t qid = g_pretopn_queries[task_id];
        g_shared_pretopn_queries_done[qid].wait_done();
        query_t& query = g_queries[qid];
        ASSERT(query.d_pretopn_begin <= query.d_shared_pretopn_begin);
        ASSERT(query.d_shared_pretopn_begin < query.d_shared_pretopn_end);
        ASSERT(query.d_shared_pretopn_end <= query.d_pretopn_end);
        for (date_t orderdate = query.d_pretopn_begin; orderdate < query.d_shared_pretopn_begin; orderdate += CONFIG_TOPN_DATES_PER_PLATE) {
            const uint32_t plate_id = calc_topn_plate_index(mktid, orderdate);
            scan_plate(plate_id, orderdate, query);
        }
        for (date_t orderdate = query.d_shared_pretopn_end; orderdate < query.d_pretopn_end; orderdate += CONFIG_TOPN_DATES_PER_PLATE) {
            const uint32_t plate_id = calc_topn_plate_index(mktid, orderdate);
            scan_plate(plate_id, orderdate, query);
        }
        g_pretopn_queries_done[qid].mark_done();
    }
}
#endif

