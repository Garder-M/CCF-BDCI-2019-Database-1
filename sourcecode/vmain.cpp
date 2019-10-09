#include "common.h"

struct date_range_t
{
    date_t d_begin;
    date_t d_end;     // date range [begin, end); assume begin < end
    std::vector<uint32_t> q_indexs;
    // TODO: copy constructor and operator=() for q_indexs; or replace it completely with pointer(requires extra q_num parameter)
};

#define SHIP_ORDER_DATE_OFFSET 128
FORCEINLINE date_t date_shift(const date_t& date, const int32_t shift) {
    return date + shift;
}

struct mapped_file_range_t
{
    const char* start;
    const char* end;
    // uint64_t map_size;
    const uint64_t* p_date_offset;
    date_range_t;
}


//==============================================================
// Global variables
//==============================================================
const std::chrono::steady_clock::time_point g_startup_time = std::chrono::steady_clock::now();
volatile uint64_t g_dummy_prevent_optimize; 

uint8_t g_max_mktsegment = 0;
std::map<const char*, uint8_t, c_string_less> g_string_to_mktsegment;
std::vector<std::string> g_mktsegment_to_string;

uint32_t g_query_count;
std::atomic_uint32_t g_synthesis_query_count { 0 };
std::pair<uint32_t /*mktsegment*/, uint32_t /*bucket_index*/> g_all_queries[256];
std::vector<query_t> g_queries_by_mktsegment[256];
std::vector<date_range_t> g_date_ranges_by_mktsegment[256];

#define MAX_DATE 10000
uint64_t* g_date_offsets_by_mktsegment[256];

std::vector<std::thread> g_workers;
uint32_t g_workers_thread_count;
threads_barrier_t g_workers_barrier;
threads_barrier_t g_workers_barrier_external;

std::vector<std::thread> g_loaders;
uint32_t g_loaders_thread_count;
threads_barrier_t g_loaders_barrier;

//iovec g_mapped_customer, g_mapped_orders, g_mapped_lineitem;
// uint64_t g_customer_filesize, g_orders_filesize, g_lineitem_filesize;
// const char *g_customer_filepath, *g_orders_filepath, *g_lineitem_filepath;
// int g_customer_fd, g_orders_fd, g_lineitem_fd;
// mpmc_queue_t<mapped_file_part_t> g_mapped_customer_queue, g_mapped_orders_queue, g_mapped_lineitem_queue, g_mapping_parts_queue;
int g_compressed_fd[256];
uint64_t g_compressed_filesize[256];
const char *g_compressed_filepath[256];
mpmc_queue_t<mapped_file_range_t> g_mapped_queues_by_mktsegment[256];

// static std::atomic_bool __orders_allocated { false };
static std::atomic_uint32_t g_date_range_offsets_by_mktsegment[256];


uint64_t g_upbound_custkey;
uint8_t* g_custkey_to_mktsegment;

uint64_t g_upbound_orderkey;
date_t* g_orderkey_to_order;

void load_queries(
    [[maybe_unused]] const int argc,
    char* const argv[])
{
    ASSERT(argc >= 5, "Unexpected argc too small: %d", argc);

    g_query_count = (uint32_t)strtol(argv[4], nullptr, 10);
    DEBUG("g_query_count: %u", g_query_count);

    std::vector<date_t> dates_by_mktsegment[256];

    for (uint32_t query_idx = 0; query_idx < g_query_count; ++query_idx) {
        uint8_t mktsegment;
        const char* const str_mktsegment = argv[5 + query_idx * 4 + 0];
        const auto it = g_string_to_mktsegment.find(str_mktsegment);
        if (it != g_string_to_mktsegment.end()) {
            mktsegment = it->second;
        }
        else {
            g_string_to_mktsegment[str_mktsegment] = ++g_max_mktsegment;
            mktsegment = g_max_mktsegment;
            g_mktsegment_to_string.resize(mktsegment + 1);
            g_mktsegment_to_string[mktsegment] = str_mktsegment;
            DEBUG("// found new mktsegment: %s -> %u", str_mktsegment, g_max_mktsegment);
        }
        
        const size_t bucket_index = g_queries_by_mktsegment[mktsegment].size();
        g_queries_by_mktsegment[mktsegment].resize(bucket_index + 1);  // usually O(1)

        query_t& query = g_queries_by_mktsegment[mktsegment][bucket_index];
        query.query_index = query_idx;

        query.q_mktsegment = mktsegment;
        query.q_orderdate.parse(argv[5 + query_idx * 4 + 1], 0x00);
        query.q_shipdate.parse(argv[5 + query_idx * 4 + 2], date_t::MAX_MKTSEGMENT);
        query.q_limit = (uint32_t)strtol(argv[5 + query_idx * 4 + 3], nullptr, 10);

        const date_t d_begin = date_shift(query.q_shipdate, SHIP_ORDER_DATE_OFFSET + 1);
        const date_t d_end = query.q_orderdate;
        if (d_begin < d_end) {
            dates_by_mktsegment[mktsegment].append(d_begin);
            dates_by_mktsegment[mktsegment].append(d_end);
        }
        // TODO: else is invalid query, left the query_result empty and skip the query
        // another invalid condition: q_shipdate < q_orderdate
        
        TRACE("query#%u: q_mktsegment=%u, q_orderdate=%04u-%02u-%02u, q_shipdate=%04u-%02u-%02u, q_limit=%u",
            query_idx, query.q_mktsegment,
            query.q_orderdate.year(), query.q_orderdate.month(), query.q_orderdate.day(),
            query.q_shipdate.year(), query.q_shipdate.month(), query.q_shipdate.day(),
            query.q_limit);
    }

    // Sort queries in each bucket by their q_orderdate (descending)
    for (uint32_t mktsegment = 1; mktsegment <= g_max_mktsegment; ++mktsegment) {
        std::sort(
            g_queries_by_mktsegment[mktsegment].begin(),
            g_queries_by_mktsegment[mktsegment].end(),
            [](const query_t& a, const query_t& b) noexcept {
                if (a.q_orderdate > b.q_orderdate) return true;
                if (a.q_orderdate < b.q_orderdate) return false;
                return (a.query_index < b.query_index);
            });

        TRACE("------------------------------------------------");
        TRACE("total queries on mktsegment#%u: %" PRIu64, mktsegment, (uint64_t)g_queries_by_mktsegment[mktsegment].size());
        for (size_t bucket_index = 0; bucket_index < g_queries_by_mktsegment[mktsegment].size(); ++bucket_index) {
            query_t& query = g_queries_by_mktsegment[mktsegment][bucket_index];
            TRACE("    query#%u: q_mktsegment=%u, q_orderdate=%04u-%02u-%02u, q_shipdate=%04u-%02u-%02u, q_limit=%u",
                query.query_index, query.q_mktsegment,
                query.q_orderdate.year(), query.q_orderdate.month(), query.q_orderdate.day(),
                query.q_shipdate.year(), query.q_shipdate.month(), query.q_shipdate.day(),
                query.q_limit);

            // TODO: Reserve for results?
            //for (uint32_t t = 0; t < MAX_WORKER_THREADS; ++t) {
            //    query.results[t].reserve(query.q_limit);
            //}

            // Fill g_all_queries
            g_all_queries[query.query_index] = std::make_pair((uint32_t)mktsegment, (uint32_t)bucket_index);
        }

        std::sort(
            dates_by_mktsegment[mktsegment].begin(),
            dates_by_mktsegment[mktsegment].end(),
            std::greater<date_t>()
        );
        const size_t size = dates_by_mktsegment[mktsegment].size();
        g_date_ranges_by_mktsegment.resize(size + 1);
        for (size_t i = 1; i < size; ++i) {
            const date_t d_begin = dates_by_mktsegment[mktsegment][i - 1];
            const date_t d_end   = dates_by_mktsegment[mktsegment][i];
            g_date_ranges_by_mktsegment[mktsegment].d_begin = d_begin;
            g_date_ranges_by_mktsegment[mktsegment].d_end = d_end;
            for (size_t j = 0; j < g_queries_by_mktsegment[mktsegment].size(); ++j) {
                if (g_queries_by_mktsegment[mktsegment][j].q_orderdate >= d_end && date_shift(g_queries_by_mktsegment[mktsegment][j].q_shipdate, SHIP_ORDER_DATE_OFFSET - 1) <= d_begin) {
                    g_date_ranges_by_mktsegment[mktsegment].q_indexs.append(g_queries_by_mktsegment[mktsegment][j].query_index);
                }
            }
        }
    }
}

void fn_loader_thread(const uint32_t tid)
{
    DEBUG("[%u] loader started", tid);

    // constexpr uint64_t customer_step_size = 1048576 * 4 - 4096;
    // constexpr uint64_t orders_step_size = 1048576 * 16 - 4096;
    constexpr uint64_t compressed_step_size = 1048576 * 16 - 4096;

    for (uint32_t mktsegment = tid + 1; mktsegment <= g_max_mktsegment; mktsegment += g_loaders_thread_count) {
        // Open files
        // open_file(g_customer_filepath, &g_customer_filesize, &g_customer_fd);
        // open_file(g_orders_filepath, &g_orders_filesize, &g_orders_fd);
        open_file(g_compressed_filepath[mktsegment], &g_compressed_filesize[mktsegment], &g_compressed_fd[mktsegment]);

        // // Table customer queue init
        // g_mapped_customer_queue.init(g_customer_filesize / customer_step_size + 1);

        // // Table orders queue init
        // g_mapped_orders_queue.init(g_orders_filesize / orders_step_size + 1);

        // // Table lineitem queue init
        // g_mapped_lineitem_queue.init(g_lineitem_filesize / lineitem_step_size + 1);
        g_mapped_queues_by_mktsegment[mktsegment].init(g_lineitem_filesize / lineitem_step_size + 1);
        
        g_date_offsets_by_mktsegment[mktsegment] = (uint64_t*)mmap(
            nullptr,
            MAX_DATE * sizeof(uint64_t),
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE,
            g_compressed_fd,
            0
        );
        ASSERT(g_date_offsets_by_mktsegment[mktsegment] != MAP_FAILED, "g_date_offsets_by_mktsegment[%d] mmap() failed", mktsegment);
        DEBUG("g_date_offsets_by_mktsegment[%d]: %p", mktsegment, g_date_offsets_by_mktsegment[mktsegment]);

        C_CALL(madvise(
            g_date_offsets_by_mktsegment[mktsegment],
            MAX_DATE * sizeof(uint64_t),
            MADV_HUGEPAGE | MADV_SEQUENTIAL | MADV_WILLNEED)
        );
    }
    g_loaders_barrier.sync();

    for (uint32_t mktsegment = 1; mktsegment <= g_max_mktsegment; ++mktsegment) {
        while (true) {
            const uint32_t off = g_date_range_offsets_by_mktsegment[mktsegment].fetch_add(1);
            if (off >= g_date_ranges_by_mktsegment[mktsegment].size()) {
                break;
            }
            mapped_file_range_t mapped;
            const date_range_t d_range = g_date_ranges_by_mktsegment[mktsegment][off];
            for (date_t l = d_range.d_begin, r = d_range.d_begin; r < d_range.d_end; r = date_shift(r, 1)) {
                bool flag = false;
                if (UNLIKELY(date_shift(r, 1) >= d_range.d_end)) {
                    r = d_range.d_end;
                    flag = true;
                }
                const uint64_t size = g_date_offsets_by_mktsegment[mktsegment][r] - g_date_offsets_by_mktsegment[mktsegment][l];
                if (size >= compressed_step_size) {
                    flag = true;
                }
                if (flag) {
                    mapped.start = (const char*)mmap(
                        nullptr,
                        size,
                        PROT_READ,
                        MAP_PRIVATE | MAP_POPULATE,
                        g_compressed_fd[mktsegment],
                        g_date_offsets_by_mktsegment[mktsegment][l]
                    );
                    if (UNLIKELY(mapped.start == MAP_FAILED)) {
                        PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
                    }
                    mapped.end = mapped.start + size;
                    // mapped.map_size = size;
                    mapped.p_date_offset = &g_date_offsets_by_mktsegment[mktsegment][l];
                    mapped.d_begin = l;
                    mapped.d_end = r;
                    mapped.q_indexs = d_range.q_indexs;
                    g_mapped_queues_by_mktsegment[mktsegment].push(mapped);
                    l = r;
                }
            }
        }
    }

    g_loaders_barrier.sync();
    for (uint32_t mktsegment = tid + 1; mktsegment <= g_max_mktsegment; mktsegment += g_loaders_thread_count) {
        g_mapped_queues_by_mktsegment[mktsegment].mark_finish_push();
    }

    DEBUG("[%u] loader done", tid);
}

void worker_load_multi_part(const uint32_t tid) {
    mapped_file_range_t mapped;
    for (int mktsegment = 1; mktsegment <= g_max_mktsegment; ++mktsegment) {
        while (g_mapped_queues_by_mktsegment[mktsegment].pop(mapped)) {
            ASSERT(mapped.start != nullptr, "[%u] BUG: pop: mapped.start == nullptr", tid);
            ASSERT(mapped.desired_end != nullptr, "[%u] BUG: pop: mapped.desired_end == nullptr", tid);

            struct lineitem_t {
            public:
                lineitem_t() noexcept = default;
                lineitem_t(uint32_t value) noexcept { _value = value; }
                FORCEINLINE lineitem_t& operator=(uint32_t value) noexcept { _value = value; }
                FORCEINLINE order_tag() const noexcept { return (uint32_t)(_value & 0x80000000) >> 31; }
                FORCEINLINE shipdate_offset() const noexcept { return (uint32_t)(_value & 0x7f000000) >> 24; }
                FORCEINLINE cent() const noexcept { return _value & 0xffffff; }
            private:
                uint32_t _value;
            };
            lineitem_t curr_items[32];
            const uint32_t* p = (uint32_t*)mapped.start;
            const uint64_t* p_date_offset = mapped.p_date_offset + 1;
            // adjust the head curr_orderdate to the precise orderdate, skipping bubble orderdate
            date_t curr_orderdate = mapped.d_begin;
            while (*p_date_offset - *(mapped.p_date_offset) <= (uint64_t)(p - mapped.start)) {
                ++p_date_offset;
                date_shift(curr_orderdate, 1);
            }
            uint32_t curr_item_count = 0;
            int32_t curr_fake_orderkey = 0;
            uint8_t curr_order_tag = lineitem_t(*p).order_tag();
            const auto query_by_same_orderkey = [&]() {
                ASSERT(curr_item_count > 0, "BUG... curr_item_count is 0");
                for (uint32_t q_index : mapped.q_indexs) {
                    const query_t& query = g_queries_by_mktsegment[mktsegment][g_all_queries[q_index].second];
                    ASSERT(curr_orderdate < query.q_orderdate, "BUG... curr_orderdate >= query.q_orderdate, tid = %d", tid);
                    uint32_t total_expend_cent = 0;
                    bool matched = false;
                    for (uint32_t i = 0; i < curr_item_count; ++i) {
                        if (date_shift(curr_orderdate, curr_items[i].shipdate_offset()) > query.q_shipdate) {
                            total_expend_cent += curr_items[i].cent();
                            matched = true;
                        }
                    }

                    if (matched) {
                        std::vector<query_result_t>& results = query.results[tid];
                        if (results.size() < query.q_limit) {
                            results.emplace_back(total_expend_cent, curr_fake_orderkey, curr_orderdate);
                            if (results.size() == query.q_limit) {
                                std::make_heap(results.begin(), results.end(), std::greater<query_result_t>());
                            }
                        }
                        else {
                            if (UNLIKELY(total_expend_cent > results.begin()->total_expend_cent)) {
                                std::pop_heap(results.begin(), results.end(), std::greater<query_result_t>());
                                *results.rbegin() = query_result_t(total_expend_cent, curr_fake_orderkey, curr_orderdate);
                                std::push_heap(results.begin(), results.end(), std::greater<query_result_t>());
                            }
                        }
                    }
                }
                // if (LIKELY(curr_fake_orderkey != -1)) {
                // }
            }

            for (; p < mapped.end; ++p) {
                if (*p.order_tag() != curr_order_tag) {
                    query_by_same_orderkey();
                    // update the curr_orderdate for the new orderkey
                    while (UNLIKELY(*p_date_offset - *(mapped.p_date_offset) <= (uint64_t)(p - mapped.start))) {
                        ++p_date_offset;
                        date_shift(curr_orderdate, 1);
                    }
                    ++curr_fake_orderkey;
                    curr_item_count = 0;
                }
                curr_items[curr_item_count] = *p;
                ++curr_item_count;
            }
            query_by_same_orderkey();

            C_CALL(munmap((void*)mapped.start, (size_t)(mapped.end - mapped.start)));

        }
    }
}

void worker_final_synthesis(const uint32_t /*tid*/)
{
    while (true) {
        const uint32_t query_idx = g_synthesis_query_count++;
        if (query_idx >= g_query_count) return;

        const uint32_t mktsegment = g_all_queries[query_idx].first;
        const uint32_t bucket_index = g_all_queries[query_idx].second;
        query_t& query = g_queries_by_mktsegment[mktsegment][bucket_index];
        auto& final_result = query.final_result;

        for (uint32_t t = 0; t < g_workers_thread_count; ++t) {
            for (const query_result_t& result : query.results[t]) {
                if (final_result.size() < query.q_limit) {
                    final_result.emplace_back(result);
                    if (final_result.size() == query.q_limit) {
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

        // TODO: translate the fake_orderkey to the real_orderkey, according to the mktsegment and recorded orderkey file

    }
}

void fn_worker_thread(const uint32_t tid) {
    DEBUG("[%u] worker started", tid);

    //g_workers_barrier_external.sync();  // sync.ext@0
    {
        [[maybe_unused]] timer tmr;
        DEBUG("[%u] worker_load_multi_part(): starts", tid);
        worker_load_multi_part(tid);
        DEBUG("[%u] worker_load_multi_part(): %.3lf msec", tid, tmr.elapsed_msec());
    }
    g_workers_barrier.sync();  // sync
    
    {
        timer tmr;
        DEBUG("[%u] worker final synthesis: starts", tid);
        worker_final_synthesis(tid);
        DEBUG("[%u] worker final synthesis: %.3lf msec", tid, tmr.elapsed_msec());
    }

    DEBUG("[%u] worker done", tid);
}

int main(int argc, char* argv[])
{
    //
    // Initializations
    //
    g_customer_filepath = argv[1];
    g_orders_filepath = argv[2];
    g_lineitem_filepath = argv[3];

    g_workers_thread_count = std::thread::hardware_concurrency();
    if (g_workers_thread_count > MAX_WORKER_THREADS) g_workers_thread_count = MAX_WORKER_THREADS;
    INFO("g_workers_thread_count: %u", g_workers_thread_count);

    g_workers_barrier.init(g_workers_thread_count);
    g_workers_barrier_external.init(g_workers_thread_count + 1);

    g_loaders_thread_count = std::thread::hardware_concurrency();
    if (g_loaders_thread_count > MAX_LOADER_THREADS) g_loaders_thread_count = MAX_LOADER_THREADS;
    INFO("g_loaders_thread_count: %u", g_loaders_thread_count);

    g_loaders_barrier.init(g_loaders_thread_count);


    //
    // Load queries
    //
    {
        [[maybe_unused]] timer tmr;
        load_queries(argc, argv);
        DEBUG("all query loaded (%.3lf msec)", tmr.elapsed_msec());
    }


    //
    // Create loader threads
    //
    for (uint32_t tid = 0; tid < g_loaders_thread_count; ++tid) {
        g_loaders.emplace_back(fn_loader_thread, tid);
        //g_loaders.rbegin()->detach();
    }

    //
    // Create worker threads
    //
    for (uint32_t tid = 0; tid < g_workers_thread_count; ++tid) {
        g_workers.emplace_back(fn_worker_thread, tid);
    }


    //
    // Wait for all loader and worker threads
    //
    for (std::thread& thr : g_loaders) thr.join();
    INFO("all %u loaders done", g_workers_thread_count);

    for (std::thread& thr : g_workers) thr.join();
    INFO("all %u workers done", g_workers_thread_count);


    //
    // Print results
    //
    for (uint32_t query_idx = 0; query_idx < g_query_count; ++query_idx) {
        const uint32_t mktsegment = g_all_queries[query_idx].first;
        const uint32_t bucket_index = g_all_queries[query_idx].second;
        query_t& query = g_queries_by_mktsegment[mktsegment][bucket_index];

        TRACE("printing query#%u: mktsegment=%u", query.query_index, query.q_mktsegment);
        fprintf(stdout, "l_orderkey|o_orderdate|revenue\n");
        for (const query_result_t& result : query.final_result) {
            fprintf(stdout, "%u|%04u-%02u-%02u|%u.%02u\n",
                result.orderkey,
                result.orderdate.year(), result.orderdate.month(), result.orderdate.day(),
                result.total_expend_cent / 100, result.total_expend_cent % 100);
        }
    }
    fflush(stdout);

    DEBUG("now exit!");
    return 0;
}