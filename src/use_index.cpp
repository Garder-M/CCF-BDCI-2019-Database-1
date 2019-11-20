#include "common.h"


//==============================================================================
// Structures
//==============================================================================
struct query_context_t
{
    done_event done { };

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
    uint32_t check_orderdate_check_shipdate_begin_bucket_id { };
    uint32_t only_check_shipdate_begin_bucket_id { };
    uint32_t nocheck_head_begin_bucket_id { };
    uint32_t pretopn_begin_bucket_id { };
    uint32_t nocheck_tail_begin_bucket_id { };
    uint32_t only_check_orderdate_begin_bucket_id { };
    uint32_t only_check_orderdate_end_bucket_id { };

    char* output = nullptr;
    uint64_t output_length = 0;

    uint32_t results_length = 0;
    query_result_t results[0];  // actually: [limit_topn]

    // char output[40 * (q_topn + 1)];
};
static_assert(sizeof(query_context_t) == 96);


//==============================================================================
// Global Variables
//==============================================================================
namespace
{
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };

    posix_shm_t<void> g_query_context_shm { };
    query_context_t** g_query_contexts = nullptr;  // [g_query_count]

    std::uint64_t* g_buckets_endoffset_major = nullptr;  // [g_shared->total_buckets]
    std::uint64_t* g_buckets_endoffset_minor = nullptr;  // [g_shared->total_buckets]

    std::thread g_output_write_thread { };  // valid only if tid == 0
}


static void generate_query_output(query_context_t& query) noexcept
{
    //
    // print query to string
    //
    char* const output = query.output;
    uint64_t& output_length = query.output_length;

    constexpr const char header[] = "l_orderkey|o_orderdate|revenue\n";
    memcpy(output, header, std::size(header) - 1);
    output_length = std::size(header) - 1;

    const auto append_u32 = [&](uint32_t n) __attribute__((always_inline)) noexcept {
        char buffer[10];
        size_t pos = std::size(buffer);
        do {
            buffer[--pos] = (char)('0' + n % 10);
            n /= 10;
        } while(n > 0);

        ASSERT(pos < std::size(buffer));
        memcpy(output + output_length, &buffer[pos], std::size(buffer) - pos);
        output_length += std::size(buffer) - pos;
    };
    const auto append_u32_width2 = [&](const uint32_t n) __attribute__((always_inline)) noexcept {
        ASSERT(n <= 99);
        output[output_length++] = (char)('0' + n / 10);
        output[output_length++] = (char)('0' + n % 10);
    };
    const auto append_u32_width4 = [&](const uint32_t n) __attribute__((always_inline)) noexcept {
        ASSERT(n <= 9999);
        output[output_length++] = (char)('0' + (n       ) / 1000);
        output[output_length++] = (char)('0' + (n % 1000) / 100);
        output[output_length++] = (char)('0' + (n % 100 ) / 10);
        output[output_length++] = (char)('0' + (n % 10  ) / 1);
    };

    for (uint32_t i = 0; i < query.results_length; ++i) {
        //printf("%u|%u-%02u-%02u|%u.%02u\n",
        //       line.orderkey,
        //       std::get<0>(ymd), std::get<1>(ymd), std::get<2>(ymd),
        //       line.total_expend_cent / 100, line.total_expend_cent % 100);
        const query_result_t& line = query.results[i];
        const auto ymd = date_get_ymd(line.orderdate);
        append_u32(((line.orderkey & ~0b111) << 2) | (line.orderkey & 0b111));
        output[output_length++] = '|';
        append_u32_width4(std::get<0>(ymd));
        output[output_length++] = '-';
        append_u32_width2(std::get<1>(ymd));
        output[output_length++] = '-';
        append_u32_width2(std::get<2>(ymd));
        output[output_length++] = '|';
        append_u32(line.total_expend_cent / 100);
        output[output_length++] = '.';
        append_u32_width2(line.total_expend_cent % 100);
        output[output_length++] = '\n';
    }
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
        curr_size += q_topn * sizeof(query_result_t);  // results
        curr_size += ((q_topn + 1) * 40ULL);  // output

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

        ctx->q_topn = (uint32_t)std::strtoul(g_argv_queries[4 * query_id + 3], nullptr, 10);
        ctx->output = (char*)(/*(query_result_t*)*/ctx->results + ctx->q_topn);
        ctx->output_length = 0;

        ctx->results_length = 0;

        const auto it = g_mktsegment_to_mktid.find(g_argv_queries[4 * query_id + 0]);
        if (__unlikely(it == g_mktsegment_to_mktid.end())) {
            ctx->is_bad_query = true;
            INFO("query #%u: unknown mktsegment: %s", query_id, g_argv_queries[4 * query_id + 0]);
            continue;
        }

        ctx->q_mktid = it->second;
        ctx->q_orderdate = date_from_string</*_Unchecked*/false>(g_argv_queries[4 * query_id + 1]);
        ctx->q_shipdate = date_from_string</*_Unchecked*/false>(g_argv_queries[4 * query_id + 2]);
        if (__unlikely(ctx->q_topn == 0)) {
            ctx->is_bad_query = true;
            INFO("query #%u: q_topn == 0", query_id);
            continue;
        }

        INFO("query #%u: q_mktid=%u,q_orderdate=%u,q_shipdate=%u,q_topn=%u",
              query_id, ctx->q_mktid, ctx->q_orderdate, ctx->q_shipdate, ctx->q_topn);
    }
}




template<bool _CheckShipdateDiff>
void scan_minor_index_nocheck_orderdate_maybe_check_shipdate(
    /*in*/ const uint32_t* const bucket_ptr,
    /*in*/ const uint64_t bucket_size,
    /*in*/ const date_t bucket_base_orderdate,
    /*inout*/ query_result_t* const results,
    /*inout*/ uint32_t& result_length,
    /*in*/ const uint32_t q_topn,
    /*in,opt*/ [[maybe_unused]] const date_t q_shipdate) noexcept
{
    [[maybe_unused]] __m256i curr_min_expend_cent;
    if (__likely(result_length > 0)) {
        curr_min_expend_cent = _mm256_set1_epi32(results[0].total_expend_cent);
    }
    else {
        curr_min_expend_cent = _mm256_setzero_si256();
    }


    const __m256i expend_mask = _mm256_set_epi32(
        0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
        0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);

    [[maybe_unused]] __m256i greater_than_value;
    [[maybe_unused]] const uint32_t greater_than_value_i32 = (q_shipdate - bucket_base_orderdate) << 24 | 0x00FFFFFF;
    if constexpr (_CheckShipdateDiff) {
        // shipdate = bucket_base_orderdate + diff
        // We want  shipdate > q_shipdate
        //  <==>    bucket_base_orderdate + diff > q_shipdate
        //  <==>    diff > q_shipdate - bucket_base_orderdate
        ASSERT((q_shipdate - bucket_base_orderdate) < (1 << 8));
        greater_than_value = _mm256_set1_epi32(greater_than_value_i32);
    }

    ASSERT(bucket_size % (4 * sizeof(uint32_t)) == 0);
    const uint32_t* p = bucket_ptr;
    const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr + bucket_size);
    const uint32_t* const end_align32 = p + __align_down(bucket_size / sizeof(uint32_t), 32);
    while (p < end_align32) {
        // Do a quick check!
        __m256i items12 = _mm256_load_si256((__m256i*)p);
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask12 = _mm256_cmpgt_epi32(items12, greater_than_value);
            items12 = _mm256_and_si256(items12, gt_mask12);
        }
        items12 = _mm256_and_si256(items12, expend_mask);

        __m256i items34 = _mm256_load_si256((__m256i*)p + 1);
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask34 = _mm256_cmpgt_epi32(items34, greater_than_value);
            items34 = _mm256_and_si256(items34, gt_mask34);
        }
        items34 = _mm256_and_si256(items34, expend_mask);
        
        __m256i items56 = _mm256_load_si256((__m256i*)p + 2);
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask56 = _mm256_cmpgt_epi32(items56, greater_than_value);
            items56 = _mm256_and_si256(items56, gt_mask56);
        }
        items56 = _mm256_and_si256(items56, expend_mask);
        
        __m256i items78 = _mm256_load_si256((__m256i*)p + 3);
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask78 = _mm256_cmpgt_epi32(items78, greater_than_value);
            items78 = _mm256_and_si256(items78, gt_mask78);
        }
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

#define _DECLARE_ONE_ORDER(N) \
            const uint32_t orderdate_diff##N = *(p + 3) >> 30; \
            const date_t orderdate##N = bucket_base_orderdate + orderdate_diff##N; \
            const uint32_t orderkey##N = *(p + 3) & ~0xC0000000U; \
            p += 4;

        _DECLARE_ONE_ORDER(1)
        _DECLARE_ONE_ORDER(2)
        _DECLARE_ONE_ORDER(3)
        _DECLARE_ONE_ORDER(4)
        _DECLARE_ONE_ORDER(5)
        _DECLARE_ONE_ORDER(6)
        _DECLARE_ONE_ORDER(7)
        _DECLARE_ONE_ORDER(8)
#undef _DECLARE_ONE_ORDER


#define _CHECK_RESULT(N) \
            if (total_expend_cent##N > 0) { \
                query_result_t tmp; \
                tmp.orderdate = orderdate##N; \
                tmp.orderkey = orderkey##N; \
                tmp.total_expend_cent = total_expend_cent##N; \
                \
                if (result_length < q_topn) { \
                    results[result_length++] = tmp; \
                    if (__unlikely(result_length == q_topn)) { \
                        std::make_heap(results, results + result_length, std::greater<>()); \
                    } \
                } \
                else { \
                    ASSERT(result_length > 0); \
                    ASSERT(result_length == q_topn); \
                    if (tmp > results[0]) { \
                        std::pop_heap(results, results + result_length, std::greater<>()); \
                        results[result_length-1] = tmp; \
                        std::push_heap(results, results + result_length, std::greater<>()); \
                        /* curr_min_expend_cent hopefully improves performance */ \
                        curr_min_expend_cent = _mm256_set1_epi32(results[0].total_expend_cent); \
                    } \
                } \
            }

        _CHECK_RESULT(1)
        _CHECK_RESULT(2)
        _CHECK_RESULT(3)
        _CHECK_RESULT(4)
        _CHECK_RESULT(5)
        _CHECK_RESULT(6)
        _CHECK_RESULT(7)
        _CHECK_RESULT(8)
    }

    while (p < end) {
        const uint32_t orderdate_diff = *(p + 3) >> 30;
        const date_t orderdate = bucket_base_orderdate + orderdate_diff;
        const uint32_t orderkey = *(p + 3) & ~0xC0000000U;

        uint32_t total_expend_cent;
        if constexpr (_CheckShipdateDiff) {
            total_expend_cent = 0;
            if (p[0] >= greater_than_value_i32) total_expend_cent += p[0] & 0x00FFFFFF;
            if (p[1] >= greater_than_value_i32) total_expend_cent += p[1] & 0x00FFFFFF;
            if (p[2] >= greater_than_value_i32) total_expend_cent += p[2] & 0x00FFFFFF;
        }
        else {
            total_expend_cent = (p[0] & 0x00FFFFFF) + (p[1] & 0x00FFFFFF) + (p[2] & 0x00FFFFFF);
        }
        p += 4;

        _CHECK_RESULT();
    }
#undef _CHECK_RESULT
}


template<bool _CheckShipdateDiff>
void scan_minor_index_check_orderdate_maybe_check_shipdate(
    /*in*/ const uint32_t* const bucket_ptr,
    /*in*/ const uint64_t bucket_size,
    /*in*/ const date_t bucket_base_orderdate,
    /*in*/ const date_t q_orderdate,
    /*inout*/ query_result_t* const results,
    /*inout*/ uint32_t& result_length,
    /*in*/ const uint32_t q_topn,
    /*in,opt*/ [[maybe_unused]] const date_t q_shipdate) noexcept
{
    [[maybe_unused]] const uint32_t greater_than_value_i32 = (q_shipdate - bucket_base_orderdate) << 24 | 0x00FFFFFF;
    if constexpr (_CheckShipdateDiff) {
        // shipdate = bucket_base_orderdate + diff
        // We want  shipdate > q_shipdate
        //  <==>    bucket_base_orderdate + diff > q_shipdate
        //  <==>    diff > q_shipdate - bucket_base_orderdate
        ASSERT(q_shipdate >= bucket_base_orderdate);
        ASSERT((q_shipdate - bucket_base_orderdate) < (1 << 8));
    }

    ASSERT(bucket_size % (4 * sizeof(uint32_t)) == 0);
    const uint32_t* p = bucket_ptr;
    const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr + bucket_size);

    while (p < end) {
        const uint32_t orderdate_diff = *(p + 3) >> 30;
        const date_t orderdate = bucket_base_orderdate + orderdate_diff;
        if (orderdate >= q_orderdate) {
            p += 4;
            continue;
        }

        const uint32_t orderkey = *(p + 3) & ~0xC0000000U;

        uint32_t total_expend_cent;
        if constexpr (_CheckShipdateDiff) {
            total_expend_cent = 0;
            if (p[0] >= greater_than_value_i32) total_expend_cent += p[0] & 0x00FFFFFF;
            if (p[1] >= greater_than_value_i32) total_expend_cent += p[1] & 0x00FFFFFF;
            if (p[2] >= greater_than_value_i32) total_expend_cent += p[2] & 0x00FFFFFF;
        }
        else {
            total_expend_cent = (p[0] & 0x00FFFFFF) + (p[1] & 0x00FFFFFF) + (p[2] & 0x00FFFFFF);
        }
        p += 4;

        if (total_expend_cent > 0) {
            query_result_t tmp;
            tmp.orderdate = orderdate;
            tmp.orderkey = orderkey;
            tmp.total_expend_cent = total_expend_cent;

            if (__unlikely(result_length < q_topn)) {
                results[result_length++] = tmp;
                if (__unlikely(result_length == q_topn)) {
                    std::make_heap(results, results + result_length, std::greater<>());
                }
            }
            else {
                ASSERT(result_length > 0);
                ASSERT(result_length == q_topn);
                if (__unlikely(tmp > results[0])) {
                    std::pop_heap(results, results + result_length, std::greater<>());
                    results[result_length-1] = tmp;
                    std::push_heap(results, results + result_length, std::greater<>());
                }
            }
        }
    }
}





template<bool _CheckShipdateDiff>
void scan_major_index_nocheck_orderdate_maybe_check_shipdate(
    /*in*/ const uint32_t* const bucket_ptr,
    /*in*/ const uint64_t bucket_size,
    /*in*/ const date_t bucket_base_orderdate,
    /*inout*/ query_result_t* const results,
    /*inout*/ uint32_t& result_length,
    /*in*/ const uint32_t q_topn,
    /*in,opt*/ [[maybe_unused]] const date_t q_shipdate) noexcept
{
    const __m256i expend_mask = _mm256_set_epi32(
        0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
        0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);

    [[maybe_unused]] __m256i greater_than_value;
    if constexpr (_CheckShipdateDiff) {
        // shipdate = bucket_base_orderdate + diff
        // We want  shipdate > q_shipdate
        //  <==>    bucket_base_orderdate + diff > q_shipdate
        //  <==>    diff > q_shipdate - bucket_base_orderdate
        ASSERT((q_shipdate - bucket_base_orderdate) < (1 << 8));
        greater_than_value = _mm256_set1_epi32((q_shipdate - bucket_base_orderdate) << 24 | 0x00FFFFFF);
    }


//    {
//        const uint32_t* p = bucket_ptr;
//        const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr + bucket_size);
//        while (p < end) {
//            const uint32_t orderdate_diff = *(p + 7) >> 30;
//            const date_t orderdate = bucket_base_orderdate + orderdate_diff;
//            const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
//
//            if (orderkey == 25202592) {
//                INFO("--------> (q_shipdate - bucket_base_orderdate): %u", (q_shipdate - bucket_base_orderdate));
//                for (uint32_t i = 0; i < 7; ++i) {
//                    const uint32_t value = p[i];
//                    const uint32_t shipdate_diff = value >> 24;
//                    const uint32_t expend_cent = value & 0x00FFFFFF;
//                    INFO("--------> shipdate_diff=%u, expend_cent=%u", shipdate_diff, expend_cent);
//                }
//
//                if constexpr (_CheckShipdateDiff) {
//                    __m256i items = _mm256_load_si256((__m256i*)p);
//                    const __m256i gt_mask = _mm256_cmpgt_epi32(items, greater_than_value);
//                    INFO("gt_mask[%u] = 0x%08x", 0, _mm256_extract_epi32(gt_mask, 0));
//                    INFO("gt_mask[%u] = 0x%08x", 1, _mm256_extract_epi32(gt_mask, 1));
//                    INFO("gt_mask[%u] = 0x%08x", 2, _mm256_extract_epi32(gt_mask, 2));
//                    INFO("gt_mask[%u] = 0x%08x", 3, _mm256_extract_epi32(gt_mask, 3));
//                    INFO("gt_mask[%u] = 0x%08x", 4, _mm256_extract_epi32(gt_mask, 4));
//                    INFO("gt_mask[%u] = 0x%08x", 5, _mm256_extract_epi32(gt_mask, 5));
//                    INFO("gt_mask[%u] = 0x%08x", 6, _mm256_extract_epi32(gt_mask, 6));
//                    INFO("gt_mask[%u] = 0x%08x", 7, _mm256_extract_epi32(gt_mask, 7));
//                    items = _mm256_and_si256(items, gt_mask);
//                    items = _mm256_and_si256(items, expend_mask);
//                }
//            }
//            p += 8;
//        }
//    }

    ASSERT(bucket_size % (8 * sizeof(uint32_t)) == 0);
    const uint32_t* p = bucket_ptr;
    const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr + bucket_size);
    const uint32_t* const end_align32 = p + __align_down(bucket_size / sizeof(uint32_t), 32);
    while (p < end_align32) {
        const uint32_t orderdate_diff1 = *(p + 7) >> 30;
        const date_t orderdate1 = bucket_base_orderdate + orderdate_diff1;
        const uint32_t orderkey1 = *(p + 7) & ~0xC0000000U;
        __m256i items1 = _mm256_load_si256((__m256i*)p);
        p += 8;
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask1 = _mm256_cmpgt_epi32(items1, greater_than_value);
            items1 = _mm256_and_si256(items1, gt_mask1);
        }
        items1 = _mm256_and_si256(items1, expend_mask);

        const uint32_t orderdate_diff2 = *(p + 7) >> 30;
        const date_t orderdate2 = bucket_base_orderdate + orderdate_diff2;
        const uint32_t orderkey2 = *(p + 7) & ~0xC0000000U;
        __m256i items2 = _mm256_load_si256((__m256i*)p);
        p += 8;
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask2 = _mm256_cmpgt_epi32(items2, greater_than_value);
            items2 = _mm256_and_si256(items2, gt_mask2);
        }
        items2 = _mm256_and_si256(items2, expend_mask);

        const uint32_t orderdate_diff3 = *(p + 7) >> 30;
        const date_t orderdate3 = bucket_base_orderdate + orderdate_diff3;
        const uint32_t orderkey3 = *(p + 7) & ~0xC0000000U;
        __m256i items3 = _mm256_load_si256((__m256i*)p);
        p += 8;
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask3 = _mm256_cmpgt_epi32(items3, greater_than_value);
            items3 = _mm256_and_si256(items3, gt_mask3);
        }
        items3 = _mm256_and_si256(items3, expend_mask);

        const uint32_t orderdate_diff4 = *(p + 7) >> 30;
        const date_t orderdate4 = bucket_base_orderdate + orderdate_diff4;
        const uint32_t orderkey4 = *(p + 7) & ~0xC0000000U;
        __m256i items4 = _mm256_load_si256((__m256i*)p);
        p += 8;
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask4 = _mm256_cmpgt_epi32(items4, greater_than_value);
            items4 = _mm256_and_si256(items4, gt_mask4);
        }
        items4 = _mm256_and_si256(items4, expend_mask);

        // Inspired by
        //   https://stackoverflow.com/questions/9775538/fastest-way-to-do-horizontal-vector-sum-with-avx-instructions
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
                if (result_length < q_topn) { \
                    results[result_length++] = tmp; \
                    if (__unlikely(result_length == q_topn)) { \
                        std::make_heap(results, results + result_length, std::greater<>()); \
                    } \
                } \
                else { \
                    ASSERT(result_length > 0); \
                    ASSERT(result_length == q_topn); \
                    if (tmp > results[0]) { \
                        std::pop_heap(results, results + result_length, std::greater<>()); \
                        results[result_length-1] = tmp; \
                        std::push_heap(results, results + result_length, std::greater<>()); \
                    } \
                } \
            }

        _CHECK_RESULT(1)
        _CHECK_RESULT(2)
        _CHECK_RESULT(3)
        _CHECK_RESULT(4)
    }

    while (p < end) {
        const uint32_t orderdate_diff = *(p + 7) >> 30;
        const date_t orderdate = bucket_base_orderdate + orderdate_diff;
        const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
        __m256i items = _mm256_load_si256((__m256i*)p);
        p += 8;
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask = _mm256_cmpgt_epi32(items, greater_than_value);
            //if (_mm256_testz_si256(gt_mask, gt_mask)) continue;  // not necessary
            items = _mm256_and_si256(items, gt_mask);
        }
        items = _mm256_and_si256(items, expend_mask);

        __m256i sum = _mm256_hadd_epi32(items, items);
        sum = _mm256_hadd_epi32(sum, sum);
        const uint32_t total_expend_cent = _mm256_extract_epi32(sum, 0) + _mm256_extract_epi32(sum, 4);

        _CHECK_RESULT();
    }
#undef _CHECK_RESULT
}


//template<bool _CheckShipdateDiff>
//void scan_major_index_check_orderdate_maybe_check_shipdate(
//    /*in*/ const uint32_t* const bucket_ptr,
//    /*in*/ const uint64_t bucket_size,
//    /*in*/ const date_t bucket_base_orderdate,
//    /*in*/ const date_t q_orderdate,
//    /*inout*/ query_result_t* const results,
//    /*inout*/ uint32_t& result_length,
//    /*in*/ const uint32_t q_topn,
//    /*in,opt*/ [[maybe_unused]] const date_t q_shipdate) noexcept
//{
//    const __m256i expend_mask = _mm256_set_epi32(
//        0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
//        0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);
//
//    [[maybe_unused]] __m256i greater_than_value;
//    if constexpr (_CheckShipdateDiff) {
//        // shipdate = bucket_base_orderdate + diff
//        // We want  shipdate > q_shipdate
//        //  <==>    bucket_base_orderdate + diff > q_shipdate
//        //  <==>    diff > q_shipdate - bucket_base_orderdate
//        ASSERT((q_shipdate - bucket_base_orderdate) < (1 << 8));
//        greater_than_value = _mm256_set1_epi32((q_shipdate - bucket_base_orderdate) << 24 | 0x00FFFFFF);
//    }
//
//    ASSERT(bucket_size % (8 * sizeof(uint32_t)) == 0);
//    const uint32_t* p = bucket_ptr;
//    const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr + bucket_size);
//
//    if (__unlikely(p >= end)) {
//        ASSERT(p == end);
//        return;
//    }
//
//    uint32_t valid_item_count = 0;
//
//    __m256i items1, items2, items3, items4;
//    date_t orderdate1, orderdate2, orderdate3, orderdate4;
//    uint32_t orderkey1, orderkey2, orderkey3, orderkey4;
//    uint32_t total_expend_cent1, total_expend_cent2, total_expend_cent3, total_expend_cent4;
//
//    while (true) {
//        // items1
//        while (p < end) {
//            const uint32_t orderdate_diff1 = *(p + 7) >> 30;
//            orderdate1 = bucket_base_orderdate + orderdate_diff1;
//            if (orderdate1 >= q_orderdate) {
//                p += 8;
//                continue;
//            }
//            orderkey1 = *(p + 7) & ~0xC0000000U;
//            items1 = _mm256_load_si256((__m256i*)p);
//            p += 8;
//            if constexpr (_CheckShipdateDiff) {
//                const __m256i gt_mask1 = _mm256_cmpgt_epi32(items1, greater_than_value);
//                items1 = _mm256_and_si256(items1, gt_mask1);
//            }
//            items1 = _mm256_and_si256(items1, expend_mask);
//            ++valid_item_count;
//            break;
//        }
//        if (p >= end) {
//            ASSERT(p == end);
//            break;
//        }
//
//        // items2
//        while (p < end) {
//            const uint32_t orderdate_diff2 = *(p + 7) >> 30;
//            orderdate2 = bucket_base_orderdate + orderdate_diff2;
//            if (orderdate2 >= q_orderdate) {
//                p += 8;
//                continue;
//            }
//            orderkey2 = *(p + 7) & ~0xC0000000U;
//            items2 = _mm256_load_si256((__m256i*)p);
//            p += 8;
//            if constexpr (_CheckShipdateDiff) {
//                const __m256i gt_mask2 = _mm256_cmpgt_epi32(items2, greater_than_value);
//                items2 = _mm256_and_si256(items2, gt_mask2);
//            }
//            items2 = _mm256_and_si256(items2, expend_mask);
//            ++valid_item_count;
//            break;
//        }
//        if (p >= end) {
//            ASSERT(p == end);
//            break;
//        }
//
//        // items3
//        while (p < end) {
//            const uint32_t orderdate_diff3 = *(p + 7) >> 30;
//            orderdate3 = bucket_base_orderdate + orderdate_diff3;
//            if (orderdate3 >= q_orderdate) {
//                p += 8;
//                continue;
//            }
//            orderkey3 = *(p + 7) & ~0xC0000000U;
//            items3 = _mm256_load_si256((__m256i*)p);
//            p += 8;
//            if constexpr (_CheckShipdateDiff) {
//                const __m256i gt_mask3 = _mm256_cmpgt_epi32(items3, greater_than_value);
//                items3 = _mm256_and_si256(items3, gt_mask3);
//            }
//            items3 = _mm256_and_si256(items3, expend_mask);
//            ++valid_item_count;
//            break;
//        }
//        if (p >= end) {
//            ASSERT(p == end);
//            break;
//        }
//
//        // items4
//        while (p < end) {
//            const uint32_t orderdate_diff4 = *(p + 7) >> 30;
//            orderdate4 = bucket_base_orderdate + orderdate_diff4;
//            if (orderdate4 >= q_orderdate) {
//                p += 8;
//                continue;
//            }
//            orderkey4 = *(p + 7) & ~0xC0000000U;
//            items4 = _mm256_load_si256((__m256i*)p);
//            p += 8;
//            if constexpr (_CheckShipdateDiff) {
//                const __m256i gt_mask4 = _mm256_cmpgt_epi32(items4, greater_than_value);
//                items4 = _mm256_and_si256(items4, gt_mask4);
//            }
//            items4 = _mm256_and_si256(items4, expend_mask);
//            ++valid_item_count;
//            break;
//        }
//
//        // Inspired by
//        //   https://stackoverflow.com/questions/9775538/fastest-way-to-do-horizontal-vector-sum-with-avx-instructions
//        const __m256i tmp1 = _mm256_hadd_epi32(items1, items2);
//        const __m256i tmp2 = _mm256_hadd_epi32(items3, items4);
//        const __m256i tmp3 = _mm256_hadd_epi32(tmp1, tmp2);
//        const __m128i tmp3lo = _mm256_castsi256_si128(tmp3);
//        const __m128i tmp3hi = _mm256_extracti128_si256(tmp3, 1);
//        const __m128i sum = _mm_add_epi32(tmp3hi, tmp3lo);
//
//        total_expend_cent1 = _mm_extract_epi32(sum, 0);
//        total_expend_cent2 = _mm_extract_epi32(sum, 1);
//        total_expend_cent3 = _mm_extract_epi32(sum, 2);
//        total_expend_cent4 = _mm_extract_epi32(sum, 3);
//
//#define _CHECK_RESULT(N) \
//            if (total_expend_cent##N > 0) { \
//                query_result_t tmp; \
//                tmp.orderdate = orderdate##N; \
//                tmp.orderkey = orderkey##N; \
//                tmp.total_expend_cent = total_expend_cent##N; \
//                \
//                if (result_length < q_topn) { \
//                    results[result_length++] = tmp; \
//                    if (__unlikely(result_length == q_topn)) { \
//                        std::make_heap(results, results + result_length, std::greater<>()); \
//                    } \
//                } \
//                else { \
//                    ASSERT(result_length > 0); \
//                    ASSERT(result_length == q_topn); \
//                    if (tmp > results[0]) { \
//                        std::pop_heap(results, results + result_length, std::greater<>()); \
//                        results[result_length-1] = tmp; \
//                        std::push_heap(results, results + result_length, std::greater<>()); \
//                    } \
//                } \
//            }
//
//        _CHECK_RESULT(1)
//        _CHECK_RESULT(2)
//        _CHECK_RESULT(3)
//        _CHECK_RESULT(4)
//
//        if (p >= end) {
//            ASSERT(p == end);
//            break;
//        }
//    }
//
//    switch (valid_item_count % 4) {
//        case 3: {
//            __m256i sum3 = _mm256_hadd_epi32(items3, items3);
//            sum3 = _mm256_hadd_epi32(sum3, sum3);
//            total_expend_cent3 = _mm256_extract_epi32(sum3, 0) + _mm256_extract_epi32(sum3, 4);
//            _CHECK_RESULT(3);
//            [[fallthrough]];
//        }
//        case 2: {
//            __m256i sum2 = _mm256_hadd_epi32(items2, items2);
//            sum2 = _mm256_hadd_epi32(sum2, sum2);
//            total_expend_cent2 = _mm256_extract_epi32(sum2, 0) + _mm256_extract_epi32(sum2, 4);
//            _CHECK_RESULT(2);
//            [[fallthrough]];
//        }
//        case 1: {
//            __m256i sum1 = _mm256_hadd_epi32(items1, items1);
//            sum1 = _mm256_hadd_epi32(sum1, sum1);
//            total_expend_cent1 = _mm256_extract_epi32(sum1, 0) + _mm256_extract_epi32(sum1, 4);
//            _CHECK_RESULT(1);
//            [[fallthrough]];
//        }
//        case 0:
//            // Do nothing
//            break;
//    }
//#undef _CHECK_RESULT
//}

template<bool _CheckShipdateDiff>
void scan_major_index_check_orderdate_maybe_check_shipdate(
    /*in*/ const uint32_t* const bucket_ptr,
    /*in*/ const uint64_t bucket_size,
    /*in*/ const date_t bucket_base_orderdate,
    /*in*/ const date_t q_orderdate,
    /*inout*/ query_result_t* const results,
    /*inout*/ uint32_t& result_length,
    /*in*/ const uint32_t q_topn,
    /*in,opt*/ [[maybe_unused]] const date_t q_shipdate) noexcept
{
    const __m256i expend_mask = _mm256_set_epi32(
        0x00000000, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF,
        0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF, 0x00FFFFFF);

    [[maybe_unused]] __m256i greater_than_value;
    if constexpr (_CheckShipdateDiff) {
        // shipdate = bucket_base_orderdate + diff
        // We want  shipdate > q_shipdate
        //  <==>    bucket_base_orderdate + diff > q_shipdate
        //  <==>    diff > q_shipdate - bucket_base_orderdate
        ASSERT(q_shipdate >= bucket_base_orderdate);
        ASSERT((q_shipdate - bucket_base_orderdate) < (1 << 8));
        greater_than_value = _mm256_set1_epi32((q_shipdate - bucket_base_orderdate) << 24 | 0x00FFFFFF);
    }

    ASSERT(bucket_size % (8 * sizeof(uint32_t)) == 0);
    const uint32_t* p = bucket_ptr;
    const uint32_t* const end = (uint32_t*)((uintptr_t)bucket_ptr + bucket_size);

    while (p < end) {
        const uint32_t orderdate_diff = *(p + 7) >> 30;
        const date_t orderdate = bucket_base_orderdate + orderdate_diff;
        if (orderdate >= q_orderdate) {
            p += 8;
            continue;
        }

        const uint32_t orderkey = *(p + 7) & ~0xC0000000U;
        __m256i items = _mm256_load_si256((__m256i*)p);
        p += 8;
        if constexpr (_CheckShipdateDiff) {
            const __m256i gt_mask = _mm256_cmpgt_epi32(items, greater_than_value);
            //if (_mm256_testz_si256(gt_mask, gt_mask)) continue;
            items = _mm256_and_si256(items, gt_mask);
        }
        items = _mm256_and_si256(items, expend_mask);

        __m256i sum = _mm256_hadd_epi32(items, items);
        sum = _mm256_hadd_epi32(sum, sum);
        const uint32_t total_expend_cent = _mm256_extract_epi32(sum, 0) + _mm256_extract_epi32(sum, 4);

        if (total_expend_cent > 0) {
            query_result_t tmp;
            tmp.orderdate = orderdate;
            tmp.orderkey = orderkey;
            tmp.total_expend_cent = total_expend_cent;

            if (result_length < q_topn) {
                results[result_length++] = tmp;
                if (__unlikely(result_length == q_topn)) {
                    std::make_heap(results, results + result_length, std::greater<>());
                }
            }
            else {
                ASSERT(result_length > 0);
                ASSERT(result_length == q_topn);
                if (tmp > results[0]) {
                    std::pop_heap(results, results + result_length, std::greater<>());
                    results[result_length-1] = tmp;
                    std::push_heap(results, results + result_length, std::greater<>());
                }
            }
        }
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
            std::min<date_t>(
                scan_end_orderdate,
                MAX_TABLE_DATE));
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
            //ASSERT(orderdate <= MAX_TABLE_DATE + CONFIG_ORDERDATES_PER_BUCKET);
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

        ctx->only_check_orderdate_begin_bucket_id = __calc_bucket_index_saturate(
            scan_end_orderdate);
        ASSERT(ctx->only_check_orderdate_begin_bucket_id >= curr_mktid_min_bucket_id);
        ASSERT(ctx->only_check_orderdate_begin_bucket_id <= curr_mktid_max_bucket_id);
        ASSERT(ctx->only_check_orderdate_begin_bucket_id <= ctx->only_check_orderdate_end_bucket_id);
        ASSERT(ctx->only_check_orderdate_begin_bucket_id + 1 >= ctx->only_check_orderdate_end_bucket_id);

        ctx->check_orderdate_check_shipdate_begin_bucket_id = ctx->only_check_orderdate_begin_bucket_id;
        ctx->only_check_shipdate_begin_bucket_id = ctx->only_check_orderdate_begin_bucket_id;
        ctx->nocheck_head_begin_bucket_id = ctx->only_check_orderdate_begin_bucket_id;
        ctx->pretopn_begin_bucket_id = ctx->only_check_orderdate_begin_bucket_id;
        ctx->nocheck_tail_begin_bucket_id = ctx->only_check_orderdate_begin_bucket_id;

        do {
            ctx->check_orderdate_check_shipdate_begin_bucket_id = calc_bucket_index(
                ctx->q_mktid,
                scan_begin_orderdate);
            ASSERT(ctx->check_orderdate_check_shipdate_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->check_orderdate_check_shipdate_begin_bucket_id <= curr_mktid_max_bucket_id);
            if (ctx->check_orderdate_check_shipdate_begin_bucket_id >= ctx->only_check_orderdate_begin_bucket_id) {
                ASSERT(ctx->check_orderdate_check_shipdate_begin_bucket_id == ctx->only_check_orderdate_begin_bucket_id);
                break;
            }


            ctx->only_check_shipdate_begin_bucket_id = __calc_bucket_index_saturate(
                scan_begin_orderdate + (CONFIG_ORDERDATES_PER_BUCKET - 1));
            ASSERT(ctx->only_check_shipdate_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->only_check_shipdate_begin_bucket_id <= curr_mktid_max_bucket_id);
            ASSERT(ctx->only_check_shipdate_begin_bucket_id >= ctx->check_orderdate_check_shipdate_begin_bucket_id);
            ASSERT(ctx->only_check_shipdate_begin_bucket_id <= ctx->check_orderdate_check_shipdate_begin_bucket_id + 1);
            if (ctx->only_check_shipdate_begin_bucket_id >= ctx->only_check_orderdate_begin_bucket_id) {
                ASSERT(ctx->only_check_shipdate_begin_bucket_id == ctx->only_check_orderdate_begin_bucket_id);
                break;
            }


            ctx->nocheck_head_begin_bucket_id = __calc_bucket_index_saturate(
                scan_begin_nocheck_orderdate + (CONFIG_ORDERDATES_PER_BUCKET - 1));
            ASSERT(ctx->nocheck_head_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->nocheck_head_begin_bucket_id <= curr_mktid_max_bucket_id);
            ASSERT(ctx->nocheck_head_begin_bucket_id >= ctx->only_check_shipdate_begin_bucket_id);
            ASSERT(ctx->nocheck_head_begin_bucket_id <= ctx->only_check_shipdate_begin_bucket_id + __div_up(g_shared->meta.max_shipdate_orderdate_diff, CONFIG_ORDERDATES_PER_BUCKET));
            if (ctx->nocheck_head_begin_bucket_id >= ctx->only_check_orderdate_begin_bucket_id) {
                ASSERT(ctx->nocheck_head_begin_bucket_id <= ctx->only_check_orderdate_begin_bucket_id + 1,
                    "Expect: %u <= %u + 1", ctx->nocheck_head_begin_bucket_id, ctx->only_check_orderdate_begin_bucket_id);
                ctx->nocheck_head_begin_bucket_id = ctx->only_check_orderdate_begin_bucket_id;
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
                if (nocheck_head_plate_base_bucket_id + BUCKETS_PER_PLATE >= ctx->only_check_orderdate_begin_bucket_id) {
                    break;
                }
                ctx->pretopn_begin_bucket_id = nocheck_head_plate_base_bucket_id + BUCKETS_PER_PLATE;
                pretopn_begin_plate_id = nocheck_head_plate_id + 1;
            }
            ASSERT(ctx->pretopn_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->pretopn_begin_bucket_id <= curr_mktid_max_bucket_id);
            ASSERT(ctx->pretopn_begin_bucket_id >= ctx->nocheck_head_begin_bucket_id);
            ASSERT(ctx->pretopn_begin_bucket_id < ctx->nocheck_head_begin_bucket_id + BUCKETS_PER_PLATE);
            if (ctx->pretopn_begin_bucket_id >= ctx->only_check_orderdate_begin_bucket_id) {
                ASSERT(ctx->pretopn_begin_bucket_id == ctx->only_check_orderdate_begin_bucket_id,
                    "Expect %u == %u", ctx->pretopn_begin_bucket_id, ctx->only_check_orderdate_begin_bucket_id);
                break;
            }

            const date_t pretopn_begin_orderdate = calc_plate_base_orderdate_by_plate_id(pretopn_begin_plate_id);
            ASSERT(pretopn_begin_orderdate <= scan_end_orderdate);
            const uint32_t num_plates = __div_down(scan_end_orderdate - pretopn_begin_orderdate, CONFIG_TOPN_DATES_PER_PLATE);
            ctx->nocheck_tail_begin_bucket_id = ctx->pretopn_begin_bucket_id + num_plates * BUCKETS_PER_PLATE;
            ASSERT(ctx->nocheck_tail_begin_bucket_id >= curr_mktid_min_bucket_id);
            ASSERT(ctx->nocheck_tail_begin_bucket_id <= curr_mktid_max_bucket_id);
            ASSERT(ctx->nocheck_tail_begin_bucket_id >= ctx->pretopn_begin_bucket_id);
            if (ctx->nocheck_tail_begin_bucket_id >= ctx->only_check_orderdate_begin_bucket_id) {
                ASSERT(ctx->nocheck_tail_begin_bucket_id == ctx->only_check_orderdate_begin_bucket_id);
                break;
            }
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
    //
    // Map major index
    //
    ASSERT(g_buckets_endoffset_major != nullptr);

    //
    // TODO: for ground truth only!
    //
    void* const ptr = mmap_reserve_space(g_shared->meta.max_bucket_size_major);

    while (true) {
        const uint32_t query_id = g_shared->use_index.worker_query_id_shared_counter++;
        if (query_id >= g_query_count) break;
        query_context_t* const ctx = g_query_contexts[query_id];

        if (ctx->is_bad_query) {
            INFO("query #%u: is_bad_query!", query_id);

            generate_query_output(*ctx);
            ctx->done.mark_done();
            continue;
        }


        //
        // Step 1: pretopn
        //
        ASSERT(g_pretopn_start_ptr != nullptr);
        ASSERT(g_pretopn_count_start_ptr != nullptr);
        if (ctx->nocheck_tail_begin_bucket_id > ctx->pretopn_begin_bucket_id) {
            ASSERT((ctx->nocheck_tail_begin_bucket_id - ctx->pretopn_begin_bucket_id) % BUCKETS_PER_PLATE == 0);

            const uint32_t from_plate_id = calc_plate_id(ctx->pretopn_begin_bucket_id);
            const uint32_t to_plate_id = from_plate_id + (ctx->nocheck_tail_begin_bucket_id - ctx->pretopn_begin_bucket_id) / BUCKETS_PER_PLATE - 1;
            TRACE("from_plate_id=%u (base_orderdate=%u) -> to_plate_id=%u (base_orderdate=%u)",
                from_plate_id, calc_plate_base_orderdate_by_plate_id(from_plate_id),
                to_plate_id, calc_plate_base_orderdate_by_plate_id(to_plate_id));
#if ENABLE_ASSERTION
            ASSERT(calc_plate_base_bucket_id_by_plate_id(from_plate_id) == ctx->pretopn_begin_bucket_id);
#endif

            for (uint32_t plate_id = from_plate_id; plate_id <= to_plate_id; ++plate_id) {
                const uint32_t topn_count = g_pretopn_count_start_ptr[plate_id];
                ASSERT(topn_count <= CONFIG_EXPECT_MAX_TOPN);
                const uint64_t* const topn_ptr = g_pretopn_start_ptr + (uint64_t)plate_id * CONFIG_EXPECT_MAX_TOPN;
                const date_t plate_base_orderdate = calc_plate_base_orderdate_by_plate_id(plate_id);

                for (uint32_t i = 0; i < topn_count; ++i) {
                    const uint64_t value = topn_ptr[i];
                    const uint32_t total_expend_cent = (uint32_t)(value >> 36);
                    const uint32_t orderkey = (uint32_t)((value >> 6) & ((1U << 30) - 1));
                    const uint32_t orderdate_diff = (uint32_t)(value & 0b111111);
                    ASSERT(total_expend_cent > 0);

                    query_result_t tmp;
                    tmp.orderdate = plate_base_orderdate + orderdate_diff;
                    tmp.orderkey = orderkey;
                    tmp.total_expend_cent = total_expend_cent;

                    if (ctx->results_length < ctx->q_topn) {
                        ctx->results[ctx->results_length++] = tmp;
                        if (__unlikely(ctx->results_length == ctx->q_topn)) {
                            std::make_heap(ctx->results, ctx->results + ctx->results_length, std::greater<>());
                        }
                    }
                    else {
                        ASSERT(ctx->results_length > 0);
                        ASSERT(ctx->results_length == ctx->q_topn);
                        if (tmp > ctx->results[0]) {
                            std::pop_heap(ctx->results, ctx->results + ctx->results_length, std::greater<>());
                            ctx->results[ctx->results_length-1] = tmp;
                            std::push_heap(ctx->results, ctx->results + ctx->results_length, std::greater<>());
                        }
                        else {
                            break;
                        }
                    }
                }
            }
        }


        //
        // Step 2: major buckets, nocheck_orderdate_check_shipdate
        //
        for (uint32_t bucket_id = ctx->check_orderdate_check_shipdate_begin_bucket_id;
            bucket_id < ctx->nocheck_head_begin_bucket_id;
            ++bucket_id) {

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t holder_begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - holder_begin_bucket_id);
            const uint64_t bucket_size_major = g_buckets_endoffset_major[bucket_id] - bucket_start_offset_major;
            if (__unlikely(bucket_size_major == 0)) continue;

            void* const mapped_ptr = mmap(
                ptr,
                g_shared->meta.max_bucket_size_major,  // NOTE: do NOT use bucket_size_major for better performance
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_major_fd[holder_id],
                bucket_start_offset_major);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == ptr);

            scan_major_index_nocheck_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/true>(
                (const uint32_t*)ptr,
                bucket_size_major,
                calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                ctx->results,
                ctx->results_length,
                ctx->q_topn,
                ctx->q_shipdate);
        }


        //
        // Step 3: major buckets, nocheck_orderdate_nocheck_shipdate
        //
        for (uint32_t bucket_id = ctx->nocheck_head_begin_bucket_id;
            bucket_id < ctx->pretopn_begin_bucket_id;
            ++bucket_id) {

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t holder_begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - holder_begin_bucket_id);
            const uint64_t bucket_size_major = g_buckets_endoffset_major[bucket_id] - bucket_start_offset_major;
            if (__unlikely(bucket_size_major == 0)) continue;

            void* const mapped_ptr = mmap(
                ptr,
                g_shared->meta.max_bucket_size_major,  // NOTE: do NOT use bucket_size_major for better performance
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_major_fd[holder_id],
                bucket_start_offset_major);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == ptr);

            scan_major_index_nocheck_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/false>(
                (const uint32_t*)ptr,
                bucket_size_major,
                calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                ctx->results,
                ctx->results_length,
                ctx->q_topn,
                ctx->q_shipdate);

            //INFO("query #%u: <2.1> nocheck_orderdate, nocheck_shipdate bucket %u", query_id, bucket_id);
        }


        //
        // Step 4: major buckets, nocheck_orderdate_nocheck_shipdate
        //
        for (uint32_t bucket_id = ctx->nocheck_tail_begin_bucket_id;
            bucket_id < ctx->only_check_orderdate_begin_bucket_id;
            ++bucket_id) {

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t holder_begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - holder_begin_bucket_id);
            const uint64_t bucket_size_major = g_buckets_endoffset_major[bucket_id] - bucket_start_offset_major;
            if (__unlikely(bucket_size_major == 0)) continue;

            void* const mapped_ptr = mmap(
                ptr,
                g_shared->meta.max_bucket_size_major,  // NOTE: do NOT use bucket_size_major for better performance
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_major_fd[holder_id],
                bucket_start_offset_major);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == ptr);

            scan_major_index_nocheck_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/false>(
                (const uint32_t*)ptr,
                bucket_size_major,
                calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                ctx->results,
                ctx->results_length,
                ctx->q_topn,
                ctx->q_shipdate);

            //INFO("query #%u: <2.2> nocheck_orderdate, nocheck_shipdate bucket %u", query_id, bucket_id);
        }


        //
        // Step 5: major buckets, check_orderdate_maybe_check_shipdate
        //
        for (uint32_t bucket_id = ctx->only_check_orderdate_begin_bucket_id;
            bucket_id < ctx->only_check_orderdate_end_bucket_id;
            ++bucket_id) {

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t holder_begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_major = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MAJOR * (bucket_id - holder_begin_bucket_id);
            const uint64_t bucket_size_major = g_buckets_endoffset_major[bucket_id] - bucket_start_offset_major;
            if (__unlikely(bucket_size_major == 0)) continue;

            void* const mapped_ptr = mmap(
                ptr,
                g_shared->meta.max_bucket_size_major,  // NOTE: do NOT use bucket_size_major for better performance
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_major_fd[holder_id],
                bucket_start_offset_major);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == ptr);

            const date_t bucket_base_orderdate = calc_bucket_base_orderdate_by_bucket_id(bucket_id);
            if (ctx->q_shipdate >= bucket_base_orderdate) {
                scan_major_index_check_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/true>(
                    (const uint32_t*)ptr,
                    bucket_size_major,
                    calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                    ctx->q_orderdate,
                    ctx->results,
                    ctx->results_length,
                    ctx->q_topn,
                    ctx->q_shipdate);
            }
            else {
                scan_major_index_check_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/false>(
                    (const uint32_t*)ptr,
                    bucket_size_major,
                    calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                    ctx->q_orderdate,
                    ctx->results,
                    ctx->results_length,
                    ctx->q_topn,
                    ctx->q_shipdate);
            }
        }



        //
        // Step 6: minor buckets, nocheck_orderdate_check_shipdate
        //
        for (uint32_t bucket_id = ctx->check_orderdate_check_shipdate_begin_bucket_id;
            bucket_id < ctx->nocheck_head_begin_bucket_id;
            ++bucket_id) {

            // Check "only_minor_max_expend" index (likely to skip minor buckets)
            ASSERT(g_only_minor_max_expend_start_ptr != nullptr);
            const uint32_t only_minor_max_expend = g_only_minor_max_expend_start_ptr[bucket_id];
            if (__likely(ctx->results_length > 0)) {
                const uint32_t curr_min_expend = ctx->results[0].total_expend_cent;
                if (__likely(only_minor_max_expend < curr_min_expend)) {
                    continue;
                }
            }

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t holder_begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_minor = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - holder_begin_bucket_id);
            const uint64_t bucket_size_minor = g_buckets_endoffset_minor[bucket_id] - bucket_start_offset_minor;
            if (__unlikely(bucket_size_minor == 0)) continue;

            void* const mapped_ptr = mmap(
                ptr,
                bucket_size_minor,
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_minor_fd[holder_id],
                bucket_start_offset_minor);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == ptr);

            scan_minor_index_nocheck_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/true>(
                (const uint32_t*)ptr,
                bucket_size_minor,
                calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                ctx->results,
                ctx->results_length,
                ctx->q_topn,
                ctx->q_shipdate);
        }


        //
        // Step 7: minor buckets, nocheck_orderdate_nocheck_shipdate
        //
        for (uint32_t bucket_id = ctx->nocheck_head_begin_bucket_id;
            bucket_id < ctx->pretopn_begin_bucket_id;
            ++bucket_id) {

            // Check "only_minor_max_expend" index (likely to skip minor buckets)
            ASSERT(g_only_minor_max_expend_start_ptr != nullptr);
            const uint32_t only_minor_max_expend = g_only_minor_max_expend_start_ptr[bucket_id];
            if (__likely(ctx->results_length > 0)) {
                const uint32_t curr_min_expend = ctx->results[0].total_expend_cent;
                if (__likely(only_minor_max_expend < curr_min_expend)) {
                    continue;
                }
            }

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t holder_begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_minor = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - holder_begin_bucket_id);
            const uint64_t bucket_size_minor = g_buckets_endoffset_minor[bucket_id] - bucket_start_offset_minor;
            if (__unlikely(bucket_size_minor == 0)) continue;

            void* const mapped_ptr = mmap(
                ptr,
                bucket_size_minor,
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_minor_fd[holder_id],
                bucket_start_offset_minor);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == ptr);

            scan_minor_index_nocheck_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/false>(
                (const uint32_t*)ptr,
                bucket_size_minor,
                calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                ctx->results,
                ctx->results_length,
                ctx->q_topn,
                ctx->q_shipdate);

            //INFO("query #%u: <2.1> nocheck_orderdate, nocheck_shipdate bucket %u", query_id, bucket_id);
        }


        //
        // Step 8: minor buckets, nocheck_orderdate_nocheck_shipdate
        //
        for (uint32_t bucket_id = ctx->nocheck_tail_begin_bucket_id;
            bucket_id < ctx->only_check_orderdate_begin_bucket_id;
            ++bucket_id) {

            // Check "only_minor_max_expend" index (likely to skip minor buckets)
            ASSERT(g_only_minor_max_expend_start_ptr != nullptr);
            const uint32_t only_minor_max_expend = g_only_minor_max_expend_start_ptr[bucket_id];
            if (__likely(ctx->results_length > 0)) {
                const uint32_t curr_min_expend = ctx->results[0].total_expend_cent;
                if (__likely(only_minor_max_expend < curr_min_expend)) {
                    continue;
                }
            }

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t holder_begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_minor = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - holder_begin_bucket_id);
            const uint64_t bucket_size_minor = g_buckets_endoffset_minor[bucket_id] - bucket_start_offset_minor;
            if (__unlikely(bucket_size_minor == 0)) continue;

            void* const mapped_ptr = mmap(
                ptr,
                bucket_size_minor,
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_minor_fd[holder_id],
                bucket_start_offset_minor);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == ptr);

            scan_minor_index_nocheck_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/false>(
                (const uint32_t*)ptr,
                bucket_size_minor,
                calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                ctx->results,
                ctx->results_length,
                ctx->q_topn,
                ctx->q_shipdate);

            //INFO("query #%u: <2.2> nocheck_orderdate, nocheck_shipdate bucket %u", query_id, bucket_id);
        }


        //
        // Step 9: minor buckets, check_orderdate_maybe_check_shipdate
        //
        for (uint32_t bucket_id = ctx->only_check_orderdate_begin_bucket_id;
            bucket_id < ctx->only_check_orderdate_end_bucket_id;
            ++bucket_id) {

            // Check "only_minor_max_expend" index (likely to skip minor buckets)
            ASSERT(g_only_minor_max_expend_start_ptr != nullptr);
            const uint32_t only_minor_max_expend = g_only_minor_max_expend_start_ptr[bucket_id];
            if (__likely(ctx->results_length > 0)) {
                const uint32_t curr_min_expend = ctx->results[0].total_expend_cent;
                if (__likely(only_minor_max_expend < curr_min_expend)) {
                    continue;
                }
            }

            const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
            ASSERT(holder_id < CONFIG_INDEX_HOLDER_COUNT);
            const uint32_t holder_begin_bucket_id = holder_id * g_shared->buckets_per_holder;

            const uintptr_t bucket_start_offset_minor = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_MINOR * (bucket_id - holder_begin_bucket_id);
            const uint64_t bucket_size_minor = g_buckets_endoffset_minor[bucket_id] - bucket_start_offset_minor;
            if (__unlikely(bucket_size_minor == 0)) continue;

            void* const mapped_ptr = mmap(
                ptr,
                bucket_size_minor,
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                g_holder_files_minor_fd[holder_id],
                bucket_start_offset_minor);
            CHECK(mapped_ptr != MAP_FAILED, "mmap() failed. errno: %d (%s)", errno, strerror(errno));
            ASSERT(mapped_ptr == ptr);

            const date_t bucket_base_orderdate = calc_bucket_base_orderdate_by_bucket_id(bucket_id);
            if (ctx->q_shipdate >= bucket_base_orderdate) {
                scan_minor_index_check_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/true>(
                    (const uint32_t*)ptr,
                    bucket_size_minor,
                    calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                    ctx->q_orderdate,
                    ctx->results,
                    ctx->results_length,
                    ctx->q_topn,
                    ctx->q_shipdate);
            }
            else {
                scan_minor_index_check_orderdate_maybe_check_shipdate</*_CheckShipdateDiff*/false>(
                    (const uint32_t*)ptr,
                    bucket_size_minor,
                    calc_bucket_base_orderdate_by_bucket_id(bucket_id),
                    ctx->q_orderdate,
                    ctx->results,
                    ctx->results_length,
                    ctx->q_topn,
                    ctx->q_shipdate);
            }
        }


        //
        // Print this query
        //
        std::sort(ctx->results, ctx->results + ctx->results_length, std::greater<>());

        generate_query_output(*ctx);

        ctx->done.mark_done();
    }


    //
    // Print queries
    //
    ASSERT(g_id == tid);
    if (tid == 0) {
        g_output_write_thread.join();
    }
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
    // Open holder files & endoffset file
    //
    {
        for (uint32_t holder_id = 0; holder_id < CONFIG_INDEX_HOLDER_COUNT; ++holder_id) {
            char filename[32];

            snprintf(filename, std::size(filename), "holder_major_%04u", holder_id);
            g_holder_files_major_fd[holder_id] = C_CALL(openat(
                g_index_directory_fd,
                filename,
                O_RDONLY | O_CLOEXEC));

            snprintf(filename, std::size(filename), "holder_minor_%04u", holder_id);
            g_holder_files_minor_fd[holder_id] = C_CALL(openat(
                g_index_directory_fd,
                filename,
                O_RDONLY | O_CLOEXEC));
        }

        __openat_file_read(
            g_index_directory_fd,
            "endoffset_major",
            &g_endoffset_file_major);
        
        __openat_file_read(
            g_index_directory_fd,
            "endoffset_minor",
            &g_endoffset_file_minor);
    }


    //
    // Open pretopn file & pretopn_count
    //
    {
        __openat_file_read(
            g_index_directory_fd,
            "pretopn",
            &g_pretopn_file);
        ASSERT(g_pretopn_file.file_size == sizeof(uint64_t) * CONFIG_EXPECT_MAX_TOPN * g_shared->total_plates);

        __openat_file_read(
            g_index_directory_fd,
            "pretopn_count",
            &g_pretopn_count_file);
        ASSERT(g_pretopn_count_file.file_size == sizeof(uint32_t) * g_shared->total_plates);

        __openat_file_read(
            g_index_directory_fd,
            "only_minor_max_expend",
            &g_only_minor_max_expend_file);
        ASSERT(g_only_minor_max_expend_file.file_size == sizeof(uint32_t) * g_shared->total_buckets);
    }


    //
    // Parse queries
    //
    {
        parse_queries();
    }


    //
    // Map pretopn_count file to memory
    //
    {
        ASSERT(g_pretopn_count_file.file_size > 0);
        ASSERT(g_pretopn_count_file.fd > 0);
        g_pretopn_count_start_ptr = (uint32_t*)my_mmap(
            g_pretopn_count_file.file_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE,
            g_pretopn_count_file.fd,
            0);
        INFO("[%u] g_pretopn_count_start_ptr: %p", g_id, g_pretopn_count_start_ptr);

        ASSERT(g_only_minor_max_expend_file.file_size > 0);
        ASSERT(g_only_minor_max_expend_file.fd > 0);
        g_only_minor_max_expend_start_ptr = (uint32_t*)my_mmap(
            g_only_minor_max_expend_file.file_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE,
            g_only_minor_max_expend_file.fd,
            0);
        INFO("[%u] g_only_minor_max_expend_start_ptr: %p", g_id, g_only_minor_max_expend_start_ptr);
    }


    //
    // Map endoffset file to memory
    //
    {
        ASSERT(g_endoffset_file_major.file_size == sizeof(uint64_t) * g_shared->total_buckets,
            "Expect: %lu = 8 * %u", g_endoffset_file_major.file_size, g_shared->total_buckets);
        g_buckets_endoffset_major = (uint64_t*)my_mmap(
            g_endoffset_file_major.file_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE,
            g_endoffset_file_major.fd,
            0);
        DEBUG("g_buckets_endoffset_major: %p", g_buckets_endoffset_major);

        ASSERT(g_endoffset_file_minor.file_size == sizeof(uint64_t) * g_shared->total_buckets);
        g_buckets_endoffset_minor = (uint64_t*)my_mmap(
            g_endoffset_file_minor.file_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE,
            g_endoffset_file_minor.fd,
            0);
        DEBUG("g_buckets_endoffset_minor: %p", g_buckets_endoffset_minor);
    }
}


void use_index_initialize_after_fork() noexcept
{
    // Parse query range
    {
        ASSERT(g_query_contexts != nullptr);

        parse_query_scan_range();
    }

    //
    // Map pretopn file
    //
    {
        ASSERT(g_pretopn_file.file_size > 0);
        ASSERT(g_pretopn_file.fd > 0);
        g_pretopn_start_ptr = (uint64_t*)my_mmap(
            g_pretopn_file.file_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE,
            g_pretopn_file.fd,
            0);
        INFO("[%u] g_pretopn_start_ptr: %p", g_id, g_pretopn_start_ptr);
    }


    //
    // Create g_output_write_thread if g_id == 0
    //
    if (g_id == 0) {
        g_output_write_thread = std::thread([]() {

            for (uint32_t query_id = 0; query_id < g_query_count; ++query_id) {
                query_context_t* const ctx = g_query_contexts[query_id];
                ctx->done.wait_done();

                ASSERT(ctx->output_length > 0);  // at least headers
                size_t total_cnt = 0;
                while (total_cnt < ctx->output_length) {
                    const size_t cnt = C_CALL(write(
                        STDOUT_FILENO,
                        ctx->output + total_cnt,
                        sizeof(char) * (ctx->output_length - total_cnt)));
                    CHECK(cnt > 0, "write() failed. errno = %d (%s)", errno, strerror(errno));
                    total_cnt += cnt;
                }
                CHECK(total_cnt == ctx->output_length);
            }
        });
    }
}

