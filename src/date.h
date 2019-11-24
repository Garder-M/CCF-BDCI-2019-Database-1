#if !defined(_BDCI19_DATE_H_INCLUDED_)
#define _BDCI19_DATE_H_INCLUDED_

typedef int16_t date_t;
static_assert(std::is_signed_v<date_t>, "date_t must be a signed type!");

typedef std::tuple<uint32_t, uint32_t, uint32_t> year_month_day_tuple;


__always_inline
constexpr uint32_t __date_u32_from_ymd(
    /*in*/ uint32_t year,
    /*in*/ uint32_t month,
    /*in*/ const uint32_t day) noexcept
{
#if !defined(__INTEL_COMPILER)
    ASSERT(year >= 1, "Unexpected year: %u", year);
    ASSERT(month > 0 && month <= 12, "Unexpected month: %u", month);
    ASSERT(day > 0 && day <= 31, "Unexpected day: %u", day);
#endif

    month = (month + 9) % 12;
    year = year - month / 10;
    return 365 * year + year / 4 - year / 100 + year / 400 + (month * 306 + 5) / 10 + (day - 1);
}

constexpr const uint32_t __min_table_year = 1992;
constexpr const uint32_t __max_table_year = 1998;
constexpr const uint32_t __min_table_date_u32 = __date_u32_from_ymd(__min_table_year, 1, 1);
constexpr const uint32_t __max_table_date_u32 = __date_u32_from_ymd(__max_table_year, 12, 31);
static_assert(__max_table_date_u32 > __min_table_date_u32);
static_assert(__max_table_date_u32 - __min_table_date_u32 + 1 < INT16_MAX);

template<bool _DontCheck>
__always_inline
constexpr date_t date_from_ymd(
    /*in*/ const uint32_t year,
    /*in*/ const uint32_t month,
    /*in*/ const uint32_t day) noexcept;

template<>
__always_inline
constexpr date_t date_from_ymd</*_DontCheck*/false>(
    /*in*/ const uint32_t year,
    /*in*/ const uint32_t month,
    /*in*/ const uint32_t day) noexcept
{
#if !defined(__INTEL_COMPILER)
    ASSERT(month > 0 && month <= 12, "Unexpected month: %u", month);
    ASSERT(day > 0 && day <= 31, "Unexpected day: %u", day);
#endif

    const uint32_t value = __date_u32_from_ymd(year, month, day);

    if (value < __min_table_date_u32)
        return 0;
    else if (value > __max_table_date_u32)
        return (__max_table_date_u32 - __min_table_date_u32 + 1 + 1);

    return value - __min_table_date_u32 + 1;
}

//template<bool _DontCheck>
//__always_inline
//constexpr date_t date_from_ymd(
//    /*in*/ const uint32_t year,
//    /*in*/ const uint32_t month,
//    /*in*/ const uint32_t day) noexcept
//{
//#if !defined(__INTEL_COMPILER)
//    ASSERT(month > 0 && month <= 12, "Unexpected month: %u", month);
//    ASSERT(day > 0 && day <= 31, "Unexpected day: %u", day);
//#endif
//
//    const uint32_t value = __date_u32_from_ymd(year, month, day);
//
//    if constexpr (_DontCheck) {
//#if !defined(__INTEL_COMPILER)
//        ASSERT(value >= __min_table_date_u32);
//        ASSERT(value <= __max_table_date_u32);
//#endif
//    }
//    else {
//        if (value < __min_table_date_u32)
//            return 0;
//        else if (value > __max_table_date_u32)
//            return (__max_table_date_u32 - __min_table_date_u32 + 1 + 1);
//    }
//
//    return value - __min_table_date_u32 + 1;
//}

constexpr const date_t MIN_TABLE_DATE = (date_from_ymd<false>(__min_table_year, 1, 1));
constexpr const date_t MAX_TABLE_DATE = (date_from_ymd<false>(__max_table_year, 12, 31));
static_assert(MIN_TABLE_DATE == 1);
static_assert(MAX_TABLE_DATE == 2557);


template<bool _DontCheck>
__always_inline
constexpr date_t __date_from_string(const char (&s)[11]) noexcept;

template<>
__always_inline
constexpr date_t __date_from_string</*_DontCheck*/false>(const char (&s)[11]) noexcept
{
    // Format of `s`: yyyy-MM-dd
#if !defined(__INTEL_COMPILER)
    ASSERT(s[4] == '-', "Expect '-' char at date s[4] but got: %c (0x%02x)", s[4], s[4]);
    ASSERT(s[7] == '-', "Expect '-' char at date s[7] but got: %c (0x%02x)", s[7], s[7]);
    ASSERT(s[10] == '\n' || s[10] == '\0' || s[10] == '|', "Unexpected char after date: %c (0x%02x)", s[10], s[10]);
#endif

    const uint32_t year = (s[0] - '0') * 1000u + (s[1] - '0') * 100u + (s[2] - '0') * 10u + (s[3] - '0');
    const uint32_t month = (s[5] - '0') * 10u + (s[6] - '0');
    const uint32_t day = (s[8] - '0') * 10u + (s[9] - '0');

    return date_from_ymd<false>(year, month, day);
}

template<>
__always_inline
constexpr date_t __date_from_string</*_DontCheck*/true>(const char (&s)[11]) noexcept
{
    // Format of `s`: yyyy-MM-dd
#if !defined(__INTEL_COMPILER)
    ASSERT(s[0] == '1');
    ASSERT(s[1] == '9');
    ASSERT(s[2] == '9');
    ASSERT(s[3] >= '2' && s[3] <= '8');
    ASSERT(s[4] == '-', "Expect '-' char at date s[4] but got: %c (0x%02x)", s[4], s[4]);
    ASSERT(s[5] >= '0' && s[5] <= '1');
    ASSERT(s[6] >= '0' && s[6] <= '9');
    ASSERT(s[7] == '-', "Expect '-' char at date s[7] but got: %c (0x%02x)", s[7], s[7]);
    ASSERT(s[8] >= '0' && s[8] <= '3');
    ASSERT(s[9] >= '0' && s[9] <= '9');
    ASSERT(s[10] == '\n' || s[10] == '\0' || s[10] == '|', "Unexpected char after date: %c (0x%02x)", s[10], s[10]);
#endif

    uint32_t month = (s[5] - '0') * 10u + (s[6] - '0');
    const uint32_t day = (s[8] - '0') * 10u + (s[9] - '0');

    static_assert(__min_table_date_u32 == 727503);

    const uint32_t tmp = (s[3] - '2' - (month < 3));
    month = (month + 9) % 12;
    const int32_t value = (month * 306 + 5) / 10 + tmp * 365 + tmp / 4 + day + 60;

    return value;
}


template<bool _DontCheck>
__always_inline
date_t date_from_string(const char* const s) noexcept
{
    return __date_from_string<_DontCheck>(*static_cast<const char(*)[11]>((void*)s));
}

__always_inline
constexpr year_month_day_tuple date_get_ymd(const date_t date) noexcept
{
#if !defined(__INTEL_COMPILER)
    ASSERT(date >= MIN_TABLE_DATE, "date should >= 1992-01-01");
    ASSERT(date <= MAX_TABLE_DATE, "date should <= 1998-12-31");
#endif

    uint32_t value = (uint64_t)(date) + __min_table_date_u32 - 1;
    uint32_t year = (10000 * (uint64_t)value + 14780) / 3652425;
    int32_t ddd = value - (365 * year + year / 4 - year / 100 + year / 400);
    if (ddd < 0) {
        year = year - 1;
        ddd = value - (365 * year + year / 4 - year / 100 + year / 400);
    }
    uint32_t mi = (100 * ddd + 52) / 3060;
    year = year + (mi + 2) / 12;
    uint32_t month = (mi + 2) % 12 + 1;
    uint32_t day = ddd - (mi * 306 + 5) / 10 + 1;
    return { year, month, day };
}

__always_inline
constexpr uint32_t date_get_year(const date_t date) noexcept
{
    return std::get<0>(date_get_ymd(date));
}

__always_inline
constexpr uint32_t date_get_month(const date_t date) noexcept
{
    return std::get<1>(date_get_ymd(date));
}

__always_inline
constexpr uint32_t date_get_day(const date_t date) noexcept
{
    return std::get<2>(date_get_ymd(date));
}



//==============================================================================
// Sanity checks for date_t
//==============================================================================
static_assert(__date_from_string<false>("0001-01-01") == 0);
static_assert(__date_from_string<false>("1234-01-01") == 0);
static_assert(__date_from_string<false>("1991-12-30") == 0);
static_assert(__date_from_string<false>("1991-12-31") == 0);
static_assert(__date_from_string<false>("1992-01-01") == 1);
static_assert(__date_from_string<false>("1992-01-02") == 2);
static_assert(__date_from_string<false>("1992-01-31") == 31);
static_assert(__date_from_string<false>("1992-02-01") == 32);
static_assert(__date_from_string<false>("1992-02-29") == 60);
static_assert(__date_from_string<false>("1992-03-01") == 61);
static_assert(__date_from_string<false>("1993-01-01") == 367);
static_assert(__date_from_string<false>("1998-12-30") == 2556);
static_assert(__date_from_string<false>("1998-12-31") == 2557);
static_assert(__date_from_string<false>("1999-01-01") == 2558);
static_assert(__date_from_string<false>("1999-01-02") == 2558);
static_assert(__date_from_string<false>("2019-12-31") == 2558);
static_assert(__date_from_string<false>("9999-12-31") == 2558);

static_assert(__date_from_string<true>("1992-01-01") == 1);
static_assert(__date_from_string<true>("1992-01-02") == 2);
static_assert(__date_from_string<true>("1992-01-31") == 31);
static_assert(__date_from_string<true>("1992-02-01") == 32);
static_assert(__date_from_string<true>("1992-02-29") == 60);
static_assert(__date_from_string<true>("1992-03-01") == 61);
static_assert(__date_from_string<true>("1993-01-01") == 367);
static_assert(__date_from_string<true>("1998-12-30") == 2556);
static_assert(__date_from_string<true>("1998-12-31") == 2557);

// According to TPC-H spec v2.18.0 chap 4.2.3, shipdate = orderdate + random[1..121]
// So, 7 bit is enough to represent the difference between shipdate and orderdate
// eg. "1995-02-01" +122 days = "1995-06-03"
static_assert(__date_from_string<false>("1995-06-03") - __date_from_string<false>("1995-02-01") == 122);  // <=127



static_assert(date_get_year(date_from_ymd<false>(1992, 1, 1))   == 1992);
static_assert(date_get_year(date_from_ymd<false>(1992, 12, 31)) == 1992);
static_assert(date_get_year(date_from_ymd<false>(1993, 1, 1))   == 1993);
static_assert(date_get_year(date_from_ymd<false>(1993, 12, 31)) == 1993);
static_assert(date_get_year(date_from_ymd<false>(1997, 1, 1))   == 1997);
static_assert(date_get_year(date_from_ymd<false>(1997, 12, 31)) == 1997);
static_assert(date_get_year(date_from_ymd<false>(1998, 1, 1))   == 1998);
static_assert(date_get_year(date_from_ymd<false>(1998, 12, 31)) == 1998);

static_assert(date_get_month(date_from_ymd<false>(1992, 1, 1))   == 1);
static_assert(date_get_month(date_from_ymd<false>(1992, 12, 31)) == 12);
static_assert(date_get_month(date_from_ymd<false>(1993, 1, 1))   == 1);
static_assert(date_get_month(date_from_ymd<false>(1993, 12, 31)) == 12);
static_assert(date_get_month(date_from_ymd<false>(1997, 1, 1))   == 1);
static_assert(date_get_month(date_from_ymd<false>(1997, 12, 31)) == 12);
static_assert(date_get_month(date_from_ymd<false>(1998, 1, 1))   == 1);
static_assert(date_get_month(date_from_ymd<false>(1998, 12, 31)) == 12);

static_assert(date_get_day(date_from_ymd<false>(1992, 1, 1))   == 1);
static_assert(date_get_day(date_from_ymd<false>(1992, 12, 31)) == 31);
static_assert(date_get_day(date_from_ymd<false>(1993, 1, 1))   == 1);
static_assert(date_get_day(date_from_ymd<false>(1993, 12, 31)) == 31);
static_assert(date_get_day(date_from_ymd<false>(1997, 1, 1))   == 1);
static_assert(date_get_day(date_from_ymd<false>(1997, 12, 31)) == 31);
static_assert(date_get_day(date_from_ymd<false>(1998, 1, 1))   == 1);
static_assert(date_get_day(date_from_ymd<false>(1998, 12, 31)) == 31);


#endif  // !defined(_BDCI19_DATE_H_INCLUDED_)
