#if !defined(_BDCI19_DATE_H_INCLUDED_)
#define _BDCI19_DATE_H_INCLUDED_

typedef int16_t date_t;
static_assert(std::is_signed_v<date_t>, "date_t must be a signed type!");

typedef std::tuple<uint32_t, uint32_t, uint32_t> year_month_day_tuple;


FORCEINLINE constexpr uint32_t __date_u32_from_ymd(
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


FORCEINLINE constexpr date_t date_from_ymd(
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
        return -1;
    else if (value > __max_table_date_u32)
        return (__max_table_date_u32 - __min_table_date_u32 + 1);
    else
        return value - __min_table_date_u32;
}

constexpr const date_t MIN_TABLE_DATE = (date_from_ymd(__min_table_year, 1, 1));
constexpr const date_t MAX_TABLE_DATE = (date_from_ymd(__max_table_year, 12, 31));
static_assert(MIN_TABLE_DATE == 0);
static_assert(MAX_TABLE_DATE == 2556);


FORCEINLINE constexpr date_t __date_from_string(const char (&s)[11]) noexcept
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

    return date_from_ymd(year, month, day);
}

FORCEINLINE date_t date_from_string(const char* const s) noexcept
{
    return __date_from_string(*static_cast<const char(*)[11]>((void*)s));
}

FORCEINLINE constexpr year_month_day_tuple date_get_ymd(const date_t date) noexcept
{
#if !defined(__INTEL_COMPILER)
    ASSERT(date >= MIN_TABLE_DATE, "date should >= 1992-01-01");
    ASSERT(date <= MAX_TABLE_DATE, "date should <= 1998-12-31");
#endif

    uint32_t value = (uint64_t)(date) + __min_table_date_u32;
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

FORCEINLINE constexpr uint32_t date_get_year(const date_t date) noexcept
{
    return std::get<0>(date_get_ymd(date));
}

FORCEINLINE constexpr uint32_t date_get_month(const date_t date) noexcept
{
    return std::get<1>(date_get_ymd(date));
}

FORCEINLINE constexpr uint32_t date_get_day(const date_t date) noexcept
{
    return std::get<2>(date_get_ymd(date));
}



//==============================================================================
// Sanity checks for date_t
//==============================================================================
static_assert(__date_from_string("0001-01-01") == -1);
static_assert(__date_from_string("1234-01-01") == -1);
static_assert(__date_from_string("1991-12-30") == -1);
static_assert(__date_from_string("1991-12-31") == -1);
static_assert(__date_from_string("1992-01-01") == 0);
static_assert(__date_from_string("1992-01-02") == 1);
static_assert(__date_from_string("1992-01-31") == 30);
static_assert(__date_from_string("1992-02-01") == 31);
static_assert(__date_from_string("1992-02-29") == 59);
static_assert(__date_from_string("1992-03-01") == 60);
static_assert(__date_from_string("1993-01-01") == 366);
static_assert(__date_from_string("1998-12-30") == 2555);
static_assert(__date_from_string("1998-12-31") == 2556);
static_assert(__date_from_string("1999-01-01") == 2557);
static_assert(__date_from_string("1999-01-02") == 2557);
static_assert(__date_from_string("2019-12-31") == 2557);
static_assert(__date_from_string("9999-12-31") == 2557);

// According to TPC-H spec v2.18.0 chap 4.2.3, shipdate = orderdate + random[1..121]
// So, 7 bit is enough to represent the difference between shipdate and orderdate
// eg. "1995-02-01" +122 days = "1995-06-03"
static_assert(__date_from_string("1995-06-03") - __date_from_string("1995-02-01") == 122);  // <=127



static_assert(date_get_year(date_from_ymd(1992, 1, 1))   == 1992);
static_assert(date_get_year(date_from_ymd(1992, 12, 31)) == 1992);
static_assert(date_get_year(date_from_ymd(1993, 1, 1))   == 1993);
static_assert(date_get_year(date_from_ymd(1993, 12, 31)) == 1993);
static_assert(date_get_year(date_from_ymd(1997, 1, 1))   == 1997);
static_assert(date_get_year(date_from_ymd(1997, 12, 31)) == 1997);
static_assert(date_get_year(date_from_ymd(1998, 1, 1))   == 1998);
static_assert(date_get_year(date_from_ymd(1998, 12, 31)) == 1998);

static_assert(date_get_month(date_from_ymd(1992, 1, 1))   == 1);
static_assert(date_get_month(date_from_ymd(1992, 12, 31)) == 12);
static_assert(date_get_month(date_from_ymd(1993, 1, 1))   == 1);
static_assert(date_get_month(date_from_ymd(1993, 12, 31)) == 12);
static_assert(date_get_month(date_from_ymd(1997, 1, 1))   == 1);
static_assert(date_get_month(date_from_ymd(1997, 12, 31)) == 12);
static_assert(date_get_month(date_from_ymd(1998, 1, 1))   == 1);
static_assert(date_get_month(date_from_ymd(1998, 12, 31)) == 12);

static_assert(date_get_day(date_from_ymd(1992, 1, 1))   == 1);
static_assert(date_get_day(date_from_ymd(1992, 12, 31)) == 31);
static_assert(date_get_day(date_from_ymd(1993, 1, 1))   == 1);
static_assert(date_get_day(date_from_ymd(1993, 12, 31)) == 31);
static_assert(date_get_day(date_from_ymd(1997, 1, 1))   == 1);
static_assert(date_get_day(date_from_ymd(1997, 12, 31)) == 31);
static_assert(date_get_day(date_from_ymd(1998, 1, 1))   == 1);
static_assert(date_get_day(date_from_ymd(1998, 12, 31)) == 31);


#endif  // !defined(_BDCI19_DATE_H_INCLUDED_)
