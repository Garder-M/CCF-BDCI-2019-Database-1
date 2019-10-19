#if !defined(_BDCI19_MACROS_H_INCLUDED_)
#define _BDCI19_MACROS_H_INCLUDED_


#define EMPTY                       /* empty place holder */
#define JUST(...)                   __VA_ARGS__
#define VA_CAR(_X_, ...)            _X_
#define COMMA_VA_CDR(_, ...)        , ##__VA_ARGS__


#define FORCEINLINE                 inline __attribute__((always_inline))

#define LIKELY(...)                 (__builtin_expect((__VA_ARGS__), 1))
#define UNLIKELY(...)               (__builtin_expect((__VA_ARGS__), 0))

#define DISABLE_COPY_MOVE_CONSTRUCTOR(_Class_) \
    _Class_(const _Class_&) noexcept = delete; \
    _Class_(_Class_&&) noexcept = delete; \
    _Class_& operator =(const _Class_&) noexcept = delete; \
    _Class_& operator =(_Class_&&) noexcept = delete



inline std::chrono::steady_clock::time_point __program_startup_time = std::chrono::steady_clock::now();

#define __LOG_INTERNAL(_Level_, _Msg_, ...) \
    do { \
        const auto _now_ = std::chrono::steady_clock::now(); \
        const uint64_t _elapsed_usec_ = (uint64_t)std::chrono::duration<double, std::micro>(_now_ - __program_startup_time).count(); \
        \
        fprintf(stderr, "\033[32;1m[%02d:%02d.%03d,%03d]\033[0m\033[34;1m %-5s [%s:%d]\033[0m " _Msg_ "\n", \
            (int)(_elapsed_usec_ / 60000000), (int)((_elapsed_usec_ / 1000000) % 60), \
            (int)(_elapsed_usec_ % 1000000) / 1000, (int)(_elapsed_usec_ % 1000), \
            (_Level_), \
            (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__), \
            __LINE__, \
            ##__VA_ARGS__); \
        fflush(stderr); \
    } while(false)


#if ENABLE_LOGGING_DEBUG
#define DEBUG(_Msg_, ...)       __LOG_INTERNAL("DEBUG", _Msg_, ##__VA_ARGS__)
#else
#define DEBUG(_Msg_, ...)
#endif

#if ENABLE_LOGGING_INFO
#define INFO(_Msg_, ...)        __LOG_INTERNAL("INFO", _Msg_, ##__VA_ARGS__)
#else
#define INFO(_Msg_, ...)
#endif


#define PANIC(_Msg_, ...) \
    do { \
        __LOG_INTERNAL("PANIC", _Msg_, ##__VA_ARGS__); \
        fflush(stderr); \
        std::abort(); \
    } while(false)


#if ENABLE_ASSERTION
#define ASSERT(_What_, ...) \
    do { \
        if (UNLIKELY(!(_What_))) { \
            PANIC("ASSERT(%s) failed. " VA_CAR("" __VA_ARGS__), #_What_ COMMA_VA_CDR("" __VA_ARGS__)); \
        } \
    } while(false)
#else
#define ASSERT(_What_, ...)
#endif


#define CHECK(_What_, ...) \
    do { \
        if (UNLIKELY(!(_What_))) { \
            PANIC("CHECK(%s) failed. " VA_CAR("" __VA_ARGS__), #_What_ COMMA_VA_CDR("" __VA_ARGS__)); \
        } \
    } while(false)


#define C_CALL(_Call_) \
    (([](auto const _ret_) { \
        static_assert(std::is_signed_v<decltype(_ret_)>, "Should return signed type"); \
        if (UNLIKELY(_ret_ < 0)) { \
            PANIC("%s failed with %ld. errno: %d (%s)", #_Call_, (int64_t)_ret_, errno, strerror(errno)); \
        } \
        return _ret_; \
    })(_Call_))

#define PTHREAD_CALL(_Call_) \
    do { \
        const int _err_ = (_Call_); \
        if (UNLIKELY(_err_ != 0)) { \
            PANIC("%s failed with %d (%s)", #_Call_, _err_, strerror(_err_)); \
        } \
    } while(false)

#define PTHREAD_CALL_NO_PANIC(_Call_) \
    do { \
        const int _err_ = (_Call_); \
        if (UNLIKELY(_err_ != 0)) { \
            INFO("%s failed with %d (%s)", #_Call_, _err_, strerror(_err_)); \
        } \
    } while(false)

#endif  // !defined(_BDCI19_MACROS_H_INCLUDED_)
