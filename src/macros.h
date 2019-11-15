#if !defined(_BDCI19_MACROS_H_INCLUDED_)
#define _BDCI19_MACROS_H_INCLUDED_


#define EMPTY                       /* empty place holder */
#define JUST(...)                   __VA_ARGS__
#define VA_CAR(_X_, ...)            _X_
#define COMMA_VA_CDR(_, ...)        , ##__VA_ARGS__

#define DISABLE_COPY_MOVE_CONSTRUCTOR(_Class_) \
    _Class_(const _Class_&) noexcept = delete; \
    _Class_(_Class_&&) noexcept = delete; \
    _Class_& operator =(const _Class_&) noexcept = delete; \
    _Class_& operator =(_Class_&&) noexcept = delete

#define ONLY_DEFAULT_CONSTRUCTOR(_Class_) \
    DISABLE_COPY_MOVE_CONSTRUCTOR(_Class_); \
    _Class_() noexcept = default


#if !defined(__always_inline)
#define __always_inline             __inline __attribute__((__always_inline__))
#endif  // !defined(__always_inline)


#if !defined(__likely)
#define __likely(...)               (__builtin_expect((__VA_ARGS__), 1))
#endif  // !defined(__likely)

#if !defined(__unlikely)
#define __unlikely(...)             (__builtin_expect((__VA_ARGS__), 0))
#endif  // !defined(__unlikely)


#define __div_up(_A_, _B_)          (((_A_) + (_B_) - 1) / (_B_))
#define __div_down(_A_, _B_)        ((_A_) / (_B_))

#define __align_up(_A_, _B_)        (((_A_) + (_B_) - 1) / (_B_) * (_B_))
#define __align_down(_A_, _B_)      ((_A_) / (_B_) * (_B_))


#define __field_offset(_Type_, _F_) ((uintptr_t)&((_Type_*)nullptr)->_F_)


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


#if ENABLE_LOGGING_TRACE
#define TRACE(_Msg_, ...)       __LOG_INTERNAL("TRACE", _Msg_, ##__VA_ARGS__)
#else
#define TRACE(_Msg_, ...)
#endif

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

#define WARN(_Msg_, ...) \
    do { \
        __LOG_INTERNAL("WARN", _Msg_, ##__VA_ARGS__); \
        fflush(stderr); \
    } while(false)

#define PANIC(_Msg_, ...) \
    do { \
        __LOG_INTERNAL("PANIC", _Msg_, ##__VA_ARGS__); \
        fflush(stderr); \
        std::abort(); \
    } while(false)



#if ENABLE_ASSERTION
#define ASSERT(_What_, ...) \
    do { \
        if (__unlikely(!(_What_))) { \
            PANIC("ASSERT(%s) failed. " VA_CAR("" __VA_ARGS__), #_What_ COMMA_VA_CDR("" __VA_ARGS__)); \
        } \
    } while(false)
#else  // !ENABLE_ASSERTION
#if defined(__INTEL_COMPILER)
#define ASSERT(_What_, ...) __assume((_What_))
#else  // !defined(__INTEL_COMPILER)
#define ASSERT(_What_, ...) \
    do { \
        if (!(_What_)) { \
            __builtin_unreachable(); \
        } \
    } while(false)
#endif  // defined(__INTEL_COMPILER)
#endif  // ENABLE_ASSERTION


#define CHECK(_What_, ...) \
    do { \
        if (__unlikely(!(_What_))) { \
            PANIC("CHECK(%s) failed. " VA_CAR("" __VA_ARGS__), #_What_ COMMA_VA_CDR("" __VA_ARGS__)); \
        } \
    } while(false)


#define C_CALL(_Call_) \
    (([](auto const _ret_) { \
        static_assert(std::is_signed_v<decltype(_ret_)>, "Should return signed type"); \
        if (__unlikely(_ret_ < 0)) { \
            PANIC("%s failed with %ld. errno = %d (%s)", #_Call_, (int64_t)_ret_, errno, strerror(errno)); \
        } \
        return _ret_; \
    })(_Call_))

#define C_CALL_NO_PANIC(_Call_) \
    (([](auto const _ret_) { \
        static_assert(std::is_signed_v<decltype(_ret_)>, "Should return signed type"); \
        if (__unlikely(_ret_ < 0)) { \
            WARN("%s failed with %ld. errno = %d (%s)", #_Call_, (int64_t)_ret_, errno, strerror(errno)); \
        } \
        return _ret_; \
    })(_Call_))


#define PTHREAD_CALL(_Call_) \
    do { \
        const int _err_ = (_Call_); \
        if (__unlikely(_err_ != 0)) { \
            PANIC("%s failed with %d (%s)", #_Call_, _err_, strerror(_err_)); \
        } \
    } while(false)

#define PTHREAD_CALL_NO_PANIC(_Call_) \
    (([](const int _err_) { \
        if (__unlikely(_err_ != 0)) { \
            WARN("%s failed with %d (%s)", #_Call_, _err_, strerror(_err_)); \
        } \
        return _err_; \
    })(_Call_))\




#endif  // !defined(_BDCI19_MACROS_H_INCLUDED_)
