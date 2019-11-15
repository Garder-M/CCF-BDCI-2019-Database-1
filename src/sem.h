#if !defined(_BDCI19_SEMEPHORE_H_INCLUDED_)
#define _BDCI19_SEMEPHORE_H_INCLUDED_

#if !defined(__SEMAPHORE_DEBUG__)
#define __SEMAPHORE_DEBUG__ 0
#endif  // !defined(__SEMAPHORE_DEBUG__)


struct process_private;
struct process_shared;

template<typename TType>
class semaphore
{
private:
    static_assert(
        std::is_same<TType, process_private>::value ||
        std::is_same<TType, process_shared>::value);
    static constexpr const uint32_t SEM_FUTEX_PRIVATE_FLAG = (std::is_same<TType, process_private>::value) ? FUTEX_PRIVATE_FLAG : 0;

    static constexpr const uint64_t SEM_VALUE_MASK = 0x00000000FFFFFFFFULL;
    static constexpr const uint32_t SEM_NWAITERS_SHIFT = 32;

private:
    template<bool _DefinitiveResult>
    [[nodiscard]]
    __always_inline
    bool try_sem_wait_fast() noexcept
    {
        uint64_t v = _value.load(std::memory_order_relaxed);
        do {
#if __SEMAPHORE_DEBUG__
            ++__sem_wait_fast_loop_count;
#endif
            if ((v & SEM_VALUE_MASK) == 0) {
                break;
            }
            if (_value.compare_exchange_weak(v, v - 1, std::memory_order_acquire, std::memory_order_relaxed)) {
                return true;
            }
        } while(_DefinitiveResult);
        return false;
    }

    __always_inline
    void sem_wait_slow() noexcept
    {
        // Add a waiter.
        // Relaxed MO is sufficient because we can rely on the
        // ordering provided by the RMW operations we use
        uint64_t v = _value.fetch_add((uint64_t)1 << SEM_NWAITERS_SHIFT, std::memory_order_relaxed);

        // Wait for a token to be available.  Retry until we can grab one.  */
        while (true) {
            // If there is no token available, sleep until there is
            if ((v & SEM_VALUE_MASK) == 0) {
                const int ret = __futex_wait<SEM_FUTEX_PRIVATE_FLAG>((int*)&_value, 0);
                // A futex return value of 0 or EAGAIN is due to a real or spurious
                // wake-up, or due to a change in the number of tokens.
                // We retry in these cases.
                // We simply don't allow other errors
                CHECK(ret == 0 || errno == EAGAIN, "Unexpected errno = %d (%s)", errno, strerror(errno));

                // Relaxed MO is sufficient; see below
                v = _value.load(std::memory_order_relaxed);

#if __SEMAPHORE_DEBUG__
                ++__futex_wait_count;
#endif
            }
            else {
                // Try to grab both a token and stop being a waiter.  We need
                // acquire MO so this synchronizes with all token providers (i.e.,
                // the RMW operation we read from or all those before it in
                // modification order; also see sem_post).  On the failure path,
                // relaxed MO is sufficient because we only eventually need the
                // up-to-date value; the futex_wait or the CAS perform the real work.
                const uint64_t new_value = v - 1 - ((uint64_t)1 << SEM_NWAITERS_SHIFT);
                if (_value.compare_exchange_weak(v, new_value, std::memory_order_acquire, std::memory_order_relaxed)) {
                    break;
                }
            }
        }
    }

public:
    __always_inline
    explicit semaphore(const int32_t init_value = 0) noexcept
        : _value((uint64_t)init_value)
    { }

    __always_inline
    void reinit(const int32_t init_value = 0) noexcept
    {
        // NOTE:
        //   reinit() must be called when no threads are using (waiting or posting) the semaphore.
        //   Otherwise, deadlock is almost surely the fate!
        _value.store((uint64_t)init_value, std::memory_order_seq_cst);
    }

    [[nodiscard]]
    __always_inline
    uint32_t approx_value() const noexcept
    {
        return (uint32_t)_value.load(std::memory_order_relaxed);
    }

    __always_inline
    void post(const uint32_t many = 1) noexcept
    {
        // Add a token to the semaphore.  We use release MO to make sure that a
        // thread acquiring this token synchronizes with us and other threads that
        // added tokens before (the release sequence includes atomic RMW operations
        // by other threads)

        // Use atomic fetch_add to make it scale better than a CAS loop?
        const uint64_t v = _value.fetch_add(many, std::memory_order_release);
        ASSERT((v & SEM_VALUE_MASK) <= UINT32_MAX - many);

        // If there is any potentially blocked waiter, wake `many` of them
        if ((v >> SEM_NWAITERS_SHIFT) > 0) {
            const int err = __futex_wake<SEM_FUTEX_PRIVATE_FLAG>((int*)&_value, many);
            CHECK(err >= 0);

#if __SEMAPHORE_DEBUG__
            ++__futex_wake_count;
#endif  // __SEMAPHORE_DEBUG__
        }
    }

    template<bool _HighContention = false>
    __always_inline
    void wait() noexcept
    {
        if (!try_sem_wait_fast</*_DefinitiveResult*/!_HighContention>()) {
            sem_wait_slow();
        }
    }

    __always_inline
    bool try_wait() noexcept
    {
        return try_sem_wait_fast</*_DefinitiveResult*/true>();
    }


private:
    std::atomic_uint64_t _value;
    static_assert(sizeof(_value) == 8);

public:
    __always_inline
    void debug_print_syscall_count([[maybe_unused]] const char* const title = "sem") noexcept
    {
#if __SEMAPHORE_DEBUG__
        DEBUG("[%s] semaphore syscall: futex_wait=%lu, futex_wake=%lu, sem_wait_fast_loop=%lu",
              title, __futex_wait_count.load(), __futex_wake_count.load(), __sem_wait_fast_loop_count.load());
#endif  // __SEMAPHORE_DEBUG__
    }

#if __SEMAPHORE_DEBUG__
    std::atomic_uint64_t __futex_wait_count { 0 };
    std::atomic_uint64_t __futex_wake_count { 0 };
    std::atomic_uint64_t __sem_wait_fast_loop_count { 0 };
#endif  // __SEMAPHORE_DEBUG__
};


typedef semaphore<process_private> process_private_semaphore;
typedef semaphore<process_shared> process_shared_semaphore;




template<typename TType>
class mutex
{
    static_assert(
            std::is_same<TType, process_private>::value ||
            std::is_same<TType, process_shared>::value);

public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(mutex);
    mutex() noexcept = default;

    __always_inline
    void lock() noexcept
    {
        _sem.wait();
    }

    __always_inline
    void unlock() noexcept
    {
        _sem.post(1);
    }

    [[nodiscard]]
    __always_inline
    bool try_lock() noexcept
    {
        return _sem.try_wait();
    }

private:
    semaphore<TType> _sem { 1 };
};

typedef mutex<process_private> process_private_mutex;
typedef mutex<process_shared> process_shared_mutex;



//==============================================================================
// Unittests
//==============================================================================
__always_inline
void test_semaphore()
{
    constexpr const uint32_t N = 10000000;

    INFO("%s() starts", __FUNCTION__);
    process_private_semaphore sem { 0 };

    std::atomic_uint64_t counter1 = 0;
    std::atomic_uint64_t counter2 = 0;

    std::thread threads1[8];
    for (std::thread& thr : threads1) {
        thr = std::thread([&]() {
            while (counter2++ < N) {
                sem.wait</*_HighContention*/true>();
            }
        });
    }

    std::thread threads2[8];
    for (std::thread& thr : threads2) {
        thr = std::thread([&]() {
            std::this_thread::yield();
            while (counter1.fetch_add(1) < N) {
                sem.post(1);
            }
        });
    }

    for (std::thread& thr : threads1) {
        thr.join();
    }
    for (std::thread& thr : threads2) {
        thr.join();
    }

    CHECK(!sem.try_wait());
    CHECK(counter2 == N + std::size(threads1));
    sem.debug_print_syscall_count(__FUNCTION__);

    INFO("%s() done", __FUNCTION__);
}


#endif  // !defined(_BDCI19_SEMEPHORE_H_INCLUDED_)
