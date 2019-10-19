#if !defined(_BDCI19_SYNC_BARRIER_H_INCLUDED_)
#define _BDCI19_SYNC_BARRIER_H_INCLUDED_

class sync_barrier
{
public:
    FORCEINLINE void init(const uint32_t thread_count) noexcept
    {
        ASSERT(thread_count > 0, "sync_barrier init() with thread_count == 0");
        _thread_count = thread_count;

        PTHREAD_CALL(pthread_barrier_init(&_barrier, nullptr, thread_count));
    }

    FORCEINLINE void sync() noexcept
    {
        ASSERT(_thread_count > 0, "sync_barrier init() not called?");

        [[maybe_unused]] const int ret = pthread_barrier_wait(&_barrier);
        ASSERT(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD, "pthread_barrier_wait() failed");
    }

    template<typename TFn, typename... TArgs>
    FORCEINLINE void sync_and_run_once(TFn fn, TArgs&&... args) noexcept
    {
        ASSERT(_thread_count > 0, "sync_barrier init() not called?");

        const int ret = pthread_barrier_wait(&_barrier);
        ASSERT(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD, "pthread_barrier_wait() failed");
        if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
            fn(std::forward<TArgs>(args)...);
        }
    }

    template<typename TFn, typename... TArgs>
    FORCEINLINE void run_once_and_sync(TFn fn, TArgs&&... args) noexcept
    {
        ASSERT(_thread_count > 0, "sync_barrier init() not called?");

        if (_run_once_and_sync_counter++ % _thread_count == 0) {
            fn(std::forward<TArgs>(args)...);
        }
        
        [[maybe_unused]] const int ret = pthread_barrier_wait(&_barrier);
        ASSERT(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD, "pthread_barrier_wait() failed");
    }

    ~sync_barrier() noexcept
    {
        if (_thread_count > 0) {
            PTHREAD_CALL(pthread_barrier_destroy(&_barrier));
            _thread_count = 0;
        }
    }

    [[nodiscard]]
    FORCEINLINE uint32_t thread_count() const noexcept
    {
        return _thread_count;
    }

private:
    std::atomic_uint64_t _run_once_and_sync_counter { 0 };
    uint32_t _thread_count = 0;
    pthread_barrier_t _barrier { };
};

#endif  // !defined(_BDCI19_SYNC_BARRIER_H_INCLUDED_)
