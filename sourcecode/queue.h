#if !defined(_BDCI19_QUEUE_H_INCLUDED_)
#define _BDCI19_QUEUE_H_INCLUDED_

template<typename T, size_t _Capacity>
class bounded_bag
{
    static_assert(std::is_trivial_v<T>, "T must be trivial type");
    static_assert(_Capacity > 0);

public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(bounded_bag);

    FORCEINLINE bounded_bag() noexcept = default;

    FORCEINLINE ~bounded_bag() noexcept
    {
        if (_initialized) {
            C_CALL(sem_destroy(&_sem_pop));
        }
    }

    FORCEINLINE void init(
        /*in*/ const std::function<T(size_t)>& fn_init,
        /*in*/ const size_t init_count /*= _Capacity*/) noexcept
    {
        ASSERT(!_initialized, "init() has been called?");
        _initialized = true;

        ASSERT(init_count <= _Capacity);
        if (init_count > 0) {
            ASSERT(fn_init);
        }

        for (size_t idx = 0; idx < init_count; ++idx) {
            _items[idx] = fn_init(idx);
        }

        _head.store(0);
        _tail.store(init_count);

        C_CALL(sem_init(&_sem_pop, /*pshared*/0, init_count));
    }

    FORCEINLINE void take(/*out*/ T* item) noexcept
    {
        C_CALL(sem_wait(&_sem_pop));

        ASSERT(_head < _tail);
        *item = _items[(_head++) % _Capacity];
    }

    FORCEINLINE void return_back(/*in*/ const T& item) noexcept
    {
        {
            std::unique_lock<decltype(_lock)> lock(_lock);
            ASSERT(_head <= _tail);
            _items[(_tail++) % _Capacity] = item;
        }

        C_CALL(sem_post(&_sem_pop));
    }

private:
    bool _initialized = false;
    T _items[_Capacity];
    sem_t _sem_pop;
    std::atomic_size_t _head;
    std::atomic_size_t _tail;
#if ENABLE_QUEUE_USE_SPIN_LOCK
    spin_lock _lock { false };
#else
    std::mutex _lock { };
#endif
};


template<typename T, size_t _Capacity>
class spsc_bounded_bag
{
    static_assert(std::is_trivial_v<T>, "T must be trivial type");
    static_assert(_Capacity > 0);

public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(spsc_bounded_bag);

    FORCEINLINE spsc_bounded_bag() noexcept = default;

    FORCEINLINE ~spsc_bounded_bag() noexcept
    {
        ASSERT(_initialized);

        C_CALL(sem_destroy(&_sem_pop));
    }

    FORCEINLINE void init(
        /*in*/ const std::function<T(size_t)>& fn_init,
        /*in*/ const size_t init_count /*= _Capacity*/) noexcept
    {
        ASSERT(!_initialized, "init() has been called?");
        _initialized = true;

        ASSERT(init_count <= _Capacity);
        if (init_count > 0) {
            ASSERT(fn_init);
        }

        for (size_t idx = 0; idx < init_count; ++idx) {
            _items[idx] = fn_init(idx);
        }

        _head = 0;
        _tail = init_count;

        C_CALL(sem_init(&_sem_pop, /*pshared*/0, init_count));
    }

    FORCEINLINE void take(/*out*/ T* item) noexcept
    {
        C_CALL(sem_wait(&_sem_pop));

        ASSERT(_head < _tail);
        *item = _items[(_head++) % _Capacity];
    }

    FORCEINLINE void return_back(/*in*/ const T& item) noexcept
    {
        {
            //std::unique_lock<decltype(_lock)> lock(_lock);
            ASSERT(_head <= _tail);
            _items[(_tail++) % _Capacity] = item;
        }

        C_CALL(sem_post(&_sem_pop));
    }

private:
    bool _initialized = false;
    T _items[_Capacity];
    sem_t _sem_pop;
    volatile size_t _head;
    volatile size_t _tail;
};



template<typename T, size_t _Capacity>
class mpmc_queue
{
    static_assert(std::is_trivial_v<T>, "T must be trivial type");
    static_assert(_Capacity > 0);

public:
    FORCEINLINE mpmc_queue() noexcept
    {
        C_CALL(sem_init(&_sem, /*pshared*/0, 0));
    }

    FORCEINLINE ~mpmc_queue() noexcept
    {
        C_CALL(sem_destroy(&_sem));
    }

    [[nodiscard]]
    constexpr size_t capacity() const noexcept
    {
        return _Capacity;
    }

    FORCEINLINE void push(const T& item) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        {
            std::unique_lock<decltype(_lock)> lock(_lock);
            _items[(_tail++) % _Capacity] = item;
        }

        C_CALL(sem_post(&_sem));
    }

    template<typename... Args>
    FORCEINLINE void emplace(Args&&... args) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        {
            std::unique_lock<decltype(_lock)> lock(_lock);
            new (&_items[(_tail++) % _Capacity]) T(std::forward<Args>(args)...);
        }

        C_CALL(sem_post(&_sem));
    }

    FORCEINLINE bool pop(/*out*/ T* item) noexcept
    {
        C_CALL(sem_wait(&_sem));

        // NOTE:
        //  We explicitly allow calling pop() before calling init()
        //  Thus, this assertion should go after sem_wait()
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called? head=%lu, tail=%lu",
            _head.load(), _tail.load());

        const size_t head = _head++;
        const size_t tail = _tail.load();
        if (UNLIKELY(head >= tail)) {
            return false;
        }

        *item = _items[head % _Capacity];

        return true;
    }

    FORCEINLINE void mark_push_finish(const uint32_t max_consumer_threads) noexcept
    {
        ASSERT(max_consumer_threads > 0, "BUG: max_consumer_threads == 0");

        for (uint32_t i = 0; i < max_consumer_threads; ++i) {
            C_CALL(sem_post(&_sem));
        }
    }

private:
    T _items[_Capacity];
    sem_t _sem;
    std::atomic_size_t _head { 0 };
    std::atomic_size_t _tail { 0 };
#if ENABLE_QUEUE_USE_SPIN_LOCK
    spin_lock _lock { false };
#else
    std::mutex _lock { };
#endif
};


template<typename T, size_t _Capacity>
class spsc_queue
{
    static_assert(std::is_trivial_v<T>, "T must be trivial type");
    static_assert(_Capacity > 0);

public:
    FORCEINLINE spsc_queue() noexcept
    {
        C_CALL(sem_init(&_sem, /*pshared*/0, 0));
    }

    FORCEINLINE ~spsc_queue() noexcept
    {
        C_CALL(sem_destroy(&_sem));
    }

    [[nodiscard]]
    constexpr size_t capacity() const noexcept
    {
        return _Capacity;
    }

    FORCEINLINE void push(const T& item) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        {
            //std::unique_lock<decltype(_lock)> lock(_lock);
            _items[(_tail++) % _Capacity] = item;
        }

        C_CALL(sem_post(&_sem));
    }

    template<typename... Args>
    FORCEINLINE void emplace(Args&&... args) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        {
            //std::unique_lock<decltype(_lock)> lock(_lock);
            new (&_items[(_tail++) % _Capacity]) T(std::forward<Args>(args)...);
        }

        C_CALL(sem_post(&_sem));
    }

    FORCEINLINE bool pop(/*out*/ T* item) noexcept
    {
        C_CALL(sem_wait(&_sem));

        // NOTE:
        //  We explicitly allow calling pop() before calling init()
        //  Thus, this assertion should go after sem_wait()
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called? head=%lu, tail=%lu", _head, _tail);

        const size_t head = _head++;
        const size_t tail = _tail;
        if (UNLIKELY(head >= tail)) {
            return false;
        }

        *item = _items[head % _Capacity];

        return true;
    }

    FORCEINLINE void mark_push_finish() noexcept
    {
        C_CALL(sem_post(&_sem));
    }

private:
    T _items[_Capacity];
    sem_t _sem;
    volatile size_t _head = 0;
    volatile size_t _tail = 0;
};


#endif  // !defined(_BDCI19_QUEUE_H_INCLUDED_)
