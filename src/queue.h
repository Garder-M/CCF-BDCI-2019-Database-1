#if !defined(_BDCI19_QUEUE_H_INCLUDED_)
#define _BDCI19_QUEUE_H_INCLUDED_

template<typename T, size_t _Capacity>
class bounded_bag
{
    static_assert(std::is_trivial_v<T>, "T must be trivial type");
    static_assert(_Capacity > 0);

public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(bounded_bag);

    __always_inline
    bounded_bag() noexcept = default;

    __always_inline
    ~bounded_bag() noexcept = default;

    __always_inline void init(
        /*in*/ const std::function<T(size_t)>& fn_init,
        /*in*/ const size_t init_count /*= _Capacity*/) noexcept
    {
        ASSERT(init_count <= _Capacity);
        if (init_count > 0) {
            ASSERT(fn_init);
        }

        for (size_t idx = 0; idx < init_count; ++idx) {
            _items[idx] = fn_init(idx);
        }

        _head.store(0, std::memory_order_acquire);
        _tail.store(init_count, std::memory_order_acquire);

        _sem_pop.reinit(init_count);
    }

    __always_inline void take(/*out*/ T* item) noexcept
    {
        _sem_pop.wait();

        ASSERT(_head < _tail);
        *item = _items[(_head++) % _Capacity];
    }

    __always_inline void return_back(/*in*/ const T& item) noexcept
    {
        {
            std::unique_lock<decltype(_lock)> lock(_lock);
            ASSERT(_head <= _tail);
            _items[(_tail++) % _Capacity] = item;
        }

        _sem_pop.post();
    }

    [[nodiscard]]
    constexpr size_t capacity() const noexcept
    {
        return _Capacity;
    }

private:
    T _items[_Capacity];
    process_shared_semaphore _sem_pop;
    std::atomic_size_t _head;
    std::atomic_size_t _tail;
#if ENABLE_QUEUE_USE_SPIN_LOCK
    spin_lock _lock { false };
#else
    process_shared_mutex _lock { };
#endif
};


template<typename T, size_t _Capacity>
class spsc_bounded_bag
{
    static_assert(std::is_trivial_v<T>, "T must be trivial type");
    static_assert(_Capacity > 0);

public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(spsc_bounded_bag);

    __always_inline
    spsc_bounded_bag() noexcept = default;

    __always_inline
    ~spsc_bounded_bag() noexcept = default;

    __always_inline void init(
        /*in*/ const std::function<T(size_t)>& fn_init,
        /*in*/ const size_t init_count /*= _Capacity*/) noexcept
    {
        ASSERT(init_count <= _Capacity);
        if (init_count > 0) {
            ASSERT(fn_init);
        }

        for (size_t idx = 0; idx < init_count; ++idx) {
            _items[idx] = fn_init(idx);
        }

        _head = 0;
        _tail = init_count;

        _sem_pop.reinit(init_count);
    }

    __always_inline void take(/*out*/ T* item) noexcept
    {
        _sem_pop.wait();

        ASSERT(_head < _tail);
        *item = _items[(_head++) % _Capacity];
    }

    __always_inline void return_back(/*in*/ const T& item) noexcept
    {
        {
            //std::unique_lock<decltype(_lock)> lock(_lock);
            ASSERT(_head <= _tail);
            _items[(_tail++) % _Capacity] = item;
        }

        _sem_pop.post();
    }

    [[nodiscard]]
    constexpr size_t capacity() const noexcept
    {
        return _Capacity;
    }

private:
    T _items[_Capacity];
    process_private_semaphore _sem_pop { 0 };
    volatile size_t _head { 0 };
    volatile size_t _tail { 0 };
};



template<typename T, size_t _Capacity>
class mpmc_queue
{
    static_assert(std::is_trivial_v<T>, "T must be trivial type");
    static_assert(_Capacity > 0);

public:
    __always_inline
    mpmc_queue() noexcept = default;

    __always_inline
    ~mpmc_queue() noexcept = default;

    [[nodiscard]]
    constexpr size_t capacity() const noexcept
    {
        return _Capacity;
    }

    __always_inline
    void push(const T& item) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        {
            std::unique_lock<decltype(_lock)> lock(_lock);
            _items[(_tail++) % _Capacity] = item;
        }

        _sem.post();
    }

    template<typename... Args>
    __always_inline void emplace(Args&&... args) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        {
            std::unique_lock<decltype(_lock)> lock(_lock);
            new (&_items[(_tail++) % _Capacity]) T(std::forward<Args>(args)...);
        }

        _sem.post();
    }

    __always_inline bool pop(/*out*/ T* item) noexcept
    {
        _sem.wait();

        // NOTE:
        //  We explicitly allow calling pop() before calling init()
        //  Thus, this assertion should go after sem_wait()
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called? head=%lu, tail=%lu",
            _head.load(), _tail.load());

        const size_t head = _head++;
        const size_t tail = _tail.load();
        if (__unlikely(head >= tail)) {
            return false;
        }

        *item = _items[head % _Capacity];

        return true;
    }

    __always_inline void mark_push_finish(const uint32_t max_consumer_threads) noexcept
    {
        ASSERT(max_consumer_threads > 0, "BUG: max_consumer_threads == 0");

        _sem.post(max_consumer_threads);
    }

private:
    T _items[_Capacity];
    process_shared_semaphore _sem { 0 };
    std::atomic_size_t _head { 0 };
    std::atomic_size_t _tail { 0 };
#if ENABLE_QUEUE_USE_SPIN_LOCK
    spin_lock _lock { false };
#else
    process_shared_mutex _lock { };
#endif
};


template<typename T, size_t _Capacity>
class spsc_queue
{
    static_assert(std::is_trivial_v<T>, "T must be trivial type");
    static_assert(_Capacity > 0);

public:
    __always_inline
    spsc_queue() noexcept = default;

    __always_inline ~spsc_queue() noexcept = default;

    [[nodiscard]]
    constexpr size_t capacity() const noexcept
    {
        return _Capacity;
    }

    __always_inline void push(const T& item) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        {
            //std::unique_lock<decltype(_lock)> lock(_lock);
            _items[(_tail++) % _Capacity] = item;
        }

        _sem.post();
    }

    template<typename... Args>
    __always_inline void emplace(Args&&... args) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        {
            //std::unique_lock<decltype(_lock)> lock(_lock);
            new (&_items[(_tail++) % _Capacity]) T(std::forward<Args>(args)...);
        }

        _sem.post();
    }

    __always_inline bool pop(/*out*/ T* item) noexcept
    {
        _sem.wait();

        // NOTE:
        //  We explicitly allow calling pop() before calling init()
        //  Thus, this assertion should go after sem_wait()
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called? head=%lu, tail=%lu", _head, _tail);

        const size_t head = _head++;
        const size_t tail = _tail;
        if (__unlikely(head >= tail)) {
            return false;
        }

        *item = _items[head % _Capacity];

        return true;
    }

    __always_inline void mark_push_finish() noexcept
    {
        _sem.post();
    }

    __always_inline void reinit() noexcept
    {
        _head = 0;
        _tail = 0;
        _sem.reinit();
    }

private:
    T _items[_Capacity];
    process_private_semaphore _sem { 0 };
    volatile size_t _head = 0;
    volatile size_t _tail = 0;
};


#endif  // !defined(_BDCI19_QUEUE_H_INCLUDED_)
