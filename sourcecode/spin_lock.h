#if !defined(_BDCI19_SPIN_LOCK_H_INCLUDED_)
#define _BDCI19_SPIN_LOCK_H_INCLUDED_

class spin_lock
{
public:
    spin_lock(const spin_lock&) noexcept = delete;
    spin_lock(spin_lock&&) noexcept = delete;
    spin_lock& operator =(const spin_lock&) noexcept = delete;
    spin_lock& operator =(spin_lock&&) noexcept = delete;

    FORCEINLINE explicit spin_lock(const bool locked = false) noexcept
        : _locked((locked ? 1 : 0))
    { }

    [[nodiscard]]
    FORCEINLINE bool is_locked() const noexcept
    {
        return _locked.load();
    }

    [[nodiscard]]
    FORCEINLINE bool try_lock() noexcept
    {
        size_t expected = 0;
        return _locked.compare_exchange_strong(expected, 1);
    }

    FORCEINLINE void lock() noexcept
    {
        while (!try_lock()) {
            while (is_locked()) { }
        }
    }

    FORCEINLINE void unlock() noexcept
    {
        _locked.store(0);
    }

private:
    std::atomic_size_t _locked;
};

#endif  // !defined(_BDCI19_SPIN_LOCK_H_INCLUDED_)
