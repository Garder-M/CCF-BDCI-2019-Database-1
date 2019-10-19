#if !defined(_BDCI19_MAPPER_H_INCLUDED_)
#define _BDCI19_MAPPER_H_INCLUDED_

#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT  (26)
#endif

#if !defined(MAP_HUGE_2MB)
#define MAP_HUGE_2MB    (21 << MAP_HUGE_SHIFT)
#endif

#if !defined(MAP_HUGE_1GB)
#define MAP_HUGE_1GB    (30 << MAP_HUGE_SHIFT)
#endif

#define MMAP_ALIGNMENT_HUGETLB              (1048576U * 2)
#define MMAP_ALIGNMENT                      (4096)

// TODO: Move to common macros!
#define MMAP_ALIGN_DOWN(_Ptr_, _Align_)     ((void*)((uintptr_t)(_Ptr_) / (uintptr_t)(_Align_) * (uintptr_t)(_Align_)))
#define MMAP_ALIGN_UP(_Ptr_, _Align_)       ((void*)MMAP_ALIGN_DOWN((uintptr_t)(_Ptr_) + (_Align_) - 1, (_Align_)))


FORCEINLINE uintptr_t __mapper_initialize() noexcept
{
    void* const ptr = mmap(
        nullptr,
        MMAP_ALIGNMENT_HUGETLB,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS,
        -1,
        0);
    CHECK(ptr != MAP_FAILED, "mmap() failed. errno = %d (%s)", errno, strerror(errno));

    uintptr_t __mmap_ptr = (uintptr_t)MMAP_ALIGN_DOWN(ptr, MMAP_ALIGNMENT_HUGETLB) - (uintptr_t)1024 * 1024 * 1024 * 1024;  // subtract 1 TB in VA
    DEBUG("__mmap_ptr: %p", (void*)__mmap_ptr);

    C_CALL(munmap(ptr, MMAP_ALIGNMENT_HUGETLB));

    return __mmap_ptr;
}

inline std::atomic_uintptr_t __mmap_ptr { __mapper_initialize() };
inline bool __mmap_hugetlb_supported = false;  // TODO: Use HugeTLB when available


FORCEINLINE void mmap_allocate_parallel(
    /*out*/ void** shared_result,
    /*in*/ sync_barrier& barrier,
    /*inout*/ std::atomic_size_t& shared_offset,  // it should be 0!
    /*in*/ [[maybe_unused]] const uint32_t tid,
    /*in*/ const size_t size) noexcept
{
    const size_t ALIGNMENT = __mmap_hugetlb_supported ? MMAP_ALIGNMENT_HUGETLB : MMAP_ALIGNMENT;
    const size_t MAX_STEP_SIZE = __mmap_hugetlb_supported ? CONFIG_MMAP_MAX_STEP_SIZE_HUGETLB : CONFIG_MMAP_MAX_STEP_SIZE;
    const size_t MIN_STEP_SIZE = __mmap_hugetlb_supported ? CONFIG_MMAP_MIN_STEP_SIZE_HUGETLB : CONFIG_MMAP_MIN_STEP_SIZE;
    ASSERT(MAX_STEP_SIZE % ALIGNMENT == 0);
    ASSERT(MIN_STEP_SIZE % ALIGNMENT == 0);

    barrier.run_once_and_sync([&]() {
        ASSERT(shared_offset.load() == 0);

        const size_t aligned_size = (size_t)MMAP_ALIGN_UP(size, MMAP_ALIGNMENT_HUGETLB);
        *shared_result = (void*)(__mmap_ptr -= aligned_size);
    });
    void* const fixed_address = *shared_result;
    ASSERT(fixed_address != nullptr);

    ASSERT(barrier.thread_count() > 0);
    size_t step_size = (size_t)MMAP_ALIGN_UP(size / barrier.thread_count(), ALIGNMENT);
    if (step_size < MIN_STEP_SIZE) {
        step_size = MIN_STEP_SIZE;
    }
    else if (step_size > MAX_STEP_SIZE) {
        step_size = MAX_STEP_SIZE;
    }

    DEBUG("[loader:%u] mmap_allocate_parallel() starts: size=%lu, step_size=%lu, fixed_address=%p",
        tid, size, step_size, fixed_address);

    while (true) {
        const size_t curr_offset = shared_offset.fetch_add(step_size);
        if (curr_offset >= size) break;

        const size_t curr_size = std::min<size_t>(step_size, size - curr_offset);
        void* const curr_address = (void*)((uintptr_t)fixed_address + curr_offset);
        void* const ptr = mmap(
            curr_address,
            curr_size,
            PROT_READ | PROT_WRITE,
            MAP_FIXED | MAP_POPULATE | MAP_PRIVATE | MAP_ANONYMOUS | (__mmap_hugetlb_supported ? MAP_HUGETLB | MAP_HUGE_2MB : 0),
            -1,
            0);
        if (UNLIKELY(ptr == MAP_FAILED)) {
            PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
        }
        ASSERT(ptr == curr_address, "BUG: expect %p == %p", ptr, curr_address);
        DEBUG("[loader:%u] mmap_allocate_parallel(): size=%lu, step_size=%lu, offset=%lu -> %p",
              tid, size, step_size, curr_offset, curr_address);

        //std::this_thread::yield();
    }

    DEBUG("[loader:%u] mmap_allocate_parallel() done: size=%lu, step_size=%lu, fixed_address=%p",
          tid, size, step_size, fixed_address);

    barrier.sync();
}


FORCEINLINE void* my_mmap(
    const size_t size,
    const int prot,
    const int flags,
    const int fd,
    const off_t offset) noexcept
{
    // NOTE: to keep __mmap_ptr aligned to MMAP_ALIGNMENT_HUGETLB
    const size_t aligned_size = (size_t)MMAP_ALIGN_UP(size, MMAP_ALIGNMENT_HUGETLB);
    void* const fixed_address = (void*)(__mmap_ptr -= aligned_size);

    void* const ptr = mmap(fixed_address, size, prot, MAP_FIXED | flags, fd, offset);
    if (UNLIKELY(ptr == MAP_FAILED)) {
        PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
    }
    ASSERT(ptr == fixed_address, "BUG: expect %p == %p", ptr, fixed_address);

    return ptr;
}


#endif  // !defined(_BDCI19_MAPPER_H_INCLUDED_)
