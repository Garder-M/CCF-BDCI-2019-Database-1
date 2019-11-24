#if !defined(_BDCI19_MAPPER_H_INCLUDED_)
#define _BDCI19_MAPPER_H_INCLUDED_

#if !defined(SHM_HUGE_SHIFT)
#define SHM_HUGE_SHIFT  (26)
#endif

#if !defined(SHM_HUGE_2MB)
#define SHM_HUGE_2MB    (21 << SHM_HUGE_SHIFT)
#endif

#if !defined(SHM_HUGE_1GB)
#define SHM_HUGE_1GB    (30 << SHM_HUGE_SHIFT)
#endif


#if !defined(MAP_HUGE_SHIFT)
#define MAP_HUGE_SHIFT  (26)
#endif

#if !defined(MAP_HUGE_2MB)
#define MAP_HUGE_2MB    (21 << MAP_HUGE_SHIFT)
#endif

#if !defined(MAP_HUGE_1GB)
#define MAP_HUGE_1GB    (30 << MAP_HUGE_SHIFT)
#endif

#define MMAP_ALIGNMENT_2MB              (1048576U * 2)


inline uintptr_t __mmap_ptr_top = 0;
inline std::atomic_uintptr_t __mmap_ptr { 0 };


__always_inline
void __mapper_initialize() noexcept
{
    void* const ptr = mmap(
        nullptr,
        PAGE_SIZE,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS,
        -1,
        0);
    CHECK(ptr != MAP_FAILED, "mmap() failed. errno = %d (%s)", errno, strerror(errno));

    C_CALL(munmap(ptr, PAGE_SIZE));

    constexpr const uintptr_t HOLE_1TB = (uintptr_t)1024 * 1024 * 1024 * 1024;  // subtract 1 TB in VA
    __mmap_ptr_top = __align_down((uintptr_t)ptr, MMAP_ALIGNMENT_2MB) - HOLE_1TB;
    DEBUG("__mmap_ptr_top: %p", (void*)__mmap_ptr_top);

    __mmap_ptr = __mmap_ptr_top;
}



//__always_inline
//void mmap_allocate_parallel(
//    /*out*/ void** shared_result,
//    /*in*/ sync_barrier& barrier,
//    /*inout*/ std::atomic_size_t& shared_offset,  // it should be 0!
//    /*in*/ [[maybe_unused]] const uint32_t tid,
//    /*in*/ const size_t size) noexcept
//{
//    const size_t ALIGNMENT = __mmap_hugetlb_supported ? MMAP_ALIGNMENT_2MB : MMAP_ALIGNMENT_NORMAL;
//    const size_t MAX_STEP_SIZE = __mmap_hugetlb_supported ? CONFIG_MMAP_MAX_STEP_SIZE_HUGETLB : CONFIG_MMAP_MAX_STEP_SIZE;
//    const size_t MIN_STEP_SIZE = __mmap_hugetlb_supported ? CONFIG_MMAP_MIN_STEP_SIZE_HUGETLB : CONFIG_MMAP_MIN_STEP_SIZE;
//    ASSERT(MAX_STEP_SIZE % ALIGNMENT == 0);
//    ASSERT(MIN_STEP_SIZE % ALIGNMENT == 0);
//
//    barrier.run_once_and_sync([&]() {
//        ASSERT(shared_offset.load() == 0);
//
//        const size_t aligned_size = ALIGN_UP(size, MMAP_ALIGNMENT_2MB);
//        *shared_result = (void*)(__mmap_ptr -= aligned_size);
//    });
//    void* const fixed_address = *shared_result;
//    ASSERT(fixed_address != nullptr);
//
//    ASSERT(barrier.thread_count() > 0);
//    size_t step_size = ALIGN_UP(size / barrier.thread_count(), ALIGNMENT);
//    if (step_size < MIN_STEP_SIZE) {
//        step_size = MIN_STEP_SIZE;
//    }
//    else if (step_size > MAX_STEP_SIZE) {
//        step_size = MAX_STEP_SIZE;
//    }
//
//    DEBUG("[loader:%u] mmap_allocate_parallel() starts: size=%lu, step_size=%lu, fixed_address=%p",
//        tid, size, step_size, fixed_address);
//
//    while (true) {
//        const size_t curr_offset = shared_offset.fetch_add(step_size);
//        if (curr_offset >= size) break;
//
//        const size_t curr_size = std::min<size_t>(step_size, size - curr_offset);
//        void* const curr_address = (void*)((uintptr_t)fixed_address + curr_offset);
//        void* const ptr = mmap(
//            curr_address,
//            curr_size,
//            PROT_READ | PROT_WRITE,
//            MAP_FIXED | MAP_POPULATE | MAP_PRIVATE | MAP_ANONYMOUS | (__mmap_hugetlb_supported ? MAP_HUGETLB | MAP_HUGE_2MB : 0),
//            -1,
//            0);
//        if (UNLIKELY(ptr == MAP_FAILED)) {
//            PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
//        }
//        ASSERT(ptr == curr_address, "BUG: expect %p == %p", ptr, curr_address);
//        DEBUG("[loader:%u] mmap_allocate_parallel(): size=%lu, step_size=%lu, offset=%lu -> %p",
//              tid, size, step_size, curr_offset, curr_address);
//
//        //std::this_thread::yield();
//    }
//
//    DEBUG("[loader:%u] mmap_allocate_parallel() done: size=%lu, step_size=%lu, fixed_address=%p",
//          tid, size, step_size, fixed_address);
//
//    barrier.sync();
//}


[[nodiscard]]
__always_inline
void* my_mmap(
    /*in*/ const size_t size,
    /*in*/ const int prot,
    /*in*/ const int flags,
    /*in*/ const int fd,
    /*in*/ const off_t offset) noexcept
{
    const size_t aligned_size = __align_up(size, MMAP_ALIGNMENT_2MB);
    void* const fixed_address = (void*)(__mmap_ptr -= aligned_size);

    void* const ptr = mmap(fixed_address, size, prot, MAP_FIXED | flags, fd, offset);
    CHECK(ptr != MAP_FAILED, "mmap() failed. errno = %d (%s)", errno, strerror(errno));
    ASSERT(ptr == fixed_address, "BUG: expect %p == %p", ptr, fixed_address);

    return ptr;
}


[[nodiscard]]
__always_inline
void* mmap_allocate_page4k(
    /*in*/ const size_t size) noexcept
{
    return my_mmap(
        size,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
        -1,
        0);
}

[[nodiscard]]
__always_inline
void* mmap_allocate_page4k_shared(
    /*in*/ const size_t size) noexcept
{
    return my_mmap(
        size,
        PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS | MAP_POPULATE,
        -1,
        0);
}

[[nodiscard]]
__always_inline
void* mmap_allocate_page2m(
    /*in*/ const size_t size) noexcept
{
    return my_mmap(
        size,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB | MAP_HUGE_2MB,
        -1,
        0);
}

__always_inline
void mmap_deallocate(
    /*in*/ void* const ptr,
    /*in*/ const size_t size) noexcept
{
    ASSERT(ptr != nullptr);

    C_CALL(munmap(ptr, __align_up(size, MMAP_ALIGNMENT_2MB)));
}


[[nodiscard]]
__always_inline
void* mmap_reserve_space(
    /*in*/ const size_t size) noexcept
{
    // NOTE: to keep __mmap_ptr aligned to MMAP_ALIGNMENT_2MB
    const size_t aligned_size = __align_up(size, MMAP_ALIGNMENT_2MB);
    void* const fixed_address = (void*)(__mmap_ptr -= aligned_size);

    return fixed_address;
}

__always_inline
void mmap_return_space(
    /*in*/ void* const ptr,
    /*in*/ const size_t size) noexcept
{
    // NOTE: to keep __mmap_ptr aligned to MMAP_ALIGNMENT_2MB
    const size_t aligned_size = __align_up(size, MMAP_ALIGNMENT_2MB);

    uintptr_t expected = (uint64_t)ptr;
    __mmap_ptr.compare_exchange_strong(expected, expected + aligned_size);
}


__always_inline
void debug_show_proc_self_maps()
{
#if ENABLE_LOGGING_INFO
    static thread_local char __buffer[1024 * 1024];

    const int fd = C_CALL(open("/proc/self/maps", O_RDONLY | O_CLOEXEC));
    const size_t cnt = C_CALL(read(fd, __buffer, std::size(__buffer)));
    C_CALL(close(fd));

    const uintptr_t curr_mmap_down = __mmap_ptr;
    const char* p = __buffer;
    const char* const end = p + cnt;

    const auto read_hex = [&](const char until_char) -> uintptr_t {
        uintptr_t result = 0;
        while (*p != until_char) {
            ASSERT((*p >= 'a' && *p <= 'f') || (*p >= '0' && *p <= '9'), "Unexpected char: %c (%u) %s", *p, *p, p);
            result = result * 16 + (*p >= 'a' ? (*p - 'a' + 10) : (*p - '0'));
            ++p;
        }
        ++p;  // skip until_char
        return result;
    };

    INFO("++++++++++++++++ /proc/self/maps ++++++++++++++++");
    while (p < end) {
        const uintptr_t start_ptr = read_hex('-');
        const uintptr_t end_ptr = read_hex(' ');

        if (!(start_ptr >= curr_mmap_down && end_ptr <= __mmap_ptr_top)) {
            while (*(p++) != '\n') { }
            continue;
        }

        ASSERT(*p == 'r' || *p == '-');
        const char flag_read = *(p++);
        ASSERT(*p == 'w' || *p == '-');
        const char flag_write = *(p++);
        ASSERT(*p == 'x' || *p == '-');
        const char flag_execute = *(p++);
        ASSERT(*p == 'p' || *p == 's');
        const char flag_private = *(p++);
        ASSERT(*p == ' ');
        ++p;  // skip ' '

        const uintptr_t offset = read_hex(' ');

        const uint32_t dev_major = (uint32_t)read_hex(':');
        const uint32_t dev_minor = (uint32_t)read_hex(' ');

        if (dev_major == 0 && dev_minor == 0) {
            ASSERT(offset == 0);
            INFO("    0x%016lx-0x%016lx (%lu MB): %c%c%c%c (anonymous)",
                 start_ptr, end_ptr, (end_ptr - start_ptr) / 1048576,
                 flag_read, flag_write, flag_execute, flag_private);
            while (*(p++) != '\n') { }
        }
        else {
            read_hex(' ');  // inode
            while (*p == ' ') ++p;
            const char* const filename_ptr = p;
            while (*p != '\n') ++p;
            INFO("    0x%016lx-0x%016lx (%lu MB): %c%c%c%c, offset=0x%lx, file=%.*s",
                 start_ptr, end_ptr, (end_ptr - start_ptr) / 1048576,
                 flag_read, flag_write, flag_execute, flag_private,
                 offset, (int)(p - filename_ptr), filename_ptr);
            ++p;  // skip '\n'
        }
    }
    INFO("-------------------------------------------------\n");
#endif  // ENABLE_LOGGING_INFO
}


#endif  // !defined(_BDCI19_MAPPER_H_INCLUDED_)
