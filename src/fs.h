#if !defined(_BDCI19_SYS_H_INCLUDED_)
#define _BDCI19_SYS_H_INCLUDED_


struct load_file_context
{
    int fd = -1;
    uint64_t file_size = 0;
};



__always_inline
uint64_t __read_file_u64(const char* const path) noexcept
{
    char buffer[64];
    const int fd = C_CALL(open(path, O_RDONLY | O_CLOEXEC));
    const size_t cnt = C_CALL(read(fd, buffer, std::size(buffer)));
    CHECK(cnt < std::size(buffer));
    C_CALL(close(fd));
    return strtoul(buffer, nullptr, 10);
}

__always_inline
bool __write_file_u64(const char* const path, const uint64_t value) noexcept
{
    const int fd = C_CALL_NO_PANIC(open(path, O_WRONLY | O_CLOEXEC));
    if (fd < 0) {
        return false;
    }

    char buffer[64];
    const int size = snprintf(buffer, std::size(buffer), "%lu", value);
    CHECK(size > 0);

    const size_t cnt = C_CALL(write(fd, buffer, size));
    CHECK(cnt == (size_t)size);
    C_CALL(close(fd));

    return true;
}


__always_inline
bool __fincore(
    /*in*/ void* const mmap_ptr,
    /*in*/ const int fd,
    /*in*/ const uint64_t offset,
    /*in*/ const uint64_t size) noexcept
{
    void* const ptr = mmap64(
        mmap_ptr,
        size,
        PROT_NONE,
        MAP_FIXED | MAP_SHARED,  // flags is important here!
        fd,
        offset);
    CHECK(ptr != MAP_FAILED, "mmap() failed");

    const uint64_t vec_size = __div_up(size, PAGE_SIZE);
    unsigned char vec[vec_size];
    memset(vec, 0x00, sizeof(unsigned char) * vec_size);

    C_CALL(mincore(ptr, size, vec));
    //C_CALL(munmap(ptr, size));  // It's caller's duty to unmap this region

    for (index64_t i = 0; i < vec_size; ++i) {
        if (!(vec[i] & 0x1)) return false;
    }
    return true;
}


__always_inline
#if ENABLE_SHM_CACHE_TXT
void __open_file_read_direct(
#else  // !ENABLE_SHM_CACHE_TXT
void __open_file_read(
#endif  // ENABLE_SHM_CACHE_TXT
    /*in*/ const char *const path,
    /*out*/ load_file_context *const ctx) noexcept
{
    ASSERT(ctx->fd == -1, "fd should be initialized to -1 to prevent bugs");
    ctx->fd = C_CALL(open(path, O_RDONLY | O_CLOEXEC | (ENABLE_SHM_CACHE_TXT ? O_DIRECT : 0)));

    struct stat64 st; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    C_CALL(fstat64(ctx->fd, &st));
    ctx->file_size = st.st_size;

    DEBUG("__open_file_read() %s: fd = %d, size = %lu", path, ctx->fd, ctx->file_size);
}


__always_inline
#if ENABLE_SHM_CACHE_TXT
void __openat_file_read_direct(
#else  // !ENABLE_SHM_CACHE_TXT
void __openat_file_read(
#endif  // ENABLE_SHM_CACHE_TXT
    /*in*/ const int dir_fd,
    /*in*/ const char *const name,
    /*out*/ load_file_context *const ctx) noexcept
{
    ASSERT(ctx->fd == -1, "fd should be initialized to -1 to prevent bugs");
    ctx->fd = C_CALL(openat(
        dir_fd,
        name,
        O_RDONLY | O_CLOEXEC | (ENABLE_SHM_CACHE_TXT ? O_DIRECT : 0)));

    struct stat64 st; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    C_CALL(fstat64(ctx->fd, &st));
    ctx->file_size = st.st_size;

    DEBUG("__openat_file_read() %s: fd = %d, size = %lu", name, ctx->fd, ctx->file_size);
}


__always_inline
void __fadvice_dont_need(
    const int fd,
    const uint64_t offset,
    const uint64_t len) noexcept
{
    C_CALL(posix_fadvise64(fd, offset, len, POSIX_FADV_DONTNEED));
}


#endif  // !defined(_BDCI19_SYS_H_INCLUDED_)
