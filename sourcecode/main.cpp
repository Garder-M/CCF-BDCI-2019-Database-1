#include "common.h"

#include <sys/sysinfo.h>

int main(int argc, char* argv[])
{
    ASSERT(argc >= 5, "argc = %d (too few arguments)", argc);

    // Load text files
    {
        const char* const customer_text_path = argv[1];
        const char* const orders_text_path = argv[2];
        const char* const lineitem_text_path = argv[3];

        const auto& open_file = [](const char* const path, int* const fd, uint64_t* const file_size) noexcept
        {
            *fd = C_CALL(open(path, O_RDONLY | O_CLOEXEC));
            struct stat64 st;
            C_CALL(fstat64(*fd, &st));
            *file_size = st.st_size;
            TRACE("open %s: fd = %d, size = %lu", path, *fd, *file_size);
        };

        open_file(customer_text_path, &g_customer_fd, &g_customer_file_size);
        open_file(orders_text_path, &g_orders_fd, &g_orders_file_size);
        open_file(lineitem_text_path, &g_lineitem_fd, &g_lineitem_file_size);
    }


    // Using index or creating index?
    {
        uint64_t tmp64 = g_customer_file_size ^ g_orders_file_size ^ g_lineitem_file_size;
        uint32_t index_hash_u32 = (uint32_t)(tmp64) ^(uint32_t)(tmp64 >> 32);

        struct statfs64 st;
        C_CALL(statfs64("/dev/shm", &st));
        const uint64_t free_size = (uint64_t)st.f_bfree * st.f_bsize;
        DEBUG("free size for /dev/shm: %lu", free_size);

        char index_dir[32];
        // if (free_size >= (uint64_t)1048576 * 1024 * 6) {  // At least 6 GB free space in /dev/shm
        //     DEBUG("/dev/shm is large enough - use it for index files");
        //     snprintf(index_dir, 32, "/dev/shm/index_%08x", index_hash_u32);
        // }
        if (false) { }
        else {
            DEBUG("/dev/shm is too small - use current working directory for index files");
            snprintf(index_dir, 32, "./index_%08x", index_hash_u32);
        }
        DEBUG("index_dir: %s", index_dir);

        g_index_directory_fd = open(index_dir, O_DIRECTORY | O_PATH | O_CLOEXEC);
        if (g_index_directory_fd >= 0) {
            g_is_creating_index = false;
            DEBUG("index directory found... using created index");
        }
        else if (errno == ENOENT) {
            g_is_creating_index = true;
            DEBUG("index directory not found... now creating index");
            C_CALL(mkdir(index_dir, 0755));
            g_index_directory_fd = C_CALL(open(index_dir, O_DIRECTORY | O_PATH | O_CLOEXEC));
        }
        else {
            PANIC("open() %s failed with %d (%s)", index_dir, errno, strerror(errno));
        }
        DEBUG("g_index_directory_fd: %d", g_index_directory_fd);
    }

    // A few common initializations
    cpu_set_t original_cpu_set;
    {
        // Get query count
        g_query_count = (uint32_t)std::strtoul(argv[4], nullptr, 10);
        DEBUG("g_query_count: %u", g_query_count);
        g_argv_queries = &argv[5];

        // How many workers do we use?
        CPU_ZERO(&original_cpu_set);
        PTHREAD_CALL(pthread_getaffinity_np(pthread_self(), sizeof(original_cpu_set), &original_cpu_set));
        const uint32_t active_cpu_cores = CPU_COUNT(&original_cpu_set);
        DEBUG("active_cpu_cores: %u", active_cpu_cores);

        g_worker_thread_count = std::min<uint32_t>(MAX_WORKER_THREADS, active_cpu_cores + 8);  // over-commit by 8 threads
        INFO("g_worker_thread_count: %u", g_worker_thread_count);
        g_worker_sync_barrier.init(g_worker_thread_count);

        // How many loaders do we use (at most)?
        g_loader_thread_count = std::min<uint32_t>(MAX_WORKER_THREADS, active_cpu_cores + 4);  // over-commit by 4 threads
        INFO("g_loader_thread_count: %u", g_loader_thread_count);
    }

    // Custom initializations
    g_loader_thread_count = 1;
    g_worker_thread_count = 1;
    if (g_is_creating_index) {
        create_index_initialize();
    }
    else {
        use_index_initialize();
        g_loader_sync_barrier.init(g_loader_thread_count);
        g_loader_threads.reserve(g_loader_thread_count);
        for (uint32_t tid = 0; tid < g_loader_thread_count; ++tid) {
            g_loader_threads.emplace_back(fn_loader_thread_use_index, tid);
        }
        for (std::thread& thr : g_loader_threads) thr.join();
    }


    // Create worker threads
    {
        g_worker_threads.reserve(g_worker_thread_count);
        for (uint32_t tid = 0; tid < g_worker_thread_count; ++tid) {
            g_worker_threads.emplace_back(
                g_is_creating_index ? fn_worker_thread_create_index : fn_worker_thread_use_index,
                tid);
        }
    }

    // Call main function
    (g_is_creating_index ? main_thread_create_index : main_thread_use_index)();

    // Wait for worker threads
    for (std::thread& thr : g_worker_threads) thr.join();

    if (g_is_creating_index) {
        char* args[argc + 1];
        for (int i = 0; i < argc; ++i) {
            args[i] = argv[i];
        }
        args[argc] = nullptr;
        INFO("================ now exec! ================");
        PTHREAD_CALL(pthread_setaffinity_np(pthread_self(), sizeof(original_cpu_set), &original_cpu_set));
        C_CALL(execv(argv[0], args));
    }
    else {
        INFO("================ now exit! ================");
    }
    return 0;
}
