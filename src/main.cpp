#include "common.h"


static void detect_preparing_page_cache() noexcept
{
    ASSERT(g_orders_file.file_size > 0);
    ASSERT(g_customer_file.file_size > 0);
    ASSERT(g_lineitem_file.file_size > 0);

    g_is_preparing_page_cache = true;

    const uint32_t nr_2mb = mem_get_nr_hugepages_2048kB();
    DEBUG("nr_hugepages (2048kB): %u", nr_2mb);

#if ENABLE_SHM_CACHE_TXT
    const uint32_t require_nr_2mb =
        __div_up(g_customer_file.file_size, 1024 * 1024 * 2) +
        __div_up(g_orders_file.file_size, 1024 * 1024 * 2) +
        __div_up(g_lineitem_file.file_size, 1024 * 1024 * 2) +
        CONFIG_EXTRA_HUGE_PAGES;
#else
    const uint32_t require_nr_2mb = CONFIG_EXTRA_HUGE_PAGES;
#endif
    DEBUG("require_nr_2mb: %u", require_nr_2mb);

    if (nr_2mb == require_nr_2mb) {
        do {
#if ENABLE_SHM_CACHE_TXT
            if (!g_customer_shm.init_fixed(SHMKEY_TXT_CUSTOMER, g_customer_file.file_size, false)) {
                break;
            }
            if (!g_orders_shm.init_fixed(SHMKEY_TXT_ORDERS, g_orders_file.file_size, false)) {
                break;
            }
            if (!g_lineitem_shm.init_fixed(SHMKEY_TXT_LINEITEM, g_lineitem_file.file_size, false)) {
                break;
            }
#else  // !ENABLE_SHM_CACHE_TXT
            // Test are txt files in page cache?
            const auto is_file_in_page_cache = [](const load_file_context& ctx) {
                ASSERT(ctx.fd > 0);
                ASSERT(ctx.file_size > 0);

                constexpr const uint64_t CHECK_SIZE = 1048576;
                void* const ptr = mmap_reserve_space(CHECK_SIZE);

                bool result = false;
                do {
                    if (!__fincore(ptr, ctx.fd, 0, CHECK_SIZE)) {
                        break;
                    }
                    if (!__fincore(ptr, ctx.fd, __align_down(ctx.file_size / 2, PAGE_SIZE), CHECK_SIZE)) {
                        break;
                    }
                    if (!__fincore(ptr, ctx.fd, __align_down(ctx.file_size - CHECK_SIZE, PAGE_SIZE), CHECK_SIZE)) {
                        break;
                    }
                    result = true;
                } while(false);

                C_CALL(munmap(ptr, CHECK_SIZE));
                mmap_return_space(ptr, ctx.file_size);

                return result;
            };

            if (!is_file_in_page_cache(g_customer_file)) break;
            INFO("is_file_in_page_cache(g_customer_file): true");

            if (!is_file_in_page_cache(g_orders_file)) break;
            INFO("is_file_in_page_cache(g_orders_file): true");

            if (!is_file_in_page_cache(g_lineitem_file)) break;
            INFO("is_file_in_page_cache(g_lineitem_file): true");

#endif  // ENABLE_SHM_CACHE_TXT
            g_is_preparing_page_cache = false;
        } while(false);
    }
    else {
        DEBUG("nr_hugepages != require_nr_2mb (%u != %u)", nr_2mb, require_nr_2mb);
    }

    INFO("g_is_preparing_page_cache: %d", (int)g_is_preparing_page_cache);

    // TODO
    if (g_is_preparing_page_cache) {
#if ENABLE_SHM_CACHE_TXT
        PANIC("TODO: preparing page cache not implemented yet!");
#else
        // Do nothing
#endif
    }
}


static void do_multi_process() noexcept
{
    //
    // Custom initializations (before fork)
    //
    if (g_is_creating_index) {
        create_index_initialize_before_fork();
    }
    else {
        use_index_initialize_before_fork();
    }


    //
    // Do fork or create threads!
    //
    g_id = -1;
    for (uint32_t child_id = 0; child_id < g_total_process_count; ++child_id) {
        if (C_CALL(fork()) == 0) {  // child
            g_id = child_id;
            break;
        }
        else {  // parent
        }
    }

    //
    // Main process:
    // Wait for child processes to exit
    //
    if (g_id == (uint32_t)-1) {
        for (uint32_t i = 0; i < g_total_process_count; ++i) {
            int status;
            C_CALL(wait(&status));
            CHECK(WIFEXITED(status), "One of child process not normally exits");
            CHECK(WEXITSTATUS(status) == 0, "One of child process exits with %d", WEXITSTATUS(status));
        }
        INFO("all child processes exits normally");
        return;
    }

    INFO("[%u] child process: g_id = %u, pid = %d", g_id, g_id, getpid());

    // Custom initializations (after fork)
    if (g_is_creating_index) {
        create_index_initialize_after_fork();
    }
    else {
        use_index_initialize_after_fork();
    }


    //
    // Create worker thread, unloader thread
    //
    std::thread worker_thread;
    std::thread unloader_thread;
    {
        worker_thread = std::thread([&]() {
#if ENABLE_PIN_THREAD_TO_CPU
            pin_thread_to_cpu_core(g_id);
#endif
#if ENABLE_ATTEMPT_SCHED_FIFO
            set_thread_fifo_scheduler(CONFIG_SCHED_FIFO_WORKER_NICE);
#endif
            (g_is_creating_index ? fn_worker_thread_create_index : fn_worker_thread_use_index)(g_id);
        });

        unloader_thread = std::thread([&]() {
#if ENABLE_PIN_THREAD_TO_CPU
            pin_thread_to_cpu_core(g_id);
#endif
#if ENABLE_ATTEMPT_SCHED_FIFO
            set_thread_fifo_scheduler(CONFIG_SCHED_FIFO_UNLOADER_NICE);
#endif
            (g_is_creating_index ? fn_unloader_thread_create_index : fn_unloader_thread_use_index)();
        });
    }


    //
    // Do like loader thread...
    //
    {
#if ENABLE_PIN_THREAD_TO_CPU
        pin_thread_to_cpu_core(g_id);
#endif
#if ENABLE_ATTEMPT_SCHED_FIFO
        set_thread_fifo_scheduler(CONFIG_SCHED_FIFO_LOADER_NICE);
#endif
        (g_is_creating_index ? fn_loader_thread_create_index : fn_loader_thread_use_index)(g_id);
    }


    //
    // Wait for loader thread
    // ...as well as unloader thread
    //
    worker_thread.join();
    unloader_thread.join();
}



int main(int argc, char* argv[])
{
    debug_print_cgroup();

    //
    // Initialize mapper
    //
    __mapper_initialize();


    //
    // Detect whether we are creating index or using index
    //
    {
        const char index_dir[32] { "./index" };
        DEBUG("use current working directory for index files: index_dir: %s", index_dir);

        g_index_directory_fd = open(index_dir, O_DIRECTORY | O_PATH | O_CLOEXEC);
        if (g_index_directory_fd >= 0) {
            g_is_creating_index = false;
            DEBUG("index directory found... using created index");
        }
        else if (errno == ENOENT) {
            g_is_creating_index = true;
            DEBUG("index directory not found... now creating index");
            C_CALL(mkdir(index_dir, 0777));  // so we don't require sudo to `rm -rf index`
            g_index_directory_fd = C_CALL(open(index_dir, O_DIRECTORY | O_PATH | O_CLOEXEC));
        }
        else {
            PANIC("open() %s failed with %d (%s)", index_dir, errno, strerror(errno));
        }
        DEBUG("g_index_directory_fd: %d", g_index_directory_fd);
    }


    //
    // Open text files if creating index
    //
    CHECK(argc >= 5, "%s <customer_txt> <orders_txt> <lineitem_txt> <query_count>", argv[0]);
    if (g_is_creating_index) {
        const char* const customer_text_path = argv[1];
        const char* const orders_text_path = argv[2];
        const char* const lineitem_text_path = argv[3];

#if ENABLE_SHM_CACHE_TXT
        __open_file_read_direct(customer_text_path, &g_customer_file);
        __open_file_read_direct(orders_text_path, &g_orders_file);
        __open_file_read_direct(lineitem_text_path, &g_lineitem_file);
#else
        __open_file_read(customer_text_path, &g_customer_file);
        __open_file_read(orders_text_path, &g_orders_file);
        __open_file_read(lineitem_text_path, &g_lineitem_file);
#endif

        detect_preparing_page_cache();
    }


    //
    // Initialize g_shared
    //
    {
        g_shared = (shared_information_t*)my_mmap(
            sizeof(shared_information_t),
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS | MAP_POPULATE,
            -1,
            0);
        DEBUG("g_shared: %p", g_shared);

        new (g_shared) shared_information_t;
    }


    //
    // A few common initializations
    //
    {
        // Get query count
        g_query_count = (uint32_t) std::strtoul(argv[4], nullptr, 10);
        DEBUG("g_query_count: %u", g_query_count);
        g_argv_queries = &argv[5];
        if (!g_is_creating_index && __unlikely(g_query_count == 0)) {
            INFO("!is_creating_index && g_query_count == 0: fast exit!");
            return 0;
        }

        // How many CPU cores do we use?
        cpu_set_t original_cpu_set;
        CPU_ZERO(&original_cpu_set);
        PTHREAD_CALL(pthread_getaffinity_np(pthread_self(), sizeof(original_cpu_set), &original_cpu_set));
        g_active_cpu_cores = CPU_COUNT(&original_cpu_set);
        INFO("g_active_cpu_cores: %u", g_active_cpu_cores);

        // How many processes do we use?
        g_total_process_count = g_active_cpu_cores;
        DEBUG("g_total_process_count: %u", g_total_process_count);
        g_shared->worker_sync_barrier.init(g_total_process_count, /*use_multi_process*/true);
        g_shared->loader_sync_barrier.init(g_total_process_count, /*use_multi_process*/true);
    }


    do_multi_process();

    INFO("======== exit ========");
    return 0;
}
