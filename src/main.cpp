#include "common.h"


struct final_prepare_page_cache_context_t
{
    done_event map_customer_done { };
    done_event map_orders_done { };
    done_event map_lineitem_done { };

    done_event can_exit { };
};


static void detect_preparing_page_cache() noexcept
{
    ASSERT(g_orders_file.file_size > 0);
    ASSERT(g_customer_file.file_size > 0);
    ASSERT(g_lineitem_file.file_size > 0);

    g_is_preparing_page_cache = true;

    const uint32_t nr_2mb = mem_get_nr_hugepages_2048kB();
    DEBUG("nr_hugepages (2048kB): %u", nr_2mb);

    if (nr_2mb != CONFIG_EXTRA_HUGE_PAGES) {
        // Try to allocate CONFIG_EXTRA_HUGE_PAGES huge pages
        bool success = mem_set_nr_hugepages_2048kB(CONFIG_EXTRA_HUGE_PAGES);
        CHECK(success, "Can't write to /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages");

        uint32_t real_nr_2mb = mem_get_nr_hugepages_2048kB();
        if (real_nr_2mb != CONFIG_EXTRA_HUGE_PAGES) {
            ASSERT(real_nr_2mb < CONFIG_EXTRA_HUGE_PAGES);
            INFO("required %u huge pages, but actually allocated %u: drop page caches and try again",
                 CONFIG_EXTRA_HUGE_PAGES, real_nr_2mb);
            success = mem_drop_cache();
            CHECK(success, "Can't drop page caches");

            success = mem_set_nr_hugepages_2048kB(CONFIG_EXTRA_HUGE_PAGES);
            CHECK(success, "Can't write to /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages");

            real_nr_2mb = mem_get_nr_hugepages_2048kB();
            CHECK(real_nr_2mb == CONFIG_EXTRA_HUGE_PAGES,
                "Still can't allocate enough huge pages: required %u huge pages, but actually allocated %u",
                CONFIG_EXTRA_HUGE_PAGES, real_nr_2mb);
        }
    }

    do {
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

        g_is_preparing_page_cache = false;

    } while(false);

    INFO("g_is_preparing_page_cache: %d", (int)g_is_preparing_page_cache);
}


[[noreturn]]
static void final_prepare_page_cache_customer(final_prepare_page_cache_context_t* const ctx) noexcept
{
    INFO("final_prepare_page_cache_customer() starts");

    ASSERT(g_customer_file.fd > 0);
    ASSERT(g_customer_file.file_size > 0);

    void* const ptr = my_mmap(
        g_customer_file.file_size,
        PROT_READ,
        MAP_PRIVATE | MAP_POPULATE | MAP_LOCKED,
        g_customer_file.fd,
        0);
    CHECK(ptr != nullptr);
    INFO("customer txt mapped to %p", ptr);

    C_CALL(mlock(ptr, g_customer_file.file_size));
    ctx->map_customer_done.mark_done();

    INFO("done final_prepare_page_cache_customer()");

    ctx->can_exit.wait_done();
    exit(0);
}

[[noreturn]]
static void final_prepare_page_cache_orders(final_prepare_page_cache_context_t* const ctx) noexcept
{
    INFO("final_prepare_page_cache_orders() starts");

    ASSERT(g_orders_file.fd > 0);
    ASSERT(g_orders_file.file_size > 0);

    // Try to populate orders txt file after customer and lineitem file done
    // So orders txt file should occupy index's page cache
    ctx->map_customer_done.wait_done();
    ctx->map_lineitem_done.wait_done();

    void* const ptr = my_mmap(
        g_orders_file.file_size,
        PROT_READ,
        MAP_SHARED | MAP_POPULATE | MAP_LOCKED,
        g_orders_file.fd,
        0);
    CHECK(ptr != nullptr);
    INFO("orders txt mapped to %p", ptr);

    C_CALL(mlock(ptr, g_orders_file.file_size));
    ctx->map_orders_done.mark_done();

    INFO("done final_prepare_page_cache_orders()");

    ctx->can_exit.wait_done();
    exit(0);
}

[[noreturn]]
static void final_prepare_page_cache_lineitem(final_prepare_page_cache_context_t* const ctx) noexcept
{
    INFO("final_prepare_page_cache_lineitem() starts");

    ASSERT(g_lineitem_file.fd > 0);
    ASSERT(g_lineitem_file.file_size > 0);

    void* const ptr = my_mmap(
        g_lineitem_file.file_size,
        PROT_READ,
        MAP_SHARED | MAP_POPULATE | MAP_LOCKED,
        g_lineitem_file.fd,
        0);
    CHECK(ptr != nullptr);
    INFO("lineitem txt mapped to %p", ptr);

    C_CALL(mlock(ptr, g_lineitem_file.file_size));
    ctx->map_lineitem_done.mark_done();

    INFO("done final_prepare_page_cache_lineitem()");

    ctx->can_exit.wait_done();
    exit(0);
}



static void final_prepare_page_cache() noexcept
{
    ASSERT(g_index_directory_fd > 0);
    ASSERT(g_customer_file.file_size > 0);
    ASSERT(g_customer_file.fd > 0);
    ASSERT(g_orders_file.file_size > 0);
    ASSERT(g_orders_file.fd > 0);
    ASSERT(g_lineitem_file.file_size > 0);
    ASSERT(g_lineitem_file.fd > 0);

    final_prepare_page_cache_context_t* const ctx = (final_prepare_page_cache_context_t*)mmap_allocate_page4k_shared(
        sizeof(final_prepare_page_cache_context_t));
    ASSERT(ctx != nullptr);
    new (ctx) final_prepare_page_cache_context_t;

    //
    // Fork 3 child process to load customer, orders, lineitem file
    //
    if (fork() == 0) {  // child 1
        final_prepare_page_cache_customer(ctx);
    }
    else if (fork() == 0) {  // child 2
        final_prepare_page_cache_orders(ctx);
    }
    else if (fork() == 0) {  // child 3
        final_prepare_page_cache_lineitem(ctx);
    }
    else {  // parent process
        ctx->map_customer_done.wait_done();
        ctx->map_orders_done.wait_done();
        ctx->map_lineitem_done.wait_done();

        // Now clear all other page caches!
        INFO("final_prepare_page_cache: now mem_drop_cache()");
        const bool success = mem_drop_cache();
        if (!success) {
            WARN("final_prepare_page_cache: mem_drop_cache() failed! Performance may drop significantly");
        }

        // Now we allows exit...
        INFO("final_prepare_page_cache: now can_exit");
        ctx->can_exit.mark_done();

        for (uint32_t i = 0; i < 3; ++i) {
            int status;
            C_CALL(wait(&status));
            CHECK(WIFEXITED(status), "final_prepare_page_cache: One of child process not normally exits");
            CHECK(WEXITSTATUS(status) == 0, "final_prepare_page_cache: One of child process exits with %d", WEXITSTATUS(status));
        }
        INFO("final_prepare_page_cache: 3 child processes exits normally");
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
    // Create worker thread
    //
    std::thread worker_thread;
    std::thread unloader_thread;
    std::thread pretopn_thread;
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

//         unloader_thread = std::thread([&]() {
// #if ENABLE_PIN_THREAD_TO_CPU
//             pin_thread_to_cpu_core(g_id);
// #endif
// #if ENABLE_ATTEMPT_SCHED_FIFO
//             set_thread_fifo_scheduler(CONFIG_SCHED_FIFO_UNLOADER_NICE);
// #endif
//             (g_is_creating_index ? fn_unloader_thread_create_index : fn_unloader_thread_use_index)();
//         });
        if (!g_is_creating_index) {
            pretopn_thread = std::thread([&]() {
#if ENABLE_PIN_THREAD_TO_CPU
                pin_thread_to_cpu_core(g_id);
#endif
#if ENABLE_ATTEMPT_SCHED_FIFO
                set_thread_fifo_scheduler(CONFIG_SCHED_FIFO_UNLOADER_NICE);
#endif
                fn_pretopn_thread_use_index(g_id);
            });
        }
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
    //
    worker_thread.join();
    // unloader_thread.join();
    
    if (!g_is_creating_index) {
        pretopn_thread.join();
    }

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
            umask(0);  // always succeeds
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

        __open_file_read(customer_text_path, &g_customer_file);
        __open_file_read(orders_text_path, &g_orders_file);
        __open_file_read(lineitem_text_path, &g_lineitem_file);

        detect_preparing_page_cache();
    }
    else {
        g_is_preparing_page_cache = false;

        // Detect whether we are preparing page cache by check the environment variable
        const char* const env_value = getenv(ENV_NAME_PREPARING_PAGE_CACHE);
        if (env_value != nullptr) {
            if (strcmp(env_value, ENV_VALUE_PREPARING_PAGE_CACHE) == 0) {
                g_is_preparing_page_cache = true;
                INFO("use_index: g_is_preparing_page_cache is true!");
            }
        }

        if (g_is_preparing_page_cache) {
            const char* const customer_text_path = argv[1];
            const char* const orders_text_path = argv[2];
            const char* const lineitem_text_path = argv[3];

            __open_file_read(customer_text_path, &g_customer_file);
            __open_file_read(orders_text_path, &g_orders_file);
            __open_file_read(lineitem_text_path, &g_lineitem_file);
        }
    }


    //
    // Initialize g_shared
    //
    {
        g_shared = (shared_information_t*)mmap_allocate_page4k_shared(sizeof(shared_information_t));
        DEBUG("g_shared: %p", g_shared);

        new (g_shared) shared_information_t;
    }


    //
    // A few common initializations
    //
    {
        // Get query count
        g_query_count = __parse_u32<'\0'>(argv[4]);
        DEBUG("g_query_count: %u", g_query_count);
        g_argv_queries = &argv[5];
        if (!g_is_creating_index && __unlikely(g_query_count == 0) && !g_is_preparing_page_cache) {
            INFO("!is_creating_index && g_query_count == 0 && !g_is_preparing_page_cache: fast exit!");
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
        if (!g_is_creating_index) {
            g_shared->pretopn_sync_barrier.init(g_total_process_count, /*use_multi_process*/true);
        }
    }


    if (g_is_creating_index || g_query_count > 0) {
        do_multi_process();
        if (g_id != (uint32_t)-1) {
            INFO("[%u] child process exits", g_id);
            return 0;
        }
    }
    else {
        ASSERT(!g_is_creating_index);  // currently use_index
        ASSERT(g_query_count == 0);
        ASSERT(g_is_preparing_page_cache);  // we must be preparing page cache
    }


    //
    // Exit main process now
    //
    if (g_is_creating_index) {
        if (g_is_preparing_page_cache) {
            C_CALL(setenv(ENV_NAME_PREPARING_PAGE_CACHE, ENV_VALUE_PREPARING_PAGE_CACHE, true));
        }

        char** args = new char*[argc + 1];
        for (int i = 0; i < argc; ++i) {
            args[i] = argv[i];
        }
        args[argc] = nullptr;
        INFO("================ now exec! ================");
        C_CALL(execv(argv[0], args));
    }
    else {
        if (g_is_preparing_page_cache) {
            INFO("now prepare page cache!");
            final_prepare_page_cache();
        }

        INFO("================ now exit! ================");
    }

    return 0;
}
