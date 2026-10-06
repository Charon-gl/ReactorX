#include <unordered_map>
#include <vector>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <functional>
#include "reactor/MainReactor.hpp"
#include "net/Dispatcher.hpp"
#include "core/ReactorEvent.hpp"
#include "http_handler.hpp"
#include "response_data.hpp"

// 为 ConnectionToken 提供哈希，便于作为 unordered_map 的 key
namespace std {
template<>
struct hash<ConnectionToken>
{
    size_t operator()(const ConnectionToken& t) const noexcept
    {
        size_t h = std::hash<uint32_t>{}(t.reactor_id);
        h = h * 0x9e3779b97f4a7c15ULL + std::hash<uint32_t>{}(t.slot);
        h = h * 0x9e3779b97f4a7c15ULL + std::hash<uint64_t>{}(t.generation);
        return h;
    }
};
}

/*
异步模式的框架已经写好了，下一步是先把框架改成同步模式，对同步模式进行压测，然后才是异步模式压测，去观察跨线程开销的代价，以及如何性能优化
*/

class app_server_sync
{
private:
    std::unique_ptr<Dispatcher> dispatcher;
    std::unique_ptr<MainReactor> reactor;

    // 每个 subreactor 线程维护一份自己的 handler 表。
    // 一条连接在建立后会被固定绑定到某个 subreactor 上，
    // 其所有数据都在该 subreactor 线程内被处理，因此用 thread_local
    // 即可避免跨线程竞争，无需加锁。
    static thread_local std::unordered_map<ConnectionToken, std::unique_ptr<http_handler>> handlers;
    // 空闲 handler 池：连接关闭后不销毁，deactivate 后放回此处复用，避免反复申请内存
    static thread_local std::vector<std::unique_ptr<http_handler>> free_handlers;

public:
    app_server_sync(uint16_t port, int core)
        : dispatcher(std::make_unique<Dispatcher>())
    {
        // 统一通过 setter 注册回调与模式，再启动 reactor。
        // 顺序很重要：mode/callback 必须在 reactor（启动 subreactor 线程）之前设置，
        // 否则会漏掉早期事件或产生读写竞态。
        dispatcher->init_mode(mode::sync);
        dispatcher->set_business_callback([this](ByteStream stream){
            submit(std::move(stream));
        });
        dispatcher->set_close_callback([this](ConnectionToken token){
            on_close(token);
        });

        reactor = std::make_unique<MainReactor>(port, core, *dispatcher);
    }


    void submit(ByteStream stream)
    {
        // 查找该连接对应的 http_handler，没有则从池中取一个（或新建）并激活
        auto it = handlers.find(stream.token);
        if(it == handlers.end())
        {
            std::unique_ptr<http_handler> handler;
            if(!free_handlers.empty())
            {
                handler = std::move(free_handlers.back());
                free_handlers.pop_back();
            }
            else
            {
                handler = std::make_unique<http_handler>(dispatcher.get());
                handler->set_push_request([this](const HttpRequest& req){
                    return push_response(req);
                });
            }
            handler->activate(stream.token);

            auto [ins, _] = handlers.emplace(stream.token, std::move(handler));
            it = ins;
        }

        it->second->on_request(std::move(stream));
    }

    // 连接关闭通知（由框架层通过 Dispatcher 回调触发）。
    // 不销毁 handler，而是 deactivate 后放回空闲池复用，
    // 这样 llhttp 解析器和各缓冲区内存可被后续连接重复使用。
    void on_close(ConnectionToken token)
    {
        auto it = handlers.find(token);
        if(it != handlers.end())
        {
            it->second->deactivate();
            free_handlers.push_back(std::move(it->second));
            handlers.erase(it);
        }
    }

    // 业务逻辑：根据 url 路由到不同大小的压测数据。
    // body 直接引用 test_data 中的静态数据（string_view，零拷贝），
    // 不设置 Content-Type，让压测路径上的分配降到最低。
    HttpResponse push_response(const HttpRequest& req)
    {
        HttpResponse resp{200};
        resp.body = test_data::route(req.url);
        return resp;
    }

    void run() { reactor->run(); }
};

thread_local std::unordered_map<ConnectionToken, std::unique_ptr<http_handler>> app_server_sync::handlers;
thread_local std::vector<std::unique_ptr<http_handler>> app_server_sync::free_handlers;


/*
异步模式业务服务器：
- I/O 仍在 SubReactor 线程，业务处理投递到自定义线程池，处理完通过
  Dispatcher::submit_operation（async_port，线程安全）投回框架发送。
- 每条连接对应一个 handler，用 shared_ptr 跨线程共享：业务线程持有引用，
  close 回调只从 map 移除，handler 在所有引用释放后才析构（避免「使用中析构」）。
- llhttp 解析是有状态的，同一连接的数据必须串行，因此用 per-connection 锁串行化；
  不同连接互不干扰，可并发。
*/
class app_server_async
{
private:
    std::unique_ptr<Dispatcher> dispatcher;
    std::unique_ptr<MainReactor> reactor;

    // 业务线程池
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex tasks_mtx;
    std::condition_variable tasks_cv;
    std::atomic<bool> stop{false};

    // 每个连接一个 handler + 串行锁
    struct handler_ctx
    {
        std::shared_ptr<http_handler> handler;
        std::mutex mtx;                 // 串行化同一连接的请求
    };
    std::mutex handlers_mtx;            // 保护 handlers（多个 subreactor 线程 + close 回调并发访问）
    std::unordered_map<ConnectionToken, std::shared_ptr<handler_ctx>> handlers;

public:
    app_server_async(uint16_t port, int core, int worker_num)
        : dispatcher(std::make_unique<Dispatcher>())
    {
        // mode/callback 必须在创建 reactor（启动 subreactor 线程）之前设置
        dispatcher->init_mode(mode::async);
        dispatcher->set_business_callback([this](ByteStream stream){
            on_data(std::move(stream));
        });
        dispatcher->set_close_callback([this](ConnectionToken token){
            on_close(token);
        });

        reactor = std::make_unique<MainReactor>(port, core, *dispatcher);

        start_workers(worker_num);
    }

    ~app_server_async()
    {
        // 先停业务线程（join 前让其处理完剩余任务），再析构 reactor（成员逆序析构）
        stop_workers();
    }

    // ---------- 业务线程池 ----------
    void start_workers(int n)
    {
        workers.reserve(n);
        for (int i = 0; i < n; ++i)
            workers.emplace_back([this]{ worker_loop(); });
    }

    void worker_loop()
    {
        while (true)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(tasks_mtx);
                tasks_cv.wait(lock, [this]{ return stop.load() || !tasks.empty(); });
                if (stop.load() && tasks.empty())
                    return;
                task = std::move(tasks.front());
                tasks.pop();
            }
            task();
        }
    }

    void stop_workers()
    {
        stop.store(true);
        tasks_cv.notify_all();
        for (auto& t : workers)
            if (t.joinable())
                t.join();
    }

    void enqueue(std::function<void()> task)
    {
        {
            std::lock_guard<std::mutex> lock(tasks_mtx);
            tasks.emplace(std::move(task));
        }
        tasks_cv.notify_one();
    }

    // ---------- 框架回调（subreactor 线程） ----------
    void on_data(ByteStream stream)
    {
        auto ctx = get_or_create_ctx(stream.token);
        enqueue([ctx, stream = std::move(stream)]() mutable {
            process(std::move(ctx), std::move(stream));
        });
    }

    void on_close(ConnectionToken token)
    {
        // 只从 map 移除，不销毁/复用 handler：可能仍有业务线程持有 ctx 在处理，
        // 由 shared_ptr 保证 handler 在所有引用释放后才析构。
        std::lock_guard<std::mutex> lock(handlers_mtx);
        handlers.erase(token);
    }

    // ---------- 业务线程执行 ----------
    static void process(std::shared_ptr<handler_ctx> ctx, ByteStream stream)
    {
        std::lock_guard<std::mutex> lock(ctx->mtx);
        ctx->handler->on_request(std::move(stream));
    }

    std::shared_ptr<handler_ctx> get_or_create_ctx(ConnectionToken token)
    {
        std::lock_guard<std::mutex> lock(handlers_mtx);
        auto it = handlers.find(token);
        if (it != handlers.end())
            return it->second;

        auto ctx = std::make_shared<handler_ctx>();
        ctx->handler = std::make_shared<http_handler>(dispatcher.get());
        ctx->handler->set_push_request([this](const HttpRequest& req){
            return push_response(req);
        });
        ctx->handler->activate(token);
        handlers.emplace(token, ctx);
        return ctx;
    }

    // ---------- 业务逻辑（与同步模式一致） ----------
    HttpResponse push_response(const HttpRequest& req)
    {
        HttpResponse resp{200};
        resp.body = test_data::route(req.url);
        return resp;
    }

    void run() { reactor->run(); }
};

void tips()
{
        std::cout << "tips:\n"
                << "arg0 is the execute_name of the process.\n"
                << "arg1 is the port you would like to listen.\n"
                << "arg2 is subreactor_num you need to create.\n"
                << "arg3 is optional:\n"
                << "     omit    -> sync mode (business runs in subreactor thread).\n"
                << "     provide -> async mode, the number = business worker thread num.\n"
                << "examples:\n"
                << "     ./Reactor 6666 8       # sync, 8 subreactors\n"
                << "     ./Reactor 6666 8 4     # async, 8 subreactors + 4 workers\n";
        std::cout << std::endl;
}

int main(int argc, char** argv)
{   
    if(argc < 3)
    {
        std::cerr << "you should input more than 3 argc.\n";
        tips();
        exit(1);
    }
    else if (argc > 4)
    {
        std::cerr << "you should input at most 4 argc.\n";
        tips();
        exit(1);
    }

    if(argc == 3)
    {
        // 同步模式：业务在 subreactor 线程内处理
        app_server_sync server(atoi(argv[1]), atoi(argv[2]));
        server.run();
    }
    else
    {
        // 异步模式：arg3 = 业务 worker 线程数
        int worker_num = atoi(argv[3]);
        app_server_async server(atoi(argv[1]), atoi(argv[2]), worker_num);
        server.run();
    }

    return 0;

}