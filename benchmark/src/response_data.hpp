#pragma once
#include <string>
#include <random>
#include <chrono>
#include <thread>

// ═══════════════════════════════════════════════════════════════════════════
// 压测用固定大小响应体。
//
// 预置了几档数据，进程首次访问时才初始化一次，之后返回引用复用，
// 避免每次请求都重新分配内存。
//
// 内容说明：
//   - 小型数据为可读文本，便于用 curl 直接验证服务连通性；
//   - 大型数据为随机字节（0~255，不可压缩），用于模拟真实二进制负载。
//
// 使用方式（在 app_server_sync::push_response 中按 url 路由）：
//   HttpResponse push_response(const HttpRequest& req) {
//       HttpResponse resp{200};
//       resp.headers.emplace_back("Content-Type", "application/octet-stream");
//       resp.body = test_data::body_for(req.url);
//       return resp;
//   }
//
// URL 与数据档位对应关系：
//   /hello      -> 小数据  (11 B)  
//   /data/1k    -> 1 KB
//   /data/4k    -> 4 KB
//   /data/8k    -> 8 KB
//   /data/16k   -> 16 KB   
//   /data/64k   -> 64 KB   (大型数据上限)
//   其他        -> 小数据
//
// 压测示例：
//   wrk -c100 -t4 -d30s http://127.0.0.1:6666/data/16k
//   wrk -c100 -t4 -d30s http://127.0.0.1:6666/data/1k
// ═══════════════════════════════════════════════════════════════════════════

namespace test_data {

// 预生成一档随机数据：共 total 字节，每个字节取 0~255 的随机值
inline std::string make_body(size_t total)
{
    std::string s;
    s.reserve(total);

    std::random_device rd;
    std::mt19937 rng{rd()};
    std::uniform_int_distribution<int> dist(0, 255);

    for (size_t i = 0; i < total; ++i)
        s.push_back(static_cast<char>(dist(rng)));

    return s;
}

// 各档静态数据（首次调用时初始化一次，之后复用）
inline const std::string& small() { static const std::string s = "hello test\n";     return s; }
inline const std::string& k1()    { static const std::string s = make_body(1024);    return s; }
inline const std::string& k4()    { static const std::string s = make_body(4096);    return s; }
inline const std::string& k8()    { static const std::string s = make_body(8192);    return s; }
inline const std::string& k16()   { static const std::string s = make_body(16384);   return s; }
inline const std::string& k64()   { static const std::string s = make_body(65536);   return s; }

// 直接按大小取（单位字节），未命中回退为小数据
inline const std::string& get(size_t bytes)
{
    if (bytes == 1024)  return k1();
    if (bytes == 4096)  return k4();
    if (bytes == 8192)  return k8();
    if (bytes == 16384) return k16();
    if (bytes == 65536) return k64();
    return small();
}

// 按 url 路由到对应档位
inline const std::string& body_for(const std::string& url)
{
    if (url == "/data/1k")  return k1();
    if (url == "/data/4k")  return k4();
    if (url == "/data/8k")  return k8();
    if (url == "/data/16k") return k16();
    if (url == "/data/64k") return k64();
    return small();
}

// ── CPU 密集负载：可调迭代次数（xorshift 混洗，volatile 防优化）────
// 默认 300000 次 ≈ 1ms @3.8GHz，具体耗时需实测校准后调整默认值
inline void compute_busy_work(int iterations)
{
    volatile uint64_t x = 0x9e3779b97f4a7c15ULL;
    for (int i = 0; i < iterations; ++i) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
    }
    (void)x;
}

// ── I/O 等待负载：sleep 指定毫秒 ─────────────────────────────────
inline void delay_wait(int ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// ── 简单 query 参数解析：从 url 取 ?key=<int>，缺省用 def ──────
inline int query_int(const std::string& url, const std::string& key, int def)
{
    auto pos = url.find(key + "=");
    if (pos == std::string::npos) return def;
    pos += key.size() + 1;
    int val = 0;
    while (pos < url.size() && url[pos] >= '0' && url[pos] <= '9') {
        val = val * 10 + (url[pos] - '0');
        ++pos;
    }
    return val ? val : def;
}

// ── 业务路由：纯回包 / compute（CPU 密集）/ delay（I/O 等待）────
// 用于模拟三类业务负载，证明框架不绑定业务、可替换协议/负载。
inline const std::string& route(const std::string& url)
{
    if (url.rfind("/compute", 0) == 0) {
        compute_busy_work(query_int(url, "n", 300000));
        return small();
    }
    if (url.rfind("/delay", 0) == 0) {
        delay_wait(query_int(url, "ms", 1));
        return small();
    }
    return body_for(url);
}

} // namespace test_data

// ═══════════════════════════════════════════════════════════════════════════
// 共享业务层：sync/async 模式共用的阻塞业务函数
//
// 两类阻塞负载：
//   /fast           → sleep 1ms（短 IO 等待）
//   /slow           → sleep 300ms（长 IO 等待，可通过 ?ms=N 调整）
//   /compute?n=N    → CPU 密集循环（xorshift，默认 300000 次 ≈1ms）
//   其他路径          → 走原 test_data::route()
//
// 两个模式调用完全相同的函数，唯一差异是执行线程：
//   - sync  模式：subreactor 线程执行 → 阻塞 IO 线程
//   - async 模式：worker 线程执行     → IO 线程不被阻塞
// ═══════════════════════════════════════════════════════════════════════════

#include <atomic>
#include <cstdlib>
#include "http_handler.hpp"

namespace business {

// ── 线程类型标记（thread_local，无锁）────────────────────────────
// sync 模式在 subreactor 线程首次执行业务时标记，
// async 模式在 worker 线程启动时标记。
// 通过函数内 static thread_local 引用实现，无需额外 .cpp 定义。
inline const char*& _thread_type_ref()
{
    thread_local const char* val = nullptr;
    return val;
}

inline void mark_thread_subreactor()
{
    _thread_type_ref() = "subreactor";
}

inline void mark_thread_worker()
{
    _thread_type_ref() = "worker";
}

inline const char* current_thread_type()
{
    const char* t = _thread_type_ref();
    return t ? t : "unknown";
}

// ── 共享阻塞业务入口 ────────────────────────────────────────────
// sync 和 async 都调用此函数，仅在各自配置的线程上执行。
inline HttpResponse process_request(const HttpRequest& req)
{
    HttpResponse resp{200};

    if (req.url == "/fast")
    {
        test_data::delay_wait(1);
        resp.body = test_data::small();
    }
    else if (req.url.rfind("/slow", 0) == 0)
    {
        const int ms = test_data::query_int(req.url, "ms", 300);
        test_data::delay_wait(ms);
        resp.body = test_data::small();
    }
    else if (req.url.rfind("/compute", 0) == 0)
    {
        test_data::compute_busy_work(test_data::query_int(req.url, "n", 300000));
        resp.body = test_data::small();
    }
    else
    {
        resp.body = test_data::route(req.url);
    }

    // 响应头标注执行线程类型，便于 curl 验证
    resp.headers.emplace_back("X-Exec-Thread", current_thread_type());

    return resp;
}

// ── 低开销埋点（可关闭、可采样）─────────────────────────────────
// 通过环境变量 REACTORX_PROFILE 控制：
//   未设置  → 完全关闭，函数体为空，编译器可优化掉
//   "1"    → 100% 采样
//   "N"    → N% 采样（与 (counter % 100 < N) 比较）
//
// thread_local ring buffer，无锁写入；使用 steady_clock 单调时钟。
namespace profile {

inline bool enabled()
{
    static const int val = []() -> int {
        const char* env = std::getenv("REACTORX_PROFILE");
        if (!env || env[0] == '\0') return 0;
        return std::atoi(env);
    }();
    return val > 0;
}

inline int sample_rate()
{
    static const int val = []() -> int {
        const char* env = std::getenv("REACTORX_PROFILE");
        if (!env || env[0] == '\0') return 0;
        int v = std::atoi(env);
        return (v > 0 && v <= 100) ? v : 100;
    }();
    return val;
}

struct Sample
{
    uint64_t start_ns;
    uint64_t end_ns;
    bool is_blocking;   // slow 或 compute 为 true
};

inline thread_local Sample tls_samples[4096];
inline thread_local uint32_t tls_sample_idx = 0;
inline thread_local uint32_t tls_req_counter = 0;

inline void record_start(bool is_blocking)
{
    if (!enabled()) return;
    ++tls_req_counter;
    if (static_cast<int>(tls_req_counter % 100) >= sample_rate()) return;
    auto& s = tls_samples[tls_sample_idx % 4096];
    s.start_ns = std::chrono::steady_clock::now().time_since_epoch().count();
    s.is_blocking = is_blocking;
}

inline void record_end()
{
    if (!enabled()) return;
    if (static_cast<int>(tls_req_counter % 100) >= sample_rate()) return;
    auto& s = tls_samples[tls_sample_idx % 4096];
    s.end_ns = std::chrono::steady_clock::now().time_since_epoch().count();
    ++tls_sample_idx;
}

} // namespace profile

} // namespace business