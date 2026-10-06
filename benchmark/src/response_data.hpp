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
