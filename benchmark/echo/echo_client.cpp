// echo_client.cpp —— 纯 TCP 回显压测客户端（不依赖 wrk2 / HTTP）
// 每个连接一个线程，串行「发满 N 字节 → 收满 N 字节」作为一个往返，
// 直接测量真实的端到端往返延迟（无 coordinated omission 虚高）。
//
// 用法：
//   ./echo_client [host] [port] [连接数] [消息字节数] [测试秒数]
//   连接数为 0 时进入扫描模式，自动扫一组连接数找峰值。
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

struct Result
{
    size_t total = 0;
    size_t errs  = 0;
    std::vector<double> lat;
};

// 单轮压测：conns 个连接打满 seconds 秒，返回 (qps, p50us, p99us, errs)
static void run_once(const char* host, int port, int conns, size_t msg_size,
                     int seconds, double* qps, double* p50, double* p99, size_t* errs)
{
    std::atomic<bool> stop{false};
    std::barrier ready(conns + 1);          // conns 个连接线程 + 主线程

    std::vector<Result> results(conns);

    std::vector<std::thread> threads;
    threads.reserve(conns);
    for (int i = 0; i < conns; ++i)
    {
        threads.emplace_back([&, i]() {
            Result& r = results[i];

            int fd = socket(AF_INET, SOCK_STREAM, 0);
            int one = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port   = htons((uint16_t)port);
            inet_pton(AF_INET, host, &addr.sin_addr);

            bool ok = connect(fd, (sockaddr*)&addr, sizeof(addr)) == 0;
            ready.arrive_and_wait();        // 等所有连接都建好（含失败），再统一开跑
            if (!ok)
            {
                r.errs++;
                close(fd);
                return;
            }

            std::vector<char> msg(msg_size, 'x');
            std::vector<char> buf(msg_size);

            while (!stop.load(std::memory_order_relaxed))
            {
                auto t0 = std::chrono::steady_clock::now();

                size_t off = 0;
                while (off < msg_size)
                {
                    ssize_t n = ::send(fd, msg.data() + off, msg_size - off, MSG_NOSIGNAL);
                    if (n <= 0) { r.errs++; goto done; }
                    off += (size_t)n;
                }
                off = 0;
                while (off < msg_size)
                {
                    ssize_t n = ::recv(fd, buf.data() + off, msg_size - off, 0);
                    if (n <= 0) { r.errs++; goto done; }
                    off += (size_t)n;
                }

                auto t1 = std::chrono::steady_clock::now();
                r.lat.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
                r.total++;
            }
        done:
            close(fd);
        });
    }

    ready.arrive_and_wait();                // 所有连接建好后统一开始计时
    auto start = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop.store(true);
    for (auto& t : threads)
        t.join();
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    std::vector<double> all;
    size_t total = 0, err = 0;
    for (auto& r : results)
    {
        total += r.total;
        err += r.errs;
        all.insert(all.end(), r.lat.begin(), r.lat.end());
    }
    std::sort(all.begin(), all.end());
    auto pct = [&](double p) { return all.empty() ? 0.0 : all[(size_t)(all.size() * p)]; };

    *qps  = total / elapsed;
    *p50  = pct(0.50);
    *p99  = pct(0.99);
    *errs = err;
}

int main(int argc, char* argv[])
{
    const char* host     = argc > 1 ? argv[1] : "127.0.0.1";
    int         port     = argc > 2 ? std::atoi(argv[2]) : 6666;
    int         conns    = argc > 3 ? std::atoi(argv[3]) : 200;
    size_t      msg_size = argc > 4 ? (size_t)std::atoi(argv[4]) : 1024;
    int         seconds  = argc > 5 ? std::atoi(argv[5]) : 5;

    if (conns > 0)
    {
        double qps, p50, p99;
        size_t errs;
        run_once(host, port, conns, msg_size, seconds, &qps, &p50, &p99, &errs);
        std::printf("连接数=%d  消息=%zuB  时长=%ds\n", conns, msg_size, seconds);
        std::printf("QPS=%.0f  P50=%.1fus  P99=%.1fus  错误=%zu\n", qps, p50, p99, errs);
    }
    else
    {
        // 扫描模式：找峰值并发
        static const int list[] = {1, 10, 50, 100, 200, 300, 500, 800, 1000};
        std::printf("扫描模式  消息=%zuB  每档=%ds\n", msg_size, seconds);
        std::printf("%-8s %12s %10s %10s %6s\n", "连接数", "QPS", "P50(us)", "P99(us)", "错误");
        for (int c : list)
        {
            double qps, p50, p99;
            size_t errs;
            run_once(host, port, c, msg_size, seconds, &qps, &p50, &p99, &errs);
            std::printf("%-8d %12.0f %10.1f %10.1f %6zu\n", c, qps, p50, p99, errs);
        }
    }
    return 0;
}
