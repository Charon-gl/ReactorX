// echo_client.cpp —— 纯 TCP 回显压测客户端（epoll 异步版）
// 使用 epoll + 非阻塞 socket，单线程即可驱动数千连接，
// 消除「一线程一连接」模型带来的客户端线程调度开销，
// 避免客户端自身成为瓶颈。
//
// 用法：
//   ./echo_client [host] [port] [连接数] [消息字节数] [测试秒数] [worker线程数]
//   连接数为 0 时进入扫描模式，自动扫一组连接数找峰值。
//   worker 线程数默认 1，设为 0 则自动取 min(conns, 4)。
//
// 可选参数（可穿插在位置参数中）：
//   -c <cpu_list>   worker 线程绑核，如 -c 1,3,5,7（worker 数 > 核数时循环使用）
//   -o <dir>        测试结果输出目录，生成 report.log + results.csv
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <numeric>
#include <pthread.h>
#include <thread>
#include <vector>

// =====================================================================
// 双输出：stdout + 可选日志目录
// -o <dir> 时在目录下生成 report.log（人类可读）和 results.csv（数据）
// =====================================================================
static FILE* g_report = nullptr;  // report.log
static FILE* g_csv    = nullptr;  // results.csv

__attribute__((format(printf, 1, 2)))
static void logf(const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    if (g_report)
    {
        va_start(ap, fmt);
        vfprintf(g_report, fmt, ap);
        va_end(ap);
    }
}

static void logc(int c)
{
    std::putchar(c);
    if (g_report) std::fputc(c, g_report);
}

// =====================================================================
// Stats —— 所有测试指标
// =====================================================================
struct Stats
{
    double qps   = 0;
    double mbps  = 0;

    double p50   = 0;
    double p90   = 0;
    double p95   = 0;
    double p99   = 0;
    double p999  = 0;
    double p9999 = 0;

    double min    = 0;
    double max    = 0;
    double mean   = 0;
    double stddev = 0;

    size_t total_requests = 0;
    size_t request_errors = 0;
    size_t conn_success   = 0;
    size_t conn_total     = 0;
    double elapsed        = 0;

    int    num_workers = 0;
    double cpu_cores   = 0;       // 测试期间消耗的 CPU 核数，> num_workers 时说明不止一个线程在跑
    bool   cpu_warn    = false;   // CPU 核数 ≥ worker 数 * 0.9 → 客户端可能达到瓶颈

    double error_rate() const
    {
        size_t total = total_requests + request_errors;
        return total > 0 ? 100.0 * request_errors / total : 0.0;
    }

    double conn_rate() const
    {
        return conn_total > 0 ? 100.0 * conn_success / conn_total : 0.0;
    }
};

static void write_csv_header(FILE* f)
{
    std::fprintf(f, "timestamp,msg_size,conns,workers,duration_sec,"
                 "qps,mbps,total_requests,"
                 "p50_us,p90_us,p95_us,p99_us,p999_us,p9999_us,"
                 "min_us,max_us,mean_us,stddev_us,"
                 "conn_success,conn_total,request_errors,error_rate_pct,"
                 "cpu_cores,cpu_warn\n");
}

static void write_csv_row(FILE* f, const Stats& s, int conns, size_t msg_size,
                          int seconds)
{
    char tbuf[32];
    time_t now = time(nullptr);
    strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", localtime(&now));

    std::fprintf(f, "%s,%zu,%d,%d,%d,"
                 "%.0f,%.2f,%zu,"
                 "%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,"
                 "%.1f,%.1f,%.1f,%.1f,"
                 "%zu,%zu,%zu,%.3f,"
                 "%.2f,%d\n",
                 tbuf, msg_size, conns, s.num_workers, seconds,
                 s.qps, s.mbps, s.total_requests,
                 s.p50, s.p90, s.p95, s.p99, s.p999, s.p9999,
                 s.min, s.max, s.mean, s.stddev,
                 s.conn_success, s.conn_total, s.request_errors,
                 s.error_rate(),
                 s.cpu_cores, s.cpu_warn ? 1 : 0);
}

// =====================================================================
// 工具函数
// =====================================================================
static double pct(const std::vector<double>& sorted, double p)
{
    if (sorted.empty()) return 0.0;
    size_t idx = (size_t)(sorted.size() * p);
    if (idx >= sorted.size()) idx = sorted.size() - 1;
    return sorted[idx];
}

static void print_sep(int w)
{
    for (int i = 0; i < w; ++i) logc('-');
    logc('\n');
}

static void print_detailed(const Stats& s, int conns, size_t msg_size,
                           int seconds)
{
    logf("\n========== Echo Benchmark Results ==========\n");
    logf("Message: %zu B  |  Connections: %d  |  Duration: %d s  |  "
         "Workers: %d\n",
         msg_size, conns, seconds, s.num_workers);
    print_sep(55);

    logf("Throughput:\n");
    logf("  QPS:          %.0f req/s\n", s.qps);
    logf("  Throughput:   %.2f MB/s\n", s.mbps);
    logf("  Total:        %zu requests\n", s.total_requests);
    logf("  Elapsed:      %.2f s\n", s.elapsed);

    print_sep(55);
    logf("Latency (us):\n");
    logf("  Min:    %10.1f   Mean:   %10.1f   Max:    %10.1f\n",
         s.min, s.mean, s.max);
    logf("  StdDev: %10.1f\n", s.stddev);
    logf("  P50:    %10.1f   P90:    %10.1f   P95:    %10.1f\n",
         s.p50, s.p90, s.p95);
    logf("  P99:    %10.1f   P999:   %10.1f   P9999:  %10.1f\n",
         s.p99, s.p999, s.p9999);

    print_sep(55);
    logf("Reliability:\n");
    logf("  Connections:  %zu/%zu (%.1f%%)\n",
         s.conn_success, s.conn_total, s.conn_rate());
    logf("  Req errors:   %zu/%zu (%.3f%%)\n",
         s.request_errors, s.total_requests + s.request_errors,
         s.error_rate());

    print_sep(55);
    logf("Client self-check:\n");
    logf("  CPU cores used:  %.2f / %d workers\n",
         s.cpu_cores, s.num_workers);
    if (s.cpu_warn)
        logf("  *** WARNING: client CPU saturated, results may be "
             "client-bound ***\n");
    else
        logf("  OK: client has CPU headroom\n");
    print_sep(55);
    logc('\n');
}

static void print_scan_header(size_t msg_size, int seconds)
{
    logf("\nScan Mode  |  Message: %zu B  |  Per-step: %d s\n",
         msg_size, seconds);

    // clang-format off
    const char* hdr =
        "------------------------------------------------------------------------------------------------------------"
        "----------------------------\n"
        " Conn   QPS          MB/s    P50       P95       P99       P999      P9999     "
        "Mean      Max       StdDev    Err%%   CPU    Eff%%\n"
        "------------------------------------------------------------------------------------------------------------"
        "----------------------------";
    // clang-format on
    logf("%s\n", hdr);
}

static void print_scan_row(int conns, const Stats& s,
                           const Stats* prev = nullptr)
{
    double eff = 0;
    if (prev && prev->qps > 0)
        eff = (s.qps / conns) / (prev->qps / (conns > 0 ? conns - 1 : 1)) * 100.0;

    const char* warn = s.cpu_warn ? " !" : "  ";
    logf(" %-5d  %-11.0f  %-7.2f  %-8.1f  %-8.1f  %-8.1f  %-8.1f  %-8.1f  "
         "%-8.1f  %-8.1f  %-8.1f  %-6.2f  %-4.2f%s %-5.0f\n",
         conns, s.qps, s.mbps, s.p50, s.p95, s.p99, s.p999, s.p9999,
         s.mean, s.max, s.stddev, s.error_rate(),
         s.cpu_cores, warn, eff);
}

// =====================================================================
// epoll 连接状态
// =====================================================================
enum ConnState : uint8_t
{
    CS_CONNECTING = 0,
    CS_SENDING    = 1,
    CS_RECEIVING  = 2,
};

struct Connection
{
    int fd = -1;
    ConnState state = CS_CONNECTING;
    bool conn_ok    = false;

    size_t send_off = 0;
    size_t recv_off = 0;

    size_t completed = 0;
    size_t errors    = 0;

    std::chrono::steady_clock::time_point req_start;

    std::vector<char> send_buf;
    std::vector<char> recv_buf;
    std::vector<double> lat;

    explicit Connection(size_t msg_sz)
        : send_buf(msg_sz, 'x'), recv_buf(msg_sz)
    {
    }
};

// =====================================================================
// 修改 epoll 监听事件
// =====================================================================
static void epoll_mod(int epfd, int fd, uint32_t events, void* ptr)
{
    epoll_event ev;
    ev.events   = events;
    ev.data.ptr = ptr;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

// =====================================================================
// 单个 epoll worker：负责 [start_idx, start_idx+count) 区间的连接
// =====================================================================
struct WorkerResult
{
    std::vector<double> lat;
    size_t total   = 0;
    size_t errs    = 0;
    size_t conn_ok = 0;
};

static WorkerResult worker_epoll(const char* host, int port,
                                 int count, size_t msg_size,
                                 const std::atomic<bool>& stop,
                                 std::barrier<>& ready)
{
    WorkerResult wr;

    int epfd = epoll_create1(0);
    if (epfd < 0)
    {
        perror("epoll_create1");
        ready.arrive_and_wait();
        return wr;
    }

    std::vector<Connection> conns;
    conns.reserve(count);
    for (int i = 0; i < count; ++i)
    {
        conns.emplace_back(msg_size);
        Connection& c = conns.back();

        c.fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (c.fd < 0)
        {
            c.errors++;
            continue;
        }

        int one = 1;
        setsockopt(c.fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons((uint16_t)port);
        inet_pton(AF_INET, host, &addr.sin_addr);

        int ret = connect(c.fd, (sockaddr*)&addr, sizeof(addr));
        if (ret < 0 && errno != EINPROGRESS)
        {
            c.errors++;
            close(c.fd);
            c.fd = -1;
            continue;
        }

        epoll_event ev;
        ev.events   = EPOLLOUT;
        ev.data.ptr = &c;
        epoll_ctl(epfd, EPOLL_CTL_ADD, c.fd, &ev);
    }

    ready.arrive_and_wait();

    constexpr int MAX_EVENTS = 256;
    epoll_event events[MAX_EVENTS];
    bool timed_out = false;

    while (!stop.load(std::memory_order_relaxed) && !timed_out)
    {
        int n = epoll_wait(epfd, events, MAX_EVENTS, 10);
        if (n < 0)
        {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < n; ++i)
        {
            Connection* c = (Connection*)events[i].data.ptr;
            uint32_t ev   = events[i].events;

            if (ev & (EPOLLERR | EPOLLHUP))
            {
                c->errors++;
                goto dead;
            }

            switch (c->state)
            {
            case CS_CONNECTING:
                if (ev & EPOLLOUT)
                {
                    int so_err = 0;
                    socklen_t len = sizeof(so_err);
                    getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &so_err, &len);
                    if (so_err == 0)
                    {
                        c->conn_ok = true;
                        c->state   = CS_SENDING;
                        c->req_start = std::chrono::steady_clock::now();
                    }
                    else
                    {
                        c->errors++;
                        goto dead;
                    }
                }
                break;

            case CS_SENDING:
                if (ev & EPOLLOUT)
                {
                    while (c->send_off < msg_size)
                    {
                        ssize_t n =
                            ::send(c->fd, c->send_buf.data() + c->send_off,
                                   msg_size - c->send_off, MSG_NOSIGNAL);
                        if (n > 0)
                        {
                            c->send_off += (size_t)n;
                        }
                        else if (n == 0)
                        {
                            c->errors++;
                            goto dead;
                        }
                        else
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                                break;
                            if (errno == EINTR) continue;
                            c->errors++;
                            goto dead;
                        }
                    }
                    if (c->send_off >= msg_size)
                    {
                        c->send_off = 0;
                        c->state    = CS_RECEIVING;
                        epoll_mod(epfd, c->fd, EPOLLIN, c);
                    }
                }
                break;

            case CS_RECEIVING:
                if (ev & EPOLLIN)
                {
                    while (c->recv_off < msg_size)
                    {
                        ssize_t n =
                            ::recv(c->fd, c->recv_buf.data() + c->recv_off,
                                   msg_size - c->recv_off, 0);
                        if (n > 0)
                        {
                            c->recv_off += (size_t)n;
                        }
                        else if (n == 0)
                        {
                            c->errors++;
                            goto dead;
                        }
                        else
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                                break;
                            if (errno == EINTR) continue;
                            c->errors++;
                            goto dead;
                        }
                    }
                    if (c->recv_off >= msg_size)
                    {
                        auto now = std::chrono::steady_clock::now();
                        c->lat.push_back(
                            std::chrono::duration<double, std::micro>(
                                now - c->req_start)
                                .count());
                        c->completed++;
                        c->recv_off  = 0;
                        c->state     = CS_SENDING;
                        c->req_start = now;
                        epoll_mod(epfd, c->fd, EPOLLOUT, c);
                    }
                }
                break;
            }
            continue;

        dead:
            epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, nullptr);
            close(c->fd);
            c->fd = -1;
        }
    }

    for (auto& c : conns)
    {
        if (c.fd >= 0)
        {
            epoll_ctl(epfd, EPOLL_CTL_DEL, c.fd, nullptr);
            close(c.fd);
        }
        wr.total += c.completed;
        wr.errs += c.errors;
        if (c.conn_ok) wr.conn_ok++;
        wr.lat.insert(wr.lat.end(), c.lat.begin(), c.lat.end());
    }
    close(epfd);
    return wr;
}

// =====================================================================
// run_once —— 汇总一次压测
// =====================================================================
static Stats run_once(const char* host, int port, int conns, size_t msg_size,
                      int seconds, int num_workers,
                      const std::vector<int>& cpus)
{
    if (num_workers <= 0)
        num_workers = std::max(1, std::min(conns, 4));
    if (num_workers > conns)
        num_workers = conns;

    std::atomic<bool> stop{false};
    std::barrier ready(num_workers + 1);

    std::vector<WorkerResult> wrs(num_workers);
    std::vector<std::thread>  threads;
    threads.reserve(num_workers);

    int base  = conns / num_workers;
    int extra = conns % num_workers;
    for (int w = 0; w < num_workers; ++w)
    {
        int cnt = base + (w < extra ? 1 : 0);
        threads.emplace_back([&, w, cnt]() {
            if (!cpus.empty())
            {
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(cpus[w % cpus.size()], &cpuset);
                pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
            }
            wrs[w] = worker_epoll(host, port, cnt, msg_size, stop, ready);
        });
    }

    ready.arrive_and_wait();

    clock_t cpu_start = clock();
    auto t0 = std::chrono::steady_clock::now();

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop.store(true);

    for (auto& t : threads)
        t.join();

    auto t1 = std::chrono::steady_clock::now();
    clock_t cpu_end = clock();

    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    double cpu_sec = (double)(cpu_end - cpu_start) / CLOCKS_PER_SEC;

    std::vector<double> all;
    size_t total = 0, errs = 0, conn_ok = 0;
    for (auto& wr : wrs)
    {
        total += wr.total;
        errs += wr.errs;
        conn_ok += wr.conn_ok;
        all.insert(all.end(), wr.lat.begin(), wr.lat.end());
    }

    std::sort(all.begin(), all.end());

    Stats s;
    s.qps            = elapsed > 0 ? total / elapsed : 0;
    s.mbps           = (s.qps * msg_size) / (1024.0 * 1024.0);
    s.elapsed        = elapsed;
    s.total_requests = total;
    s.request_errors = errs;
    s.conn_success   = conn_ok;
    s.conn_total     = conns;
    s.num_workers    = num_workers;
    s.cpu_cores      = cpu_sec / elapsed;
    s.cpu_warn       = (s.cpu_cores >= num_workers * 0.9);

    s.p50   = pct(all, 0.50);
    s.p90   = pct(all, 0.90);
    s.p95   = pct(all, 0.95);
    s.p99   = pct(all, 0.99);
    s.p999  = pct(all, 0.999);
    s.p9999 = pct(all, 0.9999);

    if (!all.empty())
    {
        s.min  = all.front();
        s.max  = all.back();

        double sum = std::accumulate(all.begin(), all.end(), 0.0);
        s.mean     = sum / all.size();

        double sq = 0.0;
        for (double v : all)
        {
            double d = v - s.mean;
            sq += d * d;
        }
        s.stddev = std::sqrt(sq / all.size());
    }

    return s;
}

// =====================================================================
// main
// =====================================================================
int main(int argc, char* argv[])
{
    const char*        host        = "127.0.0.1";
    int                port        = 6666;
    int                conns       = 200;
    size_t             msg_size    = 1024;
    int                seconds     = 5;
    int                num_workers = 0;
    const char*        outdir      = nullptr;
    std::vector<int>   cpus;

    int pos = 0;
    for (int i = 1; i < argc; ++i)
    {
        if (std::strcmp(argv[i], "-o") == 0 && i + 1 < argc)
        {
            outdir = argv[++i];
        }
        else if (std::strcmp(argv[i], "-c") == 0 && i + 1 < argc)
        {
            cpus.clear();
            const char* s = argv[++i];
            while (*s)
            {
                char* end;
                long v = strtol(s, &end, 10);
                if (end != s)
                {
                    cpus.push_back((int)v);
                    s = end;
                }
                while (*s == ',' || *s == ' ') ++s;
            }
        }
        else
        {
            switch (pos)
            {
            case 0: host        = argv[i]; break;
            case 1: port        = std::atoi(argv[i]); break;
            case 2: conns       = std::atoi(argv[i]); break;
            case 3: msg_size    = (size_t)std::atoi(argv[i]); break;
            case 4: seconds     = std::atoi(argv[i]); break;
            case 5: num_workers = std::atoi(argv[i]); break;
            }
            ++pos;
        }
    }

    if (outdir)
    {
        mkdir(outdir, 0755);

        char path[512];
        snprintf(path, sizeof(path), "%s/report.log", outdir);
        g_report = std::fopen(path, "w");
        snprintf(path, sizeof(path), "%s/results.csv", outdir);
        g_csv    = std::fopen(path, "w");

        if (!g_report || !g_csv)
        {
            std::fprintf(stderr, "无法创建输出文件于: %s\n", outdir);
            if (g_report) std::fclose(g_report);
            if (g_csv)    std::fclose(g_csv);
            return 1;
        }

        write_csv_header(g_csv);
    }

    {
        char tbuf[32];
        time_t now = time(nullptr);
        strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", localtime(&now));
        if (!cpus.empty())
        {
            std::string cpu_str;
            for (size_t i = 0; i < cpus.size(); ++i)
            {
                if (i) cpu_str += ',';
                cpu_str += std::to_string(cpus[i]);
            }
            logf("=== %s  echo_client  host=%s port=%d  cpus=%s ===\n",
                 tbuf, host, port, cpu_str.c_str());
        }
        else
        {
            logf("=== %s  echo_client  host=%s port=%d ===\n",
                 tbuf, host, port);
        }
    }

    if (conns > 0)
    {
        int nw = num_workers > 0 ? num_workers
                                 : std::max(1, std::min(conns, 4));
        Stats s = run_once(host, port, conns, msg_size, seconds, nw, cpus);
        print_detailed(s, conns, msg_size, seconds);
        if (g_csv) write_csv_row(g_csv, s, conns, msg_size, seconds);
    }
    else
    {
        static const int list[] = {1,   10,  50,  100, 200,
                                   300, 500, 800, 1000};
        print_scan_header(msg_size, seconds);

        const Stats* prev = nullptr;
        Stats prev_s;
        for (int c : list)
        {
            int nw = num_workers > 0 ? num_workers
                                     : std::max(1, std::min(std::max(c, 1), 4));
            Stats s = run_once(host, port, c, msg_size, seconds, nw, cpus);
            print_scan_row(c, s, prev);
            prev_s = s;
            prev   = &prev_s;
            if (g_csv) write_csv_row(g_csv, s, c, msg_size, seconds);
        }
        logc('\n');
    }

    if (g_report) { std::fclose(g_report); g_report = nullptr; }
    if (g_csv)    { std::fclose(g_csv);    g_csv    = nullptr; }
    return 0;
}