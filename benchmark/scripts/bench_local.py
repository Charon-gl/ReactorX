#!/usr/bin/env python3
"""
bench_local.py —— ReactorX 本机性能测试脚本（重制版）

针对本机 i7-13650HX 核拓扑重写，吸收历史教训：
  - 饱和阈值按「客户端核数 × 95%」计算（psutil 多线程满载 = 核数 × 100%）
  - 峰值 -R 按「每核 QPS」预估后在峰值附近密集取值（不再固定倍增扫全量）
  - bind failed 自动 sleep 后重试
  - 区分「可用吞吐」（P99 毫秒级）与「饱和吞吐」（延迟崩掉）
  - pipeline（HTTP/1.1 管道）作为独立连接模式，测流式解析能力
  - perf 硬件计数 sync / async 都采样

本机核拓扑（i7-13650HX）：
  - 6 个 P 核（超线程，逻辑核 0-11）：物理核取偶数逻辑核 0,2,4,6,8,10
  - 8 个 E 核（无超线程，逻辑核 12-19）
  - 共 14 物理核 / 20 逻辑核，单 NUMA

用法：
    sudo python3 bench_local.py               # 全量
    sudo python3 bench_local.py --scene sync-4   # 只跑某场景（调试）
"""

import subprocess
import time
import os
import sys
import csv
import re
import signal
import resource
import atexit
import argparse
import shutil
import socket
from pathlib import Path
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════
#                             配置区
# ═══════════════════════════════════════════════════════════════════

SCRIPT_DIR    = Path(__file__).resolve().parent
BENCHMARK_DIR = SCRIPT_DIR.parent
PROJECT_DIR   = BENCHMARK_DIR.parent
SERVER_BIN    = str(PROJECT_DIR / "build" / "benchmark" / "Reactor")

# wrk2 二进制（支持 -R 定速 + HDR histogram；原版 wrk 不支持，优先用本地编译的 wrk2）
WRK2_BIN = SCRIPT_DIR / "wrk2" / "wrk"
WRK_BIN  = str(WRK2_BIN) if WRK2_BIN.exists() else "wrk"

# ── 本机核拓扑（i7-13650HX）────────────────────────────────────────
# P 核带超线程，每个物理核取一个逻辑核（偶数），避免兄弟逻辑核争抢
P_CORES = [0, 2, 4, 6, 8, 10]      # 6 个 P 物理核
E_CORES = list(range(12, 20))      # 8 个 E 核

# ── 场景：(name, model, subreactor数, worker数) ─────────────────────
# 本机 P 核仅 6 个，最多测 4-sub。命名说明：sync-N 的服务端核数 = main + N sub = N+1 个 P 核。
SCENARIOS = [
    ("sync-1",  "sync",  1, None),   # 2 P 核（扩展性参考）
    ("sync-4",  "sync",  4, None),   # 5 P 核
    ("async-4", "async", 4, 4),      # 5 P 核 + 4 E 核 worker（异步对比）
]

# ── 负载（业务类型）：name -> (url, 每核 QPS 预估) ─────────────────
# async plan 的 compute 基准由 REACTORX_COMPUTE_N 指定，派生约 0.25/1/4 倍工作量。
# est_per_core 按本机历史拐点校准（014131 拐点 11B=220k / 16KB=140k / 64KB=60k，除以 5 核），
# 让历史拐点落在候选中间（1.0×est 附近），上界留约 40% 余量。
# 64KB 的 2ms 历史拐点（60k）偏低（自适应阈值下真实拐点更高），改按极限吞吐 ~134k 估算。
COMPUTE_BASE_N = int(os.environ.get("REACTORX_COMPUTE_N", "700000"))
LOADS = {
    "11B":     ("/",          57000),
    "16KB":    ("/data/16k",  36000),
    "64KB":    ("/data/64k",  24000),
    "compute-light":  (f"/compute?n={max(1, COMPUTE_BASE_N // 4)}", 4000),
    "compute-medium": (f"/compute?n={COMPUTE_BASE_N}",              1000),
    "compute-heavy":  (f"/compute?n={COMPUTE_BASE_N * 4}",           250),
    "delay-10ms":     ("/delay?ms=10",  100),
    "delay-50ms":     ("/delay?ms=50",   20),
    "delay-100ms":    ("/delay?ms=100", 10),
}

# ── 连接模式：keepalive（串行一请求一响应）/ pipeline（连续 N 请求）──
# pipeline 服务端零改动（llhttp 循环解析），客户端用 lua 拼接连续流
ALL_MODES = ["keepalive", "pipeline"]

# ── 测试计划 ──────────────────────────────────────────────────────
# baseline：    sync-4 × keepalive × 11B/16KB/64KB —— 框架每请求开销 + 带宽瓶颈
# pipeline：    sync-4 × {keepalive, pipeline} × 64KB —— 批处理/队头阻塞
# async：       {sync-4, async-4} × keepalive × 11B —— 本地 sync/async 对比（compute/delay 在云端测）
# scalability： sync-1/4 × keepalive × 16KB —— 本机扩展性（主力扩展性在云上）
PLANS = {
    "baseline":    {"scenes": ["sync-4"],            "loads": ["11B", "16KB", "64KB"], "modes": ["keepalive"]},
    "pipeline":    {"scenes": ["sync-4"],            "loads": ["64KB"],               "modes": ["keepalive", "pipeline"]},
    "async":       {"scenes": ["sync-4", "async-4"], "loads": ["11B"],               "modes": ["keepalive"]},
    "scalability": {"scenes": ["sync-1", "sync-4"],  "loads": ["16KB"],               "modes": ["keepalive"]},
}

ASYNC_REFERENCE_THREADS = 4  # 本机 async plan 的配对基准 sync-4
ASYNC_COMPARISON_FACTORS = (0.25, 0.5, 1.25)


def configure_async_comparison_rates(reference_threads):
    """按参考同步业务线程的理论服务时间生成固定速率档位。"""
    rates = {"11B": [100000, 250000, 400000]}
    for load_name, relative_cost in (
        ("compute-light", 0.25),
        ("compute-medium", 1.0),
        ("compute-heavy", 4.0),
    ):
        capacity = reference_threads * 1000 / relative_cost
        rates[load_name] = [max(1, int(capacity * factor / 10) * 10)
                            for factor in ASYNC_COMPARISON_FACTORS]
    for delay_ms in (10, 50, 100):
        capacity = reference_threads * 1000 / delay_ms
        rates[f"delay-{delay_ms}ms"] = [max(1, int(capacity * factor / 10) * 10)
                                        for factor in ASYNC_COMPARISON_FACTORS]
    return rates


ASYNC_COMPARISON_RATES = configure_async_comparison_rates(ASYNC_REFERENCE_THREADS)

PIPELINE_DEPTH = 3            # pipeline 深度（本机精简版暂不用 pipeline，保留供后续）
WRK_CONC       = 2000         # wrk 固定连接数
ASYNC_COMPARISON_CONCURRENCY = {"11B": WRK_CONC}
ASYNC_COMPARISON_CONCURRENCY.update({name: 100 for name in ASYNC_COMPARISON_RATES if name != "11B"})
WRK_THREADS    = 0            # 0 表示自动 = wrk 核数
DURATION       = 30           # 拐点处正式测试时长（秒）
ASYNC_LONG_LOAD_DURATION = 60  # async plan 中 compute/delay 的正式单次时长（秒）
REPEAT         = 3            # 拐点处重复次数，取中位数（平滑偶发噪声）
SCAN_DUR       = 15           # 峰值扫描每档时长（秒），15s 让尾延迟 P99 更稳
KNEE_RATIO     = 2            # 拐点突变倍数：P99 涨到健康基线的 2 倍即视为过载（相对突变，自适应任何负载）
KNEE_FLOOR_US  = 5_000        # 阈值下限：健康基线太小时（如 11B）防止噪声误判（wrk2 协调遗漏修正虚高约 1ms）
MAX_ACCEPTABLE_P99_US = 10_000  # 有效上界的硬约束：P99 不超过 10ms
MAX_SCAN_RATE = 10_000_000     # 防止扫描无界增长；达到上限仍稳定则标记为未找到上界
SCAN_CONFIRMATIONS = 2         # 每个候选速率重复确认，降低短时噪声误判
PORT_BASE = 6666
HOST      = "127.0.0.1"   # 目标服务器地址（内网双机时设为服务端内网 IP）

# ── 峰值并发扫描（方案 Phase J：测服务器能同时稳定管多少连接）─────
CONN_RATE  = 10000      # 固定目标 QPS
CONN_LIST  = [1000, 5000, 10000, 20000, 50000]   # 逐档连接数
CONN_DUR   = 30         # 每档时长
CONN_LOAD  = "11B"      # 用最小回包（避免带宽干扰，聚焦连接管理）

# ── perf 事件 ──────────────────────────────────────────────────────
PERF_CORE_EVENTS = ("instructions,cycles,"
                    "cache-misses,cache-references,"
                    "branch-misses,branch-instructions")

OUTPUT_DIR = BENCHMARK_DIR / "data" / "local" / datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_LOG = None


# ═══════════════════════════════════════════════════════════════════

def log(msg=""):
    print(msg, flush=True)
    if RUN_LOG is not None:
        RUN_LOG.write(f"{msg}\n")
        RUN_LOG.flush()


def cores_str(cores):
    return ",".join(str(c) for c in cores)


def scenario_info(name):
    for s in SCENARIOS:
        if s[0] == name:
            return s
    raise KeyError(name)


def alloc_cores(sub, worker):
    """服务端核 = main + sub（P 核）+ worker（E 核，异步）；客户端核 = 剩余全部。"""
    server = P_CORES[:1 + sub]
    if worker:
        server += E_CORES[:worker]
    used = set(server)
    wrk = [c for c in P_CORES + E_CORES if c not in used]
    return server, wrk


def alloc_comparison_cores(sub, worker):
    """为 sync/async 对照使用相同的服务端与客户端 CPU 集合。"""
    server_count = 1 + sub + (worker or 0)
    server = (P_CORES + E_CORES)[:server_count]
    used = set(server)
    wrk = [cpu for cpu in P_CORES + E_CORES if cpu not in used]
    return server, wrk


# ── FD 上限 ────────────────────────────────────────────────────────
def raise_nofile():
    """把 FD 上限拉到尽量高：root 下直接设 100 万（soft+hard 都提），
    非 root 降级到 hard 上限（至少 soft=hard）。峰值并发 50000 连接才稳。"""
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = max(hard, 1_000_000)
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, target))
        return soft, target
    except Exception:
        try:
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
            return soft, hard
        except Exception:
            return None, None


def server_preexec():
    raise_nofile()
    os.setsid()


# ── governor / NMI watchdog（压测环境，本机一般无需，但保留）──────
_OLD_GOVERNOR = None


def set_governor(g):
    gov_files = [str(p) for p in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_governor")]
    if not gov_files:
        return False
    try:
        if os.geteuid() == 0:
            for f in gov_files:
                with open(f, "w") as fh:
                    fh.write(g)
        else:
            subprocess.run(["sudo", "-n", "sh", "-c", f"echo {g} | tee " + " ".join(gov_files) + " > /dev/null"],
                           capture_output=True, timeout=10, check=True)
        return True
    except Exception:
        return False


def restore_governor():
    global _OLD_GOVERNOR
    if _OLD_GOVERNOR and set_governor(_OLD_GOVERNOR):
        print(f"已恢复 governor -> {_OLD_GOVERNOR}")
        _OLD_GOVERNOR = None


# ── wrk2 结果解析 ─────────────────────────────────────────────────
def parse_us(s):
    m = re.match(r"([\d.]+)\s*(us|ms|s)", s)
    if not m:
        return None
    v, u = float(m.group(1)), m.group(2)
    return v * (1 if u == "us" else 1000 if u == "ms" else 1000000)


def parse_wrk2(text):
    r = {"qps": 0.0, "err_connect": 0, "err_read": 0, "err_write": 0,
         "err_timeout": 0, "err_status": 0, "lat": {}, "latency_samples": None}
    m = re.search(r"Requests/sec:\s+([\d.]+)", text)
    if m:
        r["qps"] = float(m.group(1))
    m = re.search(r"Latency Distribution.*?\n(.*?)(?:\n\n|\Z)", text, re.DOTALL)
    if m:
        for line in m.group(1).splitlines():
            pm = re.match(r"\s*([\d.]+)%\s+([\d.]+\s*\w+)", line)
            if pm:
                us = parse_us(pm.group(2))
                if us is not None:
                    r["lat"][float(pm.group(1))] = us
    # 兜底：从 Lua done() 的 "Latency percentiles (us):" 输出解析
    if not r["lat"] or len(r["lat"]) < 3:
        m2 = re.search(r"Latency percentiles[^\n]*\n(.*?)(?:\n\n|\Z)", text, re.DOTALL)
        if m2:
            for line in m2.group(1).splitlines():
                pm = re.match(r"\s*p(\d+\.?\d*):\s+([\d.]+)", line)
                if pm:
                    try:
                        r["lat"][float(pm.group(1))] = float(pm.group(2))
                    except ValueError:
                        pass
    count_match = re.search(r"Total count\s*=\s*(\d+)", text)
    if count_match:
        r["latency_samples"] = int(count_match.group(1))
        if r["latency_samples"] == 0:
            r["lat"] = {}
    m = re.search(r"Socket errors:\s*connect\s+(\d+),\s*read\s+(\d+),\s*write\s+(\d+),\s*timeout\s+(\d+)", text)
    if m:
        r["err_connect"], r["err_read"], r["err_write"], r["err_timeout"] = \
            int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    # Lua done() 中可能输出 status 错误，标准 wrk2 不包含此字段
    m = re.search(r"status=(\d+)", text)
    if m:
        r["err_status"] = int(m.group(1))
    return r


def validate_wrk2():
    try:
        result = subprocess.run([WRK_BIN, "--help"], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(f"无法检查 wrk2 可执行文件 {WRK_BIN}: {exc}")
    help_text = result.stdout + result.stderr
    if "-R" not in help_text:
        raise SystemExit(
            f"{WRK_BIN} 不支持 wrk2 的 -R 定速参数；请先编译/安装 wrk2，禁止用原版 wrk 采集正式数据"
        )


def validate_port_available():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind((HOST, PORT_BASE))
        except OSError as exc:
            raise SystemExit(f"测试端口 {HOST}:{PORT_BASE} 不可用；请先确认没有残留服务占用: {exc}")


def parse_perf(path):
    try:
        text = Path(path).read_text()
    except Exception:
        return {}
    vals = {}
    for line in text.splitlines():
        m = re.match(r"\s*([\d.,]+|<not[^>]*>)\s+(\S+)", line)
        if m:
            name = m.group(2)
            if "/" in name:
                name = name.split("/")[1]
            try:
                vals[name] = vals.get(name, 0.0) + float(m.group(1).replace(",", ""))
            except ValueError:
                continue

    def ratio(a, b):
        return round(vals[a] / vals[b], 4) if vals.get(a) and vals.get(b) else None

    return {
        "ipc": ratio("instructions", "cycles"),
        "cache_miss_rate": ratio("cache-misses", "cache-references"),
        "branch_miss_rate": ratio("branch-misses", "branch-instructions"),
    }


# ── lua 脚本（short / pipeline）───────────────────────────────────
def make_lua(mode, url_path):
    if mode == "keepalive":
        return None
    p = OUTPUT_DIR / f"{mode}.lua"
    if mode == "short":
        p.write_text('request = function()\n'
                     '    return wrk.format("GET", nil, {["Connection"] = "close"})\n'
                     'end\n')
    elif mode == "pipeline":
        lines = ['init = function(args)', '   local r = {}']
        for i in range(PIPELINE_DEPTH):
            lines.append(f'   r[{i+1}] = wrk.format("GET", "{url_path}")')
        lines += ['   req = table.concat(r)', 'end',
                  'request = function()', '   return req', 'end']
        p.write_text("\n".join(lines) + "\n")
    return p


def verify_pipeline(server_port, url_path, expected_body_size, request_count=PIPELINE_DEPTH):
    request = (f"GET {url_path} HTTP/1.1\r\n"
               "Host: localhost\r\n"
               "Connection: keep-alive\r\n\r\n").encode()
    received = bytearray()

    with socket.create_connection(("127.0.0.1", server_port), timeout=5) as conn:
        conn.settimeout(5)
        conn.sendall(request * request_count)

        for _ in range(request_count):
            while b"\r\n\r\n" not in received:
                chunk = conn.recv(65536)
                if not chunk:
                    raise RuntimeError("pipeline connection closed before response headers")
                received.extend(chunk)

            header_end = received.index(b"\r\n\r\n") + 4
            headers = bytes(received[:header_end]).decode("latin1")
            if not headers.startswith("HTTP/1.1 200 "):
                raise RuntimeError(f"unexpected pipeline response: {headers.splitlines()[0]}")
            content_length = next(
                (int(line.split(":", 1)[1].strip()) for line in headers.splitlines()
                 if line.lower().startswith("content-length:")),
                None,
            )
            if content_length != expected_body_size:
                raise RuntimeError(
                    f"pipeline body length {content_length}, expected {expected_body_size}"
                )

            while len(received) - header_end < content_length:
                chunk = conn.recv(65536)
                if not chunk:
                    raise RuntimeError("pipeline connection closed before response body")
                received.extend(chunk)
            del received[:header_end + content_length]

    log(f"  pipeline 正确性通过: {request_count} 个响应，body={expected_body_size}B")


# ── 服务器启动（bind failed 重试）────────────────────────────────
def build_server_cmd(name, port, server_cores):
    _, _, sub, worker = scenario_info(name)
    cmd = ["taskset", "-c", cores_str(server_cores), SERVER_BIN, str(port), str(sub)]
    if worker is not None:
        cmd.append(str(worker))
    return cmd


def start_server_retry(name, port, server_cores, max_retries=3, retry_sleep=20, log_name=None):
    log_base = log_name or name
    for attempt in range(max_retries):
        cmd = build_server_cmd(name, port, server_cores)
        if attempt == 0:
            log(f"  启动: {' '.join(cmd)}")
        else:
            log(f"  重试（第 {attempt+1}/{max_retries} 次）: {' '.join(cmd)}")
        srv_log = open(OUTPUT_DIR / f"server_{log_base}.log", "a" if attempt > 0 else "w")
        srv = subprocess.Popen(cmd, stdout=srv_log, stderr=subprocess.STDOUT, preexec_fn=server_preexec)
        time.sleep(2)
        if srv.poll() is None:
            return srv, srv_log
        log(f"  ⚠️ 服务器启动后立即退出，退出码 {srv.poll()}，见 server_{name}.log")
        srv_log.close()
        if attempt < max_retries - 1:
            log(f"  等待 {retry_sleep}s 后重试（端口可能未释放）...")
            time.sleep(retry_sleep)
    return None, None


# ── wrk 执行 + 客户端 CPU 监控 + 可选 perf ────────────────────────
def run_wrk(url, c, lua_path, wrk_cores, dur, server_pid, rate, perf_events=None, timeout="5s"):
    import psutil
    n_cores = len(wrk_cores)
    threads = WRK_THREADS or n_cores
    cmd = ["taskset", "-c", cores_str(wrk_cores),
           WRK_BIN, f"-t{threads}", f"-c{c}", f"-d{dur}s"]
    if rate is not None:
        cmd += [f"-R{rate}"]
    cmd += ["--latency", "--timeout", timeout]
    if lua_path:
        cmd += ["-s", str(lua_path)]
    cmd.append(url)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            preexec_fn=server_preexec)

    perf_proc = None
    perf_out = None
    if perf_events and server_pid:
        perf_out = OUTPUT_DIR / f"perf_tmp_{int(time.time()*1000)}.txt"
        perf_cmd = ["perf", "stat", "-p", str(server_pid), "-e", perf_events,
                    "-o", str(perf_out), "--", "sleep", str(dur)]
        perf_proc = subprocess.Popen(perf_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    wrk_cpu = 0.0
    try:
        wp = psutil.Process(proc.pid)
        wp.cpu_percent(interval=None)
        samples = []
        while proc.poll() is None:
            try:
                samples.append(wp.cpu_percent(interval=None))
            except Exception:
                pass
            time.sleep(1)
        wrk_cpu = sum(samples) / len(samples) if samples else 0.0
    except Exception:
        wrk_cpu = 0.0

    out = proc.stdout.read() if proc.stdout else ""

    perf = {}
    if perf_proc:
        try:
            perf_proc.wait(timeout=dur + 10)
            perf = parse_perf(perf_out)
            if perf_out:
                perf_out.unlink(missing_ok=True)
        except Exception:
            pass

    r = parse_wrk2(out)
    # wrk2 increments its request counter for each completed HTTP response,
    # including responses received from a pipelined write batch.
    return r, wrk_cpu, out, perf


# ── 峰值扫描（按 P99 硬约束搜索本次测量的最高稳定速率）────────────
def scan_knee(name, sub, port, server_cores, wrk_cores, server_pid, url_path, est_per_core, worker=None,
              latency_limit_us=None):
    """找到本次实测 P99 不超过延迟上限时的最高稳定速率。
    est 仅作为首个探测点；稳定判断完全来自本次 wrk2 测量，不使用历史数值作证据。
    """
    if latency_limit_us is None:
        latency_limit_us = MAX_ACCEPTABLE_P99_US
    url = f"http://{HOST}:{port}{url_path}"
    est = max(1000, (worker or sub) * est_per_core)
    log(f"  [{name}] {url_path} 搜索起点={est}，P99 上限={latency_limit_us / 1000:.1f}ms")

    def is_stable(rate):
        samples = []
        for attempt in range(SCAN_CONFIRMATIONS):
            r, wrk_cpu, _, _ = run_wrk(url, WRK_CONC, None, wrk_cores, SCAN_DUR, server_pid, rate)
            p99 = r["lat"].get(99.0)
            errors = r["err_connect"] + r["err_read"] + r["err_write"] + r["err_timeout"]
            stable = (r["qps"] > 0 and p99 is not None and p99 <= latency_limit_us
                      and errors == 0 and wrk_cpu <= len(wrk_cores) * 90.0)
            samples.append(stable)
            log(f"  [{name}] 扫 -R{rate} #{attempt + 1}: QPS={r['qps']:.0f}, P99={p99}, "
                f"err={errors}, wrk_cpu={wrk_cpu:.0f}%, stable={stable}")
        return all(samples)

    # 先从估计速率向外扩展，分别覆盖估算偏高和偏低的情况。
    if is_stable(est):
        low = est
        high = None
        rate = est
        while rate < MAX_SCAN_RATE:
            rate = min(MAX_SCAN_RATE, rate * 2)
            if not is_stable(rate):
                high = rate
                break
            low = rate
        if high is None:
            log(f"  [{name}] 到达扫描上限 {MAX_SCAN_RATE} 仍满足约束，未找到性能上界；跳过该负载")
            return None
    else:
        high = est
        low = None
        rate = est
        while rate > 1000:
            rate = max(1000, rate // 2)
            if is_stable(rate):
                low = rate
                break
            high = rate
        if low is None:
            log(f"  [{name}] 最低探测速率 1000 仍不满足约束；没有有效吞吐上界")
            return None

    # 二分直到区间不超过 3%，减少短探测次数；最终候选仍由正式重复确认。
    while high - low > max(500, int(low * 0.03)):
        mid = (low + high) // 2
        if is_stable(mid):
            low = mid
        else:
            high = mid
    log(f"  [{name}] 有效上界候选={low} QPS，满足 P99 <= {latency_limit_us / 1000:.1f}ms")
    return low


# ── 单场景测试（精简：每个响应大小扫拐点 → 拐点处 30s + perf）─────
def run_scenario(name, load_names, modes, plan=None):
    _, model, sub, worker = scenario_info(name)
    port = PORT_BASE
    server_cores, wrk_cores = alloc_cores(sub, worker)
    log(f"\n{'='*70}\n  场景 {name}  port={port}  服务端核={len(server_cores)}  wrk核={len(wrk_cores)}\n{'='*70}")

    log_name = f"{plan}_{name}" if plan else name
    srv, srv_log = start_server_retry(name, port, server_cores, log_name=log_name)
    if srv is None:
        log(f"  ❌ {name} 服务器多次启动失败，跳过")
        return []

    rows = []
    try:
        # 预热
        log(f"  [{name}] 预热 8s（丢弃）...")
        run_wrk(f"http://{HOST}:{port}/data/16k", WRK_CONC, None, wrk_cores, 8, srv.pid, 50000)
        time.sleep(2)

        # 每个负载：先扫 keepalive 拐点，再对每个连接模式用同拐点测（对比公平）
        for lname in load_names:
            url_path, est_per_core = LOADS[lname]
            if "pipeline" in modes:
                expected_body_size = {"/data/16k": 16384, "/data/64k": 65536}.get(url_path)
                if expected_body_size is None:
                    raise RuntimeError(f"pipeline smoke test has no expected body size for {url_path}")
                verify_pipeline(port, url_path, expected_body_size)
            knee = scan_knee(name, sub, port, server_cores, wrk_cores, srv.pid, url_path, est_per_core, worker)
            if knee is None:
                log(f"  [{name}] {lname} 未找到有效上界，跳过正式测试")
                continue
            url = f"http://{HOST}:{port}{url_path}"
            def measure_repetitions(mode, rate, duration, repetitions, collect_perf):
                lua_path = make_lua(mode, url_path)
                reps = []
                for rep in range(repetitions):
                    perf_events = PERF_CORE_EVENTS if collect_perf else None
                    r, wrk_cpu, out, perf = run_wrk(url, WRK_CONC, lua_path, wrk_cores, duration,
                                                    srv.pid, rate, perf_events)
                    (OUTPUT_DIR / f"wrk_{log_name}_{lname}_{mode}_rep{rep + 1}.log").write_text(out)
                    if r["qps"] <= 0:
                        log(f"  [{name}] {lname}/{mode} -R{rate} rep{rep+1}: wrk 失败")
                        continue
                    reps.append((r, wrk_cpu, out, perf))
                    log(f"  [{name}] {lname}/{mode} -R{rate} rep{rep+1}: QPS={r['qps']:,.1f}  "
                        f"P99={r['lat'].get(99.0)}us  wrk_cpu={wrk_cpu:.0f}%  dur={duration}s")
                    time.sleep(1)

                def valid(sample):
                    sample_result, sample_wrk_cpu = sample[0], sample[1]
                    sample_errors = (sample_result["err_connect"] + sample_result["err_read"]
                                     + sample_result["err_write"] + sample_result["err_timeout"])
                    sample_p99 = sample_result["lat"].get(99.0)
                    return (sample_p99 is not None and sample_p99 <= MAX_ACCEPTABLE_P99_US
                            and sample_errors == 0 and sample_wrk_cpu <= len(wrk_cores) * 95.0)

                return reps, len(reps) == repetitions and all(valid(sample) for sample in reps)

            def formal_repetitions(mode, rate):
                return measure_repetitions(mode, rate, DURATION, REPEAT, True)

            def search_is_stable(mode, rate):
                lua_path = make_lua(mode, url_path)
                result, wrk_cpu, _, _ = run_wrk(url, WRK_CONC, lua_path, wrk_cores, SCAN_DUR,
                                                srv.pid, rate)
                errors = (result["err_connect"] + result["err_read"]
                          + result["err_write"] + result["err_timeout"])
                p99 = result["lat"].get(99.0)
                stable = (result["qps"] > 0 and p99 is not None
                          and p99 <= MAX_ACCEPTABLE_P99_US and errors == 0
                          and wrk_cpu <= len(wrk_cores) * 90.0)
                log(f"  [{name}] 回退搜索 -R{rate}: QPS={result['qps']:.0f}, P99={p99}, stable={stable}")
                return stable

            # scan_knee 已用短探测完成边界搜索；正式时长×重复数只验证最终候选。
            primary_mode = modes[0]
            candidate_rate = knee
            primary_reps, primary_valid = formal_repetitions(primary_mode, candidate_rate)
            if not primary_valid:
                high = candidate_rate
                low = None
                while candidate_rate > 1000:
                    candidate_rate = max(1000, int(candidate_rate * 0.8))
                    candidate_valid = search_is_stable(primary_mode, candidate_rate)
                    if candidate_valid:
                        low = candidate_rate
                        break
                    high = candidate_rate
                if low is None:
                    log(f"  [{name}] {lname} 在 -R1000 下仍未通过搜索约束，跳过")
                    continue

                while high - low > max(500, int(low * 0.03)):
                    mid = (low + high) // 2
                    mid_valid = search_is_stable(primary_mode, mid)
                    if mid_valid:
                        low = mid
                    else:
                        high = mid
                candidate_rate = low
                primary_reps, primary_valid = formal_repetitions(primary_mode, candidate_rate)
                log(f"  [{name}] {lname} 短探测回退至 {candidate_rate} QPS，正式确认状态={primary_valid}")
            while not primary_valid and candidate_rate > 1000:
                next_rate = max(1000, int(candidate_rate * 0.8))
                if next_rate == candidate_rate:
                    break
                candidate_rate = next_rate
                if candidate_rate > 1000 and not search_is_stable(primary_mode, candidate_rate):
                    continue
                primary_reps, primary_valid = formal_repetitions(primary_mode, candidate_rate)
                log(f"  [{name}] {lname} 继续回退至 {candidate_rate} QPS，正式确认状态={primary_valid}")
            upper_bound_found = True
            if not primary_valid:
                log(f"  [{name}] {lname} 最低速率仍未通过正式约束，跳过")
                continue

            for mode in modes:
                if mode == primary_mode:
                    reps = primary_reps
                else:
                    reps, _ = formal_repetitions(mode, candidate_rate)
                if not reps:
                    log(f"  [{name}] {lname}/{mode} 正式测试全部失败")
                    continue
                reps.sort(key=lambda x: x[0]["qps"])
                r, wrk_cpu, out, perf = reps[len(reps) // 2]   # 取中位样本
                lat = r["lat"]
                all_reps_within_latency_limit = all(
                    sample[0]["lat"].get(99.0) is not None
                    and sample[0]["lat"][99.0] <= MAX_ACCEPTABLE_P99_US
                    for sample in reps
                )
                all_reps_zero_errors = all(
                    sample[0]["err_connect"] + sample[0]["err_read"]
                    + sample[0]["err_write"] + sample[0]["err_timeout"] == 0
                    for sample in reps
                )
                row = {
                    "plan": plan, "scenario": name, "model": model, "sub": sub,
                    "server_cores": len(server_cores), "wrk_cores": len(wrk_cores),
                    "load": lname, "path": url_path, "mode": mode, "knee_rate": candidate_rate,
                    "p99_limit_us": MAX_ACCEPTABLE_P99_US,
                    "upper_bound_found": upper_bound_found,
                    "within_latency_limit": all_reps_within_latency_limit,
                    "zero_errors": all_reps_zero_errors,
                    "qps": r["qps"],
                    "qps_per_core": round(r["qps"] / len(server_cores), 1),
                    "lat_p50": lat.get(50.0), "lat_p75": lat.get(75.0),
                    "lat_p90": lat.get(90.0), "lat_p99": lat.get(99.0),
                    "lat_p999": lat.get(99.9), "lat_p9999": lat.get(99.99),
                    "wrk_cpu_avg": wrk_cpu,
                    "wrk_saturated": wrk_cpu > len(wrk_cores) * 95.0,
                    "err_connect": r["err_connect"], "err_read": r["err_read"],
                    "err_write": r["err_write"], "err_timeout": r["err_timeout"],
                    "ipc": perf.get("ipc"),
                    "cache_miss_rate": perf.get("cache_miss_rate"),
                    "branch_miss_rate": perf.get("branch_miss_rate"),
                }
                rows.append(row)
                with open(OUTPUT_DIR / "results.csv", "a", newline="") as csv_file:
                    csv.DictWriter(csv_file, fieldnames=CSV_FIELDS, extrasaction="ignore").writerow(row)
                (OUTPUT_DIR / f"wrk_{log_name}_{lname}_{mode}.log").write_text(out)
                log(f"  [{name}] {lname}/{mode} 有效速率={candidate_rate}  QPS={r['qps']:,.1f}  "
                    f"每核={row['qps_per_core']}  P99={lat.get(99.0)}  "
                    f"全部样本达标={row['within_latency_limit'] and row['zero_errors']}  wrk_cpu={wrk_cpu:.0f}%")
    finally:
        srv.send_signal(signal.SIGTERM)
        try:
            srv.wait(timeout=5)
        except subprocess.TimeoutExpired:
            srv.kill()
        srv_log.close()
        time.sleep(1)
    return rows


def run_async_comparison(name, load_names, plan="async"):
    """按固定 offered-rate 网格记录 sync/async 对照，包括超出 SLO 的样本。"""
    _, model, sub, worker = scenario_info(name)
    core_allocator = globals().get("alloc_comparison_cores", alloc_cores)
    server_cores, wrk_cores = core_allocator(sub, worker)
    port = PORT_BASE
    log_name = f"{plan}_{name}"
    log(f"\n{'='*70}\n  固定速率对照 {name}  服务端核={len(server_cores)}  wrk核={len(wrk_cores)}\n{'='*70}")

    srv, srv_log = start_server_retry(name, port, server_cores, log_name=log_name)
    if srv is None:
        log(f"  [对照] {name} 服务器启动失败，跳过")
        return []

    rows = []
    try:
        run_wrk(f"http://{HOST}:{port}/data/16k", WRK_CONC, None, wrk_cores, 5, srv.pid, 50000)
        time.sleep(1)
        for load_name in load_names:
            # sync 场景只跑 delay + 11B，跳过 compute（sync compute 已在前一轮跑完）
            if name.startswith("sync") and load_name.startswith("compute-"):
                continue
            url_path, _ = LOADS[load_name]
            url = f"http://{HOST}:{port}{url_path}"
            concurrency = ASYNC_COMPARISON_CONCURRENCY[load_name]
            duration = ASYNC_LONG_LOAD_DURATION if load_name.startswith(("compute-", "delay-")) else DURATION
            wrk_timeout = "60s" if load_name.startswith(("compute-", "delay-")) else "5s"
            reps = 2 if load_name.startswith("delay-") else REPEAT
            for rate in ASYNC_COMPARISON_RATES[load_name]:
                for repetition in range(1, reps + 1):
                    result, wrk_cpu, output, _ = run_wrk(
                        url, concurrency, None, wrk_cores, duration, srv.pid, rate,
                        timeout=wrk_timeout
                    )
                    (OUTPUT_DIR / f"wrk_{log_name}_{load_name}_R{rate}_rep{repetition}.log").write_text(output)
                    latency = result["lat"]
                    errors = (result["err_connect"] + result["err_read"]
                              + result["err_write"] + result["err_timeout"])
                    p99 = latency.get(99.0)
                    row = {
                        "plan": plan, "scenario": name, "model": model, "sub": sub,
                        "server_cores": len(server_cores), "wrk_cores": len(wrk_cores),
                        "load": load_name, "path": url_path, "mode": "keepalive",
                        "offered_rate": rate, "qps_ratio": round(result["qps"] / rate, 4),
                        "concurrency": concurrency, "repetition": repetition,
                        "latency_samples": result["latency_samples"],
                        "p99_limit_us": MAX_ACCEPTABLE_P99_US,
                        "within_latency_limit": p99 is not None and p99 <= MAX_ACCEPTABLE_P99_US,
                        "zero_errors": errors == 0,
                        "qps": result["qps"],
                        "qps_per_core": round(result["qps"] / len(server_cores), 1),
                        "lat_p50": latency.get(50.0), "lat_p75": latency.get(75.0),
                        "lat_p90": latency.get(90.0), "lat_p99": p99,
                        "lat_p999": latency.get(99.9), "lat_p9999": latency.get(99.99),
                        "wrk_cpu_avg": wrk_cpu,
                        "wrk_saturated": wrk_cpu > len(wrk_cores) * 95.0,
                        "err_connect": result["err_connect"], "err_read": result["err_read"],
                        "err_write": result["err_write"], "err_timeout": result["err_timeout"],
                    }
                    rows.append(row)
                    with open(OUTPUT_DIR / "results.csv", "a", newline="") as csv_file:
                        csv.DictWriter(csv_file, fieldnames=CSV_FIELDS, extrasaction="ignore").writerow(row)
                    p99_text = f"{p99 / 1000:.2f}ms" if p99 is not None else "-"
                    log(f"  [{name}] {load_name} -R{rate} c={concurrency} rep{repetition}: "
                        f"QPS={result['qps']:,.1f} P99={p99_text} errors={errors} "
                        f"SLO={'pass' if row['within_latency_limit'] and row['zero_errors'] else 'fail'}")
    finally:
        srv.send_signal(signal.SIGTERM)
        try:
            srv.wait(timeout=5)
        except subprocess.TimeoutExpired:
            srv.kill()
        srv_log.close()
        time.sleep(1)
    return rows


# ── 峰值并发扫描（固定 QPS，逐档加大连接数，测连接管理上限）──────
def run_conn_scan(name, load_name, rate, conns, dur):
    """固定 -R rate，逐档加大连接数 c，测服务器能同时稳定管理多少连接。
    每档记录：QPS 达成率、P99、错误数、服务器 RSS（内存）、FD 数。
    拐点：QPS 掉 / P99 暴涨 / 连接错误出现。"""
    _, model, sub, worker = scenario_info(name)
    port = PORT_BASE
    server_cores, wrk_cores = alloc_cores(sub, worker)
    url_path, _ = LOADS[load_name]
    url = f"http://{HOST}:{port}{url_path}"
    log(f"\n{'='*70}\n  峰值并发 {name}  port={port}  固定-R{rate}  连接数 {conns}\n{'='*70}")

    srv, srv_log = start_server_retry(name, port, server_cores)
    if srv is None:
        log(f"  ❌ {name} 服务器多次启动失败，跳过")
        return []

    rows = []
    try:
        import psutil
        sproc = psutil.Process(srv.pid)
        # 预热
        run_wrk(url, min(conns[0], 1000), None, wrk_cores, 5, srv.pid, rate)
        time.sleep(2)

        import threading
        for c in conns:
            rss_before = sproc.memory_info().rss
            fd_peak = [0]
            done = [False]
            def sample_fd():
                while not done[0]:
                    try:
                        fd_peak[0] = max(fd_peak[0], sproc.num_fds())
                    except Exception:
                        pass
                    time.sleep(0.5)
            t = threading.Thread(target=sample_fd, daemon=True)
            t.start()
            r, wrk_cpu, out, _ = run_wrk(url, c, None, wrk_cores, dur, srv.pid, rate)
            done[0] = True
            t.join()
            rss_after = sproc.memory_info().rss
            fd_after = fd_peak[0]
            lat = r["lat"]
            errs = r["err_connect"] + r["err_read"] + r["err_write"] + r["err_timeout"]
            row = {
                "scenario": name, "model": model, "conns": c, "rate": rate,
                "qps": r["qps"],
                "qps_ratio": round(r["qps"] / rate, 4) if rate else None,
                "lat_p50": lat.get(50.0), "lat_p99": lat.get(99.0),
                "err_connect": r["err_connect"], "err_read": r["err_read"],
                "err_write": r["err_write"], "err_timeout": r["err_timeout"],
                "rss_mb": round(rss_after / 1048576, 1),
                "rss_delta_mb": round((rss_after - rss_before) / 1048576, 1),
                "fd": fd_after,
                "wrk_cpu": wrk_cpu,
            }
            rows.append(row)
            (OUTPUT_DIR / f"conn_{name}_{c}.log").write_text(out)
            log(f"  [{name}] conn={c:>6}: QPS={r['qps']:>10,.1f} ({row['qps_ratio']*100:5.1f}%)  "
                f"P99={lat.get(99.0)}  err={errs}  RSS={row['rss_mb']}MB(+{row['rss_delta_mb']})  FD={fd_after}")
            time.sleep(2)
    finally:
        srv.send_signal(signal.SIGTERM)
        try:
            srv.wait(timeout=5)
        except subprocess.TimeoutExpired:
            srv.kill()
        srv_log.close()
        time.sleep(1)
    return rows


# ── 火焰图采集（基准测试后单独采，全景瓶颈刻画）──────────────────
def find_flamegraph_tools():
    """查找 FlameGraph 工具（stackcollapse-perf.pl / flamegraph.pl），找不到返回 (None, None)。
    兼容 sudo 运行：sudo 下 Path.home() 是 /root，需额外查真实用户（SUDO_USER）的 HOME。"""
    bases = [Path("/opt/FlameGraph"), Path("/usr/local/FlameGraph"),
             Path.home() / "FlameGraph"]
    sudo_user = os.environ.get("SUDO_USER")
    if sudo_user:
        try:
            import pwd
            bases.append(Path(pwd.getpwnam(sudo_user).pw_dir) / "FlameGraph")
        except (ImportError, KeyError):
            pass
    for base in bases:
        stack, flame = base / "stackcollapse-perf.pl", base / "flamegraph.pl"
        if stack.exists() and flame.exists():
            return stack, flame
    stack, flame = shutil.which("stackcollapse-perf.pl"), shutil.which("flamegraph.pl")
    if stack and flame:
        return Path(stack), Path(flame)
    return None, None


def collect_flamegraph(name="sync-4", mode="keepalive", path="/data/64k"):
    """单独起服务器，perf record 采样峰值档，生成火焰图（全景瓶颈刻画）。
    只在基准测试后调用，避免干扰基准数据。"""
    _, _, sub, worker = scenario_info(name)
    port = PORT_BASE
    server_cores, wrk_cores = alloc_cores(sub, worker)
    log(f"\n{'='*70}\n  火焰图采集: {name} {mode} {path} 峰值档\n{'='*70}")

    srv, srv_log = start_server_retry(name, port, server_cores)
    if srv is None:
        log("  服务器启动失败，跳过火焰图采集")
        return

    try:
        # 预热 + 扫峰值
        run_wrk(f"http://{HOST}:{port}{path}", WRK_CONC, None, wrk_cores, 5, srv.pid, 50000)
        time.sleep(1)
        est_per_core = 100000
        for u, e in LOADS.values():
            if u == path:
                est_per_core = e
                break
        peak = scan_knee(name, sub, port, server_cores, wrk_cores, srv.pid, path, est_per_core, worker)

        # perf record 并行采样
        perf_data = OUTPUT_DIR / f"flame_{name}_{mode}.data"
        perf_cmd = ["perf", "record", "-g", "-p", str(srv.pid), "-o", str(perf_data),
                    "--", "sleep", str(DURATION)]
        log(f"  perf record 采样 {DURATION}s ...")
        perf_proc = subprocess.Popen(perf_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # wrk 打峰值（与 perf record 并行）
        lua = make_lua(mode, path)
        run_wrk(f"http://{HOST}:{port}{path}", WRK_CONC, lua, wrk_cores, DURATION, srv.pid, peak)

        perf_proc.wait(timeout=DURATION + 10)

        # perf script 转文本（火焰图数据源）
        script_out = subprocess.run(["perf", "script", "-i", str(perf_data)],
                                    capture_output=True, text=True)
        perf_script = OUTPUT_DIR / f"flame_{name}_{mode}.script"
        perf_script.write_text(script_out.stdout)
        log(f"  火焰图数据: {perf_script}")

        # 若有 FlameGraph 工具则生成 SVG，否则保留 script 供手动生成
        stack, flame = find_flamegraph_tools()
        if stack and flame:
            svg = OUTPUT_DIR / f"flame_{name}_{mode}.svg"
            p1 = subprocess.Popen([str(stack), str(perf_script)], stdout=subprocess.PIPE)
            with open(svg, "w") as fh:
                p2 = subprocess.Popen([str(flame)], stdin=p1.stdout, stdout=fh)
                p1.stdout.close()
                p2.wait()
            p1.wait()
            log(f"  火焰图 SVG: {svg}")
        else:
            log(f"  未找到 FlameGraph 工具，保留 {perf_script}；"
                f"手动生成: stackcollapse-perf.pl {perf_script.name} | flamegraph.pl > flame.svg")
    finally:
        srv.send_signal(signal.SIGTERM)
        try:
            srv.wait(timeout=5)
        except subprocess.TimeoutExpired:
            srv.kill()
        srv_log.close()
        time.sleep(1)


# ── 汇总 ──────────────────────────────────────────────────────────
CSV_FIELDS = [
    "plan", "scenario", "model", "sub", "server_cores", "wrk_cores",
    "load", "path", "mode", "knee_rate", "offered_rate", "concurrency", "repetition",
    "qps_ratio", "latency_samples", "p99_limit_us", "upper_bound_found",
    "within_latency_limit", "zero_errors",
    "qps", "qps_per_core",
    "lat_p50", "lat_p75", "lat_p90", "lat_p99", "lat_p999", "lat_p9999",
    "wrk_cpu_avg", "wrk_saturated",
    "err_connect", "err_read", "err_write", "err_timeout",
    "ipc", "cache_miss_rate", "branch_miss_rate",
]

CONN_CSV_FIELDS = [
    "scenario", "model", "conns", "rate",
    "qps", "qps_ratio",
    "lat_p50", "lat_p99",
    "err_connect", "err_read", "err_write", "err_timeout",
    "rss_mb", "rss_delta_mb", "fd", "wrk_cpu",
]


def main():
    global REPEAT, DURATION, ASYNC_LONG_LOAD_DURATION, SCAN_DUR, SCAN_CONFIRMATIONS
    global MAX_ACCEPTABLE_P99_US, _OLD_GOVERNOR, RUN_LOG
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=str, default="baseline", choices=list(PLANS.keys()),
                        help="测试计划：" + " / ".join(PLANS.keys()))
    parser.add_argument("--scene", type=str, default=None, help="只跑某场景（调试，覆盖 plan）")
    parser.add_argument("--repeat", type=int, default=REPEAT, help=f"拐点处重复次数（取中位数），默认 {REPEAT}")
    parser.add_argument("--duration", type=int, default=DURATION, help=f"正式测试时长（秒），默认 {DURATION}")
    parser.add_argument("--long-load-duration", type=int, default=ASYNC_LONG_LOAD_DURATION,
                        help=f"async plan 中 compute/delay 每个样本时长（秒），默认 {ASYNC_LONG_LOAD_DURATION}")
    parser.add_argument("--scan-duration", type=int, default=SCAN_DUR, help=f"每个拐点扫描档时长（秒），默认 {SCAN_DUR}")
    parser.add_argument("--scan-confirmations", type=int, default=SCAN_CONFIRMATIONS,
                        help=f"每个搜索速率短探测次数，默认 {SCAN_CONFIRMATIONS}；最终仍按 --repeat 正式确认")
    parser.add_argument("--latency-limit-ms", type=float, default=MAX_ACCEPTABLE_P99_US / 1000,
                        help="有效吞吐的 P99 延迟上限（毫秒），默认 10")
    parser.add_argument("--flame", action="store_true", help="基准测试后单独采集火焰图（sync-4 64KB 峰值档）")
    parser.add_argument("--flame-scene", type=str, default="sync-4", help="火焰图采样场景，默认 sync-4")
    parser.add_argument("--conn", type=str, default=None, help="峰值并发扫描：场景名（如 sync-16），固定 -R 扫连接数")
    parser.add_argument("--all", action="store_true", help="依次跑稳定计划（baseline+pipeline+scalability），结果汇总到同一 csv")
    args = parser.parse_args()

    REPEAT = args.repeat
    if (args.duration <= 0 or args.long_load_duration <= 0 or args.scan_duration <= 0
            or args.scan_confirmations <= 0 or args.repeat <= 0):
        parser.error("时长、重复次数和扫描确认次数必须为正数")
    if args.latency_limit_ms <= 0:
        parser.error("--latency-limit-ms 必须为正数")
    if args.flame_scene not in {scene[0] for scene in SCENARIOS}:
        parser.error(f"未知火焰图场景: {args.flame_scene}")
    DURATION = args.duration
    ASYNC_LONG_LOAD_DURATION = args.long_load_duration
    SCAN_DUR = args.scan_duration
    SCAN_CONFIRMATIONS = args.scan_confirmations
    MAX_ACCEPTABLE_P99_US = int(args.latency_limit_ms * 1000)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RUN_LOG = open(OUTPUT_DIR / "run.log", "w", buffering=1)
    atexit.register(RUN_LOG.close)
    log(f"输出目录: {OUTPUT_DIR}")

    if not os.path.exists(SERVER_BIN):
        log(f"错误: 未找到服务器二进制 {SERVER_BIN}，请先 cmake --build build")
        sys.exit(1)

    validate_wrk2()
    validate_port_available()

    soft, hard = raise_nofile()
    if soft is not None:
        log(f"FD 上限: {soft} -> {hard}")

    gov_path = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    if gov_path.exists():
        cur = gov_path.read_text().strip()
        if cur != "performance" and set_governor("performance"):
            _OLD_GOVERNOR = cur
            atexit.register(restore_governor)
            log(f"governor 已切到 performance（原 {cur}）")

    if args.conn:
        rows = run_conn_scan(args.conn, CONN_LOAD, CONN_RATE, CONN_LIST, CONN_DUR)
        conn_csv = OUTPUT_DIR / "conn_results.csv"
        with open(conn_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CONN_CSV_FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        log(f"\n峰值并发结果: {conn_csv}")
        for r in rows:
            errs = r["err_connect"] + r["err_read"] + r["err_write"] + r["err_timeout"]
            log(f"  conn={r['conns']:>6}  QPS={r['qps']:>10,.1f} ({r['qps_ratio']*100:.1f}%)  "
                f"P99={r['lat_p99']}  err={errs}  RSS={r['rss_mb']}MB  FD={r['fd']}")
        return

    plan_names = (list(PLANS.keys()) if args.all else [args.plan])

    csv_path = OUTPUT_DIR / "results.csv"
    with open(csv_path, "w", newline="") as csv_file:
        csv.DictWriter(csv_file, fieldnames=CSV_FIELDS, extrasaction="ignore").writeheader()

    all_rows = []
    for plan_name in plan_names:
        plan = PLANS[plan_name]
        scenes = [args.scene] if (args.scene and not args.all) else plan["scenes"]
        loads = plan["loads"]
        modes = plan["modes"]
        log(f"\n计划: {plan_name}，场景 {scenes}，负载 {loads}，模式 {modes}")
        for name in scenes:
            if plan_name == "async":
                all_rows.extend(run_async_comparison(name, loads, plan_name))
            else:
                all_rows.extend(run_scenario(name, loads, modes, plan_name))

    log(f"\n结果已写入: {csv_path}")
    log(f"共 {len(all_rows)} 条记录")

    # 汇总表
    log("\n════════ 结果汇总 ════════")
    for r in all_rows:
        p99 = r["lat_p99"]
        p99_str = f"{p99/1000:.2f}ms" if p99 is not None else "-"
        knee_rate = r.get("knee_rate")
        offered = r.get("offered_rate")
        if knee_rate is not None:
            rate_fmt = f"拐点-R={knee_rate:>7}"
        elif offered is not None:
            rate_fmt = f"固定-R={offered:>7}"
        else:
            rate_fmt = f"{'':>15}"
        log(f"  [{r['plan']:<11}] {r['scenario']:<8} {r['load']:<10} {r['mode']:<9} {rate_fmt}  "
            f"QPS={r['qps']:>10,.0f}  每核={r['qps_per_core']:>8}  P99={p99_str}  "
            f"wrk_cpu={r['wrk_cpu_avg']:.0f}%  {'⚠客户端饱和' if r['wrk_saturated'] else ''}")

    # 基准测试后单独采集火焰图（全景瓶颈刻画）
    if args.flame:
        collect_flamegraph(args.flame_scene)


if __name__ == "__main__":
    main()