#!/usr/bin/env python3
"""
一体化性能测试脚本 (Python 版)
- 多 worker 场景循环测试
- taskset 绑核
- 记录 CPU 温度/频率/每核使用率/内存/网络/上下文切换/TIME_WAIT
- 短连接 / 长连接 对照
- 多响应大小维度（/ 和 /data/1k ~ /data/64k）
- 每档多轮取中位数，降低噪声
- 结果同时输出到 report.log 与 results.csv（便于绘图分析）

用法:
    python bench.py              # 自动启动服务器，按 TEST_SCENARIOS 跑全量
    python bench.py <pid>        # 对已有服务器压测（不绑核，不重启）

维度说明（本脚本会遍历以下维度做笛卡尔积）：
    scenario × CONNECTION_MODES × PATHS × CONCURRENCY × REPEAT
如需控制总时长，优先调小 REPEAT、DURATION，或裁剪 PATHS / CONCURRENCY。
"""

import subprocess
import time
import os
import sys
import threading
import re
import shutil
import signal
import csv
import atexit
import resource
from datetime import datetime
from pathlib import Path

try:
    import psutil
except ImportError:
    print("请先安装: pip install psutil")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════
#                    配置区（需要你修改）
# ═══════════════════════════════════════════════════════════════

# 路径基于脚本自身位置推导，无论从哪个目录运行都能正确定位二进制与输出目录
SCRIPT_DIR    = Path(__file__).resolve().parent       # benchmark/scripts/
BENCHMARK_DIR = SCRIPT_DIR.parent                     # benchmark/
PROJECT_DIR   = BENCHMARK_DIR.parent                  # 项目根目录

SERVER_BIN   = str(PROJECT_DIR / "build" / "Reactor")
URL          = "http://127.0.0.1:6666"                      # 不带尾斜杠，路径由 PATHS 提供
CONCURRENCY  = [500, 1000, 2000, 5000]   # 每个场景的并发梯度（500 低并发对照，5000 压极限）
WRK_THREADS  = 4
DURATION     = 30
SAMPLE_INTERVAL = 1.0
# 低于此并发用单 wrk 实例（延迟统计准确），否则用全部实例（多实例打吞吐）。
# 因为 wrk 的延迟分位数是单进程统计，多进程无法精确合并，故延迟在低并发单实例下测。
SINGLE_INSTANCE_CONCURRENCY = 2000

# 连接模式：short=短连接(Connection: close)，keepalive=长连接
CONNECTION_MODES = ["short", "keepalive"]

# 响应大小维度：URL 路径 → 标签
# 服务器端在 app_server_sync::push_response 里按 url 路由到不同大小的响应体
PATHS = [
    ("/",          "11B"),
    ("/data/4k",   "4KB"),
    ("/data/16k",  "16KB"),
    ("/data/64k",  "64KB")
]

# 每档重复次数，取 QPS 中位数作为代表值（想快速跑可设 1）
REPEAT = 3

# 场景列表：worker 数、服务器参数、绑核
#
# CPU 拓扑（i7-13650HX，6 P 核 + 8 E 核 = 14 物理核 / 20 逻辑核）：
#   P 核（性能核，带超线程，延迟敏感）：cpu 0-11
#       每个物理核取一个逻辑核以避免超线程争抢：0,2,4,6,8,10
#       （其中 cpu 4,6 为 4.9GHz 最高频核，cpu 0,2,8,10 为 4.7GHz）
#   E 核（能效核，无超线程，8 个）：cpu 12-19
#
# 绑核原则：
#   - subreactor 是延迟敏感的 I/O 线程，优先独占 P 物理核；
#   - main reactor 只做 accept，负载轻，与 sub 同池即可；
#   - wrk 用剩余核，尽量不抢服务器的 P 核；sub 数越少，wrk 能分到的 P 核越多。
#   - 注：8-sub 因超过 P 核数 + wrk 客户端饱和，实验条件不具备，已移除。
#
# 全量时长估算：场景数 x 连接模式 x 路径 x 并发 x 重复 x DURATION
#              = 6 x 2 x 4 x 4 x 3 x 30s ≈ 4.8 小时（另有 warmup 与档间 sleep）
#
# 场景分同步/异步两组：
#   - 同步：业务在 subreactor 线程（server_args 只传 port + subreactor 数）；
#   - 异步：业务在独立 worker 线程池（server_args 追加 mode=1 + worker 数），
#     worker 数取 subreactor 数，业务处理单元数与同步一致，便于对照跨线程开销。
#     异步的 worker 线程额外占用 E 核（见 server_cores 尾部多出的 E 核）。
TEST_SCENARIOS = [
    # ---- 同步 ----
    {
        "name": "1-subreactor",
        "model": "sync",
        "server_args": ["6666", "1"],
        "server_cores": "0,2",             # 2 个 P 核（main + 1 sub）
        "wrk_cores":    "4,6,8,10",        # 4 P
    },
    {
        "name": "2-subreactor",
        "model": "sync",
        "server_args": ["6666", "2"],
        "server_cores": "0,2,4",           # 3 个 P 核（main + 2 sub）
        "wrk_cores":    "6,8,10,12",       # 3 P + 1 E
    },
    {
        "name": "4-subreactor",
        "model": "sync",
        "server_args": ["6666", "4"],
        "server_cores": "0,2,4,6,8",       # 5 个 P 核（main + 4 sub）
        # 双 wrk 实例各绑 4 核，绕开单进程并发瓶颈（合计 1 P + 7 E）
        "wrk_cores":    "10,12,13,14;15,16,17,18",
    },
    # ---- 异步（mode=1，worker 数 = subreactor 数）----
    {
        "name": "1-subreactor-async",
        "model": "async",
        "server_args": ["6666", "1", "1", "1"],
        "server_cores": "0,2,12",          # 2 P（main + sub）+ 1 E（worker）
        "wrk_cores":    "4,6,8,10",        # 4 P
    },
    {
        "name": "2-subreactor-async",
        "model": "async",
        "server_args": ["6666", "2", "1", "2"],
        "server_cores": "0,2,4,12,13",     # 3 P（main + sub）+ 2 E（worker）
        "wrk_cores":    "6,8,10,14",       # 3 P + 1 E
    },
    {
        "name": "4-subreactor-async",
        "model": "async",
        "server_args": ["6666", "4", "1", "4"],
        "server_cores": "0,2,4,6,8,12,13,14,15",  # 5 P（main + sub）+ 4 E（worker）
        "wrk_cores":    "10,16,17,18",     # 1 P + 3 E（核紧张，可能客户端瓶颈）
    },
]

# ═══════════════════════════════════════════════════════════════

SERVER_PID_ARG = sys.argv[1] if len(sys.argv) > 1 else None

OUTPUT_DIR = BENCHMARK_DIR / "data" / "bench_log" / datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = OUTPUT_DIR / "report.log"
REPORT_FH = open(REPORT_PATH, "w", buffering=1)
CSV_PATH = OUTPUT_DIR / "results.csv"


def log(msg=""):
    print(msg)
    REPORT_FH.write(msg + "\n")


def section(title):
    log()
    log("═" * 72)
    log(title)
    log("═" * 72)


# ═══════════════════════════════════════════════════════════════
#                      硬件信息读取
# ═══════════════════════════════════════════════════════════════

def read_freqs():
    """读取每个逻辑核当前频率（MHz），失败返回空列表"""
    freqs = []
    for i in range(psutil.cpu_count()):
        try:
            with open(f"/sys/devices/system/cpu/cpu{i}/cpufreq/scaling_cur_freq") as f:
                freqs.append(int(f.read().strip()) / 1000.0)
        except Exception:
            freqs.append(0.0)
    return freqs


def read_temps():
    """读取温度，优先 psutil，回退到 sensors 命令"""
    try:
        temps = psutil.sensors_temperatures()
        result = {}
        for chip, entries in temps.items():
            for e in entries:
                label = e.label or chip
                if e.current:
                    result[label] = e.current
        if result:
            return result
    except Exception:
        pass

    if not shutil.which("sensors"):
        return {}
    try:
        out = subprocess.run(["sensors", "-u"], capture_output=True,
                             text=True, timeout=3).stdout
        result = {}
        current_key = None
        for line in out.splitlines():
            line = line.strip()
            if line.endswith(":") and not line.startswith("_"):
                current_key = line[:-1].strip()
            elif line.startswith("temp") and "_input" in line:
                m = re.search(r"temp\d+_input:\s+([\d.]+)", line)
                if m and current_key:
                    result[current_key] = float(m.group(1))
        return result
    except Exception:
        return {}


# ═══════════════════════════════════════════════════════════════
#                      监控线程
# ═══════════════════════════════════════════════════════════════

class Monitor(threading.Thread):
    def __init__(self, pid, interval):
        super().__init__(daemon=True)
        self.proc = psutil.Process(pid)
        self.interval = interval
        self.samples = []
        self._stop = threading.Event()
        self.proc.cpu_percent(interval=None)
        psutil.cpu_percent(percpu=True, interval=None)
        self._prev_net = psutil.net_io_counters()
        self._prev_time = time.time()
        self._prev_ctx = self._get_ctx()

    def _get_ctx(self):
        try:
            cs = self.proc.num_ctx_switches()
            return cs.voluntary + cs.involuntary
        except Exception:
            return 0

    def run(self):
        while not self._stop.is_set():
            now = time.time()
            try:
                cpu_proc = self.proc.cpu_percent(interval=None)
                with self.proc.oneshot():
                    mem = self.proc.memory_info().rss / 1024 / 1024
                    threads = self.proc.num_threads()
                    try:
                        fds = self.proc.num_fds()
                    except Exception:
                        fds = 0

                per_cpu = psutil.cpu_percent(percpu=True, interval=None)
                cpu_total = sum(per_cpu) / len(per_cpu) if per_cpu else 0

                freqs = read_freqs()
                temps = read_temps()

                net = psutil.net_io_counters()
                dt = now - self._prev_time
                rx = (net.bytes_recv - self._prev_net.bytes_recv) / dt / 1024 if dt > 0 else 0
                tx = (net.bytes_sent - self._prev_net.bytes_sent) / dt / 1024 if dt > 0 else 0
                self._prev_net = net
                self._prev_time = now

                ctx = self._get_ctx()
                ctx_rate = (ctx - self._prev_ctx) / dt if dt > 0 else 0
                self._prev_ctx = ctx

                self.samples.append({
                    "time": now,
                    "cpu_proc": cpu_proc, "cpu_total": cpu_total,
                    "mem": mem, "threads": threads, "fds": fds,
                    "rx": rx, "tx": tx, "ctx": ctx_rate,
                    "per_cpu": per_cpu, "freqs": freqs, "temps": temps,
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()

    def summary(self, t0, t1):
        ws = [s for s in self.samples if t0 <= s["time"] <= t1]
        if not ws:
            return None
        n = len(ws)

        freq_max = freq_min = freq_avg = 0
        all_freqs = [f for s in ws for f in s["freqs"] if f > 0]
        if all_freqs:
            freq_max = max(all_freqs)
            freq_min = min(all_freqs)
            freq_avg = sum(all_freqs) / len(all_freqs)

        temp_max = temp_avg = 0
        all_temps = [t for s in ws for t in s["temps"].values() if t > 0]
        if all_temps:
            temp_max = max(all_temps)
            temp_avg = sum(all_temps) / len(all_temps)

        n_cpu = len(ws[0]["per_cpu"]) if ws and ws[0]["per_cpu"] else 0
        per_cpu_avg = [0.0] * n_cpu
        if n_cpu:
            for s in ws:
                for i, v in enumerate(s["per_cpu"]):
                    per_cpu_avg[i] += v
            per_cpu_avg = [v / n for v in per_cpu_avg]

        return {
            "cpu_proc_avg": sum(s["cpu_proc"] for s in ws) / n,
            "cpu_proc_max": max(s["cpu_proc"] for s in ws),
            "cpu_total_avg": sum(s["cpu_total"] for s in ws) / n,
            "cpu_total_max": max(s["cpu_total"] for s in ws),
            "mem_avg": sum(s["mem"] for s in ws) / n,
            "mem_max": max(s["mem"] for s in ws),
            "threads_max": max(s["threads"] for s in ws),
            "fds_max": max(s["fds"] for s in ws),
            "rx_avg": sum(s["rx"] for s in ws) / n,
            "tx_avg": sum(s["tx"] for s in ws) / n,
            "ctx_avg": sum(s["ctx"] for s in ws) / n,
            "freq_avg": freq_avg, "freq_max": freq_max, "freq_min": freq_min,
            "temp_avg": temp_avg, "temp_max": temp_max,
            "per_cpu_avg": per_cpu_avg,
        }

    def clear(self):
        self.samples.clear()


# ═══════════════════════════════════════════════════════════════
#                      辅助函数
# ═══════════════════════════════════════════════════════════════

def get_time_wait():
    try:
        out = subprocess.run(["ss", "-tan", "state", "time-wait"],
                             capture_output=True, text=True, timeout=5).stdout
        return max(0, len(out.strip().split("\n")) - 1)
    except Exception:
        return -1


# ── CPU 调频策略（governor）控制 ────────────────────────────────
# 压测时切到 performance 保证 CPU 全程最高频、结果可复现；
# 测试结束后由 atexit 自动恢复原策略。
_OLD_GOVERNOR = None


def get_governor():
    """读取当前调频策略（取 cpu0，假设所有核一致）"""
    try:
        return Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").read_text().strip()
    except Exception:
        return None


def set_governor(governor):
    """切换所有 CPU 的调频策略，成功返回 True。非 root 时用 sudo -n（需 sudo 免密）。"""
    gov_files = [str(p) for p in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_governor")]
    if not gov_files:
        return False
    try:
        if os.geteuid() == 0:
            for f in gov_files:
                with open(f, "w") as fh:
                    fh.write(governor)
        else:
            sh_cmd = f"echo {governor} | tee " + " ".join(gov_files) + " > /dev/null"
            r = subprocess.run(["sudo", "-n", "sh", "-c", sh_cmd],
                               capture_output=True, timeout=10)
            if r.returncode != 0:
                return False
        return True
    except Exception:
        return False


def restore_governor():
    """测试结束后恢复原调频策略（由 atexit 调用，即使异常退出也会执行）"""
    global _OLD_GOVERNOR
    if _OLD_GOVERNOR:
        if set_governor(_OLD_GOVERNOR):
            print(f"已恢复调频策略 -> {_OLD_GOVERNOR}")
        else:
            print(f"警告: 恢复调频策略 -> {_OLD_GOVERNOR} 失败，请手动恢复")
        _OLD_GOVERNOR = None


# ── NMI watchdog 控制 ─────────────────────────────────────────
# NMI watchdog 会占用 PMU 硬件计数器，导致 perf 采样部分事件 <not counted>。
# 测试前临时禁用、结束后由 atexit 自动恢复。
_OLD_NMI_WATCHDOG = None


def get_nmi_watchdog():
    """读取当前 NMI watchdog 状态（0=禁用, 1=启用）"""
    try:
        return Path("/proc/sys/kernel/nmi_watchdog").read_text().strip()
    except Exception:
        return None


def set_nmi_watchdog(val):
    """设置 NMI watchdog（0/1），成功返回 True。非 root 时用 sudo -n。"""
    try:
        if os.geteuid() == 0:
            with open("/proc/sys/kernel/nmi_watchdog", "w") as fh:
                fh.write(val)
        else:
            r = subprocess.run(["sudo", "-n", "sh", "-c",
                                f"echo {val} > /proc/sys/kernel/nmi_watchdog"],
                               capture_output=True, timeout=10)
            if r.returncode != 0:
                return False
        return True
    except Exception:
        return False


def restore_nmi_watchdog():
    """测试结束后恢复 NMI watchdog（由 atexit 调用，即使异常退出也会执行）"""
    global _OLD_NMI_WATCHDOG
    if _OLD_NMI_WATCHDOG:
        if set_nmi_watchdog(_OLD_NMI_WATCHDOG):
            print(f"已恢复 NMI watchdog -> {_OLD_NMI_WATCHDOG}")
        else:
            print(f"警告: 恢复 NMI watchdog -> {_OLD_NMI_WATCHDOG} 失败，请手动恢复")
        _OLD_NMI_WATCHDOG = None


def parse_latency(s):
    """把 wrk 的延迟字符串（如 '2.18ms'）转成微秒数值"""
    if not s:
        return None
    m = re.match(r"([\d.]+)(us|ms|s)", s)
    if not m:
        return None
    v = float(m.group(1))
    unit = m.group(2)
    if unit == "us":
        return v
    if unit == "ms":
        return v * 1000.0
    if unit == "s":
        return v * 1000000.0
    return None


def us_to_str(us):
    """微秒数值 → 可读字符串"""
    if us is None:
        return "-"
    if us >= 1000000:
        return f"{us/1000000:.2f}s"
    if us >= 1000:
        return f"{us/1000:.2f}ms"
    return f"{us:.1f}us"


def parse_wrk(text):
    r = {"qps": 0.0, "transfer": "",
         "lat_avg_us": None, "lat_p50_us": None, "lat_p75_us": None,
         "lat_p90_us": None, "lat_p99_us": None,
         "err_connect": 0, "err_read": 0, "err_write": 0, "err_timeout": 0}

    for key, pat in [("qps", r"Requests/sec:\s+([\d.]+)"),
                     ("transfer", r"Transfer/sec:\s+([\d.]+\s*\w+)")]:
        m = re.search(pat, text)
        if m:
            r[key] = float(m.group(1)) if key == "qps" else m.group(1).strip()

    for key, pat in [("lat_avg_us", r"Latency\s+([\d.]+\s*\w+)"),
                     ("lat_p50_us", r"50%\s+([\d.]+\s*\w+)"),
                     ("lat_p75_us", r"75%\s+([\d.]+\s*\w+)"),
                     ("lat_p90_us", r"90%\s+([\d.]+\s*\w+)"),
                     ("lat_p99_us", r"99%\s+([\d.]+\s*\w+)")]:
        m = re.search(pat, text)
        if m:
            r[key] = parse_latency(m.group(1))

    m = re.search(r"Socket errors:\s*connect\s+(\d+),\s*read\s+(\d+),\s*write\s+(\d+),\s*timeout\s+(\d+)", text)
    if m:
        r["err_connect"], r["err_read"] = int(m.group(1)), int(m.group(2))
        r["err_write"], r["err_timeout"] = int(m.group(3)), int(m.group(4))
    return r


# ── perf stat 硬件计数器（缓存命中率 / IPC 等，用于定位瓶颈）──────

def _start_perf(pid, out_path):
    """后台启动 perf stat 采样服务器进程（DURATION 秒），返回 Popen 或 None"""
    if not shutil.which("perf"):
        return None
    # 只保留定位瓶颈的核心事件：IPC / 缓存命中率 / 分支预测 / 页错误 / 上下文切换。
    # 硬件计数器数量有限（一般 4~8 个），事件过多会分时复用导致 <not counted>；
    # stalled-cycles-* 在 perf 7.0 有已知 bug，已移除。
    events = (
        "instructions,cycles,"
        "cache-misses,cache-references,"
        "branch-misses,branch-instructions,"
        "page-faults,context-switches"
    )
    cmd = ["perf", "stat", "-p", str(pid), "-e", events,
           "-o", str(out_path), "--", "sleep", str(DURATION)]
    try:
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        return None


def parse_perf(path):
    """解析 perf stat 输出，返回 ipc / cache_miss_rate / branch_miss_rate 等"""
    try:
        text = Path(path).read_text()
    except Exception:
        return None

    vals = {}
    for line in text.splitlines():
        # 形如 "  1,234,567      cache-misses  # ..." 或 " <not counted>  instructions"
        # Intel 大小核（hybrid）下硬件事件会拆成 "cpu_core/instructions/" 与
        # "cpu_atom/instructions/"，需合并回 "instructions" 再累加。
        m = re.match(r"\s*([\d.,]+|<not[^>]*>)\s+(\S+)", line)
        if m:
            name = m.group(2)
            raw = m.group(1).replace(",", "")
            # cpu_core/xxx/ 或 cpu_atom/xxx/ -> xxx
            if "/" in name:
                name = name.split("/")[1]
            try:
                value = float(raw)
            except Exception:
                continue        # <not counted>/<not supported> 跳过
            vals[name] = vals.get(name, 0.0) + value

    def ratio(a, b):
        if vals.get(a) and vals.get(b):
            return round(vals[a] / vals[b], 6)
        return None

    return {
        "ipc": ratio("instructions", "cycles"),
        "cache_miss_rate": ratio("cache-misses", "cache-references"),
        "branch_miss_rate": ratio("branch-misses", "branch-instructions"),
        "page_faults": vals.get("page-faults"),
        "context_switches": vals.get("context-switches"),
    }


# ── 系统级累计计数器（功耗 / 网络重传 / 中断，测试前后做差）──────

def read_rapl_energy_uj():
    """读取 package 级累计能耗（微焦耳），失败返回 None"""
    total = 0.0
    found = False
    for d in Path("/sys/class/powercap").glob("intel-rapl:*"):
        try:
            name = (d / "name").read_text().strip()
            if not name.startswith("package-"):
                continue
            total += float((d / "energy_uj").read_text().strip())
            found = True
        except Exception:
            continue
    return total if found else None


def read_tcp_retrans():
    """读取 TCP 累计重传段数（/proc/net/snmp 的 RetransSegs），失败返回 None"""
    try:
        header = data = None
        for line in open("/proc/net/snmp"):
            if line.startswith("Tcp:"):
                if header is None:
                    header = line.split()
                else:
                    data = line.split()
        if header and data and "RetransSegs" in header:
            return int(data[header.index("RetransSegs")])
    except Exception:
        pass
    return None


def read_intr():
    """读取累计中断次数（/proc/stat 的 intr 行），失败返回 None"""
    try:
        for line in open("/proc/stat"):
            if line.startswith("intr "):
                return int(line.split()[1])
    except Exception:
        pass
    return None


def _read_sys_counters():
    """一次读取一组累计计数器（测试前后各读一次，做差）"""
    return {
        "energy_uj": read_rapl_energy_uj(),
        "tcp_retrans": read_tcp_retrans(),
        "intr": read_intr(),
    }


def make_lua(mode):
    """短连接需要 Lua 脚本；长连接返回 None（wrk 默认 keep-alive）"""
    if mode == "short":
        p = OUTPUT_DIR / "short.lua"
        p.write_text(
            'request = function()\n'
            '    return wrk.format("GET", nil, {["Connection"] = "close"})\n'
            'end\n'
        )
        return p
    return None


def _server_preexec():
    """fork 后、exec 服务器前执行：提高 FD 软上限 + 独立进程组。
    root 跑时软上限默认仅 1024，高并发（如 c2000）会触发 EMFILE，
    这里提到 1048576 避免 FD 耗尽。"""
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except Exception:
        pass
    os.setsid()


def start_server(scenario):
    """用 taskset 绑核启动服务器"""
    cmd = ["taskset", "-c", scenario["server_cores"], SERVER_BIN] + scenario["server_args"]
    log(f"启动命令: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=open(OUTPUT_DIR / f"server_{scenario['name']}.log", "w"),
        stderr=subprocess.STDOUT,
        preexec_fn=_server_preexec,
    )
    time.sleep(2)
    # 服务器若启动后立即退出，记录退出码便于定位（exit(1) vs 信号杀死/segfault）
    rc = proc.poll()
    if rc is not None:
        log(f"⚠️ 服务器启动后立即退出，退出码: {rc}（负值=被信号杀死，如 -11 即 SIGSEGV）")
    return proc


def stop_server(proc):
    """杀掉服务器进程组"""
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            pass


def warmup(scenario, pid):
    """预热：跑一轮短 wrk（不记录结果），让所有 subreactor 核心、连接池、CPU 缓存进入稳定状态。
    用 /data/16k（中等响应，覆盖内存拷贝 + CPU 处理两条路径），并发 2000 使轮询分发覆盖所有 sub。"""
    section("预热（warmup）")
    log(f"预热 10s @ {URL}/data/16k，并发 2000（结果不记录）")
    run_wrk_once(scenario, None, URL + "/data/16k", 2000, None, duration=10)
    time.sleep(2)


def _split_cores(cores):
    """把 wrk_cores 按分号拆成多实例的核列表；无分号则单实例。
    例如 "10,12,13,14;15,16,17,18" -> ["10,12,13,14", "15,16,17,18"]"""
    if not cores:
        return [None]
    parts = [s.strip() for s in str(cores).split(";") if s.strip()]
    return parts if parts else [None]


def merge_wrk_results(rs, outs):
    """汇总多个 wrk 实例：QPS/错误/字节相加，延迟按 QPS 加权平均"""
    merged = dict(rs[0]) if rs else {}
    total_qps = sum(r["qps"] for r in rs)
    merged["qps"] = total_qps

    # 延迟按 QPS 加权（请求数 ≈ qps × 时长，时长相同，权重即 qps）
    for key in ["lat_avg_us", "lat_p50_us", "lat_p75_us", "lat_p90_us", "lat_p99_us"]:
        vals = [(r.get(key), r["qps"]) for r in rs if r.get(key) is not None and r["qps"] > 0]
        merged[key] = (sum(v * w for v, w in vals) / sum(w for _, w in vals)) if vals else None

    # 错误相加
    for key in ["err_connect", "err_read", "err_write", "err_timeout"]:
        merged[key] = sum(r.get(key, 0) for r in rs)

    # 字节吞吐相加（transfer 形如 "5.84MB"）
    total_bytes = 0.0
    unit = ""
    for r in rs:
        m = re.match(r"([\d.]+)\s*(\w+)", r.get("transfer", ""))
        if m:
            total_bytes += float(m.group(1))
            unit = m.group(2)
    merged["transfer"] = f"{total_bytes:.2f}{unit}" if unit else ""

    merged_out = "\n".join(f"--- wrk 实例 {i+1} ---\n{o.strip()}" for i, o in enumerate(outs))
    return merged, merged_out


def run_wrk_once(scenario, lua_path, url, c, server_pid=None, duration=None):
    """单次（多实例）wrk，返回 (out, r_dict, t0, t1)；失败返回 (None, None, None, None)。
    wrk_cores 用分号分隔多个实例的绑核，绕开单进程并发瓶颈；QPS 取多实例之和。
    若传入 server_pid 且 perf 可用，则同时用 perf stat 采样服务器的硬件计数器。
    duration 为单次时长（秒），默认用全局 DURATION；warmup 传入短时长且不传 server_pid。"""
    dur = duration if duration is not None else DURATION

    # 多实例核分配 + 每实例并发
    cores_list = _split_cores(scenario.get("wrk_cores"))
    # 低并发用单实例（延迟统计准确），高并发用全部实例（多实例打吞吐）
    if c < SINGLE_INSTANCE_CONCURRENCY:
        cores_list = cores_list[:1]
    n_instances = len(cores_list)
    per_c = max(1, c // n_instances)

    cmds = []
    for cores in cores_list:
        wrk_cmd = ["wrk", f"-t{WRK_THREADS}", f"-c{per_c}", f"-d{dur}s",
                   "--latency", "--timeout", "5s"]
        if lua_path:
            wrk_cmd += ["-s", str(lua_path)]
        wrk_cmd.append(url)
        cmds.append((["taskset", "-c", cores] + wrk_cmd) if cores else wrk_cmd)

    # 与 wrk 并行启动 perf 采样（若可用）
    perf_proc = None
    perf_out = None
    if server_pid:
        perf_out = OUTPUT_DIR / f"perf_{server_pid}_{int(time.time()*1000)}.txt"
        perf_proc = _start_perf(server_pid, perf_out)

    hw0 = _read_sys_counters()
    t0 = time.time()

    # 启动 N 个 wrk 实例，采样其 CPU 之和（判断客户端是否成为瓶颈）
    procs = [subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
             for cmd in cmds]
    wps = []
    for p in procs:
        try:
            wp = psutil.Process(p.pid)
            wp.cpu_percent(interval=None)   # 建立基准
            wps.append(wp)
        except Exception:
            wps.append(None)

    wrk_cpu = []
    deadline = time.time() + dur + 30
    while any(p.poll() is None for p in procs) and time.time() < deadline:
        total_cpu = 0.0
        for wp in wps:
            if wp:
                try:
                    total_cpu += wp.cpu_percent(interval=None)
                except Exception:
                    pass
        wrk_cpu.append(total_cpu)
        time.sleep(1)

    if any(p.poll() is None for p in procs):
        # wrk 超时
        for p in procs:
            if p.poll() is None:
                try:
                    p.kill()
                    p.wait()
                except Exception:
                    pass
        return None, None, None, None

    t1 = time.time()
    hw1 = _read_sys_counters()
    outs = [p.stdout.read() if p.stdout else "" for p in procs]

    rs = [parse_wrk(o) for o in outs]
    r, out = merge_wrk_results(rs, outs)
    r["wrk_instances"] = n_instances

    dt = t1 - t0
    if wrk_cpu:
        r["wrk_cpu_avg"] = sum(wrk_cpu) / len(wrk_cpu)
        r["wrk_cpu_max"] = max(wrk_cpu)
    else:
        r["wrk_cpu_avg"] = None
        r["wrk_cpu_max"] = None
    r["wrk_saturated"] = (r["wrk_cpu_avg"] is not None
                          and r["wrk_cpu_avg"] >= n_instances * WRK_THREADS * 95.0)
    r["hw"] = {
        "power_w": ((hw1["energy_uj"] - hw0["energy_uj"]) / 1e6 / dt)
                   if (hw0["energy_uj"] is not None and hw1["energy_uj"] is not None) else None,
        "tcp_retrans": (hw1["tcp_retrans"] - hw0["tcp_retrans"])
                       if (hw0["tcp_retrans"] is not None and hw1["tcp_retrans"] is not None) else None,
        "intr_rate": ((hw1["intr"] - hw0["intr"]) / dt)
                     if (hw0["intr"] is not None and hw1["intr"] is not None) else None,
    }

    if perf_proc:
        try:
            perf_proc.wait(timeout=DURATION + 10)
            r["perf"] = parse_perf(perf_out)
        except Exception:
            pass
        finally:
            try:
                if perf_out and perf_out.exists():
                    perf_out.unlink()
            except Exception:
                pass

    return out, r, t0, t1


def run_one_scenario(scenario):
    """跑一个 worker 场景：连接模式 × 路径 × 并发 × 重复，返回该场景结果"""
    section(f"场景: {scenario['name']}")
    log(f"服务器绑核: {scenario['server_cores']}")
    log(f"wrk 绑核   : {scenario['wrk_cores']}")
    log(f"服务器参数 : {scenario['server_args']}")

    proc = start_server(scenario)
    pid = proc.pid
    log(f"服务器 PID: {pid}")

    monitor = Monitor(pid, SAMPLE_INTERVAL)
    monitor.start()
    time.sleep(1)

    # 预热：跑一轮短 wrk，让所有 subreactor 核心、连接池、CPU 缓存进入稳定状态
    warmup(scenario, pid)

    results = []
    try:
        for mode in CONNECTION_MODES:
            lua_path = make_lua(mode)
            for path, label in PATHS:
                url = URL + path
                for c in CONCURRENCY:
                    section(f"[{scenario['name']}] {mode} / {label} / 并发 {c}")

                    # 重复 REPEAT 次，取 QPS 中位数的那次作为代表
                    reps = []
                    for rep in range(REPEAT):
                        out, r, t0, t1 = run_wrk_once(scenario, lua_path, url, c, pid)
                        if out is None:
                            log(f"  [rep {rep+1}/{REPEAT}] wrk 超时")
                        else:
                            r["sys"] = monitor.summary(t0, t1)
                            reps.append((r, out))
                            log(f"  [rep {rep+1}/{REPEAT}] QPS={r['qps']:.1f}")
                        time.sleep(2)   # 每档 wrk 后 sleep，缓解 TIME_WAIT 累积

                    if not reps:
                        continue

                    reps.sort(key=lambda x: x[0]["qps"])
                    r, out = reps[len(reps) // 2]   # 中位样本
                    qps_list = [x[0]["qps"] for x in reps]
                    r["qps_min"] = min(qps_list)
                    r["qps_max"] = max(qps_list)
                    r["concurrency"] = c
                    r["model"] = scenario.get("model", "sync")
                    r["mode"] = mode
                    r["path"] = label

                    log("\n--- wrk 输出（中位样本）---")
                    log(out)

                    s = r["sys"]
                    if s:
                        log("--- 该档系统指标 ---")
                        log(f"服务器进程 CPU 平均/峰值 : {s['cpu_proc_avg']:.1f}% / {s['cpu_proc_max']:.1f}%")
                        log(f"系统总 CPU 平均/峰值     : {s['cpu_total_avg']:.1f}% / {s['cpu_total_max']:.1f}%")
                        log(f"CPU 频率 平均/最低/最高  : {s['freq_avg']:.0f} / {s['freq_min']:.0f} / {s['freq_max']:.0f} MHz")
                        log(f"CPU 温度 平均/最高       : {s['temp_avg']:.1f} / {s['temp_max']:.1f} °C")
                        log(f"内存 RSS 平均/峰值       : {s['mem_avg']:.1f} / {s['mem_max']:.1f} MB")
                        log(f"线程数峰值               : {s['threads_max']}")
                        log(f"FD 数峰值                : {s['fds_max']}")
                        log(f"网络 RX/TX               : {s['rx_avg']:.1f} / {s['tx_avg']:.1f} KB/s")
                        log(f"上下文切换               : {s['ctx_avg']:.0f} /s")
                        if s["per_cpu_avg"]:
                            per_cpu_str = " ".join(f"cpu{i}:{v:.0f}%" for i, v in enumerate(s["per_cpu_avg"]))
                            log(f"每核 CPU 平均             : {per_cpu_str}")
                    log(f"TIME_WAIT 连接           : {get_time_wait()}")
                    perf = r.get("perf") or {}
                    if perf:
                        log("--- perf 硬件计数器 ---")
                        for metric_name, key in [
                            ("IPC(指令/周期)", "ipc"),
                            ("缓存未命中率", "cache_miss_rate"),
                            ("分支预测未命中率", "branch_miss_rate"),
                            ("上下文切换", "context_switches"),
                            ("页错误数", "page_faults"),
                        ]:
                            v = perf.get(key)
                            log(f"{metric_name:<24}: {v if v is not None else '-'}")
                    hw = r.get("hw") or {}
                    if hw:
                        log("--- 系统硬件计数 ---")
                        pw = hw.get("power_w")
                        ir = hw.get("intr_rate")
                        tr = hw.get("tcp_retrans")
                        log(f"CPU 平均功耗             : {pw:.1f} W" if pw is not None else "CPU 平均功耗             : -")
                        log(f"中断速率                 : {ir:.0f} /s" if ir is not None else "中断速率                 : -")
                        log(f"TCP 重传段数             : {tr}" if tr is not None else "TCP 重传段数             : -")
                    log(f"QPS 多轮范围             : {r['qps_min']:.1f} ~ {r['qps_max']:.1f} (中位 {r['qps']:.1f})")
                    wca = r.get("wrk_cpu_avg")
                    if wca is not None:
                        if r.get("wrk_saturated"):
                            log(f"⚠️ wrk 客户端已饱和（CPU {wca:.0f}%/{WRK_THREADS*100}%），该档 QPS 可能受客户端限制")
                        else:
                            log(f"wrk 客户端 CPU {wca:.0f}%/{WRK_THREADS*100}%，未饱和")

                    (OUTPUT_DIR / f"wrk_{scenario['name']}_{r['model']}_{mode}_{label}_c{c}.log").write_text(out)
                    results.append(r)
                    monitor.clear()
    finally:
        monitor.stop()
        monitor.join(timeout=3)
        stop_server(proc)
        time.sleep(2)   # 等端口释放

    return results


# ═══════════════════════════════════════════════════════════════
#                      结果落盘
# ═══════════════════════════════════════════════════════════════

CSV_FIELDS = [
    "scenario", "model", "mode", "path", "concurrency",
    "qps", "qps_min", "qps_max", "transfer",
    "lat_avg_us", "lat_p50_us", "lat_p75_us", "lat_p90_us", "lat_p99_us",
    "cpu_proc_avg", "cpu_total_avg", "mem_avg", "fds_max", "threads_max",
    "ctx_avg", "rx_avg", "tx_avg", "freq_avg", "temp_max",
    "ipc", "cache_miss_rate", "branch_miss_rate",
    "page_faults", "power_w", "intr_rate", "tcp_retrans",
    "wrk_cpu_avg", "wrk_saturated", "wrk_instances",
    "err_connect", "err_read", "err_write", "err_timeout",
]


def write_csv(all_results):
    """把每个场景的所有原始数据写入 results.csv，方便用 pandas/matplotlib 分析"""
    rows = []
    for scenario_name, results in all_results.items():
        for r in results:
            s = r.get("sys") or {}
            rows.append({
                "scenario": scenario_name,
                "model": r.get("model", ""),
                "mode": r.get("mode", ""),
                "path": r.get("path", ""),
                "concurrency": r.get("concurrency", ""),
                "qps": round(r["qps"], 2),
                "qps_min": round(r.get("qps_min", r["qps"]), 2),
                "qps_max": round(r.get("qps_max", r["qps"]), 2),
                "transfer": r.get("transfer", ""),
                "lat_avg_us": r.get("lat_avg_us"),
                "lat_p50_us": r.get("lat_p50_us"),
                "lat_p75_us": r.get("lat_p75_us"),
                "lat_p90_us": r.get("lat_p90_us"),
                "lat_p99_us": r.get("lat_p99_us"),
                "cpu_proc_avg": round(s.get("cpu_proc_avg", 0), 2),
                "cpu_total_avg": round(s.get("cpu_total_avg", 0), 2),
                "mem_avg": round(s.get("mem_avg", 0), 2),
                "fds_max": s.get("fds_max", 0),
                "threads_max": s.get("threads_max", 0),
                "ctx_avg": round(s.get("ctx_avg", 0), 1),
                "rx_avg": round(s.get("rx_avg", 0), 2),
                "tx_avg": round(s.get("tx_avg", 0), 2),
                "freq_avg": round(s.get("freq_avg", 0), 0),
                "temp_max": round(s.get("temp_max", 0), 1),
                "ipc": (r.get("perf") or {}).get("ipc"),
                "cache_miss_rate": (r.get("perf") or {}).get("cache_miss_rate"),
                "branch_miss_rate": (r.get("perf") or {}).get("branch_miss_rate"),
                "page_faults": (r.get("perf") or {}).get("page_faults"),
                "power_w": (r.get("hw") or {}).get("power_w"),
                "intr_rate": (r.get("hw") or {}).get("intr_rate"),
                "tcp_retrans": (r.get("hw") or {}).get("tcp_retrans"),
                "wrk_cpu_avg": r.get("wrk_cpu_avg"),
                "wrk_saturated": r.get("wrk_saturated"),
                "wrk_instances": r.get("wrk_instances", 1),
                "err_connect": r.get("err_connect", 0),
                "err_read": r.get("err_read", 0),
                "err_write": r.get("err_write", 0),
                "err_timeout": r.get("err_timeout", 0),
            })

    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    log(f"CSV 已写入: {CSV_PATH.absolute()}")


# ═══════════════════════════════════════════════════════════════
#                      主流程
# ═══════════════════════════════════════════════════════════════

def print_summary(all_results):
    section("汇总表")
    for name, results in all_results.items():
        log(f"\n【场景: {name}】")
        header = (f"{'模型':<6} {'模式':<10} {'路径':<8} {'并发':<6} {'QPS':<10} "
                  f"{'P50':<10} {'P90':<10} {'P99':<10} "
                  f"{'CPU%':<7} {'MEM(MB)':<9} {'CTX/s':<9} {'错误':<5}")
        log(header)
        log("-" * len(header))
        for r in results:
            s = r.get("sys") or {}
            errs = r["err_connect"] + r["err_read"] + r["err_write"] + r["err_timeout"]
            log(f"{r.get('model',''):<6} {r.get('mode',''):<10} {r.get('path',''):<8} {r['concurrency']:<6} "
                f"{r['qps']:<10.1f} {us_to_str(r.get('lat_p50_us')):<10} "
                f"{us_to_str(r.get('lat_p90_us')):<10} {us_to_str(r.get('lat_p99_us')):<10} "
                f"{s.get('cpu_proc_avg', 0):<7.1f} {s.get('mem_avg', 0):<9.1f} "
                f"{s.get('ctx_avg', 0):<9.0f} {errs:<5}")


def main():
    if not shutil.which("wrk"):
        print("错误: 未找到 wrk，请先 sudo apt install wrk")
        sys.exit(1)
    if not shutil.which("taskset"):
        print("错误: 未找到 taskset，请先 sudo apt install util-linux")
        sys.exit(1)

    # 环境信息
    section("环境信息")
    try:
        _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (_hard, _hard))
        log(f"FD 软上限  : {_soft} -> {_hard}（已提高，避免 wrk/服务器高并发 FD 耗尽）")
    except Exception:
        pass
    log(f"时间      : {datetime.now()}")
    try:
        cpu = subprocess.run(["lscpu"], capture_output=True, text=True).stdout
        for line in cpu.split("\n"):
            if "Model name" in line:
                log(f"CPU       : {line.split(':', 1)[1].strip()}")
                break
    except Exception:
        pass
    log(f"逻辑核数  : {psutil.cpu_count()}")
    log(f"物理核数  : {psutil.cpu_count(logical=False)}")
    log(f"内存      : {psutil.virtual_memory().total / 1024**3:.1f} GB")
    log(f"内核      : {os.uname().release}")
    log(f"URL       : {URL}")
    log(f"连接模式  : {CONNECTION_MODES}")
    log(f"响应路径  : {[p for p, _ in PATHS]}")
    log(f"wrk 线程  : {WRK_THREADS}")
    log(f"并发梯度  : {CONCURRENCY}")
    log(f"每档时长  : {DURATION}s x {REPEAT} 轮(取中位)")
    old_gov = get_governor()
    if old_gov:
        log(f"调频策略  : {old_gov}（测试前切 performance，结束后自动恢复）")
    if old_gov and old_gov != "performance":
        if set_governor("performance"):
            global _OLD_GOVERNOR
            _OLD_GOVERNOR = old_gov
            atexit.register(restore_governor)
            log("已切换调频策略 -> performance")
        else:
            log("警告: 切换 governor 失败（需 root 或 sudo 免密），频率波动会导致结果失真，建议 sudo 运行本脚本。")

    # NMI watchdog 会占用 PMU 计数器，导致 perf 采样部分硬件事件 <not counted>，
    # 测试前临时禁用、结束后由 atexit 自动恢复。
    old_nmi = get_nmi_watchdog()
    if old_nmi is not None:
        log(f"NMI watchdog : {old_nmi}（测试前临时禁用，结束后自动恢复）")
    if old_nmi is not None and old_nmi != "0":
        if set_nmi_watchdog("0"):
            global _OLD_NMI_WATCHDOG
            _OLD_NMI_WATCHDOG = old_nmi
            atexit.register(restore_nmi_watchdog)
            log("已禁用 NMI watchdog（释放 PMU 计数器供 perf 采样）")
        else:
            log("警告: 禁用 NMI watchdog 失败，perf 硬件计数器可能 <not counted>")

    # 已有 PID 模式：只测一个场景，不绑核、不重启
    if SERVER_PID_ARG:
        pid = int(SERVER_PID_ARG)
        if not psutil.pid_exists(pid):
            print(f"错误: PID {pid} 不存在")
            sys.exit(1)
        section(f"对已有 PID {pid} 压测（不绑核、不重启）")
        monitor = Monitor(pid, SAMPLE_INTERVAL)
        monitor.start()
        time.sleep(1)
        results = []
        for mode in CONNECTION_MODES:
            lua_path = make_lua(mode)
            for path, label in PATHS:
                url = URL + path
                for c in CONCURRENCY:
                    section(f"{mode} / {label} / 并发 {c}")
                    reps = []
                    for rep in range(REPEAT):
                        out, r, t0, t1 = run_wrk_once({}, lua_path, url, c, pid)
                        if out is None:
                            continue
                        r["sys"] = monitor.summary(t0, t1)
                        reps.append((r, out))
                    if not reps:
                        continue
                    reps.sort(key=lambda x: x[0]["qps"])
                    r, out = reps[len(reps) // 2]
                    r["concurrency"] = c
                    r["model"] = "existing"
                    r["mode"] = mode
                    r["path"] = label
                    qps_list = [x[0]["qps"] for x in reps]
                    r["qps_min"] = min(qps_list)
                    r["qps_max"] = max(qps_list)
                    log(out)
                    s = r["sys"]
                    if s:
                        log(f"CPU 进程 {s['cpu_proc_avg']:.1f}% / 系统 {s['cpu_total_avg']:.1f}%")
                        log(f"频率 {s['freq_avg']:.0f} MHz / 温度 {s['temp_avg']:.1f}°C")
                    log(f"QPS 范围: {r['qps_min']:.1f} ~ {r['qps_max']:.1f} (中位 {r['qps']:.1f})")
                    results.append(r)
                    monitor.clear()
        monitor.stop()
        write_csv({"existing": results})
        print_summary({"existing": results})
        return

    # 多场景循环
    all_results = {}
    for sc in TEST_SCENARIOS:
        all_results[sc["name"]] = run_one_scenario(sc)

    write_csv(all_results)
    print_summary(all_results)

    log()
    log(f"完整报告: {REPORT_PATH.absolute()}")
    log(f"CSV 数据: {CSV_PATH.absolute()}")


if __name__ == "__main__":
    try:
        main()
    finally:
        REPORT_FH.close()
