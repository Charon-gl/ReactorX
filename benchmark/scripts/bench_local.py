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
# echo=纯回包；compute=CPU 密集（默认 ~1ms/请求）；delay=I/O 等待（默认 1ms/请求）
# est_per_core 按本机历史拐点校准（014131 拐点 11B=220k / 16KB=140k / 64KB=60k，除以 5 核），
# 让历史拐点落在候选中间（1.0×est 附近），上界比历史拐点高约 30% 留余量。
LOADS = {
    "11B":     ("/",          57000),
    "16KB":    ("/data/16k",  36000),
    "64KB":    ("/data/64k",  16000),
    "compute": ("/compute?n=700000", 1000),   # 本机 P 核校准：700000 次 ≈ 1ms
    "delay":   ("/delay?ms=1",       1000),   # sleep 1ms（实测 ~1.2ms，含调度开销）
}

# ── 连接模式：keepalive（串行一请求一响应）/ pipeline（连续 N 请求）──
# pipeline 服务端零改动（llhttp 循环解析），客户端用 lua 拼接连续流
ALL_MODES = ["keepalive", "pipeline"]

# ── 测试计划 ──────────────────────────────────────────────────────
# baseline：    sync-4 × keepalive × 11B/16KB/64KB —— 框架每请求开销 + 带宽瓶颈
# pipeline：    sync-4 × {keepalive, pipeline} × 64KB —— 批处理/队头阻塞
# async：       {sync-4, async-4} × keepalive × {11B, compute, delay} —— 模型取舍
# scalability： sync-1/4 × keepalive × 16KB —— 本机扩展性（主力扩展性在云上）
PLANS = {
    "baseline":    {"scenes": ["sync-4"],            "loads": ["11B", "16KB", "64KB"], "modes": ["keepalive"]},
    "pipeline":    {"scenes": ["sync-4"],            "loads": ["64KB"],               "modes": ["keepalive", "pipeline"]},
    "async":       {"scenes": ["sync-4", "async-4"], "loads": ["11B", "compute", "delay"], "modes": ["keepalive"]},
    "scalability": {"scenes": ["sync-1", "sync-4"],  "loads": ["16KB"],               "modes": ["keepalive"]},
}

PIPELINE_DEPTH = 3            # pipeline 深度（本机精简版暂不用 pipeline，保留供后续）
WRK_CONC       = 2000         # wrk 固定连接数
WRK_THREADS    = 0            # 0 表示自动 = wrk 核数
DURATION       = 30           # 拐点处正式测试时长（秒）
REPEAT         = 3            # 拐点处重复次数，取中位数（平滑偶发噪声）
SCAN_DUR       = 15           # 峰值扫描每档时长（秒），15s 让尾延迟 P99 更稳
KNEE_RATIO     = 5            # 拐点突变倍数：P99 涨到健康基线的 5 倍即视为过载（相对突变，自适应任何负载）
KNEE_FLOOR_US  = 5_000        # 阈值下限：健康基线太小时（如 11B）防止噪声误判（wrk2 协调遗漏修正虚高约 1ms）
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


# ═══════════════════════════════════════════════════════════════════

def log(msg=""):
    print(msg, flush=True)


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
    r = {"qps": 0.0, "err_connect": 0, "err_read": 0, "err_write": 0, "err_timeout": 0, "lat": {}}
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
    m = re.search(r"Socket errors:\s*connect\s+(\d+),\s*read\s+(\d+),\s*write\s+(\d+),\s*timeout\s+(\d+)", text)
    if m:
        r["err_connect"], r["err_read"], r["err_write"], r["err_timeout"] = \
            int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    return r


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
            lines.append(f'   r[{i+1}] = wrk.format("GET", "{url_path}?p{i}")')
        lines += ['   req = table.concat(r)', 'end',
                  'request = function()', '   return req', 'end']
        p.write_text("\n".join(lines) + "\n")
    return p


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
def run_wrk(url, c, lua_path, wrk_cores, dur, server_pid, rate, perf_events=None):
    import psutil
    n_cores = len(wrk_cores)
    threads = WRK_THREADS or n_cores
    cmd = ["taskset", "-c", cores_str(wrk_cores),
           WRK_BIN, f"-t{threads}", f"-c{c}", f"-d{dur}s"]
    if rate is not None:
        cmd += [f"-R{rate}"]
    cmd += ["--latency", "--timeout", "5s"]
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
    # pipeline 模式：wrk 统计的是"批次数"，真实 HTTP 吞吐 = QPS × depth
    if lua_path and "pipeline.lua" in str(lua_path):
        r["qps"] = r["qps"] * PIPELINE_DEPTH
    return r, wrk_cpu, out, perf


# ── 峰值扫描（自适应基线 + 相对突变，找 P99 暴涨的拐点）────────────
def scan_knee(name, sub, port, server_cores, wrk_cores, server_pid, url_path, est_per_core):
    """自适应拐点检测：先测最低档作为健康基线，以「基线 × KNEE_RATIO」为过载阈值，
    从低到高扫，P99 相对基线突变（超过阈值）即判过载，拐点取前一档。
    阈值自适应于实际负载的健康延迟，不硬编码任何消息大小的绝对延迟。"""
    url = f"http://{HOST}:{port}{url_path}"
    est = max(1000, sub * est_per_core)
    candidates = sorted({int(est * f) for f in (0.2, 0.4, 0.6, 0.8, 1.0, 1.25)})
    log(f"  [{name}] {url_path} 预估 {est}，-R 候选: {candidates}")

    # 1) 先测最低档，作为健康基线；过载阈值 = max(基线 × KNEE_RATIO, KNEE_FLOOR_US)
    r, _, _, _ = run_wrk(url, WRK_CONC, None, wrk_cores, SCAN_DUR, server_pid, candidates[0])
    base_p99 = r["lat"].get(99.0) or KNEE_FLOOR_US
    threshold = max(base_p99 * KNEE_RATIO, KNEE_FLOOR_US)
    log(f"  [{name}] 基线 P99={base_p99:.0f}us，过载阈值={threshold:.0f}us")

    prev_ok = candidates[0]
    for rate in candidates[1:]:
        r, wrk_cpu, _, _ = run_wrk(url, WRK_CONC, None, wrk_cores, SCAN_DUR, server_pid, rate)
        p99 = r["lat"].get(99.0)
        err = r["err_connect"] + r["err_read"] + r["err_write"] + r["err_timeout"]
        log(f"  [{name}] 扫 -R{rate}: QPS={r['qps']:.0f}, p99={p99}, err={err}")
        if r["qps"] <= 0 or wrk_cpu > len(wrk_cores) * 90.0:
            log(f"  [{name}] -R{rate} 失败或客户端饱和，拐点取前一档 {prev_ok}")
            return prev_ok
        if (p99 is not None and p99 > threshold) or err > 0:
            log(f"  [{name}] -R{rate} 过载（P99>{threshold/1000:.0f}ms 或出错），拐点取前一档 {prev_ok}")
            return prev_ok
        prev_ok = rate
    log(f"  [{name}] 未扫到拐点，用最高候选 {candidates[-1]}")
    return candidates[-1]


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
            knee = scan_knee(name, sub, port, server_cores, wrk_cores, srv.pid, url_path, est_per_core)
            url = f"http://{HOST}:{port}{url_path}"
            for mode in modes:
                lua = make_lua(mode, url_path)
                log(f"  [{name}] {lname}/{mode} 拐点 -R{knee}，正式测 {DURATION}s × {REPEAT} 取中位数 + perf ...")
                reps = []
                for rep in range(REPEAT):
                    r, wrk_cpu, out, perf = run_wrk(url, WRK_CONC, lua, wrk_cores, DURATION,
                                                    srv.pid, knee, PERF_CORE_EVENTS)
                    if r["qps"] <= 0:
                        log(f"  [{name}] {lname}/{mode} rep{rep+1}: wrk 失败")
                        continue
                    reps.append((r, wrk_cpu, out, perf))
                    log(f"  [{name}] {lname}/{mode} rep{rep+1}: QPS={r['qps']:,.1f}  wrk_cpu={wrk_cpu:.0f}%")
                    time.sleep(1)
                if not reps:
                    log(f"  [{name}] {lname}/{mode} 拐点测试全部失败")
                    continue
                reps.sort(key=lambda x: x[0]["qps"])
                r, wrk_cpu, out, perf = reps[len(reps) // 2]   # 取中位样本
                lat = r["lat"]
                row = {
                    "plan": plan, "scenario": name, "model": model, "sub": sub,
                    "server_cores": len(server_cores), "wrk_cores": len(wrk_cores),
                    "load": lname, "path": url_path, "mode": mode, "knee_rate": knee,
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
                (OUTPUT_DIR / f"wrk_{log_name}_{lname}_{mode}.log").write_text(out)
                log(f"  [{name}] {lname}/{mode} 拐点 QPS={r['qps']:,.1f}  每核={row['qps_per_core']}  P99={lat.get(99.0)}  wrk_cpu={wrk_cpu:.0f}%")
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
        peak = scan_knee(name, sub, port, server_cores, wrk_cores, srv.pid, path, est_per_core)

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
    "load", "path", "mode", "knee_rate",
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
    global REPEAT
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=str, default="baseline", choices=list(PLANS.keys()),
                        help="测试计划：" + " / ".join(PLANS.keys()))
    parser.add_argument("--scene", type=str, default=None, help="只跑某场景（调试，覆盖 plan）")
    parser.add_argument("--repeat", type=int, default=REPEAT, help=f"拐点处重复次数（取中位数），默认 {REPEAT}")
    parser.add_argument("--flame", action="store_true", help="基准测试后单独采集火焰图（sync-4 64KB 峰值档）")
    parser.add_argument("--conn", type=str, default=None, help="峰值并发扫描：场景名（如 sync-16），固定 -R 扫连接数")
    parser.add_argument("--all", action="store_true", help="依次跑所有计划（baseline+pipeline+async+scalability），结果汇总到同一 csv")
    args = parser.parse_args()

    REPEAT = args.repeat

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log(f"输出目录: {OUTPUT_DIR}")

    if not os.path.exists(SERVER_BIN):
        log(f"错误: 未找到服务器二进制 {SERVER_BIN}，请先 cmake --build build")
        sys.exit(1)

    log("清理残留 Reactor / wrk 进程...")
    subprocess.run(["pkill", "-9", "-f", "benchmark/Reactor"], capture_output=True)
    subprocess.run(["pkill", "-9", "wrk"], capture_output=True)
    time.sleep(1)

    soft, hard = raise_nofile()
    if soft is not None:
        log(f"FD 上限: {soft} -> {hard}")

    global _OLD_GOVERNOR
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

    plan_names = list(PLANS.keys()) if args.all else [args.plan]

    all_rows = []
    for plan_name in plan_names:
        plan = PLANS[plan_name]
        scenes = [args.scene] if (args.scene and not args.all) else plan["scenes"]
        loads = plan["loads"]
        modes = plan["modes"]
        log(f"\n计划: {plan_name}，场景 {scenes}，负载 {loads}，模式 {modes}")
        for name in scenes:
            all_rows.extend(run_scenario(name, loads, modes, plan_name))

    csv_path = OUTPUT_DIR / "results.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)

    log(f"\n结果已写入: {csv_path}")
    log(f"共 {len(all_rows)} 条记录")

    # 汇总表
    log("\n════════ 结果汇总 ════════")
    for r in all_rows:
        p99 = r["lat_p99"]
        p99_str = f"{p99/1000:.2f}ms" if p99 is not None else "-"
        log(f"  [{r['plan']:<11}] {r['scenario']:<8} {r['load']:<7} {r['mode']:<9} 拐点-R={r['knee_rate']:>7}  "
            f"QPS={r['qps']:>10,.0f}  每核={r['qps_per_core']:>8}  P99={p99_str}  "
            f"wrk_cpu={r['wrk_cpu_avg']:.0f}%  {'⚠客户端饱和' if r['wrk_saturated'] else ''}")

    # 基准测试后单独采集火焰图（全景瓶颈刻画）
    if args.flame:
        collect_flamegraph()


if __name__ == "__main__":
    main()
