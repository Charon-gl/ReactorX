#!/usr/bin/env python3
"""
bench_cloud.py —— ReactorX 云服务器测试脚本（框架版）

与本机脚本（bench_local.py）的差异：
  1. 核拓扑【自动探测】：读取 /sys 的 NUMA 节点 + 超线程兄弟，自动识别
     双路 NUMA 的 node0 / node1 物理核，无需手工写死核号。
  2. 场景更全：核足够时可测到 sync-32（本机 P 核仅 6 个，最多 4-sub）。
  3. 绑核用 numactl（绑定 NUMA 节点 + 内存），本机是单 NUMA 只用 taskset。

其余逻辑（wrk 执行、HDR 解析、perf 采样、峰值扫描、bind 重试、数据分级）
复用 bench_local.py，通过覆盖模块级配置与差异函数实现。

用法（在云服务器上）：
    sudo python3 bench_cloud.py               # 全量
    sudo python3 bench_cloud.py --scene sync-32
"""

import sys
import subprocess
import time
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
BENCHMARK_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

import bench_local as core  # 复用通用逻辑


# ═══════════════════════════════════════════════════════════════════
#                       核拓扑自动探测
# ═══════════════════════════════════════════════════════════════════

def parse_cpulist(s):
    """解析 '0-47,96-143' 格式的 CPU 列表为 list[int]"""
    out = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out


def read_siblings(cpu):
    try:
        p = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list")
        return parse_cpulist(p.read_text().strip())
    except Exception:
        return [cpu]


def physical_cores(cpus):
    """从逻辑核列表里去掉超线程兄弟：每个物理核只保留一个逻辑核。"""
    seen = set()
    out = []
    for cpu in cpus:
        key = tuple(sorted(read_siblings(cpu)))
        if key not in seen:
            seen.add(key)
            out.append(cpu)
    return out


def detect_topology():
    """探测 NUMA 节点 → 物理核映射。返回 {node_id: [物理核逻辑ID]}。
    失败时回退为单节点 {0: 全部物理核}。"""
    nodes = {}
    node_dir = Path("/sys/devices/system/node")
    for d in sorted(node_dir.glob("node*")):
        try:
            node_id = int(d.name.replace("node", ""))
            cpulist = (d / "cpulist").read_text().strip()
            nodes[node_id] = physical_cores(parse_cpulist(cpulist))
        except Exception:
            continue
    if not nodes:
        # 回退：所有 CPU 去超线程
        all_cpus = parse_cpulist(Path("/sys/devices/system/cpu/online").read_text().strip())
        nodes = {0: physical_cores(all_cpus)}
    return nodes


# ═══════════════════════════════════════════════════════════════════
#                       配置覆盖
# ═══════════════════════════════════════════════════════════════════

core.OUTPUT_DIR = BENCHMARK_DIR / "data" / "cloud" / datetime.now().strftime("%Y%m%d_%H%M%S")

# 云服务器（8575C）每核 QPS 预估：云上单核频率 ~3.6GHz，约本机 P 核（5.4GHz）的 67%。
# 本机 est_per_core 57000/36000/24000 × 0.67 ≈ 38000/24000/16000。
# 估值略保守，确保探测下界（0.6×est）低于真实拐点；上界由 1.6×est + 二分保证覆盖。
core.LOADS = {
    "11B":     ("/",          38000),
    "16KB":    ("/data/16k",  24000),
    "64KB":    ("/data/64k",  16000),
    "compute": ("/compute?n=520000", 1000),   # 云上 8575C 校准：520000 次 ≈ 1ms（实测 500000→958us, 800000→1524us）
    "delay":   ("/delay?ms=1",       1000),   # 实测 1.07ms，准
}

# 场景：扩展性 sync-4/8/16/32 + async-16（同步 vs 异步对比）
core.SCENARIOS = [
    ("sync-4",   "sync",  4,  None),
    ("sync-8",   "sync",  8,  None),
    ("sync-16",  "sync",  16, None),
    ("sync-32",  "sync",  32, None),
    ("async-16", "async", 16, 16),
]

# 计划（对应方案 Phase E/F/G/H）
core.PLANS = {
    "scalability": {"scenes": ["sync-4", "sync-8", "sync-16", "sync-32"], "loads": ["64KB"],               "modes": ["keepalive"]},
    "baseline":    {"scenes": ["sync-16"],                              "loads": ["11B", "16KB", "64KB"], "modes": ["keepalive"]},
    "pipeline":    {"scenes": ["sync-16"],                              "loads": ["64KB"],               "modes": ["keepalive", "pipeline"]},
    "async":       {"scenes": ["sync-16", "async-16"],                  "loads": ["11B", "compute", "delay"], "modes": ["keepalive"]},
}

NODES = detect_topology()
# 服务端用 node0，客户端用 node1（双 NUMA 时）；单 NUMA 则物理核对半分
if len(NODES) >= 2:
    SERVER_POOL = NODES[0]
    WRK_POOL = NODES[1]
else:
    all_phys = NODES[0]
    half = len(all_phys) // 2
    SERVER_POOL = all_phys[:half]
    WRK_POOL = all_phys[half:]

SERVER_NUMA = 0
WRK_NUMA = 1 if len(NODES) >= 2 else 0


def scenario_info(name):
    for s in core.SCENARIOS:
        if s[0] == name:
            return s
    raise KeyError(name)


def alloc_cores(sub, worker):
    """服务端 = node0 物理核前 (1+sub+worker)；客户端 = node1 全部物理核。"""
    n_server = 1 + sub + (worker or 0)
    server = SERVER_POOL[:n_server]
    wrk = list(WRK_POOL)
    if not wrk:
        wrk = list(SERVER_POOL[n_server:])
    return server, wrk


def build_server_cmd(name, port, server_cores):
    _, _, sub, worker = scenario_info(name)
    cmd = ["numactl", f"--cpunodebind={SERVER_NUMA}", f"--membind={SERVER_NUMA}",
           "taskset", "-c", core.cores_str(server_cores),
           core.SERVER_BIN, str(port), str(sub)]
    if worker is not None:
        cmd.append(str(worker))
    return cmd


def run_wrk(url, c, lua_path, wrk_cores, dur, server_pid, rate, perf_events=None):
    """numactl 绑 node1 + taskset 绑核的 wrk 执行（其余逻辑与本机一致）。"""
    import psutil
    n_cores = len(wrk_cores)
    threads = core.WRK_THREADS or n_cores
    cmd = ["numactl", f"--cpunodebind={WRK_NUMA}", f"--membind={WRK_NUMA}",
           "taskset", "-c", core.cores_str(wrk_cores),
           core.WRK_BIN, f"-t{threads}", f"-c{c}", f"-d{dur}s"]
    if rate is not None:
        cmd += [f"-R{rate}"]
    cmd += ["--latency", "--timeout", "5s"]
    if lua_path:
        cmd += ["-s", str(lua_path)]
    cmd.append(url)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            preexec_fn=core.server_preexec)

    perf_proc = None
    perf_out = None
    if perf_events and server_pid:
        perf_out = core.OUTPUT_DIR / f"perf_tmp_{int(time.time()*1000)}.txt"
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
            perf = core.parse_perf(perf_out)
            if perf_out:
                perf_out.unlink(missing_ok=True)
        except Exception:
            pass

    r = core.parse_wrk2(out)
    if lua_path and "pipeline.lua" in str(lua_path):
        r["qps"] = r["qps"] * core.PIPELINE_DEPTH
    return r, wrk_cpu, out, perf


# 覆盖 bench_local 的差异函数
core.scenario_info = scenario_info
core.alloc_cores = alloc_cores
core.build_server_cmd = build_server_cmd
core.run_wrk = run_wrk


# ═══════════════════════════════════════════════════════════════════
#                       入口
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    core.log("════════ 云服务器核拓扑（自动探测）════════")
    for node_id in sorted(NODES):
        core.log(f"  node{node_id}: {len(NODES[node_id])} 个物理核 -> {NODES[node_id][:12]}...")
    core.log(f"  服务端池(node{SERVER_NUMA}): {len(SERVER_POOL)} 核")
    core.log(f"  客户端池(node{WRK_NUMA}): {len(WRK_POOL)} 核")
    core.log("════════════════════════════════════════")

    core.main()
