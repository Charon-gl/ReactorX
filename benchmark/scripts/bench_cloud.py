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

import argparse
import importlib.util
import shutil
import sys
import tempfile
import os
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
    allowed_cpus = os.sched_getaffinity(0)
    nodes = {}
    node_dir = Path("/sys/devices/system/node")
    for d in sorted(node_dir.glob("node*")):
        try:
            node_id = int(d.name.replace("node", ""))
            cpulist = (d / "cpulist").read_text().strip()
            available = [cpu for cpu in parse_cpulist(cpulist) if cpu in allowed_cpus]
            nodes[node_id] = physical_cores(available)
        except Exception:
            continue
    if not nodes:
        # 回退：所有 CPU 去超线程
        all_cpus = [cpu for cpu in parse_cpulist(
            Path("/sys/devices/system/cpu/online").read_text().strip()
        ) if cpu in allowed_cpus]
        nodes = {0: physical_cores(all_cpus)}
    return {node: cpus for node, cpus in nodes.items() if cpus}


# ═══════════════════════════════════════════════════════════════════
#                       配置覆盖
# ═══════════════════════════════════════════════════════════════════

core.OUTPUT_DIR = BENCHMARK_DIR / "data" / "cloud" / datetime.now().strftime("%Y%m%d_%H%M%S")

# 云端 compute 基准工作量须由 calibrate.py 校准到约 1ms，再派生三档负载。
COMPUTE_ITERATIONS = os.environ.get("REACTORX_COMPUTE_N", "520000")
if not COMPUTE_ITERATIONS.isdigit() or int(COMPUTE_ITERATIONS) <= 0:
    raise SystemExit("REACTORX_COMPUTE_N 必须为正整数")

# 每核 QPS 仅用于拐点搜索的初始估计，不是预设性能结论。
# 本机 est_per_core 57000/36000/24000 × 0.67 ≈ 38000/24000/16000。
# 估值略保守，确保探测下界（0.6×est）低于真实拐点；上界由 1.6×est + 二分保证覆盖。
core.LOADS = {
    "11B":     ("/",          38000),
    "16KB":    ("/data/16k",  24000),
    "64KB":    ("/data/64k",  16000),
    "compute-light":  (f"/compute?n={max(1, int(COMPUTE_ITERATIONS) // 4)}", 4000),
    "compute-medium": (f"/compute?n={COMPUTE_ITERATIONS}",                 1000),
    "compute-heavy":  (f"/compute?n={int(COMPUTE_ITERATIONS) * 4}",          250),
    "delay-10ms":     ("/delay?ms=10",  100),
    "delay-50ms":     ("/delay?ms=50",   20),
    "delay-100ms":    ("/delay?ms=100", 10),
}

# 场景：sync-4/8/16/32 + async-16（64 物理核 / 4 NUMA × 16 → 服务端 node0+1=32 核）
# async-16: 16 sub-reactor + 8 业务 worker = 25 核
core.SCENARIOS = [
    ("sync-4",   "sync",  4,  None),
    ("sync-8",   "sync",  8,  None),
    ("sync-16",  "sync",  16, None),
    ("sync-32",  "sync",  32, None),
    ("async-16", "async", 16, 8),
]

# 计划（对应方案 Phase E/F/G/H）
core.PLANS = {
    "scalability": {"scenes": ["sync-4", "sync-8", "sync-16", "sync-32"], "loads": ["11B", "64KB"],       "modes": ["keepalive"]},
    "baseline":    {"scenes": ["sync-16"],                              "loads": ["11B", "16KB", "64KB"], "modes": ["keepalive"]},
    "pipeline":    {"scenes": ["sync-16"],                              "loads": ["64KB"],               "modes": ["keepalive", "pipeline"]},
    "async":       {"scenes": ["sync-16", "async-16"],                  "loads": [
        "compute-light", "compute-medium", "compute-heavy",
        "delay-10ms", "delay-100ms",
    ], "modes": ["keepalive"]},
}

# sync-16 与 async-16 使用相同服务端核预算。
# 速率 = 参考线程数(16) × 每核容量 / 相对耗时 × 系数(0.25, 0.5, 1.25)
core.ASYNC_COMPARISON_RATES = {
    "11B":             [1000000, 1750000],
    "compute-light":   [32000,   80000],
    "compute-medium":  [8000,    20000],
    "compute-heavy":   [2000,    5000],
    "delay-10ms":      [800,     2000],
    "delay-100ms":     [80,      200],
}
core.ASYNC_COMPARISON_CONCURRENCY = {"11B": core.WRK_CONC}
core.ASYNC_COMPARISON_CONCURRENCY.update({
    name: 100 for name in core.ASYNC_COMPARISON_RATES if name != "11B"
})

NODES = detect_topology()
if not NODES:
    raise SystemExit("未检测到当前进程可用的 CPU，请检查容器 cpuset/CPU affinity")
# 4 NUMA：服务端 node0+1 / 客户端 node2+3；2 NUMA：node0/node1 分角色；单 NUMA：物理核对半分
if len(NODES) >= 4:
    sorted_ids = sorted(NODES)
    SERVER_NUMA = ",".join(str(n) for n in sorted_ids[:2])
    WRK_NUMA = ",".join(str(n) for n in sorted_ids[2:4])
    SERVER_POOL = [c for nid in sorted_ids[:2] for c in NODES[nid]]
    WRK_POOL = [c for nid in sorted_ids[2:4] for c in NODES[nid]]
elif len(NODES) >= 2:
    sorted_ids = sorted(NODES)
    SERVER_NUMA = str(sorted_ids[0])
    WRK_NUMA = str(sorted_ids[1])
    SERVER_POOL = NODES[sorted_ids[0]]
    WRK_POOL = NODES[sorted_ids[1]]
else:
    root_id = next(iter(NODES))
    SERVER_NUMA = WRK_NUMA = str(root_id)
    all_phys = NODES[root_id]
    half = len(all_phys) // 2
    SERVER_POOL = all_phys[:half]
    WRK_POOL = all_phys[half:]


def validate_topology(scenario_names):
    errors = []
    for name in scenario_names:
        try:
            _, _, sub, worker = scenario_info(name)
        except KeyError:
            errors.append(f"unknown scenario: {name}")
            continue
        needed = 1 + sub + (worker or 0)
        if needed > len(SERVER_POOL):
            errors.append(f"{name} needs {needed} server CPUs; pool has {len(SERVER_POOL)}")
    if len(WRK_POOL) < 4:
        errors.append(f"wrk needs at least 4 physical CPUs for this plan; pool has {len(WRK_POOL)}")
    if errors:
        raise SystemExit("云端 CPU 拓扑不满足测试场景:\n  " + "\n  ".join(errors))


def requested_scenarios(argv):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--plan", choices=list(core.PLANS), default="baseline")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--scene")
    parser.add_argument("--conn")
    parser.add_argument("--flame", action="store_true")
    parser.add_argument("--flame-scene", default="sync-4")
    parser.add_argument("--preflight", action="store_true")
    args, _ = parser.parse_known_args(argv)

    if args.conn:
        plan_names = []
        names = [args.conn]
    else:
        plan_names = ([name for name in core.PLANS if name != "async"]
                  if args.all else [args.plan])
        names = []
        for plan_name in plan_names:
            plan = core.PLANS[plan_name]
            names.extend([args.scene] if args.scene and not args.all else plan["scenes"])
    if args.flame:
        names.append(args.flame_scene)
    return args, plan_names, list(dict.fromkeys(names))


def validate_cloud_environment(args, plan_names):
    if os.geteuid() != 0:
        raise SystemExit("云测请用 sudo 运行，确保 perf、CPU 绑核和 FD 配置权限正确")

    required_commands = ["numactl", "taskset"]
    if args.flame:
        required_commands.append("perf")
    missing = [name for name in required_commands if shutil.which(name) is None]
    if missing:
        raise SystemExit("缺少云测命令: " + ", ".join(missing))
    if not core.SERVER_BIN or not Path(core.SERVER_BIN).is_file():
        raise SystemExit(f"未找到服务端二进制: {core.SERVER_BIN}；请先构建 Release 版本")

    cache_file = Path(core.PROJECT_DIR) / "build" / "CMakeCache.txt"
    if not cache_file.is_file() or "CMAKE_BUILD_TYPE:STRING=Release" not in cache_file.read_text(errors="replace"):
        raise SystemExit("build/CMakeCache.txt 不是 Release 构建；请在云测前重新配置并编译 Release")

    if importlib.util.find_spec("psutil") is None:
        raise SystemExit("云测 Python 环境缺少 psutil；请先安装后再开测")

    core.validate_wrk2()
    core.validate_port_available()

    if "async" in plan_names and not args.preflight and "REACTORX_COMPUTE_N" not in os.environ:
        raise SystemExit("async 计划需要先校准 compute；请显式设置 REACTORX_COMPUTE_N")

    data_dir = Path(BENCHMARK_DIR) / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    free_bytes = shutil.disk_usage(data_dir).free
    if free_bytes < 1 << 30:
        raise SystemExit(f"数据盘剩余空间不足 1 GiB: {free_bytes / (1 << 20):.0f} MiB")

    if args.flame:
        perf_probe = subprocess.run(
            ["perf", "stat", "-e", core.PERF_CORE_EVENTS, "--", "true"],
            capture_output=True, text=True, timeout=10,
        )
        if perf_probe.returncode != 0:
            raise SystemExit("perf 硬件计数器不可用；火焰图测试要求 perf 可用")

    if args.flame:
        stack, flame = core.find_flamegraph_tools()
        if stack is None or flame is None:
            raise SystemExit("请求了 --flame，但未找到 FlameGraph 工具；请先安装 stackcollapse-perf.pl 和 flamegraph.pl")
    if args.flame:
        with tempfile.TemporaryDirectory(prefix="reactorx-perf-preflight-") as temp_dir:
            probe_data = Path(temp_dir) / "perf.data"
            record_probe = subprocess.run(
                ["perf", "record", "-g", "-o", str(probe_data), "--", "true"],
                capture_output=True, text=True, timeout=10,
            )
            if record_probe.returncode != 0 or not probe_data.is_file():
                raise SystemExit("perf record -g 不可用；请先修复 call-graph 采样权限/内核支持")


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


def alloc_comparison_cores(sub, worker):
    """sync/async 对照共享 async 所需的服务端核与同一客户端核池。"""
    worker_budget = max((scene[3] or 0 for scene in core.SCENARIOS if scene[2] == sub), default=worker or 0)
    server = SERVER_POOL[:1 + sub + worker_budget]
    wrk = list(WRK_POOL)
    if not wrk:
        wrk = list(SERVER_POOL[1 + sub + worker_budget:])
    return server, wrk


def build_server_cmd(name, port, server_cores):
    _, _, sub, worker = scenario_info(name)
    cmd = ["numactl", f"--cpunodebind={SERVER_NUMA}", f"--membind={SERVER_NUMA}",
           "taskset", "-c", core.cores_str(server_cores),
           core.SERVER_BIN, str(port), str(sub)]
    if worker is not None:
        cmd.append(str(worker))
    return cmd


def run_wrk(url, c, lua_path, wrk_cores, dur, server_pid, rate, perf_events=None, timeout="5s"):
    """numactl 绑 node1 + taskset 绑核的 wrk 执行（其余逻辑与本机一致）。"""
    import psutil
    n_cores = len(wrk_cores)
    threads = core.WRK_THREADS or n_cores
    cmd = ["numactl", f"--cpunodebind={WRK_NUMA}", f"--membind={WRK_NUMA}",
           "taskset", "-c", core.cores_str(wrk_cores),
           core.WRK_BIN, f"-t{threads}", f"-c{c}", f"-d{dur}s"]
    if rate is not None:
        cmd += [f"-R{rate}"]
    cmd += ["--latency", "--timeout", timeout]
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
    # wrk2 counts each completed HTTP response, including pipelined responses.
    return r, wrk_cpu, out, perf


# 覆盖 bench_local 的差异函数
core.scenario_info = scenario_info
core.alloc_cores = alloc_cores
core.alloc_comparison_cores = alloc_comparison_cores
core.build_server_cmd = build_server_cmd
core.run_wrk = run_wrk


# ═══════════════════════════════════════════════════════════════════
#                       入口
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if "-h" in sys.argv or "--help" in sys.argv:
        core.main()

    cloud_args, selected_plans, selected_scenarios = requested_scenarios(sys.argv[1:])
    validate_topology(selected_scenarios)
    validate_cloud_environment(cloud_args, selected_plans)
    core.log("════════ 云服务器核拓扑（自动探测）════════")
    for node_id in sorted(NODES):
        core.log(f"  node{node_id}: {len(NODES[node_id])} 个物理核 -> {NODES[node_id][:12]}...")
    core.log(f"  服务端池(node{SERVER_NUMA}): {len(SERVER_POOL)} 核")
    core.log(f"  客户端池(node{WRK_NUMA}): {len(WRK_POOL)} 核")
    core.log("════════════════════════════════════════")

    if cloud_args.preflight:
        core.log("云端 preflight 通过：拓扑、Release 构建、wrk2、依赖、perf、磁盘与端口均可用；未启动压测。")
        raise SystemExit(0)

    core.main()