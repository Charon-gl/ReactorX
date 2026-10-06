#!/usr/bin/env python3
"""
bench_max.py —— 极限（饱和）吞吐测试（独立脚本）

与 bench_local / bench_cloud 的「可用吞吐」（拐点模式，P99 毫秒级）区分，
本脚本测「饱和吞吐」：wrk 用极大 -R 尽力打，延迟会崩（P99 秒级），
得到服务器/客户端的极限 QPS 天花板。

场景与负载与基准/扩展测试对齐：
  - 场景：复用 SCENARIOS（本机 sync-1/4/async-4，云上 sync-4/8/16/32/async-16）
  - 负载：默认只测 echo 三档（11B/16KB/64KB），与基准 baseline 一一对应、可对比；
    不测 compute/delay（打满意义不大，省时间）

本机与云上通用：import bench_cloud 自动探测各自 NUMA 拓扑。

用法：
    sudo python3 bench_max.py                     # 全部场景 × echo 三负载
    sudo python3 bench_max.py --scene sync-4      # 只测某场景
    sudo python3 bench_max.py --load 11B,64KB     # 指定负载（进一步省时间）
    sudo python3 bench_max.py --repeat 3          # 每点重复次数（取中位数）

注意：
  打满模式下客户端可能先饱和（尤其本机 E 核多、客户端核不足时），此时
  wrk_saturated=True，QPS 是「客户端瓶颈」而非「服务端极限」，脚本会打印警告。
  真正测服务端极限，需要在客户端核数 > 服务端核数的机器（如云上 node1 48 核）上跑。
"""

import sys
import time
import signal
import argparse
import csv
import subprocess
import atexit
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_cloud as cloud

core = cloud.core
BENCHMARK_DIR = cloud.BENCHMARK_DIR

MAX_RATE = 10_000_000   # 极大 -R（wrk2 必填 -R），让服务器尽力打，实际 QPS 收敛到极限

MAX_CSV_FIELDS = [
    "scenario", "model", "sub", "server_cores", "wrk_cores",
    "load", "path", "qps", "qps_per_core",
    "lat_p50", "lat_p99", "lat_p999",
    "wrk_cpu", "wrk_saturated",
    "err_connect", "err_read", "err_write", "err_timeout",
    "ipc", "cache_miss_rate",
]


def prepare():
    """测试前准备：清残留、提 FD、governor 切 performance（退出自动恢复）"""
    subprocess.run(["pkill", "-9", "-f", "benchmark/Reactor"], capture_output=True)
    subprocess.run(["pkill", "-9", "wrk"], capture_output=True)
    time.sleep(1)
    core.raise_nofile()
    gov_path = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    if gov_path.exists():
        try:
            cur = gov_path.read_text().strip()
        except Exception:
            cur = ""
        if cur and cur != "performance" and core.set_governor("performance"):
            core._OLD_GOVERNOR = cur
            atexit.register(core.restore_governor)
            core.log(f"governor 已切到 performance（原 {cur}）")


def run_max(name, load_names):
    """打满测极限 QPS：不起拐点扫描，直接 -R 极大值打满。"""
    _, model, sub, worker = core.scenario_info(name)
    port = core.PORT_BASE
    server_cores, wrk_cores = core.alloc_cores(sub, worker)
    core.log(f"\n{'='*70}\n  [极限] {name}  服务端核={len(server_cores)}  wrk核={len(wrk_cores)}\n{'='*70}")

    srv, srv_log = core.start_server_retry(name, port, server_cores, log_name=f"max_{name}")
    if srv is None:
        core.log(f"  ❌ {name} 启动失败")
        return []

    rows = []
    try:
        # 预热
        core.run_wrk(f"http://{core.HOST}:{port}/data/16k", core.WRK_CONC, None, wrk_cores, 8, srv.pid, 50000)
        time.sleep(2)

        for lname in load_names:
            url_path, _ = core.LOADS[lname]
            url = f"http://{core.HOST}:{port}{url_path}"
            core.log(f"  [{name}] {lname} 打满 -R{MAX_RATE}，{core.DURATION}s × {core.REPEAT} ...")
            reps = []
            for rep in range(core.REPEAT):
                r, wrk_cpu, out, perf = core.run_wrk(url, core.WRK_CONC, None, wrk_cores,
                                                     core.DURATION, srv.pid, MAX_RATE, core.PERF_CORE_EVENTS)
                if r["qps"] <= 0:
                    core.log(f"    rep{rep+1}: wrk 失败")
                    continue
                reps.append((r, wrk_cpu, out, perf))
                core.log(f"    rep{rep+1}: QPS={r['qps']:,.1f}  P99={r['lat'].get(99.0)}  wrk_cpu={wrk_cpu:.0f}%")
                time.sleep(1)
            if not reps:
                continue
            reps.sort(key=lambda x: x[0]["qps"])
            r, wrk_cpu, out, perf = reps[len(reps) // 2]
            lat = r["lat"]
            saturated = wrk_cpu > len(wrk_cores) * 95.0
            if saturated:
                core.log(f"  ⚠️ [{name}] {lname} 客户端饱和（wrk_cpu={wrk_cpu:.0f}%），"
                         f"此 QPS 是客户端瓶颈，非服务端极限")
            rows.append({
                "scenario": name, "model": model, "sub": sub,
                "server_cores": len(server_cores), "wrk_cores": len(wrk_cores),
                "load": lname, "path": url_path,
                "qps": r["qps"],
                "qps_per_core": round(r["qps"] / len(server_cores), 1),
                "lat_p50": lat.get(50.0), "lat_p99": lat.get(99.0),
                "lat_p999": lat.get(99.9),
                "wrk_cpu": wrk_cpu, "wrk_saturated": saturated,
                "err_connect": r["err_connect"], "err_read": r["err_read"],
                "err_write": r["err_write"], "err_timeout": r["err_timeout"],
                "ipc": perf.get("ipc"), "cache_miss_rate": perf.get("cache_miss_rate"),
            })
            (core.OUTPUT_DIR / f"wrk_max_{name}_{lname}.log").write_text(out)
    finally:
        srv.send_signal(signal.SIGTERM)
        try:
            srv.wait(timeout=5)
        except Exception:
            srv.kill()
        srv_log.close()
        time.sleep(1)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scene", type=str, default=None, help="只测某场景（默认全部）")
    parser.add_argument("--load", type=str, default="11B,16KB,64KB", help="逗号分隔的负载名")
    parser.add_argument("--repeat", type=int, default=core.REPEAT)
    args = parser.parse_args()

    core.REPEAT = args.repeat
    core.OUTPUT_DIR = BENCHMARK_DIR / "data" / "max" / datetime.now().strftime("%Y%m%d_%H%M%S")
    core.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    core.log(f"输出目录: {core.OUTPUT_DIR}")

    prepare()

    scenes = [args.scene] if args.scene else [s[0] for s in core.SCENARIOS]
    loads = [x.strip() for x in args.load.split(",")]
    core.log(f"极限测试：场景 {scenes}，负载 {loads}")

    all_rows = []
    for name in scenes:
        all_rows.extend(run_max(name, loads))

    csv_path = core.OUTPUT_DIR / "max_results.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MAX_CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)

    core.log(f"\n极限结果: {csv_path}")
    for r in all_rows:
        core.log(f"  {r['scenario']:<8} {r['load']:<6} QPS={r['qps']:>12,.0f}  每核={r['qps_per_core']:>8}  "
                 f"P99={r['lat_p99']}  wrk_cpu={r['wrk_cpu']:.0f}%  {'⚠客户端瓶颈' if r['wrk_saturated'] else ''}")


if __name__ == "__main__":
    main()
