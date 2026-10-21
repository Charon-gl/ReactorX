#!/usr/bin/env python3
"""
bench_isolation.py —— ReactorX sync vs async 隔离性对比实验

两个独立的混合负载实验：
  Experiment 1  compute_mix   90% /fast(sleep 1ms) + 10% /compute(CPU busy)
  Experiment 2  delay_mix     90% /fast(sleep 1ms) + 10% /slow(sleep 300ms)

每个实验：sync(16 subreactor) vs async(16 sub + 8 worker)
每配置：30s 预热 + 120s 正式，3 次重复，取中位数
通过 bench_cloud.py 的 numactl/taskset 完成服务端和客户端的 CPU 绑核

用法（云端，sudo）：
    sudo python3 benchmark/scripts/bench_isolation.py

    # 只测一个实验
    sudo python3 benchmark/scripts/bench_isolation.py --experiment compute_mix
    sudo python3 benchmark/scripts/bench_isolation.py --experiment delay_mix

    # 只测一个速率等级
    sudo python3 benchmark/scripts/bench_isolation.py --rate low

    # 自定义参数
    sudo REACTORX_COMPUTE_ITERS=3000000 REACTORX_DELAY_MS=300 \
         python3 benchmark/scripts/bench_isolation.py
"""

import sys
import os
import subprocess
import time
import signal
import argparse
import statistics
import shutil
import json
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
BENCHMARK_DIR = SCRIPT_DIR.parent
PROJECT_DIR = BENCHMARK_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

import bench_local as base
import bench_cloud   # 导入后 base.run_wrk 等函数被覆盖为 cloud 的 numactl 版本


# ═══════════════════════════════════════════════════════════════════
#                        实验参数
# ═══════════════════════════════════════════════════════════════════

# 两个混合负载实验定义
# (实验名, lua 脚本, 短任务占比, 阻塞任务占比, 阻塞任务描述)
EXPERIMENTS = {
    "compute_mix": {
        "lua": SCRIPT_DIR / "isolation_compute.lua",
        "desc": "90% /fast(sleep 1ms) + 10% /compute(CPU busy)",
    },
    "delay_mix": {
        "lua": SCRIPT_DIR / "isolation_delay.lua",
        "desc": "90% /fast(sleep 1ms) + 10% /slow(sleep 300ms)",
    },
}

# 服务端配置（两种模式对比）
SERVER_CONFIGS = [
    # (label, subreactors, workers=None 表示 sync)
    ("sync_subreactor", 8, None),
    ("async_worker",    8, 8),
]

# 压测速率等级：(label, target_R, 说明)
# 低速率：sync 勉强能处理，async 轻松
# 高速率：sync 会先崩（超时/掉速），async 仍稳定
RATE_LEVELS = {
    "compute_mix": {
        "low":  (2000, "sync 可勉强处理"),
        "high": (5000, "sync 预期出现超时/掉速"),
    },
    "delay_mix": {
        "low":  (1000, "sync 可勉强处理"),
        "high": (3000, "sync 预期出现超时/掉速"),
    },
}

WARMUP_DURATION = 30     # 预热秒数
TEST_DURATION   = 90     # 正式测试秒数
REPETITIONS     = 3      # 每配置重复次数，取中位数
WRK_CONC        = 2000   # wrk 连接数
WRK_TIMEOUT     = "5s"   # wrk 超时
# bench_cloud.run_wrk 内部 threads = core.WRK_THREADS or n_cores
# 0 = 自动等于 wrk 绑核数（根据拓扑动态决定，避免客户端先饱和）
base.WRK_THREADS = 0
PORT            = 6666

# 环境变量默认值
COMPUTE_ITERS_DEFAULT = os.environ.get("REACTORX_COMPUTE_ITERS", "3000000")
DELAY_MS_DEFAULT      = os.environ.get("REACTORX_DELAY_MS", "300")

OUTPUT_DIR = BENCHMARK_DIR / "data" / "isolation" / datetime.now().strftime("%Y%m%d_%H%M%S")


# ═══════════════════════════════════════════════════════════════════
#                        工具函数
# ═══════════════════════════════════════════════════════════════════

def log(msg=""):
    """带时间戳的日志输出（同时输出到 stdout 和文件）。"""
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    if log._file:
        log._file.write(line + "\n")
        log._file.flush()
log._file = None


def median_of(values, key=None):
    """从记录列表中提取 key 字段，返回中位数。"""
    if key:
        vals = [key(v) for v in values]
    else:
        vals = list(values)
    if not vals:
        return None
    return statistics.median(vals)


def curl_check_xec(port, expected_type):
    """用 curl 抽查 X-Exec-Thread 响应头，验证业务执行线程类型。"""
    try:
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-D", "-",
             f"http://127.0.0.1:{port}/fast"],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.splitlines():
            if line.lower().startswith("x-exec-thread:"):
                actual = line.split(":", 1)[1].strip()
                return actual == expected_type, actual
        return False, "header not found"
    except Exception as exc:
        return False, str(exc)


def test_server_reachable(port, max_wait=10):
    """等待服务器端口可连接。"""
    import socket
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    return False


# ═══════════════════════════════════════════════════════════════════
#                        核心执行逻辑
# ═══════════════════════════════════════════════════════════════════

def _build_row(experiment_name, mode_name, srv_label, sub, worker,
                rate_label, rate_value, rep, result, wrk_cpu,
                wrk_cores_n, wrk_threads_n, xec_ok):
    """从单次 wrk 结果构造结果行。"""
    lat = result["lat"]
    errors_count = (result["err_connect"] + result["err_read"]
                    + result["err_write"] + result["err_timeout"])
    qps_ratio = round(result["qps"] / rate_value * 100, 1) if rate_value else 0

    return {
        "experiment": experiment_name,
        "mode": mode_name,
        "label": srv_label,
        "sub": sub,
        "worker": worker or 0,
        "rate_label": rate_label,
        "target_rate": rate_value,
        "repetition": rep,
        "qps": round(result["qps"], 1),
        "qps_ratio_pct": qps_ratio,
        "lat_p50_us": lat.get(50.0),
        "lat_p90_us": lat.get(90.0),
        "lat_p99_us": lat.get(99.0),
        "lat_p999_us": lat.get(99.9),
        "latency_samples": result["latency_samples"],
        "errors_total": errors_count,
        "err_connect": result["err_connect"],
        "err_read": result["err_read"],
        "err_write": result["err_write"],
        "err_timeout": result["err_timeout"],
        "err_status": result["err_status"],
        "wrk_threads": wrk_threads_n,
        "wrk_cores": wrk_cores_n,
        "wrk_cpu_pct": round(wrk_cpu, 1),
        "wrk_saturated": wrk_cpu > wrk_cores_n * 95.0,
        "xec_verified": xec_ok,
    }


def _log_rep(rep, result, rate_value, wrk_cpu, wrk_cores_n, wrk_threads_n,
             xec_ok, errors_count, sat_warn):
    """打印单次重复的日志行。"""
    lat = result["lat"]
    qps_ratio = round(result["qps"] / rate_value * 100, 1) if rate_value else 0
    p50_v, p90_v = lat.get(50.0), lat.get(90.0)
    p99_v, p999_v = lat.get(99.0), lat.get(99.9)
    if p50_v is None or p90_v is None or p99_v is None or p999_v is None:
        log(f"    ⚠ 延迟分位数不完整 lat_keys={sorted(lat.keys())}，lat={lat}")
    lat_str = (
        (f"p50={p50_v:.0f}us " if p50_v is not None else "p50=N/A ")
        + (f"p90={p90_v:.0f}us " if p90_v is not None else "p90=N/A ")
        + (f"p99={p99_v:.0f}us " if p99_v is not None else "p99=N/A ")
        + (f"p99.9={p999_v:.0f}us" if p999_v is not None else "p99.9=N/A")
    )
    log(f"    rep{rep}: QPS={result['qps']:.0f} ({qps_ratio}% of target)  "
        f"{lat_str}  errors={errors_count}  "
        f"wrk_cpu={wrk_cpu:.0f}%(on{wrk_cores_n}c/{wrk_threads_n}t){sat_warn}  "
        f"xec={'✓' if xec_ok else '✗'}")


def run_experiment(experiment_name, rate_label):
    """运行一个实验：启动服务端一次 → 预热一次 → 连跑 REP 次 → 停止服务端。"""
    exp = EXPERIMENTS[experiment_name]
    lua_path = exp["lua"]

    rate_config = RATE_LEVELS[experiment_name]
    if rate_label not in rate_config:
        log(f"  ✗ 未知速率等级: {rate_label}，支持: {list(rate_config)}")
        return []

    rate_value, rate_desc = rate_config[rate_label]

    log(f"\n{'='*72}")
    log(f"  实验: {experiment_name}  |  {exp['desc']}")
    log(f"  速率: -R{rate_value} ({rate_label}) — {rate_desc}")
    log(f"  compute_iters={COMPUTE_ITERS_DEFAULT}  delay_ms={DELAY_MS_DEFAULT}")
    log(f"  {REPETITIONS} 次重复（仅 {WARMUP_DURATION}s 预热一次） × {TEST_DURATION}s正式")
    log(f"{'='*72}")

    all_rows = []

    for srv_label, sub, worker in SERVER_CONFIGS:
        mode_name = "sync" if worker is None else "async"
        log(f"\n  --- {srv_label}: sub={sub} worker={worker or 0} ---")

        # ── 启动服务端（一次） ──
        core_allocator = getattr(base, "alloc_comparison_cores", base.alloc_cores)
        server_cores, wrk_cores = core_allocator(sub, worker)

        cmd = ["numactl", f"--cpunodebind={bench_cloud.SERVER_NUMA}",
               f"--membind={bench_cloud.SERVER_NUMA}",
               "taskset", "-c", base.cores_str(server_cores),
               base.SERVER_BIN, str(PORT), str(sub)]
        if worker is not None:
            cmd.append(str(worker))

        log_name = f"{experiment_name}_{srv_label}"
        srv_log_path = OUTPUT_DIR / f"server_{log_name}.log"
        srv_log = open(srv_log_path, "a")
        srv = subprocess.Popen(cmd, stdout=srv_log, stderr=subprocess.STDOUT,
                               preexec_fn=base.server_preexec)

        if not test_server_reachable(PORT, max_wait=15):
            log(f"  ✗ 服务器启动超时，跳过 {srv_label}")
            srv.terminate(); srv.wait(); srv_log.close()
            continue

        # ── X-Exec-Thread 验证（一次） ──
        expected = "worker" if worker is not None else "subreactor"
        xec_ok, xec_actual = curl_check_xec(PORT, expected)
        log(f"  X-Exec-Thread 验证: expected={expected}, actual={xec_actual}, ok={xec_ok}")

        wrk_cores_n = len(wrk_cores)
        wrk_threads_n = len(wrk_cores)  # wrk 线程数 = 客户端绑核数

        reps_results = []
        try:
            # ── 预热（仅一次） ──
            warm_url = f"http://127.0.0.1:{PORT}/fast"
            log(f"  预热 {WARMUP_DURATION}s（不采集）...")
            base.run_wrk(warm_url, WRK_CONC, None, wrk_cores,
                         WARMUP_DURATION, srv.pid, rate_value)
            time.sleep(2)

            # ── 正式测试（连续 REP 次，不重启不预热） ──
            test_url = f"http://127.0.0.1:{PORT}"
            for rep in range(1, REPETITIONS + 1):
                log(f"  正式测试 {TEST_DURATION}s -R{rate_value} rep{rep}...")
                result, wrk_cpu, raw_output, _perf = base.run_wrk(
                    test_url, WRK_CONC, str(lua_path), wrk_cores,
                    TEST_DURATION, srv.pid, rate_value, timeout=WRK_TIMEOUT,
                )
                out_path = OUTPUT_DIR / f"wrk_{log_name}_R{rate_value}_rep{rep}.log"
                out_path.write_text(raw_output)

                errors_count = (result["err_connect"] + result["err_read"]
                                + result["err_write"] + result["err_timeout"])
                sat_warn = " ⚠️CLIENT_SATURATED" if wrk_cpu > wrk_cores_n * 95.0 else ""

                row = _build_row(experiment_name, mode_name, srv_label, sub, worker,
                                 rate_label, rate_value, rep, result, wrk_cpu,
                                 wrk_cores_n, wrk_threads_n, xec_ok)
                reps_results.append(row)
                _log_rep(rep, result, rate_value, wrk_cpu, wrk_cores_n,
                         wrk_threads_n, xec_ok, errors_count, sat_warn)
        finally:
            srv.send_signal(signal.SIGTERM)
            try:
                srv.wait(timeout=10)
            except subprocess.TimeoutExpired:
                srv.kill(); srv.wait()
            srv_log.close()
            time.sleep(1)

        if len(reps_results) < REPETITIONS:
            log(f"    警告：只有 {len(reps_results)}/{REPETITIONS} 次成功")

        if reps_results:
            median_row = dict(reps_results[0])
            for field in ["qps", "qps_ratio_pct", "lat_p50_us", "lat_p90_us",
                           "lat_p99_us", "lat_p999_us", "errors_total"]:
                median_row[field] = median_of(reps_results, key=lambda r: r.get(field))
            median_row["repetition"] = "median"
            median_row["latency_samples"] = (
                reps_results[1]["latency_samples"] if len(reps_results) > 1
                else reps_results[0]["latency_samples"]
            )
            all_rows.extend(reps_results)
            all_rows.append(median_row)

            log(f"    → 中位数: QPS={median_row['qps']:.0f} "
                f"p50={median_row['lat_p50_us']:.0f}us "
                f"p99={median_row['lat_p99_us']:.0f}us "
                f"p99.9={median_row['lat_p999_us']:.0f}us "
                f"errors={median_row['errors_total']}")

    return all_rows


def print_summary_table(all_rows):
    """打印 Markdown 格式的汇总表格。"""
    log(f"\n{'='*72}")
    log("                    实验结果汇总")
    log(f"{'='*72}")

    for exp_name in EXPERIMENTS:
        exp_rows = [r for r in all_rows if r["experiment"] == exp_name]
        if not exp_rows:
            continue

        log(f"\n### 实验: {exp_name} — {EXPERIMENTS[exp_name]['desc']}\n")

        for rate_label in RATE_LEVELS[exp_name]:
            rate_rows = [r for r in exp_rows if r["rate_label"] == rate_label
                         and r["repetition"] == "median"]
            if not rate_rows:
                continue

            rate_value = RATE_LEVELS[exp_name][rate_label][0]
            log(f"**-R{rate_value} ({rate_label})**\n")
            log(f"| 模式 | QPS (实际) | vs 目标 | p50 | p90 | p99 | p99.9 | 错误 | wrk线程 | wrk核 | wrk CPU% | X-Exec |")
            log(f"|------|-----------|---------|-----|-----|-----|-------|------|---------|-------|----------|--------|")

            for row in sorted(rate_rows, key=lambda r: r["mode"]):
                qps = row["qps"]
                ratio = row["qps_ratio_pct"]
                log(f"| {row['label']} | {qps:.0f} | {ratio:.1f}% | "
                    f"{row['lat_p50_us']:.0f}μs | {row['lat_p90_us']:.0f}μs | "
                    f"{row['lat_p99_us']:.0f}μs | {row['lat_p999_us']:.0f}μs | "
                    f"{int(row['errors_total'])} | "
                    f"{row.get('wrk_threads', '-')} | {row.get('wrk_cores', '-')} | "
                    f"{row.get('wrk_cpu_pct', '-')}% | "
                    f"{'✓' if row['xec_verified'] else '✗'} |")

            log("")


def main():
    parser = argparse.ArgumentParser(description="ReactorX sync vs async 隔离性对比实验")
    parser.add_argument("--experiment", choices=list(EXPERIMENTS),
                        help="只跑指定实验（不指定则两个都跑）")
    parser.add_argument("--rate", choices=["low", "high"],
                        help="只跑指定速率等级（不指定则两个都跑）")
    args = parser.parse_args()

    # 环境检查
    if os.geteuid() != 0:
        raise SystemExit("请用 sudo 运行，确保 CPU 绑核和 FD 配置权限正确")
    if not Path(base.SERVER_BIN).is_file():
        raise SystemExit(f"未找到服务端二进制: {base.SERVER_BIN}")
    base.validate_wrk2()
    base.validate_port_available()

    # FD 上限 & CPU governor
    soft, hard = base.raise_nofile()
    log(f"FD 上限: {hard}")
    gov_path = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    if gov_path.exists():
        cur = gov_path.read_text().strip()
        if cur != "performance" and base.set_governor("performance"):
            log(f"governor 已切 performance（原 {cur}）")
            import atexit
            atexit.register(base.restore_governor)

    # 创建输出目录
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log._file = open(OUTPUT_DIR / "run.log", "w")

    log("═══════════════════════════════════════════════════════════════")
    log("  ReactorX sync vs async 隔离性对比实验")
    log(f"  输出目录: {OUTPUT_DIR}")
    log(f"  服务端二进制: {base.SERVER_BIN}")
    log(f"  服务端核池({bench_cloud.SERVER_NUMA}): {len(bench_cloud.SERVER_POOL)} 核 "
        f"→ {bench_cloud.SERVER_POOL[:16]}...")
    log(f"  客户端核池({bench_cloud.WRK_NUMA}): {len(bench_cloud.WRK_POOL)} 核 "
        f"→ {bench_cloud.WRK_POOL[:16]}...")
    log(f"  compute_iters={COMPUTE_ITERS_DEFAULT}  delay_ms={DELAY_MS_DEFAULT}")
    log(f"  连接数={WRK_CONC}  wrk线程数=自动(等于客户端绑核数)  超时={WRK_TIMEOUT}")
    log(f"  预热={WARMUP_DURATION}s  正式={TEST_DURATION}s  重复={REPETITIONS}次")
    log("═══════════════════════════════════════════════════════════════")

    # 选实验
    experiments_to_run = [args.experiment] if args.experiment else list(EXPERIMENTS)
    rate_labels = [args.rate] if args.rate else ["low", "high"]

    all_rows = []
    for exp_name in experiments_to_run:
        for rate_label in rate_labels:
            rows = run_experiment(exp_name, rate_label)
            all_rows.extend(rows)

    # 保存 JSON 汇总
    if all_rows:
        json_path = OUTPUT_DIR / "results.json"
        json_path.write_text(json.dumps(all_rows, indent=2, default=str))
        log(f"\n结果已保存: {json_path}")

    # 打印汇总表
    print_summary_table(all_rows)

    log("\n═══════════════════════════════════════════════════════════════")
    log(f"  实验完成！所有数据在: {OUTPUT_DIR}")
    log("═══════════════════════════════════════════════════════════════")
    log._file.close()


if __name__ == "__main__":
    main()