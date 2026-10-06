#!/usr/bin/env python3
"""
bench_remote.py —— 内网双机测试（方案 Phase L）

两种用法：

【推荐：全自动，客户端一条命令】前提：客户端 → 服务端已配 SSH 免密，
且服务端机器上已部署本脚本（scp 到 ~/benchmark/scripts/ 或 --remote-script 指定）。

    客户端（机器 B）一条命令：
        sudo python3 bench_remote.py --role client --host <A内网IP> --ssh <user@A内网IP> --scenes sync-16,async-16

    脚本会自动：SSH 起服务端 → 压测 → SSH 停服务端 → scp 拉回服务端指标，循环每个场景。

【手动模式】两端分别操作（不用 SSH）：

    机器 A：sudo python3 bench_remote.py --role server --scenes sync-16,async-16
    机器 B：sudo python3 bench_remote.py --role client --host <A内网IP>

指标采集：
  服务端 server_metrics.csv（CPU% / RSS / FD 时间序列）
  客户端 remote_results.csv（QPS / P50 / P99 / P999 / 错误 / wrk_cpu）
"""

import sys
import time
import signal
import argparse
import socket
import csv
import threading
import subprocess
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_cloud as cloud

core = cloud.core
BENCHMARK_DIR = cloud.BENCHMARK_DIR
SERVER_METRICS_DIR = "/tmp/reactorx_remote_server"


def print_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def wait_port(host, port, timeout=20):
    """等 host:port 就绪（用于 SSH 起服务器后确认已监听）"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=1)
            s.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False


# SSH 通用选项：自动接受 host key（首次连不卡确认）、5s 连接超时
SSH_OPTS = ["-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=5"]


def check_ssh(ssh_target, use_sudo=False):
    """前置检查：SSH 是否可免密登录（不通直接报错，别等压测时才发现）"""
    sudo = "sudo " if use_sudo else ""
    try:
        r = subprocess.run(["ssh", *SSH_OPTS, ssh_target, f"{sudo}python3 --version"],
                           capture_output=True, text=True, timeout=15)
        return r.returncode == 0
    except Exception:
        return False


def cleanup_remote(ssh_target, use_sudo=False):
    """清理服务端残留的 Reactor 进程（异常/中断时兜底，避免残留占端口）"""
    sudo = "sudo " if use_sudo else ""
    try:
        subprocess.run(["ssh", *SSH_OPTS, ssh_target,
                        f"{sudo}pkill -9 -f benchmark/Reactor"],
                       capture_output=True, timeout=15)
    except Exception:
        pass


def prepare_env():
    """测试前准备：清残留进程、提 FD 上限、governor 切 performance（退出自动恢复）"""
    subprocess.run(["pkill", "-9", "-f", "benchmark/Reactor"], capture_output=True)
    subprocess.run(["pkill", "-9", "wrk"], capture_output=True)
    time.sleep(1)

    soft, hard = core.raise_nofile()
    if soft is not None:
        print(f"FD 上限: {soft} -> {hard}")

    gov_path = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    if gov_path.exists():
        try:
            cur = gov_path.read_text().strip()
        except Exception:
            cur = ""
        if cur and cur != "performance" and core.set_governor("performance"):
            core._OLD_GOVERNOR = cur
            import atexit
            atexit.register(core.restore_governor)
            print(f"governor 已切到 performance（原 {cur}）")


# ═══════════════════════════════════════════════════════════════════
# 角色一：server —— 手动模式，起多场景 + 采样，保持运行等 Ctrl-C
# ═══════════════════════════════════════════════════════════════════
def server_role(scenes):
    prepare_env()
    import psutil
    servers, sprocs = [], []
    for i, scene in enumerate(scenes):
        _, _, sub, worker = cloud.scenario_info(scene)
        server_cores, _ = cloud.alloc_cores(sub, worker)
        port = core.PORT_BASE + i
        srv, srv_log = core.start_server_retry(scene, port, server_cores)
        if srv is None:
            print(f"  {scene} 启动失败，跳过")
            continue
        servers.append((scene, port, srv, srv_log))
        sprocs.append(psutil.Process(srv.pid))
        print(f"  {scene} 已启动，监听 0.0.0.0:{port}")

    if not servers:
        print("无可用服务器")
        sys.exit(1)

    core.OUTPUT_DIR = Path(SERVER_METRICS_DIR)
    core.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    metrics_path = core.OUTPUT_DIR / "server_metrics.csv"
    mf = open(metrics_path, "w")
    mf.write("ts,scene,port,cpu_pct,rss_mb,fd\n")

    stop = threading.Event()

    def sample():
        for sp in sprocs:
            sp.cpu_percent(interval=None)
        while not stop.is_set():
            ts = time.time()
            for (scene, port, _, _), sp in zip(servers, sprocs):
                try:
                    cpu = sp.cpu_percent(interval=None)
                    rss = sp.memory_info().rss / 1048576
                    fd = sp.num_fds()
                    mf.write(f"{ts:.1f},{scene},{port},{cpu:.1f},{rss:.1f},{fd}\n")
                except Exception:
                    pass
            mf.flush()
            time.sleep(1)

    t = threading.Thread(target=sample, daemon=True)
    t.start()

    print(f"\n本机内网 IP（客户端 --host 用这个）: {print_local_ip()}")
    print("服务端指标采样已启动（server_metrics.csv），现在去客户端跑 wrk")
    print("压测完回到这里按 Ctrl-C 结束...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n结束...")
    finally:
        stop.set()
        t.join(timeout=2)
        mf.close()
        for scene, port, srv, srv_log in servers:
            srv.send_signal(signal.SIGTERM)
            try:
                srv.wait(timeout=5)
            except Exception:
                srv.kill()
            srv_log.close()
        print(f"服务端指标: {metrics_path}")


# ═══════════════════════════════════════════════════════════════════
# 角色二：run —— 供 SSH 调用，起单场景 + 采样固定时长 + 自动停
# ═══════════════════════════════════════════════════════════════════
def run_one(scene, port, dur):
    prepare_env()
    import psutil
    _, _, sub, worker = cloud.scenario_info(scene)
    server_cores, _ = cloud.alloc_cores(sub, worker)
    srv, srv_log = core.start_server_retry(scene, port, server_cores)
    if srv is None:
        print(f"{scene} 启动失败", flush=True)
        sys.exit(1)

    sp = psutil.Process(srv.pid)
    sp.cpu_percent(interval=None)

    out_dir = Path(SERVER_METRICS_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = out_dir / f"{scene}_metrics.csv"
    mf = open(metrics_path, "w")
    mf.write("ts,cpu_pct,rss_mb,fd\n")

    stop = threading.Event()

    def sample():
        while not stop.is_set():
            ts = time.time()
            try:
                cpu = sp.cpu_percent(interval=None)
                rss = sp.memory_info().rss / 1048576
                fd = sp.num_fds()
                mf.write(f"{ts:.1f},{cpu:.1f},{rss:.1f},{fd}\n")
            except Exception:
                pass
            mf.flush()
            time.sleep(1)

    t = threading.Thread(target=sample, daemon=True)
    t.start()
    print(f"{scene} 已启动 port={port}，采样 {dur}s ...", flush=True)
    time.sleep(dur)
    stop.set()
    t.join(timeout=2)
    mf.close()
    srv.send_signal(signal.SIGTERM)
    try:
        srv.wait(timeout=5)
    except Exception:
        srv.kill()
    srv_log.close()
    print(f"done {scene} metrics={metrics_path}", flush=True)


# ═══════════════════════════════════════════════════════════════════
# 角色三：client —— 压测；带 --ssh 时自动 SSH 起/停服务端
# ═══════════════════════════════════════════════════════════════════
def client_role(host, scenes, load, repeat, ssh_target=None, remote_script=None, use_sudo=False):
    prepare_env()
    core.HOST = host
    nodes = cloud.detect_topology()
    if len(nodes) >= 2:
        # 双 NUMA：wrk 只用 node1 物理核，保证整个 wrk 进程在同一 NUMA 节点内（不跨节点）
        wrk_cores = list(nodes[1])
    else:
        # 单 NUMA：全部物理核（天然在同一节点内）
        wrk_cores = []
        for cpus in nodes.values():
            wrk_cores.extend(cpus)

    core.OUTPUT_DIR = BENCHMARK_DIR / "data" / "remote" / datetime.now().strftime("%Y%m%d_%H%M%S")
    core.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    core.log(f"输出目录: {core.OUTPUT_DIR}")
    core.log(f"客户端可用物理核: {len(wrk_cores)}")

    sudo = "sudo " if use_sudo else ""

    # 前置检查 SSH（免密没配好就直接退出，别等到压测才发现）
    if ssh_target and not check_ssh(ssh_target, use_sudo):
        core.log(f"❌ SSH 连不上 {ssh_target}（免密没配？），退出")
        sys.exit(1)
    # 退出时（含 Ctrl-C / 异常）自动清理服务端残留 Reactor 进程
    if ssh_target:
        import atexit
        atexit.register(cleanup_remote, ssh_target, use_sudo)

    all_rows = []
    for i, scene in enumerate(scenes):
        _, _, sub, worker = cloud.scenario_info(scene)
        port = core.PORT_BASE + i
        url_path, est_per_core = core.LOADS[load]
        url = f"http://{host}:{port}{url_path}"

        core.log(f"\n{'='*60}\n  {scene}  port={port}  host={host}\n{'='*60}")
        if len(wrk_cores) < sub + 1:
            core.log(f"  ⚠️ 客户端核({len(wrk_cores)}) < 服务端核({sub+1})，客户端可能先饱和")

        ssh_proc = None
        if ssh_target:
            # 服务端 run 时长 = 扫描 + 正式测 + 余量
            dur = core.SCAN_DUR * 5 + core.DURATION * repeat + 60
            script = remote_script or "~/benchmark/scripts/bench_remote.py"
            cmd = f"{sudo}python3 {script} --role run --scene {scene} --port {port} --dur {dur}"
            core.log(f"  SSH 起服务端: ssh {ssh_target} \"{cmd}\"")
            ssh_proc = subprocess.Popen(
                ["ssh", *SSH_OPTS, ssh_target, cmd],
                stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            if not wait_port(host, port):
                core.log(f"  ❌ 服务端 {scene} 端口 {port} 未就绪，跳过")
                ssh_proc.terminate()
                continue

        # 预热（丢弃，让服务器热身 + 连接池建立 + 缓存就绪）
        core.run_wrk(url, core.WRK_CONC, None, wrk_cores, 8, None, 50000)
        time.sleep(2)

        # 扫拐点（不起本地服务器，server_pid=None 不采 perf）
        knee = core.scan_knee(scene, sub, port, [], wrk_cores, None, url_path, est_per_core)

        # 正式测 repeat 次取中位数
        reps = []
        for rep in range(repeat):
            r, wrk_cpu, out, _ = core.run_wrk(url, core.WRK_CONC, None, wrk_cores, core.DURATION, None, knee)
            if r["qps"] <= 0:
                core.log(f"rep{rep + 1}: wrk 失败")
                continue
            reps.append((r, wrk_cpu, out))
            core.log(f"rep{rep + 1}: QPS={r['qps']:,.1f}  P99={r['lat'].get(99.0)}  wrk_cpu={wrk_cpu:.0f}%")
            time.sleep(1)

        if ssh_proc:
            # 等服务端 run 结束（含采样收尾），再 scp 拉指标
            try:
                ssh_proc.wait(timeout=dur + 30)
            except subprocess.TimeoutExpired:
                ssh_proc.terminate()
            scp_cmd = ["scp", *SSH_OPTS, f"{ssh_target}:{SERVER_METRICS_DIR}/{scene}_metrics.csv", str(core.OUTPUT_DIR)]
            subprocess.run(scp_cmd, capture_output=True)
            core.log(f"  已拉回服务端指标 {scene}_metrics.csv")

        if not reps:
            continue
        reps.sort(key=lambda x: x[0]["qps"])
        r, wrk_cpu, out = reps[len(reps) // 2]
        lat = r["lat"]
        all_rows.append({
            "scene": scene, "load": load, "host": host, "port": port,
            "knee_rate": knee, "qps": r["qps"],
            "lat_p50": lat.get(50.0), "lat_p99": lat.get(99.0),
            "lat_p999": lat.get(99.9),
            "err_connect": r["err_connect"], "err_read": r["err_read"],
            "err_write": r["err_write"], "err_timeout": r["err_timeout"],
            "wrk_cpu": wrk_cpu,
        })
        (core.OUTPUT_DIR / f"wrk_remote_{scene}_{load}.log").write_text(out)

    if all_rows:
        csv_path = core.OUTPUT_DIR / "remote_results.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
            w.writeheader()
            w.writerows(all_rows)
        core.log(f"\n双机结果: {csv_path}")
        for r in all_rows:
            core.log(f"  {r['scene']} {r['load']} 拐点-R={r['knee_rate']}  QPS={r['qps']:,.1f}  "
                     f"P50={r['lat_p50']}  P99={r['lat_p99']}  P999={r['lat_p999']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=["server", "run", "client"])
    parser.add_argument("--scenes", default="sync-16,async-16", help="逗号分隔的场景名")
    parser.add_argument("--host", default=None, help="客户端用：服务端内网 IP")
    parser.add_argument("--ssh", default=None, help="客户端用：SSH 目标（user@IP），自动起/停服务端")
    parser.add_argument("--remote-script", default=None, help="服务端脚本路径（默认 ~/benchmark/scripts/bench_remote.py）")
    parser.add_argument("--sudo", action="store_true", help="SSH 起服务端时用 sudo")
    parser.add_argument("--load", default="64KB", help="客户端用：负载名")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--port", type=int, default=None, help="run 角色用：监听端口")
    parser.add_argument("--dur", type=int, default=200, help="run 角色用：采样时长（秒）")
    args = parser.parse_args()

    scenes = [s.strip() for s in args.scenes.split(",")]

    if args.role == "server":
        server_role(scenes)
    elif args.role == "run":
        if not args.port:
            print("run 角色需要 --port")
            sys.exit(1)
        run_one(scenes[0], args.port, args.dur)
    else:
        if not args.host:
            print("客户端需要 --host <服务端内网IP>")
            sys.exit(1)
        client_role(args.host, scenes, args.load, args.repeat,
                    ssh_target=args.ssh, remote_script=args.remote_script, use_sudo=args.sudo)


if __name__ == "__main__":
    main()
