#!/usr/bin/env python3
"""
calibrate.py —— compute / delay 负载耗时校准

目的：
  /compute?n=XXX 的迭代次数 n 与单请求耗时的关系取决于 CPU 频率，每台机器不同。
  本脚本测出「多少 n 对应 ~1ms 单请求」，供 bench 脚本校准 compute 的默认迭代次数
  （把每核 QPS 预估对齐到 ~1000，即单请求 ~1ms）。

原理：
  用 wrk 单连接（-c1 -t1）打满模式压 /compute?n=N，串行下 QPS = 1 / 单请求耗时。

用法（本机或云服务器上均可）：
    sudo python3 calibrate.py
    sudo python3 calibrate.py --ns 50000,100000,200000,300000,500000,800000
"""

import sys
import time
import signal
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_local as core

# 校准输出到 /tmp，避开 data/（可能被 sudo 占为 root 属主，非 root 无写权限）
core.OUTPUT_DIR = Path("/tmp/reactorx_calibrate")


def measure(url, srv, wrk_cores, dur=3):
    """用 -R 极大值让服务器打满（-c1 串行），实际 QPS 收敛到服务器单核最大
    = 1/单请求耗时。wrk2 的 -R 是必填，故用超大 -R 模拟打满。"""
    r, _, _, _ = core.run_wrk(url, 1, None, wrk_cores[:1], dur, srv.pid, 1_000_000)
    return r["qps"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ns", type=str, default="50000,100000,200000,300000,500000,800000",
                        help="逗号分隔的 compute 迭代次数列表")
    parser.add_argument("--delay-ms", type=str, default="1,2,5", help="逗号分隔的 delay 毫秒列表")
    args = parser.parse_args()

    ns = [int(x) for x in args.ns.split(",")]
    delay_ms = [int(x) for x in args.delay_ms.split(",")]

    core.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    port = 6666
    server_cores, wrk_cores = core.alloc_cores(1, None)   # sync-1 单核，避免并行干扰
    srv, srv_log = core.start_server_retry("sync-1", port, server_cores)
    if srv is None:
        print("服务器启动失败，见 server_sync-1.log")
        sys.exit(1)

    try:
        # 预热
        core.run_wrk(f"http://127.0.0.1:{port}/", core.WRK_CONC, None, wrk_cores, 3, srv.pid, 50000)
        time.sleep(1)

        print("\n════ compute（CPU 密集）耗时校准 ════")
        print(f"{'n(迭代次数)':>12} {'QPS':>10} {'单请求耗时':>12}")
        for n in ns:
            qps = measure(f"http://127.0.0.1:{port}/compute?n={n}", srv, wrk_cores)
            cost_us = 1_000_000 / qps if qps > 0 else float("inf")
            print(f"{n:>12} {qps:>10,.1f} {cost_us:>10,.1f} us")

        print("\n════ delay（I/O 等待）耗时校准 ════")
        print(f"{'ms':>6} {'QPS':>10} {'单请求耗时':>12}")
        for ms in delay_ms:
            qps = measure(f"http://127.0.0.1:{port}/delay?ms={ms}", srv, wrk_cores)
            cost_us = 1_000_000 / qps if qps > 0 else float("inf")
            print(f"{ms:>6} {qps:>10,.1f} {cost_us:>10,.1f} us")

        print("\n提示：找「单请求耗时 ≈ 1000 us」对应的 n，作为 compute 的默认迭代次数。")
    finally:
        srv.send_signal(signal.SIGTERM)
        try:
            srv.wait(timeout=3)
        except Exception:
            srv.kill()
        srv_log.close()


if __name__ == "__main__":
    main()
