#!/usr/bin/env bash
# 本机稳定测试：wrk2 版（--all --flame，暂不含 async）+ echo 版（纯 TCP）
# 无人值守串行执行，日志输出到标准输出，用 nohup 后台跑。
set -u
cd "$(dirname "$0")"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "========== 完整本机测试开始 =========="

# ── 1/2 wrk2 版：baseline + pipeline + scalability + 火焰图 ──
log "===== 1/2 wrk2 版（--all --flame，async 暂缓）====="
sudo python3 bench_local.py --all --flame
log "wrk2 版结束（退出码 $?）"

# ── 2/2 echo 版：纯 TCP 回显，测真实延迟与极限 QPS ──
log "===== 2/2 echo 版（约 4 分钟）====="
g++ -std=c++20 -O2 ../echo/echo_server.cpp -I ../../include ../../build/libReactorX.a -pthread -o ../echo/echo_server
g++ -std=c++20 -O2 ../echo/echo_client.cpp -pthread -o ../echo/echo_client
log "echo 工具编译完成"

# 启动 echo 服务端（绑 5 个 P 核，用 7777 避开 wrk2 的 6666）
taskset -c 0,2,4,6,8 ../echo/echo_server 7777 4 > /dev/null 2>&1 &
SRV=$!
sleep 1.5

# 不同消息大小各扫一次连接数找峰值
for size in 11 1024 16384 65536; do
    log "--- echo 消息 ${size}B ---"
    ../echo/echo_client 127.0.0.1 7777 0 "$size" 5
    echo ""
done

kill "$SRV" 2>/dev/null || true

log "========== 完整本机测试结束 =========="
