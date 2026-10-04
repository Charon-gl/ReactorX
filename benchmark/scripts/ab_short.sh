#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# ab_short.sh —— short 短连接 A/B 对比脚本（轻量版）
#
# 用途：验证「关掉环境开销（AppArmor / conntrack）」前后的 QPS 差多少。
# 特点：固定绑核 + 固定预热 + 同并发同时长，保证关前/关后两次测试条件一致，
#       这样 QPS 差值才可信，不会混入频率/温度/绑核等噪声。
#
# 用法:
#   ./ab_short.sh [并发] [时长秒]
#   ./ab_short.sh 2000 30       # 默认（c2000 单实例，最典型的档位）
#
# 对比流程:
#   1. 环境未改时跑一次，记下 QPS（基线）
#   2. 关 AppArmor + conntrack
#   3. 再跑一次，对比 QPS
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
SERVER_BIN="${PROJECT_DIR}/build/Reactor"
PORT=6666
SERVER_CORES="0,2"          # 1-sub：main + sub 绑 2 个 P 核
WRK_CORES="4,6,8,10"        # wrk 绑剩余 4 个 P 核
CONC="${1:-2000}"           # 并发
DUR="${2:-30}"              # 每档时长
SL="$(mktemp /tmp/short_XXXX.lua)"

# 动态生成短连接 Lua 脚本（Connection: close 每次新建连接）
cat > "$SL" <<'EOF'
request = function()
    return wrk.format("GET", nil, {["Connection"] = "close"})
end
EOF

SERVER_PID=""
cleanup() {
  [ -n "$SERVER_PID" ] && kill -9 "$SERVER_PID" 2>/dev/null || true
  pkill -9 -f "$SERVER_BIN" 2>/dev/null || true
  rm -f "$SL"
}
trap cleanup EXIT

echo "=== short 短连接 A/B 对比 ==="
echo "  并发 c${CONC}，每档 ${DUR}s"
echo "  服务器绑核 ${SERVER_CORES} / wrk 绑核 ${WRK_CORES}"
echo ""

# 启动服务器（1-sub）
taskset -c "$SERVER_CORES" "$SERVER_BIN" "$PORT" 1 > /tmp/ab_srv.log 2>&1 &
SERVER_PID=$!
sleep 2
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "服务器启动失败: $(cat /tmp/ab_srv.log)"; exit 1
fi
echo "服务器 PID=${SERVER_PID}"

# 预热（keepalive 8s，让连接池/CPU 缓存/频率进入稳态，结果丢弃）
echo "预热 8s（丢弃）..."
taskset -c "$WRK_CORES" wrk -t4 -c"$CONC" -d8s \
  "http://127.0.0.1:$PORT/data/16k" > /dev/null 2>&1
sleep 2

# short 短连接压测关键档位
echo ""
echo "=== short 短连接 QPS ==="
for path in "/" "/data/16k" "/data/64k"; do
  qps=$(taskset -c "$WRK_CORES" wrk -t4 -c"$CONC" -d"$DUR"s -s "$SL" \
        "http://127.0.0.1:$PORT$path" 2>/dev/null | grep "Requests/sec" | awk '{print $2}')
  printf "  %-12s c%-6s => %s QPS\n" "$path" "$CONC" "${qps:-N/A}"
  sleep 1
done

echo ""
echo "=== 完成：记录上面三个 QPS，改环境后再跑一次对比 ==="
