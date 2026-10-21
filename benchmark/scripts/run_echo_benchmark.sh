#!/usr/bin/env bash
# run_echo_benchmark.sh —— echo 纯 TCP 回显完整测试（一键启动）
# 测试 1/4 subreactor × 11B/16KB/64KB，每组预热 30s，每档 30s × 2 次取平均。
# 预计总耗时约 55 分钟。
#
# 核拓扑（i7-13650HX: 6P + 8E）：
#   P 核取偶数逻辑核（0,2,4,6,8,10）避免 HT 兄弟争抢
#   客户端固定 1P+3E（10,12,13,14），两组 server 配置公平对比
#
# 输出：
#   benchmark/data/echo/<timestamp>/
#     sub1/  <size>/  rep1/ rep2/  report.log + results.csv
#     sub4/  <size>/  rep1/ rep2/  report.log + results.csv
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PROJ_DIR="$(cd ../.. && pwd)"
ECHO_DIR="$PROJ_DIR/benchmark/echo"
DATA_DIR="$PROJ_DIR/benchmark/data"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── 清理函数（Ctrl+C 或异常退出时杀 server + 恢复 governor）──
_OLD_GOV=""
cleanup() {
    if [ -n "${SRV:-}" ]; then
        kill "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
    fi
    # 恢复 CPU governor
    if [ -n "$_OLD_GOV" ]; then
        echo "  恢复 governor → $_OLD_GOV ..."
        for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
            echo "$_OLD_GOV" > "$g" 2>/dev/null || true
        done
    fi
}
trap cleanup EXIT

# ── 环境准备：CPU governor → performance ──
GOV_FILE="/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
if [ -f "$GOV_FILE" ]; then
    _OLD_GOV="$(cat "$GOV_FILE")"
    if [ "$_OLD_GOV" != "performance" ]; then
        log "CPU governor: $_OLD_GOV → performance"
        if [ "$(id -u)" -eq 0 ]; then
            for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
                echo performance > "$g" 2>/dev/null || true
            done
        else
            sudo -n sh -c 'for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo performance > "$g" 2>/dev/null; done' 2>/dev/null \
                || log "  ⚠️  需要 sudo 切换 governor，请手动执行或配置 sudo 免密"
        fi
    else
        _OLD_GOV=""
        log "CPU governor: 已是 performance"
    fi
else
    log "CPU governor: 无 cpufreq（虚拟机或已锁频），跳过"
fi

# ── 环境准备：提 FD 上限 ──
if [ "$(id -u)" -eq 0 ]; then
    ulimit -n 1000000
else
    ulimit -n "$(ulimit -Hn)" 2>/dev/null || true
fi
log "FD 上限: $(ulimit -n)"

# ── 编译 ──
log "编译 echo_server ..."
g++ -std=c++20 -O2 "$ECHO_DIR/echo_server.cpp" \
    -I "$PROJ_DIR/include" "$PROJ_DIR/build/libReactorX.a" \
    -pthread -o "$ECHO_DIR/echo_server"

log "编译 echo_client ..."
g++ -std=c++20 -O2 -Wall -Wextra "$ECHO_DIR/echo_client.cpp" \
    -pthread -o "$ECHO_DIR/echo_client"

# ── 输出目录 ──
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUTBASE="$DATA_DIR/echo/$TIMESTAMP"
mkdir -p "$OUTBASE"
log "输出目录: $OUTBASE"

# ── 客户端绑核（两组实验完全一致）──
CCPU="10,12,13,14"

DURATION=30
REPS=2

# ── 测试 ──
for sub in 1 4; do
    case $sub in
        1) SCPU="0,2"       ;;
        4) SCPU="0,2,4,6,8" ;;
    esac

    log ""
    log "══════════════════════════════════════════════"
    log "  echo_server: subreactors=$sub  cpus=$SCPU"
    log "══════════════════════════════════════════════"

    taskset -c "$SCPU" "$ECHO_DIR/echo_server" 7777 "$sub" > /dev/null 2>&1 &
    SRV=$!
    sleep 1.5

    # ── 预热 30s（丢弃）──
    log "  预热 $DURATION s（丢弃）..."
    "$ECHO_DIR/echo_client" 127.0.0.1 7777 0 65536 "$DURATION" \
        -c "$CCPU" > /dev/null 2>&1 || true
    sleep 2

    for size in 11 16384 65536; do
        if [ "$size" -ge 1024 ]; then
            label="$((size / 1024))KB"
        else
            label="${size}B"
        fi

        log ""
        log "── 消息 ${label}（${REPS} 次 × ${DURATION}s）──"

        for rep in $(seq 1 $REPS); do
            DIR="$OUTBASE/sub${sub}/${label}/rep${rep}"
            mkdir -p "$DIR"

            log "  [rep ${rep}/${REPS}]"
            "$ECHO_DIR/echo_client" 127.0.0.1 7777 0 "$size" "$DURATION" \
                -c "$CCPU" -o "$DIR"
        done
    done

    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    sleep 1
done
SRV=""   # 让 cleanup trap 不再重复杀

log ""
log "══════════════════════════════════════════════"
log "  测试完成"
log "  输出: $OUTBASE"
log "══════════════════════════════════════════════"