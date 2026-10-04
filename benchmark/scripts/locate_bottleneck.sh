#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# locate_bottleneck.sh —— ReactorX 性能瓶颈精准定位脚本（教学版）
#
# 目的：用 perf 采样（record）+ 分析（report/annotate），把「宏观指标」下钻到
#       「哪个函数、哪一行」在烧 CPU / 造成 cache miss / 分支预测失败。
#
# 与 bench.py 的关系：
#   - bench.py     用 perf stat 拿「宏观比值」（IPC、cache_miss_rate），回答「是多少」；
#   - 本脚本       用 perf record 拿「采样样本」，回答「发生在哪」（函数/行）。
#
# 用法:
#   ./locate_bottleneck.sh [sub数] [并发] [时长秒] [路径] [keepalive|short]
#   示例:
#     # 长连接 CPU 热点
#     ./locate_bottleneck.sh 1 2000 30 /data/16k keepalive
#     # 短连接 cache miss 热点（本次 Channel 复用退化要定位的）
#     PERF_EVENT=cache-misses ./locate_bottleneck.sh 1 2000 30 / short
#     # 分支预测失败热点
#     PERF_EVENT=branch-misses ./locate_bottleneck.sh 1 2000 30 / keepalive
#
# 可采样事件（PERF_EVENT 环境变量，默认 cycles）:
#   cycles        —— CPU 周期      ：回答「CPU 时间烧在哪些函数」
#   cache-misses  —— 缓存未命中    ：回答「哪些函数缓存不友好」（定位缓存瓶颈）
#   branch-misses —— 分支预测失败  ：回答「哪些分支难预测」
#
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

# ═══════════════════════════════════ 配置 ═══════════════════════════════
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
SERVER_BIN="${PROJECT_DIR}/build/Reactor"     # 服务器二进制
PORT=6666                        # 端口
SUBS="${1:-1}"                   # subreactor 数
CONC="${2:-2000}"                # wrk 并发
DUR="${3:-30}"                   # 采样/压测时长（秒）
PATH_="${4:-/data/16k}"          # 请求路径（11B=/  16KB=/data/16k  64KB=/data/64k）
MODE="${5:-keepalive}"           # keepalive=长连接  short=短连接
EVENT="${PERF_EVENT:-cycles}"    # 采样事件（见文件头说明）
SERVER_CORES="0,2"               # 1-sub：main+sub 绑 2 个 P 核（物理核各取 1 逻辑核）
WRK_CORES="4,6,8,10"             # wrk 绑剩余 4 个 P 核
SAMPLE_FREQ="4000"               # 采样频率（Hz）。--call-graph=dwarf 展开栈较慢，
                                 # 频率过高会过载丢样本（日志出现 "lost chunks"），4000 较稳

OUT_DIR="${SCRIPT_DIR}/../data/perf_bottleneck"
TS=$(date +%Y%m%d_%H%M%S)
DATA="${OUT_DIR}/perf_${EVENT}_${MODE}_${TS}.data"   # perf 采样原始数据
REPORT="${OUT_DIR}/report_${EVENT}_${MODE}_${TS}.txt" # 分析报告
SERVER_PID=""
GOVERNOR_SWITCHED=0

# ═══════════════════════════════════ 工具 ═══════════════════════════════
say()  { printf "\n\033[1;34m[步骤]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[警告]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[错误]\033[0m %s\n" "$*" >&2; exit 1; }

# 采样/分析命令前缀：root 直接 perf，非 root 用 sudo
if [ "$(id -u)" = "0" ]; then PERF="perf"; else PERF="sudo perf"; fi

# 退出时统一清理：恢复 governor + 杀服务器（即使中途报错也会执行）
# trap EXIT 的原理：脚本在正常结束或被 set -e 中断时，都会执行 cleanup。
cleanup() {
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -9 "${SERVER_PID}" 2>/dev/null || true
  fi
  pkill -9 -f "${SERVER_BIN}" 2>/dev/null || true
  if [ "${GOVERNOR_SWITCHED}" = "1" ]; then
    sudo -n sh -c 'echo powersave | tee /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor > /dev/null' 2>/dev/null || true
    say "已恢复 governor -> powersave"
  fi
}
trap cleanup EXIT

# ═══════════════════════════════════ 步骤 1：环境检查 ═══════════════════════
check_env() {
  say "检查环境"
  command -v perf >/dev/null   || die "未安装 perf（sudo apt install linux-tools-common linux-tools-generic）"
  command -v wrk >/dev/null    || die "未安装 wrk"
  command -v taskset >/dev/null || die "未安装 taskset"
  [ -x "${SERVER_BIN}" ]       || die "找不到 ${SERVER_BIN}，先 cmake --build build"

  # perf_event_paranoid：控制非 root 用户能否采样。值越小权限越宽：
  #   -1 不限制；0 允许内核级；1 允许 CPU 级；2 只允许用户态；3/4 基本禁用。
  # 采样硬件事件（cycles/cache-misses）通常需要 <=1，或 root。
  local paranoid
  paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "?")
  say "kernel.perf_event_paranoid = ${paranoid}（<=1 可直接采样硬件事件，否则需 sudo）"
  if [ "${paranoid}" != "?" ] && [ "${paranoid}" -gt 1 ] && [ "$(id -u)" != "0" ]; then
    warn "非 root 且 paranoid=${paranoid}，硬件事件可能采样失败；建议 sudo 运行本脚本"
  fi
}

# ═══════════════════════════════════ 步骤 2：切 governor ═══════════════════
setup_governor() {
  local cur
  cur=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "?")
  # 原理：powersave 会把 CPU 锁到低频，压测结果被频率波动污染，无法定位真实瓶颈。
  #       performance 让 CPU 全程满频，采样结果可复现。
  if [ "${cur}" = "performance" ]; then return 0; fi
  say "当前 governor=${cur}，切到 performance（压测必须满频，否则结果失真）"
  if sudo -n sh -c 'echo performance | tee /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor > /dev/null' 2>/dev/null; then
    GOVERNOR_SWITCHED=1
    say "已切换 governor -> performance"
  else
    warn "切换失败（需 root 或 sudo 免密），频率波动会引入噪声"
  fi
}

# ═══════════════════════════════════ 步骤 3：启动服务器 ═══════════════════
start_server() {
  say "启动服务器: taskset -c ${SERVER_CORES} ${SERVER_BIN} ${PORT} ${SUBS}"
  # taskset：把服务器进程钉在指定 CPU 上，避免被调度器搬来搬去导致采样不稳定。
  taskset -c "${SERVER_CORES}" "${SERVER_BIN}" "${PORT}" "${SUBS}" \
    > "${OUT_DIR}/server_${TS}.log" 2>&1 &
  SERVER_PID=$!
  sleep 2
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    die "服务器启动失败，日志: ${OUT_DIR}/server_${TS}.log"
  fi
  say "服务器 PID=${SERVER_PID}"
}

# ═══════════════════════════════════ 步骤 4：预热 ═══════════════════════════
warmup() {
  # 原理：第一次跑连接时，连接池未创建、CPU 缓存未热、频率未拉满，
  #       若直接采样会把「冷启动开销」也采进去，污染热点。先跑一轮丢弃。
  say "预热 5s（结果丢弃，不采样）"
  taskset -c "${WRK_CORES}" wrk -t4 -c"${CONC}" -d5s \
    "http://127.0.0.1:${PORT}${PATH_}" > /dev/null 2>&1 || true
  sleep 2
}

# ═══════════════════════════════════ 步骤 5：perf record 采样 ═══════════════════
do_record() {
  say "perf record 采样 ${DUR}s（事件=${EVENT}，频率=${SAMPLE_FREQ}Hz）+ 并行 wrk 压测"

  # 短连接需要 Lua 脚本，显式加 Connection: close 让每次请求都建新连接
  local LUA=""
  if [ "${MODE}" = "short" ]; then
    cat > "${OUT_DIR}/short.lua" <<'EOF'
request = function()
    return wrk.format("GET", nil, {["Connection"] = "close"})
end
EOF
    LUA="-s ${OUT_DIR}/short.lua"
  fi

  # 后台启动 wrk，与 perf 采样并行（压测产生负载，采样记录这段时间 CPU 热点）
  taskset -c "${WRK_CORES}" wrk -t4 -c"${CONC}" -d"${DUR}"s ${LUA} \
    "http://127.0.0.1:${PORT}${PATH_}" > "${OUT_DIR}/wrk_${TS}.txt" 2>&1 &
  local wrk_pid=$!

  # ── perf record 各参数含义 ─────────────────────────────────────────
  #   -e "${EVENT}"            采样的事件：cycles（CPU 热点）/ cache-misses（缓存 miss）/ branch-misses
  #   -p "${SERVER_PID}"       只采样服务器进程（排除 wrk 客户端），聚焦框架本身
  #   -g --call-graph=dwarf    记录调用栈。为什么用 dwarf 不用默认 fp？
  #                             本工程 -O2 优化会省略 frame pointer，fp 回溯会断链；
  #                             dwarf 用调试信息（-g 编译，已带）回溯，更准。
  #   -F "${SAMPLE_FREQ}"      每秒采样次数（默认 4000；dwarf 展开栈慢，过高会过载丢样本）
  #   -o "${DATA}"             采样结果写到 perf.data
  #   -- sleep "${DUR}"        采样持续 DUR 秒后自动停止（perf record 需要一个「持续时长的命令」）
  # ───────────────────────────────────────────────────────────────────
  ${PERF} record \
    -e "${EVENT}" \
    -g --call-graph=dwarf \
    -p "${SERVER_PID}" \
    -o "${DATA}" \
    -F "${SAMPLE_FREQ}" \
    -- sleep "${DUR}" 2>>"${OUT_DIR}/perf_err_${TS}.log" || {
      warn "perf record 失败（可能权限不足），错误见 ${OUT_DIR}/perf_err_${TS}.log"
    }

  wait "${wrk_pid}" || true
  say "采样结束，数据文件: ${DATA}"
  echo "--- wrk 结果 ---"
  grep -E "Requests/sec|Latency|Socket errors" "${OUT_DIR}/wrk_${TS}.txt" || true
}

# ═══════════════════════════════════ 步骤 6：perf report 分析 ═══════════════════
report() {
  [ -f "${DATA}" ] || return 0
  say "perf report 分析（写入 ${REPORT} 并打印）"
  {
    echo "════════ 用户态 vs 内核态占比（--sort=dso）════════"
    # 先看「框架(Reactor) / libc / 内核(vmlinux)」各自占多少，判断瓶颈在哪一层
    ${PERF} report -i "${DATA}" --stdio --no-children --sort=dso --percent-limit 0.5 2>/dev/null | head -20

    echo ""
    echo "════════ Top 热点函数·自身开销（--no-children，看用户态框架）════════"
    # --no-children 只算函数「自身」开销，不包含它调用的子函数；
    # 这样才能让用户态框架函数（Channel/TCPConnection 等）浮出来，
    # 否则会被内核入口(entry_SYSCALL_64)巨大的累计调用树淹没。
    ${PERF} report -i "${DATA}" --stdio --no-children --sort=sym --percent-limit 0.5 2>/dev/null | head -30

    echo ""
    echo "════════ 调用链视角（--children 含子调用累计开销）════════"
    # --children        把「本函数 + 它调用的子函数」开销累加，区分
    #                   「自身热点」与「被调用拖累」两种不同瓶颈
    ${PERF} report -i "${DATA}" --stdio --sort=sym --children --percent-limit 1 2>/dev/null | head -50

    echo ""
    echo "════════ 按源码行定位（需 -g，本工程 RELWITHDEBINFO 已带）════════"
    # --sort=srcline    按「源文件:行号」聚合，直接定位到具体某一行
    ${PERF} report -i "${DATA}" --stdio --no-children --sort=srcline --percent-limit 0.5 2>/dev/null | head -30
  } | tee "${REPORT}"
}

# ═══════════════════════════════════ 步骤 7：perf annotate 下钻到行 ═══════════════════
annotate() {
  [ -f "${DATA}" ] || return 0
  say "perf annotate 定位到行（自动对 top1 函数）"

  # 从 report 里提取占比最高的函数名（形如 Channel::handle_events）
  local top
  top=$(${PERF} report -i "${DATA}" --stdio --sort=sym --percent-limit 2 2>/dev/null \
        | grep -oP '\[[a-zA-Z.]\] \K[^\s(]+' | head -1)
  if [ -n "${top}" ]; then
    echo "热点函数: ${top}"
    # annotate 把采样样本落到汇编 + 对应源码行，看「具体哪条指令/哪行」最热
    ${PERF} annotate -i "${DATA}" --stdio "${top}" 2>/dev/null | head -60 || true
  fi

  echo ""
  echo "手动下钻任意函数:"
  echo "  ${PERF} annotate -i ${DATA} --stdio <函数名>       # 看某函数的指令级热点"
  echo "  ${PERF} report   -i ${DATA} --stdio --sort=srcline  # 按源码行看全局热点"
}

# ═══════════════════════════════════ 主流程 ═══════════════════════════════
main() {
  mkdir -p "${OUT_DIR}"
  say "=== 性能瓶颈定位开始 ==="
  echo "  服务器 : ${SERVER_BIN} ${PORT} ${SUBS}   (绑核 ${SERVER_CORES})"
  echo "  压测   : wrk -t4 -c${CONC} -d${DUR}s ${MODE} ${PATH_}  (绑核 ${WRK_CORES})"
  echo "  采样   : ${PERF} record -e ${EVENT} -F ${SAMPLE_FREQ}Hz -g --call-graph=dwarf"
  echo "  输出   : ${OUT_DIR}/"

  check_env
  setup_governor
  start_server
  warmup
  do_record
  report
  annotate

  say "=== 完成，报告: ${REPORT} ==="
}

main "$@"
