#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# setup_env.sh —— ReactorX 云服务器（裸金属）环境一键配置脚本
#
# 用途：把压测所需的工具链一次性装齐，拷贝到服务器后 sudo 运行即可。
#   - 编译工具链：gcc / g++ / make / cmake
#   - 压测工具   ：wrk2（HDR 精确延迟；优先用本地打包的源码编译，见下）
#   - 性能工具   ：perf（linux-tools，用于 IPC/cache-miss 定位）
#   - 拓扑工具   ：numactl（查 NUMA 节点，决定绑核方案）
#   - 火焰图     ：FlameGraph（stackcollapse-perf.pl / flamegraph.pl，生成 SVG）
#   - Python 依赖：python3 / pip / psutil（走阿里云 pip 镜像）
#   - 网络工具   ：ss / iptables（查连接、bypass conntrack）
#
# 国内服务器网络提示：
#   - apt 源：阿里云 ECS 默认已是内网源 mirrors.cloud.aliyuncs.com，无需改；
#   - pip   ：本脚本已强制走阿里云内网源 mirrors.cloud.aliyuncs.com；
#   - wrk2  ：github 可能不可达，建议本机先下载源码打包上传（见「wrk2 编译」段）。
#
# 用法：
#   sudo ./setup_env.sh
#
# 幂等：可重复运行，已安装的会跳过/覆盖更新。
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

# 脚本所在目录（用于定位本地打包的 wrk2 源码）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 颜色输出
RED='\033[1;31m'; GRN='\033[1;32m'; YLW='\033[1;33m'; BLU='\033[1;34m'; NC='\033[0m'
info()  { printf "${BLU}[信息]${NC} %s\n" "$*"; }
ok()    { printf "${GRN}[完成]${NC} %s\n" "$*"; }
warn()  { printf "${YLW}[警告]${NC} %s\n" "$*" >&2; }
die()   { printf "${RED}[错误]${NC} %s\n" "$*" >&2; exit 1; }

# ── 1. 检查 root ─────────────────────────────────────────────────────
if [ "$(id -u)" != "0" ]; then
    die "请用 sudo 运行：sudo ./setup_env.sh"
fi

# ── 2. 检测发行版 ────────────────────────────────────────────────────
OS="unknown"
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS="${ID}"
fi
info "检测到发行版: ${OS}"

# ── 3. 按发行版安装 ──────────────────────────────────────────────────
case "${OS}" in
    ubuntu|debian)
        info "apt 更新源..."
        apt-get update -y
        info "安装编译工具链 + 压测/性能/拓扑工具..."
        apt-get install -y \
            build-essential cmake git \
            libssl-dev \
            numactl \
            python3 python3-pip \
            linux-tools-common linux-tools-generic \
            iptables iproute2 \
            htop
        ;;
    centos|rhel|rocky|almalinux|fedora)
        info "dnf/yum 安装..."
        if command -v dnf >/dev/null; then
            dnf install -y gcc gcc-c++ make cmake git openssl-devel \
                numactl python3 python3-pip perf iptables iproute htop
        else
            yum install -y gcc gcc-c++ make cmake git openssl-devel \
                numactl python3 python3-pip perf iptables iproute htop
        fi
        ;;
    *)
        warn "未识别的发行版 ${OS}，请手动安装：gcc/g++/cmake/make/wrk/perf/numactl/python3-pip/psutil"
        ;;
esac

# ── 4. wrk2 编译安装（wrk2 提供 HDR histogram 精确延迟，apt 无此包）──
# 注：wrk2 是 wrk 的 fork，编译产物二进制名仍为 wrk，但带 HDR 延迟统计。
#     脚本里后续压测命令仍用 `wrk`，实际是 wrk2 的能力。
# 国内网络：优先用「本地打包上传的源码」，避免 github 不可达。
#   - 本机预下载：cd benchmark/scripts && git clone https://github.com/giltene/wrk2.git wrk2
#   - 打包时带上 benchmark/scripts/wrk2/ 整个目录
if ! command -v wrk >/dev/null 2>&1; then
    # 1) 查找本地已打包的 wrk2 源码
    WRK2_SRC=""
    for p in "${SCRIPT_DIR}/wrk2" "${SCRIPT_DIR}/wrk2_src" "/root/wrk2"; do
        if [ -f "$p/Makefile" ] && [ -f "$p/src/wrk.c" ]; then
            WRK2_SRC="$p"; break
        fi
    done

    if [ -n "$WRK2_SRC" ]; then
        # 2) 用本地源码编译
        info "使用本地 wrk2 源码编译: ${WRK2_SRC}"
        cd "$WRK2_SRC"
        make -j"$(nproc)"
        cp wrk /usr/local/bin/wrk
        cd /
        ok "wrk2 编译安装完成（本地源码）"
    else
        # 3) 本地没有，尝试 github（国内可能失败）
        info "本地无 wrk2 源码，尝试从 github 下载（国内可能失败）..."
        cd /tmp
        rm -rf wrk2_build
        if git clone https://github.com/giltene/wrk2.git wrk2_build 2>/dev/null; then
            cd wrk2_build && make -j"$(nproc)" && cp wrk /usr/local/bin/wrk
            cd /tmp && rm -rf wrk2_build
            ok "wrk2 编译安装完成（github）"
        else
            warn "wrk2 下载失败（github 不可达）。请本机 git clone wrk2 源码，打包到 benchmark/scripts/wrk2/ 后重跑本脚本"
        fi
    fi
else
    ok "wrk 已存在: $(wrk --version 2>&1 | head -1)  （如非 wrk2，建议源码重装以获得 HDR 延迟统计）"
fi

# ── 5. Python psutil ─────────────────────────────────────────────────
# Ubuntu 24.04 的 Python 3.12 启用 PEP 668（externally-managed），直接 pip install 会被拦。
# 优先用系统包 python3-psutil，其次 pip --break-system-packages 兜底。
info "安装 python psutil..."
if apt-get install -y python3-psutil 2>/dev/null; then
    ok "psutil 安装完成（apt 系统包）"
elif python3 -m pip install --break-system-packages \
      -i http://mirrors.cloud.aliyuncs.com/pypi/simple/ \
      --trusted-host mirrors.cloud.aliyuncs.com psutil 2>/dev/null; then
    ok "psutil 安装完成（pip --break-system-packages）"
else
    warn "psutil 安装失败，请手动：sudo apt install python3-psutil"
fi

# ── 6. FlameGraph 工具（火焰图 SVG 生成）────────────────────────────
# find_flamegraph_tools() 会查找 /opt/FlameGraph、~/FlameGraph、/usr/local/FlameGraph。
# 优先用本地打包的 FlameGraph（github 国内可能不可达），否则 git clone。
info "安装 FlameGraph 工具（火焰图 SVG 生成）..."
if [ -d /opt/FlameGraph ] && [ -f /opt/FlameGraph/flamegraph.pl ]; then
    ok "FlameGraph 已存在: /opt/FlameGraph"
elif [ -d "${HOME}/FlameGraph" ] && [ -f "${HOME}/FlameGraph/flamegraph.pl" ]; then
    ok "FlameGraph 已存在: ${HOME}/FlameGraph"
elif [ -f "${SCRIPT_DIR}/FlameGraph/flamegraph.pl" ]; then
    cp -r "${SCRIPT_DIR}/FlameGraph" /opt/FlameGraph
    ok "FlameGraph 已从脚本目录复制到 /opt/FlameGraph"
elif git clone https://github.com/brendangregg/FlameGraph.git /opt/FlameGraph 2>/dev/null; then
    ok "FlameGraph 已 git clone 到 /opt/FlameGraph"
else
    warn "FlameGraph 下载失败（github 不可达）。请本机 git clone FlameGraph，打包到 benchmark/scripts/FlameGraph/ 后重跑本脚本"
fi

# ── 7. 版本检查 ──────────────────────────────────────────────────────
echo ""
info "════════ 安装结果检查 ════════"
for cmd in gcc g++ make cmake git wrk perf numactl python3; do
    if command -v "$cmd" >/dev/null 2>&1; then
        printf "  %-10s %s\n" "$cmd" "$($cmd --version 2>&1 | head -1)"
    else
        printf "  %-10s ${RED}未安装${NC}\n" "$cmd"
    fi
done
python3 -c "import psutil; print('  psutil    ', psutil.__version__)" 2>/dev/null \
    || printf "  psutil     ${RED}未安装${NC}\n"
FG_DIR=""
for d in /opt/FlameGraph "${HOME}/FlameGraph" /usr/local/FlameGraph; do
    if [ -f "$d/flamegraph.pl" ]; then FG_DIR="$d"; break; fi
done
if [ -n "$FG_DIR" ]; then printf "  flamegraph %s\n" "$FG_DIR"; else printf "  flamegraph ${RED}未安装${NC}\n"; fi

# ── 7. 提示后续手动设置 ──────────────────────────────────────────────
echo ""
info "════════ 后续需确认/手动设置 ════════"
echo "  NUMA 拓扑:"
numactl -H 2>/dev/null | head -8 || echo "    numactl 不可用"
echo ""
echo "  perf 权限 kernel.perf_event_paranoid = $(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo '?')"
echo "    若 >1 且非 root 采样硬件事件会失败；建议测试时:"
echo "      sudo sysctl -w kernel.perf_event_paranoid=0"
echo ""
echo "  governor = $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo '无(虚拟机或已锁频)')"
echo "    压测前应切 performance（bench 脚本会自动处理，需 sudo 免密或 root）"
echo ""
ok "环境配置完成。下一步：cmake --build build 编译，然后跑测试脚本。"
