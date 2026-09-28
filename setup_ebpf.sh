#!/usr/bin/env bash
set -euo pipefail

# 为 kvstore 的 eBPF 实时增量采集（kprobe/uprobe + ring buffer）准备运行环境。
#
# 采集链路需要两种能力：
#   * 加载 BPF 程序（BPF_PROG_LOAD）      -> CAP_BPF 或 CAP_SYS_ADMIN
#   * 打开 perf 事件并 attach BPF 程序    -> CAP_PERFMON 或 CAP_SYS_ADMIN
# attach 首选 uprobe 动态 PMU（/sys/bus/event_source/devices/uprobe/type），
# 和 libbpf 一样直接 perf_event_open，不需要 root、不写 tracefs、不用 bpffs；
# 老内核没有该 PMU 时回退 tracefs 的 uprobe_events（这一步通常需要 root）。
#
# 注意：内核会在文件被写时清掉 security.capability，所以**每次重新编译
# build/kvstore 之后都必须重新执行本脚本**，否则启动日志会变成
# 「create ringbuf map failed ... Operation not permitted」并回退直写 backlog。
#
# 用法：
#   ./setup_ebpf.sh [--check] [--binary path/to/kvstore] [--caps ...]

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${KVS_EBPF_BIN:-${ROOT_DIR}/build/kvstore}"
CAPS=""
CHECK_ONLY=0

usage() {
    cat <<'EOF'
用法:
  ./setup_ebpf.sh [选项]

选项:
  --check            只检查环境与当前 capability，不做任何修改
  --binary <path>    kvstore 二进制路径，默认 build/kvstore
  --caps <caps>      capability 字符串，默认按内核版本自动选择
  -h, --help         显示本帮助

示例:
  ./setup_ebpf.sh --check
  ./setup_ebpf.sh
  ./setup_ebpf.sh --binary build/kvstore
  ./setup_ebpf.sh --caps cap_bpf,cap_perfmon,cap_sys_admin+ep
EOF
    exit 0
}

die() {
    echo "error: $*" >&2
    exit 1
}

run_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --check)
            CHECK_ONLY=1
            shift
            ;;
        --binary)
            [ "$#" -ge 2 ] || die "--binary needs an argument"
            BIN="$2"
            shift 2
            ;;
        --caps)
            [ "$#" -ge 2 ] || die "--caps needs an argument"
            CAPS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
done

KREL="$(uname -r)"
KMAJ="${KREL%%.*}"
KMIN="$(echo "$KREL" | cut -d. -f2)"
case "$KMIN" in
    *[!0-9]*) KMIN=0 ;;
esac

if [ -z "$CAPS" ]; then
    if [ "$KMAJ" -lt 5 ] || { [ "$KMAJ" -eq 5 ] && [ "$KMIN" -lt 8 ]; }; then
        # Linux 5.8 之前没有 CAP_BPF，也没有 BPF ringbuf。
        CAPS="cap_sys_admin+ep"
    else
        CAPS="cap_bpf,cap_perfmon,cap_sys_admin+ep"
    fi
fi

RINGBUF_OK=0
if [ "$KMAJ" -gt 5 ] || { [ "$KMAJ" -eq 5 ] && [ "$KMIN" -ge 8 ]; }; then
    RINGBUF_OK=1
fi

# uprobe PMU（首选路径）
PMU_TYPE=""
for p in /sys/bus/event_source/devices/uprobe/type /sys/bus/event_source/devices/uprobes/type; do
    if [ -r "$p" ]; then
        PMU_TYPE="$(cat "$p")"
        break
    fi
done

# tracefs（回退路径）
TRACEFS=""
for d in /sys/kernel/tracing /sys/kernel/debug/tracing; do
    if [ -w "$d/uprobe_events" ]; then
        TRACEFS="$d"
        break
    fi
done

PARANOID="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo unknown)"
if command -v getcap >/dev/null 2>&1; then
    CURCAP="$(getcap "$BIN" 2>/dev/null || true)"
else
    CURCAP=""
fi

report_env() {
    echo "[1/4] 内核版本: $KREL"
    if [ "$RINGBUF_OK" -eq 1 ]; then
        echo "      支持 BPF ringbuf (>= 5.8)"
    else
        echo "      警告: ringbuf 需要 Linux 5.8+，启动后会回退「写路径直接落 backlog」"
    fi

    echo "[2/4] uprobe attach 途径"
    if [ -n "$PMU_TYPE" ]; then
        echo "      uprobe PMU 可用 (type=$PMU_TYPE)：不需要 root/tracefs/bpffs"
    elif [ -n "$TRACEFS" ]; then
        echo "      无 uprobe PMU，但有可写 tracefs: $TRACEFS"
    else
        echo "      无 uprobe PMU，且 tracefs 不可写：采集将无法 attach（会回退直写 backlog）"
    fi

    echo "[3/4] perf_event_paranoid = $PARANOID"
    echo "      带 cap_perfmon 或 cap_sys_admin 的进程不受该值限制；"
    echo "      以普通用户且无 capability 运行、且该值 > 1 时，perf_event_open 会被拒绝。"

    echo "[4/4] 二进制: $BIN"
    if [ -n "$CURCAP" ]; then
        echo "      当前 capability: $CURCAP"
    else
        echo "      当前 capability: 无（若不以 root 运行，BPF 程序加载/attach 会失败）"
    fi
}

if [ "$CHECK_ONLY" -eq 1 ]; then
    [ -e "$BIN" ] || die "找不到 kvstore 二进制: $BIN"
    report_env
    echo
    if [ "$RINGBUF_OK" -eq 0 ]; then
        echo "结论: 当前内核不支持 ringbuf，只能走直写 backlog。"
        exit 1
    fi
    if [ -z "$PMU_TYPE" ] && [ -z "$TRACEFS" ]; then
        echo "结论: 没有可用的 uprobe attach 途径，采集无法生效。"
        exit 1
    fi
    if [ -z "$CURCAP" ] && [ "$(id -u)" -ne 0 ]; then
        echo "结论: 缺少 capability，且当前不是 root —— 需要执行 ./setup_ebpf.sh（或 sudo 运行 kvstore）。"
        exit 1
    fi
    echo "结论: 环境就绪，启动后应能看到 'uprobe attached' 与 'realtime capture active' 日志。"
    exit 0
fi

for tool in mkdir chown chmod setcap id uname grep awk cut cat; do
    command -v "$tool" >/dev/null 2>&1 || die "缺少必需命令: $tool"
done

[ -f "$BIN" ] || die "找不到 kvstore 二进制: $BIN。请先执行 cmake --build build -j\$(nproc)"
[ -x "$BIN" ] || die "kvstore 二进制没有执行权限: $BIN"

report_env

echo "[5/5] 给 $BIN 添加 capability: $CAPS"
if ! run_root setcap "$CAPS" "$BIN"; then
    die "setcap 执行失败。请确认文件系统支持 xattr/security.capability，且未以 nosuid 挂载"
fi

if command -v getcap >/dev/null 2>&1; then
    APPLIED="$(getcap "$BIN" 2>/dev/null || true)"
    echo "      当前 capability: ${APPLIED:-（空）}"
    case "$APPLIED" in
        *cap_bpf*|*cap_sys_admin*) ;;
        *) die "setcap 返回成功但 capability 未出现在 $BIN 上，请检查文件系统是否支持 security.capability" ;;
    esac
fi

cat <<EOF

eBPF 运行环境配置完成。

  二进制      : $BIN
  capability  : $CAPS

重要：内核会在文件被写入时清除 security.capability，所以**每次重新编译
（cmake --build）之后都要重新执行本脚本**，否则采集会静默退回直写 backlog：

  sudo setcap $CAPS $BIN

随时检查当前状态：

  ./setup_ebpf.sh --check

启动后确认采集是否生效（Master 侧日志）：

  [EBPF] uprobe attached via perf PMU: <path>:0x<offset>
  [EBPF] realtime capture active: uprobe ringbuf -> replication backlog (cross-host OK)
  [REPLICATION] realtime increments captured by eBPF ringbuf

若看到 "fallback to direct backlog" / "eBPF capture unavailable"，说明当前环境
不具备上述能力（缺 capability、内核不支持 ringbuf 等）。此时写路径直接落增量日志，
复制与数据一致性不受影响，只是少了 eBPF 采集这一层。

没有 BPF 权限时仍可用模拟构建验证整条采集链路：

  cmake -S . -B build-sim -DCMAKE_CXX_FLAGS=-DKVS_EBPF_SIM -DCMAKE_C_FLAGS=-DKVS_EBPF_SIM
  cmake --build build-sim -j\$(nproc)

EOF
