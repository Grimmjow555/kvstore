#!/usr/bin/env bash
set -euo pipefail

# 为 ebpf-agent 的 eBPF 实时增量采集（uprobe + ring buffer）准备运行环境。
#
# 采集链路需要两种能力：
#   * 加载 BPF 程序（BPF_PROG_LOAD）      -> CAP_BPF 或 CAP_SYS_ADMIN
#   * 打开 perf 事件并 attach BPF 程序    -> CAP_PERFMON 或 CAP_SYS_ADMIN
# attach 首选 uprobe 动态 PMU（/sys/bus/event_source/devices/uprobe/type），
# 和 libbpf 一样直接 perf_event_open，不需要 root、不写 tracefs、不用 bpffs；
# 老内核没有该 PMU 时回退 tracefs 的 uprobe_events（这一步通常需要 root）。
#
# 注意（重设计后）：采集已经从 kvstore 进程搬到独立的 ebpf-agent 进程，
# BPF 程序加载与 uprobe 挂载都发生在 agent 里，所以**要给 build/ebpf-agent
# 授权限，而不是 build/kvstore**（kvstore 只提供 kvs_ebpf_notify_write() 这个
# 挂载点，见 docs/replication-redesign.md §8.5）。
#
# 内核会在文件被写时清掉 security.capability，所以**每次重新编译
# build/ebpf-agent 之后都必须重新执行本脚本**，否则 agent 启动会打印
# 「create ringbuf map failed ... Operation not permitted」并直接退出——
# 采集是强制依赖（D3），不会再静默降级。
#
# 用法：
#   ./setup_ebpf.sh [--check] [--binary path/to/ebpf-agent] [--caps ...]

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${KVS_EBPF_BIN:-${ROOT_DIR}/build/ebpf-agent}"
CAPS=""
CHECK_ONLY=0

usage() {
    cat <<'EOF'
用法:
  ./setup_ebpf.sh [选项]

选项:
  --check            只检查环境与当前 capability，不做任何修改
  --binary <path>    ebpf-agent 二进制路径，默认 build/ebpf-agent
  --caps <caps>      capability 字符串，默认按内核版本自动选择
  -h, --help         显示本帮助

示例:
  ./setup_ebpf.sh --check
  ./setup_ebpf.sh
  ./setup_ebpf.sh --binary build/ebpf-agent
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
        echo "      警告: ringbuf 需要 Linux 5.8+；uprobe 模式不可用时会话只能用 ebpf_capture_mode sim"
    fi

    echo "[2/4] uprobe attach 途径"
    if [ -n "$PMU_TYPE" ]; then
        echo "      uprobe PMU 可用 (type=$PMU_TYPE)：不需要 root/tracefs/bpffs"
    elif [ -n "$TRACEFS" ]; then
        echo "      无 uprobe PMU，但有可写 tracefs: $TRACEFS"
    else
        echo "      无 uprobe PMU，且 tracefs 不可写：uprobe 模式无法 attach（改用 ebpf_capture_mode sim）"
    fi

    echo "[3/4] perf_event_paranoid = $PARANOID"
    echo "      带 cap_perfmon 或 cap_sys_admin 的进程不受该值限制；"
    echo "      以普通用户且无 capability 运行、且该值 > 1 时，perf_event_open 会被拒绝。"

    echo "[4/4] 采集进程二进制: $BIN"
    if [ -n "$CURCAP" ]; then
        echo "      当前 capability: $CURCAP"
    else
        echo "      当前 capability: 无（若不以 root 运行，BPF 程序加载/attach 会失败）"
    fi
}

if [ "$CHECK_ONLY" -eq 1 ]; then
    [ -e "$BIN" ] || die "找不到 ebpf-agent 二进制: $BIN"
    report_env
    echo
    if [ "$RINGBUF_OK" -eq 0 ]; then
        echo "结论: 当前内核不支持 ringbuf，uprobe 模式不可用；请把 kvstore.conf 的"
        echo "      ebpf_capture_mode 改为 sim（master 写共享内存环，agent 读该环）。"
        exit 1
    fi
    if [ -z "$PMU_TYPE" ] && [ -z "$TRACEFS" ]; then
        echo "结论: 没有可用的 uprobe attach 途径；请改用 ebpf_capture_mode sim。"
        exit 1
    fi
    if [ -z "$CURCAP" ] && [ "$(id -u)" -ne 0 ]; then
        echo "结论: 缺少 capability，且当前不是 root —— 需要执行 ./setup_ebpf.sh"
        echo "      （或 sudo 运行 ebpf-agent），否则 agent 会因采集失败直接退出。"
        exit 1
    fi
    echo "结论: 环境就绪，启动 ebpf-agent 后应能看到 'uprobe capture active' 日志。"
    exit 0
fi

for tool in mkdir chown chmod setcap id uname grep awk cut cat; do
    command -v "$tool" >/dev/null 2>&1 || die "缺少必需命令: $tool"
done

[ -f "$BIN" ] || die "找不到 ebpf-agent 二进制: $BIN。请先执行 cmake --build build -j\$(nproc)"
[ -x "$BIN" ] || die "ebpf-agent 二进制没有执行权限: $BIN"

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
（cmake --build）之后都要重新执行本脚本**，否则 ebpf-agent 会因采集失败直接退出：

  sudo setcap $CAPS $BIN

随时检查当前状态：

  ./setup_ebpf.sh --check

启动后确认采集是否生效（ebpf-agent 侧日志）：

  [EBPF] uprobe attached via perf PMU: <master binary>:0x<offset>
  [EBPF] uprobe capture active: <master binary>:0x<offset> pid=<master pid>
  [EBPF-AGENT] registered with master <ip>:<port>

脚本会把探针挂到 **kvstore（Master）二进制**的 kvs_ebpf_notify_write 符号上，
因此 Master 二进制不能被 strip，否则 agent 解析不到符号偏移。

若当前环境不具备上述能力（缺 capability、内核不支持 ringbuf、tracefs 不可写等），
可以把 kvstore.conf 的 ebpf_capture_mode 改为 sim：由 Master 把记录写进共享内存环、
agent 读该环，链路与 uprobe 模式等价，适合容器/CI 验证：

  ebpf_capture_mode sim
  ebpf_ring_file ../data/kvstore.ebpf.ring

注意采集是**强制依赖**：agent 未注册时 Master 会拒绝 SYNC（返回 -ERROR），
不会再像旧实现那样静默回退成「写路径直接落增量日志」。

EOF
