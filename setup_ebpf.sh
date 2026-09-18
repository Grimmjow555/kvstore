#!/usr/bin/env bash
set -euo pipefail

# 一键配置 kvstore 的 eBPF 本地实时同步运行环境。
#
# 脚本会完成：
#   1. 检查 bpffs 是否已挂载，必要时挂载或重挂载为可写
#   2. 创建 /sys/fs/bpf/kvstore 并授权给当前用户
#   3. 给 build/kvstore 设置 BPF capability
#
# 用法：
#   ./setup_ebpf.sh [--binary path/to/kvstore] [--pin-dir /sys/fs/bpf/kvstore] [--caps cap_bpf,cap_sys_admin+ep]

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${KVS_EBPF_BIN:-${ROOT_DIR}/build/kvstore}"
PIN_DIR="${KVS_EBPF_PIN_DIR:-/sys/fs/bpf/kvstore}"
BPF_MOUNT="/sys/fs/bpf"
CAPS=""

usage() {
    cat <<'EOF'
用法:
  ./setup_ebpf.sh [选项]

选项:
  --binary <path>    kvstore 二进制路径，默认 build/kvstore
  --pin-dir <path>   eBPF pin 目录，默认 /sys/fs/bpf/kvstore
  --caps <caps>      capability 字符串，默认根据内核版本自动选择
  -h, --help         显示本帮助

示例:
  ./setup_ebpf.sh
  ./setup_ebpf.sh --binary build/kvstore
  ./setup_ebpf.sh --caps cap_bpf,cap_sys_admin+ep
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

bpffs_mounted() {
    grep -q " ${BPF_MOUNT} bpf " /proc/mounts
}

bpffs_readonly() {
    awk -v m="${BPF_MOUNT}" '
        $2 == m && $3 == "bpf" { opts = $4 }
        END {
            if (opts ~ /(^|,)ro(,|$)/) exit 0
            exit 1
        }
    ' /proc/mounts
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --binary)
            [ "$#" -ge 2 ] || die "--binary needs an argument"
            BIN="$2"
            shift 2
            ;;
        --pin-dir)
            [ "$#" -ge 2 ] || die "--pin-dir needs an argument"
            PIN_DIR="$2"
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

if [ -z "$CAPS" ]; then
    case "$(uname -r)" in
        [0-4].*|5.[0-7]*)
            # Linux 5.8 之前没有 CAP_BPF。
            CAPS="cap_sys_admin+ep"
            ;;
        *)
            CAPS="cap_bpf,cap_sys_admin+ep"
            ;;
    esac
fi

if [ "$(id -u)" -ne 0 ] && ! command -v sudo >/dev/null 2>&1; then
    die "当前不是 root，且系统没有 sudo；请先安装 sudo 或以 root 运行本脚本"
fi

for tool in mount mkdir chown chmod setcap id uname grep awk; do
    command -v "$tool" >/dev/null 2>&1 || die "缺少必需命令: $tool"
done

[ -f "$BIN" ] || die "找不到 kvstore 二进制: $BIN。请先执行 cmake --build build -j\$(nproc)"
[ -x "$BIN" ] || die "kvstore 二进制没有执行权限: $BIN"

if bpffs_mounted; then
    echo "[1/4] bpffs 已挂载: $BPF_MOUNT"
    if bpffs_readonly; then
        echo "      bpffs 当前为只读，尝试重挂载为可写..."
        run_root mount -o remount,rw "$BPF_MOUNT" ||
            die "无法把 $BPF_MOUNT 重挂载为可写"
    else
        echo "      bpffs 当前可写"
    fi
else
    echo "[1/4] 未检测到 bpffs，正在挂载 $BPF_MOUNT ..."
    if [ -e "$BPF_MOUNT" ] && [ ! -d "$BPF_MOUNT" ]; then
        die "$BPF_MOUNT 已存在但不是目录"
    fi
    run_root mkdir -p "$BPF_MOUNT"
    run_root mount -t bpf bpf "$BPF_MOUNT" ||
        die "无法挂载 bpffs 到 $BPF_MOUNT"
fi

echo "[2/4] 准备 pin 目录: $PIN_DIR"
if [ -e "$PIN_DIR" ] && [ ! -d "$PIN_DIR" ]; then
    die "$PIN_DIR 已存在但不是目录"
fi

run_root mkdir -p "$PIN_DIR"
RUN_UID="$(id -u)"
RUN_GID="$(id -g)"
run_root chown "${RUN_UID}:${RUN_GID}" "$PIN_DIR"
run_root chmod 700 "$PIN_DIR"

echo "[3/4] 给 $BIN 添加 capability: $CAPS"
if ! run_root setcap "$CAPS" "$BIN"; then
    die "setcap 执行失败。请确认文件系统支持 xattr/security.capability，且未以 nosuid 挂载"
fi

if command -v getcap >/dev/null 2>&1; then
    echo "      当前 capability: $(getcap "$BIN")"
fi

cat <<EOF

eBPF 运行环境配置完成。

  pin 目录    : $PIN_DIR
  kvstore 程序: $BIN
  capability  : $CAPS

后续每次重新编译并替换 build/kvstore 后，需要重新执行：

  sudo setcap $CAPS $BIN

如果启动时仍提示 pin 文件已存在，可手动清理对应文件，例如：

  rm -f "$PIN_DIR"/kvstore_replication_*

验证 eBPF 队列是否生效：

  cd "$(dirname "$BIN")"
  ./$(basename "$BIN") <master_port> 0

如果看到类似如下日志，说明 eBPF 队列已启用：

  [EBPF] master realtime sync queue ready: $PIN_DIR/kvstore_replication_<master_port>

EOF
