#!/usr/bin/env bash
set -euo pipefail

# 配置 SoftiWARP RDMA 链路，供 kvstore 的 RDMA 全量同步使用。
# 脚本可以重复执行；已存在的 RDMA 链路不会重复创建。

NETDEV="${KVS_RDMA_NETDEV:-ens33}"
LINK_NAME="${KVS_RDMA_LINK:-siw1}"

usage() {
    cat <<'EOF'
用法:
  ./setup_rdma.sh [选项]

选项:
  --netdev <name>  底层网络设备，默认 ens33
  --link <name>    RDMA 链路名称，默认 siw1
  -h, --help       显示帮助

示例:
  ./setup_rdma.sh
  ./setup_rdma.sh --netdev ens33 --link siw1
EOF
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
        --netdev)
            [ "$#" -ge 2 ] || die "--netdev 需要参数"
            NETDEV="$2"
            shift 2
            ;;
        --link)
            [ "$#" -ge 2 ] || die "--link 需要参数"
            LINK_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "未知选项: $1（使用 --help 查看用法）"
            ;;
    esac
done

if [ "$(id -u)" -ne 0 ] && ! command -v sudo >/dev/null 2>&1; then
    die "当前不是 root，且系统没有 sudo；请先安装 sudo 或以 root 运行本脚本"
fi

for tool in id modprobe rdma ip; do
    command -v "$tool" >/dev/null 2>&1 || die "找不到必需命令: $tool"
done

ip link show "$NETDEV" >/dev/null 2>&1 ||
    die "找不到网络设备: $NETDEV"

if rdma link show "$LINK_NAME" >/dev/null 2>&1; then
    echo "RDMA 链路已存在: $LINK_NAME"
else
    echo "加载 SoftiWARP 内核模块: siw"
    run_root modprobe siw

    echo "创建 RDMA 链路: $LINK_NAME -> $NETDEV"
    run_root rdma link add "$LINK_NAME" type siw netdev "$NETDEV"
fi

echo
echo "RDMA 配置完成："
rdma link show "$LINK_NAME"
