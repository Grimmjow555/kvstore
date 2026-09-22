#!/usr/bin/env bash
set -euo pipefail

# 一键同步并初始化 kvstore 的 Git 子模块。

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<'EOF'
用法:
  ./setup_submodule.sh
  ./setup_submodule.sh --help

说明:
  在仓库根目录同步 .gitmodules 配置，并递归初始化所有子模块。
  脚本可以重复执行；已初始化且没有更新的子模块不会被重新下载。
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

case "${1:-}" in
    "")
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        die "未知选项: $1（使用 --help 查看用法）"
        ;;
esac

command -v git >/dev/null 2>&1 || die "找不到 git，请先安装 Git"
[ -d "$ROOT_DIR/.git" ] || die "找不到 Git 仓库: $ROOT_DIR"
[ -f "$ROOT_DIR/.gitmodules" ] || die "找不到子模块配置文件: $ROOT_DIR/.gitmodules"

cd "$ROOT_DIR"

echo "[1/2] 同步子模块配置..."
git submodule sync --recursive

echo "[2/2] 初始化并更新子模块..."
git submodule update --init --recursive

echo "子模块配置完成。"
