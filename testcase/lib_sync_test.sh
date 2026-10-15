#!/usr/bin/env bash
#
# 同步相关测试脚本的公共库（由 testcase/run_sync_*.sh、run_backends_check.sh source）。
#
# 提供：路径推导、进程启停、端口/日志等待、同步配置生成、运行时临时文件清理。
# 这些逻辑原先在 4 个脚本里各写一份（stop_process ×3、wait_for_port ×4、
# 配置 heredoc ×3、清理文件清单 ×4），这里收敛成一份。
#
# 约定：
#   * 本文件不设置 set -e / IFS，由调用方脚本自己设定；
#   * 函数失败返回非 0，是否致命由调用方决定；
#   * 全局变量以 KVS_TEST_ 前缀，避免与脚本自身变量冲突。

# ==================== 路径与默认参数 ====================

KVS_TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KVS_TEST_ROOT_DIR="$(cd "$KVS_TEST_DIR/.." && pwd)"
KVS_TEST_BUILD_DIR="$KVS_TEST_ROOT_DIR/build"
KVS_TEST_DATA_DIR="$KVS_TEST_ROOT_DIR/data"

KVS_TEST_MASTER_PORT="${KVS_TEST_MASTER_PORT:-9999}"
KVS_TEST_REPLICA_PORT="${KVS_TEST_REPLICA_PORT:-10001}"
# Master 的 rdma-client 要连的 Replica RDMA 网卡地址；回环地址 RDMA resolv 不了。
KVS_TEST_RDMA_HOST="${KVS_TEST_RDMA_HOST:-192.168.234.135}"

# kvs_test_start_bg 启动的子进程 pid；调用方启动后立刻取走。
KVS_TEST_LAST_PID=""

# ==================== 进程启停 ====================

# 优雅停止：SIGINT -> 最多等 5s -> SIGKILL；再 wait 收尸。
# pid 为空或进程已退出时静默返回（可安全重复调用）。
kvs_test_stop_process() {
    local pid="$1"
    [[ -n "$pid" ]] || return 0
    kill -0 "$pid" 2>/dev/null || return 0

    kill -INT "$pid" 2>/dev/null || true
    for _ in {1..50}; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
    done
    kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

# 清理上一次被中断的验证遗留的辅助进程（它们会占着 rdma_port）。
kvs_test_kill_stragglers() {
    pkill -x rdma-server 2>/dev/null || true
    pkill -x rdma-client 2>/dev/null || true
}

# 在 build/ 下后台启动二进制，stdout/stderr 重定向到 log。
# 用法：kvs_test_start_bg <log> <cmd> [args...]；pid 见 KVS_TEST_LAST_PID。
kvs_test_start_bg() {
    local log="$1"
    shift
    ( cd "$KVS_TEST_BUILD_DIR" && exec "$@" ) >"$log" 2>&1 &
    KVS_TEST_LAST_PID=$!
}

# 校验一组二进制存在且可执行，缺失则打印并返回 1。
kvs_test_require_executables() {
    local bin
    for bin in "$@"; do
        if [[ ! -x "$bin" ]]; then
            echo "missing executable: $bin" >&2
            return 1
        fi
    done
    return 0
}

# ==================== 等待 ====================

# 等端口可连接；默认最多 10s（100 × 0.1s）。
kvs_test_wait_for_port() {
    local port="$1" tries="${2:-100}"
    for _ in $(seq 1 "$tries"); do
        if (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "kvstore did not listen on port $port" >&2
    return 1
}

# 等日志出现某个 pattern（grep -q 的 pattern）；默认最多等 120s。
kvs_test_wait_for_log() {
    local log="$1" pattern="$2" timeout_s="${3:-120}"
    local end=$((SECONDS + timeout_s))
    while (( SECONDS < end )); do
        grep -q -- "$pattern" "$log" 2>/dev/null && return 0
        sleep 0.2
    done
    return 1
}

# ==================== 配置 ====================

# 写一份主从同步测试用的 kvstore.conf。
# 额外参数按 "key value" 原样追加到文件末尾（配置按顺序解析，后者覆盖前者），
# 例如：kvs_test_write_sync_conf "$CONF" "rdma_client_bin /tmp/wrap.sh" "sync_wait_timeout_ms 8000"
kvs_test_write_sync_conf() {
    local path="$1"
    shift

    cat >"$path" <<EOF
bind 0.0.0.0
log_level info
persistence_mode none
network_architecture ntyco
sync_tmp_file ../data/kvstore.sync.tmp
rdma_client_bin ./rdma-client
rdma_server_bin ./rdma-server
rdma_port 10000
replica_host $KVS_TEST_RDMA_HOST
rdma_transfer_timeout_ms 60000
sync_wait_timeout_ms 120000
ebpf_capture_mode sim
ebpf_ring_file ../data/kvstore.ebpf.ring
EOF

    local line
    for line in "$@"; do
        printf '%s\n' "$line" >>"$path"
    done
}

# ==================== 临时文件清理 ====================

# 删除同步测试产生的运行时产物：快照临时文件（含 .recv/.ready/子进程日志）与采集环。
# 各脚本自己的日志文件由脚本清理。
kvs_test_remove_runtime_files() {
    local base="$KVS_TEST_DATA_DIR/kvstore.sync.tmp"
    rm -f "$base" "$base.partial" "$base.recv" "$base.recv.partial" "$base.ready" \
          "$base.rdma-client.log" "$base.rdma-server.log" \
          "$KVS_TEST_DATA_DIR/kvstore.ebpf.ring"
}
