#!/usr/bin/env bash
#
# 主从全量同步端到端验证（重设计阶段 1 控制流 + 阶段 2 RDMA 独立进程搬运）。
#
# 覆盖点到点链路：
#   Master: SYNC -> 写 .tmp -> 派生 rdma-client
#   Replica: 派生 rdma-server -> SYNC -> 收 RDMA 文件 -> 加载 -> SYNC FINISHED
#   Master: 回收 rdma-client -> 删 .tmp
#
# 前提：RDMA 可用（siw）且 kvstore.conf 的 replica_host 是 RDMA 网卡 IP
# （RDMA 不能 resolve 127.0.0.1）。可用 setup_rdma.sh 准备 siw 链路。

set -euo pipefail

# shellcheck source=testcase/lib_sync_test.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib_sync_test.sh"

SERVER="$KVS_TEST_BUILD_DIR/kvstore"
RDMA_CLIENT="$KVS_TEST_BUILD_DIR/rdma-client"
RDMA_SERVER="$KVS_TEST_BUILD_DIR/rdma-server"
AGENT="$KVS_TEST_BUILD_DIR/ebpf-agent"
INSERT_BEFORE="$KVS_TEST_BUILD_DIR/testcase/master-replica/insert_before5w"
INSERT_AFTER="$KVS_TEST_BUILD_DIR/testcase/master-replica/insert_after5w"
GET10W="$KVS_TEST_BUILD_DIR/testcase/master-replica/get10w"

TMP_FILE="$KVS_TEST_DATA_DIR/kvstore.sync.tmp"
RECV_FILE="$TMP_FILE.recv"
READY_FILE="$TMP_FILE.ready"
CLIENT_LOG_FILE="$TMP_FILE.rdma-client.log"
SERVER_LOG_FILE="$TMP_FILE.rdma-server.log"

MASTER_PORT="$KVS_TEST_MASTER_PORT"
REPLICA_PORT="$KVS_TEST_REPLICA_PORT"

LOG_DIR="${TMPDIR:-/tmp}"
CONF="$LOG_DIR/kvstore-fullsync.$$.conf"
MASTER_LOG="$LOG_DIR/kvstore-fullsync-master.$$.log"
REPLICA_LOG="$LOG_DIR/kvstore-fullsync-replica.$$.log"
AGENT_LOG="$LOG_DIR/kvstore-fullsync-agent.$$.log"
MASTER_GET_LOG="$LOG_DIR/kvstore-fullsync-master-get.$$.log"
REPLICA_GET_LOG="$LOG_DIR/kvstore-fullsync-replica-get.$$.log"

MASTER_PID=""
REPLICA_PID=""
AGENT_PID=""

start_master() {
    kvs_test_start_bg "$MASTER_LOG" ./kvstore "$MASTER_PORT" 0 --config "$CONF"
    MASTER_PID=$KVS_TEST_LAST_PID
}

start_replica() {
    kvs_test_start_bg "$REPLICA_LOG" ./kvstore "$REPLICA_PORT" 1 127.0.0.1 "$MASTER_PORT" \
        --config "$CONF"
    REPLICA_PID=$KVS_TEST_LAST_PID
}

# 阶段 3 起 eBPF 是强制的：必须先有 ebpf-agent 注册，Master 才受理 SYNC。
start_agent() {
    kvs_test_start_bg "$AGENT_LOG" ./ebpf-agent --mode sim --ring ../data/kvstore.ebpf.ring \
        --master "127.0.0.1:$MASTER_PORT" --replica "127.0.0.1:$REPLICA_PORT"
    AGENT_PID=$KVS_TEST_LAST_PID
}

cleanup() {
    kvs_test_stop_process "$REPLICA_PID"
    kvs_test_stop_process "$AGENT_PID"
    kvs_test_stop_process "$MASTER_PID"
    kvs_test_kill_stragglers
    rm -f "$MASTER_LOG" "$REPLICA_LOG" "$AGENT_LOG" "$MASTER_GET_LOG" "$REPLICA_GET_LOG" "$CONF"
    kvs_test_remove_runtime_files
}

trap cleanup EXIT INT TERM

for bin in "$SERVER" "$AGENT" "$RDMA_CLIENT" "$RDMA_SERVER" "$INSERT_BEFORE" "$INSERT_AFTER" "$GET10W"; do
    [[ -x "$bin" ]] || {
        echo "missing executable: $bin" >&2
        exit 1
    }
done

kvs_test_remove_runtime_files

# 采集用 sim：独立进程拿不到 CAP_BPF，无法挂 uprobe。
kvs_test_write_sync_conf "$CONF"

echo "1) 启动 Master 并写入 10w 条（前 5w + 后 5w）"
start_master
kvs_test_wait_for_port "$MASTER_PORT"
"$INSERT_BEFORE" 127.0.0.1 "$MASTER_PORT"
"$INSERT_AFTER" 127.0.0.1 "$MASTER_PORT"

echo "2) 启动 Replica，触发 SYNC -> rdma-server/rdma-client 全量搬运"
start_agent
for _ in {1..100}; do
    grep -q "registered with master" "$AGENT_LOG" 2>/dev/null && break
    sleep 0.1
done
start_replica
kvs_test_wait_for_port "$REPLICA_PORT"
for _ in {1..240}; do
    if grep -q "full sync completed" "$REPLICA_LOG" 2>/dev/null; then
        break
    fi
    sleep 0.5
done
if ! grep -q "full sync completed" "$REPLICA_LOG"; then
    echo "Replica 全量同步未完成，日志：" >&2
    cat "$REPLICA_LOG" >&2
    [[ -f "$SERVER_LOG_FILE" ]] && cat "$SERVER_LOG_FILE" >&2
    exit 1
fi

echo "3) 校验 RDMA 独立进程链路"
if ! grep -q "rdma-server ready" "$REPLICA_LOG"; then
    echo "Replica 未启动 rdma-server" >&2
    cat "$REPLICA_LOG" >&2
    exit 1
fi
if ! grep -q "rdma-client spawned" "$MASTER_LOG"; then
    echo "Master 未派生 rdma-client" >&2
    cat "$MASTER_LOG" >&2
    exit 1
fi
if [[ ! -f "$CLIENT_LOG_FILE" ]] || ! grep -q "snapshot sent" "$CLIENT_LOG_FILE"; then
    echo "rdma-client 未完成发送，日志：" >&2
    cat "$CLIENT_LOG_FILE" 2>/dev/null >&2 || echo "(no client log)" >&2
    exit 1
fi
if [[ ! -f "$SERVER_LOG_FILE" ]] || ! grep -q "snapshot received" "$SERVER_LOG_FILE"; then
    echo "rdma-server 未完成接收，日志：" >&2
    cat "$SERVER_LOG_FILE" 2>/dev/null >&2 || echo "(no server log)" >&2
    exit 1
fi
if ! grep -q "\[SYNC\] sync finished, rdma-client released" "$MASTER_LOG"; then
    echo "Master 未在 SYNC FINISHED 后回收 rdma-client/删除 .tmp" >&2
    cat "$MASTER_LOG" >&2
    exit 1
fi

echo "4) 校验无临时文件残留（.tmp / .recv / .ready / .partial）"
for f in "$TMP_FILE" "$TMP_FILE.partial" "$RECV_FILE" "$RECV_FILE.partial" "$READY_FILE"; do
    if [[ -e "$f" ]]; then
        echo "存在残留文件: $f" >&2
        exit 1
    fi
done

echo "5) 逐条比对 Master / Replica 的 10w 数据"
"$GET10W" 127.0.0.1 "$MASTER_PORT" >"$MASTER_GET_LOG" 2>&1
"$GET10W" 127.0.0.1 "$REPLICA_PORT" >"$REPLICA_GET_LOG" 2>&1
if grep -q "FAILED" "$MASTER_GET_LOG"; then
    echo "Master 自检失败：" >&2
    grep "FAILED" "$MASTER_GET_LOG" | head -5 >&2
    exit 1
fi
if grep -q "FAILED" "$REPLICA_GET_LOG"; then
    echo "Replica 数据与 Master 不一致：" >&2
    grep "FAILED" "$REPLICA_GET_LOG" | head -5 >&2
    exit 1
fi

echo "Full sync (RDMA process split) cycle completed."
