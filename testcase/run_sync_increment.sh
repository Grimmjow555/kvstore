#!/usr/bin/env bash
#
# 主从「全量 + 增量」端到端验证（重设计阶段 3）。
#
# 链路：Master --(有序采集流)--> ebpf-agent <--(REPL_INC 反向连接)-- Replica
#   * ebpf-agent 独立进程先注册（EBPF 握手），Master 才受理 SYNC（eBPF 强制）；
#   * 全量同步期间写入的命令由 agent 缓存，读到 SYNC FINISHED 后按序补发；
#   * 之后的写入实时转发。
#
# 采集用 sim 模式（master 写共享内存环、agent 读），因为独立进程拿不到
# CAP_BPF（uprobe 模式的挂载需要 file capability）。真实 uprobe 路径见
# docs/replication-redesign.md §8.5，用 ebpf_capture_mode uprobe。

set -euo pipefail

# shellcheck source=testcase/lib_sync_test.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib_sync_test.sh"

SERVER="$KVS_TEST_BUILD_DIR/kvstore"
AGENT="$KVS_TEST_BUILD_DIR/ebpf-agent"
INSERT_BEFORE="$KVS_TEST_BUILD_DIR/testcase/master-replica/insert_before5w"
INSERT_AFTER="$KVS_TEST_BUILD_DIR/testcase/master-replica/insert_after5w"
GET10W="$KVS_TEST_BUILD_DIR/testcase/master-replica/get10w"

TMP_FILE="$KVS_TEST_DATA_DIR/kvstore.sync.tmp"
RING_FILE="$KVS_TEST_DATA_DIR/kvstore.ebpf.ring"

MASTER_PORT="$KVS_TEST_MASTER_PORT"
REPLICA_PORT="$KVS_TEST_REPLICA_PORT"

LOG_DIR="${TMPDIR:-/tmp}"
CONF="$LOG_DIR/kvstore-increment.$$.conf"
MASTER_LOG="$LOG_DIR/kvstore-incr-master.$$.log"
REPLICA_LOG="$LOG_DIR/kvstore-incr-replica.$$.log"
AGENT_LOG="$LOG_DIR/kvstore-incr-agent.$$.log"
GET_LOG="$LOG_DIR/kvstore-incr-get.$$.log"

MASTER_PID=""
REPLICA_PID=""
AGENT_PID=""

cleanup() {
    kvs_test_stop_process "$REPLICA_PID"
    kvs_test_stop_process "$AGENT_PID"
    kvs_test_stop_process "$MASTER_PID"
    kvs_test_kill_stragglers
    rm -f "$MASTER_LOG" "$REPLICA_LOG" "$AGENT_LOG" "$GET_LOG" "$CONF"
    kvs_test_remove_runtime_files
}

trap cleanup EXIT INT TERM

for bin in "$SERVER" "$AGENT" "$INSERT_BEFORE" "$INSERT_AFTER" "$GET10W"; do
    [[ -x "$bin" ]] || {
        echo "missing executable: $bin" >&2
        exit 1
    }
done

# sim 采集模式：master 创建采集环，ebpf-agent 读它。
kvs_test_write_sync_conf "$CONF"

kvs_test_remove_runtime_files
kvs_test_kill_stragglers

echo "1) 启动 Master 并写入前 5w"
kvs_test_start_bg "$MASTER_LOG" ./kvstore --config "$CONF" \
    --port "$MASTER_PORT" --role master
MASTER_PID=$KVS_TEST_LAST_PID
kvs_test_wait_for_port "$MASTER_PORT"
"$INSERT_BEFORE" 127.0.0.1 "$MASTER_PORT"

echo "2) 启动 ebpf-agent（采集 + 注册）"
    kvs_test_start_bg "$AGENT_LOG" ./ebpf-agent --mode sim --ring ../data/kvstore.ebpf.ring \
    --master "127.0.0.1:$MASTER_PORT" --listen "127.0.0.1:$KVS_TEST_AGENT_PORT"
AGENT_PID=$KVS_TEST_LAST_PID
for _ in {1..100}; do
    grep -q "registered with master" "$AGENT_LOG" 2>/dev/null && break
    sleep 0.1
done
if ! grep -q "registered with master" "$AGENT_LOG"; then
    echo "ebpf-agent 未注册成功：" >&2
    cat "$AGENT_LOG" >&2
    exit 1
fi

echo "3) 启动 Replica（全量同步），同时写入后 5w（部分落在同步窗口内被缓存）"
kvs_test_start_bg "$REPLICA_LOG" ./kvstore --config "$CONF" \
    --port "$REPLICA_PORT" --role replica \
    --master-ip 127.0.0.1 --master-port "$MASTER_PORT"
REPLICA_PID=$KVS_TEST_LAST_PID
kvs_test_wait_for_port "$REPLICA_PORT"
"$INSERT_AFTER" 127.0.0.1 "$MASTER_PORT"

echo "4) 等待增量通道建立与缓存冲刷"
for _ in {1..200}; do
    if grep -q "full sync completed" "$REPLICA_LOG" 2>/dev/null &&
        grep -q "increment channel connected to agent" "$REPLICA_LOG" 2>/dev/null; then
        break
    fi
    sleep 0.5
done
if ! grep -q "full sync completed" "$REPLICA_LOG"; then
    echo "Replica 全量同步未完成：" >&2
    cat "$REPLICA_LOG" >&2
    exit 1
fi
if ! grep -q "increment channel connected to agent" "$REPLICA_LOG"; then
    echo "增量通道未建立：" >&2
    cat "$REPLICA_LOG" >&2
    cat "$AGENT_LOG" >&2
    exit 1
fi
if ! grep -q "SYNC observed" "$AGENT_LOG"; then
    echo "agent 未观察到 SYNC 记录：" >&2
    cat "$AGENT_LOG" >&2
    exit 1
fi
if ! grep -q "SYNC FINISHED observed" "$AGENT_LOG"; then
    echo "agent 未观察到 SYNC FINISHED 记录：" >&2
    cat "$AGENT_LOG" >&2
    exit 1
fi

echo "5) 等待 Replica 追平并逐条比对 10w"
get_ok=0
for _ in {1..300}; do
    # 加超时：从机若在重放积压，客户端可能长时间拿不到响应，不能把脚本挂死。
    if timeout 60 "$GET10W" 127.0.0.1 "$REPLICA_PORT" >"$GET_LOG" 2>&1; then
        if ! grep -q "FAILED" "$GET_LOG"; then
            get_ok=1
            break
        fi
    fi
    sleep 0.5
done
if [[ "$get_ok" != "1" ]]; then
    echo "Replica 未与 Master 一致：" >&2
    grep "FAILED" "$GET_LOG" | head -5 >&2 || cat "$GET_LOG" >&2
    echo "--- agent log ---" >&2
    tail -20 "$AGENT_LOG" >&2
    echo "--- replica log ---" >&2
    tail -20 "$REPLICA_LOG" >&2
    exit 1
fi

echo "=== ebpf-agent 关键日志 ==="
grep -E "registered|SYNC observed|SYNC FINISHED observed|increment channel" "$AGENT_LOG"

echo "Increment sync (ebpf-agent process split) cycle completed."
