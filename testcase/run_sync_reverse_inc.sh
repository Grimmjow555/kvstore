#!/usr/bin/env bash
#
# 反向增量通道端到端验证（不依赖真实 RDMA 设备）。
# Replica 主动连接 Agent；RDMA 工具由本地文件复制包装脚本替代。

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib_sync_test.sh"

SERVER="$KVS_TEST_BUILD_DIR/kvstore"
AGENT="$KVS_TEST_BUILD_DIR/ebpf-agent"
INSERT_BEFORE="$KVS_TEST_BUILD_DIR/testcase/master-replica/insert_before5w"
INSERT_AFTER="$KVS_TEST_BUILD_DIR/testcase/master-replica/insert_after5w"
GET10W="$KVS_TEST_BUILD_DIR/testcase/master-replica/get10w"
CLIENT="$KVS_TEST_BUILD_DIR/testcase/integrity/integrity_client"

MASTER_PORT="$KVS_TEST_MASTER_PORT"
REPLICA_PORT="$KVS_TEST_REPLICA_PORT"
AGENT_PORT="${KVS_TEST_AGENT_PORT:-11001}"
ADVERTISE_HOST="${KVS_TEST_RDMA_AUTO_HOST:-127.0.0.1}"
ADVERTISE_PORT="${KVS_TEST_RDMA_ADVERTISE_PORT:-12000}"

LOG_DIR="${TMPDIR:-/tmp}"
CONF="$LOG_DIR/kvstore-reverse-inc.$$.conf"
MASTER_LOG="$LOG_DIR/kvstore-reverse-master.$$.log"
REPLICA_LOG="$LOG_DIR/kvstore-reverse-replica.$$.log"
AGENT_LOG="$LOG_DIR/kvstore-reverse-agent.$$.log"
GET_LOG="$LOG_DIR/kvstore-reverse-get.$$.log"
CLIENT_ARGS_LOG="$LOG_DIR/kvstore-reverse-rdma-peer.$$.log"
CLIENT_WRAPPER="$LOG_DIR/kvstore-reverse-rdma-client.$$.sh"
SERVER_WRAPPER="$LOG_DIR/kvstore-reverse-rdma-server.$$.sh"

MASTER_PID=""
REPLICA_PID=""
AGENT_PID=""

cleanup() {
    kvs_test_stop_process "$REPLICA_PID"
    kvs_test_stop_process "$AGENT_PID"
    kvs_test_stop_process "$MASTER_PID"
    kvs_test_kill_stragglers
    rm -f "$CONF" "$MASTER_LOG" "$REPLICA_LOG" "$AGENT_LOG" "$GET_LOG" \
          "$CLIENT_ARGS_LOG" "$CLIENT_WRAPPER" "$SERVER_WRAPPER"
    kvs_test_remove_runtime_files
}
trap cleanup EXIT INT TERM

kvs_test_require_executables "$SERVER" "$AGENT" "$CLIENT" "$INSERT_BEFORE" "$INSERT_AFTER" "$GET10W" ||
    exit 1
command -v python3 >/dev/null 2>&1 || {
    echo "missing executable: python3" >&2
    exit 1
}
kvs_test_remove_runtime_files

export KVS_TEST_RDMA_ARGS_LOG="$CLIENT_ARGS_LOG"
cat >"$CLIENT_WRAPPER" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
file=""
peer=""
port=""
while (($#)); do
    case "$1" in
        --file)
            file="$2"
            shift 2
            ;;
        --peer)
            peer="$2"
            shift 2
            ;;
        --port)
            port="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done
[[ -n "$file" ]]
printf 'peer=%s port=%s\n' "$peer" "$port" >"$KVS_TEST_RDMA_ARGS_LOG"
cp "$file" "$file.recv"
EOF

cat >"$SERVER_WRAPPER" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
out=""
ready=""
while (($#)); do
    case "$1" in
        --out)
            out="$2"
            shift 2
            ;;
        --ready-file)
            ready="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done
[[ -n "$out" && -n "$ready" ]]
touch "$ready"
for _ in $(seq 1 10000); do
    [[ -f ../data/kvstore.sync.tmp ]] && break
    sleep 0.01
done
[[ -f ../data/kvstore.sync.tmp ]]
cp ../data/kvstore.sync.tmp "$out"
EOF
chmod +x "$CLIENT_WRAPPER" "$SERVER_WRAPPER"

KVS_TEST_RDMA_HOST="" kvs_test_write_sync_conf "$CONF" \
    "rdma_client_bin $CLIENT_WRAPPER" \
    "rdma_server_bin $SERVER_WRAPPER" \
    "agent_port $AGENT_PORT" \
    "rdma_port $ADVERTISE_PORT"

echo "1) 启动 Master 并写入前 5w"
kvs_test_start_bg "$MASTER_LOG" ./kvstore --config "$CONF" \
    --port "$MASTER_PORT" --role master
MASTER_PID=$KVS_TEST_LAST_PID
kvs_test_wait_for_port "$MASTER_PORT"
"$INSERT_BEFORE" 127.0.0.1 "$MASTER_PORT"

echo "2) 启动 Agent 反向监听并注册 Master"
kvs_test_start_bg "$AGENT_LOG" ./ebpf-agent --mode sim \
    --ring ../data/kvstore.ebpf.ring \
    --master "127.0.0.1:$MASTER_PORT" \
    --listen "127.0.0.1:$AGENT_PORT"
AGENT_PID=$KVS_TEST_LAST_PID
for _ in {1..100}; do
    grep -q "registered with master" "$AGENT_LOG" 2>/dev/null &&
        grep -q "listening for replica" "$AGENT_LOG" 2>/dev/null && break
    sleep 0.1
done
grep -q "registered with master" "$AGENT_LOG" || {
    cat "$AGENT_LOG" >&2
    exit 1
}

python3 - "$MASTER_PORT" <<'PY'
import socket
import sys

port = int(sys.argv[1])
parts = [b"*3\r\n", b"$4\r\nSYNC\r\n", b"$9\r\nnot-an-ip\r\n", b"$5\r\n70000\r\n"]
payload = b"".join(parts)
sock = socket.create_connection(("127.0.0.1", port), 5)
sock.sendall(len(payload).to_bytes(4, "big") + payload)
header = sock.recv(4)
length = int.from_bytes(header, "big")
response = b""
while len(response) < length:
    response += sock.recv(length - len(response))
sock.close()
if response != b"-ERROR\r\n":
    print("invalid advertised RDMA endpoint was not rejected:", response, file=sys.stderr)
    sys.exit(1)
PY

echo "3) 启动 Replica，等待其主动连接 Agent"
kvs_test_start_bg "$REPLICA_LOG" ./kvstore --config "$CONF" \
    --port "$REPLICA_PORT" --role replica \
    --master-ip 127.0.0.1 --master-port "$MASTER_PORT"
REPLICA_PID=$KVS_TEST_LAST_PID
kvs_test_wait_for_port "$REPLICA_PORT"
"$INSERT_AFTER" 127.0.0.1 "$MASTER_PORT"

for _ in {1..240}; do
    if grep -q "increment channel connected to agent" "$REPLICA_LOG" 2>/dev/null &&
        grep -q "SYNC FINISHED observed" "$AGENT_LOG" 2>/dev/null; then
        break
    fi
    sleep 0.5
done

grep -q "increment channel accepted from" "$AGENT_LOG" || {
    echo "Agent 未接受 Replica 的反向增量连接：" >&2
    cat "$AGENT_LOG" >&2
    exit 1
}
grep -q "increment channel connected to agent 127.0.0.1:$AGENT_PORT" "$REPLICA_LOG" || {
    echo "Replica 未主动连接 Agent：" >&2
    cat "$REPLICA_LOG" >&2
    exit 1
}
grep -q "peer=$ADVERTISE_HOST port=$ADVERTISE_PORT" "$CLIENT_ARGS_LOG" || {
    echo "Master 未使用 Replica 动态上报的 RDMA 地址：" >&2
    cat "$CLIENT_ARGS_LOG" 2>/dev/null >&2 || true
    cat "$MASTER_LOG" >&2
    exit 1
}

echo "4) 逐条校验 10w 数据"
synced=0
for _ in {1..300}; do
    if timeout 60 "$GET10W" 127.0.0.1 "$REPLICA_PORT" >"$GET_LOG" 2>&1 &&
        ! grep -q "FAILED" "$GET_LOG"; then
        synced=1
        break
    fi
    sleep 1
done

if [[ "$synced" != "1" ]]; then
    echo "Replica 数据未追平：" >&2
    cat "$GET_LOG" >&2
    cat "$REPLICA_LOG" >&2
    cat "$AGENT_LOG" >&2
    exit 1
fi

echo "5) Agent 断开后重启，验证 Replica 重新全量同步"
kvs_test_stop_process "$AGENT_PID"
AGENT_PID=""

# Agent 离线期间写入；该命令只能通过后续全量快照落到 Replica。
response="$("$CLIENT" 127.0.0.1 "$MASTER_PORT" RSET reverse_recovery recovered)"
[[ "${response%$'\r'}" == "+OK" ]]

kvs_test_start_bg "$AGENT_LOG" ./ebpf-agent --mode sim \
    --ring ../data/kvstore.ebpf.ring \
    --master "127.0.0.1:$MASTER_PORT" \
    --listen "127.0.0.1:$AGENT_PORT"
AGENT_PID=$KVS_TEST_LAST_PID

for _ in {1..240}; do
    loads="$(grep -c "snapshot loaded" "$REPLICA_LOG" 2>/dev/null || true)"
    if [[ "${loads:-0}" -ge 2 ]]; then
        break
    fi
    sleep 0.5
done
loads="$(grep -c "snapshot loaded" "$REPLICA_LOG" 2>/dev/null || true)"
if [[ "${loads:-0}" -lt 2 ]]; then
    echo "Agent 重启后 Replica 未重新全量同步：" >&2
    cat "$REPLICA_LOG" >&2
    cat "$AGENT_LOG" >&2
    exit 1
fi

response="$("$CLIENT" 127.0.0.1 "$REPLICA_PORT" RGET reverse_recovery)"
response="${response%$'\r'}"
if [[ "$response" != $'$9\r\nrecovered' ]]; then
    echo "重同步后缺少 Agent 离线期间的写入：$response" >&2
    exit 1
fi

echo "Reverse increment sync cycle completed."
