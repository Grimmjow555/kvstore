#!/usr/bin/env bash
#
# 异常路径回归（重设计阶段 5，对应设计 §9 异常矩阵）。
#
# 覆盖四条可复现、可自动判定的异常与恢复：
#   A. SYNC 时 ebpf 未武装：Master 回 -ERROR，Replica 退避重试；agent 起来后同步成功
#   B. 缓存溢出：agent 断开增量通道 -> Replica 重新全量同步 -> 最终一致
#   C. rdma-client 首次失败：Replica 等文件失败后重试 -> 传输成功 -> 最终一致
#   D. 发送前篡改 .tmp：Replica 拒绝加载且保留已有数据
#
# 可用 KVS_TEST_SCENARIOS=A,C 只运行指定场景。

set -euo pipefail

# shellcheck source=testcase/lib_sync_test.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib_sync_test.sh"

SERVER="$KVS_TEST_BUILD_DIR/kvstore"
AGENT="$KVS_TEST_BUILD_DIR/ebpf-agent"
CLIENT="$KVS_TEST_BUILD_DIR/testcase/integrity/integrity_client"

TMP_FILE="$KVS_TEST_DATA_DIR/kvstore.sync.tmp"
RECV_FILE="$TMP_FILE.recv"
RING_FILE="$KVS_TEST_DATA_DIR/kvstore.ebpf.ring"

MASTER_PORT="$KVS_TEST_MASTER_PORT"
REPLICA_PORT="$KVS_TEST_REPLICA_PORT"

LOG_DIR="${TMPDIR:-/tmp}"
# B 场景的写入停止标志（在 B 之前初始化，方便 cleanup 统一清理）
STOP_FLAG=""
CONF="$LOG_DIR/kvstore-faults.$$.conf"
WRAPPER="$LOG_DIR/kvstore-faults-rdma-client.$$.sh"
FLAG="$LOG_DIR/kvstore-faults-rdma-client.$$.flag"
TAMPER_WRAPPER="$LOG_DIR/kvstore-faults-rdma-tamper.$$.sh"
MASTER_LOG="$LOG_DIR/kvstore-faults-master.$$.log"
REPLICA_LOG="$LOG_DIR/kvstore-faults-replica.$$.log"
AGENT_LOG="$LOG_DIR/kvstore-faults-agent.$$.log"

MASTER_PID=""
REPLICA_PID=""
AGENT_PID=""
SCENARIOS="${KVS_TEST_SCENARIOS:-A,B,C,D}"

command -v python3 >/dev/null 2>&1 || {
    echo "skip: python3 not found" >&2
    exit 0
}

scenario_enabled() {
    [[ ",$SCENARIOS," == *",$1,"* ]]
}

# 停掉本轮所有进程和遗留的 RDMA 辅助进程。
stop_all() {
    kvs_test_stop_process "$REPLICA_PID"
    kvs_test_stop_process "$AGENT_PID"
    kvs_test_stop_process "$MASTER_PID"
    kvs_test_kill_stragglers
}

cleanup() {
    stop_all
    rm -f "$CONF" "$WRAPPER" "$FLAG" "$TAMPER_WRAPPER" "$STOP_FLAG"
    rm -f "$MASTER_LOG" "$REPLICA_LOG" "$AGENT_LOG"
    kvs_test_remove_runtime_files
}

trap cleanup EXIT INT TERM

# 写 N 条 RSET，并在 Replica 上逐条校验（小而快的数据集，够验证一致性）。
seed_and_verify() {
    local n="${1:-50}"
    python3 - "$MASTER_PORT" "$REPLICA_PORT" "$n" <<'PY'
import socket, sys, time
mport, rport, n = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])

def call(port, cmd, *args, timeout=10):
    parts = [f"*{len(args)+1}\r\n", f"${len(cmd)}\r\n{cmd}\r\n"]
    for a in args:
        parts.append(f"${len(a)}\r\n{a}\r\n")
    p = "".join(parts).encode()
    s = socket.create_connection(("127.0.0.1", port), 5)
    s.sendall(len(p).to_bytes(4, "big") + p)
    s.settimeout(timeout)
    h = b""
    while len(h) < 4:
        c = s.recv(4 - len(h))
        if not c:
            break
        h += c
    ln = int.from_bytes(h, "big")
    d = b""
    while len(d) < ln:
        c = s.recv(ln - len(d))
        if not c:
            break
        d += c
    s.close()
    return d

for i in range(n):
    r = call(mport, "RSET", f"f{i}", f"v{i}")
    if r != b"+OK\r\n":
        print("seed failed", i, r); sys.exit(1)

deadline = time.time() + 120
def expected(i):
    v = f"v{i}"
    return f"${len(v)}\r\n{v}\r\n".encode()

while time.time() < deadline:
    ok = all(call(rport, "RGET", f"f{i}") == expected(i) for i in range(n))
    if ok:
        print(f"replica converged with {n} keys")
        sys.exit(0)
    time.sleep(1)
print("replica did not converge"); sys.exit(1)
PY
}

# 写测试配置并覆盖默认项：公共部分见 lib_sync_test.sh，
# 这里只把「本场景要改的项」追加到文件末尾（后者覆盖前者）。
write_conf() {
    local capture_mode="$1" rdma_client="$2" transfer_timeout="$3" wait_timeout="$4"
    kvs_test_write_sync_conf "$CONF" \
        "rdma_client_bin $rdma_client" \
        "rdma_transfer_timeout_ms $transfer_timeout" \
        "sync_wait_timeout_ms $wait_timeout" \
        "ebpf_capture_mode $capture_mode"
}

start_master() {
    kvs_test_start_bg "$MASTER_LOG" ./kvstore --config "$CONF" \
        --port "$MASTER_PORT" --role master
    MASTER_PID=$KVS_TEST_LAST_PID
    kvs_test_wait_for_port "$MASTER_PORT"
}

start_replica() {
    kvs_test_start_bg "$REPLICA_LOG" ./kvstore --config "$CONF" \
        --port "$REPLICA_PORT" --role replica \
        --master-ip 127.0.0.1 --master-port "$MASTER_PORT"
    REPLICA_PID=$KVS_TEST_LAST_PID
    kvs_test_wait_for_port "$REPLICA_PORT"
}

start_agent() {
    local extra="${1:-}"
    # shellcheck disable=SC2086
    kvs_test_start_bg "$AGENT_LOG" ./ebpf-agent --mode sim --ring ../data/kvstore.ebpf.ring \
        --master "127.0.0.1:$MASTER_PORT" --replica "127.0.0.1:$REPLICA_PORT" $extra
    AGENT_PID=$KVS_TEST_LAST_PID
    for _ in {1..100}; do
        grep -q "registered with master" "$AGENT_LOG" 2>/dev/null && return 0
        sleep 0.1
    done
    echo "ebpf-agent 未注册" >&2
    cat "$AGENT_LOG" >&2
    return 1
}

kvs_test_require_executables "$SERVER" "$AGENT" "$CLIENT" || exit 1

# ==================== A. SYNC 时 ebpf 未武装 ====================
if scenario_enabled A; then
echo "A) SYNC 时 ebpf 未武装 -> -ERROR，agent 起来后恢复"
stop_all
rm -f "$RING_FILE" "$TMP_FILE"*
write_conf sim ./rdma-client 60000 120000
start_master
start_replica                     # 先起 Replica（此时没有 agent）
for _ in {1..60}; do
    grep -q "master rejected SYNC" "$REPLICA_LOG" 2>/dev/null && break
    sleep 0.5
done
if ! grep -q "master rejected SYNC" "$REPLICA_LOG"; then
    echo "A FAILED: Replica 未收到 -ERROR" >&2
    cat "$REPLICA_LOG" >&2
    exit 1
fi
start_agent
for _ in {1..240}; do
    grep -q "full sync completed" "$REPLICA_LOG" 2>/dev/null && break
    sleep 0.5
done
grep -q "full sync completed" "$REPLICA_LOG" || {
    echo "A FAILED: agent 起来后仍未完成全量同步" >&2
    cat "$REPLICA_LOG" >&2
    exit 1
}
echo "   A passed"
fi

# ==================== B. 缓存溢出 ====================
if scenario_enabled B; then
echo "B) 缓存溢出 -> agent 断开增量通道 -> Replica 重新全量同步"
stop_all
rm -f "$RING_FILE" "$TMP_FILE"*
write_conf sim ./rdma-client 60000 120000
start_master
start_agent "--cache-max-records 1"
start_replica
# 全量同步窗口内持续写入，让 agent 的缓存溢出。
# 关键：一旦观察到溢出就让写入者停下。否则「溢出 -> 重同步 -> 窗口内又有写入
# -> 再溢出」会无限循环（cache 上限只有 1 条），测试既慢又判不出来。
STOP_FLAG="$LOG_DIR/kvstore-faults-stop.$$.flag"
rm -f "$STOP_FLAG"
python3 - "$MASTER_PORT" "$STOP_FLAG" <<'PY' &
import os, socket, sys, time
port, stop_flag = int(sys.argv[1]), sys.argv[2]
s = socket.create_connection(("127.0.0.1", port), 5)
s.settimeout(30)
i = 0
while not os.path.exists(stop_flag):
    key = f"ov{i:05d}"
    p = f"*3\r\n$4\r\nRSET\r\n${len(key)}\r\n{key}\r\n${len(key)}\r\nv{key}\r\n".encode()
    s.sendall(len(p).to_bytes(4, "big") + p)
    s.recv(64)
    i += 1
    time.sleep(0.01)
s.close()
print(f"writer stopped after {i} keys")
PY
OVERFLOW_PID=$!
for _ in {1..600}; do
    grep -q "cache overflow" "$AGENT_LOG" 2>/dev/null && break
    sleep 0.5
done
if ! grep -q "cache overflow" "$AGENT_LOG"; then
    echo "B FAILED: 未观察到缓存溢出（可调小 cache 上限或加大写入量）" >&2
    cat "$AGENT_LOG" >&2
    touch "$STOP_FLAG"
    wait "$OVERFLOW_PID" 2>/dev/null || true
    exit 1
fi
# 写入者停下，之后的重同步轮次不会再溢出。
touch "$STOP_FLAG"
wait "$OVERFLOW_PID" || true

# 溢出后必须重新全量同步：以「Replica 再次加载快照」作为确定性证据
# （触发路径可能是「通道断开」或「等通道超时」，两者都会重新加载快照）。
loads=0
for _ in {1..600}; do
    loads="$(grep -c "snapshot loaded" "$REPLICA_LOG" 2>/dev/null || true)"
    if [[ "${loads:-0}" -ge 2 ]]; then
        break
    fi
    sleep 0.5
done
if [[ "${loads:-0}" -lt 2 ]]; then
    echo "B FAILED: Replica 未因缓存溢出重新全量同步（只加载了 ${loads:-0} 次快照）" >&2
    tail -20 "$REPLICA_LOG" >&2
    exit 1
fi
seed_and_verify 50 || {
    echo "B FAILED: 重同步后数据未收敛" >&2
    exit 1
}
echo "   B passed"
fi

# ==================== C. rdma-client 首次失败 ====================
if scenario_enabled C; then
echo "C) rdma-client 首次失败 -> Replica 重试 -> 传输成功"
stop_all
rm -f "$RING_FILE" "$TMP_FILE"* "$FLAG"
cat >"$WRAPPER" <<EOF
#!/usr/bin/env bash
# 第一次直接失败，之后转发给真正的 rdma-client：模拟一次传输故障。
if [ ! -f "$FLAG" ]; then
    touch "$FLAG"
    exit 1
fi
exec "$KVS_TEST_BUILD_DIR/rdma-client" "\$@"
EOF
chmod +x "$WRAPPER"
write_conf sim "$WRAPPER" 5000 8000
start_master
start_agent
start_replica
for _ in {1..240}; do
    grep -q "full sync completed" "$REPLICA_LOG" 2>/dev/null && break
    sleep 0.5
done
if ! grep -q "full sync completed" "$REPLICA_LOG"; then
    echo "C FAILED: 首次失败后未恢复" >&2
    tail -20 "$REPLICA_LOG" >&2
    exit 1
fi
if ! grep -q "sync attempt failed" "$REPLICA_LOG"; then
    echo "C FAILED: 未观察到首次失败（rdma-client 包装脚本可能没生效）" >&2
    exit 1
fi
seed_and_verify 50 || {
    echo "C FAILED: 重试成功后数据未收敛" >&2
    exit 1
}
echo "   C passed"
fi

# ==================== D. 篡改同步快照 ====================
if scenario_enabled D; then
echo "D) rdma-client 发送前篡改 .tmp -> Replica 拒绝加载且保留现有数据"
stop_all
rm -f "$RING_FILE" "$TMP_FILE"*
cat >"$TAMPER_WRAPPER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
args=("\$@")
file=""
while ((\$#)); do
    case "\$1" in
        --file)
            file="\$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done
printf '\\x00' | dd of="\$file" bs=1 seek=16 conv=notrunc status=none
exec "$KVS_TEST_BUILD_DIR/rdma-client" "\${args[@]}"
EOF
chmod +x "$TAMPER_WRAPPER"
write_conf sim "$TAMPER_WRAPPER" 60000 120000
start_replica
STABLE_KEY="stable_$$_$RANDOM"
stable_response="$("$CLIENT" 127.0.0.1 "$REPLICA_PORT" RSET "$STABLE_KEY" preserved)"
[[ "${stable_response%$'\r'}" == "+OK" ]]

# Replica 已先有本地数据；随后启动 Master/agent，让 Replica 收到一份被篡改的
# 全量快照。校验失败后必须继续保留上面的键。
start_master
source_response="$("$CLIENT" 127.0.0.1 "$MASTER_PORT" RSET source master_value)"
[[ "${source_response%$'\r'}" == "+OK" ]]
start_agent

tamper_detected=0
for _ in {1..600}; do
    if grep -q "Snapshot integrity check failed" "$REPLICA_LOG" 2>/dev/null; then
        tamper_detected=1
        break
    fi
    sleep 0.2
done
if [[ "$tamper_detected" != "1" ]]; then
    echo "D FAILED: Replica 未检测到被篡改的同步快照" >&2
    tail -30 "$REPLICA_LOG" >&2
    exit 1
fi
if [[ ! -f "$RECV_FILE" ]]; then
    echo "D FAILED: 未保留被拒绝的接收快照现场" >&2
    exit 1
fi
stable_response="$("$CLIENT" 127.0.0.1 "$REPLICA_PORT" RGET "$STABLE_KEY")"
stable_response="${stable_response%$'\r'}"
if [[ "$stable_response" != $'$9\r\npreserved' ]]; then
    echo "D FAILED: 校验失败覆盖了 Replica 现有数据: $stable_response" >&2
    exit 1
fi
echo "   D passed"
fi

echo "Fault-injection checks completed."
