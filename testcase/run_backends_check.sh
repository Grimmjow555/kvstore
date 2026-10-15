#!/usr/bin/env bash
#
# 三个网络后端（reactor / ntyco / proactor）的协议级回归（重设计阶段 4）。
#
# 重设计动了三个后端的收发路径（握手识别 + 增量回放不回响应），这里对每个后端
# 起一次服务，按「4 字节长度头 + RESP」逐一验证四引擎命令与异常响应。
#
# 依赖 python3 做帧收发（仓库其他脚本只用 bash，这里为了可靠解析长度头而引入
# 这一项；没有 python3 时脚本会跳过并给出提示）。

set -euo pipefail

# shellcheck source=testcase/lib_sync_test.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib_sync_test.sh"

SERVER="$KVS_TEST_BUILD_DIR/kvstore"
PORT=10199
LOG_DIR="${TMPDIR:-/tmp}"
CONF="$LOG_DIR/kvstore-backends.$$.conf"
LOG="$LOG_DIR/kvstore-backends.$$.log"
SERVER_PID=""

command -v python3 >/dev/null 2>&1 || {
    echo "skip: python3 not found" >&2
    exit 0
}

cleanup() {
    kvs_test_stop_process "$SERVER_PID"
    SERVER_PID=""
    rm -f "$CONF" "$LOG"
}

trap cleanup EXIT INT TERM

kvs_test_require_executables "$SERVER" || exit 1

check_backend() {
    local backend="$1"

    cat >"$CONF" <<EOF
bind 0.0.0.0
port $PORT
log_level warn
role master
persistence_mode none
network_architecture $backend
EOF

    kvs_test_start_bg "$LOG" ./kvstore --config "$CONF"
    SERVER_PID=$KVS_TEST_LAST_PID
    kvs_test_wait_for_port "$PORT"

    python3 - "$PORT" <<'PY'
import socket, sys
port = int(sys.argv[1])

def call(cmd, *args):
    parts = [f"*{len(args)+1}\r\n", f"${len(cmd)}\r\n{cmd}\r\n"]
    for a in args:
        parts.append(f"${len(a)}\r\n{a}\r\n")
    payload = "".join(parts).encode()
    s = socket.create_connection(("127.0.0.1", port), 5)
    s.sendall(len(payload).to_bytes(4, "big") + payload)
    s.settimeout(5)
    hdr = b""
    while len(hdr) < 4:
        c = s.recv(4 - len(hdr))
        if not c:
            break
        hdr += c
    n = int.from_bytes(hdr, "big")
    data = b""
    while len(data) < n:
        c = s.recv(n - len(data))
        if not c:
            break
        data += c
    s.close()
    return data

# 四引擎命令 + 异常响应：写入、读取、存在、删除、重复写入。
checks = [
    (("SET", "k1", "v1"), b"+OK\r\n"),
    (("GET", "k1"), b"$2\r\nv1\r\n"),
    (("EXIST", "k1"), b"$5\r\nEXIST\r\n"),
    (("SET", "k1", "v1"), b"+EXIST\r\n"),
    (("DEL", "k1"), b"+OK\r\n"),
    (("GET", "k1"), b"$8\r\nNO EXIST\r\n"),
    (("RSET", "r1", "v1"), b"+OK\r\n"),
    (("RGET", "r1"), b"$2\r\nv1\r\n"),
    (("REXIST", "r1"), b"$5\r\nEXIST\r\n"),
    (("RDEL", "r1"), b"+OK\r\n"),
    (("HSET", "h1", "v1"), b"+OK\r\n"),
    (("HGET", "h1"), b"$2\r\nv1\r\n"),
    (("HDEL", "h1"), b"+OK\r\n"),
    (("SSET", "s1", "v1"), b"+OK\r\n"),
    (("SGET", "s1"), b"$2\r\nv1\r\n"),
    (("SDEL", "s1"), b"+OK\r\n"),
    (("MOD", "nope", "x"), b"$8\r\nNO EXIST\r\n"),
    (("GET", "nope"), b"$8\r\nNO EXIST\r\n"),
]

failed = 0
for (cmd, *args), expected in checks:
    got = call(cmd, *args)
    if got != expected:
        print(f"FAIL {cmd} {args}: got {got!r} expected {expected!r}")
        failed += 1
sys.exit(1 if failed else 0)
PY

    kvs_test_stop_process "$SERVER_PID"
    SERVER_PID=""
}

for backend in reactor ntyco proactor; do
    echo "--- backend: $backend ---"
    if ! check_backend "$backend"; then
        echo "backend $backend 协议回归失败" >&2
        cat "$LOG" >&2
        exit 1
    fi
done

echo "Backends protocol check completed."
