#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
SERVER="$BUILD_DIR/kvstore"
SAVE_CLIENT="$BUILD_DIR/testcase/RDB/save"
LOAD_CLIENT="$BUILD_DIR/testcase/RDB/load"
SERVER_PID=""
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/kvstore-rdb-cycle.XXXXXX")"
RUN_DIR="$TEST_ROOT/run"
SERVER_LOG="$TEST_ROOT/server.log"
PORT="${KVSTORE_TEST_PORT:-9999}"

start_server() {
    (
        cd "$RUN_DIR"
        exec "$SERVER" --bind 127.0.0.1 --port "$PORT" --role master --rdb on --aof off
    ) >"$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
}

stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        # 向服务进程发送 SIGINT，等同于前台按 Ctrl+C。
        kill -INT "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

wait_for_server() {
    for _ in {1..50}; do
        if (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "kvstore did not start listening on port $PORT" >&2
    return 1
}

cleanup() {
    stop_server
    rm -rf "$TEST_ROOT"
}

trap cleanup EXIT INT TERM

[[ -x "$SERVER" ]] || {
    echo "missing executable: $SERVER" >&2
    exit 1
}
[[ -x "$SAVE_CLIENT" ]] || {
    echo "missing executable: $SAVE_CLIENT" >&2
    exit 1
}
[[ -x "$LOAD_CLIENT" ]] || {
    echo "missing executable: $LOAD_CLIENT" >&2
    exit 1
}

echo "Starting kvstore for RDB save..."
mkdir -p "$RUN_DIR"
start_server
wait_for_server
"$SAVE_CLIENT" 127.0.0.1 "$PORT"
stop_server

echo "Restarting kvstore for RDB auto-load..."
start_server
wait_for_server
"$LOAD_CLIENT" 127.0.0.1 "$PORT"
stop_server

echo "RDB save/auto-load cycle completed."
