#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
SERVER="$BUILD_DIR/kvstore"
SAVE_CLIENT="$ROOT_DIR/testcase/AOF/save"
LOAD_CLIENT="$ROOT_DIR/testcase/AOF/load"
SERVER_PID=""
SERVER_LOG="${TMPDIR:-/tmp}/kvstore-aof-cycle.$$.log"

start_server() {
    (
        cd "$BUILD_DIR"
        exec ./kvstore 9999 0 --rdb off --aof on
    ) >"$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
}

stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

wait_for_server() {
    for _ in {1..50}; do
        if (echo > /dev/tcp/127.0.0.1/9999) 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "kvstore did not start listening on port 9999" >&2
    return 1
}

cleanup() {
    stop_server
    rm -f "$SERVER_LOG"
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

echo "Starting kvstore for AOF save..."
start_server
wait_for_server
"$SAVE_CLIENT" 0.0.0.0 9999
stop_server

echo "Starting kvstore for AOF load..."
start_server
wait_for_server
"$LOAD_CLIENT" 0.0.0.0 9999
stop_server

echo "AOF save/load cycle completed."