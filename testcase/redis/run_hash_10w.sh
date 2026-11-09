#!/usr/bin/env bash
#
# 对比 kvstore 与 Redis 在 AOF 开启/关闭时执行 100,000 条 HSET 的 QPS。

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
DEFAULT_RECORDS=100000
RESULT_DIR="${RESULT_DIR:-${TMPDIR:-/tmp}/kvstore-redis-bench-results}"

usage() {
    cat >&2 <<EOF
用法: $0 <kvstore|redis> <on|off>

环境变量:
  PORT        服务端口，默认 kvstore=18379，redis=16379
  RESULT_DIR  结果目录，默认 ${TMPDIR:-/tmp}/kvstore-redis-bench-results
  KEYSPACE    Redis 随机字段空间，默认 100000
EOF
}

TARGET="${1:-}"
AOF_MODE="${2:-}"

case "$TARGET" in
    kvstore|redis) ;;
    *)
        usage
        exit 1
        ;;
esac

case "$AOF_MODE" in
    on|off) ;;
    *)
        usage
        exit 1
        ;;
esac

mkdir -p "$RESULT_DIR"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
SUMMARY_FILE="$RESULT_DIR/hash_10w_summary.csv"
RESULT_FILE="$RESULT_DIR/${TARGET}_aof_${AOF_MODE}_10w_${TIMESTAMP}.log"
SERVER_PID=""
REDIS_PIDFILE=""
SERVER_ROOT=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [[ -n "$REDIS_PIDFILE" && -f "$REDIS_PIDFILE" ]]; then
        REDIS_PID="$(cat "$REDIS_PIDFILE")"
        if kill -0 "$REDIS_PID" 2>/dev/null; then
            kill -TERM "$REDIS_PID" 2>/dev/null || true
        fi
    fi
}
trap cleanup EXIT INT TERM

wait_for_tcp() {
    local port="$1"
    local pid="$2"
    for _ in {1..100}; do
        if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            return 0
        fi
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
            return 1
        fi
        sleep 0.1
    done
    return 1
}

extract_redis_qps() {
    awk -F, 'NR == 1 { gsub(/"/, "", $2); print $2 }' "$1"
}

extract_kvstore_qps() {
    sed -n 's/.*qps: \([0-9][0-9]*\(\.[0-9]*\)\?\).*/\1/p' "$1" | tail -n 1
}

run_kvstore() {
    local port="${PORT:-18379}"
    local server="$BUILD_DIR/kvstore"
    local client="$BUILD_DIR/testcase/AOF/insert10w"

    [[ -x "$server" ]] || {
        echo "缺少可执行文件: $server" >&2
        echo "请先构建: cmake --build build --target kvstore aof_insert10w -j\"\$(nproc)\"" >&2
        exit 1
    }
    [[ -x "$client" ]] || {
        echo "缺少可执行文件: $client" >&2
        echo "请先构建: cmake --build build --target kvstore aof_insert10w -j\"\$(nproc)\"" >&2
        exit 1
    }

    if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
        echo "端口 $port 已占用，请通过 PORT 指定其他端口" >&2
        exit 1
    fi

    SERVER_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/kvstore-hash-10w.XXXXXX")"
    local run_dir="$SERVER_ROOT/run"
    local log_file="$SERVER_ROOT/server.log"
    mkdir -p "$run_dir"

    (
        cd "$run_dir"
        exec "$server" \
            --bind 127.0.0.1 \
            --port "$port" \
            --role master \
            --rdb off \
            --aof "$AOF_MODE"
    ) >"$log_file" 2>&1 &
    SERVER_PID=$!

    if ! wait_for_tcp "$port" "$SERVER_PID"; then
        cat "$log_file" >&2
        exit 1
    fi

    "$client" 127.0.0.1 "$port" | tee "$RESULT_FILE"
}

run_redis() {
    local port="${PORT:-16379}"
    local keyspace="${KEYSPACE:-100000}"
    local appendonly="no"
    local appendfsync_args=()

    for command_name in redis-server redis-cli redis-benchmark; do
        command -v "$command_name" >/dev/null 2>&1 || {
            echo "缺少命令: $command_name" >&2
            exit 1
        }
    done

    if redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1; then
        echo "端口 $port 已有 Redis 实例，请通过 PORT 指定其他端口" >&2
        exit 1
    fi

    if [[ "$AOF_MODE" == "on" ]]; then
        appendonly="yes"
        appendfsync_args=(--appendfsync everysec)
    fi

    SERVER_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/redis-hash-10w.XXXXXX")"
    REDIS_PIDFILE="$SERVER_ROOT/redis.pid"
    local log_file="$SERVER_ROOT/redis.log"

    redis-server \
        --port "$port" \
        --bind 127.0.0.1 \
        --dir "$SERVER_ROOT" \
        --save "" \
        --appendonly "$appendonly" \
        "${appendfsync_args[@]}" \
        --daemonize yes \
        --pidfile "$REDIS_PIDFILE" \
        --logfile "$log_file"

    if ! wait_for_tcp "$port" ""; then
        echo "Redis 启动失败，日志: $log_file" >&2
        exit 1
    fi

    local actual_appendonly
    actual_appendonly="$(
        redis-cli -h 127.0.0.1 -p "$port" --raw CONFIG GET appendonly |
            tail -n 1
    )"
    if [[ "$actual_appendonly" != "$appendonly" ]]; then
        echo "AOF 配置不符合预期: expected=$appendonly actual=$actual_appendonly" >&2
        exit 1
    fi

    redis-cli -h 127.0.0.1 -p "$port" FLUSHALL >/dev/null

    redis-benchmark \
        -h 127.0.0.1 \
        -p "$port" \
        -n "$DEFAULT_RECORDS" \
        -c 1 \
        -P 1 \
        -k 1 \
        -r "$keyspace" \
        -e \
        --csv \
        HSET bench:hash Teacher__rand_int__ King__rand_int__ |
        tee "$RESULT_FILE"
}

echo "TARGET=$TARGET AOF=$AOF_MODE RECORDS=$DEFAULT_RECORDS"

if [[ "$TARGET" == "kvstore" ]]; then
    run_kvstore
    QPS="$(extract_kvstore_qps "$RESULT_FILE")"
    HLEN="N/A"
    ENCODING="N/A"
else
    run_redis
    QPS="$(extract_redis_qps "$RESULT_FILE")"
    HLEN="$(redis-cli -h 127.0.0.1 -p "${PORT:-16379}" HLEN bench:hash)"
    ENCODING="$(redis-cli -h 127.0.0.1 -p "${PORT:-16379}" OBJECT ENCODING bench:hash)"
fi

if [[ -z "$QPS" ]]; then
    echo "无法解析 QPS，结果文件: $RESULT_FILE" >&2
    exit 1
fi

if [[ ! -f "$SUMMARY_FILE" ]]; then
    echo "timestamp,target,aof,records,qps,hlen,encoding,result_file" >"$SUMMARY_FILE"
fi
printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$TIMESTAMP" "$TARGET" "$AOF_MODE" "$DEFAULT_RECORDS" "$QPS" "$HLEN" "$ENCODING" \
    "$RESULT_FILE" >>"$SUMMARY_FILE"

echo "QPS=$QPS"
echo "RESULT_FILE=$RESULT_FILE"
echo "SUMMARY_FILE=$SUMMARY_FILE"
if [[ -n "$SERVER_ROOT" ]]; then
    echo "SERVER_DIR=$SERVER_ROOT"
fi
