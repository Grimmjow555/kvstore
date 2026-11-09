#!/usr/bin/env bash
#
# 对比 kvstore 与 Redis 的批量 HSET QPS。两边固定发送 100,000 条命令，只改变批大小。

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
TOTAL_COMMANDS=100000
RESULT_DIR="${RESULT_DIR:-${TMPDIR:-/tmp}/kvstore-redis-bench-results}"

usage() {
    cat >&2 <<EOF
用法: $0 <redis|kvstore> <batch-size>

batch-size: 10, 20, 40, 80, 160
每次测试固定发送 $TOTAL_COMMANDS 条 HSET 命令，批次数自动计算。

环境变量:
  PORT        服务端口，默认 redis=16379，kvstore=18379
  KEYSPACE    Redis 随机字段空间，默认 100000
  RESULT_DIR  结果目录，默认 ${TMPDIR:-/tmp}/kvstore-redis-bench-results
EOF
}

if (($# != 2)); then
    usage
    exit 1
fi

TARGET="$1"
BATCH_SIZE="$2"

case "$TARGET" in
    redis|kvstore) ;;
    *)
        usage
        exit 1
        ;;
esac

case "$BATCH_SIZE" in
    10|20|40|80|160) ;;
    *)
        usage
        exit 1
        ;;
esac

BATCH_COUNT=$((TOTAL_COMMANDS / BATCH_SIZE))
KEYSPACE="${KEYSPACE:-100000}"
mkdir -p "$RESULT_DIR"

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
SUMMARY_FILE="$RESULT_DIR/batch_summary.csv"
RESULT_FILE="$RESULT_DIR/batch_${TARGET}_${BATCH_SIZE}_${TIMESTAMP}.log"
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

run_kvstore_batch() {
    local port="${PORT:-18379}"
    local server="$BUILD_DIR/kvstore"
    local client="$BUILD_DIR/testcase/batchsend/batchsend_hash"

    [[ -x "$server" ]] || {
        echo "缺少可执行文件: $server" >&2
        echo "请先构建: cmake --build build --target kvstore batchsend_hash -j\"\$(nproc)\"" >&2
        exit 1
    }
    [[ -x "$client" ]] || {
        echo "缺少可执行文件: $client" >&2
        echo "请先构建: cmake --build build --target kvstore batchsend_hash -j\"\$(nproc)\"" >&2
        exit 1
    }

    if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
        echo "端口 $port 已占用，请通过 PORT 指定其他端口" >&2
        exit 1
    fi

    SERVER_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/kvstore-batch.XXXXXX")"
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
            --aof off
    ) >"$log_file" 2>&1 &
    SERVER_PID=$!

    if ! wait_for_tcp "$port" "$SERVER_PID"; then
        cat "$log_file" >&2
        exit 1
    fi

    "$client" 127.0.0.1 "$port" "$BATCH_SIZE" | tee "$RESULT_FILE"
}

run_redis_batch() {
    local port="${PORT:-16379}"

    for command_name in redis-server redis-cli redis-benchmark awk; do
        command -v "$command_name" >/dev/null 2>&1 || {
            echo "缺少命令: $command_name" >&2
            exit 1
        }
    done

    if redis-cli -h 127.0.0.1 -p "$port" PING >/dev/null 2>&1; then
        echo "端口 $port 已有 Redis 实例，请通过 PORT 指定其他端口" >&2
        exit 1
    fi

    SERVER_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/redis-batch.XXXXXX")"
    REDIS_PIDFILE="$SERVER_ROOT/redis.pid"
    local log_file="$SERVER_ROOT/redis.log"

    redis-server \
        --port "$port" \
        --bind 127.0.0.1 \
        --dir "$SERVER_ROOT" \
        --save "" \
        --appendonly no \
        --daemonize yes \
        --pidfile "$REDIS_PIDFILE" \
        --logfile "$log_file"

    if ! wait_for_tcp "$port" ""; then
        echo "Redis 启动失败，日志: $log_file" >&2
        exit 1
    fi

    redis-cli -h 127.0.0.1 -p "$port" FLUSHALL >/dev/null

    redis-benchmark \
        -h 127.0.0.1 \
        -p "$port" \
        -n "$TOTAL_COMMANDS" \
        -c 1 \
        -P "$BATCH_SIZE" \
        -k 1 \
        -r "$KEYSPACE" \
        -e \
        --csv \
        HSET bench:hash Teacher__rand_int__ King__rand_int__ |
        tee "$RESULT_FILE"

    if grep -q "Error from server" "$RESULT_FILE"; then
        echo "Redis 返回错误，结果文件: $RESULT_FILE" >&2
        exit 1
    fi
}

echo "TARGET=$TARGET BATCH_SIZE=$BATCH_SIZE BATCH_COUNT=$BATCH_COUNT TOTAL=$TOTAL_COMMANDS"

if [[ "$TARGET" == "kvstore" ]]; then
    run_kvstore_batch
    QPS="$(extract_kvstore_qps "$RESULT_FILE")"
    HLEN="N/A"
    ENCODING="N/A"
else
    run_redis_batch
    QPS="$(extract_redis_qps "$RESULT_FILE")"
    HLEN="$(redis-cli -h 127.0.0.1 -p "${PORT:-16379}" HLEN bench:hash)"
    ENCODING="$(redis-cli -h 127.0.0.1 -p "${PORT:-16379}" OBJECT ENCODING bench:hash)"
fi

if [[ -z "$QPS" ]]; then
    echo "无法解析 QPS，结果文件: $RESULT_FILE" >&2
    exit 1
fi

if [[ ! -f "$SUMMARY_FILE" ]]; then
    echo "timestamp,target,batch_size,batch_count,total,qps,hlen,encoding,result_file" \
        >"$SUMMARY_FILE"
fi
printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$TIMESTAMP" "$TARGET" "$BATCH_SIZE" "$BATCH_COUNT" "$TOTAL_COMMANDS" "$QPS" \
    "$HLEN" "$ENCODING" "$RESULT_FILE" >>"$SUMMARY_FILE"

echo "QPS=$QPS"
echo "RESULT_FILE=$RESULT_FILE"
echo "SUMMARY_FILE=$SUMMARY_FILE"
if [[ -n "$SERVER_ROOT" ]]; then
    echo "SERVER_DIR=$SERVER_ROOT"
fi
