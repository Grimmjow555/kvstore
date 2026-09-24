#!/usr/bin/env bash
#
# monitor_mem.sh —— 自动记录 ./kvstore 运行期间的物理内存与虚拟内存。
#
# 记录三个状态：
#   开始状态：进程启动后（默认立即抓取，可用 --settle 延后）的第一份采样
#   峰值状态：内核统计的峰值 VmHWM（物理）/ VmPeak（虚拟），并附采样观测到的最大值
#   结束状态：进程退出前抓到的最后一份采样
#
# 数据来源为 /proc/<pid>/status 中的 VmRSS / VmSize / VmHWM / VmPeak，单位均为 kB。
# 服务从 build/ 目录启动，确保代码中的 ../data/... 持久化路径可用。
#
# 结束方式：
#   交互运行（stdin 是终端）时按回车，即可结束服务并打印报告；
#   这种方式不发信号，不会像 Ctrl+C 那样把管道里的 tee 一起杀掉。
#   也支持 Ctrl+C（SIGINT）与 --duration 定时结束。

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$ROOT_DIR/build"
# 默认使用构建产物 build/kvstore，可用 --bin 指定其它可执行文件。
SERVER=""
SERVER_LOG="${TMPDIR:-/tmp}/kvstore-mem-monitor.$$.log"

# 默认采样间隔（秒），支持小数。
INTERVAL="0.2"
# 运行时长（秒），为空表示一直运行到进程退出或用户 Ctrl+C。
DURATION=""
# 启动后延迟多少秒再记录“开始状态”，默认避开进程刚 exec 时的瞬时值。
SETTLE="0.5"
# 采样明细 CSV 输出路径，为空表示不写文件。
OUTPUT=""
# 监控已运行的进程时使用。
ATTACH_PID=""

SERVER_PID=""
SERVER_ARGS=()
declare -i LAUNCHED=0
# attach 模式需要校验 comm，避免 PID 复用后读到别的进程。
declare -i VERIFY_COMM=0

# 关闭服务时的等待上限（秒）：先 SIGINT，再 SIGTERM，最后 SIGKILL。
INT_WAIT=5
TERM_WAIT=5

usage() {
    cat <<'EOF'
用法:
  ./monitor_mem.sh [选项] [--] [kvstore 参数...]

说明:
  启动 ./kvstore（工作目录为 build/），自动采样其物理内存与虚拟内存，
  并输出开始状态、峰值状态、结束状态。

选项:
  -i, --interval <秒>   采样间隔，默认 0.2，支持小数
  -d, --duration <秒>   运行指定秒数后自动停止服务并记录结束状态；默认不限制
  -b, --bin <路径>      指定服务可执行文件，默认 build/kvstore
  -s, --settle <秒>     启动后延迟指定秒数再记录“开始状态”，默认 0.5；
                        设为 0 则记录进程刚启动时的瞬时值
  -o, --output <文件>   把每次采样以 CSV 写入文件（表头 elapsed_s,timestamp,VmRSS_kB,VmSize_kB,VmHWM_kB,VmPeak_kB）
  -p, --pid <PID>       监控一个已经在运行的 kvstore 进程，不再启动新进程
  -h, --help            显示本帮助

示例:
  ./monitor_mem.sh 9999 0
  ./monitor_mem.sh --duration 30 --output /tmp/kvstore_mem.csv 9999 0
  ./monitor_mem.sh --pid "$(pgrep -n kvstore)"

停止方式:
  1) 交互运行（stdin 是终端）时，按回车结束服务并打印报告；
     这是推荐的结束方式：它不发信号，因而不会打断 `| tee 文件` 这样的管道。
  2) 按 Ctrl+C（SIGINT）也可以结束服务并打印报告，但 SIGINT 会同时杀掉管道里的
     tee 等进程，报告可能无法写入文件。
  3) 指定 --duration <秒> 时到点自动结束。
  以上方式都会先向服务转发 SIGINT 优雅关闭，并在关闭过程中继续采样以抓取结束状态。
EOF
}

die() {
    echo "错误: $*" >&2
    exit 1
}

# 校验“正数（可为小数）”参数。
is_positive_number() {
    awk -v v="$1" 'BEGIN { exit !(v ~ /^[0-9]+(\.[0-9]+)?$/ && v > 0) }'
}

# 校验“非负数（可为小数）”参数。
is_non_negative_number() {
    awk -v v="$1" 'BEGIN { exit !(v ~ /^[0-9]+(\.[0-9]+)?$/ && v >= 0) }'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--interval)
            [[ $# -ge 2 ]] || die "$1 缺少参数"
            INTERVAL="$2"
            shift 2
            ;;
        -d|--duration)
            [[ $# -ge 2 ]] || die "$1 缺少参数"
            DURATION="$2"
            shift 2
            ;;
        -s|--settle)
            [[ $# -ge 2 ]] || die "$1 缺少参数"
            SETTLE="$2"
            shift 2
            ;;
        -o|--output)
            [[ $# -ge 2 ]] || die "$1 缺少参数"
            OUTPUT="$2"
            shift 2
            ;;
        -b|--bin)
            [[ $# -ge 2 ]] || die "$1 缺少参数"
            SERVER="$2"
            shift 2
            ;;
        -p|--pid)
            [[ $# -ge 2 ]] || die "$1 缺少参数"
            ATTACH_PID="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            SERVER_ARGS+=("$@")
            break
            ;;
        -*)
            die "未知选项: $1（使用 --help 查看用法）"
            ;;
        *)
            SERVER_ARGS+=("$1")
            shift
            ;;
    esac
done

is_positive_number "$INTERVAL" || die "采样间隔必须是大于 0 的数字: $INTERVAL"
is_non_negative_number "$SETTLE" || die "settle 必须是大于等于 0 的数字: $SETTLE"
if [[ -n "$DURATION" ]]; then
    is_positive_number "$DURATION" || die "运行时长必须是大于 0 的数字: $DURATION"
fi
[[ -z "$ATTACH_PID" || "$ATTACH_PID" =~ ^[0-9]+$ ]] || die "--pid 必须是数字: $ATTACH_PID"
[[ -n "$SERVER" ]] || SERVER="$BUILD_DIR/kvstore"
# 启动前把相对路径转成绝对路径，因为随后会切换工作目录到 build/。
case "$SERVER" in /*) ;; *) SERVER="$PWD/$SERVER" ;; esac
case "$OUTPUT" in ""|/*) ;; *) OUTPUT="$PWD/$OUTPUT" ;; esac

# 读取 /proc/<pid>/status 中的某个字段（单位 kB），字段不存在时返回非 0。
read_status_field() {
    local pid="$1" field="$2" file="/proc/$1/status"
    [[ -r "$file" ]] || return 1
    # status 里的字段名带冒号，例如 "VmRSS:  1234 kB"。
    awk -v f="$field" '$1 == f ":" { print $2; exit }' "$file" 2>/dev/null
}

# attach 模式判断目标进程是否仍是 kvstore（顺带规避 PID 复用）。
# 自己启动的进程无需该校验：子进程未回收前 PID 不会被复用。
is_target_process() {
    local pid="$1"
    [[ -r "/proc/$pid/status" ]] || return 1
    if (( VERIFY_COMM == 0 )); then
        return 0
    fi
    local comm
    comm="$(cat "/proc/$pid/comm" 2>/dev/null || true)"
    [[ "$comm" == kvstore* ]]
}

# 采样一次，输出 "VmRSS VmSize VmHWM VmPeak"；进程已退出时返回非 0。
sample_fields() {
    local pid="$1" rss vsz hwm peak
    is_target_process "$pid" || return 1
    rss="$(read_status_field "$pid" VmRSS)" || return 1
    vsz="$(read_status_field "$pid" VmSize)" || return 1
    [[ -n "$rss" && -n "$vsz" ]] || return 1
    # VmHWM / VmPeak 理论上始终存在，缺失时退化为当前值。
    hwm="$(read_status_field "$pid" VmHWM)" || hwm="$rss"
    peak="$(read_status_field "$pid" VmPeak)" || peak="$vsz"
    printf '%s %s %s %s\n' "$rss" "$vsz" "$hwm" "$peak"
}

# kB -> "12.34 MB (12636 kB)"。
fmt_mem() {
    awk -v kb="$1" 'BEGIN { printf "%.2f MB (%d kB)", kb / 1024, kb }'
}

now_ts() {
    date +%s.%N
}

# 浮点比较：$1 >= $2 返回 0。
ge() {
    awk -v a="$1" -v b="$2" 'BEGIN { exit !(a >= b) }'
}

now_decimal() {
    awk -v a="$1" -v b="$2" 'BEGIN { printf "%.3f", a - b }'
}

STOP_REQUESTED=0

# Ctrl+C / SIGTERM 只做标记，实际关闭在采样循环中处理，以便继续采样抓结束状态。
request_stop() {
    STOP_REQUESTED=1
}

# 交互按键：只有回车表示“结束运行并输出报告”，其它按键忽略。
handle_hotkey() {
    local raw="$1"
    local key="${raw//$'\r'/}"
    key="${key//$'\n'/}"
    if [[ -z "$key" ]]; then
        echo "收到回车，结束运行并输出报告..." >&2
        STOP_REQUESTED=1
    fi
}

# 兜底清理：仅在脚本异常退出、服务进程仍存活时使用。
cleanup() {
    if (( LAUNCHED == 1 )) && [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        local i
        for i in $(seq 1 10); do
            kill -0 "$SERVER_PID" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill -KILL "$SERVER_PID" 2>/dev/null || true
        fi
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

trap request_stop INT TERM

if [[ -n "$ATTACH_PID" ]]; then
    [[ -d "/proc/$ATTACH_PID" ]] || die "进程不存在: $ATTACH_PID"
    VERIFY_COMM=1
    is_target_process "$ATTACH_PID" || die "PID $ATTACH_PID 不是 kvstore 进程（--pid 只能用于 kvstore）"
    SERVER_PID="$ATTACH_PID"
    SERVER_CMD="kvstore (attach pid=$ATTACH_PID)"
else
    [[ -x "$SERVER" ]] || die "找不到可执行文件: $SERVER（请先在 build/ 下完成构建）"
    LAUNCHED=1
    SERVER_CMD="$SERVER${SERVER_ARGS[*]:+ ${SERVER_ARGS[*]}}"
    # 必须在 build/ 目录启动，持久化路径才指向 ../data/...。
    # 这里直接后台启动而不套一层子 shell，确保 $! 就是服务进程，便于采样和发信号。
    cd "$BUILD_DIR"
    # 非交互 shell 会把后台任务设为忽略 SIGINT，而 kvstore 依赖默认信号行为退出。
    # 用 env 恢复默认处理，保证 Ctrl+C / SIGINT 能真正让服务走正常退出流程。
    LAUNCHER=()
    if env --default-signal=INT true >/dev/null 2>&1; then
        LAUNCHER=(env --default-signal=INT,QUIT)
    fi
    if (( ${#SERVER_ARGS[@]} > 0 )); then
        ${LAUNCHER[@]+"${LAUNCHER[@]}"} "$SERVER" "${SERVER_ARGS[@]}" >"$SERVER_LOG" 2>&1 &
    else
        ${LAUNCHER[@]+"${LAUNCHER[@]}"} "$SERVER" >"$SERVER_LOG" 2>&1 &
    fi
    SERVER_PID=$!
    # 只注册 EXIT，避免覆盖上面的 INT/TERM 处理。
    trap cleanup EXIT
fi

# ---- 统计变量 ----
declare -i SAMPLE_COUNT=0
FIRST_RSS="" FIRST_VSZ=""
START_RSS="" START_VSZ=""
LAST_RSS="" LAST_VSZ=""
MAX_RSS=0 MAX_VSZ=0
# 采样观测到峰值时的时刻（秒，相对启动）。
MAX_RSS_AT="0" MAX_VSZ_AT="0"
KERNEL_HWM=0 KERNEL_PEAK=0
FIRST_ELAPSED=0
START_ELAPSED=0

if [[ -n "$OUTPUT" ]]; then
    mkdir -p "$(dirname "$OUTPUT")"
    printf 'elapsed_s,timestamp,VmRSS_kB,VmSize_kB,VmHWM_kB,VmPeak_kB\n' >"$OUTPUT"
fi

BASE_TS="$(now_ts)"
SHUTDOWN_SENT=0
SHUTDOWN_TS=""
SIGNAL_STAGE=""
HOTKEY=""

# stdin 是终端时才启用按键控制（管道/重定向场景下 read 会立即 EOF）。
INTERACTIVE=0
if [[ -t 0 ]]; then
    INTERACTIVE=1
    echo "提示：按回车结束服务并打印内存报告。" >&2
fi

while true; do
    if ! FIELDS="$(sample_fields "$SERVER_PID")"; then
        break
    fi
    read -r RSS VSZ HWM PEAK <<<"$FIELDS"
    TS="$(now_ts)"
    ELAPSED="$(now_decimal "$TS" "$BASE_TS")"

    SAMPLE_COUNT+=1
    if [[ -z "$FIRST_RSS" ]]; then
        FIRST_RSS="$RSS"
        FIRST_VSZ="$VSZ"
        FIRST_ELAPSED="$ELAPSED"
    fi
    if [[ -z "$START_RSS" ]] && ge "$ELAPSED" "$SETTLE"; then
        START_RSS="$RSS"
        START_VSZ="$VSZ"
        START_ELAPSED="$ELAPSED"
    fi
    LAST_RSS="$RSS"
    LAST_VSZ="$VSZ"
    if (( RSS > MAX_RSS )); then MAX_RSS="$RSS"; MAX_RSS_AT="$ELAPSED"; fi
    if (( VSZ > MAX_VSZ )); then MAX_VSZ="$VSZ"; MAX_VSZ_AT="$ELAPSED"; fi
    if (( HWM > KERNEL_HWM )); then KERNEL_HWM="$HWM"; fi
    if (( PEAK > KERNEL_PEAK )); then KERNEL_PEAK="$PEAK"; fi

    if [[ -n "$OUTPUT" ]]; then
        printf '%s,%s,%s,%s,%s,%s\n' "$ELAPSED" "$TS" "$RSS" "$VSZ" "$HWM" "$PEAK" >>"$OUTPUT"
    fi

    # 到点自动停止，或收到 Ctrl+C。
    if (( SHUTDOWN_SENT == 0 )) && (( LAUNCHED == 1 )); then
        if [[ -n "$DURATION" ]] && ge "$ELAPSED" "$DURATION"; then
            STOP_REQUESTED=1
        fi
        if (( STOP_REQUESTED == 1 )); then
            kill -INT "$SERVER_PID" 2>/dev/null || true
            SHUTDOWN_SENT=1
            SHUTDOWN_TS="$(now_ts)"
            SIGNAL_STAGE="INT"
            echo "已向 kvstore(PID $SERVER_PID) 发送 SIGINT，正在采样关闭过程..."
        fi
    elif (( LAUNCHED == 0 )) && (( STOP_REQUESTED == 1 )); then
        # attach 模式只结束监控，不向目标进程发信号。
        echo "已停止监控（未干预 PID $SERVER_PID）。"
        break
    fi

    # 关闭阶段超时则升级信号，避免脚本卡死。
    if (( SHUTDOWN_SENT == 1 )) && kill -0 "$SERVER_PID" 2>/dev/null; then
        WAITED="$(now_decimal "$(now_ts)" "$SHUTDOWN_TS")"
        if [[ "$SIGNAL_STAGE" == "INT" ]] && ge "$WAITED" "$INT_WAIT"; then
            kill -TERM "$SERVER_PID" 2>/dev/null || true
            SIGNAL_STAGE="TERM"
            SHUTDOWN_TS="$(now_ts)"
            echo "服务未在 ${INT_WAIT}s 内退出，已发送 SIGTERM。" >&2
        elif [[ "$SIGNAL_STAGE" == "TERM" ]] && ge "$WAITED" "$TERM_WAIT"; then
            kill -KILL "$SERVER_PID" 2>/dev/null || true
            SIGNAL_STAGE="KILL"
            SHUTDOWN_TS="$(now_ts)"
            echo "服务未在 ${TERM_WAIT}s 内响应 SIGTERM，已发送 SIGKILL。" >&2
        fi
    fi

    # 等待下一个采样点：交互模式下顺带读取按键，超时则继续下一轮采样。
    if (( INTERACTIVE == 1 )); then
        if read -n 1 -t "$INTERVAL" -r HOTKEY; then
            handle_hotkey "$HOTKEY"
        fi
    else
        sleep "$INTERVAL"
    fi
done

# ---- 收集退出码 ----
EXIT_CODE="N/A"
EXIT_DESC="已停止监控（不是本脚本启动的进程）"
if (( LAUNCHED == 1 )); then
    RC=0
    wait "$SERVER_PID" 2>/dev/null || RC=$?
    EXIT_CODE="$RC"
    if (( RC == 0 )); then
        EXIT_DESC="正常退出"
    elif (( RC >= 128 )); then
        EXIT_DESC="被信号 $((RC - 128)) 终止（非优雅退出）"
    else
        EXIT_DESC="异常退出，返回码 $RC"
    fi
fi

echo
echo "================ kvstore 内存记录 ================"
echo "命令        : $SERVER_CMD"
echo "PID         : $SERVER_PID"
echo "采样次数    : $SAMPLE_COUNT（间隔 ${INTERVAL}s）"
echo

if (( SAMPLE_COUNT == 0 )); then
    echo "未采集到任何内存数据，进程可能未成功启动。"
    if [[ -f "$SERVER_LOG" ]]; then
        echo "服务输出末尾 20 行（$SERVER_LOG）："
        tail -n 20 "$SERVER_LOG" >&2 || true
    fi
    exit 1
fi

# 进程在 settle 之前就退出时，用第一份采样兜底。
if [[ -z "$START_RSS" ]]; then
    START_RSS="$FIRST_RSS"
    START_VSZ="$FIRST_VSZ"
    START_ELAPSED="$FIRST_ELAPSED"
fi

printf '开始状态    : 物理内存(VmRSS) %s | 虚拟内存(VmSize) %s\n' "$(fmt_mem "$START_RSS")" "$(fmt_mem "$START_VSZ")"
printf '              （启动后 %ss 采样，共 %s 次采样）\n' "$START_ELAPSED" "$SAMPLE_COUNT"
printf '峰值状态    : 物理内存(VmHWM) %s | 虚拟内存(VmPeak) %s\n' "$(fmt_mem "$KERNEL_HWM")" "$(fmt_mem "$KERNEL_PEAK")"
printf '              采样观测最大   %s（t=%ss）| %s（t=%ss）\n' \
    "$(fmt_mem "$MAX_RSS")" "$MAX_RSS_AT" "$(fmt_mem "$MAX_VSZ")" "$MAX_VSZ_AT"
printf '结束状态    : 物理内存(VmRSS) %s | 虚拟内存(VmSize) %s\n' "$(fmt_mem "$LAST_RSS")" "$(fmt_mem "$LAST_VSZ")"
echo "退出情况    : $EXIT_CODE（$EXIT_DESC）"
if [[ -n "$OUTPUT" ]]; then
    echo "采样明细    : $OUTPUT"
fi
if [[ -f "$SERVER_LOG" ]]; then
    echo "服务日志    : $SERVER_LOG"
fi

if (( LAUNCHED == 1 )) && [[ "$EXIT_CODE" != "0" ]]; then
    echo
    echo "服务输出末尾 20 行："
    tail -n 20 "$SERVER_LOG" || true
fi

exit 0
