// CaptureRuntime 的底层后端：uprobe + BPF ringbuf / 共享内存环。
//
// 重设计阶段 3：采集从 kvstore 进程搬到独立的 ebpf-agent 进程。kvstore 里只剩
// 一个挂载点 kvs_ebpf_notify_write()（见 replication/kvs_replication.cpp），本文件负责
// 把记录取出来交给 agent 的状态机（缓存 / 转发给 Replica）。
//
// 两种模式（运行期选择，见 tools/ebpf_agent.cpp 的 --mode）：
//   * uprobe（默认）：本进程建 ringbuf map、加载 BPF 程序、把 uprobe 挂到
//     master 二进制的 kvs_ebpf_notify_write 上（符号偏移从 ELF 解析），
//     再 mmap ringbuf 消费；
//   * sim：内核采集不可用时，master 把记录写进 kvs_ebpf_ring 的共享内存环，
//     本进程从环里取。两条路径对上层完全等价。
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_ebpf_agent.h"
#include "kvs_ebpf_ring.h"

#include <elf.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/bpf.h>
#include <linux/perf_event.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

// ==================== agent 侧采集：定位与阅读提示 ====================
//
// 这是复制链路里 ebpf-agent 的采集半边（设计见 docs/replication-redesign.md §8.5）。
// kvstore 进程只提供挂载点 kvs_ebpf_notify_write()（replication/kvs_replication.cpp），
// 本文件在 **独立进程** 里把记录取出来交给 agent 状态机（tools/ebpf_agent.cpp）。
//
// 两种模式（由 kvs_ebpf_agent_capture_start 的 mode 参数在运行期选择）：
//   * uprobe：建 ringbuf map -> 加载 BPF 程序 -> 从 **Master 二进制** 的 .symtab 解析
//     kvs_ebpf_notify_write 的文件偏移并挂 uprobe（perf_event_open 指定 master pid）
//     -> consumer_thread() 轮询 ringbuf；
//   * sim：Master 把记录写进共享内存环（replication/kvs_ebpf_ring.cpp），本文件打开该环
//     并轮询取记录。记录格式与内核 ringbuf 完全一致，消费代码也是同一份。
//
// 消费侧统一走 process_event()：校验序号连续性 -> 交给 g_on_record 回调。
//
// 正确性要点：
//   * ringbuf/环满都会丢记录，因此每条记录带自增 seq；process_event() 发现缺口就调用
//     g_on_resync 回调（agent 据此断开增量通道，让 Replica 重新全量同步），绝不静默丢命令；
//   * 任何 attach 失败都返回 -1，调用方（agent main）直接退出——采集是强制依赖，
//     不会再像旧实现那样回退成「写路径直接落 backlog」。

// 内核 ABI 里固定的 helper 编号。本机 linux/bpf.h 只提供 __BPF_FUNC_MAPPER，
// 不生成 BPF_FUNC_* 名字，这里按 ABI 常量补上（顺序见 __BPF_FUNC_MAPPER）。
#define KVS_BPF_FUNC_PROBE_READ_USER 112
#define KVS_BPF_FUNC_PROBE_READ_USER_STR 114
#define KVS_BPF_FUNC_RINGBUF_RESERVE 131
#define KVS_BPF_FUNC_RINGBUF_SUBMIT 132

// x86_64 struct pt_regs 中 di/si/dx 的字节偏移。
// 采集函数签名：notify(seq=di, argc=si, argv=dx)。
#define KVS_PT_REG_DI 112
#define KVS_PT_REG_SI 104
#define KVS_PT_REG_DX 96

#define KVS_UPROBE_GROUP "kvstore_repl"
#define KVS_UPROBE_EVENT "write"

#define KVS_MAX_PROG_INSNS 128
// 验证器日志初始大小。log_level=1 会打印每条指令的状态，太小会因截断返回 ENOSPC；
// prog_load() 在 ENOSPC 时会继续放大重试。
#define KVS_VERIFIER_LOG_SIZE (64u * 1024)

static int g_ringbuf_fd = -1;
static int g_prog_fd = -1;
static int g_perf_fd = -1;
static int g_running = 0;
static int g_active = 0;
static long g_page_size = 0;
static pthread_t g_consumer_tid;

// 消费线程读取的三块映射：consumer 页 / producer 页 / 数据区（双映射）。
static unsigned long long* g_cons_pos = NULL;
static unsigned long long* g_prod_pos = NULL;
static unsigned char* g_data = NULL;

// 序号水位：g_consumed_seq 由消费线程推进，g_notified_seq 保留给看门狗
// （check_capture_health）使用；uprobe 模式下没有 in-process notify，看门狗主要
// 起兜底作用，真正保证不丢命令的是 process_event() 的序号连续性检查。
static pthread_mutex_t g_seq_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_seq_cond = PTHREAD_COND_INITIALIZER;
static unsigned int g_notified_seq = 0;
static unsigned int g_consumed_seq = 0;
static unsigned int g_expected_seq = 0; // 0 表示还没收到第一条

static int g_uprobe_registered = 0;

// 运行期采集模式（KVS_EBPF_MODE_UPROBE / KVS_EBPF_MODE_SIM）。
static int g_agent_mode = KVS_EBPF_MODE_UPROBE;

// sim 模式下的采集环路径：Master 重启后需要用它重新打开新环。
static char g_agent_ring_path[512] = {0};

// agent 状态机回调：g_on_record 处理一条完整记录，g_on_resync 表示采集丢事件、
// 需要断开增量通道让 Replica 重同步（说明见下方「agent 回调」小节）。
static kvs_ebpf_record_cb_t g_on_record = NULL;
static kvs_ebpf_resync_cb_t g_on_resync = NULL;

static void agent_signal_resync(const char* reason) {
    if (g_on_resync != NULL) {
        g_on_resync(reason);
    }
}

// ==================== bpf() 系统调用与指令构造 ====================

static int bpf_syscall(enum bpf_cmd cmd, union bpf_attr* attr) {
    return (int)syscall(__NR_bpf, cmd, attr, sizeof(*attr));
}

static struct bpf_insn mk_insn(unsigned char code, unsigned char dst, unsigned char src, short off,
                               int imm) {
    struct bpf_insn in;
    memset(&in, 0, sizeof(in));
    in.code = code;
    in.dst_reg = dst;
    in.src_reg = src;
    in.off = off;
    in.imm = imm;
    return in;
}

struct prog_builder {
    struct bpf_insn insns[KVS_MAX_PROG_INSNS];
    int n;
};

static int pb_emit(struct prog_builder* b, unsigned char code, unsigned char dst, unsigned char src,
                   short off, int imm) {
    if (b->n >= KVS_MAX_PROG_INSNS) {
        return -1;
    }
    b->insns[b->n] = mk_insn(code, dst, src, off, imm);
    return b->n++;
}

static void pb_patch(struct prog_builder* b, int idx, int target) {
    b->insns[idx].off = (short)(target - idx - 1);
}

// [步骤 2 支线] 生成内核 BPF 程序（裸指令，不依赖 libbpf）：
//
//   seq  = ctx->di; argc = ctx->si; argv = ctx->dx;
//   if (argc < 1 || argc > 3) return 0;
//   rec = bpf_ringbuf_reserve(&rb, EVENT_SIZE, 0);
//   if (!rec) return 0;                       // 队列满：丢事件，由序号缺口发现
//   rec->seq = seq; rec->argc = argc; rec->arg_len[] = 0;
//   for (i = 0; i < argc; i++)
//       n = bpf_probe_read_user_str(rec->data[i], ARG_MAX, argv[i]);
//       if (n > 0) rec->arg_len[i] = n;
//   bpf_ringbuf_submit(rec, 0);
static int build_capture_program(struct prog_builder* b, int ringbuf_fd) {
    b->n = 0;

    const unsigned char reg_seq = 7, reg_argc = 8, reg_argv = 9, reg_rec = 6;

#define EMIT(C, D, S, O, I)                                                                        \
    do {                                                                                           \
        if (pb_emit(b, (C), (D), (S), (O), (I)) < 0) {                                             \
            return -1;                                                                             \
        }                                                                                          \
    } while (0)

    EMIT(BPF_ALU64 | BPF_MOV | BPF_X, 6, 1, 0, 0);                   // r6 = ctx
    EMIT(BPF_LDX | BPF_MEM | BPF_DW, reg_seq, 6, KVS_PT_REG_DI, 0);  // r7 = seq
    EMIT(BPF_LDX | BPF_MEM | BPF_DW, reg_argc, 6, KVS_PT_REG_SI, 0); // r8 = argc
    EMIT(BPF_LDX | BPF_MEM | BPF_DW, reg_argv, 6, KVS_PT_REG_DX, 0); // r9 = argv

    int j_argc_low = b->n;
    EMIT(BPF_JMP | BPF_JSLT | BPF_K, reg_argc, 0, 0, 1);
    int j_argc_high = b->n;
    EMIT(BPF_JMP | BPF_JSGT | BPF_K, reg_argc, 0, 0, KVS_EBPF_EVENT_MAX_ARGS);

    EMIT(BPF_LD | BPF_DW | BPF_IMM, 1, BPF_PSEUDO_MAP_FD, 0, ringbuf_fd);
    EMIT(0, 0, 0, 0, 0);
    EMIT(BPF_ALU64 | BPF_MOV | BPF_K, 2, 0, 0, KVS_EBPF_EVENT_SIZE);
    EMIT(BPF_ALU64 | BPF_MOV | BPF_K, 3, 0, 0, 0);
    EMIT(BPF_JMP | BPF_CALL, 0, 0, 0, KVS_BPF_FUNC_RINGBUF_RESERVE);

    int j_reserve = b->n;
    EMIT(BPF_JMP | BPF_JEQ | BPF_K, 0, 0, 0, 0);
    EMIT(BPF_ALU64 | BPF_MOV | BPF_X, reg_rec, 0, 0, 0); // r6 = rec

    EMIT(BPF_STX | BPF_MEM | BPF_W, reg_rec, reg_seq, 0, 0);  // rec->seq
    EMIT(BPF_STX | BPF_MEM | BPF_W, reg_rec, reg_argc, 4, 0); // rec->argc
    EMIT(BPF_ST | BPF_MEM | BPF_W, reg_rec, 0, 8, 0);         // arg_len[0..2] = 0
    EMIT(BPF_ST | BPF_MEM | BPF_W, reg_rec, 0, 12, 0);
    EMIT(BPF_ST | BPF_MEM | BPF_W, reg_rec, 0, 16, 0);
    EMIT(BPF_ST | BPF_MEM | BPF_W, reg_rec, 0, 20, 0);

    // argv 是从 pt_regs 读出来的标量，验证器不允许直接解引用，必须用
    // bpf_probe_read_user 先把 argv[i] 这个用户态指针读到栈上，再读字符串。
    for (int i = 0; i < KVS_EBPF_EVENT_MAX_ARGS; ++i) {
        int label_skip = 0; // 稍后回填
        int j_argc = b->n;
        EMIT(BPF_JMP | BPF_JSLT | BPF_K, reg_argc, 0, 0, i + 1); // argc < i+1 时跳过

        EMIT(BPF_ALU64 | BPF_MOV | BPF_X, 1, 10, 0, 0); // r1 = fp - 8
        EMIT(BPF_ALU64 | BPF_ADD | BPF_K, 1, 0, 0, -8);
        EMIT(BPF_ALU64 | BPF_MOV | BPF_K, 2, 0, 0, 8);
        EMIT(BPF_ALU64 | BPF_MOV | BPF_X, 3, reg_argv, 0, 0);
        EMIT(BPF_ALU64 | BPF_ADD | BPF_K, 3, 0, 0, i * 8);
        EMIT(BPF_JMP | BPF_CALL, 0, 0, 0, KVS_BPF_FUNC_PROBE_READ_USER);
        int j_ptr = b->n;
        EMIT(BPF_JMP | BPF_JNE | BPF_K, 0, 0, 0, 0); // 读指针失败 -> 跳过

        EMIT(BPF_LDX | BPF_MEM | BPF_DW, 3, 10, -8, 0); // r3 = argv[i]
        EMIT(BPF_ALU64 | BPF_MOV | BPF_X, 1, reg_rec, 0, 0);
        EMIT(BPF_ALU64 | BPF_ADD | BPF_K, 1, 0, 0,
             KVS_EBPF_EVENT_HEADER_SIZE + i * KVS_EBPF_EVENT_ARG_MAX);
        EMIT(BPF_ALU64 | BPF_MOV | BPF_K, 2, 0, 0, KVS_EBPF_EVENT_ARG_MAX);
        EMIT(BPF_JMP | BPF_CALL, 0, 0, 0, KVS_BPF_FUNC_PROBE_READ_USER_STR);
        int j_str = b->n;
        EMIT(BPF_JMP | BPF_JSLE | BPF_K, 0, 0, 0, 0); // 读失败 -> 跳过
        int j_trunc = b->n;
        EMIT(BPF_JMP | BPF_JEQ | BPF_K, 0, 0, 0, KVS_EBPF_EVENT_ARG_MAX); // 截断 -> 跳过
        EMIT(BPF_STX | BPF_MEM | BPF_W, reg_rec, 0, 8 + i * 4, 0);

        label_skip = b->n;
        pb_patch(b, j_argc, label_skip);
        pb_patch(b, j_ptr, label_skip);
        pb_patch(b, j_str, label_skip);
        pb_patch(b, j_trunc, label_skip);
    }

    // 提交
    EMIT(BPF_ALU64 | BPF_MOV | BPF_X, 1, reg_rec, 0, 0);
    EMIT(BPF_ALU64 | BPF_MOV | BPF_K, 2, 0, 0, 0);
    EMIT(BPF_JMP | BPF_CALL, 0, 0, 0, KVS_BPF_FUNC_RINGBUF_SUBMIT);

    int label_exit = b->n;
    EMIT(BPF_ALU64 | BPF_MOV | BPF_K, 0, 0, 0, 0);
    EMIT(BPF_JMP | BPF_EXIT, 0, 0, 0, 0);

    pb_patch(b, j_argc_low, label_exit);
    pb_patch(b, j_argc_high, label_exit);
    pb_patch(b, j_reserve, label_exit);

#undef EMIT
    return 0;
}

// ==================== 记录解析 ====================

// 记录编解码（kvs_ebpf_event_parse / kvs_ebpf_event_fits）已随记录格式一起
// 移到 replication/kvs_ebpf_ring.cpp，master 与 agent 共用同一份实现。

// ==================== ringbuf 消费 ====================

// 单条记录的处理：解析 -> 校验序号连续性 -> 交给 agent 状态机（缓存/转发）。
// 序号缺口意味着中间的事件被 ringbuf 丢弃，此时必须先重同步再继续。
// 已解析出 (seq, argc, argv) 之后统一走这里：校验序号连续性 -> 交给状态机。
static void process_event(unsigned int seq, int argc, char* argv[]) {
    unsigned int expected = 0;
    pthread_mutex_lock(&g_seq_mutex);
    expected = g_expected_seq;
    g_expected_seq = seq + 1;
    pthread_mutex_unlock(&g_seq_mutex);
    if (expected != 0 && seq != expected) {
        kvs_log(KVS_LOG_ERROR,
                "[EBPF] capture stream lost %u event(s) (expected seq=%u got %u), force resync",
                seq - expected, expected, seq);
        agent_signal_resync("sequence gap");
    }

    if (g_on_record != NULL) {
        g_on_record(argc, argv);
    }

    pthread_mutex_lock(&g_seq_mutex);
    g_consumed_seq = seq;
    pthread_cond_broadcast(&g_seq_cond);
    pthread_mutex_unlock(&g_seq_mutex);
}

static void dispatch_record(const unsigned char* data, unsigned int len) {
    char argbuf[KVS_EBPF_EVENT_MAX_ARGS * KVS_EBPF_EVENT_ARG_MAX];
    char* argv[KVS_EBPF_EVENT_MAX_ARGS];
    unsigned int seq = 0;

    int argc = kvs_ebpf_event_parse(data, len, &seq, argbuf, sizeof(argbuf), argv,
                                    KVS_EBPF_EVENT_MAX_ARGS);
    if (argc <= 0) {
        kvs_log(KVS_LOG_ERROR, "[EBPF] ringbuf record malformed/truncated (len=%u), force resync",
                len);
        agent_signal_resync("malformed record");
        return;
    }
    process_event(seq, argc, argv);
}

// sim 模式：从共享内存环取记录。与内核 ringbuf 路径走同一套序号校验与回调。
static void shm_ring_drain(void) {
    char argbuf[KVS_EBPF_EVENT_MAX_ARGS * KVS_EBPF_EVENT_ARG_MAX];
    char* argv[KVS_EBPF_EVENT_MAX_ARGS];

    while (1) {
        unsigned int seq = 0;
        int argc = kvs_ebpf_ring_pop(&seq, argbuf, sizeof(argbuf), argv, KVS_EBPF_EVENT_MAX_ARGS);
        if (argc == 0) {
            return; // 环空
        }
        if (argc < 0) {
            kvs_log(KVS_LOG_ERROR, "[EBPF] capture ring record malformed, force resync");
            agent_signal_resync("malformed record");
            continue;
        }
        process_event(seq, argc, argv);
    }
}

static void ringbuf_drain(void) {
    if (g_cons_pos == NULL || g_prod_pos == NULL || g_data == NULL) {
        return;
    }
    const unsigned long long mask = (unsigned long long)KVS_EBPF_RINGBUF_SIZE - 1;
    unsigned long long cons = __atomic_load_n(g_cons_pos, __ATOMIC_ACQUIRE);
    unsigned long long prod = __atomic_load_n(g_prod_pos, __ATOMIC_ACQUIRE);

    while (cons < prod) {
        unsigned char* hdr = g_data + (cons & mask);
        unsigned int len = __atomic_load_n((unsigned int*)hdr, __ATOMIC_ACQUIRE);
        if ((len & BPF_RINGBUF_BUSY_BIT) != 0) {
            break; // 生产者尚未提交，下次再来
        }
        unsigned int data_len =
            len & ~(unsigned int)(BPF_RINGBUF_BUSY_BIT | BPF_RINGBUF_DISCARD_BIT);
        if ((len & BPF_RINGBUF_DISCARD_BIT) == 0) {
            dispatch_record(hdr + 8, data_len);
        }
        cons += ((unsigned long long)data_len + 8 + 7ull) & ~7ull;
    }

    __atomic_store_n(g_cons_pos, cons, __ATOMIC_RELEASE);
}

static unsigned long long ebpf_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (unsigned long long)ts.tv_sec * 1000ull + (unsigned long long)ts.tv_nsec / 1000000ull;
}

// 看门狗：如果「已通知序号」长期领先「已消费序号」，说明采集通道卡住或者
// 尾部事件被丢弃（后面没有新记录可供比对序号）。这种情况必须重同步，不能静默丢命令。
static void check_capture_health(void) {
    static unsigned long long stuck_since_ms = 0;

    pthread_mutex_lock(&g_seq_mutex);
    unsigned int notified = g_notified_seq;
    unsigned int consumed = g_consumed_seq;
    pthread_mutex_unlock(&g_seq_mutex);

    if (consumed >= notified) {
        stuck_since_ms = 0;
        return;
    }
    unsigned long long now = ebpf_now_ms();
    if (stuck_since_ms == 0) {
        stuck_since_ms = now;
        return;
    }
    if (now - stuck_since_ms > 1000) {
        stuck_since_ms = 0;
        kvs_log(KVS_LOG_ERROR,
                "[EBPF] capture stalled or dropped events (notified=%u consumed=%u), "
                "force full resync",
                notified, consumed);
        agent_signal_resync("capture stalled");
    }
}

// 采集消费线程：agent 进程里的用户态半边。
//
// 生命周期：kvs_ebpf_agent_capture_start() 完成 attach 后创建本线程；
// ebpf_teardown() / kvs_ebpf_agent_capture_stop() 通过清零 g_running 并 pthread_join
// 请求本线程退出。
//
// 每轮循环：
//   1. uprobe 模式：poll ringbuf map fd（100ms 超时）后 ringbuf_drain()；
//      sim 模式：1ms 短睡眠后 shm_ring_drain()（没有可 poll 的内核 fd）。
//   2. 两种 drain 最终都走 process_event()：校验序号连续性 -> g_on_record 回调
//      （agent 状态机据此缓存或转发）。
//   3. check_capture_health()：采集卡住或尾部事件被丢弃时触发 g_on_resync。
//
// 本线程必须保持轻量：消费不及时会让 ringbuf/环被填满并丢记录，虽然序号缺口能触发
// 重同步，但会造成一次不必要的全量同步。
static void* consumer_thread(void*) {
    while (__atomic_load_n(&g_running, __ATOMIC_RELAXED)) {
        if (g_agent_mode == KVS_EBPF_MODE_SIM) {
            // sim：master 直接写共享内存环，这里短睡眠轮询。
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 1000000L;
            nanosleep(&ts, NULL);
            shm_ring_drain();
        } else if (g_ringbuf_fd >= 0) {
            // 真实内核路径：等待 ringbuf 可读。即使没有新事件，100ms 超时也能让循环
            // 周期性执行看门狗检查。
            struct pollfd pfd;
            pfd.fd = g_ringbuf_fd;
            pfd.events = POLLIN;
            pfd.revents = 0;
            poll(&pfd, 1, 100);
            ringbuf_drain();
        }
        check_capture_health();
    }
    // 收到停止请求后再排空一次，消费掉退出瞬间已经提交到 ringbuf 的尾部记录。
    if (g_agent_mode == KVS_EBPF_MODE_SIM) {
        shm_ring_drain();
    } else {
        ringbuf_drain();
    }
    return NULL;
}

// ==================== agent 回调 ====================
//
// 采集侧只负责「把记录按顺序取出来」，记录怎么用由 agent 状态机决定：
//   * g_on_record：一条完整记录（写命令 / SYNC / SYNC FINISHED）；
//   * g_on_resync：采集丢事件或记录损坏，需要重同步（agent 断开增量通道）。
//
// 挂载点 kvs_ebpf_notify_write() 本身在 master 进程里（replication/kvs_replication.cpp），
// 本文件通过 uprobe 读它的参数，不定义该符号。

// ==================== 真实内核路径：uprobe + ringbuf attach ====================

// 从目标二进制（master）的 ELF 里解析符号的文件偏移。
//
// agent 是独立进程，不能用 dl_iterate_phdr 解析「自己」的符号，必须读 master
// 二进制的 .symtab 拿到符号的虚拟地址，再按 PT_LOAD 段换算成 uprobe 需要的
// 文件偏移。注意 master 二进制不能被 strip，否则 .symtab 缺失、这里会失败。
static int resolve_symbol_offset_in_elf(const char* path, const char* symbol,
                                        unsigned long* out_offset) {
    if (path == NULL || symbol == NULL || out_offset == NULL) {
        return -1;
    }

    void* map = NULL;
    size_t size = 0;
    const unsigned char* base = NULL;
    const Elf64_Shdr* symtab = NULL;
    const Elf64_Shdr* strtab = NULL;
    unsigned long sym_vaddr = 0;
    size_t sym_count = 0;
    const Elf64_Sym* syms = NULL;
    const char* strs = NULL;
    const Elf64_Ehdr* eh = NULL;
    const Elf64_Shdr* shdrs = NULL;
    const char* name = NULL;
    int found = 0;
    int rc = -1;

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] open %s failed (errno=%d, %s)", path, errno, strerror(errno));
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size <= 0) {
        close(fd);
        return -1;
    }

    map = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        return -1;
    }

    base = (const unsigned char*)map;
    size = (size_t)st.st_size;

    if (size < sizeof(Elf64_Ehdr) || memcmp(base, ELFMAG, SELFMAG) != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] %s is not an ELF file", path);
        goto done;
    }

    eh = (const Elf64_Ehdr*)base;
    if (eh->e_shoff == 0 || eh->e_shnum == 0 || eh->e_shentsize != sizeof(Elf64_Shdr) ||
        (size_t)eh->e_shoff + (size_t)eh->e_shnum * sizeof(Elf64_Shdr) > size) {
        kvs_log(KVS_LOG_WARN, "[EBPF] %s has no usable section table", path);
        goto done;
    }

    shdrs = (const Elf64_Shdr*)(base + eh->e_shoff);
    for (int i = 0; i < eh->e_shnum; ++i) {
        if (shdrs[i].sh_type == SHT_SYMTAB) {
            symtab = &shdrs[i];
            if (shdrs[i].sh_link < eh->e_shnum) {
                strtab = &shdrs[shdrs[i].sh_link];
            }
            break;
        }
    }
    if (symtab == NULL || strtab == NULL) {
        kvs_log(KVS_LOG_WARN, "[EBPF] %s has no .symtab (stripped?), cannot locate %s", path,
                symbol);
        goto done;
    }

    sym_count = symtab->sh_size / sizeof(Elf64_Sym);
    syms = (const Elf64_Sym*)(base + symtab->sh_offset);
    strs = (const char*)(base + strtab->sh_offset);

    for (size_t i = 0; i < sym_count; ++i) {
        name = strs + syms[i].st_name;
        if (syms[i].st_name != 0 && ELF64_ST_TYPE(syms[i].st_info) == STT_FUNC &&
            strcmp(name, symbol) == 0) {
            sym_vaddr = (unsigned long)syms[i].st_value;
            found = 1;
            break;
        }
    }
    if (!found) {
        kvs_log(KVS_LOG_WARN, "[EBPF] symbol %s not found in %s", symbol, path);
        goto done;
    }

    // 虚拟地址 -> 文件偏移：找到包含该地址的 PT_LOAD 段。
    for (int i = 0; i < eh->e_phnum; ++i) {
        const Elf64_Phdr* ph =
            (const Elf64_Phdr*)(base + eh->e_phoff + (size_t)i * eh->e_phentsize);
        if (ph->p_type != PT_LOAD || ph->p_memsz == 0) {
            continue;
        }
        if (sym_vaddr >= ph->p_vaddr && sym_vaddr < ph->p_vaddr + ph->p_memsz) {
            *out_offset = (unsigned long)(sym_vaddr - ph->p_vaddr + ph->p_offset);
            rc = 0;
            break;
        }
    }
    if (rc != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] cannot map %s vaddr=0x%lx to a file offset in %s", symbol,
                sym_vaddr, path);
    }

done:
    munmap(map, (size_t)st.st_size);
    return rc;
}

static const char* tracefs_root(void) {
    static const char* cached = NULL;
    if (cached != NULL) {
        return cached;
    }
    if (access("/sys/kernel/tracing/uprobe_events", W_OK) == 0) {
        cached = "/sys/kernel/tracing";
    } else if (access("/sys/kernel/debug/tracing/uprobe_events", W_OK) == 0) {
        cached = "/sys/kernel/debug/tracing";
    }
    return cached;
}

static void uprobe_unregister(void) {
    const char* root = tracefs_root();
    if (root == NULL) {
        return;
    }
    char path[256];
    snprintf(path, sizeof(path), "%s/uprobe_events", root);
    int fd = open(path, O_WRONLY | O_APPEND);
    if (fd < 0) {
        return;
    }
    char cmd[128];
    int n = snprintf(cmd, sizeof(cmd), "-:%s/%s\n", KVS_UPROBE_GROUP, KVS_UPROBE_EVENT);
    if (n > 0) {
        ssize_t ignored = write(fd, cmd, (size_t)n);
        (void)ignored;
    }
    close(fd);
}

static int uprobe_register(const char* exe, unsigned long offset, int* out_id) {
    const char* root = tracefs_root();
    if (root == NULL) {
        kvs_log(KVS_LOG_WARN,
                "[EBPF] tracefs not writable (/sys/kernel/tracing or debugfs); uprobe attach "
                "unavailable");
        return -1;
    }

    char path[256];
    snprintf(path, sizeof(path), "%s/uprobe_events", root);
    int fd = open(path, O_WRONLY | O_APPEND);
    if (fd < 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] open %s failed (errno=%d, %s); uprobe attach unavailable",
                path, errno, strerror(errno));
        return -1;
    }

    char cmd[512];
    int n = snprintf(cmd, sizeof(cmd), "-:%s/%s\n", KVS_UPROBE_GROUP, KVS_UPROBE_EVENT);
    if (n > 0) {
        ssize_t ignored = write(fd, cmd, (size_t)n); // 清掉上一次的残留，失败无所谓
        (void)ignored;
    }
    n = snprintf(cmd, sizeof(cmd), "p:%s/%s %s:0x%lx\n", KVS_UPROBE_GROUP, KVS_UPROBE_EVENT, exe,
                 offset);
    if (n <= 0 || write(fd, cmd, (size_t)n) < 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] register uprobe %s failed (errno=%d, %s)", cmd, errno,
                strerror(errno));
        close(fd);
        return -1;
    }
    close(fd);

    char id_path[256];
    snprintf(id_path, sizeof(id_path), "%s/events/%s/%s/id", root, KVS_UPROBE_GROUP,
             KVS_UPROBE_EVENT);
    fd = open(id_path, O_RDONLY);
    if (fd < 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] open %s failed (errno=%d, %s)", id_path, errno,
                strerror(errno));
        uprobe_unregister();
        return -1;
    }
    char buf[32];
    ssize_t got = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (got <= 0) {
        uprobe_unregister();
        return -1;
    }
    buf[got] = '\0';
    *out_id = atoi(buf);
    if (*out_id <= 0) {
        uprobe_unregister();
        return -1;
    }
    return 0;
}

// 打开一个 perf 事件并挂上 BPF 程序。pid/cpu 组合按「本进程优先」逐个尝试。
// agent 是独立进程，必须显式指定要观测的 master pid：先试目标进程，再退回
// 「所有进程 + CPU0」（uprobe 挂在 master 二进制上，其它进程不会命中该探针）。
static int perf_open_and_attach(struct perf_event_attr* pe, int prog_fd, int target_pid,
                                const char* what) {
    int pids[2] = {target_pid, -1};
    int cpus[2] = {-1, 0};
    int fd = -1;
    int last_errno = 0;
    for (int i = 0; i < 2 && fd < 0; ++i) {
        if (i == 0 && target_pid <= 0) {
            continue;
        }
        fd = (int)syscall(__NR_perf_event_open, pe, pids[i], cpus[i], -1, PERF_FLAG_FD_CLOEXEC);
        if (fd < 0) {
            last_errno = errno;
        }
    }
    if (fd < 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] perf_event_open(%s) failed (errno=%d, %s)", what, last_errno,
                strerror(last_errno));
        return -1;
    }
    if (ioctl(fd, PERF_EVENT_IOC_SET_BPF, (unsigned long)prog_fd) != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] PERF_EVENT_IOC_SET_BPF(%s) failed (errno=%d, %s)", what,
                errno, strerror(errno));
        close(fd);
        return -1;
    }
    if (ioctl(fd, PERF_EVENT_IOC_ENABLE, 0) != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] PERF_EVENT_IOC_ENABLE(%s) failed (errno=%d, %s)", what, errno,
                strerror(errno));
        close(fd);
        return -1;
    }
    return fd;
}

static int read_sysfs_int(const char* path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    char buf[32];
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) {
        return -1;
    }
    buf[n] = '\0';
    return atoi(buf);
}

// 路径一（首选）：动态 uprobe PMU，和 libbpf 一样直接用 perf_event_open 建 uprobe，
// 不写 tracefs，因此只需要 capability，不需要 root、也不依赖 bpffs。
static int uprobe_attach_pmu(const char* exe, unsigned long offset, int prog_fd, int target_pid) {
    int type = read_sysfs_int("/sys/bus/event_source/devices/uprobe/type");
    if (type <= 0) {
        return -1; // 老内核走 tracefs 回退
    }

    struct perf_event_attr pe;
    memset(&pe, 0, sizeof(pe));
    pe.type = (uint32_t)type;
    pe.size = sizeof(pe);
    pe.config = 0; // 0 = 入口探针，1 = kretprobe/uprobe return
    pe.uprobe_path = (uint64_t)(uintptr_t)exe;
    pe.probe_offset = offset;
    pe.sample_period = 1;
    pe.wakeup_events = 1;

    int fd = perf_open_and_attach(&pe, prog_fd, target_pid, "uprobe-pmu");
    if (fd >= 0) {
        kvs_log(KVS_LOG_INFO, "[EBPF] uprobe attached via perf PMU: %s:0x%lx", exe, offset);
    }
    return fd;
}

// 路径二（回退）：老内核没有 uprobe PMU 时，用 tracefs 注册 uprobe + tracepoint 事件。
static int uprobe_attach_tracefs(const char* exe, unsigned long offset, int prog_fd,
                                 int target_pid) {
    int tracepoint_id = 0;
    if (uprobe_register(exe, offset, &tracepoint_id) != 0) {
        return -1;
    }
    g_uprobe_registered = 1;

    struct perf_event_attr pe;
    memset(&pe, 0, sizeof(pe));
    pe.type = PERF_TYPE_TRACEPOINT;
    pe.size = sizeof(pe);
    pe.config = (unsigned long long)tracepoint_id;
    pe.sample_period = 1;
    pe.wakeup_events = 1;

    int fd = perf_open_and_attach(&pe, prog_fd, target_pid, "uprobe-tracefs");
    if (fd < 0) {
        uprobe_unregister();
        g_uprobe_registered = 0;
        return -1;
    }
    kvs_log(KVS_LOG_INFO, "[EBPF] uprobe attached via tracefs: %s:0x%lx", exe, offset);
    return fd;
}

static int ringbuf_map_create(void) {
    union bpf_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.map_type = BPF_MAP_TYPE_RINGBUF;
    attr.max_entries = KVS_EBPF_RINGBUF_SIZE;
    int fd = bpf_syscall(BPF_MAP_CREATE, &attr);
    if (fd < 0) {
        kvs_log(KVS_LOG_WARN,
                "[EBPF] create ringbuf map failed (errno=%d, %s); uprobe mode needs CAP_BPF "
                "(larger ringbuf also needs memlock) — use ebpf_capture_mode sim instead",
                errno, strerror(errno));
    }
    return fd;
}

// 一次 BPF_PROG_LOAD。log_level=0 时不带日志缓冲区（内核要求 log_buf/log_size 同时为 0）。
static int prog_try_load(const struct prog_builder* b, char* log_buf, size_t log_size,
                         unsigned int log_level) {
    union bpf_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.prog_type = BPF_PROG_TYPE_KPROBE;
    attr.insn_cnt = (uint32_t)b->n;
    attr.insns = (uint64_t)(uintptr_t)b->insns;
    attr.license = (uint64_t)(uintptr_t) "GPL";
    if (log_level != 0 && log_buf != NULL) {
        memset(log_buf, 0, log_size);
        attr.log_buf = (uint64_t)(uintptr_t)log_buf;
        attr.log_size = log_size;
        attr.log_level = log_level;
    }
    return bpf_syscall(BPF_PROG_LOAD, &attr);
}

static int prog_load(int ringbuf_fd) {
    struct prog_builder b;
    if (build_capture_program(&b, ringbuf_fd) != 0) {
        kvs_log(KVS_LOG_ERROR, "[EBPF] build capture program failed (too many instructions)");
        return -1;
    }

    // 先不带日志加载：成功时不需要日志，也省掉内核打印每条指令状态的开销。
    int fd = prog_try_load(&b, NULL, 0, 0);
    if (fd >= 0) {
        kvs_log(KVS_LOG_INFO, "[EBPF] kprobe program loaded (%d insns)", b.n);
        return fd;
    }

    // 失败时再带日志重试，拿完整原因。注意 log_level=1 会打印每条指令的寄存器状态，
    // 缓冲区太小的话内核会直接返回 ENOSPC（即使程序本身没问题），所以像 libbpf 一样
    // 在 ENOSPC 时放大缓冲区重试。
    size_t log_size = KVS_VERIFIER_LOG_SIZE;
    char* log_buf = (char*)malloc(log_size);
    if (log_buf == NULL) {
        kvs_log(KVS_LOG_WARN, "[EBPF] load kprobe program failed (errno=%d, %s)", errno,
                strerror(errno));
        return -1;
    }

    int err = 0;
    for (int attempt = 0; attempt < 4; ++attempt) {
        fd = prog_try_load(&b, log_buf, log_size, 1);
        if (fd >= 0) {
            break;
        }
        err = errno;
        if (err == ENOSPC && log_size < (4u << 20)) {
            size_t bigger = log_size * 4;
            char* grown = (char*)realloc(log_buf, bigger);
            if (grown != NULL) {
                log_buf = grown;
                log_size = bigger;
                continue;
            }
        }
        break;
    }

    int loaded = (fd >= 0);
    if (loaded) {
        kvs_log(KVS_LOG_INFO, "[EBPF] kprobe program loaded (%d insns)", b.n);
    } else if (log_buf[0] != '\0') {
        // 验证器拒绝时内核返回的 errno 通常就是 EACCES，容易误判成「权限不足」，
        // 这里把 errno 和完整日志一起打出来。
        kvs_log(KVS_LOG_WARN, "[EBPF] kprobe program rejected by verifier (errno=%d): %s", err,
                log_buf);
    } else {
        kvs_log(KVS_LOG_WARN, "[EBPF] load kprobe program failed (errno=%d, %s)", err,
                strerror(err));
    }
    free(log_buf);
    return loaded ? fd : -1;
}

static int ringbuf_mmap(int map_fd) {
    g_page_size = sysconf(_SC_PAGESIZE);
    if (g_page_size <= 0) {
        return -1;
    }
    g_cons_pos = (unsigned long long*)mmap(NULL, (size_t)g_page_size, PROT_READ | PROT_WRITE,
                                           MAP_SHARED, map_fd, 0);
    g_prod_pos = (unsigned long long*)mmap(NULL, (size_t)g_page_size, PROT_READ, MAP_SHARED, map_fd,
                                           (off_t)g_page_size);
    g_data = (unsigned char*)mmap(NULL, 2u * KVS_EBPF_RINGBUF_SIZE, PROT_READ, MAP_SHARED, map_fd,
                                  (off_t)(2 * g_page_size));
    if (g_cons_pos == MAP_FAILED || g_prod_pos == MAP_FAILED || g_data == MAP_FAILED) {
        kvs_log(KVS_LOG_WARN, "[EBPF] mmap ringbuf failed (errno=%d, %s)", errno, strerror(errno));
        if (g_cons_pos != MAP_FAILED && g_cons_pos != NULL)
            munmap(g_cons_pos, (size_t)g_page_size);
        if (g_prod_pos != MAP_FAILED && g_prod_pos != NULL)
            munmap(g_prod_pos, (size_t)g_page_size);
        if (g_data != MAP_FAILED && g_data != NULL)
            munmap(g_data, 2u * KVS_EBPF_RINGBUF_SIZE);
        g_cons_pos = NULL;
        g_prod_pos = NULL;
        g_data = NULL;
        return -1;
    }
    return 0;
}

// ==================== 启动 / 停止 ====================

static void ebpf_teardown(void) {
    if (__atomic_load_n(&g_running, __ATOMIC_RELAXED)) {
        __atomic_store_n(&g_running, 0, __ATOMIC_RELAXED);
        pthread_join(g_consumer_tid, NULL);
    }
    if (g_perf_fd >= 0) {
        ioctl(g_perf_fd, PERF_EVENT_IOC_DISABLE, 0);
        close(g_perf_fd);
        g_perf_fd = -1;
    }
    if (g_uprobe_registered) {
        uprobe_unregister();
        g_uprobe_registered = 0;
    }
    if (g_prog_fd >= 0) {
        close(g_prog_fd);
        g_prog_fd = -1;
    }
    if (g_ringbuf_fd >= 0) {
        close(g_ringbuf_fd);
        g_ringbuf_fd = -1;
    }
    if (g_cons_pos != NULL) {
        munmap(g_cons_pos, (size_t)g_page_size);
        g_cons_pos = NULL;
    }
    if (g_prod_pos != NULL) {
        munmap(g_prod_pos, (size_t)g_page_size);
        g_prod_pos = NULL;
    }
    if (g_data != NULL) {
        munmap(g_data, 2u * KVS_EBPF_RINGBUF_SIZE);
        g_data = NULL;
    }
    pthread_mutex_lock(&g_seq_mutex);
    g_notified_seq = 0;
    g_consumed_seq = 0;
    g_expected_seq = 0;
    pthread_mutex_unlock(&g_seq_mutex);
    __atomic_store_n(&g_active, 0, __ATOMIC_RELAXED);
}

// 启用采集：uprobe 模式建 ringbuf map -> 加载校验 BPF 程序 -> 把 uprobe 挂到
// master 二进制的 kvs_ebpf_notify_write 上；sim 模式打开共享内存环。
// 任何一步失败都返回 -1（eBPF 强制：调用方据此拒绝全量同步）。
int kvs_ebpf_agent_capture_start(int mode, const char* ring_path, int master_pid,
                                 const char* master_binary, kvs_ebpf_record_cb_t on_record,
                                 kvs_ebpf_resync_cb_t on_resync) {
    if (__atomic_load_n(&g_active, __ATOMIC_RELAXED)) {
        return 0;
    }

    g_on_record = on_record;
    g_on_resync = on_resync;
    g_agent_mode = (mode == KVS_EBPF_MODE_SIM) ? KVS_EBPF_MODE_SIM : KVS_EBPF_MODE_UPROBE;

    if (g_agent_mode == KVS_EBPF_MODE_SIM) {
        // sim：内核采集不可用时由 master 直接写共享内存环，agent 只负责读。
        if (kvs_ebpf_ring_open(ring_path) != 0) {
            kvs_log(KVS_LOG_ERROR, "[EBPF] open capture ring %s failed",
                    ring_path ? ring_path : "");
            return -1;
        }
        snprintf(g_agent_ring_path, sizeof(g_agent_ring_path), "%s", ring_path ? ring_path : "");
        __atomic_store_n(&g_active, 1, __ATOMIC_RELAXED);
        __atomic_store_n(&g_running, 1, __ATOMIC_RELAXED);
        if (pthread_create(&g_consumer_tid, NULL, consumer_thread, NULL) != 0) {
            __atomic_store_n(&g_running, 0, __ATOMIC_RELAXED);
            __atomic_store_n(&g_active, 0, __ATOMIC_RELAXED);
            kvs_ebpf_ring_close();
            return -1;
        }
        kvs_log(KVS_LOG_INFO, "[EBPF] sim capture active: shared ring %s", ring_path);
        return 0;
    }

#if !defined(__x86_64__)
    kvs_log(KVS_LOG_WARN, "[EBPF] unsupported architecture for uprobe capture");
    return -1;
#else
    if (master_pid <= 0 || master_binary == NULL || master_binary[0] == '\0') {
        kvs_log(KVS_LOG_ERROR, "[EBPF] uprobe capture needs --pid and --binary");
        return -1;
    }

    g_ringbuf_fd = ringbuf_map_create();
    if (g_ringbuf_fd < 0) {
        return -1;
    }

    g_prog_fd = prog_load(g_ringbuf_fd);
    if (g_prog_fd < 0) {
        ebpf_teardown();
        return -1;
    }

    // agent 是独立进程：符号偏移从 master 二进制解析，探针只挂到 master 进程。
    unsigned long offset = 0;
    if (resolve_symbol_offset_in_elf(master_binary, "kvs_ebpf_notify_write", &offset) != 0) {
        ebpf_teardown();
        return -1;
    }

    // 首选 uprobe PMU（不需要 tracefs/root），失败再退回 tracefs 注册。
    g_perf_fd = uprobe_attach_pmu(master_binary, offset, g_prog_fd, master_pid);
    if (g_perf_fd < 0) {
        g_perf_fd = uprobe_attach_tracefs(master_binary, offset, g_prog_fd, master_pid);
    }
    if (g_perf_fd < 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] attach uprobe to pid=%d failed", master_pid);
        ebpf_teardown();
        return -1;
    }

    if (ringbuf_mmap(g_ringbuf_fd) != 0) {
        ebpf_teardown();
        return -1;
    }

    __atomic_store_n(&g_running, 1, __ATOMIC_RELAXED);
    if (pthread_create(&g_consumer_tid, NULL, consumer_thread, NULL) != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] create consumer thread failed");
        ebpf_teardown();
        return -1;
    }

    kvs_log(KVS_LOG_INFO, "[EBPF] uprobe capture active: %s:0x%lx pid=%d", master_binary, offset,
            master_pid);
    return 0;
#endif // __x86_64__
}

void kvs_ebpf_agent_capture_stop(void) {
    if (!__atomic_load_n(&g_active, __ATOMIC_RELAXED) &&
        !__atomic_load_n(&g_running, __ATOMIC_RELAXED)) {
        return;
    }
    ebpf_teardown();
    if (g_agent_mode == KVS_EBPF_MODE_SIM) {
        kvs_ebpf_ring_close(); // 消费方不删除环文件（由 master 负责）
    }
}

int kvs_ebpf_agent_capture_reopen(const char* ring_path) {
    if (g_agent_mode != KVS_EBPF_MODE_SIM) {
        kvs_log(KVS_LOG_ERROR,
                "[EBPF] master restarted; uprobe is bound to the old process and cannot be "
                "re-attached without a new --pid — restart ebpf-agent");
        return -1;
    }

    // 停消费线程 -> 重开新环 -> 重启消费线程。新 Master 会重建环文件，
    // 旧映射指向已删除的 inode，必须整个换掉。
    if (__atomic_load_n(&g_running, __ATOMIC_RELAXED)) {
        __atomic_store_n(&g_running, 0, __ATOMIC_RELAXED);
        pthread_join(g_consumer_tid, NULL);
    }
    kvs_ebpf_ring_close();

    const char* path = (ring_path != NULL && ring_path[0] != '\0') ? ring_path : g_agent_ring_path;
    if (kvs_ebpf_ring_open(path) != 0) {
        kvs_log(KVS_LOG_ERROR, "[EBPF] reopen capture ring %s failed", path != NULL ? path : "");
        __atomic_store_n(&g_active, 0, __ATOMIC_RELAXED);
        return -1;
    }
    snprintf(g_agent_ring_path, sizeof(g_agent_ring_path), "%s", path != NULL ? path : "");

    // 新环序号从 0 开始，水位一起重置，避免把旧水位当成缺口。
    pthread_mutex_lock(&g_seq_mutex);
    g_notified_seq = 0;
    g_consumed_seq = 0;
    g_expected_seq = 0;
    pthread_mutex_unlock(&g_seq_mutex);

    __atomic_store_n(&g_active, 1, __ATOMIC_RELAXED);
    __atomic_store_n(&g_running, 1, __ATOMIC_RELAXED);
    if (pthread_create(&g_consumer_tid, NULL, consumer_thread, NULL) != 0) {
        __atomic_store_n(&g_running, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&g_active, 0, __ATOMIC_RELAXED);
        return -1;
    }
    kvs_log(KVS_LOG_INFO, "[EBPF] capture reopened on %s (master restarted)", path);
    return 0;
}
