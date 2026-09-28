// 实时增量采集：kprobe/uprobe + BPF ring buffer。
//
// 不依赖 libbpf/clang/bpftool：程序用裸 bpf() 系统调用加载，uprobe 通过 tracefs
// 注册，ringbuf 直接用 mmap 消费。这样在只装了 gcc 的机器上也能构建。
//
// 编译期开关 KVS_EBPF_SIM 会把「内核部分」替换成用户态 ringbuf 模拟，用于在
// 没有 CAP_BPF 的环境（容器/CI）里验证事件采集 -> ringbuf -> 消费 -> 复制链路；
// 生产构建不定义该宏，走真实内核路径。
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "kvs_ebpf.h"
#include "kvs_config.h"
#include "kvs_replication.h"

#include <errno.h>
#include <fcntl.h>
#include <link.h>
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

// 序号水位：notify 侧写入 g_notified_seq，消费线程写入 g_consumed_seq。
// 两者配合用于「全量快照前先排空采集通道」以及「超大命令直写 backlog 前的排序」。
static pthread_mutex_t g_seq_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_seq_cond = PTHREAD_COND_INITIALIZER;
static unsigned int g_notified_seq = 0;
static unsigned int g_consumed_seq = 0;
static unsigned int g_expected_seq = 0; // 0 表示还没收到第一条

static int g_uprobe_registered = 0;

#ifdef KVS_EBPF_SIM
// 用户态模拟模式下的记录投放（真实内核模式由挂在内核里的程序完成）。
static void sim_emit_record(unsigned int seq, int argc, char* argv[]);
#endif

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

// 生成采集程序：
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

#define EMIT(C, D, S, O, I)                              \
    do {                                                 \
        if (pb_emit(b, (C), (D), (S), (O), (I)) < 0) {   \
            return -1;                                   \
        }                                                \
    } while (0)

    EMIT(BPF_ALU64 | BPF_MOV | BPF_X, 6, 1, 0, 0);                        // r6 = ctx
    EMIT(BPF_LDX | BPF_MEM | BPF_DW, reg_seq, 6, KVS_PT_REG_DI, 0);       // r7 = seq
    EMIT(BPF_LDX | BPF_MEM | BPF_DW, reg_argc, 6, KVS_PT_REG_SI, 0);      // r8 = argc
    EMIT(BPF_LDX | BPF_MEM | BPF_DW, reg_argv, 6, KVS_PT_REG_DX, 0);      // r9 = argv

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

int kvs_ebpf_event_parse(const unsigned char* data, unsigned int len, unsigned int* out_seq,
                         char* argbuf, size_t argbuf_size, char* argv[], int max_args) {
    if (data == NULL || argbuf == NULL || argv == NULL || len < KVS_EBPF_EVENT_HEADER_SIZE) {
        return -1;
    }

    uint32_t seq = 0;
    uint32_t argc = 0;
    memcpy(&seq, data, 4);
    memcpy(&argc, data + 4, 4);
    if (argc == 0 || argc > (uint32_t)KVS_EBPF_EVENT_MAX_ARGS || (int)argc > max_args) {
        return -1;
    }

    size_t used = 0;
    for (uint32_t i = 0; i < argc; ++i) {
        uint32_t alen = 0;
        memcpy(&alen, data + 8 + i * 4, 4);
        if (alen == 0 || alen > KVS_EBPF_EVENT_ARG_MAX) {
            return -1;
        }
        size_t off = KVS_EBPF_EVENT_HEADER_SIZE + (size_t)i * KVS_EBPF_EVENT_ARG_MAX;
        if (off + alen > len || used + alen > argbuf_size) {
            return -1;
        }
        memcpy(argbuf + used, data + off, alen);
        if (argbuf[used + alen - 1] != '\0') {
            return -1; // 记录被截断：宁可重同步也不能复制半条命令
        }
        argv[i] = argbuf + used;
        used += alen;
    }

    if (out_seq != NULL) {
        *out_seq = seq;
    }
    return (int)argc;
}

int kvs_ebpf_event_fits(int argc, char* argv[]) {
    if (argc < 1 || argc > KVS_EBPF_EVENT_MAX_ARGS || argv == NULL) {
        return 0;
    }
    for (int i = 0; i < argc; ++i) {
        if (argv[i] == NULL) {
            return 0;
        }
        size_t n = strlen(argv[i]) + 1; // 含结尾 '\0'
        if (n > KVS_EBPF_EVENT_ARG_MAX) {
            return 0;
        }
    }
    return 1;
}

// ==================== ringbuf 消费 ====================

static void dispatch_record(const unsigned char* data, unsigned int len) {
    char argbuf[KVS_EBPF_EVENT_MAX_ARGS * KVS_EBPF_EVENT_ARG_MAX];
    char* argv[KVS_EBPF_EVENT_MAX_ARGS];
    unsigned int seq = 0;

    int argc = kvs_ebpf_event_parse(data, len, &seq, argbuf, sizeof(argbuf), argv,
                                    KVS_EBPF_EVENT_MAX_ARGS);
    if (argc <= 0) {
        kvs_log(KVS_LOG_ERROR,
                "[EBPF] ringbuf record malformed/truncated (len=%u), force full resync", len);
        kvs_replication_resync();
        return;
    }

    unsigned int expected = 0;
    pthread_mutex_lock(&g_seq_mutex);
    expected = g_expected_seq;
    g_expected_seq = seq + 1;
    pthread_mutex_unlock(&g_seq_mutex);
    if (expected != 0 && seq != expected) {
        kvs_log(KVS_LOG_ERROR,
                "[EBPF] ringbuf lost %u event(s) (expected seq=%u got %u), force full resync",
                seq - expected, expected, seq);
        kvs_replication_resync();
    }

    kvs_replication_on_captured_command(argc, argv);

    pthread_mutex_lock(&g_seq_mutex);
    g_consumed_seq = seq;
    pthread_cond_broadcast(&g_seq_cond);
    pthread_mutex_unlock(&g_seq_mutex);
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
        unsigned int data_len = len & ~(unsigned int)(BPF_RINGBUF_BUSY_BIT | BPF_RINGBUF_DISCARD_BIT);
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
        kvs_replication_resync();
    }
}

static void* consumer_thread(void*) {
    while (__atomic_load_n(&g_running, __ATOMIC_RELAXED)) {
        if (g_ringbuf_fd >= 0) {
            struct pollfd pfd;
            pfd.fd = g_ringbuf_fd;
            pfd.events = POLLIN;
            pfd.revents = 0;
            poll(&pfd, 1, 100);
        } else {
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 1000000L; // 模拟模式：1ms 轮询
            nanosleep(&ts, NULL);
        }
        ringbuf_drain();
        check_capture_health();
    }
    ringbuf_drain();
    return NULL;
}

// ==================== 写路径入口 ====================

// 真实内核模式下本函数体不做任何事：参数由挂在函数入口的 kprobe 程序通过
// pt_regs 读取，这里只维护「已通知序号」水位，供快照对齐与超大命令排序使用。
__attribute__((noinline, used)) void kvs_ebpf_notify_write(unsigned int seq, int argc, char* argv[]) {
    (void)argc;
    (void)argv;

    pthread_mutex_lock(&g_seq_mutex);
    if (seq > g_notified_seq) {
        g_notified_seq = seq;
    }
    pthread_mutex_unlock(&g_seq_mutex);

#ifdef KVS_EBPF_SIM
    sim_emit_record(seq, argc, argv);
#endif
}

void kvs_ebpf_wait_drained(int timeout_ms) {
    if (!__atomic_load_n(&g_active, __ATOMIC_RELAXED)) {
        return;
    }
    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    deadline.tv_sec += timeout_ms / 1000;
    deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec += 1;
        deadline.tv_nsec -= 1000000000L;
    }

    pthread_mutex_lock(&g_seq_mutex);
    while (g_consumed_seq < g_notified_seq) {
        if (pthread_cond_timedwait(&g_seq_cond, &g_seq_mutex, &deadline) == ETIMEDOUT) {
            kvs_log(KVS_LOG_WARN,
                    "[EBPF] drain timeout: notified=%u consumed=%u, replication order may differ",
                    g_notified_seq, g_consumed_seq);
            break;
        }
    }
    pthread_mutex_unlock(&g_seq_mutex);
}

int kvs_ebpf_realtime_active(void) { return __atomic_load_n(&g_active, __ATOMIC_RELAXED); }

// ==================== 真实内核路径：uprobe + ringbuf attach ====================

#ifndef KVS_EBPF_SIM

struct sym_lookup {
    unsigned long addr;
    unsigned long offset;
    int found;
};

static int find_main_load_segment(struct dl_phdr_info* info, size_t size, void* data) {
    (void)size;
    struct sym_lookup* s = (struct sym_lookup*)data;
    // 主可执行文件的 dlpi_name 为空串
    if (info->dlpi_name != NULL && info->dlpi_name[0] != '\0') {
        return 0;
    }
    for (int i = 0; i < info->dlpi_phnum; ++i) {
        const ElfW(Phdr)* ph = &info->dlpi_phdr[i];
        if (ph->p_type != PT_LOAD) {
            continue;
        }
        unsigned long start = (unsigned long)info->dlpi_addr + ph->p_vaddr;
        unsigned long end = start + ph->p_memsz;
        if (s->addr < start || s->addr >= end) {
            continue;
        }
        s->offset = (unsigned long)(s->addr - (unsigned long)info->dlpi_addr - ph->p_vaddr +
                                    ph->p_offset);
        s->found = 1;
        return 1;
    }
    return 0;
}

// 把函数运行时地址换算成它所在 ELF 文件里的偏移（uprobe 用的是文件偏移）。
static int resolve_self_symbol_offset(void* fn, unsigned long* out_offset) {
    struct sym_lookup s;
    memset(&s, 0, sizeof(s));
    s.addr = (unsigned long)fn;
    dl_iterate_phdr(find_main_load_segment, &s);
    if (!s.found) {
        return -1;
    }
    *out_offset = s.offset;
    return 0;
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
                "[EBPF] tracefs not writable (/sys/kernel/tracing or debugfs), fallback to direct "
                "backlog");
        return -1;
    }

    char path[256];
    snprintf(path, sizeof(path), "%s/uprobe_events", root);
    int fd = open(path, O_WRONLY | O_APPEND);
    if (fd < 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] open %s failed (errno=%d, %s), fallback to direct backlog",
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
static int perf_open_and_attach(struct perf_event_attr* pe, int prog_fd, const char* what) {
    int pids[3] = {(int)getpid(), 0, -1};
    int cpus[3] = {-1, -1, 0};
    int fd = -1;
    int last_errno = 0;
    for (int i = 0; i < 3 && fd < 0; ++i) {
        fd = (int)syscall(__NR_perf_event_open, pe, pids[i], cpus[i], -1, PERF_FLAG_FD_CLOEXEC);
        if (fd < 0) {
            last_errno = errno;
        }
    }
    if (fd < 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] perf_event_open(%s) failed (errno=%d, %s)", what,
                last_errno, strerror(last_errno));
        return -1;
    }
    if (ioctl(fd, PERF_EVENT_IOC_SET_BPF, (unsigned long)prog_fd) != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] PERF_EVENT_IOC_SET_BPF(%s) failed (errno=%d, %s)", what,
                errno, strerror(errno));
        close(fd);
        return -1;
    }
    if (ioctl(fd, PERF_EVENT_IOC_ENABLE, 0) != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] PERF_EVENT_IOC_ENABLE(%s) failed (errno=%d, %s)", what,
                errno, strerror(errno));
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
static int uprobe_attach_pmu(const char* exe, unsigned long offset, int prog_fd) {
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

    int fd = perf_open_and_attach(&pe, prog_fd, "uprobe-pmu");
    if (fd >= 0) {
        kvs_log(KVS_LOG_INFO, "[EBPF] uprobe attached via perf PMU: %s:0x%lx", exe, offset);
    }
    return fd;
}

// 路径二（回退）：老内核没有 uprobe PMU 时，用 tracefs 注册 uprobe + tracepoint 事件。
static int uprobe_attach_tracefs(const char* exe, unsigned long offset, int prog_fd) {
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

    int fd = perf_open_and_attach(&pe, prog_fd, "uprobe-tracefs");
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
                "[EBPF] create ringbuf map failed (errno=%d, %s); fallback to direct backlog "
                "(need CAP_BPF; larger ringbuf needs memlock)",
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
    g_prod_pos = (unsigned long long*)mmap(NULL, (size_t)g_page_size, PROT_READ, MAP_SHARED,
                                           map_fd, (off_t)g_page_size);
    g_data = (unsigned char*)mmap(NULL, 2u * KVS_EBPF_RINGBUF_SIZE, PROT_READ, MAP_SHARED, map_fd,
                                  (off_t)(2 * g_page_size));
    if (g_cons_pos == MAP_FAILED || g_prod_pos == MAP_FAILED || g_data == MAP_FAILED) {
        kvs_log(KVS_LOG_WARN, "[EBPF] mmap ringbuf failed (errno=%d, %s)", errno, strerror(errno));
        if (g_cons_pos != MAP_FAILED && g_cons_pos != NULL) munmap(g_cons_pos, (size_t)g_page_size);
        if (g_prod_pos != MAP_FAILED && g_prod_pos != NULL) munmap(g_prod_pos, (size_t)g_page_size);
        if (g_data != MAP_FAILED && g_data != NULL) munmap(g_data, 2u * KVS_EBPF_RINGBUF_SIZE);
        g_cons_pos = NULL;
        g_prod_pos = NULL;
        g_data = NULL;
        return -1;
    }
    return 0;
}

#endif // !KVS_EBPF_SIM

// ==================== KVS_EBPF_SIM：用户态 ringbuf 模拟 ====================

#ifdef KVS_EBPF_SIM

static int sim_ringbuf_setup(void) {
    g_page_size = sysconf(_SC_PAGESIZE);
    g_cons_pos = (unsigned long long*)mmap(NULL, (size_t)g_page_size, PROT_READ | PROT_WRITE,
                                           MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    g_prod_pos = (unsigned long long*)mmap(NULL, (size_t)g_page_size, PROT_READ | PROT_WRITE,
                                           MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (g_cons_pos == MAP_FAILED || g_prod_pos == MAP_FAILED) {
        return -1;
    }
    *g_cons_pos = 0;
    *g_prod_pos = 0;

    // 用一个 memfd 映射两次，得到和内核 ringbuf 一样的「双映射」环形缓冲。
    int fd = memfd_create("kvstore-ringbuf", MFD_CLOEXEC);
    if (fd < 0 || ftruncate(fd, KVS_EBPF_RINGBUF_SIZE) != 0) {
        if (fd >= 0) close(fd);
        return -1;
    }
    void* base = mmap(NULL, 2u * KVS_EBPF_RINGBUF_SIZE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS,
                      -1, 0);
    if (base == MAP_FAILED) {
        close(fd);
        return -1;
    }
    if (mmap(base, KVS_EBPF_RINGBUF_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) ==
            MAP_FAILED ||
        mmap((char*)base + KVS_EBPF_RINGBUF_SIZE, KVS_EBPF_RINGBUF_SIZE, PROT_READ | PROT_WRITE,
             MAP_SHARED | MAP_FIXED, fd, 0) == MAP_FAILED) {
        close(fd);
        return -1;
    }
    close(fd);
    g_data = (unsigned char*)base;
    return 0;
}

static void sim_emit_record(unsigned int seq, int argc, char* argv[]) {
    if (g_data == NULL || g_prod_pos == NULL || g_cons_pos == NULL) {
        return;
    }
    if (argc < 1 || argc > KVS_EBPF_EVENT_MAX_ARGS || argv == NULL) {
        return;
    }

    unsigned char rec[KVS_EBPF_EVENT_SIZE];
    memset(rec, 0, sizeof(rec));
    memcpy(rec, &seq, 4);
    uint32_t n_argc = (uint32_t)argc;
    memcpy(rec + 4, &n_argc, 4);
    for (int i = 0; i < argc; ++i) {
        const char* s = argv[i] != NULL ? argv[i] : "";
        size_t n = strlen(s) + 1;
        if (n > KVS_EBPF_EVENT_ARG_MAX) {
            return; // 与真实路径一致：超长命令不走采集通道
        }
        uint32_t len = (uint32_t)n;
        memcpy(rec + 8 + i * 4, &len, 4);
        memcpy(rec + KVS_EBPF_EVENT_HEADER_SIZE + (size_t)i * KVS_EBPF_EVENT_ARG_MAX, s, n);
    }

    const unsigned long long total = (8ull + KVS_EBPF_EVENT_SIZE + 7ull) & ~7ull;
    unsigned long long prod = __atomic_load_n(g_prod_pos, __ATOMIC_RELAXED);
    unsigned long long cons = __atomic_load_n(g_cons_pos, __ATOMIC_ACQUIRE);
    if (prod + total - cons > (unsigned long long)KVS_EBPF_RINGBUF_SIZE) {
        return; // 队列满：模拟内核 reserve 失败（真实路径由序号缺口发现）
    }

    unsigned char* hdr = g_data + (prod & ((unsigned long long)KVS_EBPF_RINGBUF_SIZE - 1));
    unsigned int busy_len = (unsigned int)KVS_EBPF_EVENT_SIZE | BPF_RINGBUF_BUSY_BIT;
    memcpy(hdr, &busy_len, 4);
    unsigned int pg_off = 0;
    memcpy(hdr + 4, &pg_off, 4);
    memcpy(hdr + 8, rec, KVS_EBPF_EVENT_SIZE);
    unsigned int committed_len = (unsigned int)KVS_EBPF_EVENT_SIZE;
    __atomic_store_n((unsigned int*)hdr, committed_len, __ATOMIC_RELEASE);
    __atomic_store_n(g_prod_pos, prod + total, __ATOMIC_RELEASE);
}

#endif // KVS_EBPF_SIM

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
#ifndef KVS_EBPF_SIM
    if (g_uprobe_registered) {
        uprobe_unregister();
        g_uprobe_registered = 0;
    }
#endif
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

int kvs_ebpf_realtime_start(void) {
    if (__atomic_load_n(&g_active, __ATOMIC_RELAXED)) {
        return 0;
    }
#if !defined(__x86_64__)
    kvs_log(KVS_LOG_WARN, "[EBPF] unsupported architecture, fallback to direct backlog");
    return -1;
#else

#ifdef KVS_EBPF_SIM
    if (sim_ringbuf_setup() != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] simulation ringbuf setup failed");
        return -1;
    }
    __atomic_store_n(&g_active, 1, __ATOMIC_RELAXED);
    __atomic_store_n(&g_running, 1, __ATOMIC_RELAXED);
    if (pthread_create(&g_consumer_tid, NULL, consumer_thread, NULL) != 0) {
        __atomic_store_n(&g_running, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&g_active, 0, __ATOMIC_RELAXED);
        return -1;
    }
    kvs_log(KVS_LOG_INFO,
            "[EBPF] simulation mode: write path feeds a userspace ringbuf (no kernel probe)");
    return 0;
#else
    g_ringbuf_fd = ringbuf_map_create();
    if (g_ringbuf_fd < 0) {
        return -1;
    }

    g_prog_fd = prog_load(g_ringbuf_fd);
    if (g_prog_fd < 0) {
        ebpf_teardown();
        return -1;
    }

    char exe[512];
    ssize_t n = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    if (n <= 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] readlink(/proc/self/exe) failed (errno=%d, %s)", errno,
                strerror(errno));
        ebpf_teardown();
        return -1;
    }
    exe[n] = '\0';

    unsigned long offset = 0;
    if (resolve_self_symbol_offset((void*)(uintptr_t)&kvs_ebpf_notify_write, &offset) != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] cannot resolve notify symbol offset in %s", exe);
        ebpf_teardown();
        return -1;
    }

    // 首选 uprobe PMU（不需要 tracefs/root），失败再退回 tracefs 注册。
    g_perf_fd = uprobe_attach_pmu(exe, offset, g_prog_fd);
    if (g_perf_fd < 0) {
        g_perf_fd = uprobe_attach_tracefs(exe, offset, g_prog_fd);
    }
    if (g_perf_fd < 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] attach uprobe failed, fallback to direct backlog");
        ebpf_teardown();
        return -1;
    }

    if (ringbuf_mmap(g_ringbuf_fd) != 0) {
        ebpf_teardown();
        return -1;
    }

    __atomic_store_n(&g_running, 1, __ATOMIC_RELAXED);
    if (pthread_create(&g_consumer_tid, NULL, consumer_thread, NULL) != 0) {
        kvs_log(KVS_LOG_WARN, "[EBPF] create consumer thread failed, fallback to direct backlog");
        ebpf_teardown();
        return -1;
    }

    kvs_log(KVS_LOG_INFO,
            "[EBPF] realtime capture active: uprobe ringbuf -> replication backlog (cross-host OK)");
    return 0;
#endif // KVS_EBPF_SIM

#endif // __x86_64__
}

void kvs_ebpf_realtime_stop(void) {
    if (!__atomic_load_n(&g_active, __ATOMIC_RELAXED) && !__atomic_load_n(&g_running, __ATOMIC_RELAXED)) {
        return;
    }
    ebpf_teardown();
}
