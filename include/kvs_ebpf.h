#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ==================== 实时增量采集：kprobe/uprobe + BPF ring buffer ====================
//
// 采集链路：
//
//   Master 写路径（持有存储锁）
//     -> kvs_ebpf_notify_write(seq, argc, argv)    uprobe 挂载点
//     -> BPF_PROG_TYPE_KPROBE 程序读取 argv，写入 BPF_MAP_TYPE_RINGBUF
//     -> 用户态消费线程解析记录、校验序号
//     -> kvs_replication_on_captured_command()     -> backlog + 发送线程
//     -> TCP/RDMA 投递给 Replica（跨主机同样成立）
//
// 关于 kprobe / uprobe：两者共用同一种程序类型（BPF_PROG_TYPE_KPROBE）和同一套
// attach 流程（perf_event_open + PERF_EVENT_IOC_SET_BPF）。触发点是 kvstore 自身的
// 写路径，只能挂 uprobe：kprobe 只能挂内核函数，既读不到应用层命令参数，也会在
// proactor(io_uring) 后端下漏掉事件（该后端不经过 read/write 系统调用）。
//
// 正确性：ringbuf 是「可能丢事件」的通道（reserve 失败即丢）。因此记录里带自增序号，
// 消费线程一旦发现序号缺口就打印错误并触发一次全量重同步——绝不静默丢命令。
// 任何一步 attach 失败（无 CAP_BPF、tracefs 不可写、内核不支持 ringbuf 等）都会自动
// 回退到「写路径直接落 backlog」，功能与数据一致性不受影响。
#define KVS_ENABLE_EBPF_REALTIME 1

// 一条写命令最多采集的参数个数（命令表里最长的是 SET/MOD：cmd key value）。
#define KVS_EBPF_EVENT_MAX_ARGS 3

// 单个参数最多采集的字节数（含结尾 '\0'）。超过该上限的命令不走 eBPF 采集，
// 由复制模块直接写 backlog，避免被截断。
#define KVS_EBPF_EVENT_ARG_MAX 1024

// ringbuf 记录布局（8 字节对齐）：
//   u32 seq | u32 argc | u32 arg_len[3] | u32 pad | char data[3][ARG_MAX]
#define KVS_EBPF_EVENT_HEADER_SIZE 24
#define KVS_EBPF_EVENT_SIZE \
    (KVS_EBPF_EVENT_HEADER_SIZE + KVS_EBPF_EVENT_MAX_ARGS * KVS_EBPF_EVENT_ARG_MAX)

// ringbuf 容量，必须是 2 的幂且大于单条记录。
#define KVS_EBPF_RINGBUF_SIZE (1u << 18)

// 启动/停止 Master 侧实时增量采集。start 返回 0 表示采集通道已经生效。
int kvs_ebpf_realtime_start(void);
void kvs_ebpf_realtime_stop(void);

// 采集通道当前是否生效。只有返回 1 时写路径才应该调用 notify。
int kvs_ebpf_realtime_active(void);

// 等待消费线程排空到「调用时刻已经通知过的序号」，最多等待 timeout_ms 毫秒。
// 全量快照取 base_seq 前调用，避免「快照已包含、增量又补发」的重叠窗口。
void kvs_ebpf_wait_drained(int timeout_ms);

// 判断一条命令是否适合走 eBPF 采集通道（参数个数与长度都在上限内）。
int kvs_ebpf_event_fits(int argc, char* argv[]);

// 写路径调用：把一条写命令交给采集通道。
//
// 该函数是 uprobe 的挂载点，必须保留独立符号（noinline、不被优化掉）；
// seq 必须在存储锁内单调递增，消费线程靠它发现丢事件。
// 注意：真实内核模式下本函数自身不做任何事（参数由挂在内核里的程序读取），
// 只有 KVS_EBPF_SIM 编译期模拟模式才会在用户态落一条记录。
void kvs_ebpf_notify_write(unsigned int seq, int argc, char* argv[]);

// 解析一条 ringbuf 记录：参数内容按顺序拷进 argbuf，argv 指向 argbuf 内部。
// 成功返回参数个数（>=1），记录非法/被截断返回 -1。消费线程与单测共用。
int kvs_ebpf_event_parse(const unsigned char* data, unsigned int len, unsigned int* out_seq,
                         char* argbuf, size_t argbuf_size, char* argv[], int max_args);

#ifdef __cplusplus
}
#endif
