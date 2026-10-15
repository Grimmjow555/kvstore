#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ==================== 实时增量采集（重设计阶段 3） ====================
//
// 采集已经搬到独立的 ebpf-agent 进程，kvstore 进程里只保留一个「挂载点」函数：
//
//   写命令 / SYNC / SYNC FINISHED（都在存储锁内）
//     -> kvs_ebpf_notify_command(argc, argv)   （分配自增序号 + 调用挂载点）
//     -> kvs_ebpf_notify_write(seq, argc, argv) （uprobe 挂载点）
//          uprobe 模式：函数体不做采集，参数由挂在函数入口的 BPF 程序读取
//          sim 模式   ：把记录写进 kvs_ebpf_ring 的共享内存环
//     -> ebpf-agent 消费（内核 ringbuf 或共享内存环）-> 缓存/转发给 Replica
//
// 关键点：挂载点必须在存储锁内、且与写命令走同一条有序流，这样 SYNC /
// SYNC FINISHED 记录与写命令的先后关系就等于真实提交顺序，agent 才能用
// 「读到 SYNC 开始缓存、读到 SYNC FINISHED 冲刷」覆盖整个全量窗口（见设计 §7.1）。

// 一条写命令最多采集的参数个数（命令表里最长的是 SET/MOD：cmd key value）。
#define KVS_EBPF_EVENT_MAX_ARGS 3

// 单个参数最多采集的字节数（含结尾 '\0'）。超过该上限的命令不走采集通道。
#define KVS_EBPF_EVENT_ARG_MAX 1024

// 记录布局（8 字节对齐）：
//   u32 seq | u32 argc | u32 arg_len[3] | u32 pad | char data[3][ARG_MAX]
#define KVS_EBPF_EVENT_HEADER_SIZE 24
#define KVS_EBPF_EVENT_SIZE \
    (KVS_EBPF_EVENT_HEADER_SIZE + KVS_EBPF_EVENT_MAX_ARGS * KVS_EBPF_EVENT_ARG_MAX)

// 内核 ringbuf 容量（uprobe 模式，ebpf-agent 侧创建），必须是 2 的幂且大于单条记录。
#define KVS_EBPF_RINGBUF_SIZE (1u << 18)

// 采集模式（运行期可选，见 kvstore.conf 的 ebpf_capture_mode）。
#define KVS_EBPF_MODE_UPROBE 0 // 默认：agent 在内核里挂 uprobe + ringbuf
#define KVS_EBPF_MODE_SIM 1    // 无 CAP_BPF 时：master 写共享内存环，agent 读

// master 侧初始化：uprobe 模式只记录模式；sim 模式创建共享内存环。
// 返回 0 表示采集通道已就绪。
int kvs_ebpf_capture_init(const char* ring_path, int mode);

// 进程退出时释放采集侧资源（sim 模式会删除环文件）。
void kvs_ebpf_capture_shutdown(void);

// 是否允许产出采集记录。只有 ebpf-agent 注册（armed）后才开启：
// 注册之前的写入对同步没有意义（它们会被快照覆盖），若照常产出会把有界队列
// 灌满并产生 seq 缺口。sim 模式在开启时同时重置共享内存环。
void kvs_ebpf_capture_enable(int enabled);

// 递增并返回全局采集序号。写命令与控制命令共用，保证同一条有序流。
unsigned int kvs_ebpf_next_seq(void);

// 写路径与控制命令的统一入口：分配序号后调用 uprobe 挂载点。
void kvs_ebpf_notify_command(int argc, char* argv[]);

// uprobe 挂载点。
//
// 必须保留独立符号（noinline、used）供内核 uprobe 挂到函数入口；参数布局
// (seq=di, argc=si, argv=dx) 是 BPF 程序读取 pt_regs 的约定，不要改签名。
// sim 模式下本函数把记录写进共享内存环；uprobe 模式下函数体不采集。
void kvs_ebpf_notify_write(unsigned int seq, int argc, char* argv[]);

// 记录编解码（kvs_ebpf_event_fits / kvs_ebpf_event_parse）与记录布局一起定义在
// include/kvs_ebpf_ring.h：它们是「记录格式」的一部分，Master 与 agent 共用同一份，
// 不做两处声明。

#ifdef __cplusplus
}
#endif
