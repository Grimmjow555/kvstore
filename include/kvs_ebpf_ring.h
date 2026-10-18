#pragma once

#include <stddef.h>
#include <stdint.h>

#include "kvs_ebpf.h"

#ifdef __cplusplus
extern "C" {
#endif

// ==================== 采集环（master 生产 / ebpf-agent 消费） ====================
//
// 重设计阶段 3：eBPF 采集从 kvstore 进程内搬到独立的 ebpf-agent 进程。两条路径
// 共用同一套记录格式（见 include/kvs_ebpf.h 的 KVS_EBPF_EVENT_*）：
//
//   * uprobe 模式（默认）：agent 在内核里建 ringbuf map 并把 uprobe 挂到 master
//     二进制上，master 侧只有 kvs_ebpf_notify_write() 这个挂载点，本身不做采集；
//   * sim 模式：内核采集不可用时，master 把记录写进本模块的共享内存环，agent
//     从环里取。两条路径对 agent 上层完全等价，用于在无 CAP_BPF 的环境验证
//     端到端链路（设计 §8.5 / §13）。
//
// 单生产者（master）单消费者（agent），用 head/tail 的 acquire/release 保证顺序；
// 环满时生产者丢弃并累计 dropped，消费端靠记录里的自增 seq 发现缺口。

// 一条记录的内存布局，与 BPF ringbuf 里 kvs_ebpf_event_parse() 期望的布局一致：
//   u32 seq | u32 argc | u32 arg_len[3] | u32 reserved | char data[3][ARG_MAX]
#define KVS_EBPF_RECORD_SIZE KVS_EBPF_EVENT_SIZE

// 环容量（记录条数）。必须是 2 的幂，便于取模。
#define KVS_EBPF_RING_CAPACITY 4096u

// 创建（master）共享内存环文件，容量固定为 KVS_EBPF_RING_CAPACITY。
// 已存在同名文件时直接覆盖重建。成功返回 0。
int kvs_ebpf_ring_create(const char* path);

// 打开（agent）已存在的环文件。成功返回 0，失败（文件不存在/魔数不符）返回 -1。
int kvs_ebpf_ring_open(const char* path);

// 解除映射。master 与 agent 都可调用；创建方额外负责删除文件。
void kvs_ebpf_ring_close(void);

// 清空环（head/tail 归零）。master 在 ebpf-agent 注册时调用：注册之前的记录
// 对本次会话没有意义，清掉可以避免 agent 一上来就读到陈旧记录并误判 seq 缺口。
void kvs_ebpf_ring_reset(void);

// 生产者：写入一条记录。参数超长/环满返回 -1（环满时累计 dropped）。
int kvs_ebpf_ring_push(unsigned int seq, int argc, char* argv[]);

// 消费者：取出一条记录并解析成 argv。
//   返回 1 = 取到一条（*out_seq 与 argv 有效，返回值即参数个数）；
//   返回 0 = 环为空；
//   返回 -1 = 记录非法（调用方应据此触发重同步）。
int kvs_ebpf_ring_pop(unsigned int* out_seq, char* argbuf, size_t argbuf_size, char* argv[],
                      int max_args);

// 生产端累计丢弃的记录数（用于诊断；agent 侧读到 0）。
uint64_t kvs_ebpf_ring_dropped(void);

// 判断一条命令是否适合进采集通道（参数个数与长度都在上限内）。
// 声明位于本头文件，Master 与 agent 共用 replication/kvs_ebpf_ring.cpp 的实现。
int kvs_ebpf_event_fits(int argc, char* argv[]);

// 解析一条记录：参数内容按顺序拷进 argbuf，argv 指向 argbuf 内部。
// 成功返回参数个数（>=1），记录非法/被截断返回 -1。
int kvs_ebpf_event_parse(const unsigned char* data, unsigned int len, unsigned int* out_seq,
                         char* argbuf, size_t argbuf_size, char* argv[], int max_args);

#ifdef __cplusplus
}
#endif
