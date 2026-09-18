#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// eBPF 队列中每条复制命令的最大长度。这里与网络层 MAX_ALLOWED_LEN 保持一致，
// 保证常规写命令可以完整放入 eBPF map；超长命令会自动回退到 TCP 实时同步。
#define KVS_EBPF_MAX_COMMAND_LEN (1024 * 1024)

// 队列容量。队列满时，如果重试后仍无法写入，则回退到 TCP 实时同步。
#define KVS_EBPF_MAX_ENTRIES 16

// 初始化 Master 端的 eBPF 复制队列并 pin 到 bpffs。
// 成功返回 0，失败返回 -1（调用方应回退到 TCP 实时同步）。
int kvs_ebpf_master_init(unsigned short master_port);

// 关闭 Master 端 eBPF 复制队列，并尝试清理 pin 文件。
void kvs_ebpf_master_destroy();

// 查询 Master 端 eBPF 队列是否可用。
int kvs_ebpf_master_available();

// 判断 fd 对端是否为本地回环地址。仅本地 Replica 可以通过 eBPF map 同步。
int kvs_ebpf_peer_is_local(int fd);

// 将一条已编码的 RESP 命令推送到 eBPF 队列。
// 成功返回 0；队列不可用或命令超长返回 -1。
int kvs_ebpf_push(const char* data, unsigned int len);

// 从 eBPF 队列弹出一条命令。out_len 输入为 out 缓冲区大小，输出为实际长度。
// 成功返回 0；队列为空或不可用返回 -1。
int kvs_ebpf_pop(char* out, unsigned int* out_len);

// 丢弃队列中当前所有待消费命令。Replica 收到 REPLICA_RESET 后使用。
void kvs_ebpf_drain();

// Replica 打开 Master pin 在 bpffs 上的 eBPF 队列。
// 成功返回 0，失败返回 -1（调用方应回退到 TCP 实时同步）。
int kvs_ebpf_replica_open(unsigned short master_port);

// 关闭 Replica 端 eBPF 队列。
void kvs_ebpf_replica_close();

// 查询 Replica 端 eBPF 队列是否可用。
int kvs_ebpf_replica_available();

#ifdef __cplusplus
}
#endif
