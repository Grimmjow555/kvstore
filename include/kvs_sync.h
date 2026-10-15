#pragma once

#include "kvs_replication.h" // kvs_role_t

#ifdef __cplusplus
extern "C" {
#endif

// ==================== 主从全量同步控制流（重设计阶段 1） ====================
//
// 设计见 docs/replication-redesign.md。本模块只落地「控制命令 + 快照落盘/加载」：
//
//   Replica --SYNC--> Master
//   Master  : 序列化快照 -> 写 <sync_tmp_file> -> +OK
//   Replica : 取到 .tmp -> kvs_snapshot_load() -> --SYNC FINISHED--> Master
//   Master  : 删除 .tmp -> +OK
//
// 阶段 1 的两处临时实现（阶段 2 / 阶段 3 替换）：
//   * 快照文件的获取：单机验证时 Master/Replica 共用 data/ 目录，Replica 等文件
//     出现后直接加载；阶段 2 会替换为「派生 rdma-server，等它把文件写到本地」。
//   * SYNC 尚不校验 eBPF 是否已武装；阶段 3 接入 EBPF 握手后再补「未武装即拒绝」。

// 角色初始化：Master 准备 .tmp 路径，Replica 记录主节点地址。
int kvs_sync_init(kvs_role_t role);

// Replica 侧：显式设置要连接的 Master 地址（命令行位置参数会覆盖配置文件）。
void kvs_sync_set_master_addr(const char* ip, int port);

// ==================== Master 命令处理 ====================
// 两个函数都必须在 kvs_filter_protocol 内、持有存储锁时调用：
// SYNC 需要在锁内序列化，保证文件内容就是「此刻」的一致快照。
int kvs_sync_handle_sync(char* response, int response_size);
int kvs_sync_handle_sync_finished(char* response, int response_size);

// ==================== Replica 同步驱动 ====================
// start() 只拉起后台线程，Master 未启动时也能正常启动并按退避重试。
int kvs_sync_start();
void kvs_sync_stop();
void kvs_sync_destroy();

// ==================== 采集进程注册 / 增量通道（阶段 3） ====================
//
// Master 侧：ebpf-agent 通过 EBPF 握手注册；未注册时 Master 拒绝 SYNC（eBPF 强制）。
// Replica 侧：ebpf-agent 通过 REPL_INC 握手建立增量通道，后续负载按复制回放处理。
// 两个函数都返回 1 表示「这条连接已被复制模块受理」（网络层据此回 +OK）。
int kvs_sync_accept_ebpf_handshake(int fd, const char* data, int length);
int kvs_sync_accept_repl_inc_handshake(int fd, const char* data, int length);

// 采集进程是否已注册（Master 只在已注册时受理 SYNC）。
int kvs_sync_ebpf_armed(void);

// Replica 侧：若 fd 是增量通道，把 payload 当 RESP 命令回放，返回 1；否则返回 0。
int kvs_sync_try_replay(int fd, char* payload, int length);

// 连接断开时清理该 fd 在同步模块里登记的会话（由网络层断连路径调用）。
void kvs_sync_forget_fd(int fd);

// Replica 侧：等待增量通道状态变化（wait_connected=1 等接入、0 等断开）。
// 返回 1 表示达到目标状态，0 表示超时/被停止。全量同步完成后用它等待
// ebpf-agent 接入；增量通道断开即视为「需要重新全量同步」。
int kvs_sync_wait_increment(int wait_connected, int timeout_ms);

#ifdef __cplusplus
}
#endif
