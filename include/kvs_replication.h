#pragma once

#include "kvs_replication_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

// ==================== 复制通道握手 token ====================
//
// 握手负载本身也是 RESP 数组（外层仍是「4 字节长度头 + 负载」），由复制双方共用：
//
//   EBPF 2                 ebpf-agent -> Master   采集进程注册
//   REPL_INC 2 <session>   Replica -> ebpf-agent  增量通道建立
//
// 编解码集中在 replication/kvs_replication_protocol.cpp，避免两侧格式漂移。

// 当前进程在复制拓扑中的角色。
typedef enum { KVS_ROLE_MASTER = 0, KVS_ROLE_REPLICA = 1 } kvs_role_t;

// ==================== 复制接口（重设计后只剩极小的一层） ====================
//
// 重设计把「全量搬运」与「增量采集+投递」都拆给了独立进程：
//   * 全量搬运      -> rdma-client / rdma-server（阶段 2）
//   * 增量采集+转发 -> ebpf-agent（阶段 3）
//
// 因此 kvstore 进程里只剩四件事：
//   1) 写路径把成功执行的命令交给有序采集流（kvs_replication_append）；
//   2) 网络层识别 EBPF 握手（kvs_replication_accept_handshake）；
//   3) 断连时清理同步会话登记（kvs_replication_remove_replica）；
//   4) 区分「复制回放线程」与「本地客户端线程」，避免回放误记 AOF
//      （kvs_replication_is_replaying / _set_replaying）。
//
// 当前由 Master 为每次全量同步分配独立 session_id，Agent 按 session_id 维护
// 最多 16 个目标；旧的进程内 backlog、发送线程与进程内 RDMA 监听已删除。

// 设置角色。复制拓扑里 Master 负责全量快照，Replica 负责接收与回放。
int kvs_replication_init(kvs_role_t role);

// 写命令入口：把命令交给有序采集流，由 ebpf-agent 采集并转发给 Replica。
int kvs_replication_append(int argc, char* argv[]);

// 握手入口：识别 Master 侧的 EBPF（采集进程注册）。返回 1 表示
// 「这条连接已被复制模块受理」，网络层据此回 +OK。
int kvs_replication_accept_handshake(int fd, const char* data, int length);

// 连接断开时由网络层调用：清理该 fd 在同步模块里登记的会话。
void kvs_replication_remove_replica(int fd);

// 复制回放状态：回放线程置 1，使写分支跳过 AOF 记录与再转发。
int kvs_replication_is_replaying();
void kvs_replication_set_replaying(int value);

// ==================== 主从全量同步控制流 ====================
//
//   Replica --SYNC <rdma_ip> <rdma_port>--> Master
//   Master  : 分配 session_id -> 序列化快照 -> 写会话专用 .tmp -> 派生 rdma-client
//             -> +OK <session_id>
//   Replica : 接收 .tmp -> 加载 -> REPL_INC 2 <session_id>
//             -> SYNC FINISHED <session_id> -> Master
//   Master  : 只回收该 session_id 的 rdma-client 与临时文件 -> +OK
//
// Replica 在反向 REPL_INC 通道断开时重新执行全量同步。

// 角色初始化。Master 准备同步路径，Replica 记录 Master 地址。
int kvs_sync_init(kvs_role_t role);

// Replica 侧：显式设置要连接的 Master 地址。
void kvs_sync_set_master_addr(const char* ip, int port);

// Master 命令处理。处理器只会在发布有序事件和序列化快照时自行持有存储锁；
// 文件 I/O 与子进程管理在锁外执行，避免多个会话互相阻塞普通写入。
int kvs_sync_handle_sync(int client_fd, char* tokens[], int count, char* response,
                         int response_size);
int kvs_sync_handle_sync_finished(int client_fd, char* tokens[], int count, char* response,
                                  int response_size);

// Replica 同步驱动。start() 只拉起后台线程，Master 未启动时也会按退避重试。
int kvs_sync_start();
void kvs_sync_stop();
void kvs_sync_destroy();

// Master 侧：ebpf-agent 通过 EBPF 握手注册；未注册时 Master 拒绝 SYNC。
// Master 侧识别 Agent 注册；返回 1 表示连接已被复制模块受理。
int kvs_sync_accept_ebpf_handshake(int fd, const char* data, int length);

// 采集进程是否已注册（Master 只在已注册时受理 SYNC）。
int kvs_sync_ebpf_armed(void);

// 连接断开时清理该 fd 在同步模块里登记的会话。
void kvs_sync_forget_fd(int fd);

#ifdef __cplusplus
}
#endif
