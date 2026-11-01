#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// ==================== 复制通道握手 token ====================
//
// 握手负载本身也是 RESP 数组（外层仍是「4 字节长度头 + 负载」），由复制双方共用：
//
//   KVS_REPL_HANDSHAKE_EBPF     ebpf-agent -> Master   采集进程注册
//   KVS_REPL_HANDSHAKE_REPL_INC Replica -> ebpf-agent  增量通道建立
//
// 接收侧在 tools/ebpf_agent.cpp，发送侧在 replication/kvs_sync.cpp。
// 两侧必须字面一致，因此集中定义在这里。
#define KVS_REPL_HANDSHAKE_EBPF "*1\r\n$4\r\nEBPF\r\n"
#define KVS_REPL_HANDSHAKE_REPL_INC "*1\r\n$8\r\nREPL_INC\r\n"

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
// 旧的进程内 backlog、副本槽位、发送线程、REPLICA 握手与进程内 RDMA 监听
// 都已在阶段 4 删除（见 docs/replication-redesign.md §12）。

// 设置角色。复制拓扑里 Master 负责全量快照，Replica 负责接收与回放。
int kvs_replication_init(kvs_role_t role);

// 写命令入口：把命令交给有序采集流，由 ebpf-agent 采集并转发给 Replica。
int kvs_replication_append(int argc, char* argv[]);

// 握手入口：识别 Master 侧的 EBPF（采集进程注册）。返回 1 表示
// 「这条连接已被复制模块受理」，网络层据此回 +OK。
int kvs_replication_accept_handshake(int fd, const char* data, int length);

// 握手响应发送完成后由网络层调用。新架构下握手本身不触发任何服务端动作
// （全量同步由 Replica 主动发 SYNC 驱动），保留函数以维持网络层调用点。
void kvs_replication_finish_handshake(int fd);

// 连接断开时由网络层调用：清理该 fd 在同步模块里登记的会话。
void kvs_replication_remove_replica(int fd);

// 运行期整库替换后要求在线 Replica 重新全量同步：往有序采集流写一条
// INVALIDATE 记录，ebpf-agent 读到后断开增量通道，Replica 据此重新走 SYNC。
int kvs_replication_resync();

// 复制回放状态：回放线程置 1，使写分支跳过 AOF 记录与再转发。
int kvs_replication_is_replaying();
void kvs_replication_set_replaying(int value);

// ==================== 主从全量同步控制流 ====================
//
//   Replica --SYNC <rdma_ip> <rdma_port>--> Master
//   Master  : 序列化快照 -> 原子写 data/kvstore.sync.tmp -> 派生 rdma-client
//   Replica : 接收 .tmp -> kvs_snapshot_load() -> 主动连接 Agent -> --SYNC FINISHED--> Master
//   Master  : 回收 rdma-client -> 删除 .tmp -> +OK
//
// Replica 在反向 REPL_INC 通道断开时重新执行全量同步。

// 角色初始化。Master 准备同步路径，Replica 记录 Master 地址。
int kvs_sync_init(kvs_role_t role);

// Replica 侧：显式设置要连接的 Master 地址。
void kvs_sync_set_master_addr(const char* ip, int port);

// Master 命令处理。两个函数都必须在 kvs_filter_protocol 内、持有存储锁时调用：
// SYNC 需要在锁内序列化，保证文件内容就是命令到达时刻的一致快照。
int kvs_sync_handle_sync(char* tokens[], int count, char* response, int response_size);
int kvs_sync_handle_sync_finished(char* response, int response_size);

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

// Replica 侧：等待增量通道状态变化（wait_connected=1 等接入、0 等断开）。
// 返回 1 表示达到目标状态，0 表示超时或复制驱动已停止。
int kvs_sync_wait_increment(int wait_connected, int timeout_ms);

#ifdef __cplusplus
}
#endif
