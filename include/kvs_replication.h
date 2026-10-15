#pragma once

#ifdef __cplusplus
extern "C" {
#endif

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
//   2) 网络层识别 EBPF / REPL_INC 握手（kvs_replication_accept_handshake）；
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

// 握手入口：识别 Master 侧的 EBPF（采集进程注册）与 Replica 侧的 REPL_INC
// （增量通道）。返回 1 表示「这条连接已被复制模块受理」，网络层据此回 +OK。
int kvs_replication_accept_handshake(int fd, const char* data, int length);

// 握手响应发送完成后由网络层调用。新架构下握手本身不触发任何服务端动作
// （全量同步由 Replica 主动发 SYNC 驱动），保留函数以维持网络层调用点。
void kvs_replication_finish_handshake(int fd);

// 连接断开时由网络层调用：清理该 fd 在同步模块里登记的会话。
void kvs_replication_remove_replica(int fd);

// RDB LOAD / AOF LOAD 之后要求在线 Replica 重新全量同步：往有序采集流写一条
// INVALIDATE 记录，ebpf-agent 读到后断开增量通道，Replica 据此重新走 SYNC。
int kvs_replication_resync();

// 复制回放状态：回放线程置 1，使写分支跳过 AOF 记录与再转发。
int kvs_replication_is_replaying();
void kvs_replication_set_replaying(int value);

#ifdef __cplusplus
}
#endif
