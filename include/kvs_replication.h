#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// 当前进程在复制拓扑中的角色。
typedef enum { KVS_ROLE_MASTER = 0, KVS_ROLE_REPLICA = 1 } kvs_role_t;

// ==================== 复制通道帧类型 ====================
//
// 复制通道沿用外层「4 字节网络序长度 + 负载」的帧格式，但在负载首字节增加
// 一个类型字段，用于把增量命令、全量快照等不同语义的数据区分开：
//
//   4 字节长度 N（含类型字节） | 1 字节类型 | N-1 字节负载
//
// 之所以要类型字段：全量快照现在是二进制 RDB 内容，不能再像旧实现那样靠
// 字符串比较区分 REPLICA_RESET / 全量命令。
enum {
    KVS_REPL_FRAME_COMMAND = 1,       // 增量写命令（负载为 RESP 文本）
    KVS_REPL_FRAME_RESET = 2,         // 要求 Replica 丢弃本地数据，准备全量同步
    KVS_REPL_FRAME_SNAPSHOT = 3,      // 全量快照分片（负载为 RDB 二进制片段）
    KVS_REPL_FRAME_SNAPSHOT_END = 4,  // 全量快照结束
    KVS_REPL_FRAME_FULLSYNC_DONE = 5, // 全量同步完成（含增量补齐）
    KVS_REPL_FRAME_ACK = 6,           // 预留给 Replica -> Master 的进度回执
};

// ==================== 公共接口 ====================

// 设置角色并初始化复制模块状态。
int kvs_replication_init(kvs_role_t role);

// Replica 侧：设置要连接的 Master 地址（命令行位置参数会覆盖配置文件）。
void kvs_replication_set_master_addr(const char* ip, int port);

// ==================== Master 端接口 ====================

// 注册一个已建立连接的 Replica，等待初始全量同步。
int kvs_replication_add_replica(int fd);

// 网络层检测到连接断开时调用：只清理复制侧槽位，不关闭 fd。
// fd 的生命周期始终归网络层所有，复制模块绝不 close() 网络层的 fd，
// 否则会出现关闭后 fd 号被复用、复制帧被写进普通客户端连接的问题。
void kvs_replication_remove_replica(int fd);

// 将成功执行的写命令发送给所有已完成全量同步的 Replica。
int kvs_replication_append(int argc, char* argv[]);

// eBPF 采集通道的消费回调：把 ringbuf 里取出的写命令追加到增量日志。
// 由 replication/kvs_ebpf.cpp 的消费线程调用，调用方不持有任何锁。
void kvs_replication_on_captured_command(int argc, char* argv[]);

// 判断是否为 Replica 发来的握手请求。
int kvs_replication_is_handshake(const char* data, int length);

// 校验并注册 Replica 发来的握手请求。
int kvs_replication_accept_handshake(int fd, const char* data, int length);

// 发送握手响应并开始向 Replica 发送全量快照。
void kvs_replication_finish_handshake(int fd);

// 要求所有已连接 Replica 重新执行全量同步。
int kvs_replication_resync();

// ==================== Replica 端接口 ====================

// 连接 Master（单次尝试，失败返回 -1）。
int kvs_replication_connect_master(const char* ip, int port);

// 启动、停止和销毁 Replica 同步线程及连接。
// start() 只负责拉起后台监督线程，实际的连接/重连/全量同步都在该线程内完成，
// 因此 Master 未启动时 Replica 也能正常启动，并持续按退避策略重试。
int kvs_replication_start();
void kvs_replication_stop();
void kvs_replication_destroy();

// 查询和设置 Replica 的复制回放状态。
int kvs_replication_is_replaying();
void kvs_replication_set_replaying(int value);

#ifdef KVS_ENABLE_RDMA
// ==================== RDMA 全量同步接口 ====================

#define KVS_RDMA_PORT_OFFSET 1

// Master 启动独立 RDMA 监听端口（TCP 端口 + KVS_RDMA_PORT_OFFSET）。
int kvs_replication_start_rdma_listener(unsigned short tcp_port);

// Replica 主动连接 Master 的 RDMA 端口，完成一次全量快照同步。
int kvs_replication_rdma_full_sync(const char* master_ip, int master_port);

// 停止 Master 的 RDMA 监听线程并释放相关资源。
void kvs_replication_stop_rdma_listener();

// Master 侧：记录「某个对端刚通过 RDMA 拿到了一份对应 seq 的快照」。
// 该 Replica 随后发来的 TCP 握手会消费这个 seq，从而只补发 seq 之后的增量命令，
// 避免旧实现里 RDMA 全量快照与增量命令之间丢写。
void kvs_replication_note_rdma_snapshot(const char* peer_ip, unsigned long long seq);

// Master 侧：读取当前增量日志末尾序号（用于 RDMA 快照对齐），内部加锁。
unsigned long long kvs_replication_backlog_offset();

// 查询/设置当前进程是否已成功启用 RDMA 全量同步。
int kvs_replication_is_rdma_enabled();
void kvs_replication_set_rdma_enabled(int enabled);
#endif

#ifdef __cplusplus
}
#endif
