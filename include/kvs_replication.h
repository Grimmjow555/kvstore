#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// 当前进程在复制拓扑中的角色。
typedef enum { KVS_ROLE_MASTER = 0, KVS_ROLE_REPLICA = 1 } kvs_role_t;

// ==================== 公共接口 ====================

// 设置角色并初始化复制模块状态。
int kvs_replication_init(kvs_role_t role);

// ==================== Master 端接口 ====================

// 注册一个已建立连接的 Replica，等待初始全量同步。
int kvs_replication_add_replica(int fd);

// 移除 Replica 连接并关闭对应 socket。
void kvs_replication_remove_replica(int fd);

// 将成功执行的写命令发送给所有已完成全量同步的 Replica。
int kvs_replication_append(int argc, char* argv[]);

// 判断是否为 Replica 发来的握手请求。
int kvs_replication_is_handshake(const char* data, int length);

// 校验并注册 Replica 发来的握手请求。
int kvs_replication_accept_handshake(int fd, const char* data, int length);

// 发送握手响应并开始向 Replica 发送全量快照。
void kvs_replication_finish_handshake(int fd);

// 要求所有已连接 Replica 重新执行全量同步。
int kvs_replication_resync();

// ==================== Replica 端接口 ====================

// 连接 Master。
int kvs_replication_connect_master(const char* ip, int port);

// 启动、停止和销毁 Replica 同步线程及连接。
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

// 查询/设置当前进程是否已成功启用 RDMA 全量同步。
int kvs_replication_is_rdma_enabled();
void kvs_replication_set_rdma_enabled(int enabled);
#endif

#ifdef __cplusplus
}
#endif
