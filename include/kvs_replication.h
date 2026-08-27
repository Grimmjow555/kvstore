#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { KVS_ROLE_MASTER = 0, KVS_ROLE_REPLICA = 1 } kvs_role_t;

// 功能：初始化主从复制模块
// 作用：
// 1. 设置当前服务器角色：Master 或 Replica
// 2. 初始化 Master 保存 Replica socket fd 的数组
// 3. 初始化 Replica 的 pending 状态
int kvs_replication_init(kvs_role_t role);

// 功能：将一个已经连接的 Replica 加入 Master 的 Replica 列表
//
// 参数：
// fd：Replica 与 Master 建立 TCP 连接后的 socket fd
//
// pending = 1 表示：
// Replica 已经连接，但还没有完成初始数据同步
int kvs_replication_add_replica(int fd);

// 功能：从 Master 的 Replica 列表中删除一个 Replica
//
// 使用场景：
// 1. Replica 断开连接
// 2. send() 失败
// 3. Replica 主动退出
//
// 同时关闭对应 socket。
void kvs_replication_remove_replica(int fd);

/*
 * Master：
 * 将成功执行的写命令发送给所有 Replica
 */
int kvs_replication_append(int argc, char* argv[]);

/*
 * Replica：
 * 连接 Master
 */
int kvs_replication_connect_master(const char* ip, int port);

/* Master: identify and register a replica handshake. */
int kvs_replication_is_handshake(const char* data, int length);
int kvs_replication_accept_handshake(int fd, const char* data, int length);
void kvs_replication_finish_handshake(int fd);
int kvs_replication_resync();

/*
 * Replica：
 * 启动同步线程
 */
int kvs_replication_start();

void kvs_replication_stop();

void kvs_replication_destroy();

int kvs_replication_is_replaying();

void kvs_replication_set_replaying(int value);

#ifdef __cplusplus
}
#endif