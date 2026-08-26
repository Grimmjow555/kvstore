#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { KVS_ROLE_MASTER = 0, KVS_ROLE_REPLICA = 1 } kvs_role_t;

/*
 * 初始化复制模块
 */
int kvs_replication_init(kvs_role_t role);

/*
 * Master：
 * 添加一个 Replica
 */
int kvs_replication_add_replica(int fd);

/*
 * Master：
 * 删除 Replica
 */
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