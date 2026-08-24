#pragma once

typedef enum { KVS_ROLE_MASTER = 0, KVS_ROLE_REPLICA } kvs_role_t;

int kvs_replication_init(kvs_role_t role);

int kvs_replication_set_master(const char* ip, int port);

int kvs_replication_append(int argc, char* argv[]);

int kvs_replication_full_sync(int fd);

int kvs_replication_sync(int fd);

void kvs_replication_destroy();
