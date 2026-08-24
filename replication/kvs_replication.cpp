#include "kvs_replication.h"

static kvs_role_t replication_role;

int kvs_replication_init(kvs_role_t role) {
    replication_role = role;

    return 0;
}

int kvs_replication_set_master(const char* ip, int port) {
    // 后续在这里保存 master IP 和 port
    return 0;
}

int kvs_replication_append(int argc, char* argv[]) {
    if (replication_role != KVS_ROLE_MASTER) {
        return 0;
    }

    /*
     * Master 在这里把已经成功执行的
     * SET / DEL / MOD 命令发送给 Replica。
     */

    return 0;
}

int kvs_replication_full_sync(int fd) {
    /*
     * 1. 发送 Snapshot
     * 2. Replica 加载 Snapshot
     * 3. 发送 Snapshot 期间产生的 AOF
     */

    return 0;
}

int kvs_replication_sync(int fd) {
    /*
     * Replica 与 Master 建立连接后的同步逻辑。
     */

    return 0;
}

void kvs_replication_destroy() {}