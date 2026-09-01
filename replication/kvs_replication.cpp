#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_replication.h"
#include "kvs_skiptable.h"
#include "kvstore.h"
#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

extern int kvs_protocol(char* msg, int length, char* response, int response_size);

#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif
#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif
#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif
#if ENABLE_SKIPTABLE
extern kvs_skiptable_t global_skiptable;
#endif

// Master 最多同时维护的 Replica 连接数。
#define MAX_REPLICAS 16

static kvs_role_t g_role = KVS_ROLE_MASTER;

static int replica_fds[MAX_REPLICAS]; //记录连接的fd
static int replica_pending[MAX_REPLICAS]; // 记录是全量同步的状态，1为准备全量同步，0为结束全量同步

static pthread_mutex_t replica_mutex = PTHREAD_MUTEX_INITIALIZER;
static int master_fd = -1;

static pthread_t replication_tid;
static volatile int replication_running = 0;
static int replication_replaying = 0;

/*
 * 发送方向：Replica -> Master
 *
 * 作用：
 *   Replica 连接 Master 后，通过该消息向 Master 声明自己的身份，
 *   请求 Master 将当前连接加入 Replica 列表。
 */
static const char* replication_handshake = "*1\r\n$7\r\nREPLICA\r\n";
/* 发送方向：Master -> Replica
 *
 * 作用：
 *   通知 Replica 当前数据需要重新同步。
 *
 *   Replica 收到该命令后，应当：
 *
 *     1. 清空当前数据；
 *     2. 重新初始化各个 KV 存储结构；
 *     3. 接收 Master 发送的完整快照；
 *     4. 完成全量同步后，再继续接收后续增量命令。
 *
 * 通常与 send_full_snapshot() 配合使用：
 *
 *   REPLICA_RESET
 *          ↓
 *   send_full_snapshot()
 *          ↓
 *   SET / RSET / HSET / SSET ...
 */
static const char* replication_reset = "*1\r\n$13\r\nREPLICA_RESET\r\n";

void* replication_thread(void* arg);

// ==================== 公共状态与初始化 ====================

// 初始化角色、Replica 连接槽位和同步状态。
int kvs_replication_init(kvs_role_t role) {
    g_role = role;

    for (int i = 0; i < MAX_REPLICAS; ++i) {
        replica_fds[i] = -1;
        replica_pending[i] = 0;
    }

    return 0;
}

// ==================== 公共网络工具 ====================

// TCP send() 可能只发送部分数据，因此循环直到完成整个缓冲区。
static int send_all(int fd, const char* data, size_t len) {
    size_t sent = 0;

    while (sent < len) {

        ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return -1;
        }

        sent += n;
    }

    return 0;
}

// 发送自定义复制帧：4 字节网络字节序长度 + RESP 负载。
static int send_frame(int fd, const char* data, size_t len) {
    if (len > UINT32_MAX) {
        return -1;
    }

    uint32_t net_len = htonl((uint32_t)len);
    if (send_all(fd, (const char*)&net_len, sizeof(net_len)) < 0) {
        return -1;
    }
    return send_all(fd, data, len);
}

// TCP recv() 可能只接收部分数据，因此循环直到读取指定长度。
static int recv_all(int fd, char* data, size_t len) {
    size_t received = 0;
    while (received < len) {
        ssize_t n = recv(fd, data + received, len - received, 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return -1;
        }
        received += (size_t)n;
    }
    return 0;
}

// ==================== Master 端函数 ====================

// 判断收到的数据是否为 Replica 握手请求。返回 1 表示是握手请求。
int kvs_replication_is_handshake(const char* data, int length) {
    static const char handshake[] = "*1\r\n$7\r\nREPLICA\r\n";
    return data != NULL && length == (int)(sizeof(handshake) - 1) &&
           memcmp(data, handshake, sizeof(handshake) - 1) == 0;
}

// 校验并注册 Replica 发来的握手请求。
int kvs_replication_accept_handshake(int fd, const char* data, int length) {
    if (!kvs_replication_is_handshake(data, length)) {
        return 0;
    }
    return kvs_replication_add_replica(fd) == 0 ? 1 : -1;
}

// 将已建立连接的 Replica 放入 Master 的连接槽位，并标记为待同步。
int kvs_replication_add_replica(int fd) {
    if (g_role != KVS_ROLE_MASTER) {
        return -1;
    }

    pthread_mutex_lock(&replica_mutex);

    for (int i = 0; i < MAX_REPLICAS; ++i) {

        if (replica_fds[i] == -1) {
            replica_fds[i] = fd;
            replica_pending[i] = 1; //
            pthread_mutex_unlock(&replica_mutex);
            printf("[REPLICATION] replica connected fd=%d\n", fd);
            return 0;
        }
    }

    pthread_mutex_unlock(&replica_mutex);

    return -1;
}

// 从 Master 的连接列表中移除 Replica，并关闭对应 socket。
void kvs_replication_remove_replica(int fd) {
    pthread_mutex_lock(&replica_mutex);

    for (int i = 0; i < MAX_REPLICAS; ++i) {

        if (replica_fds[i] == fd) {
            close(replica_fds[i]);
            replica_fds[i] = -1;
            replica_pending[i] = 0;
            printf("[REPLICATION] replica removed fd=%d\n", fd);
            break;
        }
    }

    pthread_mutex_unlock(&replica_mutex);
}

// 将 Master 上成功执行的写命令编码为 RESP，并推送给已完成同步的 Replica。
int kvs_replication_append(int argc, char* argv[]) {

    if (g_role != KVS_ROLE_MASTER) {
        return 0;
    }

    if (argc <= 0 || argv == NULL) {
        return -1;
    }

    // 为命令头、参数长度字段和参数内容预留空间。
    size_t size = 32;

    for (int i = 0; i < argc; ++i) {
        size += strlen(argv[i]) + 32;
    }

    char* buffer = (char*)kvs_malloc(size);

    if (!buffer) {
        return -1;
    }

    int offset = 0;

    offset += snprintf(buffer + offset, size - offset, "*%d\r\n", argc);

    for (int i = 0; i < argc; ++i) {

        offset +=
            snprintf(buffer + offset, size - offset, "$%zu\r\n%s\r\n", strlen(argv[i]), argv[i]);
    }

    //确保主库在并发处理 Replica
    //连接、断开、全量同步、重同步时，向各个从库发送增量命令时不会踩到竞态
    pthread_mutex_lock(&replica_mutex);

    for (int i = 0; i < MAX_REPLICAS; ++i) {

        int fd = replica_fds[i];

        if (fd < 0) {
            continue;
        }
        if (replica_pending[i]) {
            continue;
        }

        if (send_frame(fd, buffer, (size_t)offset) < 0) {

            close(fd);

            replica_fds[i] = -1;
            replica_pending[i] = 0;

            printf("[REPLICATION] replica disconnected fd=%d\n", fd);
        }
    }

    pthread_mutex_unlock(&replica_mutex);

    kvs_free(buffer);
    return 0;
}

// 将一条内存中的 key/value 编码为 RESP 命令并发送给 Replica。
static int send_memory_record(int fd, const char* command, const char* key, const char* value) {
    if (command == NULL || key == NULL || value == NULL) {
        return -1;
    }

    size_t command_len = strlen(command);
    size_t key_len = strlen(key);
    size_t value_len = strlen(value);
    if (command_len > SIZE_MAX - key_len - value_len - 64) {
        return -1;
    }

    size_t size = command_len + key_len + value_len + 64;
    char* buffer = (char*)kvs_malloc(size);
    if (buffer == NULL) {
        return -1;
    }

    int length = snprintf(buffer, size, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                          command_len, command, key_len, key, value_len, value);
    int result = length < 0 || (size_t)length >= size ? -1 : send_frame(fd, buffer, length);
    kvs_free(buffer);
    return result;
}

#if ENABLE_RBTREE
static int send_rbtree_records(int fd, rbtree_node* node, rbtree_node* nil) {
    if (node == nil) {
        return 0;
    }
    if (send_rbtree_records(fd, node->left, nil) < 0 ||
        send_memory_record(fd, "RSET", node->key, (const char*)node->value) < 0) {
        return -1;
    }
    return send_rbtree_records(fd, node->right, nil);
}
#endif

// 遍历 Master 当前内存中的四种存储结构，完成 Replica 全量重同步。
static int send_full_snapshot(int fd) {
#if ENABLE_ARRAY
    for (int i = 0; i < global_array.max_idx; ++i) {
        if (global_array.table[i].key != NULL &&
            send_memory_record(fd, "SET", global_array.table[i].key, global_array.table[i].value) <
                0) {
            return -1;
        }
    }
#endif
#if ENABLE_RBTREE
    if (send_rbtree_records(fd, global_rbtree.root, global_rbtree.nil) < 0) {
        return -1;
    }
#endif
#if ENABLE_HASH
    for (int i = 0; i < global_hash.max_slots; ++i) {
        for (hashnode_t* node = global_hash.nodes[i]; node != NULL; node = node->next) {
            if (send_memory_record(fd, "HSET", node->key, node->value) < 0) {
                return -1;
            }
        }
    }
#endif
#if ENABLE_SKIPTABLE
    for (Node* node = global_skiptable.header->forward[0]; node != NULL; node = node->forward[0]) {
        if (send_memory_record(fd, "SSET", node->key, node->value) < 0) {
            return -1;
        }
    }
#endif
    return 0;
}

void kvs_replication_finish_handshake(int fd) {
    if (g_role != KVS_ROLE_MASTER) {
        return;
    }

    pthread_mutex_lock(&replica_mutex);
    int slot = -1;
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        if (replica_fds[i] == fd && replica_pending[i]) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        pthread_mutex_unlock(&replica_mutex);
        return;
    }

    if (send_full_snapshot(fd) < 0) {
        close(fd);
        replica_fds[slot] = -1;
        replica_pending[slot] = 0;
        pthread_mutex_unlock(&replica_mutex);
        return;
    }

    if (replica_fds[slot] == fd) {
        replica_pending[slot] = 0;
    }
    pthread_mutex_unlock(&replica_mutex);
}

// Master 主动要求所有 Replica 重新同步
int kvs_replication_resync() {
    if (g_role != KVS_ROLE_MASTER) {
        return 0;
    }

    pthread_mutex_lock(&replica_mutex);
    int result = 0;
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        int fd = replica_fds[i];
        if (fd < 0 || replica_pending[i]) {
            continue;
        }
        replica_pending[i] = 1; //准备全量同步
        if (send_frame(fd, replication_reset, strlen(replication_reset)) < 0 ||
            send_full_snapshot(fd) < 0) {
            close(fd);
            replica_fds[i] = -1;
            replica_pending[i] = 0;
            result = -1;
            continue;
        }
        replica_pending[i] = 0; //已完成全量同步
    }
    pthread_mutex_unlock(&replica_mutex);
    return result;
}

// ==================== Replica 端函数 ====================

// Replica 主动与 Master 建立 TCP 连接。
int kvs_replication_connect_master(const char* ip, int port) {

    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        return -1;
    }

    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {

        close(fd);

        return -1;
    }

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {

        close(fd);

        return -1;
    }

    printf("[REPLICATION] connected to master %s:%d\n", ip, port);

    master_fd = fd;

    return fd;
}

// 判断当前是否正在回放 Master 发来的复制命令。
int kvs_replication_is_replaying() { return replication_replaying; }

// 设置 Replica 的复制回放状态，供本地写路径区分复制命令和客户端命令。
void kvs_replication_set_replaying(int value) { replication_replaying = value; }

// 启动 Replica 的同步线程，并发送握手帧。
int kvs_replication_start() {
    if (g_role != KVS_ROLE_REPLICA || master_fd < 0 || replication_running) {
        return -1;
    }
    replication_running = 1;
    if (send_frame(master_fd, replication_handshake, strlen(replication_handshake)) < 0) {
        replication_running = 0;
        close(master_fd);
        master_fd = -1;
        return -1;
    }
    if (pthread_create(&replication_tid, NULL, replication_thread, &master_fd) != 0) {
        replication_running = 0;
        close(master_fd);
        master_fd = -1;
        return -1;
    }
    return 0;
}

// 停止 Replica 同步线程并关闭 Master 连接。
void kvs_replication_stop() {
    if (!replication_running) {
        return;
    }
    replication_running = 0;
    shutdown(master_fd, SHUT_RDWR);
    pthread_join(replication_tid, NULL);
    close(master_fd);
    master_fd = -1;
}

// 销毁 Replica 端的复制资源。
void kvs_replication_destroy() {
    if (g_role == KVS_ROLE_REPLICA) {
        kvs_replication_stop();
    }
}

// ==================== Replica 回放线程 ====================

void* replication_thread(void* arg) {
    int fd = *(int*)arg;

    // =============================================
    // 块1：忽略握手响应（Master 回复的 +OK）
    // =============================================
    // Replica 发送 REPLICA 命令后，Master 会回复一个帧（如 "+OK\r\n"）。
    // 这里读取并丢弃该响应，使 socket 进入干净的命令接收状态。
    uint32_t net_len;
    if (recv_all(fd, (char*)&net_len, sizeof(net_len)) < 0) {
        replication_running = 0;
        return NULL;
    }
    uint32_t response_len = ntohl(net_len);
    char* buffer = (char*)kvs_malloc((size_t)response_len + 1);
    if (buffer == NULL || recv_all(fd, buffer, response_len) < 0) {
        kvs_free(buffer);
        replication_running = 0;
        return NULL;
    }
    kvs_free(buffer);

    // =============================================
    // 块2：主循环——持续接收增量命令
    // =============================================
    while (replication_running) {
        // ---- 2.1 接收命令长度头（4 字节，网络序） ----
        if (recv_all(fd, (char*)&net_len, sizeof(net_len)) < 0) {
            break;
        }
        uint32_t command_len = ntohl(net_len);
        if (command_len == 0 || command_len > INT_MAX) {
            break;
        }

        // ---- 2.2 接收命令数据体 ----
        buffer = (char*)kvs_malloc((size_t)command_len + 1);
        if (buffer == NULL || recv_all(fd, buffer, command_len) < 0) {
            kvs_free(buffer);
            break;
        }
        buffer[command_len] = '\0';

        // ---- 2.3 特殊命令：重置数据库 ----
        // 如果 Master 推送了 "RESET" 命令，则清空本地所有数据。
        // 通常用于全量同步前的清理，确保数据一致性。
        if (strcmp(buffer, replication_reset) == 0) {
            kvs_reset_data();
            kvs_free(buffer);
            continue;
        }

        // ---- 2.4 正常命令执行（标记为“回放中”） ----
        // 设置标志，防止 AOF 等模块再次记录这条命令（避免无限循环）。
        kvs_replication_set_replaying(1);

        // 分配响应缓冲区（虽然此处未实际发送响应，但为 kvs_protocol 提供了存储空间）
        char response[128] = {0};
        if (response != NULL) {
            // 执行命令（如 SET/DEL），写入 response（但实际未使用）
            kvs_protocol(buffer, (int)command_len, response, 128);
        }

        kvs_free(buffer);

        // 清除回放标志
        kvs_replication_set_replaying(0);
    }

    // =============================================
    // 块3：循环退出清理
    // =============================================
    // 当连接断开、协议错误或主动停止时，将运行标志置 0，线程结束。
    replication_running = 0;
    return NULL;
}