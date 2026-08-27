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

// Master 最多同时维护的 Replica 连接数。
#define MAX_REPLICAS 16

static kvs_role_t g_role = KVS_ROLE_MASTER;

static int replica_fds[MAX_REPLICAS];
static int replica_pending[MAX_REPLICAS];

static pthread_mutex_t replica_mutex = PTHREAD_MUTEX_INITIALIZER;
static int master_fd = -1;
static pthread_t replication_tid;
static volatile int replication_running = 0;
static int replication_replaying = 0;

typedef struct {
    char magic[8];
    uint32_t version;
    uint32_t count;
} snapshot_header_t;

typedef struct {
    uint32_t key_len;
    uint32_t value_len;
} snapshot_record_t;

static int send_all(int fd, const char* data, size_t len);
static int send_frame(int fd, const char* data, size_t len);
static int recv_all(int fd, char* data, size_t len);
static int send_full_snapshot(int fd);
static int send_snapshot_file(int fd, const char* filename, const char* command);

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

// 初始化角色、Replica 连接槽位和同步状态。
int kvs_replication_init(kvs_role_t role) {
    g_role = role;

    for (int i = 0; i < MAX_REPLICAS; ++i) {
        replica_fds[i] = -1;
        replica_pending[i] = 0;
    }

    return 0;
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
            replica_pending[i] = 1;
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

    // 待同步的 Replica 暂不接收增量命令，避免顺序被全量快照打乱。
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

// 判断当前是否正在回放 Master 发来的复制命令。
int kvs_replication_is_replaying() { return replication_replaying; }

// 设置 Replica 的复制回放状态，供本地写路径区分复制命令和客户端命令。
void kvs_replication_set_replaying(int value) { replication_replaying = value; }

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

// 判断收到的数据是否为 Replica 握手请求。
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

// 读取一个快照文件，将记录转换为对应存储引擎的 RESP 写命令并发送。
static int send_snapshot_file(int fd, const char* filename, const char* command) {
    FILE* fp = fopen(filename, "rb");
    if (fp == NULL) {
        return errno == ENOENT ? 0 : -1;
    }

    snapshot_header_t header;
    if (fread(&header, sizeof(header), 1, fp) != 1 || memcmp(header.magic, "KVSDB01", 7) != 0 ||
        header.version != 1) {
        fclose(fp);
        return -1;
    }

    for (uint32_t i = 0; i < header.count; ++i) {
        snapshot_record_t record;
        if (fread(&record, sizeof(record), 1, fp) != 1) {
            fclose(fp);
            return -1;
        }

        char* key = (char*)kvs_malloc(record.key_len + 1);
        char* value = (char*)kvs_malloc(record.value_len + 1);
        if (key == NULL || value == NULL || fread(key, record.key_len, 1, fp) != 1 ||
            fread(value, record.value_len, 1, fp) != 1) {
            kvs_free(key);
            kvs_free(value);
            fclose(fp);
            return -1;
        }
        key[record.key_len] = '\0';
        value[record.value_len] = '\0';

        size_t command_len = strlen(command);
        if (record.key_len > SIZE_MAX - record.value_len - command_len - 64) {
            kvs_free(key);
            kvs_free(value);
            fclose(fp);
            return -1;
        }

        size_t size = command_len + record.key_len + record.value_len + 64;
        char* buffer = (char*)kvs_malloc(size);
        if (buffer == NULL) {
            kvs_free(key);
            kvs_free(value);
            fclose(fp);
            return -1;
        }
        int length =
            snprintf(buffer, size, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", command_len,
                     command, (size_t)record.key_len, key, (size_t)record.value_len, value);
        int result = send_frame(fd, buffer, (size_t)length);
        kvs_free(buffer);
        kvs_free(key);
        kvs_free(value);
        if (result < 0) {
            fclose(fp);
            return -1;
        }
    }

    fclose(fp);
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

//把 Master 当前四种存储引擎的数据文件全部发送给某个
// Replica，用于一次完整的数据重同步
static int send_full_snapshot(int fd) {
    const char* filenames[] = {"../data/array.data", "../data/rbtree.data", "../data/hash.data",
                               "../data/skiptable.data"};
    const char* commands[] = {"SET", "RSET", "HSET", "SSET"};
    for (int i = 0; i < 4; ++i) {
        if (send_snapshot_file(fd, filenames[i], commands[i]) < 0) {
            return -1;
        }
    }
    return 0;
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

// Replica 启动复制
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

void kvs_replication_destroy() {
    if (g_role == KVS_ROLE_REPLICA) {
        kvs_replication_stop();
    }
}

void* replication_thread(void* arg) {
    int fd = *(int*)arg;

    // Ignore the framed response to the handshake.
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

    while (replication_running) {
        if (recv_all(fd, (char*)&net_len, sizeof(net_len)) < 0) {
            break;
        }
        uint32_t command_len = ntohl(net_len);
        if (command_len == 0 || command_len > INT_MAX) {
            break;
        }

        buffer = (char*)kvs_malloc((size_t)command_len + 1);
        if (buffer == NULL || recv_all(fd, buffer, command_len) < 0) {
            kvs_free(buffer);
            break;
        }
        buffer[command_len] = '\0';

        if (strcmp(buffer, replication_reset) == 0) {
            kvs_reset_data();
            kvs_free(buffer);
            continue;
        }

        kvs_replication_set_replaying(1);

        // 响应也使用动态内存，避免长 value 被固定响应缓冲区截断。
        size_t response_size = (size_t)command_len + 1;
        char* response = (char*)kvs_malloc(response_size);
        if (response != NULL && response_size <= INT_MAX) {
            kvs_protocol(buffer, (int)command_len, response, (int)response_size);
        }
        kvs_free(response);

        kvs_free(buffer);

        kvs_replication_set_replaying(0);
    }

    replication_running = 0;

    return NULL;
}