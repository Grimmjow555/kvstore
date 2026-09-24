#include "kvs_array.h"
#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_replication.h"
#include "kvs_skiptable.h"
#include "kvstore.h"
#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
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
static int replica_ebpf[MAX_REPLICAS]; // 该 Replica 是否使用 eBPF 队列进行实时同步

static pthread_mutex_t replica_mutex = PTHREAD_MUTEX_INITIALIZER;
static int master_fd = -1;

static pthread_t replication_tid;
static volatile int replication_running = 0;
static int replication_replaying = 0;

static char g_master_ip[INET_ADDRSTRLEN] = {0};
static int g_master_port = 0;

#ifdef KVS_ENABLE_RDMA
static int g_rdma_enabled = 0;

int kvs_replication_is_rdma_enabled() { return g_rdma_enabled; }

void kvs_replication_set_rdma_enabled(int enabled) { g_rdma_enabled = enabled ? 1 : 0; }
#endif

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
/* 发送方向：Master -> Replica
 *
 * 作用：
 *   仅在 eBPF 实时同步路径下使用。Master 通过 TCP 发送完全量快照后，
 *   再发送该控制帧，通知 Replica“全量快照已结束，可以开始消费 eBPF 队列”。
 *
 * 这样保证全量快照与后续 eBPF 实时命令之间有一个明确的顺序屏障：
 * 不会出现 Replica 先重放新写入，之后又被快照中的旧值覆盖。
 */
static const char* replication_fullsync_done = "*1\r\n$21\r\nREPLICA_FULLSYNC_DONE\r\n";

void* replication_thread(void* arg);

// ==================== 公共状态与初始化 ====================

// 初始化角色、Replica 连接槽位和同步状态。
int kvs_replication_init(kvs_role_t role) {
    g_role = role;

    for (int i = 0; i < MAX_REPLICAS; ++i) {
        replica_fds[i] = -1;
        replica_pending[i] = 0;
        replica_ebpf[i] = 0;
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
// 长度头与负载合并到同一缓冲区后一次发出，避免拆成两个小包写入触发 TCP 延迟。
static int send_frame(int fd, const char* data, size_t len) {
    if (len > UINT32_MAX) {
        return -1;
    }

    uint32_t net_len = htonl((uint32_t)len);
    size_t total_len = sizeof(net_len) + len;

    char stack_buf[1024];
    char* frame = stack_buf;
    if (total_len > sizeof(stack_buf)) {
        frame = (char*)malloc(total_len);
        if (frame == NULL) {
            return -1;
        }
    }

    memcpy(frame, &net_len, sizeof(net_len));
    if (len > 0) {
        memcpy(frame + sizeof(net_len), data, len);
    }

    int rc = send_all(fd, frame, total_len);

    if (frame != stack_buf) {
        free(frame);
    }

    return rc;
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

    pthread_mutex_lock(&replica_mutex);
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        if (replica_fds[i] == fd) {
            pthread_mutex_unlock(&replica_mutex);
            // 同一个 Replica 在重同步完成后会再次发送 REPLICA 握手。
            // 此时只需要保留 pending 状态，等待 finish_handshake 将其置为 0。
            return 1;
        }
    }
    pthread_mutex_unlock(&replica_mutex);

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
            replica_ebpf[i] = kvs_ebpf_master_available() && kvs_ebpf_peer_is_local(fd);
            pthread_mutex_unlock(&replica_mutex);
            kvs_log(KVS_LOG_INFO, "[REPLICATION] replica connected fd=%d", fd);
            if (replica_ebpf[i]) {
                kvs_log(KVS_LOG_INFO,
                        "[REPLICATION] replica fd=%d will use eBPF realtime sync", fd);
            }
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
            replica_ebpf[i] = 0;
            kvs_log(KVS_LOG_INFO, "[REPLICATION] replica removed fd=%d", fd);
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

    char* buffer = (char*)malloc(size);

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

        int sent = -1;
        if (replica_ebpf[i]) {
            sent = kvs_ebpf_push(buffer, (unsigned int)offset);
            if (sent == 0) {
                continue;
            }

            // eBPF 队列写入失败（例如被回收或队列暂时不可用）时，
            // 该副本回退到 TCP 实时同步；后续命令继续走 TCP。
            kvs_log(KVS_LOG_WARN,
                    "[REPLICATION] eBPF push failed for fd=%d, fallback to TCP sync", fd);
            replica_ebpf[i] = 0;
        }

        if (sent < 0 && send_frame(fd, buffer, (size_t)offset) < 0) {

            close(fd);

            replica_fds[i] = -1;
            replica_pending[i] = 0;
            replica_ebpf[i] = 0;

            kvs_log(KVS_LOG_WARN, "[REPLICATION] replica disconnected fd=%d", fd);
        }
    }

    pthread_mutex_unlock(&replica_mutex);

    free(buffer);
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
    char* buffer = (char*)malloc(size);
    if (buffer == NULL) {
        return -1;
    }

    int length = snprintf(buffer, size, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                          command_len, command, key_len, key, value_len, value);
    int result = length < 0 || (size_t)length >= size ? -1 : send_frame(fd, buffer, length);
    free(buffer);
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

    int need_full_snapshot = 1;
#ifdef KVS_ENABLE_RDMA
    if (kvs_replication_is_rdma_enabled()) {
        // RDMA 模式下，Replica 在发送 TCP REPLICA 握手前已经完成全量同步；
        // 重同步时也会在收到 REPLICA_RESET 后重新走一次 RDMA 全量同步。
        need_full_snapshot = 0;
    }
#endif

    if (need_full_snapshot && send_full_snapshot(fd) < 0) {
        close(fd);
        replica_fds[slot] = -1;
        replica_pending[slot] = 0;
        replica_ebpf[slot] = 0;
        pthread_mutex_unlock(&replica_mutex);
        return;
    }

    // eBPF 实时同步路径下，全量快照与 eBPF 队列是两条独立通道。
    // 这里补一个 TCP 控制帧作为顺序屏障，Replica 收到后才能开始消费队列。
    if (replica_ebpf[slot] &&
        send_frame(fd, replication_fullsync_done, strlen(replication_fullsync_done)) < 0) {
        close(fd);
        replica_fds[slot] = -1;
        replica_pending[slot] = 0;
        replica_ebpf[slot] = 0;
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

        int need_full_snapshot = 1;
#ifdef KVS_ENABLE_RDMA
        if (kvs_replication_is_rdma_enabled()) {
            // RDMA 模式下仅发送 REPLICA_RESET。Replica 会清空数据，
            // 重新连接 Master 的 RDMA 端口拉取快照，然后再次发送 REPLICA 握手。
            need_full_snapshot = 0;
        }
#endif

        if (send_frame(fd, replication_reset, strlen(replication_reset)) < 0 ||
            (need_full_snapshot && send_full_snapshot(fd) < 0) ||
            (need_full_snapshot && replica_ebpf[i] &&
             send_frame(fd, replication_fullsync_done, strlen(replication_fullsync_done)) < 0)) {
            close(fd);
            replica_fds[i] = -1;
            replica_pending[i] = 0;
            replica_ebpf[i] = 0;
            result = -1;
            continue;
        }

        // TCP 全量同步在这里直接结束；RDMA 路径仍保持 pending=1，
        // 等 Replica 完成 RDMA 全量同步并重新发送 REPLICA 握手后由
        // kvs_replication_finish_handshake 再结束 pending。
        if (need_full_snapshot) {
            replica_pending[i] = 0; //已完成全量同步
        }
    }
    pthread_mutex_unlock(&replica_mutex);
    return result;
}

// ==================== Replica 端函数 ====================

// Replica 主动与 Master 建立 TCP 连接。
int kvs_replication_connect_master(const char* ip, int port) {
    if (ip == NULL || strlen(ip) >= INET_ADDRSTRLEN) {
        kvs_log(KVS_LOG_ERROR, "[REPLICATION] invalid master address");
        return -1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        kvs_log(KVS_LOG_ERROR, "[REPLICATION] create socket failed: %s", strerror(errno));
        return -1;
    }

    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {

        close(fd);

        kvs_log(KVS_LOG_ERROR, "[REPLICATION] invalid master ip: %s", ip);
        return -1;
    }

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {

        close(fd);

        kvs_log(KVS_LOG_ERROR, "[REPLICATION] connect to master %s:%d failed: %s", ip, port,
                strerror(errno));
        return -1;
    }

    kvs_log(KVS_LOG_INFO, "[REPLICATION] connected to master %s:%d", ip, port);

    snprintf(g_master_ip, sizeof(g_master_ip), "%s", ip);
    g_master_port = port;

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

    // 本地 Replica 可以打开 Master pin 的 eBPF 队列；失败则继续使用 TCP 实时同步。
    kvs_ebpf_replica_open((unsigned short)g_master_port);

    replication_running = 1;
    if (send_frame(master_fd, replication_handshake, strlen(replication_handshake)) < 0) {
        kvs_log(KVS_LOG_ERROR, "[REPLICATION] failed to send handshake to master");
        kvs_ebpf_replica_close();
        replication_running = 0;
        close(master_fd);
        master_fd = -1;
        return -1;
    }
    if (pthread_create(&replication_tid, NULL, replication_thread, &master_fd) != 0) {
        kvs_log(KVS_LOG_ERROR, "[REPLICATION] failed to create replication thread");
        kvs_ebpf_replica_close();
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
    kvs_ebpf_replica_close();
    kvs_log(KVS_LOG_INFO, "[REPLICATION] replica sync stopped");
}

// 销毁 Replica 端的复制资源。
void kvs_replication_destroy() {
    if (g_role == KVS_ROLE_REPLICA) {
        kvs_replication_stop();
    }
#ifdef KVS_ENABLE_RDMA
    if (g_role == KVS_ROLE_MASTER) {
        kvs_replication_stop_rdma_listener();
    }
#endif
    kvs_ebpf_master_destroy();
    kvs_ebpf_replica_close();
}

// ==================== Replica 回放线程 ====================

void* replication_thread(void* arg) {
    int fd = *(int*)arg;
    int ebpf_active = kvs_ebpf_replica_available();
    char* buffer = NULL;

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
    buffer = (char*)malloc((size_t)response_len + 1);
    if (buffer == NULL || recv_all(fd, buffer, response_len) < 0) {
        free(buffer);
        replication_running = 0;
        return NULL;
    }
    free(buffer);
    buffer = NULL;

    // =============================================
    // 块2：主循环——持续接收增量命令。
    //
    // 若 Replica 成功打开了本地 eBPF 队列，则实时写命令从 eBPF 队列读取，
    // TCP 连接只负责 REPLICA_RESET、全量快照等控制/全量同步帧；
    // 否则保持原来的纯 TCP 实时同步路径。
    // =============================================
    char* ebpf_buffer = NULL;
    int ebpf_ready = 0;
    if (ebpf_active) {
        ebpf_buffer = (char*)malloc(KVS_EBPF_MAX_COMMAND_LEN);
        if (ebpf_buffer == NULL) {
            ebpf_active = 0;
        }
    }

    while (replication_running) {
        // ---- 2.1 优先消费 eBPF 队列中的实时写命令 ----
        if (ebpf_active && ebpf_ready) {
            int processed = 0;
            unsigned int ebpf_len = (unsigned int)KVS_EBPF_MAX_COMMAND_LEN;
            while (processed < 32 && kvs_ebpf_pop(ebpf_buffer, &ebpf_len) == 0) {
                if (ebpf_len == 0) {
                    ebpf_len = (unsigned int)KVS_EBPF_MAX_COMMAND_LEN;
                    continue;
                }

                ebpf_buffer[ebpf_len] = '\0';
                kvs_replication_set_replaying(1);
                char response[128] = {0};
                kvs_protocol(ebpf_buffer, (int)ebpf_len, response, sizeof(response));
                kvs_replication_set_replaying(0);

                ebpf_len = (unsigned int)KVS_EBPF_MAX_COMMAND_LEN;
                ++processed;
            }
        }

        // ---- 2.2 检查 TCP 控制通道 ----
        // eBPF 路径下使用短超时，保证既能及时消费队列，又能收到控制帧；
        // 纯 TCP 路径下保持原有阻塞接收，不改变既有行为。
        if (ebpf_active) {
            struct pollfd pfd;
            pfd.fd = fd;
            pfd.events = POLLIN;
            pfd.revents = 0;

            int poll_ret = poll(&pfd, 1, 5);
            if (poll_ret < 0) {
                if (errno == EINTR) {
                    continue;
                }
                break;
            }
            if (poll_ret == 0) {
                continue;
            }
            if ((pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                break;
            }
            if ((pfd.revents & POLLIN) == 0) {
                continue;
            }
        }

        // ---- 2.3 接收 TCP 帧长度头（4 字节，网络序） ----
        if (recv_all(fd, (char*)&net_len, sizeof(net_len)) < 0) {
            break;
        }
        uint32_t command_len = ntohl(net_len);
        if (command_len == 0 || command_len > INT_MAX) {
            break;
        }

        // ---- 2.4 接收 TCP 帧数据体 ----
        buffer = (char*)malloc((size_t)command_len + 1);
        if (buffer == NULL || recv_all(fd, buffer, command_len) < 0) {
            free(buffer);
            break;
        }
        buffer[command_len] = '\0';

        // ---- 2.5 eBPF 全量同步完成标记 ----
        // Master 在 eBPF 路径下会先通过 TCP 发完全量快照，再发送该标记；
        // 收到标记后，后续实时写命令才允许从 eBPF 队列重放。
        if (strcmp(buffer, replication_fullsync_done) == 0) {
            ebpf_ready = 1;
            free(buffer);
            buffer = NULL;
            continue;
        }

        // ---- 2.6 特殊命令：重置数据库 ----
        // 如果 Master 推送了 "RESET" 命令，则清空本地所有数据。
        // 通常用于全量同步前的清理，确保数据一致性。
        if (strcmp(buffer, replication_reset) == 0) {
            // 重置期间禁止消费 eBPF 队列，等待下一次全量同步完成标记。
            ebpf_ready = 0;
            // eBPF 队列中可能还残留重置前的旧命令，直接丢弃，
            // 避免全量同步完成后又重放过期写操作。
            kvs_ebpf_drain();
            kvs_reset_data();
#ifdef KVS_ENABLE_RDMA
            if (kvs_replication_is_rdma_enabled()) {
                // 清空本地数据后，通过 RDMA 从 Master 拉取新的全量快照；
                // 成功后再发送一次 REPLICA 握手，让 Master 结束 pending 状态。
                if (kvs_replication_rdma_full_sync(g_master_ip, g_master_port) != 0) {
                    free(buffer);
                    break;
                }
                if (send_frame(fd, replication_handshake, strlen(replication_handshake)) < 0) {
                    free(buffer);
                    break;
                }
            }
#endif
            free(buffer);
            buffer = NULL;
            continue;
        }

        // ---- 2.7 正常 TCP 命令执行（标记为“回放中”） ----
        // 设置标志，防止 AOF 等模块再次记录这条命令（避免无限循环）。
        kvs_replication_set_replaying(1);

        // 分配响应缓冲区（虽然此处未实际发送响应，但为 kvs_protocol 提供了存储空间）
        char response[128] = {0};
        if (response != NULL) {
            // 执行命令（如 SET/DEL），写入 response（但实际未使用）
            kvs_protocol(buffer, (int)command_len, response, 128);
        }

        free(buffer);
        buffer = NULL;

        // 清除回放标志
        kvs_replication_set_replaying(0);
    }

    // =============================================
    // 块3：循环退出清理
    // =============================================
    // 当连接断开、协议错误或主动停止时，将运行标志置 0，线程结束。
    replication_running = 0;
    free(ebpf_buffer);
    kvs_log(KVS_LOG_INFO, "[REPLICATION] replica sync thread stopped");
    return NULL;
}
