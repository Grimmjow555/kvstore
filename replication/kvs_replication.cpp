#include "kvs_replication.h"
#include "kvstore.h"
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define MAX_REPLICAS 16

static kvs_role_t g_role = KVS_ROLE_MASTER;

static int replica_fds[MAX_REPLICAS];

static pthread_mutex_t replica_mutex = PTHREAD_MUTEX_INITIALIZER;
static int master_fd = -1;
static pthread_t replication_tid;
static volatile int replication_running = 0;

void* replication_thread(void* arg);

int kvs_replication_init(kvs_role_t role) {
    g_role = role;

    for (int i = 0; i < MAX_REPLICAS; ++i) {
        replica_fds[i] = -1;
    }

    return 0;
}

int kvs_replication_add_replica(int fd) {
    if (g_role != KVS_ROLE_MASTER) {
        return -1;
    }

    pthread_mutex_lock(&replica_mutex);

    for (int i = 0; i < MAX_REPLICAS; ++i) {

        if (replica_fds[i] == -1) {

            replica_fds[i] = fd;

            pthread_mutex_unlock(&replica_mutex);

            printf("[REPLICATION] replica connected fd=%d\n", fd);

            return 0;
        }
    }

    pthread_mutex_unlock(&replica_mutex);

    return -1;
}

void kvs_replication_remove_replica(int fd) {
    pthread_mutex_lock(&replica_mutex);

    for (int i = 0; i < MAX_REPLICAS; ++i) {

        if (replica_fds[i] == fd) {

            close(replica_fds[i]);

            replica_fds[i] = -1;

            printf("[REPLICATION] replica removed fd=%d\n", fd);

            break;
        }
    }

    pthread_mutex_unlock(&replica_mutex);
}

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

static int send_frame(int fd, const char* data, size_t len) {
    uint32_t net_len = htonl((uint32_t)len);
    if (send_all(fd, (const char*)&net_len, sizeof(net_len)) < 0) {
        return -1;
    }
    return send_all(fd, data, len);
}

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

int kvs_replication_append(int argc, char* argv[]) {
    if (g_role != KVS_ROLE_MASTER) {
        return 0;
    }

    if (argc <= 0 || argv == NULL) {
        return -1;
    }

    /*
     * 计算 RESP 命令大小
     */
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

    /*
     * 发送给所有 Replica
     */
    pthread_mutex_lock(&replica_mutex);

    for (int i = 0; i < MAX_REPLICAS; ++i) {

        int fd = replica_fds[i];

        if (fd == -1) {
            continue;
        }

        if (send_frame(fd, buffer, (size_t)offset) < 0) {

            close(fd);

            replica_fds[i] = -1;

            printf("[REPLICATION] replica disconnected fd=%d\n", fd);
        }
    }

    pthread_mutex_unlock(&replica_mutex);

    kvs_free(buffer);

    return 0;
}

static int replication_replaying = 0;

int kvs_replication_is_replaying() { return replication_replaying; }

void kvs_replication_set_replaying(int value) { replication_replaying = value; }

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

int kvs_replication_is_handshake(const char* data, int length) {
    static const char handshake[] = "*1\r\n$7\r\nREPLICA\r\n";
    return data != NULL && length == (int)(sizeof(handshake) - 1) &&
           memcmp(data, handshake, sizeof(handshake) - 1) == 0;
}

int kvs_replication_accept_handshake(int fd, const char* data, int length) {
    if (!kvs_replication_is_handshake(data, length)) {
        return 0;
    }
    return kvs_replication_add_replica(fd) == 0 ? 1 : -1;
}

static const char* replication_handshake = "*1\r\n$7\r\nREPLICA\r\n";

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

extern int kvs_protocol(char* msg, int length, char* response, int response_size);

void* replication_thread(void* arg) {
    int fd = *(int*)arg;

    char buffer[1024 * 1024 + 1];

    // Ignore the framed response to the handshake.
    uint32_t net_len;
    if (recv_all(fd, (char*)&net_len, sizeof(net_len)) < 0) {
        replication_running = 0;
        return NULL;
    }
    uint32_t response_len = ntohl(net_len);
    if (response_len > sizeof(buffer) - 1 || recv_all(fd, buffer, response_len) < 0) {
        replication_running = 0;
        return NULL;
    }

    while (replication_running) {
        if (recv_all(fd, (char*)&net_len, sizeof(net_len)) < 0) {
            break;
        }
        uint32_t command_len = ntohl(net_len);
        if (command_len == 0 || command_len > sizeof(buffer) - 1 ||
            recv_all(fd, buffer, command_len) < 0) {
            break;
        }
        buffer[command_len] = '\0';

        kvs_replication_set_replaying(1);

        /*
         * 直接执行 Master 发来的 RESP 命令
         */
        char response[1024];

        kvs_protocol(buffer, (int)command_len, response, sizeof(response));

        kvs_replication_set_replaying(0);
    }

    replication_running = 0;

    return NULL;
}