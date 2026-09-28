

#include "kvs_config.h"
#include "kvs_replication.h"
#include "kvs_shutdown.h"
#include "nty_coroutine.h"
#include <arpa/inet.h> // htonl, ntohl
#include <arpa/inet.h>
#include <errno.h>
#include <stdint.h> // uint32_t
#include <stdio.h>
#include <stdlib.h> // malloc/free/realloc
#include <string.h> // memcpy, memset
#include <sys/socket.h>
#include <unistd.h> // recv, send, close

#define MAX_ALLOWED_LEN 1024 * 1024 // 1MBS
#define BUFFER_SIZE 1024

typedef int (*msg_handler)(char* msg, int length, char* response, int response_size);
static msg_handler kvs_handler;

struct nty_server_args {
    char bind_ip[64];
    unsigned short port;
};

// 已接入连接的 fd 表。协程调度器是单线程的，accept 与各 reader 协程不会并发
// 修改这张表，因此不需要加锁。关闭服务时要靠它逐个 shutdown，
// 让阻塞在 recv 上的 reader 协程返回，否则调度器永远不会结束。
static int* g_client_fds = NULL;
static int g_client_fd_count = 0;
static int g_client_fd_capacity = 0;

static void nty_client_fd_add(int fd) {
    if (g_client_fd_count == g_client_fd_capacity) {
        int new_capacity = g_client_fd_capacity > 0 ? g_client_fd_capacity * 2 : 64;
        int* new_fds = (int*)realloc(g_client_fds, sizeof(int) * (size_t)new_capacity);
        if (new_fds == NULL) {
            // 极端情况下退化为不跟踪：该连接在关闭时不会被主动 shutdown。
            kvs_log(KVS_LOG_WARN, "[NETWORK] client fd table grow failed, fd %d not tracked", fd);
            return;
        }
        g_client_fds = new_fds;
        g_client_fd_capacity = new_capacity;
    }
    g_client_fds[g_client_fd_count++] = fd;
}

static void nty_client_fd_remove(int fd) {
    for (int i = 0; i < g_client_fd_count; ++i) {
        if (g_client_fds[i] == fd) {
            g_client_fds[i] = g_client_fds[--g_client_fd_count];
            return;
        }
    }
}

// 关闭一条客户端连接：先从表中摘掉，再关闭 fd。
static void nty_client_close(int fd) {
    nty_client_fd_remove(fd);
    close(fd);
}

// 监听 fd 的当前持有者。-1 表示已经关闭。
// server 协程与关闭协程都运行在同一个调度线程上，直接读写即可。
static int g_listen_fd = -1;

// 监听地址：关闭时用它做一次自连接，把阻塞在 accept() 上的协程唤醒。
// 绑定到 INADDR_ANY 时改写成回环地址（0.0.0.0 不能作为连接目标）。
static struct sockaddr_in g_wake_addr;
static int g_wake_addr_valid = 0;

// 对监听地址发起一次一次性连接并立即关闭。accept 侧收到这个连接后会看到
// 停止标志，把连接关掉并退出 accept 循环。
static void nty_wake_accept(void) {
    if (g_listen_fd < 0 || !g_wake_addr_valid) {
        return;
    }

    // 允许重试：accept 协程可能刚好在处理上一个连接，此时第一次连接可能失败。
    for (int attempt = 0; attempt < 3; ++attempt) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            kvs_log(KVS_LOG_WARN, "[NETWORK] shutdown wake-up socket failed: %s", strerror(errno));
            return;
        }
        if (connect(fd, (struct sockaddr*)&g_wake_addr, sizeof(g_wake_addr)) == 0) {
            close(fd);
            return;
        }
        kvs_log(KVS_LOG_WARN, "[NETWORK] shutdown wake-up connect failed: %s", strerror(errno));
        close(fd);
        nty_schedule_sched_sleepdown(nty_coroutine_get_sched()->curr_thread, 50);
        nty_coroutine_yield(nty_coroutine_get_sched()->curr_thread);
    }
}

// 定时检查停止标志的辅助协程。
//
// 需要这样一个协程的原因：NtyCo 的 accept()/recv() 内部都用 nty_poll_inner(...,
// timeout = 1) 等待，而 nty_schedule_sched_wait() 对 timeout == 1 有特殊处理
// （见 NtyCo-master/core/nty_schedule.c: `if (timeout == 1) return;`），
// 不会把协程挂进睡眠树。也就是说这些等待只能被 fd 上的事件唤醒，既不会超时，
// 也不会因为 fd 被关闭而返回——server 协程空载时就一直卡在 accept() 里。
//
// 所以这里的做法是：定时唤醒自己检查停止标志，发现停止请求后
//   1. shutdown 所有已接入的连接，让 reader 协程从 recv 上返回；
//   2. 对监听地址发起一次连接，用这个“事件”把阻塞在 accept() 上的 server
//      协程唤醒，它随后会因为停止标志退出循环并关闭监听 fd。
static void shutdown_watcher(void* arg) {
    (void)arg;

    nty_coroutine* co = nty_coroutine_get_sched()->curr_thread;

    while (!g_kvs_shutdown) {
        // 注意：NtyCo 的 nty_coroutine_sleep(msecs > 0) 只把协程挂进睡眠树、
        // 并不会主动让出，必须紧跟一次 yield，否则这里会退化成忙等，
        // 把整个调度线程卡死在这个协程里。
        nty_schedule_sched_sleepdown(co, KVS_SHUTDOWN_POLL_MS);
        nty_coroutine_yield(co);
    }

    for (int i = 0; i < g_client_fd_count; ++i) {
        shutdown(g_client_fds[i], SHUT_RDWR);
    }

    nty_wake_accept();
}

// 循环接收，直到读满指定字节数或出错
int recv_full(int fd, void* buffer, size_t len) {
    char* p = (char*)buffer;
    size_t received = 0;
    while (received < len) {
        ssize_t n = recv(fd, p + received, len - received, 0);
        if (n <= 0) {
            return -1; // 连接关闭或错误
        }
        received += n;
    }
    return 0;
}

// 循环发送，直到全部数据发出或出错
int send_full(int fd, const void* buffer, size_t len) {
    const char* p = (const char*)buffer;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, 0);
        if (n <= 0) {
            return -1; // 连接关闭或错误
        }
        sent += n;
    }
    return 0;
}

#if 1
// 头部存储长度的协议，并且根据长度设置buffer大小
void server_reader(void* arg) {
    // int fd = *(int*)arg;
    int fd = (int)(intptr_t)arg; // 将 void* 转换为 int

    while (1) {
        // 1. 读取 4 字节长度头
        uint32_t net_len;
        if (recv_full(fd, &net_len, sizeof(net_len)) != 0) {
            kvs_replication_remove_replica(fd);
            nty_client_close(fd);
            break;
        }
        uint32_t msg_len = ntohl(net_len);

        // 2. 检查消息长度是否在允许范围内
        if (msg_len == 0 || msg_len > MAX_ALLOWED_LEN) {
            const char* err = "-ERR invalid message length\r\n";
            send(fd, err, strlen(err), 0);
            kvs_replication_remove_replica(fd);
            nty_client_close(fd);
            break;
        }

        // 3. 动态分配接收缓冲区
        char* buf = (char*)malloc(msg_len + 1);
        if (!buf) {
            kvs_replication_remove_replica(fd);
            nty_client_close(fd);
            break;
        }

        // 4. 读取消息体
        if (recv_full(fd, buf, msg_len) != 0) {
            free(buf);
            kvs_replication_remove_replica(fd);
            nty_client_close(fd);
            break;
        }
        buf[msg_len] = '\0'; // 确保字符串结束

        // 5. 动态分配响应缓冲区，并在头部额外预留 4 字节长度头空间，
        //    这样长度头和响应体可以合并成一次 send 发出。
        char* resp_frame = (char*)malloc(sizeof(uint32_t) + MAX_ALLOWED_LEN + 1);
        if (!resp_frame) {
            free(buf);
            kvs_replication_remove_replica(fd);
            nty_client_close(fd);
            break;
        }
        char* response = resp_frame + sizeof(uint32_t);

        // 6. 处理请求
        int is_replica = kvs_replication_accept_handshake(fd, buf, (int)msg_len) == 1;
        int slength;
        if (is_replica) {
            slength = snprintf(response, MAX_ALLOWED_LEN + 1, "+OK\r\n");
        } else {
            slength = kvs_handler(buf, (int)msg_len, response, MAX_ALLOWED_LEN + 1);
        }
        if (slength < 0) {
            slength = 0; // 或发送错误响应
        }

        // 防止响应长度超过分配大小（理论上 kvs_handler 应保证不超）
        if (slength > MAX_ALLOWED_LEN) {
            slength = MAX_ALLOWED_LEN; // 截断或关闭连接，此处简单截断
        }

        // 7. 发送响应：把长度头写进预留的 4 字节，长度头与响应体一次 send 发完，
        //    避免拆成两个小包写入时被 TCP 小包延迟拖慢。
        uint32_t resp_net_len = htonl((uint32_t)slength);
        memcpy(resp_frame, &resp_net_len, sizeof(resp_net_len));
        int send_ok = 1;
        if (send_full(fd, resp_frame, sizeof(resp_net_len) + (size_t)slength) != 0) {
            send_ok = 0;
        }

        // 8. 释放内存
        free(buf);
        free(resp_frame);

        if (!send_ok) {
            kvs_replication_remove_replica(fd);
            nty_client_close(fd);
            break;
        }
        if (is_replica) {
            kvs_replication_finish_handshake(fd);
        }
    }
}
#else
// 原始的读写
void server_reader(void* arg) {
    int fd = *(int*)arg;
    int ret = 0;

    while (1) {

        char buf[1024] = {0};
        ret = recv(fd, buf, 1024, 0);
        if (ret > 0) {
            // printf("read from server: %.*s\n", ret, buf);

            char response[1024] = {0};

            // int slength = kvs_protocol(buf, ret, response);
            int slength = kvs_handler(buf, ret, response);

            ret = send(fd, response, slength, 0);

            // ret = send(fd, buf, sizeof(buf), 0);
            if (ret == -1) {
                close(fd);
                break;
            }
        } else if (ret == 0) {
            close(fd);
            break;
        }
    }
}
#endif

static void server(void* arg) {

    struct nty_server_args* args = (struct nty_server_args*)arg;
    unsigned short port = args->port;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        kvs_log(KVS_LOG_ERROR, "[NETWORK] socket create failed: %s", strerror(errno));
        return;
    }
    // 服务重启时避免被上一次残留的 TIME_WAIT 连接占用端口。
    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in local, remote;
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    if (args->bind_ip[0] == '\0' || strcmp(args->bind_ip, "*") == 0) {
        local.sin_addr.s_addr = INADDR_ANY;
    } else if (inet_pton(AF_INET, args->bind_ip, &local.sin_addr) != 1) {
        kvs_log(KVS_LOG_ERROR, "[NETWORK] invalid bind ip: %s", args->bind_ip);
        return;
    }
    if (bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in)) != 0) {
        // 监听失败必须退出：继续走 accept 会在无效 fd 上忙等，既不能服务也会占满 CPU。
        kvs_log(KVS_LOG_ERROR, "[NETWORK] bind failed on port %d: %s", port, strerror(errno));
        close(fd);
        return;
    }
    if (listen(fd, 20) != 0) {
        kvs_log(KVS_LOG_ERROR, "[NETWORK] listen failed on port %d: %s", port, strerror(errno));
        close(fd);
        return;
    }
    kvs_log(KVS_LOG_INFO, "[NETWORK] listen port: %d", port);

    // 记录监听 fd 与可连接的本地地址，供 shutdown_watcher 在停止时唤醒 accept。
    g_listen_fd = fd;
    g_wake_addr = local;
    if (g_wake_addr.sin_addr.s_addr == htonl(INADDR_ANY)) {
        g_wake_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    }
    g_wake_addr_valid = 1;

    while (!g_kvs_shutdown) {
        socklen_t len = sizeof(struct sockaddr_in);
        int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
        if (cli_fd < 0) {
            // 停止过程中的监听 fd 关闭或本次连接异常：前者退出循环。
            if (g_kvs_shutdown) {
                break;
            }
            continue;
        }

        if (g_kvs_shutdown) {
            // 这是关闭流程里用于唤醒 accept 的自连接，直接丢弃。
            close(cli_fd);
            break;
        }

        nty_client_fd_add(cli_fd);

        nty_coroutine* read_co;

        nty_coroutine_create(&read_co, server_reader, (void*)(intptr_t)cli_fd);
        // nty_coroutine_create(&read_co, server_reader, &cli_fd);
    }

    // 关闭监听 fd（已接入的客户端连接由 watcher 负责 shutdown）。
    if (g_listen_fd >= 0) {
        g_listen_fd = -1;
        close(fd);
    }
    g_wake_addr_valid = 0;
    kvs_log(KVS_LOG_INFO, "[NETWORK] ntyco accept loop exited");
}

int ntyco_start(const char* bind_ip, unsigned short port, msg_handler handler) {

    // unsigned short port = atoi(argv[1]); //原始情况，直接从命令行读取端口，需要转化为整数

    kvs_handler = handler;

    static struct nty_server_args server_args;
    memset(&server_args, 0, sizeof(server_args));
    server_args.port = port;
    if (bind_ip != NULL) {
        snprintf(server_args.bind_ip, sizeof(server_args.bind_ip), "%s", bind_ip);
    }

    nty_coroutine* co = NULL;
    nty_coroutine_create(&co, server, &server_args);

    // 关闭协调协程：监听停止标志，负责唤醒卡在 accept/recv 上的协程。
    nty_coroutine* watcher = NULL;
    nty_coroutine_create(&watcher, shutdown_watcher, NULL);

    nty_schedule_run();
    return 0;
}
