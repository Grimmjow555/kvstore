#include <arpa/inet.h>
#include <errno.h>
#include <iostream>
#include <netinet/in.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "kvs_config.h"
#include "kvs_replication.h"
#include "kvs_shutdown.h"
#include "server.h"

//定义了一个类型别s名msg_handler
//它代表的类型就是“返回值 int、参数 (char*, int, char*, int)的函数指针”。
typedef int (*msg_handler)(char* msg, int length, char* response, int response_size);
static msg_handler kvs_handler;

int kvs_request(struct conn* c) {
    // printf("[kvs_request]recv %d: %s\n", c->rlength, c->rbuffer);

    // 容量留出 4 字节：发送时在响应体前面补长度头，一次 send 发完
    c->wlength =
        kvs_handler(c->rbuffer.data(), c->rlength, c->wbuffer.data(), c->wbuffer.size() - 4);

    return 0;
}

int kvs_response(struct conn* c) {
    // printf("recv %d: %s\n", c->wlength, c->wbuffer);
    return 0;
}

int epfd = 0;

// struct conn conn_list[CONN_SIZE] = {0};
std::vector<conn> conn_list(CONN_SIZE);

// 关闭一条客户端连接：摘掉 epoll 注册、关闭 fd，并把该连接占用的收发缓冲区
// 还给分配器。缓冲区是按连接按需分配的（见 conn::ensure_*），如果断连时只
// close(fd) 而不释放，这块内存会一直挂在这个 fd 对应的 conn 上，直到该 fd 被
// 新连接复用，反复连接/断开会持续累积。
static void close_client_conn(int clientfd) {
    epoll_ctl(epfd, EPOLL_CTL_DEL, clientfd, nullptr);
    // 关闭前先通知复制模块释放槽位：fd 号可能立刻被下一个连接复用，
    // 若复制侧仍保留旧映射，增量数据会被写进普通客户端连接。
    kvs_replication_remove_replica(clientfd);
    close(clientfd);

    if (clientfd >= 0 && (size_t)clientfd < conn_list.size()) {
        // swap 一个空 vector 是 C++11 里确定能把容量释放掉的写法，
        // clear()+shrink_to_fit() 的释放只是非强制建议。
        std::vector<char>().swap(conn_list[clientfd].rbuffer);
        std::vector<char>().swap(conn_list[clientfd].wbuffer);
        conn_list[clientfd].fd = -1;
        conn_list[clientfd].rlength = 0;
        conn_list[clientfd].wlength = 0;
    }
}

static int init_server(const char* bind_ip, unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    // 服务重启时避免被上一次残留的 TIME_WAIT 连接占用端口。
    int reuse = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in servaddr = {0};
    servaddr.sin_family = AF_INET;
    if (bind_ip == nullptr || bind_ip[0] == '\0' || strcmp(bind_ip, "*") == 0) {
        servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, bind_ip, &servaddr.sin_addr) != 1) {
        kvs_log(KVS_LOG_ERROR, "[NETWORK] invalid bind ip: %s", bind_ip);
        return -1;
    }
    servaddr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) == -1) {
        kvs_log(KVS_LOG_ERROR, "[NETWORK] bind failed: %s", strerror(errno));
        return -1;
    }
    listen(sockfd, 10);
    kvs_log(KVS_LOG_INFO, "[NETWORK] listen finished on port %d, listenfd: %d", port, sockfd);

    return sockfd;
}

int set_event(int fd, int event, int flag) {
    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events = event;
    if (flag == 1) { //传入fd
        epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
        return 1;
    } else { //修改fd
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
        return 0;
    }
}

int event_register(int fd, int event) {
    if (fd < 0)
        return -1;
    conn_list[fd].fd = fd;
    conn_list[fd].r_action.recv_callback = recv_cb;
    conn_list[fd].send_callback = send_cb;
    conn_list[fd].rlength = 0;
    conn_list[fd].wlength = 0;
    // 缓冲区在 recv_cb 首次收到数据时按需扩容，这里不再清空：
    // 接收缓冲区只会按 rlength 读取，发送缓冲区每次响应都会被重新写入。

    set_event(fd, event, 1);
    return 0;
}

int accept_cb(int listenfd) {
    struct sockaddr_in clientaddr = {0};
    socklen_t len = sizeof(clientaddr);
    int clientfd = accept(listenfd, (struct sockaddr*)&clientaddr, &len);
    if (clientfd < 0) {
        kvs_log(KVS_LOG_WARN, "[NETWORK] accept error: %d", errno);
        return -1;
    }

    kvs_log(KVS_LOG_DEBUG, "[NETWORK] accept finished, clientfd: %d", clientfd);

    // 给客户端连接设置接收超时：配合下面的读循环，半包请求不会让事件循环
    // 永久卡在 recv 上——每次超时都回到循环头检查停止标志。
    struct timeval recv_timeout;
    recv_timeout.tv_sec = 0;
    recv_timeout.tv_usec = KVS_SHUTDOWN_POLL_MS * 1000;
    setsockopt(clientfd, SOL_SOCKET, SO_RCVTIMEO, &recv_timeout, sizeof(recv_timeout));

#if USE_EPOLLET
    event_register(clientfd, EPOLLIN | EPOLLET);
#else
    event_register(clientfd, EPOLLIN);
#endif

    return 0;
}
#if 1
int recv_cb(int clientfd) {
    // 1. 读取 4 字节长度头。
    //
    // 这里把「首次读取」和「补齐半包」合并成同一个循环，并显式处理
    // EAGAIN/EWOULDBLOCK（SO_RCVTIMEO 到期）与 EINTR：这两种情况下都回到
    // 循环头检查停止标志，避免客户端只发半个请求时事件循环再也退不出去。
    uint32_t net_len;
    ssize_t n = 0;
    while (n < (ssize_t)sizeof(net_len)) {
        if (g_kvs_shutdown) {
            // 收到停止请求：不必等这个半包收完，直接断开让服务尽快收尾
            close_client_conn(clientfd);
            return 0;
        }

        ssize_t ret = recv(clientfd, (char*)&net_len + n, sizeof(net_len) - n, 0);
        if (ret == 0) {
            kvs_log(KVS_LOG_INFO, "[NETWORK] client disconnect: %d", clientfd);
            close_client_conn(clientfd);
            return 0;
        } else if (ret < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            kvs_log(KVS_LOG_WARN, "[NETWORK] recv header error, errno: %d, %s", errno,
                    strerror(errno));
            close_client_conn(clientfd);
            return 0;
        }
        n += ret;
    }

    uint32_t msg_len = ntohl(net_len);

    // 检查消息体长度是否超过缓冲区（留一个字节给 '\0'）
    // 检查消息长度是否在允许范围内
    if (msg_len > MAX_ALLOWED_LEN) {
        kvs_log(KVS_LOG_WARN, "[NETWORK] message too long: %u (max: %u)", msg_len, MAX_ALLOWED_LEN);
        // 可选：发送错误响应并继续服务，或直接关闭连接
        const char* err = "-ERR message too long\r\n";
        send(clientfd, err, strlen(err), 0);
        close_client_conn(clientfd);
        return 0;
    }

    // 按需扩容接收缓冲区（只给真正收到数据的连接分配）
    if (conn_list[clientfd].ensure_rbuffer((size_t)msg_len + 1) != 0) {
        kvs_log(KVS_LOG_WARN, "[NETWORK] rbuffer resize failed, msg_len: %u", msg_len);
        close_client_conn(clientfd);
        return 0;
    }

    // 2. 读取消息体（循环读满 msg_len 字节）
    char* buffer = conn_list[clientfd].rbuffer.data();

    n = 0;
    while (n < (ssize_t)msg_len) {
        if (g_kvs_shutdown) {
            close_client_conn(clientfd);
            return 0;
        }

        ssize_t ret = recv(clientfd, buffer + n, msg_len - n, 0);
        if (ret == 0) {
            kvs_log(KVS_LOG_INFO, "[NETWORK] client disconnect: %d", clientfd);
            close_client_conn(clientfd);
            return 0;
        } else if (ret < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            kvs_log(KVS_LOG_WARN, "[NETWORK] recv body error, errno: %d, %s", errno,
                    strerror(errno));
            close_client_conn(clientfd);
            return 0;
        }
        n += ret;
    }

    buffer[msg_len] = '\0'; // 方便字符串处理
    conn_list[clientfd].rlength = msg_len;

    // 响应缓冲区同样按需分配：分配后 kvs_handler 最多可写 wbuffer.size()-4 字节，
    // 发送时再在最前面补 4 字节长度头。
    if (conn_list[clientfd].ensure_wbuffer(MAX_ALLOWED_LEN + 4) != 0) {
        kvs_log(KVS_LOG_WARN, "[NETWORK] wbuffer resize failed, clientfd: %d", clientfd);
        close_client_conn(clientfd);
        return 0;
    }

    int is_replica = kvs_replication_accept_handshake(clientfd, buffer, msg_len) == 1;
    if (is_replica) {
        conn_list[clientfd].wlength = snprintf(conn_list[clientfd].wbuffer.data(),
                                               conn_list[clientfd].wbuffer.size() - 4, "+OK\r\n");
    } else {
        // printf("[%d]RECV: %s\n", conn_list[clientfd].rlength, conn_list[clientfd].rbuffer);

        // 封装 kvs 请求
        kvs_request(&conn_list[clientfd]);
    }

    set_event(clientfd, EPOLLOUT, 0);

    return msg_len;
}

int send_cb(int clientfd) {
    struct conn* c = &conn_list[clientfd];

    // 把响应体后移 4 字节，在头部补上长度头，长度头与响应体一次 send 发完，
    // 避免拆成两个小包写入时被 TCP 小包延迟拖慢。
    uint32_t net_len = htonl(conn_list[clientfd].wlength);
    size_t total_len = sizeof(net_len) + (size_t)c->wlength;
    if (c->wlength < 0 || total_len > c->wbuffer.size()) {
        kvs_log(KVS_LOG_WARN, "[NETWORK] invalid response length: %d", c->wlength);
        close_client_conn(clientfd);
        return -1;
    }

    memmove(c->wbuffer.data() + sizeof(net_len), c->wbuffer.data(), (size_t)c->wlength);
    memcpy(c->wbuffer.data(), &net_len, sizeof(net_len));

    ssize_t n = send(clientfd, c->wbuffer.data(), total_len, 0);
    if (n <= 0) {
        kvs_log(KVS_LOG_WARN, "[NETWORK] send error: errno %d %s", errno, strerror(errno));
        close_client_conn(clientfd);
        return -1;
    }

    kvs_replication_finish_handshake(clientfd);

    // 发送完成，重新监听读事件
    set_event(clientfd, EPOLLIN, 0);

    return (int)n;
}

#else
int recv_cb(int clientfd) {
    memset(conn_list[clientfd].rbuffer, 0, BUFFER_LENGTH);
    int count = recv(clientfd, conn_list[clientfd].rbuffer, BUFFER_LENGTH, 0);
    if (count == 0) {
        kvs_log(KVS_LOG_INFO, "[NETWORK] client disconnect: %d", clientfd);
        epoll_ctl(epfd, EPOLL_CTL_DEL, clientfd, nullptr);
        close(clientfd);
        return 0;
    } else if (count < 0) {
        kvs_log(KVS_LOG_WARN, "[NETWORK] recv error: count=%d, errno=%d, %s", count, errno,
                strerror(errno));
        epoll_ctl(epfd, EPOLL_CTL_DEL, clientfd, nullptr);
        close(clientfd);
        return 0;
    }

    conn_list[clientfd].rlength = count;
    // printf("[%d]RECV: %s\n", conn_list[clientfd].rlength, conn_list[clientfd].rbuffer);

    //封装kvs请求
    kvs_request(&conn_list[clientfd]);

    set_event(clientfd, EPOLLOUT, 0);

    return count;
}

int send_cb(int clientfd) {
    int count = send(clientfd, conn_list[clientfd].wbuffer, conn_list[clientfd].wlength, 0);
    // printf("SEND: %d\n", count);

#if USE_EPOLLET
    set_event(clientfd, EPOLLIN | EPOLLET, 0);
#else
    set_event(clientfd, EPOLLIN, 0);
#endif

    return count;
}
#endif

int is_listenfd(int* sockfds, int fd) {
    for (int i = 0; i < PORT_NUMS; i++) {
        if (fd == *(sockfds + i)) {
            return 1;
        }
    }
    return 0;
}

// int main() {
int reactor_start(const char* bind_ip, unsigned short port, msg_handler handler) {
    kvs_handler = handler;
    // unsigned short port = 2000;
    epfd = epoll_create(1);
    int sockfds[PORT_NUMS];
    for (int i = 0; i < PORT_NUMS; ++i) {
        int listenfd = init_server(bind_ip, port + i);
        if (listenfd < 0) {
            // 监听失败必须退出：继续拿无效 fd 去索引 conn_list 会越界写。
            kvs_log(KVS_LOG_ERROR, "[NETWORK] listen init failed on port %d", port + i);
            for (int j = 0; j < i; ++j) {
                close(sockfds[j]);
            }
            return -1;
        }
        sockfds[i] = listenfd;
        conn_list[sockfds[i]].fd = sockfds[i];
        conn_list[sockfds[i]].r_action.accept_callback = accept_cb;
        set_event(sockfds[i], EPOLLIN, 1);
    }

    struct epoll_event events[1024] = {0};
    // 用有限超时轮询而不是 -1 永久阻塞：SIGINT/SIGTERM 可能被投递到复制线程，
    // 主线程的 epoll_wait 未必收到 EINTR，靠超时保证停止标志一定能被看到。
    while (!g_kvs_shutdown) { // mainloop
        int nready = epoll_wait(epfd, events, 1024, KVS_SHUTDOWN_POLL_MS);
        if (nready < 0) {
            if (errno == EINTR) {
                continue; // 回到循环头检查停止标志
            }
            kvs_log(KVS_LOG_ERROR, "[NETWORK] epoll_wait failed: %s", strerror(errno));
            break;
        }
        for (int i = 0; i < nready; ++i) {
            int connfd = events[i].data.fd;
            if (is_listenfd(sockfds, connfd)) {
                conn_list[connfd].r_action.accept_callback(connfd);
                continue;
            }

            if (events[i].events & EPOLLIN) {
                conn_list[connfd].r_action.recv_callback(connfd);
            }
            if (events[i].events & EPOLLOUT) {
                conn_list[connfd].send_callback(connfd);
            }
        }
    }

    for (int i = 0; i < PORT_NUMS; ++i) {
        close(sockfds[i]);
    }
    kvs_log(KVS_LOG_INFO, "[NETWORK] reactor loop exited");
    return 0;
}
