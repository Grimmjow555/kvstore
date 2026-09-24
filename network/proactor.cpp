#include "kvs_config.h"
#include "kvs_replication.h"
#include <arpa/inet.h>
#include <errno.h>
#include <iostream>
#include <liburing.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

typedef int (*msg_handler)(char* msg, int length, char* response, int response_size);
static msg_handler kvs_handler;

#define ENTRIES_LENGTH 1024 // 提交队列和完成队列的大小
#define BUFFER_LENGTH 1024
#define MAX_ALLOWED_LEN 1024 * 1024 // 1MB

enum class EVENT { ACCEPT = 0, READ = 1, WRITE = 2 };
// 新增读取状态枚举
enum class READ_STATE { HEADER = 0, DATA = 1 };
struct conn_ctx {
    int listenfd;
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);

    int clientfd;
#if 1
    std::vector<char> rbuffer; // 读取缓冲区
    std::vector<char> wbuffer; // 写入缓冲区
#else
    char rbuffer[BUFFER_LENGTH];
    char wbuffer[BUFFER_LENGTH];
#endif
    // struct iovec send_iov[2]; // 发送用的 iovec，与 ctx 同生命周期

    EVENT event;

    // 以下为新增字段，用于长度前缀协议
    READ_STATE rstate;   // 当前读取阶段：头部 or 数据
    uint32_t header;     // 存放 4 字节长度头（网络字节序）
    int header_recv_len; // 头部已接收的字节数（处理半包）
    int msg_len;         // 解析出的消息体长度
    int data_recv_len;   // 消息体已接收的字节数

    size_t wtotal_len; // 本次响应帧总长度：4 字节长度头 + 响应体
    size_t wsent_len;  // 已成功发出的字节数，用于发送缓冲区满时的续发

    conn_ctx() {
        rbuffer.resize(BUFFER_LENGTH);
        // 响应缓冲区按最大消息长度分配，并额外预留 4 字节长度头空间
        wbuffer.resize(MAX_ALLOWED_LEN + 4);
        wtotal_len = 0;
        wsent_len = 0;
    }
};

static int init_server(const char* bind_ip, unsigned short port) {

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    if (bind_ip == nullptr || bind_ip[0] == '\0' || strcmp(bind_ip, "*") == 0) {
        servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, bind_ip, &servaddr.sin_addr) != 1) {
        kvs_log(KVS_LOG_ERROR, "[NETWORK] invalid bind ip: %s", bind_ip);
        return -1;
    }
    servaddr.sin_port = htons(port);

    // 如果端口正在被占用会绑定失败
    if (bind(listenfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) == -1) {
        kvs_log(KVS_LOG_ERROR, "[NETWORK] bind failed: %s", strerror(errno));
        return -1;
    }
    listen(listenfd, 10);
    kvs_log(KVS_LOG_INFO, "[NETWORK] listen finished, listenfd: %d", listenfd);

    return listenfd;
}

int set_event_accept(struct io_uring* ring, int listenfd, int flags) {
    struct conn_ctx* ctx = new conn_ctx();
    ctx->listenfd = listenfd;
    ctx->event = EVENT::ACCEPT;

    // 获得一个SQ队列的空闲节点
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);

    io_uring_prep_accept(sqe, listenfd, (struct sockaddr*)&(ctx->clientaddr), &(ctx->len),
                         flags); // 提交接受请求，listenfd为监听fd

    sqe->user_data = (__u64)(uintptr_t)ctx;
    return 0;
}

#if 1
int set_event_recv(struct io_uring* ring, int clientfd, int flags) {
    struct conn_ctx* ctx = new conn_ctx();
    if (!ctx)
        return -1;

    ctx->clientfd = clientfd;
    ctx->event = EVENT::READ;

    // 初始化长度前缀协议相关字段
    ctx->rstate = READ_STATE::HEADER; // 当前阶段：读头部
    ctx->header = 0;
    ctx->header_recv_len = 0;
    ctx->msg_len = 0;
    ctx->data_recv_len = 0;

    // 只提交读取 4 字节长度头的请求
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        delete ctx;
        return -1;
    }
    io_uring_prep_recv(sqe, clientfd, &ctx->header, sizeof(uint32_t), flags);
    sqe->user_data = (__u64)(uintptr_t)ctx;
    return 0;
}
#else
int set_event_recv(struct io_uring* ring, int clientfd, int flags) {
    struct conn_ctx* ctx = new conn_ctx();
    ctx->clientfd = clientfd;
    ctx->event = EVENT::READ;
    memset(ctx->rbuffer, 0, BUFFER_LENGTH);
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_recv(sqe, clientfd, ctx->rbuffer, BUFFER_LENGTH, flags);
    sqe->user_data = (__u64)(uintptr_t)ctx;
    return 0;
}
#endif

#if 1
int set_event_send(struct io_uring* ring, conn_ctx* ctx, int sendlen, int flags) {

    // 参数检查：sendlen 必须为正数，且加上4字节头后不能超过缓冲区
    if (!ctx || sendlen <= 0 || (size_t)sendlen + sizeof(uint32_t) > ctx->wbuffer.size()) {
        return -1;
    }

    ctx->event = EVENT::WRITE;

    // 将长度转换为网络字节序
    uint32_t net_len = htonl((uint32_t)sendlen);

    // 将原响应数据向后移动4字节（memmove 处理重叠区域）
    memmove(ctx->wbuffer.data() + 4, ctx->wbuffer.data(), sendlen);

    // 在开头写入4字节长度头
    memcpy(ctx->wbuffer.data(), &net_len, sizeof(net_len));

    // 总发送长度 = 4字节头 + 实际数据长度
    size_t total_len = sizeof(net_len) + sendlen;
    ctx->wtotal_len = total_len;
    ctx->wsent_len = 0;

    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        return -1;
    }
    io_uring_prep_send(sqe, ctx->clientfd, ctx->wbuffer.data(), total_len, flags);
    sqe->user_data = (__u64)(uintptr_t)ctx;

    return 0;
}
#else
int set_event_send(struct io_uring* ring, conn_ctx* ctx, size_t sendlen, int flags) {
    ctx->event = EVENT::WRITE;
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_send(sqe, ctx->clientfd, ctx->wbuffer, sendlen, flags);
    sqe->user_data = (__u64)(uintptr_t)ctx;
    return 0;
}
#endif

int handle_cqe(struct io_uring* ring, struct io_uring_cqe* entries, int listenfd) {

    //读取上下文信息
    struct conn_ctx* ctx = (conn_ctx*)(uintptr_t)(entries->user_data);

    switch (ctx->event) {
    case EVENT::ACCEPT: {
        int clientfd = entries->res;
        kvs_log(KVS_LOG_DEBUG, "[NETWORK] accept connection [%d]", clientfd);
        set_event_recv(ring, clientfd, 0);
        set_event_accept(ring, listenfd, 0);
        delete ctx;
        break;
    }

#if 1
    case EVENT::READ: {
        int recvlen = entries->res;
        if (recvlen <= 0) {
            close(ctx->clientfd);
            kvs_log(KVS_LOG_INFO, "[NETWORK] connection [%d] break", ctx->clientfd);
            delete ctx;
            break;
        }

        if (ctx->rstate == READ_STATE::HEADER) {
            // 处理头部接收
            ctx->header_recv_len += recvlen;
            if (ctx->header_recv_len < sizeof(uint32_t)) {
                // 半包：继续读取剩余头部
                struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
                io_uring_prep_recv(sqe, ctx->clientfd, (char*)&ctx->header + ctx->header_recv_len,
                                   sizeof(uint32_t) - ctx->header_recv_len, 0);
                sqe->user_data = (__u64)(uintptr_t)ctx;
            } else {
                // 头部完整，解析长度
                uint32_t msg_len = ntohl(ctx->header);
                if (msg_len > MAX_ALLOWED_LEN) {
                    kvs_log(KVS_LOG_WARN, "[NETWORK] message too long: %u (max: %u)", msg_len,
                            MAX_ALLOWED_LEN);
                    close(ctx->clientfd);
                    delete ctx;
                    break;
                }
                ctx->msg_len = msg_len;
                ctx->data_recv_len = 0;
                ctx->rstate = READ_STATE::DATA;
                ctx->rbuffer.resize(msg_len + 1); // 调整缓冲区大小以适应消息体

                // 提交读取消息体请求
                struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
                io_uring_prep_recv(sqe, ctx->clientfd, ctx->rbuffer.data(), msg_len, 0);
                sqe->user_data = (__u64)(uintptr_t)ctx;
            }
        } else if (ctx->rstate == READ_STATE::DATA) {
            // 处理数据接收
            ctx->data_recv_len += recvlen;
            if (ctx->data_recv_len < ctx->msg_len) {
                // 半包：继续读取剩余数据
                struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
                io_uring_prep_recv(sqe, ctx->clientfd, ctx->rbuffer.data() + ctx->data_recv_len,
                                   ctx->msg_len - ctx->data_recv_len, 0);
                sqe->user_data = (__u64)(uintptr_t)ctx;
            } else {
                // 数据完整，处理请求
                ctx->rbuffer[ctx->msg_len] = '\0'; // 确保字符串结尾
                int ret;
                if (kvs_replication_accept_handshake(ctx->clientfd, ctx->rbuffer.data(),
                                                     ctx->msg_len) == 1) {
                    ret = snprintf(ctx->wbuffer.data(), ctx->wbuffer.size() - 4, "+OK\r\n");
                } else {
                    ret = kvs_handler(ctx->rbuffer.data(), ctx->msg_len, ctx->wbuffer.data(),
                                      (int)(ctx->wbuffer.size() - 4));
                }
                // 转入发送阶段，不要 delete ctx
                set_event_send(ring, ctx, ret, 0);
            }
        }
        break;
    }
#else
    case EVENT::READ: {
        int recvlen = entries->res;
        if (recvlen <= 0) {
            close(ctx->clientfd);
            kvs_log(KVS_LOG_INFO, "[NETWORK] connection [%d] break", ctx->clientfd);
            delete ctx;
        } else {
            // printf("connection [%d], recv %d: %s\n", ctx->clientfd, recvlen, ctx->rbuffer);

            int ret = kvs_handler(ctx->rbuffer, recvlen, ctx->wbuffer);

            set_event_send(ring, ctx, ret, 0);
        }
        break;
    }
#endif

#if 0
    case EVENT::WRITE: {
        int sendlen = entries->res;
        if (sendlen < 0) {
            kvs_log(KVS_LOG_WARN, "[NETWORK] send error: %d (%s)", -sendlen, strerror(-sendlen));
            close(ctx->clientfd);
            delete ctx;
            break;
        }
        kvs_log(KVS_LOG_DEBUG, "[NETWORK] sendback to connection --> %d: [%d]%s", ctx->clientfd,
                sendlen, ctx->wbuffer);

        // 正常处理
        set_event_recv(ring, ctx->clientfd, 0);
        delete ctx;
        break;
    }
#else
    case EVENT::WRITE: {
        int sendlen = entries->res;
        if (sendlen <= 0) {
            // sendlen < 0 为发送出错，sendlen == 0 表示对端已关闭连接
            kvs_log(KVS_LOG_WARN, "[NETWORK] send error on connection [%d]: %d (%s)",
                    ctx->clientfd, sendlen,
                    sendlen < 0 ? strerror(-sendlen) : "connection closed");
            close(ctx->clientfd);
            delete ctx;
            break;
        }

        ctx->wsent_len += (size_t)sendlen;
        if (ctx->wsent_len < ctx->wtotal_len) {
            // 只发出去一部分（通常是发送缓冲区满），继续提交剩余字节
            struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
            if (!sqe) {
                io_uring_submit(ring);
                sqe = io_uring_get_sqe(ring);
            }
            if (!sqe) {
                kvs_log(KVS_LOG_WARN, "[NETWORK] no sqe to resend, close connection [%d]",
                        ctx->clientfd);
                close(ctx->clientfd);
                delete ctx;
                break;
            }
            io_uring_prep_send(sqe, ctx->clientfd, ctx->wbuffer.data() + ctx->wsent_len,
                               ctx->wtotal_len - ctx->wsent_len, 0);
            sqe->user_data = (__u64)(uintptr_t)ctx;
            break; // 保留 ctx，等待下一次 WRITE 完成事件
        }

        kvs_replication_finish_handshake(ctx->clientfd);
        set_event_recv(ring, ctx->clientfd, 0);
        // printf("sendback to connection --> %d: [%zu]%s\n", ctx->clientfd,
        //        ctx->wsent_len - 4, ctx->wbuffer.data());

        delete ctx;
        break;
    }
#endif
    }

    return 0;
}

// int main(int argc, char* argv[]) {
int proactor_start(const char* bind_ip, unsigned short port, msg_handler handler) {
    // unsigned short port = 2000;       //原始情况

    kvs_handler = handler;

    int listenfd = init_server(bind_ip, port); // 初始化并监听端口
    if (listenfd < 0) {
        kvs_log(KVS_LOG_ERROR, "[NETWORK] server initialization failed");
        return 1;
    }

    // 初始化io_uring实例的参数结构体
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    struct io_uring ring; // 操纵句柄
    io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);

#if 0 //常规做法
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    int clientfd = accept(listenfd, (struct sockaddr*)&clientaddr, &len,0);//这里会阻塞，等待连接建立
#endif

    set_event_accept(&ring, listenfd, 0);

    while (1) {

        // 1. 先尝试非阻塞批量获取所有已完成的 CQE

        // struct io_uring_cqe包含三个字段，res, user_data, flags。其中res存储io操作的返回值
        struct io_uring_cqe* cqes[128];
        // 取出cqe，最多128个，存入cqes数组
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);
        if (nready > 0) {
            for (int i = 0; i < nready; ++i) { // 遍历cqes数组，分别处理
                handle_cqe(&ring, cqes[i], listenfd);
            }
            io_uring_cq_advance(&ring, nready); // 回收CQ队列中已取出的nready个
        } else {
            io_uring_submit(&ring); // 提交SQ队列

            // 2. 没有已完成的事件，阻塞等待一个事件

            struct io_uring_cqe* cqe;
            int ret = io_uring_wait_cqe(&ring, &cqe); // 阻塞，等待完成队列就绪
            if (ret < 0) {
                // 处理错误（如 -EINTR）
                continue;
            }
            handle_cqe(&ring, cqe, listenfd);
            io_uring_cqe_seen(&ring, cqe);
        }
    }
}
