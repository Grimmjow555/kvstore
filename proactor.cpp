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

// extern int kvs_protocol(char* msg, int length, char* response);

typedef int (*msg_handler)(char* msg, int length, char* response);
static msg_handler kvs_handler;

#define ENTRIES_LENGTH 1024 // 提交队列和完成队列的大小
#define BUFFER_LENGTH 1024

static char response[BUFFER_LENGTH] = {0}; //

enum class EVENT { ACCEPT = 0, READ = 1, WRITE = 2 };

struct conn_ctx {
    int listenfd;
    int clientfd;
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    char buffer[BUFFER_LENGTH];
    EVENT event;
};

static int init_server(unsigned short port) {

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    // 如果端口正在被占用会绑定失败
    if (bind(listenfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) == -1) {
        printf("bind failed: %s\n", strerror(errno));
        return -1;
    }
    listen(listenfd, 10);
    printf("listen finished, listenfd: %d\n", listenfd);

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

int set_event_recv(struct io_uring* ring, int clientfd, int flags) {
    struct conn_ctx* ctx = new conn_ctx();
    ctx->clientfd = clientfd;
    ctx->event = EVENT::READ;
    memset(ctx->buffer, 0, BUFFER_LENGTH);
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_recv(sqe, clientfd, ctx->buffer, BUFFER_LENGTH, flags);
    sqe->user_data = (__u64)(uintptr_t)ctx;
    return 0;
}

int set_event_send(struct io_uring* ring, conn_ctx* ctx, char* buffer, size_t recvlen, int flags) {
    ctx->event = EVENT::WRITE;
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_send(sqe, ctx->clientfd, buffer, recvlen, flags);
    sqe->user_data = (__u64)(uintptr_t)ctx;
    return 0;
}

int handle_cqe(struct io_uring* ring, struct io_uring_cqe* entries, int listenfd) {

    //读取上下文信息
    struct conn_ctx* ctx = (conn_ctx*)(uintptr_t)(entries->user_data);

    switch (ctx->event) {
    case EVENT::ACCEPT: {
        int clientfd = entries->res;
        printf("accept connection [%d]\n", clientfd);
        set_event_recv(ring, clientfd, 0);
        set_event_accept(ring, listenfd, 0);
        delete ctx;
        break;
    }

    case EVENT::READ: {
        int recvlen = entries->res;
        if (recvlen <= 0) {
            close(ctx->clientfd);
            printf("connection [%d] break\n", ctx->clientfd);
            delete ctx;
        } else {
            printf("connection [%d], recv %d: %s\n", ctx->clientfd, recvlen, ctx->buffer);

            // int ret = kvs_protocol(ctx->buffer, recvlen, response);
            int ret = kvs_handler(ctx->buffer, recvlen, response);

            set_event_send(ring, ctx, response, ret, 0);
        }
        break;
    }

    case EVENT::WRITE: {
        int sendlen = entries->res;

        printf("sendback to connection [%d]: %d\n", ctx->clientfd, sendlen);

        set_event_recv(ring, ctx->clientfd, 0);
        delete ctx;
        break;
    }
    }

    return 0;
}

// int main(int argc, char* argv[]) {
int proactor_start(unsigned short port, msg_handler handler) {
    // unsigned short port = 2000;       //原始情况
    printf("USE proactor\n");

    kvs_handler = handler;

    int listenfd = init_server(port); // 初始化并监听端口
    if (listenfd < 0) {
        fprintf(stderr, "Server initialization failed.\n");
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