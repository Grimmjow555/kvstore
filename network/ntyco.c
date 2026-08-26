

#include "kvs_replication.h"
#include "nty_coroutine.h"
#include <arpa/inet.h> // htonl, ntohl
#include <arpa/inet.h>
#include <stdint.h> // uint32_t
#include <string.h> // memcpy, memset
#include <unistd.h> // recv, send, close


#define MAX_ALLOWED_LEN 1024 * 1024 // 1MBS
#define BUFFER_SIZE 1024

typedef int (*msg_handler)(char* msg, int length, char* response, int response_size);
static msg_handler kvs_handler;

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
    int ret = 0;

    while (1) {
        // 1. 读取 4 字节长度头
        uint32_t net_len;
        if (recv_full(fd, &net_len, sizeof(net_len)) != 0) {
            close(fd);
            break;
        }
        uint32_t msg_len = ntohl(net_len);

        // 2. 检查消息长度是否在允许范围内
        if (msg_len == 0 || msg_len > MAX_ALLOWED_LEN) {
            const char* err = "-ERR invalid message length\r\n";
            send(fd, err, strlen(err), 0);
            close(fd);
            break;
        }

        // 3. 动态分配接收缓冲区
        char* buf = (char*)malloc(msg_len + 1);
        if (!buf) {
            close(fd);
            break;
        }

        // 4. 读取消息体
        if (recv_full(fd, buf, msg_len) != 0) {
            free(buf);
            close(fd);
            break;
        }
        buf[msg_len] = '\0'; // 确保字符串结束

        // 5. 动态分配响应缓冲区（与最大允许长度相同，或使用更小的合理上限）
        char* response = (char*)malloc(MAX_ALLOWED_LEN);
        if (!response) {
            free(buf);
            close(fd);
            break;
        }

        // 6. 处理请求
        int slength;
        if (kvs_replication_accept_handshake(fd, buf, (int)msg_len) == 1) {
            slength = snprintf(response, MAX_ALLOWED_LEN, "+OK\r\n");
        } else {
            slength = kvs_handler(buf, (int)msg_len, response, MAX_ALLOWED_LEN);
        }
        if (slength < 0) {
            slength = 0; // 或发送错误响应
        }

        // 防止响应长度超过分配大小（理论上 kvs_handler 应保证不超）
        if (slength > MAX_ALLOWED_LEN) {
            slength = MAX_ALLOWED_LEN; // 截断或关闭连接，此处简单截断
        }

        // 7. 发送响应：先发长度头，再发数据
        uint32_t resp_net_len = htonl((uint32_t)slength);
        int send_ok = 1;
        if (send_full(fd, &resp_net_len, sizeof(resp_net_len)) != 0 ||
            send_full(fd, response, slength) != 0) {
            send_ok = 0;
        }

        // 8. 释放内存
        free(buf);
        free(response);

        if (!send_ok) {
            close(fd);
            break;
        }
    }
}
#elif 0
// 以头部存储长度的协议读写
void server_reader(void* arg) {
    int fd = *(int*)arg;
    int ret = 0;

    while (1) {
        // 1. 读取 4 字节长度头
        uint32_t net_len;
        if (recv_full(fd, &net_len, sizeof(net_len)) != 0) {
            // 读取头部失败或对端关闭
            close(fd);
            break;
        }
        uint32_t msg_len = ntohl(net_len);

        // 2. 检查消息长度是否合法（防止溢出，留一个字节给结束符）
        if (msg_len >= 1024) {
            close(fd);
            break;
        }

        // 3. 读取消息体
        char buf[1024] = {0};
        if (recv_full(fd, buf, msg_len) != 0) {
            close(fd);
            break;
        }
        buf[msg_len] = '\0'; // 方便字符串处理

        // 4. 处理请求，生成响应
        char response[1024] = {0};
        int slength = kvs_handler(buf, msg_len, response);
        if (slength < 0) {
            // 处理失败，可发送错误响应或直接关闭
            slength = 0; // 或构造错误字符串
        }

        // 5. 发送响应：先发送长度头，再发送数据
        uint32_t resp_net_len = htonl((uint32_t)slength);
        if (send_full(fd, &resp_net_len, sizeof(resp_net_len)) != 0 ||
            send_full(fd, response, slength) != 0) {
            close(fd);
            break;
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

    unsigned short port = *(unsigned short*)arg;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return;

    struct sockaddr_in local, remote;
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = INADDR_ANY;
    bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

    listen(fd, 20);
    printf("listen port : %d\n", port);

    while (1) {
        socklen_t len = sizeof(struct sockaddr_in);
        int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);

        nty_coroutine* read_co;

        nty_coroutine_create(&read_co, server_reader, (void*)(intptr_t)cli_fd);
        // nty_coroutine_create(&read_co, server_reader, &cli_fd);
    }
}

int ntyco_start(unsigned short port, msg_handler handler) {

    // unsigned short port = atoi(argv[1]); //原始情况，直接从命令行读取端口，需要转化为整数

    kvs_handler = handler;

    nty_coroutine* co = NULL;
    nty_coroutine_create(&co, server, &port);

    nty_schedule_run();
}
