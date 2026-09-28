#pragma once

#include <cstddef>
#include <vector>

#define MAX_ALLOWED_LEN 1024 * 1024 // 1MB
#define BUFFER_LENGTH 1024
#define CONN_SIZE 1024

#define PORT_NUMS 1

#define USE_EPOLLET 0

typedef int (*RCALLBACK)(int fd);

struct conn {
    int fd;
#if 1
    std::vector<char> rbuffer; // 接收缓冲区，按需分配（见 ensure_rbuffer）
    std::vector<char> wbuffer; // 发送缓冲区，按需分配（见 ensure_wbuffer）
#else
    char rbuffer[BUFFER_LENGTH];
    char wbuffer[BUFFER_LENGTH];
#endif
    int rlength;
    int wlength;

    RCALLBACK send_callback;
    union { //只能执行其中一个，如果是clientfd，就执行recv；sockfd执行accept
        RCALLBACK recv_callback;
        RCALLBACK accept_callback;
    } r_action;

    // 连接表是全局的（CONN_SIZE 个 conn），如果在这里预分配缓冲区，进程启动时
    // 就要常驻 CONN_SIZE * (BUFFER_LENGTH + MAX_ALLOWED_LEN + 4) 的物理内存
    // （当前取值约 1GB），并且这部分开销与内存分配模式无关。因此初始不分配，
    // 只在真正收发数据的连接上按需扩容。
    conn() : fd(-1), rlength(0), wlength(0) {}

    // 保证接收缓冲区至少 need 字节，分配失败返回 -1。
    int ensure_rbuffer(size_t need) { return reserve_buffer(rbuffer, need); }

    // 保证发送缓冲区至少 need 字节，分配失败返回 -1。发送时会在响应体前
    // 额外塞 4 字节长度头，调用方需要把这一点算进 need。
    int ensure_wbuffer(size_t need) { return reserve_buffer(wbuffer, need); }

    private:
    static int reserve_buffer(std::vector<char>& buffer, size_t need) {
        if (buffer.size() >= need) {
            return 0;
        }
        try {
            buffer.resize(need);
        } catch (...) {
            return -1;
        }
        return 0;
    }
};

int accept_cb(int listenfd);
int recv_cb(int clientfd);
int send_cb(int clientfd);

int kvs_request(struct conn* c);
int kvs_response(struct conn* c);
