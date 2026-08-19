#pragma once
#include <vector>

#define MAX_ALLOWED_LEN 1024 * 1024 // 1MB
#define BUFFER_LENGTH 1024
#define CONN_SIZE 1024

#define PORT_NUMS 1

#define ECHO 1

#define USE_EPOLLET 0

typedef int (*RCALLBACK)(int fd);

struct conn {
    int fd;
#if 1
    std::vector<char> rbuffer; // 接收缓冲区
    std::vector<char> wbuffer; // 发送缓冲区
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

    conn() : fd(-1), rlength(0), wlength(0) {
        rbuffer.resize(BUFFER_LENGTH);
        wbuffer.resize(BUFFER_LENGTH);
    }
};

int accept_cb(int listenfd);
int recv_cb(int clientfd);
int send_cb(int clientfd);

int kvs_request(struct conn* c);
int kvs_response(struct conn* c);