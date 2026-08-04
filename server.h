#pragma once

#define BUFFER_LENGTH 1024
#define CONN_SIZE 1024

#define PORT_NUMS 1

#define ECHO 1

#define USE_EPOLLET 0

typedef int (*RCALLBACK)(int fd);

struct conn {
    int fd;
    char rbuffer[BUFFER_LENGTH];
    int rlength;
    char wbuffer[BUFFER_LENGTH];
    int wlength;

    RCALLBACK send_callback;
    union { //只能执行其中一个，如果是clientfd，就执行recv；sockfd执行accept
        RCALLBACK recv_callback;
        RCALLBACK accept_callback;
    } r_action;
};

int kvs_request(struct conn* c);
int kvs_response(struct conn* c);