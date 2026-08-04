#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "server.h"
extern int kvs_protocol(char* msg, int length, char* response);

typedef int (*RCALLBACK)(int fd);

int epfd = 0;

struct conn conn_list[CONN_SIZE] = {0};

int accept_cb(int listenfd);
int recv_cb(int clientfd);
int send_cb(int clientfd);

int init_server(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in servaddr = {0};
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) == -1) {
        printf("bind failed: %s\n", strerror(errno));
        return -1;
    }
    listen(sockfd, 10);
    printf("listen finished on port %d, listenfd: %d\n", port, sockfd);

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
    memset(conn_list[fd].rbuffer, 0, BUFFER_LENGTH);
    conn_list[fd].wlength = 0;
    memset(conn_list[fd].wbuffer, 0, BUFFER_LENGTH);

    set_event(fd, event, 1);
    return 0;
}

int accept_cb(int listenfd) {
    struct sockaddr_in clientaddr = {0};
    socklen_t len = sizeof(clientaddr);
    int clientfd = accept(listenfd, (struct sockaddr*)&clientaddr, &len);
    if (clientfd < 0) {
        printf("accept error: %d\n", errno);
        return -1;
    }

    printf("accept finished, clientfd: %d\n", clientfd);

#if USE_EPOLLET
    event_register(clientfd, EPOLLIN | EPOLLET);
#else
    event_register(clientfd, EPOLLIN);
#endif

    return 0;
}
int recv_cb(int clientfd) {
    int count = recv(clientfd, conn_list[clientfd].rbuffer, BUFFER_LENGTH, 0);
    if (count <= 0) {
        printf("client disconnect: %d\n", clientfd);
        epoll_ctl(epfd, EPOLL_CTL_DEL, clientfd, NULL); // unfinished
        close(clientfd);
        return 0;
    }

#if USE_EPOLLET
    while (true) { //若管道中数据不为空，继续读取
        conn_list[clientfd].rlength = count;
        printf("[%d]RECV: %s\n", conn_list[clientfd].rlength, conn_list[clientfd].rbuffer);
        memset(conn_list[clientfd].rbuffer, 0, BUFFER_LENGTH);

        count = recv(clientfd, conn_list[clientfd].rbuffer, BUFFER_LENGTH, 0);
        if (count <= 0)
            break;
    }
#else
    conn_list[clientfd].rlength = count;
    printf("[%d]RECV: %s\n", conn_list[clientfd].rlength, conn_list[clientfd].rbuffer);
#endif
    // memcpy(conn_list[clientfd].wbuffer, conn_list[clientfd].rbuffer, count);
    // conn_list[clientfd].wlength = conn_list[clientfd].rlength;

    int ret = kvs_protocol(conn_list[clientfd].rbuffer, conn_list[clientfd].rlength,
                           conn_list[clientfd].wbuffer);
    conn_list[clientfd].wlength = ret;

    set_event(clientfd, EPOLLOUT, 0);

    return count;
}

int send_cb(int clientfd) {
    int count = send(clientfd, conn_list[clientfd].wbuffer, conn_list[clientfd].wlength, 0);
    printf("SEND: %d\n", count);

#if USE_EPOLLET
    set_event(clientfd, EPOLLIN | EPOLLET, 0);
#else
    set_event(clientfd, EPOLLIN, 0);
#endif

    return count;
}

int is_listenfd(int* sockfds, int fd) {
    for (int i = 0; i < PORT_NUMS; i++) {
        if (fd == *(sockfds + i)) {
            return 1;
        }
    }
    return 0;
}
int main() {
    unsigned short port = 2000;
    epfd = epoll_create(1);
    int sockfds[PORT_NUMS];
    for (int i = 0; i < PORT_NUMS; ++i) {
        sockfds[i] = init_server(port + i);
        conn_list[sockfds[i]].fd = sockfds[i];
        conn_list[sockfds[i]].r_action.accept_callback = accept_cb;
        set_event(sockfds[i], EPOLLIN, 1);
    }

    struct epoll_event events[1024] = {0};
    while (1) { // mainloop
        int nready = epoll_wait(epfd, events, 1024, -1);
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
}