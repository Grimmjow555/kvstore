#include "kvs_netutil.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

static int kvs_net_wait(int fd, short events, int timeout_ms) {
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = events;
    pfd.revents = 0;
    return poll(&pfd, 1, timeout_ms > 0 ? timeout_ms : -1);
}

int kvs_net_connect(const char* ip, int port, int timeout_ms) {
    if (ip == NULL || ip[0] == '\0' || port <= 0 || port > 65535) {
        return -1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    int rc = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (rc != 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }
    if (rc != 0) {
        if (kvs_net_wait(fd, POLLOUT, timeout_ms) <= 0) {
            close(fd);
            return -1;
        }
        int so_error = 0;
        socklen_t slen = sizeof(so_error);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &slen) != 0 || so_error != 0) {
            close(fd);
            return -1;
        }
    }

    if (flags >= 0) {
        fcntl(fd, F_SETFL, flags);
    }
    if (timeout_ms > 0) {
        struct timeval tv;
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

static int kvs_net_send_all(int fd, const void* buf, size_t len, int timeout_ms) {
    const char* p = (const char*)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (kvs_net_wait(fd, POLLOUT, timeout_ms) <= 0) {
                return -1;
            }
            continue;
        }
        return -1;
    }
    return 0;
}

static int kvs_net_recv_all(int fd, void* buf, size_t len, int timeout_ms) {
    char* p = (char*)buf;
    size_t got = 0;
    while (got < len) {
        if (kvs_net_wait(fd, POLLIN, timeout_ms) <= 0) {
            return -1;
        }
        ssize_t n = recv(fd, p + got, len - got, 0);
        if (n > 0) {
            got += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return -1;
    }
    return 0;
}

int kvs_net_send_frame(int fd, const char* payload, size_t len, int timeout_ms) {
    uint32_t net_len = htonl((uint32_t)len);
    if (kvs_net_send_all(fd, &net_len, sizeof(net_len), timeout_ms) != 0) {
        return -1;
    }
    return kvs_net_send_all(fd, payload, len, timeout_ms);
}

int kvs_net_recv_frame(int fd, char* out, size_t out_size, int timeout_ms) {
    if (out == NULL || out_size < 2) {
        return -1;
    }
    uint32_t net_len = 0;
    if (kvs_net_recv_all(fd, &net_len, sizeof(net_len), timeout_ms) != 0) {
        return -1;
    }
    uint32_t len = ntohl(net_len);
    if (len == 0 || (size_t)len + 1 > out_size) {
        return -1;
    }
    if (kvs_net_recv_all(fd, out, len, timeout_ms) != 0) {
        return -1;
    }
    out[len] = '\0';
    return (int)len;
}

int kvs_net_recv_frame_ok(int fd, int timeout_ms) {
    char payload[1025];
    int len = kvs_net_recv_frame(fd, payload, sizeof(payload), timeout_ms);
    if (len < 0) {
        return -1;
    }
    return payload[0] == '+' ? 1 : 0;
}
