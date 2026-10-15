#pragma once

// ==================== 复制相关进程共用的极简 TCP 工具 ====================
//
// 两个进程都需要同一套东西：
//   * kvstore 的同步驱动（replication/kvs_sync.cpp）—— 连 Master、发 SYNC、
//     读响应；
//   * 独立的 ebpf-agent（tools/ebpf_agent.cpp）—— 连 Master 注册、连 Replica
//     建增量通道、读握手响应。
//
// 这段逻辑原先在两个文件里各写了一份（函数名不同、超时处理也不同），这里收敛成
// header-only 的一份实现：两个进程各自编译一份，不引入共享库或额外依赖。
//
// 约定：
//   * timeout_ms <= 0 表示无限等待；
//   * socket 用「非阻塞 connect + 超时」建立，之后保持阻塞模式，并把超时写进
//     SO_SNDTIMEO / SO_RCVTIMEO；
//   * 失败一律返回 -1；kvs_net_recv_frame 成功返回负载长度。

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

static inline int kvs_net_wait(int fd, short events, int timeout_ms) {
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = events;
    pfd.revents = 0;
    return poll(&pfd, 1, timeout_ms > 0 ? timeout_ms : -1);
}

// 连接 ip:port；超时或失败返回 -1。成功后 socket 处于阻塞模式，
// 收发超时由 SO_SNDTIMEO / SO_RCVTIMEO 控制。
static inline int kvs_net_connect(const char* ip, int port, int timeout_ms) {
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
        fcntl(fd, F_SETFL, flags); // 恢复阻塞模式
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

// 发满 len 字节（EINTR 重试；阻塞超时由 SO_SNDTIMEO 决定）。
static inline int kvs_net_send_all(int fd, const void* buf, size_t len, int timeout_ms) {
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
            // 非阻塞兜底（正常路径 socket 是阻塞的，由 SO_SNDTIMEO 兜住）。
            if (kvs_net_wait(fd, POLLOUT, timeout_ms) <= 0) {
                return -1;
            }
            continue;
        }
        return -1;
    }
    return 0;
}

// 收满 len 字节；用 poll 控超时（不依赖 SO_RCVTIMEO），对端关闭或超时返回 -1。
static inline int kvs_net_recv_all(int fd, void* buf, size_t len, int timeout_ms) {
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
        // n == 0：对端关闭；n < 0：致命错误。
        return -1;
    }
    return 0;
}

// 发一帧：外层协议是「4 字节网络序长度 + 负载」。
static inline int kvs_net_send_frame(int fd, const char* payload, size_t len, int timeout_ms) {
    uint32_t net_len = htonl((uint32_t)len);
    if (kvs_net_send_all(fd, &net_len, sizeof(net_len), timeout_ms) != 0) {
        return -1;
    }
    return kvs_net_send_all(fd, payload, len, timeout_ms);
}

// 收一帧，负载写入 out（以 '\0' 结尾）；成功返回负载长度（>=1），失败返回 -1。
// out_size 同时充当最大帧长限制。
static inline int kvs_net_recv_frame(int fd, char* out, size_t out_size, int timeout_ms) {
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

// 收一帧并判断是否被受理（负载以 '+' 开头，例如 "+OK\r\n"）。
// 返回 1 = 受理，0 = 对端拒绝或响应非法，-1 = 收包失败。
static inline int kvs_net_recv_frame_ok(int fd, int timeout_ms) {
    char payload[1025];
    int len = kvs_net_recv_frame(fd, payload, sizeof(payload), timeout_ms);
    if (len < 0) {
        return -1;
    }
    return payload[0] == '+' ? 1 : 0;
}

#ifdef __cplusplus
}
#endif
