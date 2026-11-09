#include <arpa/inet.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

int send_msg(int connfd, const char* msg, int length);
int recv_msg(int connfd, char* msg, int length);

#define MAX_MSG_LENGTH 512
#define REQ_BUF_SIZE 256
#define TIME_SUB_MS(tv1, tv2)                                                                      \
    ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
#define SET_NUMS 400000
#define PRINT_PASS 0

// 与 testcase/AOF/save.cpp 使用相同的 RESP 请求构造方式和四引擎数据规模。
int build_resp_request(char* buf, size_t capacity, const char* cmd, int argc, const char* argv[]) {
    size_t offset = 0;

#define APPEND_FORMAT(...)                                                                         \
    do {                                                                                           \
        int written = snprintf(buf + offset, capacity - offset, __VA_ARGS__);                      \
        if (written < 0 || (size_t)written >= capacity - offset)                                   \
            return -1;                                                                             \
        offset += (size_t)written;                                                                 \
    } while (0)

    APPEND_FORMAT("*%d\r\n", argc + 1);
    APPEND_FORMAT("$%zu\r\n%s\r\n", strlen(cmd), cmd);
    for (int i = 0; i < argc; i++) {
        APPEND_FORMAT("$%zu\r\n%s\r\n", strlen(argv[i]), argv[i]);
    }

#undef APPEND_FORMAT
    return (int)offset;
}

static void print_stats(const char* name, long long cmd_count, long long time_ms) {
    long long safe_ms = time_ms > 0 ? time_ms : 1;
    double qps = (double)cmd_count * 1000.0 / (double)safe_ms;
    double avg_ms = cmd_count > 0 ? (double)time_ms / (double)cmd_count : 0.0;

    printf("%s --> commands: %lld, time_used: %lld ms, qps: %.0f, avg: %.4f ms/cmd\n", name,
           cmd_count, time_ms, qps, avg_ms);
}

void testcase(int connfd, const char* msg, int length, const char* casename) {
    if (!msg || !casename || length <= 0) {
        printf("==> FAILED -> invalid testcase input: %s\n", casename ? casename : "(null)");
        exit(1);
    }

    if (send_msg(connfd, msg, length) != 0) {
        printf("==> FAILED -> %s, send error\n", casename);
        exit(1);
    }

    char result[MAX_MSG_LENGTH] = {0};
    int result_length = recv_msg(connfd, result, MAX_MSG_LENGTH - 1);
    if (result_length != length || memcmp(result, msg, (size_t)length) != 0) {
        printf("==> FAILED -> %s, echo mismatch (received %d bytes, expected %d)\n", casename,
               result_length, length);
        exit(1);
    }

#if PRINT_PASS
    printf("==> PASS -> %s\n", casename);
#endif
}

int connect_tcpserver(const char* ip, unsigned short port) {
    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    if (connfd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &server_addr.sin_addr) != 1 ||
        connect(connfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) != 0) {
        perror("connect");
        close(connfd);
        return -1;
    }

    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(connfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(connfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    printf("connect %s on port %d, fd: %d\n", ip, port, connfd);
    return connfd;
}

void array_testcase(int connfd) {
    char req[REQ_BUF_SIZE];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        char value[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        int req_len = build_resp_request(req, sizeof(req), "SET", 2, args);
        if (req_len < 0) {
            printf("==> FAILED -> build SET request %d\n", i);
            exit(1);
        }
        testcase(connfd, req, req_len, "SET-Teacher");
    }

    gettimeofday(&tv_end, NULL);
    print_stats("ECHO array SET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
}

void rbtree_testcase(int connfd) {
    char req[REQ_BUF_SIZE];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        char value[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        int req_len = build_resp_request(req, sizeof(req), "RSET", 2, args);
        if (req_len < 0) {
            printf("==> FAILED -> build RSET request %d\n", i);
            exit(1);
        }
        testcase(connfd, req, req_len, "RSET-Teacher");
    }

    gettimeofday(&tv_end, NULL);
    print_stats("ECHO rbtree RSET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
}

void hash_testcase(int connfd) {
    char req[REQ_BUF_SIZE];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        char value[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        int req_len = build_resp_request(req, sizeof(req), "HSET", 2, args);
        if (req_len < 0) {
            printf("==> FAILED -> build HSET request %d\n", i);
            exit(1);
        }
        testcase(connfd, req, req_len, "HSET-Teacher");
    }

    gettimeofday(&tv_end, NULL);
    print_stats("ECHO hash HSET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
}

void skiptable_testcase(int connfd) {
    char req[REQ_BUF_SIZE];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        char value[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        int req_len = build_resp_request(req, sizeof(req), "SSET", 2, args);
        if (req_len < 0) {
            printf("==> FAILED -> build SSET request %d\n", i);
            exit(1);
        }
        testcase(connfd, req, req_len, "SSET-Teacher");
    }

    gettimeofday(&tv_end, NULL);
    print_stats("ECHO skiptable SSET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        printf("usage: %s <ip> <port>\n", argv[0]);
        return 1;
    }

    unsigned short port = (unsigned short)atoi(argv[2]);
    if (port == 0) {
        printf("invalid port: %s\n", argv[2]);
        return 1;
    }

    int connfd = connect_tcpserver(argv[1], port);
    if (connfd < 0) {
        return 1;
    }

    printf("ECHO: verify %d records across four storage engines\n", SET_NUMS);
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    // rbtree_testcase(connfd);
    // array_testcase(connfd);
    hash_testcase(connfd);
    // skiptable_testcase(connfd);

    gettimeofday(&tv_end, NULL);
    print_stats("ECHO TOTAL", SET_NUMS, TIME_SUB_MS(tv_end, tv_begin));
    printf("***ECHO TEST SUCCESS***\n");

    close(connfd);
    return 0;
}

int send_all(int fd, const void* buffer, size_t length) {
    const char* ptr = (const char*)buffer;
    size_t sent = 0;
    while (sent < length) {
        ssize_t n = send(fd, ptr + sent, length - sent, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

int send_msg(int connfd, const char* msg, int length) {
    if (msg == NULL || length <= 0) {
        return -1;
    }

    uint32_t net_len = htonl((uint32_t)length);
    size_t total_len = sizeof(net_len) + (size_t)length;
    char stack_buf[512];
    char* frame = stack_buf;
    if (total_len > sizeof(stack_buf)) {
        frame = (char*)malloc(total_len);
        if (frame == NULL) {
            return -1;
        }
    }

    memcpy(frame, &net_len, sizeof(net_len));
    memcpy(frame + sizeof(net_len), msg, (size_t)length);
    int rc = send_all(connfd, frame, total_len);
    if (frame != stack_buf) {
        free(frame);
    }
    return rc;
}

int recv_all(int fd, void* buffer, size_t length) {
    char* ptr = (char*)buffer;
    size_t received = 0;
    while (received < length) {
        ssize_t n = recv(fd, ptr + received, length - received, 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return -1;
        }
        received += (size_t)n;
    }
    return 0;
}

int recv_msg(int connfd, char* msg, int length) {
    if (msg == NULL || length < 0) {
        return -1;
    }

    uint32_t net_len = 0;
    if (recv_all(connfd, &net_len, sizeof(net_len)) != 0) {
        return -1;
    }
    uint32_t data_len = ntohl(net_len);
    if (data_len > (uint32_t)length) {
        return -2;
    }
    if (recv_all(connfd, msg, data_len) != 0) {
        return -1;
    }

    msg[data_len] = '\0';
    return (int)data_len;
}
