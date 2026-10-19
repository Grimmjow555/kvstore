int send_msg(int connfd, const char* msg, int length);
int recv_msg(int connfd, char* msg, int length);
#include <arpa/inet.h>
#include <iostream>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

#define MAX_MSG_LENGTH 1024
#define TIME_SUB_MS(tv1, tv2)                                                                      \
    ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
#ifndef START_NUM
#define START_NUM 0
#endif
#define SET_NUMS 50000
#define PRINT_PASS 0

// 将空格分隔的命令字符串（如 "SSET Teacher King"）转换为 RESP 格式
// 返回静态缓冲区指针（调用后立即使用，因为会被后续调用覆盖）
/**
 * @brief 构建一个符合 RESP (Redis Serialization Protocol) 协议的请求字符串
 *
 * RESP 协议格式：
 *   *<参数个数>\r\n
 *   $<参数1长度>\r\n
 *   <参数1>\r\n
 *   $<参数2长度>\r\n
 *   <参数2>\r\n
 *   ...
 * 其中第一个参数固定为命令名（如 "SET"），后续为命令的参数。
 *
 * @param cmd   命令名称（例如 "SET", "GET", "SSET" 等）
 * @param argc  命令参数的个数（不包括命令名本身）
 * @param argv  指向参数指针数组，每个元素是一个以 '\0' 结尾的字符串
 *
 * @return 请求长度；缓冲区不足时返回 -1。
 *
 * @note 该函数支持参数中包含任意字节（包括空格、换行等），因为长度前缀
 *       保证了数据的边界，这是 RESP 协议的二进制安全特性。
 */
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

/**
 * @brief 打印一次统计的命令数、耗时与 QPS
 *
 * @param name      统计项名称，如 "INSERT ARRAY SET"
 * @param cmd_count 统计到的命令条数
 * @param time_ms   统计到的累计耗时（毫秒）
 */
static void print_stats(const char* name, long long cmd_count, long long time_ms) {
    long long safe_ms = time_ms > 0 ? time_ms : 1; // 避免除零
    double qps = (double)cmd_count * 1000.0 / (double)safe_ms;
    double avg_ms = cmd_count > 0 ? (double)time_ms / (double)cmd_count : 0.0;

    printf("%s --> commands: %lld, time_used: %lld ms, qps: %.0f, avg: %.4f ms/cmd\n", name,
           cmd_count, time_ms, qps, avg_ms);
}

void testcase(int connfd, const char* msg, const char* expected_pattern, const char* casename,
              int thread_id) {
    if (!msg || !expected_pattern || !casename)
        return;

    send_msg(connfd, msg, strlen(msg));

    char result[MAX_MSG_LENGTH] = {0};
    recv_msg(connfd, result, MAX_MSG_LENGTH);

    if (strcmp(result, expected_pattern) == 0) {
#if PRINT_PASS
        printf("==> PASS ->  %s\n", casename);
#endif
    } else {
        printf("==> FAILED -> %s, '%s' != '%s'\n", casename, result, expected_pattern);
        exit(1);
    }
}

int connect_tcpserver(const char* ip, unsigned short port) {

    int connfd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(struct sockaddr_in));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip);
    server_addr.sin_port = htons(port);

    if (0 != connect(connfd, (struct sockaddr*)&server_addr, sizeof(struct sockaddr_in))) {
        perror("connect");
        return -1;
    }
    printf("connect %s on port %d, fd: %d\n", ip, port, connfd);

    return connfd;
}

void array_testcase(int connfd) {
    char req[128];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < SET_NUMS / 4; i++) {
        // SET Teacher King
        char key[32];
        int len = sprintf(key, "Teacher%d", i + START_NUM / 4);
        key[len] = '\0'; // 确保字符串结束
        char value[32];
        len = sprintf(value, "King%d", i + START_NUM / 4);
        value[len] = '\0'; // 确保字符串结束
        const char* args1[] = {key, value};
        if (build_resp_request(req, sizeof(req), "SET", 2, args1) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "SET-Teacher", 0);
    }

    gettimeofday(&tv_end, NULL);
    print_stats("INSERT ARRAY SET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
}

void rbtree_testcase(int connfd) {
    char req[128];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i + START_NUM / 4);
        key[len] = '\0';
        char value[32];
        len = sprintf(value, "King%d", i + START_NUM / 4);
        value[len] = '\0';
        const char* args[] = {key, value};
        if (build_resp_request(req, sizeof(req), "RSET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "RSET-Teacher", 0);
    }

    gettimeofday(&tv_end, NULL);
    print_stats("INSERT RBTREE RSET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
}

void hash_testcase(int connfd) {
    char req[128];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i + START_NUM / 4);
        key[len] = '\0';
        char value[32];
        len = sprintf(value, "King%d", i + START_NUM / 4);
        value[len] = '\0';
        const char* args[] = {key, value};
        if (build_resp_request(req, sizeof(req), "HSET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "HSET-Teacher", 0);
    }

    gettimeofday(&tv_end, NULL);
    print_stats("INSERT HASH HSET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
}

void skiptable_testcase(int connfd) {
    char req[128];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    // const char* args1[] = {"SSET","Teacher", "King","SEXIST","Teacher"};

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i + START_NUM / 4);
        key[len] = '\0';
        char value[32];
        len = sprintf(value, "King%d", i + START_NUM / 4);
        value[len] = '\0';
        const char* args[] = {key, value};
        if (build_resp_request(req, sizeof(req), "SSET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "SSET-Teacher", 0);
    }

    gettimeofday(&tv_end, NULL);
    print_stats("INSERT SKIPTABLE SSET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
}

//   ./test 192.168.234.135  2000 1
//   ./test 39.97.42.225 9999 1
// 0: rbtree; 1: array; 2: hash; 3: skiptable
int main(int argc, char* argv[]) {

    if (argc != 3) {
        printf("arg error\n");
        return -1;
    }

    char* ip = argv[1];
    unsigned short port = atoi(argv[2]);

    int connfd = connect_tcpserver(ip, port);

    printf("INSERT key range: [%d, %d)\n", START_NUM / 4, START_NUM / 4 + SET_NUMS / 4);

    struct timeval tv_all_begin, tv_all_end;
    gettimeofday(&tv_all_begin, NULL);

    rbtree_testcase(connfd);

    array_testcase(connfd);

    hash_testcase(connfd);

    skiptable_testcase(connfd);

    gettimeofday(&tv_all_end, NULL);
    print_stats("INSERT TOTAL", SET_NUMS, TIME_SUB_MS(tv_all_end, tv_all_begin));

    printf("***insert %d records done***\n", SET_NUMS);

    return 0;
}


// 循环发送直到全部数据发出或失败。
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
    if (msg == NULL || length < 0) {
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
    if (length > 0) {
        memcpy(frame + sizeof(net_len), msg, (size_t)length);
    }

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
