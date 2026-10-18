

#include "testcase.h"
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
#define REQ_BUF_SIZE 256 // 请求内容较短，直接在栈上构建，避免频繁堆分配
#define TIME_SUB_MS(tv1, tv2)                                                                      \
    ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
#define SET_NUMS 100000
#define PRINT_PASS 0
#define SAVE 1 // 测试保存功能

/**
 * @brief 将命令构建为符合 RESP (Redis Serialization Protocol) 协议的请求字符串
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
 * @param buf       调用者提供的缓冲区（通常是栈数组）
 * @param capacity  缓冲区大小（字节数）
 * @param cmd       命令名称（例如 "SET", "GET", "SSET" 等）
 * @param argc      命令参数的个数（不包括命令名本身）
 * @param argv      指向参数指针数组，每个元素是一个以 '\0' 结尾的字符串
 *
 * @return 写入的请求长度；缓冲区不足或参数非法时返回 -1。
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

    APPEND_FORMAT("*%d\r\n", argc + 1); // +1 for command
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
 * @param name      统计项名称，如 "AOF SAVE array SET"
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
    char req[REQ_BUF_SIZE];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

#if SAVE

    for (int i = 0; i < SET_NUMS / 4; i++) {
        // SET Teacher King
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0'; // 确保字符串结束
        char value[32];
        len = sprintf(value, "King%d", i);
        value[len] = '\0'; // 确保字符串结束
        const char* args1[] = {key, value};
        if (build_resp_request(req, sizeof(req), "SET", 2, args1) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "SET-Teacher", 0);
    }

#else

    // GET 与 SAVE 使用相同数量的数据，逐条验证 RDB 恢复结果
    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        const char* args[] = {key};
        if (build_resp_request(req, sizeof(req), "GET", 1, args) < 0)
            exit(1);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase(connfd, req, expected, "GET-Teacher", 0);
    }

#endif

    gettimeofday(&tv_end, NULL);
#if SAVE
    print_stats("AOF SAVE array SET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
#else
    print_stats("AOF AUTO LOAD array GET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
#endif
}

void rbtree_testcase(int connfd) {
    char req[REQ_BUF_SIZE];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

#if SAVE
    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        char value[32];
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        if (build_resp_request(req, sizeof(req), "RSET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "RSET-Teacher", 0);
    }

#else

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        const char* args[] = {key};
        if (build_resp_request(req, sizeof(req), "RGET", 1, args) < 0)
            exit(1);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase(connfd, req, expected, "RGET-Teacher", 0);
    }
#endif

    gettimeofday(&tv_end, NULL);
#if SAVE
    print_stats("AOF SAVE rbtree RSET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
#else
    print_stats("AOF AUTO LOAD rbtree RGET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
#endif
}

void hash_testcase(int connfd) {
    char req[REQ_BUF_SIZE];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

#if SAVE
    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        char value[32];
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        if (build_resp_request(req, sizeof(req), "HSET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "HSET-Teacher", 0);
    }

#else

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        const char* args[] = {key};
        if (build_resp_request(req, sizeof(req), "HGET", 1, args) < 0)
            exit(1);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase(connfd, req, expected, "HGET-Teacher", 0);
    }
#endif

    gettimeofday(&tv_end, NULL);
#if SAVE
    print_stats("AOF SAVE hash HSET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
#else
    print_stats("AOF AUTO LOAD hash HGET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
#endif
}

void skiptable_testcase(int connfd) {
    char req[REQ_BUF_SIZE];
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    // const char* args1[] = {"SSET","Teacher", "King","SEXIST","Teacher"};
#if SAVE
    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        char value[32];
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        if (build_resp_request(req, sizeof(req), "SSET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "SSET-Teacher", 0);
    }

#else

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        const char* args[] = {key};
        if (build_resp_request(req, sizeof(req), "SGET", 1, args) < 0)
            exit(1);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase(connfd, req, expected, "SGET-Teacher", 0);
    }
#endif

    gettimeofday(&tv_end, NULL);
#if SAVE
    print_stats("AOF SAVE skiptable SSET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
#else
    print_stats("AOF AUTO LOAD skiptable SGET", SET_NUMS / 4, TIME_SUB_MS(tv_end, tv_begin));
#endif
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

#if SAVE
    printf("AOF SAVE: insert %d records, and recorded to AOF file\n", SET_NUMS);

    const char* args_save[] = {};
    char req[REQ_BUF_SIZE];
    if (build_resp_request(req, sizeof(req), "AOF CLEAR", 0, args_save) < 0)
        exit(1);
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);
    testcase(connfd, req, "+OK\r\n", "AOF CLEAR", 0);
    gettimeofday(&tv_end, NULL);
    print_stats("AOF SAVE: \"AOF CLEAR\"", 1, TIME_SUB_MS(tv_end, tv_begin));

    // 只统计后续真正的写命令，排除 AOF CLEAR 本身
    gettimeofday(&tv_begin, NULL);

    rbtree_testcase(connfd);

    array_testcase(connfd);

    hash_testcase(connfd);

    skiptable_testcase(connfd);

    gettimeofday(&tv_end, NULL);
    print_stats("AOF SAVE TOTAL", SET_NUMS, TIME_SUB_MS(tv_end, tv_begin));

#else

    printf("AOF AUTO LOAD: verify %d records restored at startup\n", SET_NUMS);

    // 持久化数据由服务启动时自动加载，这里只统计验证读取命令。
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    rbtree_testcase(connfd);

    array_testcase(connfd);

    hash_testcase(connfd);

    skiptable_testcase(connfd);

    gettimeofday(&tv_end, NULL);
    print_stats("AOF AUTO LOAD TOTAL", SET_NUMS, TIME_SUB_MS(tv_end, tv_begin));

    printf("***AOF TEST SUCCESS***\n");

#endif
    return 0;
}
