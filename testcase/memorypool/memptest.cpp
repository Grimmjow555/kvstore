

#include "testcase.h"
#include <arpa/inet.h>
#include <iostream>
#include <pthread.h>
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

#define PRINT_PASS 0
#define TEST_COUNT 40000

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

struct test_thread_context {
    const char* ip;
    unsigned short port;
    int structure_id;
};

void array_testcase_3w(int connfd);
void rbtree_testcase_3w(int connfd);
void hash_testcase_3w(int connfd);
void skiptable_testcase_3w(int connfd);

void* structure_test_entry(void* arg) {
    test_thread_context* context = (test_thread_context*)arg;
    int connfd = connect_tcpserver(context->ip, context->port);
    if (connfd < 0)
        return NULL;

    switch (context->structure_id) {
    case 0:
        array_testcase_3w(connfd);
        break;
    case 1:
        rbtree_testcase_3w(connfd);
        break;
    case 2:
        hash_testcase_3w(connfd);
        break;
    case 3:
        skiptable_testcase_3w(connfd);
        break;
    }

    close(connfd);
    return NULL;
}

void array_testcase_3w(int connfd) {

    int count = TEST_COUNT;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    // 第一阶段：SET Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "SET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "SET-Teacher", 0);
    }

    // 第二阶段：MOD Teacher{i} Darren{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "Darren%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "MOD", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "MOD-Darren-Teacher", 0);
    }

    // 第三阶段：DEL Teacher{i}
    for (i = 0; i < count; i++) {
        char key[64];
        snprintf(key, sizeof(key), "Teacher%d", i);

        const char* args[] = {key};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "DEL", 1, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "DEL-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("ARRAY testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void rbtree_testcase_3w(int connfd) {

    int count = TEST_COUNT;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    // 第一阶段：RSET Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "RSET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "RSET-Teacher", 0);
    }

    // 第二阶段：RMOD Teacher{i} Darren{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "Darren%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "RMOD", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "RMOD-Darren-Teacher", 0);
    }

    // 第三阶段：RDEL Teacher{i}
    for (i = 0; i < count; i++) {
        char key[64];
        snprintf(key, sizeof(key), "Teacher%d", i);

        const char* args[] = {key};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "RDEL", 1, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "RDEL-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("RBTREE testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void hash_testcase_3w(int connfd) {

    int count = TEST_COUNT;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    // 第一阶段：HSET Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "HSET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "HSET-Teacher", 0);
    }

    // 第二阶段：HMOD Teacher{i} Darren{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "Darren%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "HMOD", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "HMOD-Darren-Teacher", 0);
    }

    // 第三阶段：HDEL Teacher{i}
    for (i = 0; i < count; i++) {
        char key[64];
        snprintf(key, sizeof(key), "Teacher%d", i);

        const char* args[] = {key};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "HDEL", 1, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "HDEL-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("HASH testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void skiptable_testcase_3w(int connfd) {

    int count = TEST_COUNT;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    // 第一阶段：SSET Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "SSET", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "SSET-Teacher", 0);
    }

    // 第二阶段：SMOD Teacher{i} Darren{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "Darren%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "SMOD", 2, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "SMOD-Darren-Teacher", 0);
    }

    // 第三阶段：SDEL Teacher{i}
    for (i = 0; i < count; i++) {
        char key[64];
        snprintf(key, sizeof(key), "Teacher%d", i);

        const char* args[] = {key};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "SDEL", 1, args) < 0)
            exit(1);
        testcase(connfd, req, "+OK\r\n", "SDEL-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("SKIPTABLE testcase --> time_used: %d, qps: %d\n", time_used,
           3 * count * 1000 / time_used);
}

//   ./test 192.168.234.135  2000 1
// 0: rbtree; 1: array; 2: hash; 3: skiptable
int main(int argc, char* argv[]) {

    if (argc != 3) {
        printf("arg error\n");
        return -1;
    }

    char* ip = argv[1];
    unsigned short port = atoi(argv[2]);

    printf("四线程并发测试：每个线程负责一种存储结构\n");

    pthread_t threads[4];
    test_thread_context contexts[4] = {
        {ip, port, 0},
        {ip, port, 1},
        {ip, port, 2},
        {ip, port, 3},
    };

    for (int i = 0; i < 4; i++) {
        if (pthread_create(&threads[i], NULL, structure_test_entry, &contexts[i]) != 0) {
            perror("pthread_create");
            return -1;
        }
    }

    for (int i = 0; i < 4; i++) {
        pthread_join(threads[i], NULL);
    }
    printf("***memorypool test case finished***\n");

    return 0;
}
