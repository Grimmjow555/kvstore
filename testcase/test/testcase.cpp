

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

#define PRINT_PASS 0
#define TEST_COUNT 1000

#define LEVEL1 1 //使用最基础的9条测试样例，测试一次
#define LEVEL2 0 //使用最基础的9条测试样例，测试 TEST_COUNT 次

#define LEVEL3w 0 //使用30000条测试样例

#define LEVEL3 0 //使用多线程，每个线程9条测试样例，测试 TEST_COUNT 次
//若只有一套样例，考虑到线程安全，需要对这一套样例加锁，实际上和串行无异。
//所以我们多套样例应该互不相同，互不影响结果。这样可以不加锁使用多线程，具体做法是为每一套样例的key添加对应的fd

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

void testcase_raw(int connfd, const char* msg, const char* expected_pattern, const char* casename,
                  int thread_id) {
    if (!msg || !expected_pattern || !casename)
        return;

    send_msg(connfd, msg, strlen(msg));

    char result[MAX_MSG_LENGTH] = {0};
    recv_msg(connfd, result, MAX_MSG_LENGTH);

    if (strcmp(result, expected_pattern) == 0) {
#if PRINT_PASS
#if LEVEL3
        printf("thread[%d]==> PASS ->  %s\n", thread_id, casename);
#else
        printf("==> PASS ->  %s\n", casename);
#endif
#endif
    } else {
#if LEVEL3
        printf("thread[%d]==> FAILED -> %s, '%s' != '%s'\n", thread_id, casename, result,
               expected_pattern);
#else
        printf("==> FAILED -> %s, '%s' != '%s'\n", casename, result, expected_pattern);
#endif
        exit(1);
    }
}
void testcase(int connfd, const char* msg, const char* pattern, const char* casename,
              int thread_id) {

    if (!msg || !pattern || !casename)
        return;

    send_msg(connfd, msg, strlen(msg));

    char result[MAX_MSG_LENGTH] = {0};
    recv_msg(connfd, result, MAX_MSG_LENGTH);

    if (strcmp(result, pattern) == 0) {

#if PRINT_PASS
#if LEVEL3
        printf("thread[%d]==> PASS ->  %s\n", thread_id, casename);
#else
        printf("==> PASS ->  %s\n", casename);
#endif
#endif

    } else {

#if LEVEL3
        printf("thread[%d]==> FAILED -> %s, '%s' != '%s' \n", thread_id, casename, result, pattern);

#else
        printf("==> FAILED -> %s, '%s' != '%s' \n", casename, result, pattern);
#endif

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

    // SET Teacher King
    const char* args1[] = {"Teacher", "King"};
    if (build_resp_request(req, sizeof(req), "SET", 2, args1) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "SET-Teacher", 0);

    // GET Teacher
    const char* args2[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "GET", 1, args2) < 0)
        exit(1);
    testcase_raw(connfd, req, "$4\r\nKing\r\n", "GET-King-Teacher", 0);

    // MOD Teacher Darren
    const char* args3[] = {"Teacher", "Darren"};
    if (build_resp_request(req, sizeof(req), "MOD", 2, args3) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "MOD-D-Teacher", 0);

    // GET Teacher (should return Darren)
    const char* args4[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "GET", 1, args4) < 0)
        exit(1);
    testcase_raw(connfd, req, "$6\r\nDarren\r\n", "GET-Darren-Teacher", 0);

    // EXIST Teacher
    const char* args5[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "EXIST", 1, args5) < 0)
        exit(1);
    testcase_raw(connfd, req, "$5\r\nEXIST\r\n", "EXIST-Teacher", 0); // RESP 整数存在为 1

    // DEL Teacher
    const char* args6[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "DEL", 1, args6) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "DEL-Teacher", 0);

    // 再 GET 应返回 NO EXIST（根据你希望的格式）
    const char* args7[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "GET", 1, args7) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "GET-K-Teacher",
                 0); // 假设你返回 $8\r\nNO EXIST\r\n

    // MOD 不存在的键
    const char* args8[] = {"Teacher", "KING"};
    if (build_resp_request(req, sizeof(req), "MOD", 2, args8) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "MOD-K-Teacher", 0);

    // EXIST 不存在的键
    const char* args9[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "EXIST", 1, args9) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "EXIST-Teacher", 0);
}

void array_testcase_pth(int connfd, int thread_id) {
    int count = TEST_COUNT;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        // 生成每个线程独立的键名
        char key[64];
        snprintf(key, sizeof(key), "Teacher%d", thread_id);

        // SET key King
        const char* args1[] = {key, "King"};
        char req1[REQ_BUF_SIZE];
        if (build_resp_request(req1, sizeof(req1), "SET", 2, args1) < 0)
            exit(1);
        testcase_raw(connfd, req1, "+OK\r\n", "SET-Teacher", thread_id);

        // GET key
        const char* args2[] = {key};
        char req2[REQ_BUF_SIZE];
        if (build_resp_request(req2, sizeof(req2), "GET", 1, args2) < 0)
            exit(1);
        testcase_raw(connfd, req2, "$4\r\nKing\r\n", "GET-Teacher", thread_id);

        // MOD key Darren
        const char* args3[] = {key, "Darren"};
        char req3[REQ_BUF_SIZE];
        if (build_resp_request(req3, sizeof(req3), "MOD", 2, args3) < 0)
            exit(1);
        testcase_raw(connfd, req3, "+OK\r\n", "MOD-Teacher", thread_id);

        // GET key (should return Darren)
        const char* args4[] = {key};
        char req4[REQ_BUF_SIZE];
        if (build_resp_request(req4, sizeof(req4), "GET", 1, args4) < 0)
            exit(1);
        testcase_raw(connfd, req4, "$6\r\nDarren\r\n", "GET-Teacher", thread_id);

        // EXIST key
        const char* args5[] = {key};
        char req5[REQ_BUF_SIZE];
        if (build_resp_request(req5, sizeof(req5), "EXIST", 1, args5) < 0)
            exit(1);
        testcase_raw(connfd, req5, "$5\r\nEXIST\r\n", "EXIST-Teacher", thread_id);

        // DEL key
        const char* args6[] = {key};
        char req6[REQ_BUF_SIZE];
        if (build_resp_request(req6, sizeof(req6), "DEL", 1, args6) < 0)
            exit(1);
        testcase_raw(connfd, req6, "+OK\r\n", "DEL-Teacher", thread_id);

        // GET key (should return NO EXIST)
        const char* args7[] = {key};
        char req7[REQ_BUF_SIZE];
        if (build_resp_request(req7, sizeof(req7), "GET", 1, args7) < 0)
            exit(1);
        testcase_raw(connfd, req7, "$8\r\nNO EXIST\r\n", "GET-Teacher", thread_id);

        // MOD key KING (key not exist)
        const char* args8[] = {key, "KING"};
        char req8[REQ_BUF_SIZE];
        if (build_resp_request(req8, sizeof(req8), "MOD", 2, args8) < 0)
            exit(1);
        testcase_raw(connfd, req8, "$8\r\nNO EXIST\r\n", "MOD-Teacher", thread_id);

        // EXIST key (key not exist)
        const char* args9[] = {key};
        char req9[REQ_BUF_SIZE];
        if (build_resp_request(req9, sizeof(req9), "EXIST", 1, args9) < 0)
            exit(1);
        testcase_raw(connfd, req9, "$8\r\nNO EXIST\r\n", "EXIST-Teacher", thread_id);
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);

    printf("thread[%d] ARRAY testcase --> time_used: %d, qps: %d\n", thread_id, time_used,
           9 * count * 1000 / time_used);
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
        testcase_raw(connfd, req, "+OK\r\n", "SET-Teacher", 0);
    }

    // 第二阶段：GET Teacher{i}  -> 期望返回 King{i} (RESP bulk string)
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        // 构造期望的 RESP 响应：$len\r\nvalue\r\n
        int len = strlen(value);
        char expected[128];
        snprintf(expected, sizeof(expected), "$%d\r\n%s\r\n", len, value);

        const char* args[] = {key};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "GET", 1, args) < 0)
            exit(1);
        testcase_raw(connfd, req, expected, "GET-King-Teacher", 0);
    }

    // 第三阶段：MOD Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "MOD", 2, args) < 0)
            exit(1);
        testcase_raw(connfd, req, "+OK\r\n", "MOD-King-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("ARRAY testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void rbtree_testcase(int connfd) {
    char req[REQ_BUF_SIZE];

    // RSET Teacher King
    const char* args1[] = {"Teacher", "King"};
    if (build_resp_request(req, sizeof(req), "RSET", 2, args1) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "RSET-Teacher", 0);

    // RGET Teacher
    const char* args2[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "RGET", 1, args2) < 0)
        exit(1);
    testcase_raw(connfd, req, "$4\r\nKing\r\n", "RGET-King-Teacher", 0);

    // RMOD Teacher Darren
    const char* args3[] = {"Teacher", "Darren"};
    if (build_resp_request(req, sizeof(req), "RMOD", 2, args3) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "RMOD-D-Teacher", 0);

    // RGET Teacher (should return Darren)
    const char* args4[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "RGET", 1, args4) < 0)
        exit(1);
    testcase_raw(connfd, req, "$6\r\nDarren\r\n", "RGET-Darren-Teacher", 0);

    // REXIST Teacher
    const char* args5[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "REXIST", 1, args5) < 0)
        exit(1);
    testcase_raw(connfd, req, "$5\r\nEXIST\r\n", "REXIST-Teacher", 0); // RESP 整数存在为 1

    // RDEL Teacher
    const char* args6[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "RDEL", 1, args6) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "RDEL-Teacher", 0);

    // 再 GET 应返回 NO EXIST（根据你希望的格式）
    const char* args7[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "RGET", 1, args7) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "RGET-K-Teacher",
                 0); // 假设你返回 $8\r\nNO EXIST\r\n

    // RMOD 不存在的键
    const char* args8[] = {"Teacher", "KING"};
    if (build_resp_request(req, sizeof(req), "RMOD", 2, args8) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "RMOD-K-Teacher", 0);

    // REXIST 不存在的键
    const char* args9[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "REXIST", 1, args9) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "REXIST-Teacher", 0);
}

void rbtree_testcase_pth(int connfd, int thread_id) {
    int count = TEST_COUNT;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        // 生成每个线程独立的键名
        char key[64];
        snprintf(key, sizeof(key), "Teacher%d", thread_id);

        // RSET key King
        const char* args1[] = {key, "King"};
        char req1[REQ_BUF_SIZE];
        if (build_resp_request(req1, sizeof(req1), "RSET", 2, args1) < 0)
            exit(1);
        testcase_raw(connfd, req1, "+OK\r\n", "RSET-Teacher", thread_id);

        // RGET key
        const char* args2[] = {key};
        char req2[REQ_BUF_SIZE];
        if (build_resp_request(req2, sizeof(req2), "RGET", 1, args2) < 0)
            exit(1);
        testcase_raw(connfd, req2, "$4\r\nKing\r\n", "RGET-Teacher", thread_id);

        // RMOD key Darren
        const char* args3[] = {key, "Darren"};
        char req3[REQ_BUF_SIZE];
        if (build_resp_request(req3, sizeof(req3), "RMOD", 2, args3) < 0)
            exit(1);
        testcase_raw(connfd, req3, "+OK\r\n", "RMOD-Teacher", thread_id);

        // RGET key (should return Darren)
        const char* args4[] = {key};
        char req4[REQ_BUF_SIZE];
        if (build_resp_request(req4, sizeof(req4), "RGET", 1, args4) < 0)
            exit(1);
        testcase_raw(connfd, req4, "$6\r\nDarren\r\n", "RGET-Teacher", thread_id);

        // REXIST key
        const char* args5[] = {key};
        char req5[REQ_BUF_SIZE];
        if (build_resp_request(req5, sizeof(req5), "REXIST", 1, args5) < 0)
            exit(1);
        testcase_raw(connfd, req5, "$5\r\nEXIST\r\n", "REXIST-Teacher", thread_id);

        // RDEL key
        const char* args6[] = {key};
        char req6[REQ_BUF_SIZE];
        if (build_resp_request(req6, sizeof(req6), "RDEL", 1, args6) < 0)
            exit(1);
        testcase_raw(connfd, req6, "+OK\r\n", "RDEL-Teacher", thread_id);

        // RGET key (should return NO EXIST)
        const char* args7[] = {key};
        char req7[REQ_BUF_SIZE];
        if (build_resp_request(req7, sizeof(req7), "RGET", 1, args7) < 0)
            exit(1);
        testcase_raw(connfd, req7, "$8\r\nNO EXIST\r\n", "RGET-Teacher", thread_id);

        // RMOD key KING (key not exist)
        const char* args8[] = {key, "KING"};
        char req8[REQ_BUF_SIZE];
        if (build_resp_request(req8, sizeof(req8), "RMOD", 2, args8) < 0)
            exit(1);
        testcase_raw(connfd, req8, "$8\r\nNO EXIST\r\n", "RMOD-Teacher", thread_id);

        // REXIST key (key not exist)
        const char* args9[] = {key};
        char req9[REQ_BUF_SIZE];
        if (build_resp_request(req9, sizeof(req9), "REXIST", 1, args9) < 0)
            exit(1);
        testcase_raw(connfd, req9, "$8\r\nNO EXIST\r\n", "REXIST-Teacher", thread_id);
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);

    printf("thread[%d] RBTREE testcase --> time_used: %d, qps: %d\n", thread_id, time_used,
           9 * count * 1000 / time_used);
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
        testcase_raw(connfd, req, "+OK\r\n", "RSET-Teacher", 0);
    }

    // 第二阶段：RGET Teacher{i}  -> 期望返回 King{i} (RESP bulk string)
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        // 构造期望的 RESP 响应：$len\r\nvalue\r\n
        int len = strlen(value);
        char expected[128];
        snprintf(expected, sizeof(expected), "$%d\r\n%s\r\n", len, value);

        const char* args[] = {key};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "RGET", 1, args) < 0)
            exit(1);
        testcase_raw(connfd, req, expected, "RGET-King-Teacher", 0);
    }

    // 第三阶段：RMOD Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "RMOD", 2, args) < 0)
            exit(1);
        testcase_raw(connfd, req, "+OK\r\n", "RMOD-King-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("RBTREE testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void hash_testcase(int connfd) {
    char req[REQ_BUF_SIZE];

    // HSET Teacher King
    const char* args1[] = {"Teacher", "King"};
    if (build_resp_request(req, sizeof(req), "HSET", 2, args1) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "HSET-Teacher", 0);

    // HGET Teacher
    const char* args2[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "HGET", 1, args2) < 0)
        exit(1);
    testcase_raw(connfd, req, "$4\r\nKing\r\n", "HGET-King-Teacher", 0);

    // HMOD Teacher Darren
    const char* args3[] = {"Teacher", "Darren"};
    if (build_resp_request(req, sizeof(req), "HMOD", 2, args3) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "HMOD-D-Teacher", 0);

    // HGET Teacher (should return Darren)
    const char* args4[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "HGET", 1, args4) < 0)
        exit(1);
    testcase_raw(connfd, req, "$6\r\nDarren\r\n", "HGET-Darren-Teacher", 0);

    // HEXIST Teacher
    const char* args5[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "HEXIST", 1, args5) < 0)
        exit(1);
    testcase_raw(connfd, req, "$5\r\nEXIST\r\n", "HEXIST-Teacher", 0); // RESP 整数存在为 1

    // HDEL Teacher
    const char* args6[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "HDEL", 1, args6) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "HDEL-Teacher", 0);

    // 再 GET 应返回 NO EXIST（根据你希望的格式）
    const char* args7[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "HGET", 1, args7) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "HGET-K-Teacher",
                 0); // 假设你返回 $8\r\nNO EXIST\r\n

    // HMOD 不存在的键
    const char* args8[] = {"Teacher", "KING"};
    if (build_resp_request(req, sizeof(req), "HMOD", 2, args8) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "HMOD-K-Teacher", 0);

    // HEXIST 不存在的键
    const char* args9[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "HEXIST", 1, args9) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "HEXIST-Teacher", 0);
}

void hash_testcase_pth(int connfd, int thread_id) {
    int count = TEST_COUNT;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        // 生成每个线程独立的键名
        char key[64];
        snprintf(key, sizeof(key), "Teacher%d", thread_id);

        // HSET key King
        const char* args1[] = {key, "King"};
        char req1[REQ_BUF_SIZE];
        if (build_resp_request(req1, sizeof(req1), "HSET", 2, args1) < 0)
            exit(1);
        testcase_raw(connfd, req1, "+OK\r\n", "HSET-Teacher", thread_id);

        // HGET key
        const char* args2[] = {key};
        char req2[REQ_BUF_SIZE];
        if (build_resp_request(req2, sizeof(req2), "HGET", 1, args2) < 0)
            exit(1);
        testcase_raw(connfd, req2, "$4\r\nKing\r\n", "HGET-Teacher", thread_id);

        // HMOD key Darren
        const char* args3[] = {key, "Darren"};
        char req3[REQ_BUF_SIZE];
        if (build_resp_request(req3, sizeof(req3), "HMOD", 2, args3) < 0)
            exit(1);
        testcase_raw(connfd, req3, "+OK\r\n", "HMOD-Teacher", thread_id);

        // HGET key (should return Darren)
        const char* args4[] = {key};
        char req4[REQ_BUF_SIZE];
        if (build_resp_request(req4, sizeof(req4), "HGET", 1, args4) < 0)
            exit(1);
        testcase_raw(connfd, req4, "$6\r\nDarren\r\n", "HGET-Teacher", thread_id);

        // HEXIST key
        const char* args5[] = {key};
        char req5[REQ_BUF_SIZE];
        if (build_resp_request(req5, sizeof(req5), "HEXIST", 1, args5) < 0)
            exit(1);
        testcase_raw(connfd, req5, "$5\r\nEXIST\r\n", "HEXIST-Teacher", thread_id);

        // HDEL key
        const char* args6[] = {key};
        char req6[REQ_BUF_SIZE];
        if (build_resp_request(req6, sizeof(req6), "HDEL", 1, args6) < 0)
            exit(1);
        testcase_raw(connfd, req6, "+OK\r\n", "HDEL-Teacher", thread_id);

        // HGET key (should return NO EXIST)
        const char* args7[] = {key};
        char req7[REQ_BUF_SIZE];
        if (build_resp_request(req7, sizeof(req7), "HGET", 1, args7) < 0)
            exit(1);
        testcase_raw(connfd, req7, "$8\r\nNO EXIST\r\n", "HGET-Teacher", thread_id);

        // HMOD key KING (key not exist)
        const char* args8[] = {key, "KING"};
        char req8[REQ_BUF_SIZE];
        if (build_resp_request(req8, sizeof(req8), "HMOD", 2, args8) < 0)
            exit(1);
        testcase_raw(connfd, req8, "$8\r\nNO EXIST\r\n", "HMOD-Teacher", thread_id);

        // HEXIST key (key not exist)
        const char* args9[] = {key};
        char req9[REQ_BUF_SIZE];
        if (build_resp_request(req9, sizeof(req9), "HEXIST", 1, args9) < 0)
            exit(1);
        testcase_raw(connfd, req9, "$8\r\nNO EXIST\r\n", "HEXIST-Teacher", thread_id);
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);

    printf("thread[%d] HASH testcase --> time_used: %d, qps: %d\n", thread_id, time_used,
           9 * count * 1000 / time_used);
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
        testcase_raw(connfd, req, "+OK\r\n", "HSET-Teacher", 0);
    }

    // 第二阶段：HGET Teacher{i}  -> 期望返回 King{i} (RESP bulk string)
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        // 构造期望的 RESP 响应：$len\r\nvalue\r\n
        int len = strlen(value);
        char expected[128];
        snprintf(expected, sizeof(expected), "$%d\r\n%s\r\n", len, value);

        const char* args[] = {key};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "HGET", 1, args) < 0)
            exit(1);
        testcase_raw(connfd, req, expected, "HGET-King-Teacher", 0);
    }

    // 第三阶段：HMOD Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "HMOD", 2, args) < 0)
            exit(1);
        testcase_raw(connfd, req, "+OK\r\n", "HMOD-King-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("HASH testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void skiptable_testcase(int connfd) {
    char req[REQ_BUF_SIZE];

    // const char* args1[] = {"SSET","Teacher", "King","SEXIST","Teacher"};

    // SSET Teacher King
    const char* args1[] = {"Teacher", "King"};
    if (build_resp_request(req, sizeof(req), "SSET", 2, args1) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "SSET-Teacher", 0);

    // SGET Teacher
    const char* args2[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "SGET", 1, args2) < 0)
        exit(1);
    testcase_raw(connfd, req, "$4\r\nKing\r\n", "SGET-King-Teacher", 0);

    // SMOD Teacher Darren
    const char* args3[] = {"Teacher", "Darren"};
    if (build_resp_request(req, sizeof(req), "SMOD", 2, args3) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "SMOD-D-Teacher", 0);

    // SGET Teacher (should return Darren)
    const char* args4[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "SGET", 1, args4) < 0)
        exit(1);
    testcase_raw(connfd, req, "$6\r\nDarren\r\n", "SGET-Darren-Teacher", 0);

    // SEXIST Teacher
    const char* args5[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "SEXIST", 1, args5) < 0)
        exit(1);
    testcase_raw(connfd, req, "$5\r\nEXIST\r\n", "SEXIST-Teacher", 0); // RESP 整数存在为 1

    // SDEL Teacher
    const char* args6[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "SDEL", 1, args6) < 0)
        exit(1);
    testcase_raw(connfd, req, "+OK\r\n", "SDEL-Teacher", 0);

    // 再 GET 应返回 NO EXIST（根据你希望的格式）
    const char* args7[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "SGET", 1, args7) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "SGET-K-Teacher",
                 0); // 假设你返回 $8\r\nNO EXIST\r\n

    // SMOD 不存在的键
    const char* args8[] = {"Teacher", "KING"};
    if (build_resp_request(req, sizeof(req), "SMOD", 2, args8) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "SMOD-K-Teacher", 0);

    // SEXIST 不存在的键
    const char* args9[] = {"Teacher"};
    if (build_resp_request(req, sizeof(req), "SEXIST", 1, args9) < 0)
        exit(1);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "SEXIST-Teacher", 0);
}

void skiptable_testcase_pth(int connfd, int thread_id) {
    int count = TEST_COUNT;
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        // 根据线程 ID 生成唯一的键名
        char key[64];
        snprintf(key, sizeof(key), "Teacher%d", thread_id);

        // SSET key King
        const char* args1[] = {key, "King"};
        char req1[REQ_BUF_SIZE];
        if (build_resp_request(req1, sizeof(req1), "SSET", 2, args1) < 0)
            exit(1);
        testcase_raw(connfd, req1, "+OK\r\n", "SSET-Teacher", thread_id);

        // SGET key
        const char* args2[] = {key};
        char req2[REQ_BUF_SIZE];
        if (build_resp_request(req2, sizeof(req2), "SGET", 1, args2) < 0)
            exit(1);
        testcase_raw(connfd, req2, "$4\r\nKing\r\n", "SGET-Teacher", thread_id);

        // SMOD key Darren
        const char* args3[] = {key, "Darren"};
        char req3[REQ_BUF_SIZE];
        if (build_resp_request(req3, sizeof(req3), "SMOD", 2, args3) < 0)
            exit(1);
        testcase_raw(connfd, req3, "+OK\r\n", "SMOD-Teacher", thread_id);

        // SGET key (should return Darren)
        const char* args4[] = {key};
        char req4[REQ_BUF_SIZE];
        if (build_resp_request(req4, sizeof(req4), "SGET", 1, args4) < 0)
            exit(1);
        testcase_raw(connfd, req4, "$6\r\nDarren\r\n", "SGET-Teacher", thread_id);

        // SEXIST key
        const char* args5[] = {key};
        char req5[REQ_BUF_SIZE];
        if (build_resp_request(req5, sizeof(req5), "SEXIST", 1, args5) < 0)
            exit(1);
        testcase_raw(connfd, req5, "$5\r\nEXIST\r\n", "SEXIST-Teacher", thread_id);

        // SDEL key
        const char* args6[] = {key};
        char req6[REQ_BUF_SIZE];
        if (build_resp_request(req6, sizeof(req6), "SDEL", 1, args6) < 0)
            exit(1);
        testcase_raw(connfd, req6, "+OK\r\n", "SDEL-Teacher", thread_id);

        // SGET key (should return NO EXIST)
        const char* args7[] = {key};
        char req7[REQ_BUF_SIZE];
        if (build_resp_request(req7, sizeof(req7), "SGET", 1, args7) < 0)
            exit(1);
        testcase_raw(connfd, req7, "$8\r\nNO EXIST\r\n", "SGET-K-Teacher", thread_id);

        // SMOD key KING (key not exist)
        const char* args8[] = {key, "KING"};
        char req8[REQ_BUF_SIZE];
        if (build_resp_request(req8, sizeof(req8), "SMOD", 2, args8) < 0)
            exit(1);
        testcase_raw(connfd, req8, "$8\r\nNO EXIST\r\n", "SMOD-K-Teacher", thread_id);

        // SEXIST key (key not exist)
        const char* args9[] = {key};
        char req9[REQ_BUF_SIZE];
        if (build_resp_request(req9, sizeof(req9), "SEXIST", 1, args9) < 0)
            exit(1);
        testcase_raw(connfd, req9, "$8\r\nNO EXIST\r\n", "SEXIST-Teacher", thread_id);
    }

    gettimeofday(&tv_end, NULL);
    int time_used = TIME_SUB_MS(tv_end, tv_begin);

    printf("thread[%d] SKIPTABLE testcase --> time_used: %d, qps: %d\n", thread_id, time_used,
           9 * count * 1000 / time_used);
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
        testcase_raw(connfd, req, "+OK\r\n", "SSET-Teacher", 0);
    }

    // 第二阶段：SGET Teacher{i}  -> 期望返回 King{i} (RESP bulk string)
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        // 构造期望的 RESP 响应：$len\r\nvalue\r\n
        int len = strlen(value);
        char expected[128];
        snprintf(expected, sizeof(expected), "$%d\r\n%s\r\n", len, value);

        const char* args[] = {key};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "SGET", 1, args) < 0)
            exit(1);
        testcase_raw(connfd, req, expected, "SGET-King-Teacher", 0);
    }

    // 第三阶段：SMOD Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char req[REQ_BUF_SIZE];
        if (build_resp_request(req, sizeof(req), "SMOD", 2, args) < 0)
            exit(1);
        testcase_raw(connfd, req, "+OK\r\n", "SMOD-King-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("SKIPTABLE testcase --> time_used: %d, qps: %d\n", time_used,
           3 * count * 1000 / time_used);
}
#if LEVEL3

#define THREAD_NUM 10
struct test_context_t {
    int thread_id;
    const char* ip;
    unsigned short port;
    int mode;
};

static void* test_qps_entry(void* arg) {
    test_context_t* pctx = (test_context_t*)arg;
    int connfd = connect_tcpserver(pctx->ip, pctx->port);
    if (connfd < 0) {
        printf("connect_tcpserver failed\n");
        return nullptr;
    }
    printf("thread[%d] --> connfd[%d]\n", pctx->thread_id, connfd);

    if (pctx->mode == 0) {
        rbtree_testcase_pth(connfd, pctx->thread_id);
    } else if (pctx->mode == 1) {
        array_testcase_pth(connfd, pctx->thread_id);
    } else if (pctx->mode == 2) {
        hash_testcase_pth(connfd, pctx->thread_id);
    } else if (pctx->mode == 3) {
        skiptable_testcase_pth(connfd, pctx->thread_id);
    }

    return nullptr;
}
#endif

//   ./test 192.168.234.135  2000 1
// 0: rbtree; 1: array; 2: hash; 3: skiptable
int main(int argc, char* argv[]) {

    if (argc != 4) {
        printf("arg error\n");
        return -1;
    }

    char* ip = argv[1];
    unsigned short port = atoi(argv[2]);
    int mode = atoi(argv[3]);

#if LEVEL1
    printf("LEVEL1:使用9条测试样例, 测试一次\n");
    int connfd = connect_tcpserver(ip, port);

    if (mode == 0) {
        rbtree_testcase(connfd);
    } else if (mode == 1) {
        array_testcase(connfd);
    } else if (mode == 2) {
        hash_testcase(connfd);
    } else if (mode == 3) {
        skiptable_testcase(connfd);
    }

#elif LEVEL2
    printf("LEVEL2:使用9条测试样例, 测试10000次\n");
    int connfd = connect_tcpserver(ip, port);

    if (mode == 0) {
        rbtree_testcase_pth(connfd, 0);
    } else if (mode == 1) {
        array_testcase_pth(connfd, 0);
    } else if (mode == 2) {
        hash_testcase_pth(connfd, 0);
    } else if (mode == 3) {
        skiptable_testcase_pth(connfd, 0);
    }
#elif LEVEL3w
    printf("LEVEL3w:使用30000条测试样例\n");
    int connfd = connect_tcpserver(ip, port);

    if (mode == 0) {
        rbtree_testcase_3w(connfd);
    } else if (mode == 1) {
        array_testcase_3w(connfd);
    } else if (mode == 2) {
        hash_testcase_3w(connfd);
    } else if (mode == 3) {
        skiptable_testcase_3w(connfd);
    }
#elif LEVEL3

    printf("LEVEL3:每个线程使用9条测试样例, 测试10000次, 共计10个线程\n");
    std::vector<test_context_t> ctx(THREAD_NUM);
    std::vector<pthread_t> ptid(THREAD_NUM);

    for (int i = 0; i < THREAD_NUM; ++i) {
        ctx[i].thread_id = i;
        ctx[i].ip = ip;
        ctx[i].port = port;
        ctx[i].mode = mode;

        pthread_create(&ptid[i], NULL, test_qps_entry, &ctx[i]);
    }
    for (int i = 0; i < THREAD_NUM; ++i) {
        pthread_join(ptid[i], NULL);
    }

#endif
    return 0;
}
