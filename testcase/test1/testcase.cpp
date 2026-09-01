

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
#define TIME_SUB_MS(tv1, tv2)                                                                      \
    ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

#define PRINT_PASS 0
#define TEST_COUNT 10

#define LEVEL1 0 //使用最基础的9条测试样例，测试一次
#define LEVEL2 0 //使用最基础的9条测试样例，测试 TEST_COUNT 次

#define LEVEL3w 0 //使用30000条测试样例

#define LEVEL3 1 //使用多线程，每个线程9条测试样例，测试 TEST_COUNT 次
//若只有一套样例，考虑到线程安全，需要对这一套样例加锁，实际上和串行无异。
//所以我们多套样例应该互不相同，互不影响结果。这样可以不加锁使用多线程，具体做法是为每一套样例的key添加对应的fd

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
 * @return 动态分配的 RESP 格式字符串，调用者必须使用 free() 释放；
 *         若内存分配失败，返回 NULL。
 *
 * @note 该函数支持参数中包含任意字节（包括空格、换行等），因为长度前缀
 *       保证了数据的边界，这是 RESP 协议的二进制安全特性。
 */
char* build_resp_request(const char* cmd, int argc, const char* argv[]) {
    // 计算总长度：*<argc>\r\n + 每个参数的 $<len>\r\n<data>\r\n
    int total_len = 0;
    total_len += snprintf(NULL, 0, "*%d\r\n", argc + 1); // +1 for command
    total_len += snprintf(NULL, 0, "$%zu\r\n%s\r\n", strlen(cmd), cmd);
    for (int i = 0; i < argc; i++) {
        total_len += snprintf(NULL, 0, "$%zu\r\n%s\r\n", strlen(argv[i]), argv[i]);
    }
    char* buf = (char*)malloc(total_len + 1);
    if (!buf)
        return NULL;
    char* p = buf;
    p += sprintf(p, "*%d\r\n", argc + 1);
    p += sprintf(p, "$%zu\r\n%s\r\n", strlen(cmd), cmd);
    for (int i = 0; i < argc; i++) {
        p += sprintf(p, "$%zu\r\n%s\r\n", strlen(argv[i]), argv[i]);
    }
    *p = '\0';
    return buf;
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
    char* req = NULL;

    // SET Teacher King
    const char* args1[] = {"Teacher", "King"};
    req = build_resp_request("SET", 2, args1);
    testcase_raw(connfd, req, "+OK\r\n", "SET-Teacher", 0);
    free(req);

    // GET Teacher
    const char* args2[] = {"Teacher"};
    req = build_resp_request("GET", 1, args2);
    testcase_raw(connfd, req, "$4\r\nKing\r\n", "GET-King-Teacher", 0);
    free(req);

    // MOD Teacher Darren
    const char* args3[] = {"Teacher", "Darren"};
    req = build_resp_request("MOD", 2, args3);
    testcase_raw(connfd, req, "+OK\r\n", "MOD-D-Teacher", 0);
    free(req);

    // GET Teacher (should return Darren)
    const char* args4[] = {"Teacher"};
    req = build_resp_request("GET", 1, args4);
    testcase_raw(connfd, req, "$6\r\nDarren\r\n", "GET-Darren-Teacher", 0);
    free(req);

    // EXIST Teacher
    const char* args5[] = {"Teacher"};
    req = build_resp_request("EXIST", 1, args5);
    testcase_raw(connfd, req, "$5\r\nEXIST\r\n", "EXIST-Teacher", 0); // RESP 整数存在为 1
    free(req);

    // DEL Teacher
    const char* args6[] = {"Teacher"};
    req = build_resp_request("DEL", 1, args6);
    testcase_raw(connfd, req, "+OK\r\n", "DEL-Teacher", 0);
    free(req);

    // 再 GET 应返回 NO EXIST（根据你希望的格式）
    const char* args7[] = {"Teacher"};
    req = build_resp_request("GET", 1, args7);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "GET-K-Teacher",
                 0); // 假设你返回 $8\r\nNO EXIST\r\n
    free(req);

    // MOD 不存在的键
    const char* args8[] = {"Teacher", "KING"};
    req = build_resp_request("MOD", 2, args8);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "MOD-K-Teacher", 0);
    free(req);

    // EXIST 不存在的键
    const char* args9[] = {"Teacher"};
    req = build_resp_request("EXIST", 1, args9);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "EXIST-Teacher", 0);
    free(req);
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
        char* req1 = build_resp_request("SET", 2, args1);
        testcase_raw(connfd, req1, "+OK\r\n", "SET-Teacher", thread_id);
        free(req1);

        // GET key
        const char* args2[] = {key};
        char* req2 = build_resp_request("GET", 1, args2);
        testcase_raw(connfd, req2, "$4\r\nKing\r\n", "GET-Teacher", thread_id);
        free(req2);

        // MOD key Darren
        const char* args3[] = {key, "Darren"};
        char* req3 = build_resp_request("MOD", 2, args3);
        testcase_raw(connfd, req3, "+OK\r\n", "MOD-Teacher", thread_id);
        free(req3);

        // GET key (should return Darren)
        const char* args4[] = {key};
        char* req4 = build_resp_request("GET", 1, args4);
        testcase_raw(connfd, req4, "$6\r\nDarren\r\n", "GET-Teacher", thread_id);
        free(req4);

        // EXIST key
        const char* args5[] = {key};
        char* req5 = build_resp_request("EXIST", 1, args5);
        testcase_raw(connfd, req5, "$5\r\nEXIST\r\n", "EXIST-Teacher", thread_id);
        free(req5);

        // DEL key
        const char* args6[] = {key};
        char* req6 = build_resp_request("DEL", 1, args6);
        testcase_raw(connfd, req6, "+OK\r\n", "DEL-Teacher", thread_id);
        free(req6);

        // GET key (should return NO EXIST)
        const char* args7[] = {key};
        char* req7 = build_resp_request("GET", 1, args7);
        testcase_raw(connfd, req7, "$8\r\nNO EXIST\r\n", "GET-Teacher", thread_id);
        free(req7);

        // MOD key KING (key not exist)
        const char* args8[] = {key, "KING"};
        char* req8 = build_resp_request("MOD", 2, args8);
        testcase_raw(connfd, req8, "$8\r\nNO EXIST\r\n", "MOD-Teacher", thread_id);
        free(req8);

        // EXIST key (key not exist)
        const char* args9[] = {key};
        char* req9 = build_resp_request("EXIST", 1, args9);
        testcase_raw(connfd, req9, "$8\r\nNO EXIST\r\n", "EXIST-Teacher", thread_id);
        free(req9);
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
        char* req = build_resp_request("SET", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "SET-Teacher", 0);
        free(req);
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
        char* req = build_resp_request("GET", 1, args);
        testcase_raw(connfd, req, expected, "GET-King-Teacher", 0);
        free(req);
    }

    // 第三阶段：MOD Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char* req = build_resp_request("MOD", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "MOD-King-Teacher", 0);
        free(req);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("ARRAY testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void rbtree_testcase(int connfd) {
    char* req = NULL;

    // RSET Teacher King
    const char* args1[] = {"Teacher", "King"};
    req = build_resp_request("RSET", 2, args1);
    testcase_raw(connfd, req, "+OK\r\n", "RSET-Teacher", 0);
    free(req);

    // RGET Teacher
    const char* args2[] = {"Teacher"};
    req = build_resp_request("RGET", 1, args2);
    testcase_raw(connfd, req, "$4\r\nKing\r\n", "RGET-King-Teacher", 0);
    free(req);

    // RMOD Teacher Darren
    const char* args3[] = {"Teacher", "Darren"};
    req = build_resp_request("RMOD", 2, args3);
    testcase_raw(connfd, req, "+OK\r\n", "RMOD-D-Teacher", 0);
    free(req);

    // RGET Teacher (should return Darren)
    const char* args4[] = {"Teacher"};
    req = build_resp_request("RGET", 1, args4);
    testcase_raw(connfd, req, "$6\r\nDarren\r\n", "RGET-Darren-Teacher", 0);
    free(req);

    // REXIST Teacher
    const char* args5[] = {"Teacher"};
    req = build_resp_request("REXIST", 1, args5);
    testcase_raw(connfd, req, "$5\r\nEXIST\r\n", "REXIST-Teacher", 0); // RESP 整数存在为 1
    free(req);

    // RDEL Teacher
    const char* args6[] = {"Teacher"};
    req = build_resp_request("RDEL", 1, args6);
    testcase_raw(connfd, req, "+OK\r\n", "RDEL-Teacher", 0);
    free(req);

    // 再 GET 应返回 NO EXIST（根据你希望的格式）
    const char* args7[] = {"Teacher"};
    req = build_resp_request("RGET", 1, args7);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "RGET-K-Teacher",
                 0); // 假设你返回 $8\r\nNO EXIST\r\n
    free(req);

    // RMOD 不存在的键
    const char* args8[] = {"Teacher", "KING"};
    req = build_resp_request("RMOD", 2, args8);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "RMOD-K-Teacher", 0);
    free(req);

    // REXIST 不存在的键
    const char* args9[] = {"Teacher"};
    req = build_resp_request("REXIST", 1, args9);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "REXIST-Teacher", 0);
    free(req);
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
        char* req1 = build_resp_request("RSET", 2, args1);
        testcase_raw(connfd, req1, "+OK\r\n", "RSET-Teacher", thread_id);
        free(req1);

        // RGET key
        const char* args2[] = {key};
        char* req2 = build_resp_request("RGET", 1, args2);
        testcase_raw(connfd, req2, "$4\r\nKing\r\n", "RGET-Teacher", thread_id);
        free(req2);

        // RMOD key Darren
        const char* args3[] = {key, "Darren"};
        char* req3 = build_resp_request("RMOD", 2, args3);
        testcase_raw(connfd, req3, "+OK\r\n", "RMOD-Teacher", thread_id);
        free(req3);

        // RGET key (should return Darren)
        const char* args4[] = {key};
        char* req4 = build_resp_request("RGET", 1, args4);
        testcase_raw(connfd, req4, "$6\r\nDarren\r\n", "RGET-Teacher", thread_id);
        free(req4);

        // REXIST key
        const char* args5[] = {key};
        char* req5 = build_resp_request("REXIST", 1, args5);
        testcase_raw(connfd, req5, "$5\r\nEXIST\r\n", "REXIST-Teacher", thread_id);
        free(req5);

        // RDEL key
        const char* args6[] = {key};
        char* req6 = build_resp_request("RDEL", 1, args6);
        testcase_raw(connfd, req6, "+OK\r\n", "RDEL-Teacher", thread_id);
        free(req6);

        // RGET key (should return NO EXIST)
        const char* args7[] = {key};
        char* req7 = build_resp_request("RGET", 1, args7);
        testcase_raw(connfd, req7, "$8\r\nNO EXIST\r\n", "RGET-Teacher", thread_id);
        free(req7);

        // RMOD key KING (key not exist)
        const char* args8[] = {key, "KING"};
        char* req8 = build_resp_request("RMOD", 2, args8);
        testcase_raw(connfd, req8, "$8\r\nNO EXIST\r\n", "RMOD-Teacher", thread_id);
        free(req8);

        // REXIST key (key not exist)
        const char* args9[] = {key};
        char* req9 = build_resp_request("REXIST", 1, args9);
        testcase_raw(connfd, req9, "$8\r\nNO EXIST\r\n", "REXIST-Teacher", thread_id);
        free(req9);
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
        char* req = build_resp_request("RSET", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "RSET-Teacher", 0);
        free(req);
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
        char* req = build_resp_request("RGET", 1, args);
        testcase_raw(connfd, req, expected, "RGET-King-Teacher", 0);
        free(req);
    }

    // 第三阶段：RMOD Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char* req = build_resp_request("RMOD", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "RMOD-King-Teacher", 0);
        free(req);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("RBTREE testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void hash_testcase(int connfd) {
    char* req = NULL;

    // HSET Teacher King
    const char* args1[] = {"Teacher", "King"};
    req = build_resp_request("HSET", 2, args1);
    testcase_raw(connfd, req, "+OK\r\n", "HSET-Teacher", 0);
    free(req);

    // HGET Teacher
    const char* args2[] = {"Teacher"};
    req = build_resp_request("HGET", 1, args2);
    testcase_raw(connfd, req, "$4\r\nKing\r\n", "HGET-King-Teacher", 0);
    free(req);

    // HMOD Teacher Darren
    const char* args3[] = {"Teacher", "Darren"};
    req = build_resp_request("HMOD", 2, args3);
    testcase_raw(connfd, req, "+OK\r\n", "HMOD-D-Teacher", 0);
    free(req);

    // HGET Teacher (should return Darren)
    const char* args4[] = {"Teacher"};
    req = build_resp_request("HGET", 1, args4);
    testcase_raw(connfd, req, "$6\r\nDarren\r\n", "HGET-Darren-Teacher", 0);
    free(req);

    // HEXIST Teacher
    const char* args5[] = {"Teacher"};
    req = build_resp_request("HEXIST", 1, args5);
    testcase_raw(connfd, req, "$5\r\nEXIST\r\n", "HEXIST-Teacher", 0); // RESP 整数存在为 1
    free(req);

    // HDEL Teacher
    const char* args6[] = {"Teacher"};
    req = build_resp_request("HDEL", 1, args6);
    testcase_raw(connfd, req, "+OK\r\n", "HDEL-Teacher", 0);
    free(req);

    // 再 GET 应返回 NO EXIST（根据你希望的格式）
    const char* args7[] = {"Teacher"};
    req = build_resp_request("HGET", 1, args7);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "HGET-K-Teacher",
                 0); // 假设你返回 $8\r\nNO EXIST\r\n
    free(req);

    // HMOD 不存在的键
    const char* args8[] = {"Teacher", "KING"};
    req = build_resp_request("HMOD", 2, args8);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "HMOD-K-Teacher", 0);
    free(req);

    // HEXIST 不存在的键
    const char* args9[] = {"Teacher"};
    req = build_resp_request("HEXIST", 1, args9);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "HEXIST-Teacher", 0);
    free(req);
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
        char* req1 = build_resp_request("HSET", 2, args1);
        testcase_raw(connfd, req1, "+OK\r\n", "HSET-Teacher", thread_id);
        free(req1);

        // HGET key
        const char* args2[] = {key};
        char* req2 = build_resp_request("HGET", 1, args2);
        testcase_raw(connfd, req2, "$4\r\nKing\r\n", "HGET-Teacher", thread_id);
        free(req2);

        // HMOD key Darren
        const char* args3[] = {key, "Darren"};
        char* req3 = build_resp_request("HMOD", 2, args3);
        testcase_raw(connfd, req3, "+OK\r\n", "HMOD-Teacher", thread_id);
        free(req3);

        // HGET key (should return Darren)
        const char* args4[] = {key};
        char* req4 = build_resp_request("HGET", 1, args4);
        testcase_raw(connfd, req4, "$6\r\nDarren\r\n", "HGET-Teacher", thread_id);
        free(req4);

        // HEXIST key
        const char* args5[] = {key};
        char* req5 = build_resp_request("HEXIST", 1, args5);
        testcase_raw(connfd, req5, "$5\r\nEXIST\r\n", "HEXIST-Teacher", thread_id);
        free(req5);

        // HDEL key
        const char* args6[] = {key};
        char* req6 = build_resp_request("HDEL", 1, args6);
        testcase_raw(connfd, req6, "+OK\r\n", "HDEL-Teacher", thread_id);
        free(req6);

        // HGET key (should return NO EXIST)
        const char* args7[] = {key};
        char* req7 = build_resp_request("HGET", 1, args7);
        testcase_raw(connfd, req7, "$8\r\nNO EXIST\r\n", "HGET-Teacher", thread_id);
        free(req7);

        // HMOD key KING (key not exist)
        const char* args8[] = {key, "KING"};
        char* req8 = build_resp_request("HMOD", 2, args8);
        testcase_raw(connfd, req8, "$8\r\nNO EXIST\r\n", "HMOD-Teacher", thread_id);
        free(req8);

        // HEXIST key (key not exist)
        const char* args9[] = {key};
        char* req9 = build_resp_request("HEXIST", 1, args9);
        testcase_raw(connfd, req9, "$8\r\nNO EXIST\r\n", "HEXIST-Teacher", thread_id);
        free(req9);
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
        char* req = build_resp_request("HSET", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "HSET-Teacher", 0);
        free(req);
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
        char* req = build_resp_request("HGET", 1, args);
        testcase_raw(connfd, req, expected, "HGET-King-Teacher", 0);
        free(req);
    }

    // 第三阶段：HMOD Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char* req = build_resp_request("HMOD", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "HMOD-King-Teacher", 0);
        free(req);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("HASH testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void skiptable_testcase(int connfd) {
    char* req = NULL;

    // const char* args1[] = {"SSET","Teacher", "King","SEXIST","Teacher"};

    // SSET Teacher King
    const char* args1[] = {"Teacher", "King"};
    req = build_resp_request("SSET", 2, args1);
    testcase_raw(connfd, req, "+OK\r\n", "SSET-Teacher", 0);
    free(req);

    // SGET Teacher
    const char* args2[] = {"Teacher"};
    req = build_resp_request("SGET", 1, args2);
    testcase_raw(connfd, req, "$4\r\nKing\r\n", "SGET-King-Teacher", 0);
    free(req);

    // SMOD Teacher Darren
    const char* args3[] = {"Teacher", "Darren"};
    req = build_resp_request("SMOD", 2, args3);
    testcase_raw(connfd, req, "+OK\r\n", "SMOD-D-Teacher", 0);
    free(req);

    // SGET Teacher (should return Darren)
    const char* args4[] = {"Teacher"};
    req = build_resp_request("SGET", 1, args4);
    testcase_raw(connfd, req, "$6\r\nDarren\r\n", "SGET-Darren-Teacher", 0);
    free(req);

    // SEXIST Teacher
    const char* args5[] = {"Teacher"};
    req = build_resp_request("SEXIST", 1, args5);
    testcase_raw(connfd, req, "$5\r\nEXIST\r\n", "SEXIST-Teacher", 0); // RESP 整数存在为 1
    free(req);

    // SDEL Teacher
    const char* args6[] = {"Teacher"};
    req = build_resp_request("SDEL", 1, args6);
    testcase_raw(connfd, req, "+OK\r\n", "SDEL-Teacher", 0);
    free(req);

    // 再 GET 应返回 NO EXIST（根据你希望的格式）
    const char* args7[] = {"Teacher"};
    req = build_resp_request("SGET", 1, args7);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "SGET-K-Teacher",
                 0); // 假设你返回 $8\r\nNO EXIST\r\n
    free(req);

    // SMOD 不存在的键
    const char* args8[] = {"Teacher", "KING"};
    req = build_resp_request("SMOD", 2, args8);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "SMOD-K-Teacher", 0);
    free(req);

    // SEXIST 不存在的键
    const char* args9[] = {"Teacher"};
    req = build_resp_request("SEXIST", 1, args9);
    testcase_raw(connfd, req, "$8\r\nNO EXIST\r\n", "SEXIST-Teacher", 0);
    free(req);
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
        char* req1 = build_resp_request("SSET", 2, args1);
        testcase_raw(connfd, req1, "+OK\r\n", "SSET-Teacher", thread_id);
        free(req1);

        // SGET key
        const char* args2[] = {key};
        char* req2 = build_resp_request("SGET", 1, args2);
        testcase_raw(connfd, req2, "$4\r\nKing\r\n", "SGET-Teacher", thread_id);
        free(req2);

        // SMOD key Darren
        const char* args3[] = {key, "Darren"};
        char* req3 = build_resp_request("SMOD", 2, args3);
        testcase_raw(connfd, req3, "+OK\r\n", "SMOD-Teacher", thread_id);
        free(req3);

        // SGET key (should return Darren)
        const char* args4[] = {key};
        char* req4 = build_resp_request("SGET", 1, args4);
        testcase_raw(connfd, req4, "$6\r\nDarren\r\n", "SGET-Teacher", thread_id);
        free(req4);

        // SEXIST key
        const char* args5[] = {key};
        char* req5 = build_resp_request("SEXIST", 1, args5);
        testcase_raw(connfd, req5, "$5\r\nEXIST\r\n", "SEXIST-Teacher", thread_id);
        free(req5);

        // SDEL key
        const char* args6[] = {key};
        char* req6 = build_resp_request("SDEL", 1, args6);
        testcase_raw(connfd, req6, "+OK\r\n", "SDEL-Teacher", thread_id);
        free(req6);

        // SGET key (should return NO EXIST)
        const char* args7[] = {key};
        char* req7 = build_resp_request("SGET", 1, args7);
        testcase_raw(connfd, req7, "$8\r\nNO EXIST\r\n", "SGET-K-Teacher", thread_id);
        free(req7);

        // SMOD key KING (key not exist)
        const char* args8[] = {key, "KING"};
        char* req8 = build_resp_request("SMOD", 2, args8);
        testcase_raw(connfd, req8, "$8\r\nNO EXIST\r\n", "SMOD-K-Teacher", thread_id);
        free(req8);

        // SEXIST key (key not exist)
        const char* args9[] = {key};
        char* req9 = build_resp_request("SEXIST", 1, args9);
        testcase_raw(connfd, req9, "$8\r\nNO EXIST\r\n", "SEXIST-Teacher", thread_id);
        free(req9);
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
        char* req = build_resp_request("SSET", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "SSET-Teacher", 0);
        free(req);
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
        char* req = build_resp_request("SGET", 1, args);
        testcase_raw(connfd, req, expected, "SGET-King-Teacher", 0);
        free(req);
    }

    // 第三阶段：SMOD Teacher{i} King{i}
    for (i = 0; i < count; i++) {
        char key[64], value[64];
        snprintf(key, sizeof(key), "Teacher%d", i);
        snprintf(value, sizeof(value), "King%d", i);

        const char* args[] = {key, value};
        char* req = build_resp_request("SMOD", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "SMOD-King-Teacher", 0);
        free(req);
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
