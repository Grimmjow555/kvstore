

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
#define SET_NUMS 25
#define PRINT_PASS 1
#define SAVE 1 // 测试保存功能

#define LEVEL1 1 //使用最基础的9条测试样例，测试一次

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

#if SAVE

    for (int i = 0; i < SET_NUMS; i++) {
        // SET Teacher King
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0'; // 确保字符串结束
        char value[32];
        len = sprintf(value, "King%d", i);
        value[len] = '\0'; // 确保字符串结束
        const char* args1[] = {key, value};
        req = build_resp_request("SET", 2, args1);
        testcase_raw(connfd, req, "+OK\r\n", "SET-Teacher", 0);
        free(req);
    }

#else

    // GET 与 SAVE 使用相同数量的数据，逐条验证 RDB 恢复结果
    for (int i = 0; i < SET_NUMS; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        const char* args[] = {key};
        req = build_resp_request("GET", 1, args);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase_raw(connfd, req, expected, "GET-Teacher", 0);
        free(req);
    }

#endif
}

void rbtree_testcase(int connfd) {
    char* req = NULL;

#if SAVE
    for (int i = 0; i < SET_NUMS; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        char value[32];
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        req = build_resp_request("RSET", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "RSET-Teacher", 0);
        free(req);
    }

#else

    for (int i = 0; i < SET_NUMS; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        const char* args[] = {key};
        req = build_resp_request("RGET", 1, args);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase_raw(connfd, req, expected, "RGET-Teacher", 0);
        free(req);
    }
#endif
}

void hash_testcase(int connfd) {
    char* req = NULL;

#if SAVE
    for (int i = 0; i < SET_NUMS; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        char value[32];
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        req = build_resp_request("HSET", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "HSET-Teacher", 0);
        free(req);
    }

#else

    for (int i = 0; i < SET_NUMS; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        const char* args[] = {key};
        req = build_resp_request("HGET", 1, args);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase_raw(connfd, req, expected, "HGET-Teacher", 0);
        free(req);
    }
#endif
}

void skiptable_testcase(int connfd) {
    char* req = NULL;

    // const char* args1[] = {"SSET","Teacher", "King","SEXIST","Teacher"};
#if SAVE
    for (int i = 0; i < SET_NUMS; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        char value[32];
        len = sprintf(value, "King%d", i);
        value[len] = '\0';
        const char* args[] = {key, value};
        req = build_resp_request("SSET", 2, args);
        testcase_raw(connfd, req, "+OK\r\n", "SSET-Teacher", 0);
        free(req);
    }

#else

    for (int i = 0; i < SET_NUMS; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i);
        key[len] = '\0';
        const char* args[] = {key};
        req = build_resp_request("SGET", 1, args);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase_raw(connfd, req, expected, "SGET-Teacher", 0);
        free(req);
    }
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

    printf("LEVEL1:使用9条测试样例, 测试一次\n");
    int connfd = connect_tcpserver(ip, port);

#if SAVE
    const char* args_save[] = {};
    char* req = nullptr;
    req = build_resp_request("AOF CLEAR", 0, args_save);
    testcase_raw(connfd, req, "+OK\r\n", "AOF CLEAR", 0);
    free(req);

    rbtree_testcase(connfd);

    array_testcase(connfd);

    hash_testcase(connfd);

    skiptable_testcase(connfd);

#else

    printf("AOF LOAD\n");
    const char* args_load[] = {};
    char* req = nullptr;
    req = build_resp_request("AOF LOAD", 0, args_load);
    testcase_raw(connfd, req, "+OK\r\n", "AOF LOAD", 0);
    free(req);

    rbtree_testcase(connfd);

    array_testcase(connfd);

    hash_testcase(connfd);

    skiptable_testcase(connfd);

#endif
    return 0;
}
