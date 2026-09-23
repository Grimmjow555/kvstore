

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
#ifndef START_NUM
#define START_NUM 0
#endif
#define SET_NUMS 500
#define PRINT_PASS 0
#define INSERT 1 // 测试保存功能

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
    char req[128];

#if INSERT

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
        testcase_raw(connfd, req, "+OK\r\n", "SET-Teacher", 0);
    }

#else

    // GET 与 SAVE 使用相同数量的数据，逐条验证 RDB 恢复结果
    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i + START_NUM / 4);
        key[len] = '\0';
        const char* args[] = {key};
        if (build_resp_request(req, sizeof(req), "GET", 1, args) < 0)
            exit(1);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase_raw(connfd, req, expected, "GET-Teacher", 0);
    }

#endif
}

void rbtree_testcase(int connfd) {
    char req[128];

#if INSERT
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
        testcase_raw(connfd, req, "+OK\r\n", "RSET-Teacher", 0);
    }

#else

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i + START_NUM / 4);
        key[len] = '\0';
        const char* args[] = {key};
        if (build_resp_request(req, sizeof(req), "RGET", 1, args) < 0)
            exit(1);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase_raw(connfd, req, expected, "RGET-Teacher", 0);
    }
#endif
}

void hash_testcase(int connfd) {
    char req[128];

#if INSERT
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
        testcase_raw(connfd, req, "+OK\r\n", "HSET-Teacher", 0);
    }

#else

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i + START_NUM / 4);
        key[len] = '\0';
        const char* args[] = {key};
        if (build_resp_request(req, sizeof(req), "HGET", 1, args) < 0)
            exit(1);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase_raw(connfd, req, expected, "HGET-Teacher", 0);
    }
#endif
}

void skiptable_testcase(int connfd) {
    char req[128];

    // const char* args1[] = {"SSET","Teacher", "King","SEXIST","Teacher"};
#if INSERT
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
        testcase_raw(connfd, req, "+OK\r\n", "SSET-Teacher", 0);
    }

#else

    for (int i = 0; i < SET_NUMS / 4; i++) {
        char key[32];
        int len = sprintf(key, "Teacher%d", i + START_NUM / 4);
        key[len] = '\0';
        const char* args[] = {key};
        if (build_resp_request(req, sizeof(req), "SGET", 1, args) < 0)
            exit(1);

        char expected[64];
        snprintf(expected, sizeof(expected), "$%zu\r\nKing%d\r\n",
                 strlen("King") + (size_t)snprintf(NULL, 0, "%d", i), i);
        testcase_raw(connfd, req, expected, "SGET-Teacher", 0);
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

    int connfd = connect_tcpserver(ip, port);

#if INSERT

    rbtree_testcase(connfd);

    array_testcase(connfd);

    hash_testcase(connfd);

    skiptable_testcase(connfd);

#else

    rbtree_testcase(connfd);

    array_testcase(connfd);

    hash_testcase(connfd);

    skiptable_testcase(connfd);

#endif
    return 0;
}
