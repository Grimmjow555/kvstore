

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

#define PRINT_PASS 1
#define SAVE 0 // 测试保存功能

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

    // SAVE (should return OK)
    const char* args_save[] = {};
    req = build_resp_request("SAVE ALL", 0, args_save);
    testcase_raw(connfd, req, "+OK\r\n", "SAVE_ALL", 0);
    free(req);

#else

    // // LOAD (should return OK)
    // const char* args_load[] = {};
    // req = build_resp_request("LOAD", 0, args_load);
    // testcase_raw(connfd, req, "+OK\r\n", "LOAD_ARRAY", 0);
    // free(req);

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
#endif
}

void rbtree_testcase(int connfd) {
    char* req = NULL;

#if SAVE
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

    // // RSAVE (should return OK)
    // const char* args_save[] = {};
    // req = build_resp_request("RSAVE", 0, args_save);
    // testcase_raw(connfd, req, "+OK\r\n", "SAVE_RBTREE", 0);
    // free(req);

#else

    // // RLOAD (should return OK)
    // const char* args_load[] = {};
    // req = build_resp_request("RLOAD", 0, args_load);
    // testcase_raw(connfd, req, "+OK\r\n", "LOAD_RBTREE", 0);
    // free(req);

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
#endif
}

void hash_testcase(int connfd) {
    char* req = NULL;

#if SAVE
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

    // // HSAVE (should return OK)
    // const char* args_save[] = {};
    // req = build_resp_request("HSAVE", 0, args_save);
    // testcase_raw(connfd, req, "+OK\r\n", "SAVE_HASH", 0);
    // free(req);

#else

    // // HLOAD (should return OK)
    // const char* args_load[] = {};
    // req = build_resp_request("HLOAD", 0, args_load);
    // testcase_raw(connfd, req, "+OK\r\n", "LOAD_HASH", 0);
    // free(req);

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
#endif
}

void skiptable_testcase(int connfd) {
    char* req = NULL;

    // const char* args1[] = {"SSET","Teacher", "King","SEXIST","Teacher"};
#if SAVE
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

    // // SSAVE (should return OK)
    // const char* args_save[] = {};
    // req = build_resp_request("SSAVE", 0, args_save);
    // testcase_raw(connfd, req, "+OK\r\n", "SAVE_SKIPTABLE", 0);
    // free(req);

#else

    // // SLOAD (should return OK)
    // const char* args_load[] = {};
    // req = build_resp_request("SLOAD", 0, args_load);
    // testcase_raw(connfd, req, "+OK\r\n", "LOAD_SKIPTABLE", 0);
    // free(req);

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
#endif
}

//   ./test 192.168.234.135  2000 1
//   ./test 39.97.42.225 9999 1
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

#endif
    return 0;
}
