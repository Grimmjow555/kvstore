

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

// 将空格分隔的命令字符串（如 "SSET Teacher King"）转换为 RESP 格式
// 返回静态缓冲区指针（调用后立即使用，因为会被后续调用覆盖）

typedef struct {
    const char* cmd;     // 命令名，如 "SSET"
    int argc;            // 参数个数（不包括命令名）
    const char* argv[4]; // 参数数组，最多支持4个参数（根据实际调整）
} Command;

// 构建单个 RESP 请求（返回动态分配的字符串，调用者 free）
char* build_resp_single(const char* cmd, int argc, const char* argv[]) {
    int total_len = 0;
    total_len += snprintf(NULL, 0, "*%d\r\n", argc + 1);
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

// 构建批量 RESP 请求（多条命令拼接）
char* build_resp_bulk(const Command* cmds, int num_cmds) {
    int total_len = 0;
    for (int i = 0; i < num_cmds; i++) {
        total_len += snprintf(NULL, 0, "*%d\r\n", cmds[i].argc + 1);
        total_len += snprintf(NULL, 0, "$%zu\r\n%s\r\n", strlen(cmds[i].cmd), cmds[i].cmd);
        for (int j = 0; j < cmds[i].argc; j++) {
            total_len +=
                snprintf(NULL, 0, "$%zu\r\n%s\r\n", strlen(cmds[i].argv[j]), cmds[i].argv[j]);
        }
    }
    char* buf = (char*)malloc(total_len + 1);
    if (!buf)
        return NULL;
    char* p = buf;
    for (int i = 0; i < num_cmds; i++) {
        p += sprintf(p, "*%d\r\n", cmds[i].argc + 1);
        p += sprintf(p, "$%zu\r\n%s\r\n", strlen(cmds[i].cmd), cmds[i].cmd);
        for (int j = 0; j < cmds[i].argc; j++) {
            p += sprintf(p, "$%zu\r\n%s\r\n", strlen(cmds[i].argv[j]), cmds[i].argv[j]);
        }
    }
    *p = '\0';
    return buf;
}

#define MAX_MSG_LENGTH 1024
#define TIME_SUB_MS(tv1, tv2)                                                                      \
    ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

#define PRINT_PASS 1

// 发送批量请求，接收响应，并逐一比对预期
void testcase_bulk(int connfd, const Command* cmds, int num_cmds, const char* expected[],
                   int expected_count, int thread_id) {
    if (!cmds || num_cmds == 0 || !expected || expected_count != num_cmds) {
        printf("Invalid bulk test parameters\n");
        return;
    }

    // 构建批量 RESP 请求
    char* bulk_req = build_resp_bulk(cmds, num_cmds);
    if (!bulk_req) {
        printf("Failed to build bulk request\n");
        return;
    }

    // 发送
    send_msg(connfd, bulk_req, strlen(bulk_req));
    free(bulk_req);

    // 逐条接收响应并比对
    for (int i = 0; i < num_cmds; i++) {
        char result[MAX_MSG_LENGTH] = {0};
        recv_msg(connfd, result, MAX_MSG_LENGTH);
        printf("%s\n", result);
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
    // 定义多条命令
    Command cmds[] = {{"SET", 2, {"Teacher 1", "King 1\n1"}}, // value 含空格和换行
                      {"EXIST", 1, {"Teacher 1"}},
                      {"GET", 1, {"Teacher 1"}},
                      {"DEL", 1, {"Teacher 1"}},
                      {"GET", 1, {"Teacher 1"}}};
    // 预期响应（按顺序）
    const char* expected[] = {
        "+OK\r\n",             // SET 成功
        "$5\r\nEXIST\r\n",     // EXIST 存在（假设你用 :1 表示存在）
        "$7\r\nKing 1\n1\r\n", // GET 返回值（注意长度和内容）
        "+OK\r\n",             // DEL 成功
        "$8\r\nNO EXIST\r\n"   // GET 不存在的键，返回 NO EXIST
    };
    int num = sizeof(cmds) / sizeof(Command);
    testcase_bulk(connfd, cmds, num, expected, num, 0);
}
void rbtree_testcase(int connfd) {
    // 定义多条命令
    Command cmds[] = {{"RSET", 2, {"Teacher 1", "King 1\n1"}}, // value 含空格和换行
                      {"REXIST", 1, {"Teacher 1"}},
                      {"RGET", 1, {"Teacher 1"}},
                      {"RDEL", 1, {"Teacher 1"}},
                      {"RGET", 1, {"Teacher 1"}}};
    // 预期响应（按顺序）
    const char* expected[] = {
        "+OK\r\n",             // RSET 成功
        "$5\r\nEXIST\r\n",     // EXIST 存在（假设你用 :1 表示存在）
        "$7\r\nKing 1\n1\r\n", // GET 返回值（注意长度和内容）
        "+OK\r\n",             // DEL 成功
        "$8\r\nNO EXIST\r\n"   // GET 不存在的键，返回 NO EXIST
    };
    int num = sizeof(cmds) / sizeof(Command);
    testcase_bulk(connfd, cmds, num, expected, num, 0);
}

void hash_testcase(int connfd) {
    // 定义多条命令
    Command cmds[] = {{"HSET", 2, {"Teacher 1", "King 1\n1"}}, // value 含空格和换行
                      {"HEXIST", 1, {"Teacher 1"}},
                      {"HGET", 1, {"Teacher 1"}},
                      {"HDEL", 1, {"Teacher 1"}},
                      {"HGET", 1, {"Teacher 1"}}};
    // 预期响应（按顺序）
    const char* expected[] = {
        "+OK\r\n",             // HSET 成功
        "$5\r\nEXIST\r\n",     // EXIST 存在（假设你用 :1 表示存在）
        "$7\r\nKing 1\n1\r\n", // GET 返回值（注意长度和内容）
        "+OK\r\n",             // DEL 成功
        "$8\r\nNO EXIST\r\n"   // GET 不存在的键，返回 NO EXIST
    };
    int num = sizeof(cmds) / sizeof(Command);
    testcase_bulk(connfd, cmds, num, expected, num, 0);
}

void skiptable_testcase(int connfd) {
    // 定义多条命令
    Command cmds[] = {{"SSET", 2, {"Teacher 1", "King 1\n1"}}, // value 含空格和换行
                      {"SEXIST", 1, {"Teacher 1"}},
                      {"SGET", 1, {"Teacher 1"}},
                      {"SDEL", 1, {"Teacher 1"}},
                      {"SGET", 1, {"Teacher 1"}}};
    // 预期响应（按顺序）
    const char* expected[] = {
        "+OK\r\n",             // SSET 成功
        "$5\r\nEXIST\r\n",     // EXIST 存在（假设你用 :1 表示存在）
        "$7\r\nKing 1\n1\r\n", // GET 返回值（注意长度和内容）
        "+OK\r\n",             // DEL 成功
        "$8\r\nNO EXIST\r\n"   // GET 不存在的键，返回 NO EXIST
    };
    int num = sizeof(cmds) / sizeof(Command);
    testcase_bulk(connfd, cmds, num, expected, num, 0);
}

//   ./test 192.168.234.135  2000 1
// 0: rbtree; 1: array; 2: hash
int main(int argc, char* argv[]) {

    if (argc != 4) {
        printf("arg error\n");
        return -1;
    }

    char* ip = argv[1];
    unsigned short port = atoi(argv[2]);
    int mode = atoi(argv[3]);

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

    return 0;
}
