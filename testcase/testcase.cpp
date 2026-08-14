

#include <arpa/inet.h>
#include <iostream>
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

#define LEVEL1 0  //使用最基础的9条测试样例，测试一次
#define LEVEL2 0  //使用最基础的9条测试样例，测试10000次
#define LEVEL3w 1 //使用30000条测试样例

#define LEVEL3 0 //使用多线程，每个线程9条测试样例，测试10000次
//若只有一套样例，考虑到线程安全，需要对这一套样例加锁，实际上和串行无异。
//所以我们多套样例应该互不相同，互不影响结果。这样可以不加锁使用多线程，具体做法是为每一套样例的key添加对应的fd

// 循环发送直到全部数据发出或失败
int send_all(int fd, const void* buffer, size_t length) {
    const char* ptr = (const char*)buffer;
    size_t sent = 0;
    while (sent < length) {
        ssize_t n = send(fd, ptr + sent, length - sent, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR)
                continue; // 被信号中断，重试
            return -1;    // 发送失败
        }
        sent += n;
    }
    return 0;
}

int send_msg(int connfd, const char* msg, int length) {
    if (msg == NULL || length < 0)
        return -1;

    // 将长度转换为网络字节序
    uint32_t net_len = htonl((uint32_t)length);
    if (send_all(connfd, &net_len, sizeof(net_len)) != 0) {
        return -1; // 头部发送失败
    }
    if (send_all(connfd, msg, length) != 0) {
        return -1; // 数据发送失败
    }
    return 0; // 成功
}

// 辅助函数：确保完整接收指定长度的数据
// 返回值：0 表示成功，-1 表示出错，0 也可能表示对端关闭（需通过返回值判断）
int recv_all(int fd, void* buffer, size_t length) {
    char* ptr = (char*)buffer;
    size_t received = 0;
    while (received < length) {
        ssize_t n = recv(fd, ptr + received, length - received, 0);
        if (n < 0) {
            if (errno == EINTR)
                continue; // 被信号中断，重试
            return -1;    // 真正的错误
        } else if (n == 0) {
            // 对端关闭连接，且尚未读满数据
            return -1; // 可以根据需要返回特殊值
        }
        received += n;
    }
    return 0;
}

// 接收一条完整消息
// 参数：
//   connfd - 连接套接字
//   msg    - 接收缓冲区（由调用者提供）
//   length - 缓冲区大小（字节数）
// 返回值：
//   >0     - 成功，返回实际接收到的消息体长度
//   -1     - 出错或对端关闭
//   -2     - 缓冲区不足（消息体长度超过 length）
int recv_msg(int connfd, char* msg, int length) {
    if (msg == NULL || length < 0)
        return -1;

    // 1. 先接收 4 字节长度前缀
    uint32_t net_len;
    if (recv_all(connfd, &net_len, sizeof(net_len)) != 0) {
        return -1; // 接收头部失败或连接关闭
    }
    uint32_t data_len = ntohl(net_len); // 转换为本地字节序

    // 2. 检查缓冲区是否足够
    if (data_len > (uint32_t)length) {
        // 缓冲区太小，无法容纳完整消息
        // 实际项目中可以选择动态分配或丢弃剩余数据，这里简单返回 -2
        return -2;
    }

    // 3. 按长度读取消息体
    if (recv_all(connfd, msg, data_len) != 0) {
        return -1; // 接收数据失败或连接关闭
    }

    // 4. 可选：为字符串添加结束符（如果协议是文本）
    msg[data_len] = '\0'; // 注意：可能超出缓冲区1字节，需确保 length > data_len

    return (int)data_len;
}

// int send_msg(int connfd, const char* msg, int length) {

//     int res = send(connfd, msg, length, 0);
//     if (res < 0) {
//         perror("send");
//         exit(1);
//     }
//     return res;
// }

// int recv_msg(int connfd, char* msg, int length) {

//     int res = recv(connfd, msg, length, 0);
//     if (res < 0) {
//         perror("recv");
//         exit(1);
//     }
//     return res;
// }

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
    testcase(connfd, "SET Teacher King", "OK\r\n", "SET-Teacher", 0);
    testcase(connfd, "GET Teacher", "King\r\n", "GET-King-Teacher", 0);
    testcase(connfd, "MOD Teacher Darren", "OK\r\n", "MOD-D-Teacher", 0);
    testcase(connfd, "GET Teacher", "Darren\r\n", "GET-Darren-Teacher", 0);
    testcase(connfd, "EXIST Teacher", "EXIST\r\n", "EXIST-Teacher", 0);
    testcase(connfd, "DEL Teacher", "OK\r\n", "DEL-Teacher", 0);
    testcase(connfd, "GET Teacher", "NO EXIST\r\n", "GET-K-Teacher", 0);
    testcase(connfd, "MOD Teacher KING", "NO EXIST\r\n", "MOD-K-Teacher", 0);
    testcase(connfd, "EXIST Teacher", "NO EXIST\r\n", "EXIST-Teacher", 0);
}

void array_testcase_pth(int connfd, int thread_id) {

    int count = 10000;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        // Teacher + pthread_id
        char buf[128]; // 临时缓冲区

        snprintf(buf, sizeof(buf), "SET Teacher%d King", thread_id);
        testcase(connfd, buf, "OK\r\n", "SET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "GET Teacher%d", thread_id);
        testcase(connfd, buf, "King\r\n", "GET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "MOD Teacher%d Darren", thread_id);
        testcase(connfd, buf, "OK\r\n", "MOD-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "GET Teacher%d", thread_id);
        testcase(connfd, buf, "Darren\r\n", "GET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "EXIST Teacher%d", thread_id);
        testcase(connfd, buf, "EXIST\r\n", "GET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "DEL Teacher%d", thread_id);
        testcase(connfd, buf, "OK\r\n", "DEL-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "GET Teacher%d", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "GET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "MOD Teacher%d KING", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "MOD-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "EXIST Teacher%d", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "GET-Teacher", thread_id);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("thread[%d]", thread_id);

    printf("array testcase --> time_used: %d, qps: %d\n", time_used, 9 * count * 1000 / time_used);
}

void array_testcase_3w(int connfd) {

    int count = 10000;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "SET Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "SET-Teacher", 0);
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "GET Teacher%d", i);

        char result[128] = {0};
        snprintf(result, 128, "King%d\r\n", i);

        testcase(connfd, cmd, result, "GET-King-Teacher", 0);
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "MOD Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "GET-King-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("array testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void rbtree_testcase(int connfd) {
    testcase(connfd, "RSET Teacher King", "OK\r\n", "RSET-Teacher", 0);
    testcase(connfd, "RGET Teacher", "King\r\n", "RGET-King-Teacher", 0);
    testcase(connfd, "RMOD Teacher Darren", "OK\r\n", "RMOD-D-Teacher", 0);
    testcase(connfd, "RGET Teacher", "Darren\r\n", "RGET-Darren-Teacher", 0);
    testcase(connfd, "REXIST Teacher", "EXIST\r\n", "REXIST-Teacher", 0);
    testcase(connfd, "RDEL Teacher", "OK\r\n", "RDEL-Teacher", 0);
    testcase(connfd, "RGET Teacher", "NO EXIST\r\n", "RGET-K-Teacher", 0);
    testcase(connfd, "RMOD Teacher KING", "NO EXIST\r\n", "RMOD-K-Teacher", 0);
    testcase(connfd, "REXIST Teacher", "NO EXIST\r\n", "REXIST-Teacher", 0);
}

void rbtree_testcase_pth(int connfd, int thread_id) {

    int count = 10000;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {
        // Teacher + connfd
        char buf[128]; // 临时缓冲区

        snprintf(buf, sizeof(buf), "RSET Teacher%d King", thread_id);
        testcase(connfd, buf, "OK\r\n", "RSET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "RGET Teacher%d", thread_id);
        testcase(connfd, buf, "King\r\n", "RGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "RMOD Teacher%d Darren", thread_id);
        testcase(connfd, buf, "OK\r\n", "RMOD-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "RGET Teacher%d", thread_id);
        testcase(connfd, buf, "Darren\r\n", "RGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "REXIST Teacher%d", thread_id);
        testcase(connfd, buf, "EXIST\r\n", "RGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "RDEL Teacher%d", thread_id);
        testcase(connfd, buf, "OK\r\n", "RDEL-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "RGET Teacher%d", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "RGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "RMOD Teacher%d KING", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "RMOD-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "REXIST Teacher%d", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "RGET-Teacher", thread_id);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("thread[%d]", thread_id);

    printf("rbtree testcase --> time_used: %d, qps: %d\n", time_used, 9 * count * 1000 / time_used);
}

void rbtree_testcase_3w(int connfd) {

    int count = 10000;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "RSET Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "RSET-Teacher", 0);
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "RGET Teacher%d", i);

        char result[128] = {0};
        snprintf(result, 128, "King%d\r\n", i);

        testcase(connfd, cmd, result, "RGET-King-Teacher", 0);
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "RMOD Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "RGET-King-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("rbtree testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void hash_testcase(int connfd) {
    testcase(connfd, "HSET Teacher King", "OK\r\n", "HSET-Teacher", 0);
    testcase(connfd, "HGET Teacher", "King\r\n", "HGET-King-Teacher", 0);
    testcase(connfd, "HMOD Teacher Darren", "OK\r\n", "HMOD-D-Teacher", 0);
    testcase(connfd, "HGET Teacher", "Darren\r\n", "HGET-Darren-Teacher", 0);
    testcase(connfd, "HEXIST Teacher", "EXIST\r\n", "HEXIST-Teacher", 0);
    testcase(connfd, "HDEL Teacher", "OK\r\n", "HDEL-Teacher", 0);
    testcase(connfd, "HGET Teacher", "NO EXIST\r\n", "HGET-K-Teacher", 0);
    testcase(connfd, "HMOD Teacher KING", "NO EXIST\r\n", "HMOD-K-Teacher", 0);
    testcase(connfd, "HEXIST Teacher", "NO EXIST\r\n", "HEXIST-Teacher", 0);
}

void hash_testcase_pth(int connfd, int thread_id) {

    int count = 10000;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {

        // Teacher + connfd
        char buf[128]; // 临时缓冲区

        snprintf(buf, sizeof(buf), "HSET Teacher%d King", thread_id);
        testcase(connfd, buf, "OK\r\n", "HSET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "HGET Teacher%d", thread_id);
        testcase(connfd, buf, "King\r\n", "HGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "HMOD Teacher%d Darren", thread_id);
        testcase(connfd, buf, "OK\r\n", "HMOD-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "HGET Teacher%d", thread_id);
        testcase(connfd, buf, "Darren\r\n", "HGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "HEXIST Teacher%d", thread_id);
        testcase(connfd, buf, "EXIST\r\n", "HGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "HDEL Teacher%d", thread_id);
        testcase(connfd, buf, "OK\r\n", "HDEL-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "HGET Teacher%d", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "HGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "HMOD Teacher%d KING", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "HMOD-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "HEXIST Teacher%d", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "HGET-Teacher", thread_id);
    }
    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("thread[%d]", thread_id);

    printf("hash testcase --> time_used: %d, qps: %d\n", time_used, 9 * count * 1000 / time_used);
}

void hash_testcase_3w(int connfd) {

    int count = 10000;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "HSET Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "HSET-Teacher", 0);
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "HGET Teacher%d", i);

        char result[128] = {0};
        snprintf(result, 128, "King%d\r\n", i);

        testcase(connfd, cmd, result, "HGET-King-Teacher", 0);
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "HMOD Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "HGET-King-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("hash testcase --> time_used: %d, qps: %d\n", time_used, 3 * count * 1000 / time_used);
}

void skiptable_testcase(int connfd) {
    testcase(connfd, "SSET Teacher King", "OK\r\n", "SSET-Teacher", 0);
    testcase(connfd, "SGET Teacher", "King\r\n", "SGET-King-Teacher", 0);
    testcase(connfd, "SMOD Teacher Darren", "OK\r\n", "SMOD-D-Teacher", 0);
    testcase(connfd, "SGET Teacher", "Darren\r\n", "SGET-Darren-Teacher", 0);
    testcase(connfd, "SEXIST Teacher", "EXIST\r\n", "SEXIST-Teacher", 0);
    testcase(connfd, "SDEL Teacher", "OK\r\n", "SDEL-Teacher", 0);
    testcase(connfd, "SGET Teacher", "NO EXIST\r\n", "SGET-K-Teacher", 0);
    testcase(connfd, "SMOD Teacher KING", "NO EXIST\r\n", "SMOD-K-Teacher", 0);
    testcase(connfd, "SEXIST Teacher", "NO EXIST\r\n", "SEXIST-Teacher", 0);
}

void skiptable_testcase_pth(int connfd, int thread_id) {

    int count = 10000;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (int i = 0; i < count; i++) {

        // Teacher + connfd
        char buf[128]; // 临时缓冲区

        snprintf(buf, sizeof(buf), "SSET Teacher%d King", thread_id);
        testcase(connfd, buf, "OK\r\n", "SSET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "SGET Teacher%d", thread_id);
        testcase(connfd, buf, "King\r\n", "SGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "SMOD Teacher%d Darren", thread_id);
        testcase(connfd, buf, "OK\r\n", "SMOD-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "SGET Teacher%d", thread_id);
        testcase(connfd, buf, "Darren\r\n", "SGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "SEXIST Teacher%d", thread_id);
        testcase(connfd, buf, "EXIST\r\n", "SGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "SDEL Teacher%d", thread_id);
        testcase(connfd, buf, "OK\r\n", "SDEL-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "SGET Teacher%d", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "SGET-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "SMOD Teacher%d KING", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "SMOD-Teacher", thread_id);

        snprintf(buf, sizeof(buf), "SEXIST Teacher%d", thread_id);
        testcase(connfd, buf, "NO EXIST\r\n", "SGET-Teacher", thread_id);
    }
    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("thread[%d]", thread_id);

    printf("skiptable testcase --> time_used: %d, qps: %d\n", time_used,
           9 * count * 1000 / time_used);
}

void skiptable_testcase_3w(int connfd) {

    int count = 10000;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "SSET Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "SSET-Teacher", 0);
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "SGET Teacher%d", i);

        char result[128] = {0};
        snprintf(result, 128, "King%d\r\n", i);

        testcase(connfd, cmd, result, "SGET-King-Teacher", 0);
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "SMOD Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "SGET-King-Teacher", 0);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("skiptable testcase --> time_used: %d, qps: %d\n", time_used,
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
// 0: rbtree; 1: array; 2: hash
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
