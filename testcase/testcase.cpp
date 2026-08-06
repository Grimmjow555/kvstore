

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

int connect_tcpserver(const char* ip, unsigned short port);
void array_testcase(int connfd);
void array_testcase_1w(int connfd);
void rbtree_testcase(int connfd);
void rbtree_testcase_1w(int connfd);
void rbtree_testcase_3w(int connfd);
void hash_testcase(int connfd);

#define ORIGINAL 0
//若为0，则使用多线程
//若只有一套样例，考虑到线程安全，需要对这一套样例加锁，实际上和串行无异。
//所以我们多套样例应该互不相同，互不影响结果。这样可以不加锁使用多线程，具体做法是为每一套样例的key添加对应的fd

#if ORIGINAL
#else

#define THREAD_NUM 10
struct test_context_t {
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

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    if (pctx->mode == 0) {
        rbtree_testcase_1w(connfd);
    } else if (pctx->mode == 1) {
        rbtree_testcase_3w(connfd);
    } else if (pctx->mode == 2) {
        array_testcase_1w(connfd);
    } else if (pctx->mode == 3) {
        hash_testcase(connfd);
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("[fd: %d]mode[%d] --> time_used: %d, qps: %d\n", connfd, pctx->mode, time_used,
           90000 * 1000 / time_used);

    return nullptr;
}
#endif

int send_msg(int connfd, const char* msg, int length) {

    int res = send(connfd, msg, length, 0);
    if (res < 0) {
        perror("send");
        exit(1);
    }
    return res;
}

int recv_msg(int connfd, char* msg, int length) {

    int res = recv(connfd, msg, length, 0);
    if (res < 0) {
        perror("recv");
        exit(1);
    }
    return res;
}

void testcase(int connfd, const char* msg, const char* pattern, const char* casename) {

    if (!msg || !pattern || !casename)
        return;

    send_msg(connfd, msg, strlen(msg));

    char result[MAX_MSG_LENGTH] = {0};
    recv_msg(connfd, result, MAX_MSG_LENGTH);

    if (strcmp(result, pattern) == 0) {
        // printf("==> PASS ->  %s\n", casename);
    } else {
        printf("==> FAILED -> %s, '%s' != '%s' \n", casename, result, pattern);
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
    int interval = 0;
    // int interval = 200000; // 等待 0.2 秒（如果需要）

    char buf[128]; // 临时缓冲区

    // SET Teacher + connfd
    snprintf(buf, sizeof(buf), "SET Teacher%d King", connfd);
    testcase(connfd, buf, "OK\r\n", "SET-Teacher");
    usleep(interval);

    // GET Teacher + connfd
    snprintf(buf, sizeof(buf), "GET Teacher%d", connfd);
    testcase(connfd, buf, "King\r\n", "GET-Teacher");
    usleep(interval * 10);

    // MOD Teacher + connfd
    snprintf(buf, sizeof(buf), "MOD Teacher%d Darren", connfd);
    testcase(connfd, buf, "OK\r\n", "MOD-Teacher");
    usleep(interval);

    // GET Teacher + connfd
    snprintf(buf, sizeof(buf), "GET Teacher%d", connfd);
    testcase(connfd, buf, "Darren\r\n", "GET-Teacher");
    usleep(interval * 10);

    // EXIST Teacher + connfd
    snprintf(buf, sizeof(buf), "EXIST Teacher%d", connfd);
    testcase(connfd, buf, "EXIST\r\n", "GET-Teacher");
    usleep(interval);

    // DEL Teacher + connfd
    snprintf(buf, sizeof(buf), "DEL Teacher%d", connfd);
    testcase(connfd, buf, "OK\r\n", "DEL-Teacher");
    usleep(interval);

    // GET Teacher + connfd
    snprintf(buf, sizeof(buf), "GET Teacher%d", connfd);
    testcase(connfd, buf, "NO EXIST\r\n", "GET-Teacher");
    usleep(interval);

    // MOD Teacher + connfd
    snprintf(buf, sizeof(buf), "MOD Teacher%d KING", connfd);
    testcase(connfd, buf, "NO EXIST\r\n", "MOD-Teacher");
    usleep(interval);

    // EXIST Teacher + connfd
    snprintf(buf, sizeof(buf), "EXIST Teacher%d", connfd);
    testcase(connfd, buf, "NO EXIST\r\n", "GET-Teacher");
}

void array_testcase_1w(int connfd) {

    int count = 10000;
    int i = 0;

    for (i = 0; i < count; i++) {
        // Teacher + connfd
        char buf[128]; // 临时缓冲区

        snprintf(buf, sizeof(buf), "SET Teacher%d King", connfd);
        testcase(connfd, buf, "OK\r\n", "SET-Teacher");

        snprintf(buf, sizeof(buf), "GET Teacher%d", connfd);
        testcase(connfd, buf, "King\r\n", "GET-Teacher");

        snprintf(buf, sizeof(buf), "MOD Teacher%d Darren", connfd);
        testcase(connfd, buf, "OK\r\n", "MOD-Teacher");

        snprintf(buf, sizeof(buf), "GET Teacher%d", connfd);
        testcase(connfd, buf, "Darren\r\n", "GET-Teacher");

        snprintf(buf, sizeof(buf), "EXIST Teacher%d", connfd);
        testcase(connfd, buf, "EXIST\r\n", "GET-Teacher");

        snprintf(buf, sizeof(buf), "DEL Teacher%d", connfd);
        testcase(connfd, buf, "OK\r\n", "DEL-Teacher");

        snprintf(buf, sizeof(buf), "GET Teacher%d", connfd);
        testcase(connfd, buf, "NO EXIST\r\n", "GET-Teacher");

        snprintf(buf, sizeof(buf), "MOD Teacher%d KING", connfd);
        testcase(connfd, buf, "NO EXIST\r\n", "MOD-Teacher");

        snprintf(buf, sizeof(buf), "EXIST Teacher%d", connfd);
        testcase(connfd, buf, "NO EXIST\r\n", "GET-Teacher");
    }
}

void rbtree_testcase(int connfd) {

    testcase(connfd, "RSET Teacher King", "OK\r\n", "RSET-Teacher");
    testcase(connfd, "RGET Teacher", "King\r\n", "RGET-King-Teacher");
    testcase(connfd, "RMOD Teacher Darren", "OK\r\n", "RMOD-D-Teacher");
    testcase(connfd, "RGET Teacher", "Darren\r\n", "RGET-Darren-Teacher");
    testcase(connfd, "REXIST Teacher", "EXIST\r\n", "REXIST-Teacher");
    testcase(connfd, "RDEL Teacher", "OK\r\n", "RDEL-Teacher");
    testcase(connfd, "RGET Teacher", "NO EXIST\r\n", "RGET-K-Teacher");
    testcase(connfd, "RMOD Teacher KING", "NO EXIST\r\n", "RMOD-K-Teacher");
    testcase(connfd, "REXIST Teacher", "NO EXIST\r\n", "REXIST-Teacher");
}

void rbtree_testcase_1w(int connfd) {

    int count = 10000;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (i = 0; i < count; i++) {

        testcase(connfd, "RSET Teacher King", "OK\r\n", "RSET-Teacher");
        testcase(connfd, "RGET Teacher", "King\r\n", "RGET-King-Teacher");
        testcase(connfd, "RMOD Teacher Darren", "OK\r\n", "RMOD-D-Teacher");
        testcase(connfd, "RGET Teacher", "Darren\r\n", "RGET-Darren-Teacher");
        testcase(connfd, "REXIST Teacher", "EXIST\r\n", "REXIST-Teacher");
        testcase(connfd, "RDEL Teacher", "OK\r\n", "RDEL-Teacher");
        testcase(connfd, "RGET Teacher", "NO EXIST\r\n", "RGET-K-Teacher");
        testcase(connfd, "RMOD Teacher KING", "NO EXIST\r\n", "RMOD-K-Teacher");
        testcase(connfd, "REXIST Teacher", "NO EXIST\r\n", "REXIST-Teacher");
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("rbtree testcase --> time_used: %d, qps: %d\n", time_used, 90000 * 1000 / time_used);
}

void rbtree_testcase_3w(int connfd) {

    int count = 10000;
    int i = 0;

    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "RSET Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "RSET-Teacher");
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "RGET Teacher%d", i);

        char result[128] = {0};
        snprintf(result, 128, "King%d\r\n", i);

        testcase(connfd, cmd, result, "RGET-King-Teacher");
    }

    for (i = 0; i < count; i++) {

        char cmd[128] = {0};
        snprintf(cmd, 128, "RMOD Teacher%d King%d", i, i);
        testcase(connfd, cmd, "OK\r\n", "RGET-King-Teacher");
    }

    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);

    int time_used = TIME_SUB_MS(tv_end, tv_begin); // ms

    printf("rbtree testcase --> time_used: %d, qps: %d\n", time_used, 30000 * 1000 / time_used);
}

void hash_testcase(int connfd) {

    testcase(connfd, "HSET Teacher King", "OK\r\n", "HSET-Teacher");
    testcase(connfd, "HGET Teacher", "King\r\n", "HGET-King-Teacher");
    testcase(connfd, "HMOD Teacher Darren", "OK\r\n", "HMOD-D-Teacher");
    testcase(connfd, "HGET Teacher", "Darren\r\n", "HGET-Darren-Teacher");
    testcase(connfd, "HEXIST Teacher", "EXIST\r\n", "HEXIST-Teacher");
    testcase(connfd, "HDEL Teacher", "OK\r\n", "HDEL-Teacher");
    testcase(connfd, "HGET Teacher", "NO EXIST\r\n", "HGET-K-Teacher");
    testcase(connfd, "HMOD Teacher KING", "NO EXIST\r\n", "HMOD-K-Teacher");
    testcase(connfd, "HEXIST Teacher", "NO EXIST\r\n", "HEXIST-Teacher");
}

//    ./test 192.168.234.135  2000 2
int main(int argc, char* argv[]) {

    if (argc != 4) {
        printf("arg error\n");
        return -1;
    }

    char* ip = argv[1];
    unsigned short port = atoi(argv[2]);
    int mode = atoi(argv[3]);

#if ORIGINAL

    int connfd = connect_tcpserver(ip, port);

    if (mode == 0) {
        rbtree_testcase_1w(connfd);
    } else if (mode == 1) {
        rbtree_testcase_3w(connfd);
    } else if (mode == 2) {
        array_testcase_1w(connfd);
    } else if (mode == 3) {
        hash_testcase(connfd);
    }

#else

    printf("use pthread!\n");
    test_context_t ctx{.ip = ip, .port = port, .mode = mode};
    std::vector<pthread_t> ptid(THREAD_NUM);

    for (int i = 0; i < THREAD_NUM; ++i) {
        pthread_create(&ptid[i], NULL, test_qps_entry, &ctx);
    }
    for (int i = 0; i < THREAD_NUM; ++i) {
        pthread_join(ptid[i], NULL);
    }

#endif
    return 0;
}
