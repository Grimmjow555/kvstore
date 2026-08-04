

#include "nty_coroutine.h"

#include <arpa/inet.h>
// extern int kvs_protocol(char* msg, int length, char* response);

typedef int (*msg_handler)(char* msg, int length, char* response);
static msg_handler kvs_handler;

void server_reader(void* arg) {
    int fd = *(int*)arg;
    int ret = 0;

    while (1) {

        char buf[1024] = {0};
        ret = recv(fd, buf, 1024, 0);
        if (ret > 0) {
            printf("read from server: %.*s\n", ret, buf);

            char response[1024] = {0};

            // int slength = kvs_protocol(buf, ret, response);
            int slength = kvs_handler(buf, ret, response);

            ret = send(fd, response, slength, 0);

            // ret = send(fd, buf, sizeof(buf), 0);
            if (ret == -1) {
                close(fd);
                break;
            }
        } else if (ret == 0) {
            close(fd);
            break;
        }
    }
}

static void server(void* arg) {

    unsigned short port = *(unsigned short*)arg;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return;

    struct sockaddr_in local, remote;
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = INADDR_ANY;
    bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

    listen(fd, 20);
    printf("listen port : %d\n", port);

    while (1) {
        socklen_t len = sizeof(struct sockaddr_in);
        int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);

        nty_coroutine* read_co;
        nty_coroutine_create(&read_co, server_reader, &cli_fd);
    }
}

int ntyco_start(unsigned short port, msg_handler handler) {
    printf("USE NtyCo\n");

    // unsigned short port = atoi(argv[1]); //原始情况，直接从命令行读取端口，需要转化为整数

    kvs_handler = handler;

    nty_coroutine* co = NULL;
    nty_coroutine_create(&co, server, &port);

    nty_schedule_run();
}
