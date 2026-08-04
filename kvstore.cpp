#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/*
 *msg: request message
 *length: length of request message
 *response: need to send
 *@return: length of response
 */

int kvs_protocol(char* msg, int length, char* response) {
    printf("[kvs_protocol] recv %d: %s\n", length, msg);
    memcpy(response, msg, length);
    return strlen(response);
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        return -1;
    }
    unsigned short port = atoi(argv[1]); //命令行传入的是字符串，这里需要转化为整数

    int select_network_architecture = atoi(argv[2]);

    switch (select_network_architecture) { //
    case NETWORK_REACTOR: {
        reactor_start(port, kvs_protocol);
        break;
    }

    case NETWORK_NTYCO: {
        ntyco_start(port, kvs_protocol);
        break;
    }

    case NETWORK_PROACTOR: {
        proactor_start(port, kvs_protocol);
        break;
    }
    default:
        printf("no such NETWORK ARCHITECTURE");
        break;
    }
}