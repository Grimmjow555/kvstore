#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int kvs_split_token(char* msg, char* tokens[]) {

    if (msg == nullptr || tokens == nullptr)
        return -1;
    char* token = strtok(msg, " ");
    int idx = 0;
    while (token != nullptr) {
        printf("idx: %d, %s\n", idx, token);
        tokens[idx++] = token;
        token = strtok(nullptr, " ");
    }
    return idx; // idx为空格的数量，tokens[idx]为空格的位置
}

// SET Key Value
// tokens[0]: SET
// tokens[1]: Key
// tokens[2]: Value

int kvs_filter_protocol(char* tokens[], int count, char* response) {
    if (tokens == nullptr || count == 0 || response == nullptr)
        return -1;
    int cmd = KVS_CMD_START;
    for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; ++cmd) {
        if (strcmp(tokens[0], command[cmd]) == 0) {
            break;
        }
    }

    int ret = 0;
    switch (cmd) {
    case KVS_CMD_SET: {
        ret = sprintf(response, "cmd: SET\n");
        break;
    }
    case KVS_CMD_GET: {
        ret = sprintf(response, "cmd: GET\n");
        break;
    }
    case KVS_CMD_DEL: {
        ret = sprintf(response, "cmd: DEL\n");
        break;
    }
    case KVS_CMD_MOD: {
        ret = sprintf(response, "cmd: MOD\n");
        break;
    }
    case KVS_CMD_EXIST: {
        ret = sprintf(response, "cmd: EXIST\n");
        break;
    }
    default: {
        break;
    }
    }

    return ret;
}

/*
 *msg: request message
 *length: length of request message
 *response: need to send
 *@return: length of response
 */
int kvs_protocol(char* msg, int length, char* response) {
    // 协议类型：SET KEY VALUE
    //          GET KEY

    // printf("[kvs_protocol] recv %d: %s\n", length, msg);
    // memcpy(response, msg, length);

    char* tokens[KVS_MAX_TOKENS] = {0};
    int count = kvs_split_token(msg, tokens);
    if (count == -1)
        return -1;

    return kvs_filter_protocol(tokens, count, response);
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        return -1;
    }
    unsigned short port = atoi(argv[1]); //命令行传入的是字符串，这里需要转化为整数

    int select_network_architecture = atoi(argv[2]);

    switch (select_network_architecture) { //
    case NETWORK_REACTOR: {
        printf("*****USE reactor*****\n");
        reactor_start(port, kvs_protocol);
        break;
    }

    case NETWORK_NTYCO: {
        printf("*****USE NtyCo*****\n");
        ntyco_start(port, kvs_protocol);
        break;
    }

    case NETWORK_PROACTOR: {
        printf("*****USE proactor*****\n");
        proactor_start(port, kvs_protocol);
        break;
    }

    default: {
        printf("no such NETWORK ARCHITECTURE");
        break;
    }
    }
}