#include "kvstore.h"

#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif

void* kvs_malloc(size_t size) { return malloc(size); }

void kvs_free(void* ptr) { return free(ptr); }

const char* command[] = {"SET", "GET", "DEL", "MOD", "EXIST"};

enum KVS_CMD {
    KVS_CMD_START = 0,
    KVS_CMD_SET = KVS_CMD_START,
    KVS_CMD_GET,
    KVS_CMD_DEL,
    KVS_CMD_MOD,
    KVS_CMD_EXIST,
    KVS_CMD_COUNT,
};

const char* response[] = {};

int kvs_split_token(char* msg, char* tokens[]) {

    if (msg == nullptr || tokens == nullptr)
        return -1;
    char* token = strtok(msg, " ");
    int idx = 0;
    while (token != nullptr) {
        // printf("idx: %d, %s\n", idx, token); // idx为子串的索引
        tokens[idx++] = token;
        token = strtok(nullptr, " ");
    }
    return idx; // idx为子串的数量
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

    int length = 0;

    // char* key = tokens[1];
    // char* value = tokens[2];

    switch (cmd) {
    case KVS_CMD_SET: {
        int ret = kvs_array_set(&global_array, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_GET: {
        char* value = kvs_array_get(&global_array, tokens[1]);
        if (value == nullptr) {
            length = sprintf(response, "NO EXIST\r\n");
        } else {
            length = sprintf(response, "%s\r\n", value);
        }

        break;
    }
    case KVS_CMD_DEL: {
        int ret = kvs_array_del(&global_array, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_MOD: {

        int ret = kvs_array_mod(&global_array, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_EXIST: {
        int ret = kvs_array_exist(&global_array, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "EXIST\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    default: {
        break;
    }
    }

    return length;
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

int init_kvengine() {
#if ENABLE_ARRAY
    memset(&global_array, 0, sizeof(kvs_array_t));
    kvs_array_create(&global_array);
#endif
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        return -1;
    }
    unsigned short port = atoi(argv[1]); //命令行传入的是字符串，这里需要转化为整数

    int select_network_architecture = atoi(argv[2]);

    init_kvengine();

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