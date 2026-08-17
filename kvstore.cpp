#include "kvstore.h"

#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif

#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif

#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif

#if ENABLE_SKIPTABLE
extern kvs_skiptable_t global_skiptable;
#endif

void* kvs_malloc(size_t size) { return malloc(size); }

void kvs_free(void* ptr) { return free(ptr); }

const char* command[] = {"SET",    "GET",  "DEL",    "MOD",  "EXIST", "RSET",  "RGET",
                         "RDEL",   "RMOD", "REXIST", "HSET", "HGET",  "HDEL",  "HMOD",
                         "HEXIST", "SSET", "SGET",   "SDEL", "SMOD",  "SEXIST"};

enum KVS_CMD {
    KVS_CMD_START = 0,
    // array
    KVS_CMD_SET = KVS_CMD_START,
    KVS_CMD_GET,
    KVS_CMD_DEL,
    KVS_CMD_MOD,
    KVS_CMD_EXIST,

    // rbtree
    KVS_CMD_RSET,
    KVS_CMD_RGET,
    KVS_CMD_RDEL,
    KVS_CMD_RMOD,
    KVS_CMD_REXIST,

    // hash
    KVS_CMD_HSET,
    KVS_CMD_HGET,
    KVS_CMD_HDEL,
    KVS_CMD_HMOD,
    KVS_CMD_HEXIST,

    // skiptable
    KVS_CMD_SSET,
    KVS_CMD_SGET,
    KVS_CMD_SDEL,
    KVS_CMD_SMOD,
    KVS_CMD_SEXIST,

    KVS_CMD_COUNT,
};

const char* response[] = {};

// 从缓冲区解析一条 RESP 命令，返回参数数组，并记录消耗字节数
char** resp_parse_command(char* buffer, int* argc, int* consumed) {
    if (!buffer || buffer[0] != '*')
        return NULL;
    int param_count = atoi(buffer + 1);
    char* p = strstr(buffer, "\r\n");
    if (!p)
        return NULL;
    p += 2; // 跳过 \r\n
    *consumed = (int)(p - buffer);

    char** argv = (char**)kvs_malloc((param_count + 1) * sizeof(char*));
    if (!argv)
        return NULL;

    for (int i = 0; i < param_count; i++) {
        if (*p != '$') {
            kvs_free(argv);
            return NULL;
        }
        int len = atoi(p + 1);
        p = strstr(p, "\r\n") + 2; // 跳过 $len\r\n
        if (len < 0) {             // 不支持 null 批量字符串
            kvs_free(argv);
            return NULL;
        }
        char* data = (char*)kvs_malloc(len + 1);
        if (!data) {
            kvs_free(argv);
            return NULL;
        }
        memcpy(data, p, len);
        data[len] = '\0';
        argv[i] = data;
        p += len + 2; // 跳过数据及 \r\n
    }
    argv[param_count] = NULL;
    *argc = param_count;
    *consumed = (int)(p - buffer);
    return argv;
}

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
#if 1

// response: 存储相应信息（已包含RESP协议头）
// return: response长度
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
// array
#if ENABLE_ARRAY
    case KVS_CMD_SET: {
        int ret = kvs_array_set(&global_array, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n"); // 成功
        } else if (ret > 0) {
            length = sprintf(response, "+EXIST\r\n"); // 键已存在（仍视为成功）
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_GET: {
        char* value = kvs_array_get(&global_array, tokens[1]);
        if (value == NULL) {
            // 返回 "NO EXIST" 的批量字符串格式
            length = sprintf(response, "$8\r\nNO EXIST\r\n");
            // 或简单字符串格式（+ 开头）
            // length = sprintf(response, "+NO EXIST\r\n");
        } else {
            length = sprintf(response, "$%zu\r\n%s\r\n", strlen(value), value);
        }
        break;
    }
    case KVS_CMD_DEL: {
        int ret = kvs_array_del(&global_array, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n"); // 删除成功
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n"); // 键不存在（作为批量字符串返回）
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_MOD: {
        int ret = kvs_array_mod(&global_array, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n");
        }
        break;
    }
    case KVS_CMD_EXIST: {
        int ret = kvs_array_exist(&global_array, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "$5\r\nEXIST\r\n"); // 存在
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n"); // 不存在
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n");
        }
        break;
    }
#endif

// rbtree
#if ENABLE_RBTREE
    case KVS_CMD_RSET: {
        int ret = kvs_rbtree_set(&global_rbtree, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n"); // 成功
        } else if (ret > 0) {
            length = sprintf(response, "+EXIST\r\n"); // 键已存在（仍视为成功）
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_RGET: {
        char* value = kvs_rbtree_get(&global_rbtree, tokens[1]);
        if (value == NULL) {
            // 返回 "NO EXIST" 的批量字符串格式
            length = sprintf(response, "$8\r\nNO EXIST\r\n");
            // 或简单字符串格式（+ 开头）
            // length = sprintf(response, "+NO EXIST\r\n");
        } else {
            length = sprintf(response, "$%zu\r\n%s\r\n", strlen(value), value);
        }
        break;
    }
    case KVS_CMD_RDEL: {
        int ret = kvs_rbtree_del(&global_rbtree, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n"); // 删除成功
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n"); // 键不存在（作为批量字符串返回）
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_RMOD: {
        int ret = kvs_rbtree_mod(&global_rbtree, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n");
        }
        break;
    }
    case KVS_CMD_REXIST: {
        int ret = kvs_rbtree_exist(&global_rbtree, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "$5\r\nEXIST\r\n"); // 存在
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n"); // 不存在
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n");
        }
        break;
    }
#endif

// hash
#if ENABLE_HASH
    case KVS_CMD_HSET: {
        int ret = kvs_hash_set(&global_hash, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n"); // 成功
        } else if (ret > 0) {
            length = sprintf(response, "+EXIST\r\n"); // 键已存在（仍视为成功）
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_HGET: {
        char* value = kvs_hash_get(&global_hash, tokens[1]);
        if (value == NULL) {
            // 返回 "NO EXIST" 的批量字符串格式
            length = sprintf(response, "$8\r\nNO EXIST\r\n");
            // 或简单字符串格式（+ 开头）
            // length = sprintf(response, "+NO EXIST\r\n");
        } else {
            length = sprintf(response, "$%zu\r\n%s\r\n", strlen(value), value);
        }
        break;
    }
    case KVS_CMD_HDEL: {
        int ret = kvs_hash_del(&global_hash, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n"); // 删除成功
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n"); // 键不存在（作为批量字符串返回）
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_HMOD: {
        int ret = kvs_hash_mod(&global_hash, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n");
        }
        break;
    }
    case KVS_CMD_HEXIST: {
        int ret = kvs_hash_exist(&global_hash, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "$5\r\nEXIST\r\n"); // 存在
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n"); // 不存在
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n");
        }
        break;
    }
#endif

// skiptable
#if ENABLE_SKIPTABLE

    case KVS_CMD_SSET: {
        int ret = kvs_skiptable_set(&global_skiptable, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n"); // 成功
        } else if (ret > 0) {
            length = sprintf(response, "+EXIST\r\n"); // 键已存在（仍视为成功）
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_SGET: {
        char* value = kvs_skiptable_get(&global_skiptable, tokens[1]);
        if (value == NULL) {
            // 返回 "NO EXIST" 的批量字符串格式
            length = sprintf(response, "$8\r\nNO EXIST\r\n");
            // 或简单字符串格式（+ 开头）
            // length = sprintf(response, "+NO EXIST\r\n");
        } else {
            length = sprintf(response, "$%zu\r\n%s\r\n", strlen(value), value);
        }
        break;
    }
    case KVS_CMD_SDEL: {
        int ret = kvs_skiptable_del(&global_skiptable, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n"); // 删除成功
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n"); // 键不存在（作为批量字符串返回）
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_SMOD: {
        int ret = kvs_skiptable_mod(&global_skiptable, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "+OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n");
        }
        break;
    }
    case KVS_CMD_SEXIST: {
        int ret = kvs_skiptable_exist(&global_skiptable, tokens[1]);
        if (ret == 0) {
            length = sprintf(response, "$5\r\nEXIST\r\n"); // 存在
        } else if (ret > 0) {
            length = sprintf(response, "$8\r\nNO EXIST\r\n"); // 不存在
        } else if (ret < 0) {
            length = sprintf(response, "-ERROR\r\n");
        }
        break;
    }

#endif

    default: {
        break;
    }
    }

    return length;
}

#else
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
// array
#if ENABLE_ARRAY
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
#endif

// rbtree
#if ENABLE_RBTREE
    case KVS_CMD_RSET: {
        int ret = kvs_rbtree_set(&global_rbtree, tokens[1], tokens[2]);

        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_RGET: {
        char* value = kvs_rbtree_get(&global_rbtree, tokens[1]);

        if (value == nullptr) {
            length = sprintf(response, "NO EXIST\r\n");
        } else {
            length = sprintf(response, "%s\r\n", value);
        }

        break;
    }
    case KVS_CMD_RDEL: {
        int ret = kvs_rbtree_del(&global_rbtree, tokens[1]);

        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_RMOD: {
        int ret = kvs_rbtree_mod(&global_rbtree, tokens[1], tokens[2]);

        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_REXIST: {
        int ret = kvs_rbtree_exist(&global_rbtree, tokens[1]);

        if (ret == 0) {
            length = sprintf(response, "EXIST\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
#endif

// hash
#if ENABLE_HASH
    case KVS_CMD_HSET: {
        int ret = kvs_hash_set(&global_hash, tokens[1], tokens[2]);

        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_HGET: {
        char* value = kvs_hash_get(&global_hash, tokens[1]);

        if (value == nullptr) {
            length = sprintf(response, "NO EXIST\r\n");
        } else {
            length = sprintf(response, "%s\r\n", value);
        }

        break;
    }
    case KVS_CMD_HDEL: {
        int ret = kvs_hash_del(&global_hash, tokens[1]);

        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_HMOD: {
        int ret = kvs_hash_mod(&global_hash, tokens[1], tokens[2]);

        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_HEXIST: {
        int ret = kvs_hash_exist(&global_hash, tokens[1]);

        if (ret == 0) {
            length = sprintf(response, "EXIST\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
#endif

// skiptable
#if ENABLE_SKIPTABLE

    case KVS_CMD_SSET: {
        int ret = kvs_skiptable_set(&global_skiptable, tokens[1], tokens[2]);
        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_SGET: {
        char* value = kvs_skiptable_get(&global_skiptable, tokens[1]);
        if (value == nullptr) {
            length = sprintf(response, "NO EXIST\r\n");
        } else {
            length = sprintf(response, "%s\r\n", value);
        }
        break;
    }
    case KVS_CMD_SDEL: {
        int ret = kvs_skiptable_del(&global_skiptable, tokens[1]);

        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_SMOD: {
        int ret = kvs_skiptable_mod(&global_skiptable, tokens[1], tokens[2]);

        if (ret == 0) {
            length = sprintf(response, "OK\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }
    case KVS_CMD_SEXIST: {
        int ret = kvs_skiptable_exist(&global_skiptable, tokens[1]);

        if (ret == 0) {
            length = sprintf(response, "EXIST\r\n");
        } else if (ret > 0) {
            length = sprintf(response, "NO EXIST\r\n");
        } else if (ret < 0) {
            length = sprintf(response, "ERROR\r\n");
        }

        break;
    }

#endif

    default: {
        break;
    }
    }

    return length;
}
#endif

#if 1
/*
 *msg: request message
 *length: length of request message
 *response: need to send
 *@return: length of response
 */
int kvs_protocol(char* msg, int length, char* response) {
    int total_used = 0;
    int resp_offset = 0;
    while (total_used < length) {
        int argc, consumed;
        char** argv = resp_parse_command(msg + total_used, &argc, &consumed);
        if (!argv)
            break;

        char tmp_resp[512];
        int len = kvs_filter_protocol(argv, argc, tmp_resp);
        memcpy(response + resp_offset, tmp_resp, len);
        resp_offset += len;

        for (int i = 0; i < argc; i++)
            kvs_free(argv[i]);
        kvs_free(argv);
        total_used += consumed;
    }
    return resp_offset;
}
#else
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
#endif

int init_kvengine() {
#if ENABLE_ARRAY
    memset(&global_array, 0, sizeof(kvs_array_t));
    kvs_array_create(&global_array);
#endif

#if ENABLE_RBTREE
    memset(&global_rbtree, 0, sizeof(kvs_rbtree_t));
    kvs_rbtree_create(&global_rbtree);
#endif

#if ENABLE_HASH
    memset(&global_hash, 0, sizeof(kvs_hash_t));
    kvs_hash_create(&global_hash);
#endif

#if ENABLE_SKIPTABLE
    memset(&global_skiptable, 0, sizeof(kvs_skiptable_t));
    kvs_skiptable_create(&global_skiptable);
#endif
    return 0;
}

int destroy_kvengine() {
#if ENABLE_ARRAY
    kvs_array_destroy(&global_array);
#endif

#if ENABLE_RBTREE
    kvs_rbtree_destroy(&global_rbtree);
#endif

#if ENABLE_HASH
    kvs_hash_destroy(&global_hash);
#endif

#if ENABLE_SKIPTABLE
    kvs_skiptable_destroy(&global_skiptable);
#endif
    return 0;
}

// ./kvstore 2000 select
// select 1: reactor
// select 2: NtyCo
// select 3: proactor

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
    destroy_kvengine();
}