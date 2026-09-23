#include "aof.h"
#include "kvs_array.h"
#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_replication.h"
#include "kvs_skiptable.h"
#include "kvs_snapshot.h"
#include "kvstore.h"
#include "memorypool.h"
#include "network.h"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

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

void* kvs_malloc(size_t size) {
#if ENABLE_MEMORYPOOL
    return slab_alloc(size);
#else
    return malloc(size);
#endif
}

void* kvs_calloc(size_t size) {
#if ENABLE_MEMORYPOOL
    return slab_calloc(size);
#else
    return calloc(1, size);
#endif
}

void kvs_free(void* ptr) {
#if ENABLE_MEMORYPOOL
    if (ptr != nullptr)
        slab_free_ptr(ptr);
#else
    free(ptr);
#endif
}

const char* command[] = {"SET",      "GET",      "DEL",      "MOD",      "EXIST",

                         "RSET",     "RGET",     "RDEL",     "RMOD",     "REXIST",

                         "HSET",     "HGET",     "HDEL",     "HMOD",     "HEXIST",

                         "SSET",     "SGET",     "SDEL",     "SMOD",     "SEXIST",

                         "RDB SAVE", "RDB LOAD", "AOF LOAD", "AOF CLEAR"};

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

    KVS_CMD_RDB_SAVE,
    KVS_CMD_RDB_LOAD,
    KVS_CMD_AOF_LOAD,
    KVS_CMD_AOF_CLEAR,

    KVS_CMD_COUNT,
};

const char* response[] = {};

/**
 * @brief 解析 RESP (REdis Serialization Protocol) 协议中的数组命令。
 *        该函数将客户端发送的 RESP 数组格式的命令（如 "*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n"）
 *        解析为字符串数组 argv，便于后续命令分发和参数处理。
 *
 * @param buffer   输入的 RESP 协议缓冲区（以 '\0' 结尾的字符串）。
 * @param argc     输出参数，解析出的参数个数（即数组元素个数）。
 * @param consumed 输出参数，函数从 buffer 开头一共解析了多少字节（用于粘包处理时移动指针）。
 *
 * @return 成功返回动态分配的 char** 数组（每个元素是独立的字符串副本），
 *         最后一个元素为 NULL（类似 execv 风格）。
 *         失败返回 NULL（并释放已分配内存）。
 *
 * @note 1. 本函数仅支持 RESP 的数组（以 '*' 开头）和批量字符串（以 '$' 开头）。
 *       2. 调用者负责最终释放返回的 argv 中每个字符串及其本身。
 */
char** resp_parse_command(char* buffer, int* argc, int* consumed) {
    // 1. 基本校验：非空且必须以 '*' 开头（RESP 数组格式）
    if (!buffer || buffer[0] != '*')
        return NULL;

    // 2. 提取数组元素个数（参数个数）
    int param_count = atoi(buffer + 1);

    // 3. 查找第一个 \r\n（数组头部结束）
    char* p = strstr(buffer, "\r\n");
    if (!p)
        return NULL;
    p += 2; // 跳过 "\r\n"

    // 4. 记录头部已消费字节数（从 buffer 开头到第一个 \r\n 之后）
    *consumed = (int)(p - buffer);

    // 5. 为 argv 数组分配内存（参数个数 + 1 个 NULL 结尾）
    char** argv = (char**)malloc((param_count + 1) * sizeof(char*));
    if (!argv)
        return NULL;

    // 6. 循环解析每个参数（期望为批量字符串 $...）
    for (int i = 0; i < param_count; i++) {
        // 6.1 检查当前参数是否以 '$' 开头（批量字符串）
        if (*p != '$') {
            free(argv);
            return NULL;
        }

        // 6.2 读取该批量字符串的长度
        int len = atoi(p + 1); // 跳过 '$' 读取数字

        // 6.3 定位到该批量字符串数据的起始位置（跳过 $len\r\n）
        p = strstr(p, "\r\n") + 2; // 注意：此处未检查 strstr 是否为 NULL，但之前已经假设格式正确

        // 6.4 处理 null 批量字符串（长度为 -1），本函数不支持
        if (len < 0) {
            free(argv);
            return NULL;
        }

        // 6.5 为当前参数值分配内存（len + 1 字节，用于存放 '\0'）
        char* data = (char*)malloc(len + 1);
        if (!data) {
            // 分配失败，释放已分配的 argv 及其它已分配的数据
            // 注意：前面 i 个参数已分配，需要释放
            for (int j = 0; j < i; j++) {
                free(argv[j]);
            }
            free(argv);
            return NULL;
        }

        // 6.6 拷贝数据
        memcpy(data, p, len);
        data[len] = '\0'; // 添加字符串结束符
        argv[i] = data;   // 存入 argv

        // 6.7 移动指针到下一个参数（跳过当前数据及结尾的 \r\n）
        p += len + 2;
    }

    // 7. 设置 argv 结尾为 NULL，便于遍历
    argv[param_count] = NULL;

    // 8. 输出参数个数和已消费字节数
    *argc = param_count;
    *consumed = (int)(p - buffer); // p 此时指向整个命令结束后的位置

    // 9. 返回解析好的 argv
    return argv;
}

// SET Key Value
// tokens[0]: SET
// tokens[1]: Key
// tokens[2]: Value
// response: 存储相应信息（已包含RESP协议头）
// return: response长度
int kvs_filter_protocol(char* tokens[], int count, char* response, int response_size) {
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
            // 在AOF功能打开时，正常写入时记录 AOF；AOF 恢复或 Replica 重放时不重复记录
            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
            }

            // 非 Replica 重放状态下，将写操作同步给 Replica
            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(3, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n"); // 成功
        } else if (ret > 0) {
            length = snprintf(response, response_size, "+EXIST\r\n"); // 键已存在（仍视为成功）
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_GET: {
        char* value = kvs_array_get(&global_array, tokens[1]);
        if (value == NULL) {
            // 返回 "NO EXIST" 的批量字符串格式
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n");
            // 或简单字符串格式（+ 开头）
            // length = sprintf(response, "+NO EXIST\r\n");
        } else {
            length = snprintf(response, response_size, "$%zu\r\n%s\r\n", strlen(value), value);
        }
        break;
    }
    case KVS_CMD_DEL: {
        int ret = kvs_array_del(&global_array, tokens[1]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(2, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(2, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n"); // 删除成功
        } else if (ret > 0) {
            length = snprintf(response, response_size,
                              "$8\r\nNO EXIST\r\n"); // 键不存在（作为批量字符串返回）
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_MOD: {
        int ret = kvs_array_mod(&global_array, tokens[1], tokens[2]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(3, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n");
        } else if (ret > 0) {
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n");
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }
    case KVS_CMD_EXIST: {
        int ret = kvs_array_exist(&global_array, tokens[1]);
        if (ret == 0) {
            length = snprintf(response, response_size, "$5\r\nEXIST\r\n"); // 存在
        } else if (ret > 0) {
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n"); // 不存在
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

#endif

// rbtree
#if ENABLE_RBTREE
    case KVS_CMD_RSET: {
        int ret = kvs_rbtree_set(&global_rbtree, tokens[1], tokens[2]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(3, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n"); // 成功
        } else if (ret > 0) {
            length = snprintf(response, response_size, "+EXIST\r\n"); // 键已存在（仍视为成功）
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_RGET: {
        char* value = kvs_rbtree_get(&global_rbtree, tokens[1]);
        if (value == NULL) {
            // 返回 "NO EXIST" 的批量字符串格式
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n");
            // 或简单字符串格式（+ 开头）
            // length = snprintf(response, response_size, "+NO EXIST\r\n");
        } else {
            length = snprintf(response, response_size, "$%zu\r\n%s\r\n", strlen(value), value);
        }
        break;
    }
    case KVS_CMD_RDEL: {
        int ret = kvs_rbtree_del(&global_rbtree, tokens[1]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(2, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(2, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n"); // 删除成功
        } else if (ret > 0) {
            length = snprintf(response, response_size,
                              "$8\r\nNO EXIST\r\n"); // 键不存在（作为批量字符串返回）
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_RMOD: {
        int ret = kvs_rbtree_mod(&global_rbtree, tokens[1], tokens[2]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(3, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n");
        } else if (ret > 0) {
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n");
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }
    case KVS_CMD_REXIST: {
        int ret = kvs_rbtree_exist(&global_rbtree, tokens[1]);
        if (ret == 0) {
            length = snprintf(response, response_size, "$5\r\nEXIST\r\n"); // 存在
        } else if (ret > 0) {
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n"); // 不存在
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

#endif

// hash
#if ENABLE_HASH
    case KVS_CMD_HSET: {
        int ret = kvs_hash_set(&global_hash, tokens[1], tokens[2]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(3, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n"); // 成功
        } else if (ret > 0) {
            length = snprintf(response, response_size, "+EXIST\r\n"); // 键已存在（仍视为成功）
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_HGET: {
        char* value = kvs_hash_get(&global_hash, tokens[1]);
        if (value == NULL) {
            // 返回 "NO EXIST" 的批量字符串格式
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n");
            // 或简单字符串格式（+ 开头）
            // length = snprintf(response, response_size, "+NO EXIST\r\n");
        } else {
            length = snprintf(response, response_size, "$%zu\r\n%s\r\n", strlen(value), value);
        }
        break;
    }
    case KVS_CMD_HDEL: {
        int ret = kvs_hash_del(&global_hash, tokens[1]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(2, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(2, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n"); // 删除成功
        } else if (ret > 0) {
            length = snprintf(response, response_size,
                              "$8\r\nNO EXIST\r\n"); // 键不存在（作为批量字符串返回）
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_HMOD: {
        int ret = kvs_hash_mod(&global_hash, tokens[1], tokens[2]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(3, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n");
        } else if (ret > 0) {
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n");
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }
    case KVS_CMD_HEXIST: {
        int ret = kvs_hash_exist(&global_hash, tokens[1]);
        if (ret == 0) {
            length = snprintf(response, response_size, "$5\r\nEXIST\r\n"); // 存在
        } else if (ret > 0) {
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n"); // 不存在
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

#endif

// skiptable
#if ENABLE_SKIPTABLE

    case KVS_CMD_SSET: {
        int ret = kvs_skiptable_set(&global_skiptable, tokens[1], tokens[2]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(3, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n"); // 成功
        } else if (ret > 0) {
            length = snprintf(response, response_size, "+EXIST\r\n"); // 键已存在（仍视为成功）
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_SGET: {
        char* value = kvs_skiptable_get(&global_skiptable, tokens[1]);
        if (value == NULL) {
            // 返回 "NO EXIST" 的批量字符串格式
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n");
            // 或简单字符串格式（+ 开头）
            // length = snprintf(response, response_size, "+NO EXIST\r\n");
        } else {
            length = snprintf(response, response_size, "$%zu\r\n%s\r\n", strlen(value), value);
        }
        break;
    }
    case KVS_CMD_SDEL: {
        int ret = kvs_skiptable_del(&global_skiptable, tokens[1]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(2, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(2, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n"); // 删除成功
        } else if (ret > 0) {
            length = snprintf(response, response_size,
                              "$8\r\nNO EXIST\r\n"); // 键不存在（作为批量字符串返回）
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n"); // 错误
        }
        break;
    }
    case KVS_CMD_SMOD: {
        int ret = kvs_skiptable_mod(&global_skiptable, tokens[1], tokens[2]);
        if (ret == 0) {

            if (kvs_config_aof_enabled() && !kvs_aof_is_replaying() &&
                !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
            }

            if (!kvs_replication_is_replaying()) {
                kvs_replication_append(3, tokens);
            }

            length = snprintf(response, response_size, "+OK\r\n");
        } else if (ret > 0) {
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n");
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }
    case KVS_CMD_SEXIST: {
        int ret = kvs_skiptable_exist(&global_skiptable, tokens[1]);
        if (ret == 0) {
            length = snprintf(response, response_size, "$5\r\nEXIST\r\n"); // 存在
        } else if (ret > 0) {
            length = snprintf(response, response_size, "$8\r\nNO EXIST\r\n"); // 不存在
        } else if (ret < 0) {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

#endif

    case KVS_CMD_RDB_SAVE: {
        if (!kvs_config_rdb_enabled()) {
            length = snprintf(response, response_size, "-ERROR\r\n");
            break;
        }

        int ret = kvs_snapshot_save("../data/kvstore.data");

        if (ret == 0) {
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

    case (KVS_CMD_RDB_LOAD): {
        if (!kvs_config_rdb_enabled()) {
            length = snprintf(response, response_size, "-ERROR\r\n");
            break;
        }

        int ret = kvs_snapshot_load("../data/kvstore.data");

        if (ret == 0) {
            kvs_replication_resync();
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

#if AOF_ENABLE
    case KVS_CMD_AOF_LOAD: {
        if (!kvs_config_aof_enabled()) {
            length = snprintf(response, response_size, "-ERROR\r\n");
            break;
        }

        int ret = kvs_aof_replay("../data/append.aof");
        if (ret == 0) {
            kvs_replication_resync();
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

    case KVS_CMD_AOF_CLEAR: {
        if (!kvs_config_aof_enabled()) {
            length = snprintf(response, response_size, "-ERROR\r\n");
            break;
        }

        int ret = kvs_aof_clear();
        if (ret == 0) {
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
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

/*
 *msg: request message
 *length: length of request message
 *response: need to send
 *response_size: size of response buffer
 *@return: length of response
 */
int kvs_protocol(char* msg, int length, char* response, int response_size) {
    int msg_used = 0;
    int resp_offset = 0;
    while (msg_used < length && resp_offset < response_size) {
        int argc, consumed;
        char** argv = resp_parse_command(msg + msg_used, &argc, &consumed);
        if (!argv)
            break;

        // 计算剩余可写入 response 的空间
        int remaining = response_size - resp_offset;
        if (remaining <= 0)
            break;

        // 动态分配临时缓冲区，至少保证能容纳任何单条命令的响应（可设一个合理上限）
        // 这里假设单条命令响应不会超过 remaining，否则会截断
        char* tmp_resp = (char*)malloc(remaining);
        if (!tmp_resp)
            break;

        int len = kvs_filter_protocol(argv, argc, tmp_resp, remaining);
        if (len > 0 && len < remaining) {
            memcpy(response + resp_offset, tmp_resp, len);
            resp_offset += len;
        } else if (len >= remaining) {
            // 响应被截断，根据业务可选择断开或返回错误
            // 这里简单处理为返回错误
            free(tmp_resp);
            for (int i = 0; i < argc; i++)
                free(argv[i]);
            free(argv);
            break;
        }
        free(tmp_resp);

        for (int i = 0; i < argc; i++)
            free(argv[i]);
        free(argv);
        msg_used += consumed;
    }
    return resp_offset;
}

//初始化存储引擎
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

//销毁存储引擎
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

//引擎数据重置
int kvs_reset_data() {
    destroy_kvengine();
#if ENABLE_MEMORYPOOL
    // 这里只重置统计信息，不再 slab_dest() + slab_init()：
    // 把 chunk 交还系统会让其它线程（Replica 回放线程）手里正在使用的块变成悬空指针。
    // 引擎数据已由 destroy/init 重建，内存池自身的空闲链表本来就是一致的。
    slab_reset();
#endif
    return init_kvengine();
}

static int ensure_data_directory() {
    if (mkdir("../data", 0755) == 0 || errno == EEXIST) {
        struct stat data_stat;
        if (stat("../data", &data_stat) == 0 && S_ISDIR(data_stat.st_mode)) {
            return 0;
        }
    }

    kvs_log(KVS_LOG_ERROR, "Failed to create data directory: %s", strerror(errno));
    return -1;
}

// ./kvstore <port> <role> <master_ip> <master_port>
// role: 0(Master) 1(Replica)
// ./kvstore 9999 0
// ./kvstore 2000 1 39.97.42.225 9999
int main(int argc, char* argv[]) {

    kvs_config_set_defaults();
    if (kvs_config_parse_switches(&argc, &argv) != 0) {
        kvs_log(KVS_LOG_ERROR,
                "Failed to parse configuration. Use --config <file> or see usage below.");
        return -1;
    }

    const char* bind_ip = kvs_config_bind_ip();
    int port = kvs_config_port();
    int role = kvs_config_role();
    const char* master_ip = kvs_config_master_ip();
    int master_port = kvs_config_master_port();
    struct in_addr bind_addr;
    int bind_ip_valid = bind_ip != nullptr && inet_pton(AF_INET, bind_ip, &bind_addr) == 1;

    // 兼容旧的纯位置参数调用方式。位置参数会覆盖配置文件或命令行开关。
    if (argc >= 2 && argv[1] != nullptr && argv[1][0] != '\0') {
        port = atoi(argv[1]);
    }
    if (argc >= 3 && argv[2] != nullptr && argv[2][0] != '\0') {
        if (strcmp(argv[2], "master") == 0) {
            role = 0;
        } else if (strcmp(argv[2], "replica") == 0 || strcmp(argv[2], "slave") == 0) {
            role = 1;
        } else {
            role = atoi(argv[2]);
        }
    }
    if (role == KVS_ROLE_REPLICA && argc >= 5) {
        master_ip = argv[3];
        master_port = atoi(argv[4]);
    }

    if (port <= 0 || port > 65535 || (role != 0 && role != 1) ||
        bind_ip == nullptr || bind_ip[0] == '\0' ||
        bind_ip_valid != 1 ||
        (role == KVS_ROLE_REPLICA && (master_ip == nullptr || master_ip[0] == '\0' ||
                                     master_port <= 0 || master_port > 65535))) {
        printf("Usage:\n"
               "  %s [--config kvstore.conf]\n"
               "     [--bind 0.0.0.0] [--port 9999]\n"
               "     [--role master|replica]\n"
               "     [--master-ip 127.0.0.1] [--master-port 19001]\n"
               "     [--log-level debug|info|warn|error|off]\n"
               "     [--persistence-mode none|rdb|aof|both]\n"
               "  Legacy Master : %s <port> 0\n"
               "  Legacy Replica: %s <port> 1 <master_ip> <master_port>\n",
               argv[0], argv[0], argv[0]);
        return -1;
    }

    kvs_log(KVS_LOG_INFO,
            "Configuration: bind=%s port=%d role=%s log_level=%d persistence=rdb:%s,aof:%s",
            bind_ip, port, role == 0 ? "master" : "replica", kvs_config_log_level(),
            kvs_config_rdb_enabled() ? "on" : "off",
            kvs_config_aof_enabled() ? "on" : "off");

    if (ensure_data_directory() != 0) {
        return -1;
    }

    // 初始化 KV Engine
    init_kvengine();

    //初始化内存池
#if ENABLE_MEMORYPOOL
    slab_init();
#endif

    // 初始化复制模块
    if (role == 0) {
        kvs_replication_init(KVS_ROLE_MASTER);
        // 优先创建本地 eBPF 实时同步队列；失败时后续增量同步自动回退 TCP。
        if (kvs_ebpf_master_init((unsigned short)port) != 0) {
            kvs_log(KVS_LOG_WARN,
                    "eBPF realtime sync unavailable, falling back to TCP realtime sync");
        }
#ifdef KVS_ENABLE_RDMA
        if (kvs_replication_start_rdma_listener((unsigned short)port) != 0) {
            // RDMA 设备不可用时不要阻止服务启动，自动回退到 TCP 全量同步。
            kvs_log(KVS_LOG_WARN,
                    "RDMA listener unavailable, falling back to TCP full sync");
        }
#endif
        if (kvs_config_aof_enabled() && kvs_aof_init("../data/append.aof") != 0) {
            kvs_log(KVS_LOG_ERROR, "AOF init failed");
            return -1;
        }
    } else {
        kvs_replication_init(KVS_ROLE_REPLICA);
    }

    // Replica 连接 Master
    if (role == KVS_ROLE_REPLICA) {
        int fd = kvs_replication_connect_master(master_ip, master_port);

        if (fd < 0) {
            kvs_log(KVS_LOG_ERROR, "Failed to connect master %s:%d", master_ip, master_port);
            return -1;
        }

#ifdef KVS_ENABLE_RDMA
        // 先通过 RDMA 完成已有数据的全量同步，再发送 TCP 握手进入增量同步。
        if (kvs_replication_rdma_full_sync(master_ip, master_port) != 0) {
            // Master 可能同样因没有 RDMA 设备而回退为 TCP 全量同步。
            kvs_log(KVS_LOG_WARN,
                    "RDMA full sync unavailable, falling back to TCP full sync");
        }
#endif

        if (kvs_replication_start() != 0) {
            kvs_log(KVS_LOG_ERROR, "Failed to start replication");
            return -1;
        }
    }

    // 启动网络服务
#if USE_REACTOR
    // printf("**********USE reactor**********\n");
    reactor_start(bind_ip, (unsigned short)port, kvs_protocol);

#elif USE_NTYCO
    // printf("**********USE NtyCo**********\n");
    ntyco_start(bind_ip, (unsigned short)port, kvs_protocol);

#elif USE_PROACTOR
    // printf("**********USE proactor**********\n");
    proactor_start(bind_ip, (unsigned short)port, kvs_protocol);

#endif

#if AOF_ENABLE
    kvs_aof_close();
#endif
    // 先停止复制线程、释放引擎数据，最后才销毁内存池：
    // slab_dest() 会释放全部 chunk，必须确认没有其它线程还在分配/释放内存。
    kvs_replication_destroy();
    destroy_kvengine();
#if ENABLE_MEMORYPOOL
    slab_dest();
#endif
    return 0;
}
