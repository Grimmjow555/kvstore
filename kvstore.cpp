#include "aof.h"
#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_replication.h"
#include "kvs_skiptable.h"
#include "kvstore.h"
#include "network.h"
#include <cstring>
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

void* kvs_malloc(size_t size) { return malloc(size); }

void kvs_free(void* ptr) { return free(ptr); }

const char* command[] = {"SET",      "GET",      "DEL",  "MOD",  "EXIST",  "SAVE",  "LOAD",
                         "RSET",     "RGET",     "RDEL", "RMOD", "REXIST", "RSAVE", "RLOAD",
                         "HSET",     "HGET",     "HDEL", "HMOD", "HEXIST", "HSAVE", "HLOAD",
                         "SSET",     "SGET",     "SDEL", "SMOD", "SEXIST", "SSAVE", "SLOAD",

                         "LOAD_AOF", "CLEAR_AOF"};

enum KVS_CMD {
    KVS_CMD_START = 0,
    // array
    KVS_CMD_SET = KVS_CMD_START,
    KVS_CMD_GET,
    KVS_CMD_DEL,
    KVS_CMD_MOD,
    KVS_CMD_EXIST,
    KVS_CMD_SAVE,
    KVS_CMD_LOAD,

    // rbtree
    KVS_CMD_RSET,
    KVS_CMD_RGET,
    KVS_CMD_RDEL,
    KVS_CMD_RMOD,
    KVS_CMD_REXIST,
    KVS_CMD_RSAVE,
    KVS_CMD_RLOAD,

    // hash
    KVS_CMD_HSET,
    KVS_CMD_HGET,
    KVS_CMD_HDEL,
    KVS_CMD_HMOD,
    KVS_CMD_HEXIST,
    KVS_CMD_HSAVE,
    KVS_CMD_HLOAD,

    // skiptable
    KVS_CMD_SSET,
    KVS_CMD_SGET,
    KVS_CMD_SDEL,
    KVS_CMD_SMOD,
    KVS_CMD_SEXIST,
    KVS_CMD_SSAVE,
    KVS_CMD_SLOAD,

    KVS_CMD_LOAD_AOF,
    KVS_CMD_CLEAR_AOF,

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
 *       2. 不支持 null 批量字符串（长度为 -1），遇到会返回 NULL。
 *       3. 调用者负责最终释放返回的 argv 中每个字符串及其本身（通过 kvs_free）。
 *       4. 本函数内部使用 kvs_malloc/kvs_free，需确保外部已实现。
 *       5. 遇到解析错误（如格式不匹配）会立即返回 NULL，调用者需自行处理。
 */
char** resp_parse_command(char* buffer, int* argc, int* consumed) {
    // 1. 基本校验：非空且必须以 '*' 开头（RESP 数组格式）
    if (!buffer || buffer[0] != '*')
        return NULL;

    // 2. 提取数组元素个数（参数个数）
    int param_count = atoi(buffer + 1); // 跳过 '*' 读取数字

    // 3. 查找第一个 \r\n（数组头部结束）
    char* p = strstr(buffer, "\r\n");
    if (!p)
        return NULL;
    p += 2; // 跳过 "\r\n"

    // 4. 记录头部已消费字节数（从 buffer 开头到第一个 \r\n 之后）
    *consumed = (int)(p - buffer);

    // 5. 为 argv 数组分配内存（参数个数 + 1 个 NULL 结尾）
    char** argv = (char**)kvs_malloc((param_count + 1) * sizeof(char*));
    if (!argv)
        return NULL;

    // 6. 循环解析每个参数（期望为批量字符串 $...）
    for (int i = 0; i < param_count; i++) {
        // 6.1 检查当前参数是否以 '$' 开头（批量字符串）
        if (*p != '$') {
            kvs_free(argv);
            return NULL;
        }

        // 6.2 读取该批量字符串的长度
        int len = atoi(p + 1); // 跳过 '$' 读取数字

        // 6.3 定位到该批量字符串数据的起始位置（跳过 $len\r\n）
        p = strstr(p, "\r\n") + 2; // 注意：此处未检查 strstr 是否为 NULL，但之前已经假设格式正确

        // 6.4 处理 null 批量字符串（长度为 -1），本函数不支持
        if (len < 0) {
            kvs_free(argv);
            return NULL;
        }

        // 6.5 为当前参数值分配内存（len + 1 字节，用于存放 '\0'）
        char* data = (char*)kvs_malloc(len + 1);
        if (!data) {
            // 分配失败，释放已分配的 argv 及其它已分配的数据
            // 注意：前面 i 个参数已分配，需要释放
            for (int j = 0; j < i; j++) {
                kvs_free(argv[j]);
            }
            kvs_free(argv);
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
            // 非 AOF 恢复 且 非Replica进行同步时
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);         // 修改成功之后记录增量日志
                kvs_replication_append(3, tokens); // 同步给 Replica
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(2, tokens);
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
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
    case (KVS_CMD_SAVE): {
        int ret = kvs_array_save(&global_array, "../data/array.data");
        if (ret == 0) {
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }
    case (KVS_CMD_LOAD): {
        int ret = kvs_array_load(&global_array, "../data/array.data");
        if (ret == 0) {
            kvs_replication_resync();
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(2, tokens);
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
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
    case (KVS_CMD_RSAVE): {
        int ret = kvs_rbtree_save(&global_rbtree, "../data/rbtree.data");
        if (ret == 0) {
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }
    case (KVS_CMD_RLOAD): {
        int ret = kvs_rbtree_load(&global_rbtree, "../data/rbtree.data");
        if (ret == 0) {
            kvs_replication_resync();
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(2, tokens);
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
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
    case (KVS_CMD_HSAVE): {
        int ret = kvs_hash_save(&global_hash, "../data/hash.data");
        if (ret == 0) {
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

    case (KVS_CMD_HLOAD): {
        int ret = kvs_hash_load(&global_hash, "../data/hash.data");
        if (ret == 0) {
            kvs_replication_resync();
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(2, tokens);
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
            if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
                kvs_aof_append(3, tokens);
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
    case (KVS_CMD_SSAVE): {
        int ret = kvs_skiptable_save(&global_skiptable, "../data/skiptable.data");
        if (ret == 0) {
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }
    case (KVS_CMD_SLOAD): {
        int ret = kvs_skiptable_load(&global_skiptable, "../data/skiptable.data");
        if (ret == 0) {
            kvs_replication_resync();
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

#endif

#if AOF_ENABLE
    case KVS_CMD_LOAD_AOF: {
        int ret = kvs_aof_replay("../data/append.aof");
        if (ret == 0) {
            length = snprintf(response, response_size, "+OK\r\n");
        } else {
            length = snprintf(response, response_size, "-ERROR\r\n");
        }
        break;
    }

    case KVS_CMD_CLEAR_AOF: {
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
    int total_used = 0;
    int resp_offset = 0;
    while (total_used < length && resp_offset < response_size) {
        int argc, consumed;
        char** argv = resp_parse_command(msg + total_used, &argc, &consumed);
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
                kvs_free(argv[i]);
            kvs_free(argv);
            break;
        }
        free(tmp_resp);

        for (int i = 0; i < argc; i++)
            kvs_free(argv[i]);
        kvs_free(argv);
        total_used += consumed;
    }
    return resp_offset;
}

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

int kvs_reset_data() {
    destroy_kvengine();
    return init_kvengine();
}

// ./kvstore <port> <network> <role> <master_ip> <master_port>
// network: 1(reactor) 2(NtyCo) 3(proactor)
// role: 0(Master) 1(Replica)
// ./kvstore 9999 0 0
// ./kvstore 2000 0 1 39.97.42.225 9999
int main(int argc, char* argv[]) {

    if (argc < 4) {
        printf("Usage:\n"
               "  Master : %s <port> <network> 0\n"
               "  Replica: %s <port> <network> 1 <master_ip> <master_port>\n",
               argv[0], argv[0]);

        return -1;
    }

    unsigned short port = atoi(argv[1]);

    int select_network_architecture = atoi(argv[2]);

    int role = atoi(argv[3]);

    /*
     * 初始化 KV Engine
     */
    init_kvengine();

    /*
     * 初始化复制模块
     */
    if (role == 0) {

        kvs_replication_init(KVS_ROLE_MASTER);

    } else {

        kvs_replication_init(KVS_ROLE_REPLICA);
    }

#if AOF_ENABLE
    // 初始化 AOF
    if (kvs_aof_init("../data/append.aof") != 0) {
        fprintf(stderr, "Failed to initialize AOF\n");

        destroy_kvengine();
        return -1;
    }

#endif

    /*
     * Replica 连接 Master
     */
    if (role == KVS_ROLE_REPLICA) {

        if (argc != 6) {

            fprintf(stderr, "Replica requires master ip and port\n");

            return -1;
        }

        const char* master_ip = argv[4];

        int master_port = atoi(argv[5]);

        int fd = kvs_replication_connect_master(master_ip, master_port);

        if (fd < 0) {

            fprintf(stderr, "Failed to connect master\n");

            return -1;
        }

        if (kvs_replication_start() != 0) {
            fprintf(stderr, "Failed to start replication\n");
            return -1;
        }
    }

    /*
     * 启动网络服务
     */
    switch (select_network_architecture) { //
    case NETWORK_REACTOR: {
        printf("**********USE reactor**********\n");
        reactor_start(port, kvs_protocol);
        break;
    }

    case NETWORK_NTYCO: {
        printf("**********USE NtyCo**********\n");
        ntyco_start(port, kvs_protocol);
        break;
    }

    case NETWORK_PROACTOR: {
        printf("**********USE proactor**********\n");
        proactor_start(port, kvs_protocol);
        break;
    }

    default: {
        printf("no such NETWORK ARCHITECTURE");
        break;
    }
    }
#if AOF_ENABLE
    kvs_aof_close();
#endif
    kvs_replication_destroy();
    destroy_kvengine();
}
