#include "aof.h"
#include "kvs_array.h"
#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_replication.h"
#include "kvs_shutdown.h"
#include "kvs_skiptable.h"
#include "kvs_snapshot.h"
#include "kvstore.h"
#include "memorypool.h"
#include "network.h"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// ==================== 优雅关闭 ====================
//
// 三个网络后端的服务循环都是常驻的，收到 SIGINT/SIGTERM 时如果不做处理，
// 进程会被默认动作直接终止，下面的收尾流程（AOF 落盘 -> 停止复制线程 ->
// 释放引擎 -> 销毁内存池）不会执行。
//
// 信号处理函数必须保持异步信号安全，只允许置位 g_kvs_shutdown；各后端循环
// 用有限超时轮询该标志（信号可能投递到任意线程，不能依赖 EINTR 一定会打断
// 事件循环线程）。
//
// 兜底语义：置位后同时启动 alarm 定时器，收尾若卡在某个子系统内部
// （例如第三方 RDMA 库的阻塞调用）就强制退出；再收到一次停止信号同样
// 立即强制退出，避免出现“按了 Ctrl+C 却永远停不下来”的情况。
// 收尾超时（秒）：可编译期覆盖，便于测试
#ifndef KVS_SHUTDOWN_GRACE_SEC
#define KVS_SHUTDOWN_GRACE_SEC 10
#endif

volatile sig_atomic_t g_kvs_shutdown = 0;

// 信号处理函数里只能调用异步信号安全的接口：write + _exit 满足，
// 日志、锁、内存分配都不允许出现在这里。
#define KVS_SHUTDOWN_FORCE_EXIT(msg_literal)                                                       \
    do {                                                                                           \
        static const char kvs_force_exit_msg[] = msg_literal;                                      \
        ssize_t ignored =                                                                          \
            write(STDERR_FILENO, kvs_force_exit_msg, sizeof(kvs_force_exit_msg) - 1);              \
        (void)ignored;                                                                             \
        _exit(0);                                                                                  \
    } while (0)

static void kvs_shutdown_signal_handler(int signo) {
    (void)signo;

    if (g_kvs_shutdown) {
        // 第二次收到停止信号：不再等收尾，直接退出
        KVS_SHUTDOWN_FORCE_EXIT("\n[kvstore] 再次收到停止信号，跳过剩余收尾直接退出\n");
    }

    g_kvs_shutdown = 1;
    // 兜底定时器：收尾若卡住，到点由 SIGALRM 处理函数强制退出
    alarm(KVS_SHUTDOWN_GRACE_SEC);
}

static void kvs_shutdown_alarm_handler(int signo) {
    (void)signo;
    KVS_SHUTDOWN_FORCE_EXIT("\n[kvstore] 优雅关闭超时，强制退出\n");
}

void kvs_shutdown_install_handlers() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = kvs_shutdown_signal_handler;
    // 不设 SA_RESTART：让阻塞中的系统调用返回 EINTR，尽快回到循环判断标志。
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    // 兜底定时器：SIGALRM 到点直接退出，见 kvs_shutdown_alarm_handler
    struct sigaction sa_alarm;
    memset(&sa_alarm, 0, sizeof(sa_alarm));
    sa_alarm.sa_handler = kvs_shutdown_alarm_handler;
    sigemptyset(&sa_alarm.sa_mask);
    sa_alarm.sa_flags = 0;
    sigaction(SIGALRM, &sa_alarm, nullptr);

    // 客户端中途断开时 send/write 会触发 SIGPIPE，默认动作会直接杀死进程。
    // 各后端的写路径已经按返回 -1 处理断连，这里只需忽略该信号。
    struct sigaction ign;
    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    ign.sa_flags = 0;
    sigaction(SIGPIPE, &ign, nullptr);
}

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

// ==================== 存储引擎全局锁 ====================
//
// 复制模块存在多个访问存储引擎的线程：
//   - Master：事件循环线程处理客户端命令，RDMA 监听线程生成全量快照；
//   - Replica：事件循环线程服务本地客户端，复制回放线程应用 Master 命令。
// 存储引擎本身不加锁，因此统一在这里串行化。
//
// 必须是可重入锁：命令分发内部还会调用 kvs_snapshot_load / kvs_aof_replay，
// 它们又会进入 kvs_filter_protocol。
static pthread_mutex_t g_data_mutex;
static pthread_once_t g_data_mutex_once = PTHREAD_ONCE_INIT;

static void kvs_data_mutex_init() {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&g_data_mutex, &attr);
    pthread_mutexattr_destroy(&attr);
}

void kvs_data_lock() {
    pthread_once(&g_data_mutex_once, kvs_data_mutex_init);
    pthread_mutex_lock(&g_data_mutex);
}

void kvs_data_unlock() {
    pthread_once(&g_data_mutex_once, kvs_data_mutex_init);
    pthread_mutex_unlock(&g_data_mutex);
}

void* kvs_malloc(size_t size) {
#if ENABLE_MEMORYPOOL
    return slab_alloc(size);
#else
    return malloc(size);
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

    // 整个命令（含持久化、复制入队）都在存储锁内完成，保证 Replica 回放
    // 线程与本地客户端线程不会并发修改 rbtree/skiptable 等非线程安全结构。
    kvs_data_lock();

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

    kvs_data_unlock();
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
        char* tmp_resp = (char*)kvs_malloc(remaining);
        if (!tmp_resp)
            break;

        int len = kvs_filter_protocol(argv, argc, tmp_resp, remaining);
        if (len > 0 && len < remaining) {
            memcpy(response + resp_offset, tmp_resp, len);
            resp_offset += len;
        } else if (len >= remaining) {
            // 响应被截断，根据业务可选择断开或返回错误
            // 这里简单处理为返回错误
            kvs_free(tmp_resp);
            for (int i = 0; i < argc; i++)
                kvs_free(argv[i]);
            kvs_free(argv);
            break;
        }
        kvs_free(tmp_resp);

        for (int i = 0; i < argc; i++)
            kvs_free(argv[i]);
        kvs_free(argv);
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

    // 尽早安装信号处理：保证 SIGINT/SIGTERM 只置停止标志，
    // 由网络服务循环退出后统一走下面的收尾流程。
    kvs_shutdown_install_handlers();

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

    if (port <= 0 || port > 65535 || (role != 0 && role != 1) || bind_ip == nullptr ||
        bind_ip[0] == '\0' || bind_ip_valid != 1 ||
        (role == KVS_ROLE_REPLICA && (master_ip == nullptr || master_ip[0] == '\0' ||
                                      master_port <= 0 || master_port > 65535))) {
        printf("Usage:\n"
               "  %s [--config kvstore.conf]\n"
               "     [--bind 0.0.0.0] [--port 9999]\n"
               "     [--role master|replica]\n"
               "     [--master-ip 127.0.0.1] [--master-port 19001]\n"
               "     [--log-level debug|info|warn|error|off]\n"
               "     [--persistence-mode none|rdb|aof|both]\n"
               "     [--network reactor|ntyco|proactor]\n"
               "  Legacy Master : %s <port> 0\n"
               "  Legacy Replica: %s <port> 1 <master_ip> <master_port>\n",
               argv[0], argv[0], argv[0]);
        return -1;
    }

    kvs_log(KVS_LOG_INFO,
            "Configuration: bind=%s port=%d role=%s log_level=%d persistence=rdb:%s,aof:%s "
            "network=%s",
            bind_ip, port, role == 0 ? "master" : "replica", kvs_config_log_level(),
            kvs_config_rdb_enabled() ? "on" : "off", kvs_config_aof_enabled() ? "on" : "off",
            kvs_config_network_name());

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
#if !KVS_ENABLE_EBPF_REALTIME
        kvs_log(KVS_LOG_INFO,
                "eBPF realtime sync disabled, replication uses backlog + sender thread");
#endif
#ifdef KVS_ENABLE_RDMA
        if (kvs_replication_start_rdma_listener((unsigned short)port) != 0) {
            // RDMA 设备不可用时不要阻止服务启动，自动回退到 TCP 全量同步。
            kvs_log(KVS_LOG_WARN, "RDMA listener unavailable, falling back to TCP full sync");
        }
#endif
        if (kvs_config_aof_enabled() && kvs_aof_init("../data/append.aof") != 0) {
            kvs_log(KVS_LOG_ERROR, "AOF init failed");
            return -1;
        }
    } else {
        kvs_replication_init(KVS_ROLE_REPLICA);
        // 位置参数形式（./kvstore <port> 1 <master_ip> <master_port>）只在 main 里
        // 覆盖了局部变量，这里必须把最终地址同步给复制模块。
        kvs_replication_set_master_addr(master_ip, master_port);
    }

    // Replica 连接 Master
    if (role == KVS_ROLE_REPLICA) {
        // 连接、RDMA 全量、握手、断线重连全部交给复制监督线程，
        // 网络服务不必等 Master 就绪即可启动。
        if (kvs_replication_start() != 0) {
            kvs_log(KVS_LOG_ERROR, "Failed to start replication");
            return -1;
        }
        kvs_log(KVS_LOG_INFO, "Replica replication started in background, master=%s:%d", master_ip,
                master_port);
    }

    // 启动网络服务。网络框架由 kvstore.conf 的 network_architecture
    // （或命令行 --network）在运行时选择，三个后端都编在同一个二进制里；
    // 默认值仍来自 include/network.h 的编译期宏。
    switch (kvs_config_network()) {
    case KVS_NETWORK_REACTOR:
        kvs_log(KVS_LOG_INFO, "Starting network architecture: reactor (epoll)");
        reactor_start(bind_ip, (unsigned short)port, kvs_protocol);
        break;
    case KVS_NETWORK_PROACTOR:
        kvs_log(KVS_LOG_INFO, "Starting network architecture: proactor (io_uring)");
        proactor_start(bind_ip, (unsigned short)port, kvs_protocol);
        break;
    case KVS_NETWORK_NTYCO:
    default:
        kvs_log(KVS_LOG_INFO, "Starting network architecture: ntyco (coroutine)");
        ntyco_start(bind_ip, (unsigned short)port, kvs_protocol);
        break;
    }

#if AOF_ENABLE
    kvs_log(KVS_LOG_INFO, "Shutdown requested, flushing AOF and stopping replication");
    kvs_aof_close();
#else
    kvs_log(KVS_LOG_INFO, "Shutdown requested, stopping replication");
#endif
    // 先停止复制线程、释放引擎数据，最后才销毁内存池：
    // slab_dest() 会释放全部 chunk，必须确认没有其它线程还在分配/释放内存。
    kvs_replication_destroy();
    destroy_kvengine();
#if ENABLE_MEMORYPOOL
    slab_dest();
#endif
    kvs_log(KVS_LOG_INFO, "Shutdown finished");
    return 0;
}
