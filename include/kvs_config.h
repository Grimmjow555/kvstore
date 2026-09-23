#pragma once

#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

// 日志级别，数值越小越详细。KVS_LOG_OFF 用于关闭所有 kvs_log 输出。
typedef enum {
    KVS_LOG_DEBUG = 0,
    KVS_LOG_INFO = 1,
    KVS_LOG_WARN = 2,
    KVS_LOG_ERROR = 3,
    KVS_LOG_OFF = 4
} kvs_log_level_t;

// 服务运行所需的全部配置。字段中的 role 沿用复制模块约定：
// 0 表示 Master，1 表示 Replica。
typedef struct {
    char bind_ip[64];    // 服务监听 IP
    int port;            // 服务监听端口
    int log_level;       // kvs_log_level_t 对应的整数
    int role;            // 0=Master, 1=Replica
    char master_ip[64];  // Replica 连接的主节点 IP
    int master_port;     // Replica 连接的主节点端口
    int rdb_enabled;     // 是否启用 RDB
    int aof_enabled;     // 是否启用 AOF
} kvs_config_t;

// 恢复默认配置。默认监听 0.0.0.0:9999，角色为 Master，
// 日志级别 INFO，RDB 与 AOF 默认都关闭。
void kvs_config_set_defaults(void);

// 从指定 key=value 或 key value 配置文件中读取并覆盖当前配置。
// 文件不存在时返回 -1；成功返回 0。
int kvs_config_load(const char* filename);

// 尝试按 ./kvstore.conf、../kvstore.conf 的顺序加载默认配置。
// 两个文件都不存在时返回 0（继续使用当前默认值）；文件存在但解析失败时返回 -1。
int kvs_config_load_default(void);

// 解析并移除命令行配置开关。
//
// 支持的开关：
//   --config <file>
//   --bind <ip>
//   --port <port>
//   --log-level <debug|info|warn|error|off>
//   --role <master|replica|0|1>
//   --master-ip <ip>
//   --master-port <port>
//   --persistence-mode <none|rdb|aof|both>
//   --rdb <on|off>
//   --aof <on|off>
//
// 若指定 --config，先读取该文件，再用其他命令行开关覆盖；否则先尝试加载默认配置文件。
// 解析成功后会把这些选项从 argv 中移除，便于继续按旧式位置参数处理。
// 返回 0 表示成功，非 0 表示配置错误。
int kvs_config_parse_switches(int* argc, char*** argv);

const char* kvs_config_bind_ip(void);
int kvs_config_port(void);
int kvs_config_log_level(void);
int kvs_config_role(void);
const char* kvs_config_master_ip(void);
int kvs_config_master_port(void);
int kvs_config_rdb_enabled(void);
int kvs_config_aof_enabled(void);

// 按当前日志级别输出一条日志。低于当前级别的日志会被过滤。
void kvs_log(int level, const char* fmt, ...);

#ifdef __cplusplus
}
#endif
