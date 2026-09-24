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

// kvs_malloc / kvs_free 的底层实现。该选项在进程启动时确定，运行期间不可切换：
// 同一块内存必须由分配它的分配器释放，运行中替换会导致跨分配器释放。
typedef enum {
    KVS_ALLOC_MALLOC = 0,     // 系统 malloc/free，不使用内存池
    KVS_ALLOC_JEMALLOC = 1,   // jemalloc，构建时链接 libjemalloc 后使用
    KVS_ALLOC_MEMORYPOOL = 2  // 项目内置的 slab 内存池
} kvs_allocator_t;

// 网络框架（网络架构）选择。三个后端都会编入同一个二进制，进程启动时按配置
// 选择其中一个运行，运行期间不能切换（事件循环一旦启动即接管线程）。
typedef enum {
    KVS_NETWORK_REACTOR = 0,  // reactor：epoll 事件循环
    KVS_NETWORK_NTYCO = 1,    // NtyCo：协程框架
    KVS_NETWORK_PROACTOR = 2  // proactor：io_uring
} kvs_network_t;

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
    int allocator;       // kvs_allocator_t 对应的整数
    int network;         // kvs_network_t 对应的整数
} kvs_config_t;

// 恢复默认配置。默认监听 0.0.0.0:9999，角色为 Master，
// 日志级别 INFO，RDB 与 AOF 默认都关闭，内存分配方式为内置 slab 内存池，
// 网络框架沿用 include/network.h 中的编译期选择。
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
//   --memory-allocator <malloc|system|jemalloc|memorypool|slab>
//   --network <reactor|ntyco|proactor>
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
int kvs_config_allocator(void);

// 返回当前内存分配方式的稳定名称：malloc / jemalloc / memorypool。
const char* kvs_config_allocator_name(void);

// 当前网络框架，取值见 kvs_network_t。
int kvs_config_network(void);

// 返回当前网络框架的稳定名称：reactor / ntyco / proactor。
const char* kvs_config_network_name(void);

// 按当前日志级别输出一条日志。低于当前级别的日志会被过滤。
void kvs_log(int level, const char* fmt, ...);

#ifdef __cplusplus
}
#endif
