#pragma once

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

// 网络框架（网络架构）选择。三个后端都会编入同一个二进制，进程启动时按配置
// 选择其中一个运行，运行期间不能切换（事件循环一旦启动即接管线程）。
typedef enum {
    KVS_NETWORK_REACTOR = 0,  // reactor：epoll 事件循环
    KVS_NETWORK_NTYCO = 1,    // NtyCo：协程框架
    KVS_NETWORK_PROACTOR = 2  // proactor：io_uring
} kvs_network_t;

// 恢复默认配置。默认监听 0.0.0.0:9999，角色为 Master，
// 日志级别 INFO，持久化关闭；RDB 与 AOF 只能择一开启。
void kvs_config_set_defaults(void);

// 从指定 key=value 或 key value 配置文件中读取并覆盖当前配置。
// 文件不存在时返回 -1；成功返回 0。
int kvs_config_load(const char* filename);

// 尝试按 ./kvstore.conf、../kvstore.conf 的顺序加载默认配置。
// 两个文件都不存在时返回 0（继续使用当前默认值）；文件存在但解析失败时返回 -1。
int kvs_config_load_default(void);

// 解析命令行配置开关。启动参数只接受命名开关，不接受位置参数。
//
// 支持的开关：
//   --config <file>
//   --bind <ip>
//   --port <port>
//   --log-level <debug|info|warn|error|off>
//   --role <master|replica|0|1>
//   --master-ip <ip>
//   --master-port <port>
//   --agent-port <port>
//   --rdma-advertise-host <ip>
//   --max-replicas <1..16>
//   --sync-session-timeout-ms <0|1000..86400000>
//   --persistence-mode <none|rdb|aof>
//   --rdb <on|off>
//   --aof <on|off>
//   --network <reactor|ntyco|proactor>
//
// 若指定 --config，先读取该文件，再用其他命令行开关覆盖；否则先尝试加载默认配置文件。
// 出现任何非开关参数时返回错误。
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

const char* kvs_config_rdma_client_bin(void);
const char* kvs_config_rdma_server_bin(void);
const char* kvs_config_rdma_advertise_host(void);
int kvs_config_rdma_port(void);
int kvs_config_agent_port(void);
int kvs_config_max_replicas(void);
int kvs_config_sync_session_timeout_ms(void);
int kvs_config_rdma_transfer_timeout_ms(void);
int kvs_config_sync_wait_timeout_ms(void);
int kvs_config_ebpf_capture_mode(void);
const char* kvs_config_ebpf_ring_file(void);

// 当前网络框架，取值见 kvs_network_t。
int kvs_config_network(void);

// 返回当前网络框架的稳定名称：reactor / ntyco / proactor。
const char* kvs_config_network_name(void);

// 按当前日志级别输出一条日志。低于当前级别的日志会被过滤。
void kvs_log(int level, const char* fmt, ...);

#ifdef __cplusplus
}
#endif
