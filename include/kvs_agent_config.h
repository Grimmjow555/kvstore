#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char master_ip[64];
    int master_port;
    char listen_ip[64];
    int listen_port;
    int mode; // 0 = uprobe，1 = sim
    char ring_path[512];
    int master_pid;
    char master_binary[512];
    long cache_max_bytes;
    long cache_max_records;
} kvs_agent_config_t;

void kvs_agent_config_init(kvs_agent_config_t* config);

// 成功返回 0；文件无法读取或字段非法时返回 -1。
int kvs_agent_config_load_file(kvs_agent_config_t* config, const char* path);

// 依次尝试 ./ebpf-agent.conf、../ebpf-agent.conf。
// 找到并成功加载返回 1，没有默认文件返回 0，文件存在但非法返回 -1。
int kvs_agent_config_load_default(kvs_agent_config_t* config);

// 只扫描 --config，不应用其他命令行开关。
int kvs_agent_config_select_path(int argc, char* const argv[], char* path, size_t path_size,
                                 int* has_explicit_path);

// 应用命令行开关。成功返回 0，参数错误返回 -1，请求帮助返回 1。
int kvs_agent_config_parse_args(kvs_agent_config_t* config, int argc, char* const argv[]);

// 检查启动所需的地址以及当前采集模式所需参数。
int kvs_agent_config_validate(const kvs_agent_config_t* config);

#ifdef __cplusplus
}
#endif
