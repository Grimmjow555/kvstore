#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// 持久化配置的最小状态结构。
// 当前阶段只落地两个独立开关，后续配置文件/命令行解析可复用该结构。
typedef struct {
    int rdb_enabled; // 是否启用 RDB 全量持久化
    int aof_enabled; // 是否启用 AOF 增量持久化
} kvs_persistence_config_t;

// 恢复默认配置：当前默认保持项目原有行为，即 RDB 与 AOF 都启用。
void kvs_config_set_defaults(void);

// 从命令行参数中解析 --rdb on|off、--aof on|off。
// 解析成功后会把已识别的开关参数从 argv 中移除，便于继续按位置参数处理。
// 返回 0 表示成功，非 0 表示参数格式错误。
int kvs_config_parse_switches(int* argc, char*** argv);

int kvs_config_rdb_enabled(void);
int kvs_config_aof_enabled(void);

#ifdef __cplusplus
}
#endif
