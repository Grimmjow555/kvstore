#pragma once

#include "kvs_ebpf.h"

#ifdef __cplusplus
extern "C" {
#endif

// ==================== ebpf-agent 侧采集（重设计阶段 3） ====================
//
// 采集记录回调：agent 状态机实现。argv 指向的缓冲由采集侧复用，回调返回后即失效，
// 需要在回调内完成编码/入队。
typedef void (*kvs_ebpf_record_cb_t)(int argc, char* argv[]);

// 需要重同步时的回调：采集丢事件、记录非法、看门狗超时等。agent 据此断开增量通道，
// 让 Replica 重新走一次全量同步（设计 §9）。
typedef void (*kvs_ebpf_resync_cb_t)(const char* reason);

// 启动采集：mode 取 KVS_EBPF_MODE_UPROBE / KVS_EBPF_MODE_SIM。
//   uprobe：需要 master_pid 与 master_binary（解析挂载点符号偏移）；
//   sim：需要 ring_path（master 已创建好的共享内存环）。
// 成功返回 0；任何一步失败返回 -1（调用方按「采集强制」处理）。
int kvs_ebpf_agent_capture_start(int mode, const char* ring_path, int master_pid,
                                 const char* master_binary, kvs_ebpf_record_cb_t on_record,
                                 kvs_ebpf_resync_cb_t on_resync);

void kvs_ebpf_agent_capture_stop(void);

// Master 重启后重新建立采集通道：
//   * sim：环文件被新 Master 重建，这里重开 Ring（停消费线程 -> 重开 -> 重启）；
//   * uprobe：探针绑在旧进程上已失效，需要新的 master pid 才能重挂，返回 -1。
// 返回 0 表示已恢复；调用方据此决定是继续还是提示重启 agent。
int kvs_ebpf_agent_capture_reopen(const char* ring_path);

#ifdef __cplusplus
}
#endif
