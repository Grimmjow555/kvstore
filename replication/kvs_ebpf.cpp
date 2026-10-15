// master 侧的采集挂载点（重设计阶段 3）。
//
// 采集本体（内核 BPF 程序、ringbuf 消费、转发）已经搬到独立的 ebpf-agent 进程，
// 见 replication/kvs_ebpf_agent.cpp 与 tools/ebpf_agent.cpp。本文件只做两件事：
//
//   1) 提供 uprobe 挂载点 kvs_ebpf_notify_write()：真实模式函数体不做采集，
//      参数由挂在内核里的程序通过 pt_regs 读取；sim 模式把记录写进共享内存环。
//   2) 维护全局自增序号，保证写命令与 SYNC/SYNC FINISHED 控制记录落在同一条
//      有序流里（设计 §7.1 的正确性基础）。
//
// 注意：调用点都在存储锁内（见 kvs_filter_protocol 与 kvs_sync.cpp），序号
// 因此在锁内单调递增，采集流顺序 == 提交顺序。
#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_ebpf_ring.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

// 运行期采集模式：默认 uprobe；无 CAP_BPF 时配置为 sim。
static int g_capture_mode = KVS_EBPF_MODE_UPROBE;
static int g_capture_active = 0;
// 是否允许产出记录：ebpf-agent 注册后才置 1（见 kvs_sync_accept_ebpf_handshake）。
static int g_capture_enabled = 0;

// 全局采集序号：写命令与控制命令共用，保证同一条有序流。
static unsigned int g_event_seq = 0;

// sim 模式下由 master 创建、agent 打开的共享内存环路径。
static char g_ring_path[512] = {0};

int kvs_ebpf_capture_init(const char* ring_path, int mode) {
    g_capture_mode = (mode == KVS_EBPF_MODE_SIM) ? KVS_EBPF_MODE_SIM : KVS_EBPF_MODE_UPROBE;
    __atomic_store_n(&g_event_seq, 0u, __ATOMIC_RELAXED);
    g_capture_active = 0;
    g_capture_enabled = 0;
    g_ring_path[0] = '\0';

    if (g_capture_mode == KVS_EBPF_MODE_SIM) {
        if (ring_path == NULL || ring_path[0] == '\0') {
            kvs_log(KVS_LOG_ERROR, "[EBPF] sim mode requires a capture ring path");
            return -1;
        }
        if (kvs_ebpf_ring_create(ring_path) != 0) {
            kvs_log(KVS_LOG_ERROR, "[EBPF] create capture ring %s failed", ring_path);
            return -1;
        }
        snprintf(g_ring_path, sizeof(g_ring_path), "%s", ring_path);
    }

    g_capture_active = 1;
    return 0;
}

void kvs_ebpf_capture_shutdown(void) {
    if (g_capture_mode == KVS_EBPF_MODE_SIM) {
        kvs_ebpf_ring_close(); // 创建方负责删除环文件
    }
    g_capture_active = 0;
    g_capture_enabled = 0;
    g_ring_path[0] = '\0';
}

void kvs_ebpf_capture_enable(int enabled) {
    if (enabled) {
        // 注册之前的记录对本次会话没有意义，且会把有界环灌满 → 直接清空。
        if (g_capture_mode == KVS_EBPF_MODE_SIM) {
            kvs_ebpf_ring_reset();
        }
        __atomic_store_n(&g_event_seq, 0u, __ATOMIC_RELAXED);
        __atomic_store_n(&g_capture_enabled, 1, __ATOMIC_RELEASE);
        kvs_log(KVS_LOG_INFO, "[EBPF] capture enabled (agent armed)");
    } else {
        __atomic_store_n(&g_capture_enabled, 0, __ATOMIC_RELEASE);
        kvs_log(KVS_LOG_WARN, "[EBPF] capture disabled (agent gone)");
    }
}

unsigned int kvs_ebpf_next_seq(void) {
    return __atomic_add_fetch(&g_event_seq, 1u, __ATOMIC_RELAXED);
}

// uprobe 挂载点。真实模式下本函数体不做任何采集工作，只有 sim 模式写共享环。
//
// noinline/used 必须保留：内核 uprobe 按文件偏移挂到函数入口，函数一旦被内联
// 或优化掉，agent 就无法解析符号并挂载。签名 (seq, argc, argv) 对应 BPF 程序
// 读取的 pt_regs 中 di/si/dx，不要改动。
__attribute__((noinline, used)) void kvs_ebpf_notify_write(unsigned int seq, int argc,
                                                           char* argv[]) {
    if (__atomic_load_n(&g_capture_active, __ATOMIC_RELAXED) == 0 ||
        __atomic_load_n(&g_capture_enabled, __ATOMIC_RELAXED) == 0) {
        return;
    }
    if (g_capture_mode != KVS_EBPF_MODE_SIM) {
        return; // uprobe 模式：参数由内核里的 BPF 程序读取
    }
    if (!kvs_ebpf_event_fits(argc, argv)) {
        return; // 超长/超参数命令不进采集通道，由调用方另行处理
    }
    (void)kvs_ebpf_ring_push(seq, argc, argv);
}

void kvs_ebpf_notify_command(int argc, char* argv[]) {
    if (argc <= 0 || argv == NULL) {
        return;
    }
    kvs_ebpf_notify_write(kvs_ebpf_next_seq(), argc, argv);
}
