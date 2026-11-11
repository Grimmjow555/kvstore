// kvstore 进程内的复制出入口（重设计阶段 4/5）。
//
// 采集、搬运、投递都已经拆成独立进程，本文件是 kvstore 侧唯一的一层：
//
//   * 采集挂载点 kvs_ebpf_notify_write()：真实模式函数体不做采集（参数由挂在
//     内核里的 uprobe 程序读取），sim 模式把记录写进共享内存环；
//   * 全局采集序号与 kvs_ebpf_notify_command()：保证写命令与 SYNC/SYNC FINISHED
//     控制记录落在同一条有序流里（设计 §7.1 的正确性基础）；
//   * 写路径入口 kvs_replication_append()：只把命令交给上面这条流；
//   * 网络层 EBPF 注册路由、断连清理、回放线程标记。
//
// 之所以把「采集挂载点」和「复制入口」放在同一个文件：它们都是 kvstore 进程内、
// 写路径上的胶水代码，调用链就是
//   kvs_replication_append -> kvs_ebpf_notify_command -> kvs_ebpf_notify_write，
// 放在一起比拆成两个文件更容易看清顺序与锁的关系。
//
// 注意：这些调用点都在存储锁内（见 kvs_filter_protocol 与 sync/master_sync.cpp），
// 序号因此在锁内单调递增，采集流顺序 == 提交顺序。
#include "kvs_replication.h"

#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_ebpf_ring.h"

#include <stdio.h>

// ==================== 采集挂载点与有序流 ====================

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

// ==================== 复制接口（kvstore 进程内的薄层） ====================

static kvs_role_t g_role = KVS_ROLE_MASTER;

// 复制回放标记必须是线程局部量：Replica 的回放线程与本地客户端线程共享同一份
// 全局标志时，回放窗口内本地客户端的写会被误判成回放而不落 AOF。
static __thread int replication_replaying = 0;

int kvs_replication_init(kvs_role_t role) {
    g_role = role;
    return 0;
}

int kvs_replication_append(int argc, char* argv[]) {
    if (g_role != KVS_ROLE_MASTER) {
        return 0;
    }
    if (argc <= 0 || argv == NULL) {
        return -1;
    }

    // 写命令只交给有序采集流：调用点在 kvs_filter_protocol 的写分支内，天然持
    // 存储锁，因此这里分配的序号与 SYNC / SYNC FINISHED 记录的先后关系就是真实
    // 提交顺序。缓存与转发都由 ebpf-agent 负责。
    kvs_ebpf_notify_command(argc, argv);
    return 0;
}

int kvs_replication_accept_handshake(int fd, const char* data, int length) {
    // Master 侧的 EBPF 注册。Replica 的增量通道是主动外连，不经过网络后端。
    if (kvs_sync_accept_ebpf_handshake(fd, data, length) == 1) {
        return 1;
    }
    return 0;
}

void kvs_replication_remove_replica(int fd) {
    if (fd < 0) {
        return;
    }
    // 连接断开时把该 fd 上的复制/同步会话摘掉，否则 fd 复用后旧映射仍然有效。
    kvs_sync_forget_fd(fd);
}

int kvs_replication_is_replaying() { return replication_replaying; }

void kvs_replication_set_replaying(int value) { replication_replaying = value; }
