// 复制模块在 kvstore 进程内剩下的最小一层（重设计阶段 4）。
//
// 采集、搬运、投递都已经拆成独立进程，本文件只保留四个调用点：
//   * 写路径 -> 有序采集流（kvs_replication_append）
//   * 网络层握手 -> EBPF / REPL_INC（kvs_replication_accept_handshake）
//   * 断连清理（kvs_replication_remove_replica）
//   * 回放线程标记（is/set_replaying）
// 以及 RDB/AOF LOAD 之后触发的重同步（kvs_replication_resync）。
#include "kvs_replication.h"

#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_sync.h"

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
    // 提交顺序（设计 §7.1 的正确性基础）。缓存与转发都由 ebpf-agent 负责。
    kvs_ebpf_notify_command(argc, argv);
    return 0;
}

int kvs_replication_accept_handshake(int fd, const char* data, int length) {
    // Master 侧的 EBPF（ebpf-agent 注册）与 Replica 侧的 REPL_INC（增量通道）：
    // 网络层对返回 1 的连接统一回 +OK，三个后端因此不需要各自识别 token。
    if (kvs_sync_accept_ebpf_handshake(fd, data, length) == 1) {
        return 1;
    }
    if (kvs_sync_accept_repl_inc_handshake(fd, data, length) == 1) {
        return 1;
    }
    return 0;
}

void kvs_replication_finish_handshake(int fd) { (void)fd; }

void kvs_replication_remove_replica(int fd) {
    if (fd < 0) {
        return;
    }
    // 连接断开时把该 fd 上的复制/同步会话摘掉，否则 fd 复用后旧映射仍然有效。
    kvs_sync_forget_fd(fd);
}

int kvs_replication_resync() {
    if (g_role != KVS_ROLE_MASTER) {
        return 0;
    }

    // 新架构下没有「服务端主动推快照」的通道：Master 只能把「数据已变」这件事
    // 写进有序采集流，由 agent 断开增量通道，Replica 侧观察到断开后重新走
    // SYNC（设计 §8.2 步骤 7）。
    char* token[1] = {(char*)"INVALIDATE"};
    kvs_ebpf_notify_command(1, token);
    kvs_log(KVS_LOG_INFO, "[REPLICATION] invalidate recorded; online replica will re-sync");
    return 0;
}

int kvs_replication_is_replaying() { return replication_replaying; }

void kvs_replication_set_replaying(int value) { replication_replaying = value; }
