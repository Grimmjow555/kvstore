// 主从全量同步控制流（重设计阶段 1 + 阶段 2）。
//
// 设计与阶段划分见 docs/replication-redesign.md。
//   * Master：SYNC -> 序列化 -> 写 <sync_tmp_file> -> 派生 rdma-client -> +OK；
//             SYNC FINISHED -> 回收 rdma-client -> 删除 .tmp -> +OK；
//   * Replica：派生 rdma-server -> 连 Master 发 SYNC -> 等 RDMA 文件落盘 ->
//             加载 -> SYNC FINISHED -> 回收 rdma-server。
//
// SYNC 的序列化放在存储锁内，保证快照是「收到 SYNC 那一刻」的一致状态；
// 参与 RDMA 搬运的是两个独立进程（rdma-client / rdma-server），协议与约束见
// include/kvs_rdma_transfer.h。
#include "kvs_sync.h"

#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_io_uring.h"
#include "kvs_netutil.h"
#include "kvs_repl_proto.h"
#include "kvs_replication.h"
#include "kvs_snapshot.h"
#include "kvstore.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char** environ;

// 增量回放复用命令分发入口（同 kvs_replication.cpp 的做法）。
extern int kvs_protocol(char* msg, int length, char* response, int response_size);

// 控制通道沿用外层「4 字节网络序长度 + 负载」帧格式。
// 连接 Master 的超时。
#define KVS_SYNC_CONNECT_TIMEOUT_MS 3000
// 收发一帧的超时（SYNC 期间 Master 要序列化 + 落盘，给足余量）。
#define KVS_SYNC_IO_TIMEOUT_MS 120000
// Replica 等待 rdma-server 就绪（端口开始监听）的最长时间。
#define KVS_SYNC_RDMA_READY_TIMEOUT_MS 10000
// 全量同步完成后等待 ebpf-agent 接入增量通道的最长时间。超时说明 agent 侧
// 这一轮同步失效（例如缓存溢出后主动放弃），需要重新全量同步。
#define KVS_SYNC_INCREMENT_WAIT_MS 10000
// 失败后的重试退避区间。
#define KVS_SYNC_BACKOFF_MIN_MS 200
#define KVS_SYNC_BACKOFF_MAX_MS 5000

static kvs_role_t g_sync_role = KVS_ROLE_MASTER;

// Master 侧：是否有一次全量同步正在进行（用于拒绝重复 SYNC）。
static volatile int g_sync_in_progress = 0;
// Master 派生的 rdma-client 进程；-1 表示当前没有。
static pid_t g_rdma_client_pid = -1;

// Replica 侧状态。
static pthread_t g_sync_tid;
static volatile int g_sync_running = 0;
static char g_master_ip[INET_ADDRSTRLEN] = {0};
static int g_master_port = 0;
// Replica 派生的 rdma-server 进程；-1 表示当前没有。
static pid_t g_rdma_server_pid = -1;

// 阶段 3：Master 侧已注册的 ebpf-agent 连接；-1 表示未武装。
static int g_ebpf_agent_fd = -1;
// 阶段 3：Replica 侧已建立的增量通道连接；-1 表示未接入。
static int g_repl_inc_fd = -1;

// 增量通道状态变化通知：Replica 全量同步完成后靠它等待接入/断开。
static pthread_mutex_t g_inc_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_inc_cond = PTHREAD_COND_INITIALIZER;

// ==================== 通用工具 ====================

static unsigned long long sync_now_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (unsigned long long)ts.tv_sec * 1000ull + (unsigned long long)ts.tv_nsec / 1000000ull;
}

static void sync_sleep_ms(int ms) {
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

// 可被打断的睡眠：收到停止请求时立即返回，避免退出要等整个退避周期。
static void sync_sleep_interruptible(int ms, volatile int* running) {
    while (ms > 0 && *running) {
        int step = ms > 50 ? 50 : ms;
        sync_sleep_ms(step);
        ms -= step;
    }
}

// 发送一条单 token 的 RESP 命令（例如 SYNC / SYNC FINISHED）。
static int sync_send_command(int fd, const char* cmd) {
    char payload[64];
    int n = snprintf(payload, sizeof(payload), "*1\r\n$%zu\r\n%s\r\n", strlen(cmd), cmd);
    if (n <= 0 || (size_t)n >= sizeof(payload)) {
        return -1;
    }
    return kvs_net_send_frame(fd, payload, (size_t)n, KVS_SYNC_IO_TIMEOUT_MS);
}

// ==================== 子进程管理 ====================

// 派生一个辅助进程（rdma-client / rdma-server），把它的 stdout/stderr 重定向到
// log_path，避免与 kvstore 自己的日志混在一起。成功返回 0 并写出 pid。
static int sync_spawn_child(const char* bin, char* const argv[], const char* log_path,
                            pid_t* out_pid) {
    if (bin == NULL || bin[0] == '\0' || out_pid == NULL) {
        return -1;
    }

    posix_spawn_file_actions_t actions;
    if (posix_spawn_file_actions_init(&actions) != 0) {
        return -1;
    }
    if (log_path != NULL && log_path[0] != '\0') {
        posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log_path,
                                         O_WRONLY | O_CREAT | O_TRUNC, 0644);
        posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, log_path,
                                         O_WRONLY | O_CREAT | O_TRUNC, 0644);
    }

    pid_t pid = -1;
    int rc = posix_spawn(&pid, bin, &actions, NULL, argv, environ);
    posix_spawn_file_actions_destroy(&actions);

    if (rc != 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] spawn %s failed: %s", bin, strerror(rc));
        return -1;
    }
    *out_pid = pid;
    return 0;
}

// 等待子进程退出：1 = 已退出且退出码为 0；0 = 超时仍在运行；-1 = 已退出但失败。
static int sync_wait_child(pid_t pid, int timeout_ms) {
    unsigned long long deadline = sync_now_ms() + (unsigned long long)timeout_ms;
    while (1) {
        int status = 0;
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                return 1;
            }
            kvs_log(KVS_LOG_WARN, "[SYNC] child pid=%d exited abnormally (status=%d)", (int)pid,
                    status);
            return -1;
        }
        if (r < 0) {
            return -1;
        }
        if (sync_now_ms() >= deadline) {
            return 0;
        }
        sync_sleep_ms(20);
    }
}

// 终止并回收子进程（幂等）。*pid 置 -1。
static void sync_kill_child(pid_t* pid) {
    if (pid == NULL || *pid <= 0) {
        return;
    }
    pid_t p = *pid;
    *pid = -1;

    kill(p, SIGTERM);
    int status = 0;
    for (int i = 0; i < 40; ++i) {
        if (waitpid(p, &status, WNOHANG) == p) {
            return;
        }
        sync_sleep_ms(50); // 最多等 2s
    }
    kvs_log(KVS_LOG_WARN, "[SYNC] child pid=%d did not exit, sending SIGKILL", (int)p);
    kill(p, SIGKILL);
    waitpid(p, &status, 0);
}

// ==================== Master：SYNC / SYNC FINISHED ====================

// 把快照缓冲原子地写到 path：先写 path.partial（io_uring + fsync），再 rename。
// 这样读者只在 <path> 上看到完整文件，不会读到写了一半的内容。
//
// 说明：rdma-server 侧有一份等价实现（replication/kvs_rdma_transfer.cpp 的
// xfer_write_file_atomic）。两者刻意没有合并：那边是独立的 rdma-* 进程，不想为了
// 一次落盘引入 liburing 依赖，因此用裸 write + fsync；这里是 kvstore 进程，走
// io_uring 与 RDB SAVE 的写盘方式保持一致。改动其一时请同步核对另一处。
static int sync_write_tmp_atomic(const char* path, const void* data, size_t len) {
    char partial[512];
    int n = snprintf(partial, sizeof(partial), "%s.partial", path);
    if (n <= 0 || (size_t)n >= sizeof(partial)) {
        return -1;
    }

    kvs_io_uring_file_t* fp = kvs_io_uring_open(partial, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fp == NULL) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] open %s failed: %s", partial, strerror(errno));
        return -1;
    }
    int ret = kvs_io_uring_write_and_fsync(fp, data, len);
    kvs_io_uring_close(fp);
    if (ret != 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] write %s failed", partial);
        unlink(partial);
        return -1;
    }

    if (rename(partial, path) != 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] rename %s -> %s failed: %s", partial, path,
                strerror(errno));
        unlink(partial);
        return -1;
    }
    return 0;
}

// 派生 rdma-client 把 .tmp 搬到 Replica。log 文件与 .tmp 同目录，便于排查。
static int sync_spawn_rdma_client(const char* tmp_path) {
    char port_str[16];
    char timeout_str[16];
    char log_path[512];
    snprintf(port_str, sizeof(port_str), "%d", kvs_config_rdma_port());
    snprintf(timeout_str, sizeof(timeout_str), "%d", kvs_config_rdma_transfer_timeout_ms());
    snprintf(log_path, sizeof(log_path), "%s.rdma-client.log", tmp_path);

    char* argv[] = {(char*)kvs_config_rdma_client_bin(),
                    (char*)"--file",
                    (char*)tmp_path,
                    (char*)"--peer",
                    (char*)kvs_config_replica_host(),
                    (char*)"--port",
                    port_str,
                    (char*)"--timeout-ms",
                    timeout_str,
                    NULL};

    if (sync_spawn_child(kvs_config_rdma_client_bin(), argv, log_path, &g_rdma_client_pid) != 0) {
        return -1;
    }
    kvs_log(KVS_LOG_INFO, "[SYNC] rdma-client spawned (pid=%d) -> %s:%d, log=%s",
            (int)g_rdma_client_pid, kvs_config_replica_host(), kvs_config_rdma_port(), log_path);
    return 0;
}

int kvs_sync_handle_sync(char* response, int response_size) {
    if (g_sync_role != KVS_ROLE_MASTER) {
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    if (g_sync_in_progress) {
        // 上一次同步没能走到 SYNC FINISHED（例如 rdma-client 传输失败、Replica
        // 中途退出）。单从机设计下，再次收到 SYNC 就说明对端在重试：回收上一次的
        // 客户端与残留文件，用这一次重新开始，避免「卡住后再也同步不了」。
        kvs_log(KVS_LOG_WARN,
                "[SYNC] previous sync did not finish; superseding it with a new attempt");
        sync_kill_child(&g_rdma_client_pid);
        unlink(kvs_config_sync_tmp_file());
        g_sync_in_progress = 0;
    }

    // eBPF 强制（设计 D3）：采集进程没注册就不做全量同步。
    // 否则「全量窗口内的新写入」没有采集方，会静默丢失。
    if (!kvs_sync_ebpf_armed()) {
        kvs_log(KVS_LOG_WARN, "[SYNC] rejected: ebpf-agent is not registered");
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    // (1) 先把「同步开始」写进有序采集流：agent 读到这条记录后进入 CACHING，
    //     之后落地的写命令都会被缓存。该调用在存储锁内、且在序列化之前，
    //     因此缓存窗口必然覆盖整个快照窗口（设计 §7.1）。
    char* sync_token[1] = {(char*)"SYNC"};
    kvs_ebpf_notify_command(1, sync_token);

    char* blob = NULL;
    size_t blob_len = 0;
    if (kvs_snapshot_serialize(&blob, &blob_len) != 0 || blob == NULL) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] snapshot serialize failed");
        free(blob);
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    const char* path = kvs_config_sync_tmp_file();
    int rc = sync_write_tmp_atomic(path, blob, blob_len);
    free(blob);
    if (rc != 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] write snapshot %s failed", path);
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    kvs_log(KVS_LOG_INFO, "[SYNC] snapshot written: %s (%zu bytes)", path, blob_len);

    // 阶段 2：全量搬运交给独立进程。派生失败说明这次同步无法完成，直接回 -ERROR，
    // 让 Replica 重试，避免留下一个永远等不到文件的半成品状态。
    if (sync_spawn_rdma_client(path) != 0) {
        unlink(path);
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    g_sync_in_progress = 1;
    return snprintf(response, response_size, "+OK\r\n");
}

int kvs_sync_handle_sync_finished(char* response, int response_size) {
    if (g_sync_role != KVS_ROLE_MASTER) {
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    // 先把「同步结束」写进同一条有序流：agent 读到后会冲刷缓存并转入实时转发。
    // Replica 只有在快照已加载完之后才会发这条命令，因此缓存命令必然落在
    // 快照之后，顺序正确。
    char* done_token[1] = {(char*)"SYNC FINISHED"};
    kvs_ebpf_notify_command(1, done_token);

    // 回收 rdma-client：正常情况下它已经发完并退出，这里只是收尸。
    sync_kill_child(&g_rdma_client_pid);

    const char* path = kvs_config_sync_tmp_file();
    if (unlink(path) != 0 && errno != ENOENT) {
        kvs_log(KVS_LOG_WARN, "[SYNC] unlink %s failed: %s", path, strerror(errno));
    }
    g_sync_in_progress = 0;
    kvs_log(KVS_LOG_INFO, "[SYNC] sync finished, rdma-client released and %s removed", path);
    return snprintf(response, response_size, "+OK\r\n");
}

// ==================== Replica：同步驱动 ====================

// ==================== 采集进程注册 / 增量通道（阶段 3） ====================

static int sync_handshake_matches(const char* data, int length, const char* token) {
    if (data == NULL || token == NULL || length <= 0) {
        return 0;
    }
    size_t token_len = strlen(token);
    return (size_t)length == token_len && memcmp(data, token, token_len) == 0;
}

int kvs_sync_ebpf_armed(void) { return g_ebpf_agent_fd >= 0; }

int kvs_sync_accept_ebpf_handshake(int fd, const char* data, int length) {
    if (g_sync_role != KVS_ROLE_MASTER || fd < 0) {
        return 0;
    }
    if (!sync_handshake_matches(data, length, KVS_REPL_HANDSHAKE_EBPF)) {
        return 0;
    }
    if (g_ebpf_agent_fd >= 0 && g_ebpf_agent_fd != fd) {
        // 单从机/单采集进程设计：后到的连接顶替前一个，前一个失联通常意味着
        // agent 重启过，这里以最新注册为准。
        kvs_log(KVS_LOG_WARN, "[SYNC] ebpf-agent re-registered: fd %d -> %d", g_ebpf_agent_fd, fd);
    }
    __atomic_store_n(&g_ebpf_agent_fd, fd, __ATOMIC_RELEASE);
    // 注册之前的记录对本次同步没有意义；开启采集并清空旧记录，
    // 让 agent 从「注册之后的第一条记录」开始做序号连续性判断。
    kvs_ebpf_capture_enable(1);
    kvs_log(KVS_LOG_INFO, "[SYNC] ebpf-agent registered (fd=%d), full sync enabled", fd);
    return 1;
}

int kvs_sync_accept_repl_inc_handshake(int fd, const char* data, int length) {
    if (g_sync_role != KVS_ROLE_REPLICA || fd < 0) {
        return 0;
    }
    if (!sync_handshake_matches(data, length, KVS_REPL_HANDSHAKE_REPL_INC)) {
        return 0;
    }
    if (g_repl_inc_fd >= 0 && g_repl_inc_fd != fd) {
        kvs_log(KVS_LOG_WARN, "[SYNC] increment channel replaced: fd %d -> %d", g_repl_inc_fd, fd);
    }
    pthread_mutex_lock(&g_inc_mutex);
    __atomic_store_n(&g_repl_inc_fd, fd, __ATOMIC_RELEASE);
    pthread_cond_broadcast(&g_inc_cond);
    pthread_mutex_unlock(&g_inc_mutex);
    kvs_log(KVS_LOG_INFO, "[SYNC] increment channel established (fd=%d)", fd);
    return 1;
}

// 增量回放：把 payload 当作 Master 的命令执行。回放期间置线程局部 replaying 标记，
// 使写分支跳过 AOF 记录与再转发（与旧复制回放一致的语义）。
int kvs_sync_try_replay(int fd, char* payload, int length) {
    if (fd < 0 || fd != __atomic_load_n(&g_repl_inc_fd, __ATOMIC_ACQUIRE) || payload == NULL) {
        return 0;
    }
    if (length <= 0) {
        return 1; // 空帧（例如握手响应）直接吞掉
    }

    static unsigned long long replayed = 0;
    char response[256] = {0};
    kvs_replication_set_replaying(1);
    kvs_protocol(payload, length, response, sizeof(response));
    kvs_replication_set_replaying(0);

    if (++replayed % 5000 == 0) {
        kvs_log(KVS_LOG_INFO, "[SYNC] replayed %llu increment command(s)", replayed);
    }
    return 1;
}

void kvs_sync_forget_fd(int fd) {
    if (fd < 0) {
        return;
    }
    if (__atomic_load_n(&g_ebpf_agent_fd, __ATOMIC_ACQUIRE) == fd) {
        __atomic_store_n(&g_ebpf_agent_fd, -1, __ATOMIC_RELEASE);
        kvs_ebpf_capture_enable(0);
        kvs_log(KVS_LOG_WARN, "[SYNC] ebpf-agent disconnected (fd=%d), SYNC disabled", fd);
    }
    pthread_mutex_lock(&g_inc_mutex);
    if (__atomic_load_n(&g_repl_inc_fd, __ATOMIC_ACQUIRE) == fd) {
        __atomic_store_n(&g_repl_inc_fd, -1, __ATOMIC_RELEASE);
        kvs_log(KVS_LOG_WARN, "[SYNC] increment channel closed (fd=%d)", fd);
    }
    pthread_cond_broadcast(&g_inc_cond);
    pthread_mutex_unlock(&g_inc_mutex);
}

// 等待增量通道接入（wait_connected=1）或断开（0）。返回 1 表示达到目标状态。
int kvs_sync_wait_increment(int wait_connected, int timeout_ms) {
    unsigned long long deadline = sync_now_ms() + (unsigned long long)timeout_ms;
    pthread_mutex_lock(&g_inc_mutex);
    while (g_sync_running) {
        int connected = __atomic_load_n(&g_repl_inc_fd, __ATOMIC_ACQUIRE) >= 0;
        if (connected == (wait_connected != 0)) {
            pthread_mutex_unlock(&g_inc_mutex);
            return 1;
        }
        if (timeout_ms >= 0 && sync_now_ms() >= deadline) {
            break;
        }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 100 * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000L;
        }
        pthread_cond_timedwait(&g_inc_cond, &g_inc_mutex, &ts);
    }
    pthread_mutex_unlock(&g_inc_mutex);
    return 0;
}

// Replica 侧的本地接收路径：与 Master 的发布路径区分开，避免单机验证时两者
// 指向同一个文件。Master 写 <sync_tmp_file>，rdma-server 写 <sync_tmp_file>.recv。
static void sync_recv_path(char* out, size_t out_size) {
    snprintf(out, out_size, "%s.recv", kvs_config_sync_tmp_file());
}

static void sync_ready_path(char* out, size_t out_size) {
    snprintf(out, out_size, "%s.ready", kvs_config_sync_tmp_file());
}

static int sync_path_exists(const char* path) {
    struct stat st;
    return stat(path, &st) == 0;
}

// 派生 rdma-server 并等它开始监听（就绪文件出现）。
static int sync_spawn_rdma_server() {
    char recv_path[512];
    char ready_path[512];
    char log_path[512];
    char port_str[16];
    char timeout_str[16];
    sync_recv_path(recv_path, sizeof(recv_path));
    sync_ready_path(ready_path, sizeof(ready_path));
    snprintf(log_path, sizeof(log_path), "%s.rdma-server.log", kvs_config_sync_tmp_file());
    snprintf(port_str, sizeof(port_str), "%d", kvs_config_rdma_port());
    snprintf(timeout_str, sizeof(timeout_str), "%d", kvs_config_rdma_transfer_timeout_ms());

    // 上一轮的残留会让我们误判「已就绪」，先删掉。
    unlink(ready_path);
    unlink(recv_path);

    char* argv[] = {(char*)kvs_config_rdma_server_bin(),
                    (char*)"--listen-port",
                    port_str,
                    (char*)"--out",
                    recv_path,
                    (char*)"--ready-file",
                    ready_path,
                    (char*)"--timeout-ms",
                    timeout_str,
                    NULL};

    if (sync_spawn_child(kvs_config_rdma_server_bin(), argv, log_path, &g_rdma_server_pid) != 0) {
        return -1;
    }

    unsigned long long deadline = sync_now_ms() + KVS_SYNC_RDMA_READY_TIMEOUT_MS;
    while (g_sync_running) {
        if (sync_path_exists(ready_path)) {
            kvs_log(KVS_LOG_INFO, "[SYNC] rdma-server ready (pid=%d) on port %d, log=%s",
                    (int)g_rdma_server_pid, kvs_config_rdma_port(), log_path);
            return 0;
        }
        // 子进程若在就绪前就退出，说明端口占用/RDMA 不可用，直接失败。
        int status = 0;
        if (waitpid(g_rdma_server_pid, &status, WNOHANG) == g_rdma_server_pid) {
            kvs_log(KVS_LOG_ERROR, "[SYNC] rdma-server exited before ready (status=%d), see %s",
                    status, log_path);
            g_rdma_server_pid = -1;
            return -1;
        }
        if (sync_now_ms() >= deadline) {
            kvs_log(KVS_LOG_ERROR, "[SYNC] rdma-server not ready in %dms, see %s",
                    KVS_SYNC_RDMA_READY_TIMEOUT_MS, log_path);
            sync_kill_child(&g_rdma_server_pid);
            return -1;
        }
        sync_sleep_ms(20);
    }
    sync_kill_child(&g_rdma_server_pid);
    return -1;
}

// 一次完整的 Replica 侧全量同步。成功返回 0。
static int sync_run_replica_once(int fd) {
    char resp[256];
    char recv_path[512];
    char ready_path[512];
    char log_path[512];
    sync_recv_path(recv_path, sizeof(recv_path));
    sync_ready_path(ready_path, sizeof(ready_path));
    snprintf(log_path, sizeof(log_path), "%s.rdma-server.log", kvs_config_sync_tmp_file());

    int rc = -1;
    int waited = 0;
    int load_rc = -1;

    // (1) 先起接收端并等它就绪，再向 Master 要快照——否则 Master 派生的
    //     rdma-client 可能先到、连接被拒。
    if (sync_spawn_rdma_server() != 0) {
        return -1;
    }

    // (2) 请求全量同步。
    if (sync_send_command(fd, "SYNC") != 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] send SYNC failed");
        goto cleanup;
    }
    if (kvs_net_recv_frame(fd, resp, sizeof(resp), KVS_SYNC_IO_TIMEOUT_MS) < 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] no response for SYNC");
        goto cleanup;
    }
    if (strncmp(resp, "+OK", 3) != 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] master rejected SYNC: %s", resp);
        goto cleanup;
    }

    // (3) 等 rdma-server 收完文件并退出，这同时是「文件已完整落盘」的信号。
    waited = sync_wait_child(g_rdma_server_pid, kvs_config_sync_wait_timeout_ms());
    if (waited != 1) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] wait rdma-server %s, see %s",
                waited == 0 ? "timeout" : "failed", log_path);
        if (waited != 0) {
            // 已经 waitpid 收尸：清掉 pid，避免清理路径再去 kill 一个不存在的进程。
            g_rdma_server_pid = -1;
        }
        goto cleanup;
    }
    g_rdma_server_pid = -1;

    // (4) 加载快照。kvs_snapshot_load 内部会先 kvs_reset_data()，必须持存储锁。
    kvs_data_lock();
    load_rc = kvs_snapshot_load(recv_path);
    kvs_data_unlock();
    if (load_rc != 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] load snapshot %s failed", recv_path);
        goto cleanup;
    }
    kvs_log(KVS_LOG_INFO, "[SYNC] snapshot loaded from %s", recv_path);

    // (5) 告诉 Master 可以删 .tmp、回收 rdma-client 了。
    if (sync_send_command(fd, "SYNC FINISHED") != 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] send SYNC FINISHED failed");
        goto cleanup;
    }
    if (kvs_net_recv_frame(fd, resp, sizeof(resp), KVS_SYNC_IO_TIMEOUT_MS) < 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] no response for SYNC FINISHED");
        goto cleanup;
    }
    if (strncmp(resp, "+OK", 3) != 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] master rejected SYNC FINISHED: %s", resp);
        goto cleanup;
    }
    rc = 0;

cleanup:
    sync_kill_child(&g_rdma_server_pid);
    unlink(ready_path);
    if (rc == 0) {
        unlink(recv_path); // 已加载进内存，本地副本不再需要
    }
    return rc;
}

static void* sync_replica_thread(void*) {
    int backoff = KVS_SYNC_BACKOFF_MIN_MS;

    while (g_sync_running) {
        int fd = kvs_net_connect(g_master_ip, g_master_port, KVS_SYNC_CONNECT_TIMEOUT_MS);
        if (fd < 0) {
            kvs_log(KVS_LOG_WARN, "[SYNC] connect master %s:%d failed, retry in %dms",
                    g_master_ip, g_master_port, backoff);
            sync_sleep_interruptible(backoff, &g_sync_running);
            backoff = backoff < KVS_SYNC_BACKOFF_MAX_MS ? backoff * 2 : KVS_SYNC_BACKOFF_MAX_MS;
            continue;
        }

        int rc = sync_run_replica_once(fd);
        close(fd);

        if (rc == 0) {
            kvs_log(KVS_LOG_INFO, "[SYNC] full sync completed (master %s:%d)", g_master_ip,
                    g_master_port);
            backoff = KVS_SYNC_BACKOFF_MIN_MS;

            // 阶段 3：全量完成后等待 ebpf-agent 的增量通道接入。通道断开说明
            // 采集侧丢过事件（或 agent 重启），按设计 §8.2 步骤 7 重新全量同步。
            kvs_log(KVS_LOG_INFO, "[SYNC] waiting for increment channel from ebpf-agent");
            if (!kvs_sync_wait_increment(1, KVS_SYNC_INCREMENT_WAIT_MS)) {
                if (!g_sync_running) {
                    break; // 收到停止请求
                }
                // 通道迟迟不出现：agent 可能放弃了这一轮（如缓存溢出后主动
                // 放弃），此时快照窗口期间的数据无法补齐，必须重新同步。
                kvs_log(KVS_LOG_WARN,
                        "[SYNC] increment channel did not appear in %dms, re-running full sync",
                        KVS_SYNC_INCREMENT_WAIT_MS);
                continue;
            }
            kvs_log(KVS_LOG_INFO, "[SYNC] increment channel online, replaying increments");
            if (!kvs_sync_wait_increment(0, -1)) {
                break; // 收到停止请求
            }
            kvs_log(KVS_LOG_WARN, "[SYNC] increment channel lost, re-running full sync");
            continue;
        }

        kvs_log(KVS_LOG_WARN, "[SYNC] sync attempt failed, retry in %dms", backoff);
        sync_sleep_interruptible(backoff, &g_sync_running);
        backoff = backoff < KVS_SYNC_BACKOFF_MAX_MS ? backoff * 2 : KVS_SYNC_BACKOFF_MAX_MS;
    }

    return NULL;
}

// ==================== 生命周期 ====================

int kvs_sync_init(kvs_role_t role) {
    g_sync_role = role;
    if (role == KVS_ROLE_REPLICA) {
        const char* ip = kvs_config_master_ip();
        if (ip != NULL && ip[0] != '\0') {
            snprintf(g_master_ip, sizeof(g_master_ip), "%s", ip);
        }
        g_master_port = kvs_config_master_port();
    }
    kvs_log(KVS_LOG_INFO, "[SYNC] tmp snapshot file: %s, rdma port: %d",
            kvs_config_sync_tmp_file(), kvs_config_rdma_port());
    return 0;
}

void kvs_sync_set_master_addr(const char* ip, int port) {
    if (ip != NULL && ip[0] != '\0') {
        snprintf(g_master_ip, sizeof(g_master_ip), "%s", ip);
    }
    if (port > 0) {
        g_master_port = port;
    }
}

int kvs_sync_start() {
    if (g_sync_role != KVS_ROLE_REPLICA || g_sync_running) {
        return -1;
    }
    if (g_master_ip[0] == '\0' || g_master_port <= 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] master address is not configured");
        return -1;
    }

    g_sync_running = 1;
    if (pthread_create(&g_sync_tid, NULL, sync_replica_thread, NULL) != 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] failed to create sync thread");
        g_sync_running = 0;
        return -1;
    }
    return 0;
}

void kvs_sync_stop() {
    if (!g_sync_running) {
        return;
    }
    g_sync_running = 0;
    pthread_join(g_sync_tid, NULL);
    sync_kill_child(&g_rdma_server_pid);
    kvs_log(KVS_LOG_INFO, "[SYNC] replica sync driver stopped");
}

void kvs_sync_destroy() {
    if (g_sync_role == KVS_ROLE_REPLICA) {
        kvs_sync_stop();
    } else {
        sync_kill_child(&g_rdma_client_pid);
    }
}
