// 主从全量同步控制流（重设计阶段 1 + 阶段 2）。
//
// 设计与阶段划分见 docs/replication-redesign.md。
//   * Master：SYNC -> 序列化 -> 写 <data/kvstore.sync.tmp> -> 派生 rdma-client -> +OK；
//             SYNC FINISHED -> 回收 rdma-client -> 删除 .tmp -> +OK；
//   * Replica：派生 rdma-server -> 连 Master 发 SYNC <rdma_ip> <rdma_port>
//              -> 等 RDMA 文件落盘 ->
//             加载 -> SYNC FINISHED -> 回收 rdma-server。
//
// SYNC 的序列化放在存储锁内，保证快照是「收到 SYNC 那一刻」的一致状态；
// 参与 RDMA 搬运的是两个独立进程（rdma-client / rdma-server），协议与约束见
// include/kvs_rdma_transfer.h。
#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_io_uring.h"
#include "kvs_network.h"
#include "kvs_netutil.h"
#include "kvs_replication.h"
#include "kvs_snapshot.h"
#include "kvs_runtime.h"

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

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

extern char** environ;

// 增量回放复用命令分发入口（同 kvs_replication.cpp 的做法）。
extern int kvs_protocol(int client_fd, char* msg, int length, char* response, int response_size);

// 控制通道沿用外层「4 字节网络序长度 + 负载」帧格式。
// 连接 Master 的超时。
#define KVS_SYNC_CONNECT_TIMEOUT_MS 3000
// 收发一帧的超时（SYNC 期间 Master 要序列化 + 落盘，给足余量）。
#define KVS_SYNC_IO_TIMEOUT_MS 120000
// Replica 等待 rdma-server 就绪（端口开始监听）的最长时间。
#define KVS_SYNC_RDMA_READY_TIMEOUT_MS 10000
// 失败后的重试退避区间。
#define KVS_SYNC_BACKOFF_MIN_MS 200
#define KVS_SYNC_BACKOFF_MAX_MS 5000

static kvs_role_t g_sync_role = KVS_ROLE_MASTER;

enum master_session_state_t {
    MASTER_SESSION_PREPARING = 0,
    MASTER_SESSION_AWAITING_FINISHED = 1,
};

struct master_session_t {
    std::string session_id;
    int control_fd;
    char tmp_path[512];
    pid_t rdma_client_pid;
    unsigned long long deadline_ms;
    master_session_state_t state;
};

// Master 侧每个全量同步请求一个独立会话。会话表只保存尚未 SYNC FINISHED 的
// 全量会话；进入增量阶段后由 Agent 的目标表接管。
static std::unordered_map<std::string, master_session_t> g_master_sessions;
static std::unordered_map<int, std::string> g_control_sessions;
static pthread_mutex_t g_master_sessions_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_master_sessions_cond = PTHREAD_COND_INITIALIZER;
static std::atomic<unsigned long long> g_next_session_id(1);
static volatile int g_session_reaper_running = 0;
static pthread_t g_session_reaper_tid;

// Replica 侧状态。
static pthread_t g_sync_tid;
static volatile int g_sync_running = 0;
static char g_master_ip[INET_ADDRSTRLEN] = {0};
static int g_master_port = 0;
static int g_agent_port = 0;
// Replica 派生的 rdma-server 进程；-1 表示当前没有。
static pid_t g_rdma_server_pid = -1;

// 阶段 3：Master 侧已注册的 ebpf-agent 连接；-1 表示未武装。
static int g_ebpf_agent_fd = -1;
// 阶段 3：Replica 侧已建立的增量通道连接；-1 表示未接入。
static int g_repl_inc_fd = -1;
static pthread_t g_inc_tid;
static volatile int g_inc_running = 0;
static bool g_inc_thread_started = false;

// 增量通道状态变化通知：Replica 全量同步完成后靠它等待断开。
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

static int sync_send_commands(int fd, const char* const argv[], int argc) {
    char payload[256];
    size_t payload_len = 0;
    if (kvs_repl_build_resp_array(argv, argc, payload, sizeof(payload), &payload_len) != 0) {
        return -1;
    }
    return kvs_net_send_frame(fd, payload, payload_len, KVS_SYNC_IO_TIMEOUT_MS);
}

// Replica 在 SYNC 中上报本次可访问的 RDMA endpoint。默认取控制连接使用的
// 本地地址；多网卡/测试环境可用 rdma_advertise_host 覆盖。
static int sync_resolve_advertised_endpoint(int fd, char* ip, size_t ip_size, int* port) {
    if (fd < 0 || ip == NULL || ip_size == 0 || port == NULL) {
        return -1;
    }

    const char* override = kvs_config_rdma_advertise_host();
    if (override != NULL && override[0] != '\0') {
        struct in_addr address;
        if (inet_pton(AF_INET, override, &address) != 1 ||
            strlen(override) >= ip_size) {
            kvs_log(KVS_LOG_ERROR, "[SYNC] invalid rdma_advertise_host: %s", override);
            return -1;
        }
        snprintf(ip, ip_size, "%s", override);
    } else {
        struct sockaddr_in local;
        socklen_t local_size = sizeof(local);
        if (getsockname(fd, (struct sockaddr*)&local, &local_size) != 0 ||
            local.sin_family != AF_INET ||
            inet_ntop(AF_INET, &local.sin_addr, ip, (socklen_t)ip_size) == NULL) {
            kvs_log(KVS_LOG_ERROR, "[SYNC] cannot determine local RDMA address");
            return -1;
        }
    }

    *port = kvs_config_rdma_port();
    if (*port <= 0 || *port > 65535) {
        return -1;
    }
    kvs_log(KVS_LOG_INFO, "[SYNC] advertising RDMA endpoint %s:%d", ip, *port);
    return 0;
}

static int sync_send_sync_request(int fd, const char* rdma_ip, int rdma_port) {
    char port_text[16];
    char payload[256];
    int port_len =
        snprintf(port_text, sizeof(port_text), "%d", rdma_port);
    if (port_len <= 0 || (size_t)port_len >= sizeof(port_text) || rdma_ip == NULL) {
        return -1;
    }
    int n = snprintf(payload, sizeof(payload),
                     "*3\r\n$4\r\nSYNC\r\n$%zu\r\n%s\r\n$%d\r\n%s\r\n", strlen(rdma_ip),
                     rdma_ip, port_len, port_text);
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

static void sync_master_tmp_path(const char* session_id, char* out, size_t out_size) {
    snprintf(out, out_size, "../data/kvstore.sync.%s.tmp", session_id);
}

// 派生 rdma-client 把 .tmp 搬到 Replica。log 文件与 .tmp 同目录，便于排查。
static int sync_spawn_rdma_client(const char* tmp_path, const char* peer_ip, int peer_port,
                                  pid_t* out_pid) {
    char port_str[16];
    char timeout_str[16];
    char log_path[512];
    if (out_pid == NULL) {
        return -1;
    }
    snprintf(port_str, sizeof(port_str), "%d", peer_port);
    snprintf(timeout_str, sizeof(timeout_str), "%d", kvs_config_rdma_transfer_timeout_ms());
    snprintf(log_path, sizeof(log_path), "%s.rdma-client.log", tmp_path);

    char* argv[] = {(char*)kvs_config_rdma_client_bin(),
                    (char*)"--file",
                    (char*)tmp_path,
                    (char*)"--peer",
                    (char*)peer_ip,
                    (char*)"--port",
                    port_str,
                    (char*)"--timeout-ms",
                    timeout_str,
                    NULL};

    if (sync_spawn_child(kvs_config_rdma_client_bin(), argv, log_path, out_pid) != 0) {
        return -1;
    }
    kvs_log(KVS_LOG_INFO, "[SYNC] rdma-client spawned (pid=%d) -> %s:%d, log=%s",
            (int)*out_pid, peer_ip, peer_port, log_path);
    return 0;
}

static void sync_remove_session_files(const char* tmp_path) {
    char partial[768];
    unlink(tmp_path);
    snprintf(partial, sizeof(partial), "%s.partial", tmp_path);
    unlink(partial);
}

// 发布 ABORT 后回收单个会话。调用者不得持有会话锁或存储锁。
static void sync_abort_master_session(const std::string& session_id, const char* reason) {
    master_session_t session;
    bool found = false;
    pthread_mutex_lock(&g_master_sessions_mutex);
    std::unordered_map<std::string, master_session_t>::iterator it =
        g_master_sessions.find(session_id);
    if (it != g_master_sessions.end()) {
        session = it->second;
        g_master_sessions.erase(it);
        std::unordered_map<int, std::string>::iterator fd_it =
            g_control_sessions.find(session.control_fd);
        if (fd_it != g_control_sessions.end() && fd_it->second == session_id) {
            g_control_sessions.erase(fd_it);
        }
        found = true;
    }
    pthread_cond_broadcast(&g_master_sessions_cond);
    pthread_mutex_unlock(&g_master_sessions_mutex);
    if (!found) {
        return;
    }

    char* abort_args[] = {(char*)"SYNC ABORT", (char*)session_id.c_str()};
    kvs_data_lock();
    kvs_ebpf_notify_command(2, abort_args);
    kvs_data_unlock();

    sync_kill_child(&session.rdma_client_pid);
    sync_remove_session_files(session.tmp_path);
    kvs_log(KVS_LOG_WARN, "[SYNC] session %s aborted: %s", session_id.c_str(), reason);
}

static void sync_abort_all_master_sessions(const char* reason) {
    std::vector<std::string> ids;
    pthread_mutex_lock(&g_master_sessions_mutex);
    for (std::unordered_map<std::string, master_session_t>::const_iterator it =
             g_master_sessions.begin();
         it != g_master_sessions.end(); ++it) {
        ids.push_back(it->first);
    }
    pthread_mutex_unlock(&g_master_sessions_mutex);
    for (size_t i = 0; i < ids.size(); ++i) {
        sync_abort_master_session(ids[i], reason);
    }
}

static void* sync_session_reaper_thread(void*) {
    pthread_mutex_lock(&g_master_sessions_mutex);
    while (g_session_reaper_running) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&g_master_sessions_cond, &g_master_sessions_mutex, &ts);

        unsigned long long now = sync_now_ms();
        std::vector<std::string> expired;
        for (std::unordered_map<std::string, master_session_t>::const_iterator it =
                 g_master_sessions.begin();
             it != g_master_sessions.end(); ++it) {
            if (it->second.deadline_ms <= now) {
                expired.push_back(it->first);
            }
        }
        pthread_mutex_unlock(&g_master_sessions_mutex);
        for (size_t i = 0; i < expired.size(); ++i) {
            sync_abort_master_session(expired[i], "session timeout");
        }
        pthread_mutex_lock(&g_master_sessions_mutex);
    }
    pthread_mutex_unlock(&g_master_sessions_mutex);
    return NULL;
}

int kvs_sync_handle_sync(int client_fd, char* tokens[], int count, char* response,
                         int response_size) {
    if (g_sync_role != KVS_ROLE_MASTER) {
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    // eBPF 强制（设计 D3）：采集进程没注册就不做全量同步。
    // 否则「全量窗口内的新写入」没有采集方，会静默丢失。
    if (!kvs_sync_ebpf_armed()) {
        kvs_log(KVS_LOG_WARN, "[SYNC] rejected: ebpf-agent is not registered");
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    char replica_ip[INET_ADDRSTRLEN] = {0};
    int replica_port = 0;
    if (count == 3 && tokens != NULL && tokens[1] != NULL && tokens[2] != NULL) {
        struct in_addr parsed_address;
        char* end = NULL;
        long parsed_port = strtol(tokens[2], &end, 10);
        if (inet_pton(AF_INET, tokens[1], &parsed_address) != 1 || end == tokens[2] ||
            *end != '\0' || parsed_port <= 0 || parsed_port > 65535) {
            kvs_log(KVS_LOG_WARN, "[SYNC] invalid advertised RDMA endpoint");
            return snprintf(response, response_size, "-ERROR\r\n");
        }
        snprintf(replica_ip, sizeof(replica_ip), "%s", tokens[1]);
        replica_port = (int)parsed_port;
        kvs_log(KVS_LOG_INFO, "[SYNC] replica advertised RDMA endpoint %s:%d", replica_ip,
                replica_port);
    } else {
        kvs_log(KVS_LOG_WARN, "[SYNC] invalid SYNC argument count: %d", count);
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    if (client_fd < 0) {
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    master_session_t session;
    char session_id[KVS_REPL_SESSION_ID_SIZE + 1] = {0};
    pthread_mutex_lock(&g_master_sessions_mutex);
    if (g_master_sessions.size() >= (size_t)kvs_config_max_replicas()) {
        pthread_mutex_unlock(&g_master_sessions_mutex);
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    do {
        unsigned long long candidate = g_next_session_id.fetch_add(1);
        if (kvs_repl_format_session_id(candidate, session_id, sizeof(session_id)) != 0) {
            pthread_mutex_unlock(&g_master_sessions_mutex);
            return snprintf(response, response_size, "-ERROR\r\n");
        }
    } while (g_master_sessions.find(session_id) != g_master_sessions.end());

    session.session_id = session_id;
    session.control_fd = client_fd;
    session.rdma_client_pid = -1;
    session.deadline_ms =
        sync_now_ms() + (unsigned long long)kvs_config_sync_session_timeout_ms();
    session.state = MASTER_SESSION_PREPARING;
    sync_master_tmp_path(session_id, session.tmp_path, sizeof(session.tmp_path));
    g_master_sessions.insert(std::make_pair(session.session_id, session));
    g_control_sessions[client_fd] = session.session_id;
    pthread_mutex_unlock(&g_master_sessions_mutex);

    // SYNC 事件与序列化在同一存储锁窗口内，只影响 session_id 对应目标。
    char* sync_token[] = {(char*)"SYNC", session_id};
    char* blob = NULL;
    size_t blob_len = 0;
    kvs_data_lock();
    kvs_ebpf_notify_command(2, sync_token);
    int serialize_rc = kvs_snapshot_serialize(&blob, &blob_len);
    kvs_data_unlock();
    if (serialize_rc != 0 || blob == NULL) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] snapshot serialize failed");
        free(blob);
        sync_abort_master_session(session_id, "snapshot serialize failed");
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    int rc = sync_write_tmp_atomic(session.tmp_path, blob, blob_len);
    free(blob);
    if (rc != 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] write snapshot %s failed", session.tmp_path);
        sync_abort_master_session(session_id, "snapshot write failed");
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    kvs_log(KVS_LOG_INFO, "[SYNC] session %s snapshot written: %s (%zu bytes)", session_id,
            session.tmp_path, blob_len);

    pid_t child_pid = -1;
    if (sync_spawn_rdma_client(session.tmp_path, replica_ip, replica_port, &child_pid) != 0) {
        sync_abort_master_session(session_id, "rdma-client spawn failed");
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    pthread_mutex_lock(&g_master_sessions_mutex);
    std::unordered_map<std::string, master_session_t>::iterator active =
        g_master_sessions.find(session_id);
    if (active == g_master_sessions.end()) {
        pthread_mutex_unlock(&g_master_sessions_mutex);
        sync_kill_child(&child_pid);
        sync_remove_session_files(session.tmp_path);
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    active->second.rdma_client_pid = child_pid;
    active->second.state = MASTER_SESSION_AWAITING_FINISHED;
    pthread_mutex_unlock(&g_master_sessions_mutex);
    return snprintf(response, response_size, "+OK %s\r\n", session_id);
}

int kvs_sync_handle_sync_finished(int client_fd, char* tokens[], int count, char* response,
                                  int response_size) {
    if (g_sync_role != KVS_ROLE_MASTER) {
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    if (tokens == NULL || count != 2 || !kvs_repl_validate_session_id(tokens[1])) {
        return snprintf(response, response_size, "-ERROR\r\n");
    }

    master_session_t session;
    pthread_mutex_lock(&g_master_sessions_mutex);
    std::unordered_map<std::string, master_session_t>::iterator it =
        g_master_sessions.find(tokens[1]);
    if (it == g_master_sessions.end() ||
        (client_fd >= 0 && it->second.control_fd != client_fd) ||
        it->second.state != MASTER_SESSION_AWAITING_FINISHED) {
        pthread_mutex_unlock(&g_master_sessions_mutex);
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    session = it->second;
    g_master_sessions.erase(it);
    std::unordered_map<int, std::string>::iterator fd_it =
        g_control_sessions.find(session.control_fd);
    if (fd_it != g_control_sessions.end() && fd_it->second == session.session_id) {
        g_control_sessions.erase(fd_it);
    }
    pthread_cond_broadcast(&g_master_sessions_cond);
    pthread_mutex_unlock(&g_master_sessions_mutex);

    char* done_token[] = {(char*)"SYNC FINISHED", (char*)session.session_id.c_str()};
    kvs_data_lock();
    kvs_ebpf_notify_command(2, done_token);
    kvs_data_unlock();

    sync_kill_child(&session.rdma_client_pid);
    sync_remove_session_files(session.tmp_path);
    kvs_log(KVS_LOG_INFO, "[SYNC] session %s finished, rdma-client released and %s removed",
            session.session_id.c_str(), session.tmp_path);
    return snprintf(response, response_size, "+OK\r\n");
}

// ==================== Replica：同步驱动 ====================

// ==================== 采集进程注册 / 增量通道（阶段 3） ====================

int kvs_sync_ebpf_armed(void) { return g_ebpf_agent_fd >= 0; }

int kvs_sync_accept_ebpf_handshake(int fd, const char* data, int length) {
    if (g_sync_role != KVS_ROLE_MASTER || fd < 0) {
        return 0;
    }
    if (kvs_repl_parse_ebpf_handshake(data, length) != 0) {
        return 0;
    }
    if (g_ebpf_agent_fd >= 0 && g_ebpf_agent_fd != fd) {
        kvs_log(KVS_LOG_WARN, "[SYNC] ebpf-agent re-registered: fd %d -> %d", g_ebpf_agent_fd, fd);
        sync_abort_all_master_sessions("ebpf-agent re-registered");
    }
    __atomic_store_n(&g_ebpf_agent_fd, fd, __ATOMIC_RELEASE);
    // 注册之前的记录对本次同步没有意义；开启采集并清空旧记录，
    // 让 agent 从「注册之后的第一条记录」开始做序号连续性判断。
    kvs_ebpf_capture_enable(1);
    kvs_log(KVS_LOG_INFO, "[SYNC] ebpf-agent registered (fd=%d), full sync enabled", fd);
    return 1;
}

// 读取增量通道上的定长内容。返回 1 = 成功，0 = 复制驱动停止，-1 = 连接断开/错误。
static int sync_read_exact(int fd, void* buffer, size_t length) {
    char* output = (char*)buffer;
    size_t received = 0;
    while (received < length && g_inc_running) {
        struct pollfd pfd;
        pfd.fd = fd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        int ready = poll(&pfd, 1, 200);
        if (ready == 0) {
            continue;
        }
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        ssize_t count = recv(fd, output + received, length - received, 0);
        if (count > 0) {
            received += (size_t)count;
            continue;
        }
        if (count == 0) {
            return -1;
        }
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
        }
        return -1;
    }
    return received == length ? 1 : 0;
}

static void sync_replay_increment(int fd, char* payload, int length) {
    if (length <= 0) {
        return;
    }
    static unsigned long long replayed = 0;
    char response[256] = {0};
    kvs_replication_set_replaying(1);
    kvs_protocol(-1, payload, length, response, sizeof(response));
    kvs_replication_set_replaying(0);
    if (++replayed % 5000 == 0) {
        kvs_log(KVS_LOG_INFO, "[SYNC] replayed %llu increment command(s)", replayed);
    }
    (void)fd;
}

static void* sync_increment_reader_thread(void* argument) {
    int fd = (int)(intptr_t)argument;
    while (g_inc_running) {
        uint32_t network_length = 0;
        int read_rc = sync_read_exact(fd, &network_length, sizeof(network_length));
        if (read_rc != 1) {
            break;
        }
        uint32_t length = ntohl(network_length);
        if (length == 0 || length > KVS_NET_MAX_MESSAGE_SIZE) {
            kvs_log(KVS_LOG_WARN, "[SYNC] invalid increment frame length: %u", length);
            break;
        }
        std::vector<char> payload((size_t)length + 1);
        read_rc = sync_read_exact(fd, payload.data(), length);
        if (read_rc != 1) {
            break;
        }
        payload[length] = '\0';
        sync_replay_increment(fd, payload.data(), (int)length);
    }

    close(fd);
    pthread_mutex_lock(&g_inc_mutex);
    if (__atomic_load_n(&g_repl_inc_fd, __ATOMIC_ACQUIRE) == fd) {
        __atomic_store_n(&g_repl_inc_fd, -1, __ATOMIC_RELEASE);
    }
    pthread_cond_broadcast(&g_inc_cond);
    pthread_mutex_unlock(&g_inc_mutex);
    kvs_log(KVS_LOG_WARN, "[SYNC] increment channel closed (fd=%d)", fd);
    return NULL;
}

static int sync_connect_increment_channel(const char* session_id) {
    char payload[256];
    size_t payload_len = 0;
    if (kvs_repl_build_repl_inc(session_id, payload, sizeof(payload), &payload_len) != 0) {
        return -1;
    }
    int fd = kvs_net_connect(g_master_ip, g_agent_port, KVS_SYNC_CONNECT_TIMEOUT_MS);
    if (fd < 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] connect agent %s:%d failed", g_master_ip, g_agent_port);
        return -1;
    }
    if (kvs_net_send_frame(fd, payload, payload_len, KVS_SYNC_IO_TIMEOUT_MS) != 0 ||
        kvs_net_recv_frame_ok(fd, KVS_SYNC_IO_TIMEOUT_MS) != 1) {
        kvs_log(KVS_LOG_WARN, "[SYNC] agent rejected REPL_INC handshake");
        close(fd);
        return -1;
    }
    return fd;
}

static int sync_start_increment_reader(int fd) {
    if (fd < 0 || g_inc_thread_started) {
        return -1;
    }
    g_inc_running = 1;
    pthread_mutex_lock(&g_inc_mutex);
    __atomic_store_n(&g_repl_inc_fd, fd, __ATOMIC_RELEASE);
    pthread_cond_broadcast(&g_inc_cond);
    pthread_mutex_unlock(&g_inc_mutex);
    if (pthread_create(&g_inc_tid, NULL, sync_increment_reader_thread,
                       (void*)(intptr_t)fd) != 0) {
        g_inc_running = 0;
        __atomic_store_n(&g_repl_inc_fd, -1, __ATOMIC_RELEASE);
        close(fd);
        return -1;
    }
    g_inc_thread_started = true;
    kvs_log(KVS_LOG_INFO, "[SYNC] increment channel connected to agent %s:%d (fd=%d)",
            g_master_ip, g_agent_port, fd);
    return 0;
}

static void sync_stop_increment_reader() {
    if (!g_inc_thread_started) {
        return;
    }
    g_inc_running = 0;
    int fd = __atomic_load_n(&g_repl_inc_fd, __ATOMIC_ACQUIRE);
    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
    }
    pthread_join(g_inc_tid, NULL);
    g_inc_thread_started = false;
    pthread_mutex_lock(&g_inc_mutex);
    __atomic_store_n(&g_repl_inc_fd, -1, __ATOMIC_RELEASE);
    pthread_cond_broadcast(&g_inc_cond);
    pthread_mutex_unlock(&g_inc_mutex);
}

void kvs_sync_forget_fd(int fd) {
    if (fd < 0) {
        return;
    }
    if (__atomic_load_n(&g_ebpf_agent_fd, __ATOMIC_ACQUIRE) == fd) {
        __atomic_store_n(&g_ebpf_agent_fd, -1, __ATOMIC_RELEASE);
        kvs_ebpf_capture_enable(0);
        kvs_log(KVS_LOG_WARN, "[SYNC] ebpf-agent disconnected (fd=%d), SYNC disabled", fd);
        sync_abort_all_master_sessions("ebpf-agent disconnected");
        return;
    }

    std::string session_id;
    pthread_mutex_lock(&g_master_sessions_mutex);
    std::unordered_map<int, std::string>::iterator it = g_control_sessions.find(fd);
    if (it != g_control_sessions.end()) {
        session_id = it->second;
    }
    pthread_mutex_unlock(&g_master_sessions_mutex);
    if (!session_id.empty()) {
        sync_abort_master_session(session_id, "control connection closed");
    }
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

// Replica 在收到 Master 会话 ID 之前必须先启动 RDMA 接收端，因此接收中的
// 临时文件按控制连接 fd 隔离；收到会话 ID 后再改名为会话文件并校验加载。
static void sync_recv_base_path(int control_fd, char* out, size_t out_size) {
    snprintf(out, out_size, "../data/kvstore.sync.replica.%d.%d.tmp.recv", (int)getpid(),
             control_fd);
}

static void sync_ready_path(int control_fd, char* out, size_t out_size) {
    snprintf(out, out_size, "../data/kvstore.sync.replica.%d.%d.ready", (int)getpid(),
             control_fd);
}

static void sync_recv_session_path(const char* session_id, char* out, size_t out_size) {
    snprintf(out, out_size, "../data/kvstore.sync.%s.tmp.recv", session_id);
}

static int sync_path_exists(const char* path) {
    struct stat st;
    return stat(path, &st) == 0;
}

// 派生 rdma-server 并等它开始监听（就绪文件出现）。
static int sync_spawn_rdma_server(int control_fd) {
    char recv_path[512];
    char ready_path[512];
    char log_path[512];
    char port_str[16];
    char timeout_str[16];
    sync_recv_base_path(control_fd, recv_path, sizeof(recv_path));
    sync_ready_path(control_fd, ready_path, sizeof(ready_path));
    snprintf(log_path, sizeof(log_path),
             "../data/kvstore.sync.replica.%d.%d.rdma-server.log", (int)getpid(),
             control_fd);
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
    char recv_base_path[512];
    char recv_session_path[512] = {0};
    char ready_path[512];
    char log_path[512];
    char session_id[KVS_REPL_SESSION_ID_SIZE + 1] = {0};
    sync_recv_base_path(fd, recv_base_path, sizeof(recv_base_path));
    sync_ready_path(fd, ready_path, sizeof(ready_path));
    snprintf(log_path, sizeof(log_path),
             "../data/kvstore.sync.replica.%d.%d.rdma-server.log", (int)getpid(), fd);

    int rc = -1;
    int waited = 0;
    int load_rc = -1;
    int increment_fd = -1;
    bool increment_started = false;
    bool received_renamed = false;
    char advertise_ip[INET_ADDRSTRLEN] = {0};
    int advertise_port = 0;
    const char* finished_args[2] = {NULL, NULL};

    // (1) 先起接收端并等它就绪，再向 Master 要快照——否则 Master 派生的
    //     rdma-client 可能先到、连接被拒。
    if (sync_spawn_rdma_server(fd) != 0) {
        return -1;
    }

    // (2) 上报本次可访问的 RDMA endpoint 并请求全量同步。
    if (sync_resolve_advertised_endpoint(fd, advertise_ip, sizeof(advertise_ip),
                                         &advertise_port) != 0 ||
        sync_send_sync_request(fd, advertise_ip, advertise_port) != 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] send SYNC failed");
        goto cleanup;
    }
    if (kvs_net_recv_frame(fd, resp, sizeof(resp), KVS_SYNC_IO_TIMEOUT_MS) < 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] no response for SYNC");
        goto cleanup;
    }
    if (kvs_repl_parse_ok_session(resp, (int)strlen(resp), session_id,
                                  sizeof(session_id)) != 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] master rejected SYNC: %s", resp);
        goto cleanup;
    }
    sync_recv_session_path(session_id, recv_session_path, sizeof(recv_session_path));
    kvs_log(KVS_LOG_INFO, "[SYNC] full-sync session assigned: %s", session_id);

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
    if (rename(recv_base_path, recv_session_path) != 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] rename received snapshot %s -> %s failed: %s",
                recv_base_path, recv_session_path, strerror(errno));
        goto cleanup;
    }
    received_renamed = true;

    // (4) 加载快照。kvs_snapshot_load 内部会先 kvs_reset_data()，必须持存储锁。
    kvs_data_lock();
    load_rc = kvs_snapshot_load(recv_session_path);
    kvs_data_unlock();
    if (load_rc != 0) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] load snapshot %s failed", recv_session_path);
        goto cleanup;
    }
    kvs_log(KVS_LOG_INFO, "[SYNC] snapshot loaded from %s", recv_session_path);

    // (5) Replica 在发送 SYNC FINISHED 前主动接入 Agent。Agent 读到 FINISHED
    //     后会立刻冲刷全量窗口内的缓存，因此通道必须先就绪。
    increment_fd = sync_connect_increment_channel(session_id);
    if (increment_fd < 0 || sync_start_increment_reader(increment_fd) != 0) {
        goto cleanup;
    }
    increment_started = true;

    // (6) 告诉 Master 可以删 .tmp、回收 rdma-client 了。
    finished_args[0] = "SYNC FINISHED";
    finished_args[1] = session_id;
    if (sync_send_commands(fd, finished_args, 2) != 0) {
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
    if (__atomic_load_n(&g_repl_inc_fd, __ATOMIC_ACQUIRE) < 0) {
        kvs_log(KVS_LOG_WARN, "[SYNC] increment channel closed during SYNC FINISHED");
        goto cleanup;
    }
    rc = 0;

cleanup:
    sync_kill_child(&g_rdma_server_pid);
    unlink(ready_path);
    unlink(recv_base_path);
    if (!received_renamed && recv_session_path[0] != '\0') {
        unlink(recv_session_path);
    }
    if (rc == 0 && received_renamed) {
        unlink(recv_session_path);
    }
    if (rc != 0 && increment_started) {
        sync_stop_increment_reader();
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

            // Replica 已在 SYNC FINISHED 前主动建立增量通道；此后只等待它断开。
            // 断开说明采集侧丢过事件或 agent 重启，必须重新全量同步。
            if (__atomic_load_n(&g_repl_inc_fd, __ATOMIC_ACQUIRE) < 0) {
                kvs_log(KVS_LOG_WARN, "[SYNC] increment channel unavailable after full sync");
                sync_stop_increment_reader();
                continue;
            }
            kvs_log(KVS_LOG_INFO, "[SYNC] increment channel online, replaying increments");
            if (!kvs_sync_wait_increment(0, -1)) {
                break; // 收到停止请求
            }
            kvs_log(KVS_LOG_WARN, "[SYNC] increment channel lost, re-running full sync");
            sync_stop_increment_reader();
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
        g_agent_port = kvs_config_agent_port();
        kvs_log(KVS_LOG_INFO, "[SYNC] per-session snapshots, rdma port: %d, agent port: %d",
                kvs_config_rdma_port(), g_agent_port);
    } else {
        g_session_reaper_running = 1;
        if (pthread_create(&g_session_reaper_tid, NULL, sync_session_reaper_thread, NULL) != 0) {
            g_session_reaper_running = 0;
            kvs_log(KVS_LOG_ERROR, "[SYNC] failed to create session reaper thread");
            return -1;
        }
        kvs_log(KVS_LOG_INFO,
                "[SYNC] per-session snapshots, rdma port: %d, max replicas: %d, timeout: %dms",
                kvs_config_rdma_port(), kvs_config_max_replicas(),
                kvs_config_sync_session_timeout_ms());
    }
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
    if (g_master_ip[0] == '\0' || g_master_port <= 0 || g_agent_port <= 0 ||
        g_agent_port > 65535) {
        kvs_log(KVS_LOG_ERROR, "[SYNC] master or agent address is not configured");
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
    int increment_fd = __atomic_load_n(&g_repl_inc_fd, __ATOMIC_ACQUIRE);
    if (increment_fd >= 0) {
        shutdown(increment_fd, SHUT_RDWR);
    }
    pthread_join(g_sync_tid, NULL);
    sync_stop_increment_reader();
    sync_kill_child(&g_rdma_server_pid);
    kvs_log(KVS_LOG_INFO, "[SYNC] replica sync driver stopped");
}

void kvs_sync_destroy() {
    if (g_sync_role == KVS_ROLE_REPLICA) {
        kvs_sync_stop();
    } else {
        if (g_session_reaper_running) {
            pthread_mutex_lock(&g_master_sessions_mutex);
            g_session_reaper_running = 0;
            pthread_cond_broadcast(&g_master_sessions_cond);
            pthread_mutex_unlock(&g_master_sessions_mutex);
            pthread_join(g_session_reaper_tid, NULL);
        }
        sync_abort_all_master_sessions("master shutting down");
    }
}
