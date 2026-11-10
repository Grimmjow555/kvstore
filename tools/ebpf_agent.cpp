// ebpf-agent：独立的增量采集与多从机扇出进程。
//
// 采集流中的写命令、SYNC、SYNC FINISHED、SYNC ABORT 与 INVALIDATE 保持全局
// 顺序。SYNC 类控制记录携带 Master 分配的 session_id，Hub 只切换对应目标；
// 其他在线 Replica 的 LIVE 状态和发送队列不受影响。
#include "kvs_agent_config.h"
#include "kvs_ebpf_agent.h"
#include "kvs_ebpf_ring.h"
#include "kvs_netutil.h"
#include "kvs_replica_hub.h"
#include "kvs_replication.h"

#include <arpa/inet.h>
#include <cstdarg>
#include <errno.h>
#include <memory>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <vector>

namespace {

char g_master_ip[64];
int g_master_port = 0;
char g_listen_ip[64];
int g_listen_port = 0;
std::unique_ptr<kvs::ReplicaHub> g_hub;
pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;

const int kHandshakeTimeoutMs = 3000;

void agent_log(const char* fmt, ...) {
    char ts[32];
    time_t now = time(NULL);
    struct tm tm_value;
    memset(&tm_value, 0, sizeof(tm_value));
    localtime_r(&now, &tm_value);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_value);

    pthread_mutex_lock(&g_log_mutex);
    fprintf(stderr, "[%s][EBPF-AGENT] ", ts);
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
    fflush(stderr);
    pthread_mutex_unlock(&g_log_mutex);
}

void on_capture_record(int argc, char* argv[]) {
    if (!g_hub || argc <= 0 || argv == NULL) {
        return;
    }
    if (argc == 2 && strcmp(argv[0], "SYNC") == 0 &&
        kvs_repl_validate_session_id(argv[1])) {
        if (g_hub->on_sync(argv[1])) {
            agent_log("SYNC observed for session %s: caching that target", argv[1]);
        } else {
            agent_log("SYNC ignored for session %s: target capacity reached", argv[1]);
        }
        return;
    }
    if (argc == 2 && strcmp(argv[0], "SYNC FINISHED") == 0 &&
        kvs_repl_validate_session_id(argv[1])) {
        if (g_hub->on_finished(argv[1])) {
            agent_log("SYNC FINISHED observed for session %s: target is live", argv[1]);
        } else {
            agent_log("SYNC FINISHED invalid for session %s", argv[1]);
        }
        return;
    }
    if (argc == 2 && strcmp(argv[0], "SYNC ABORT") == 0 &&
        kvs_repl_validate_session_id(argv[1])) {
        g_hub->on_abort(argv[1]);
        agent_log("SYNC ABORT observed for session %s", argv[1]);
        return;
    }
    if (argc == 1 && strcmp(argv[0], "INVALIDATE") == 0) {
        g_hub->on_invalidate();
        agent_log("INVALIDATE observed: dropping all replica targets");
        return;
    }

    // 旧协议的单 token SYNC 不再被接受；普通写命令原样广播。
    if ((argc == 1 && (strcmp(argv[0], "SYNC") == 0 ||
                       strcmp(argv[0], "SYNC FINISHED") == 0)) ||
        (argc > 1 && (strcmp(argv[0], "SYNC") == 0 ||
                      strcmp(argv[0], "SYNC FINISHED") == 0 ||
                      strcmp(argv[0], "SYNC ABORT") == 0))) {
        agent_log("ignored unversioned replication control record");
        return;
    }
    if (g_hub->on_write(argc, argv)) {
        agent_log("cache overflow or outbuf overflow dropped a slow target");
    }
}

void on_capture_resync(const char* reason) {
    if (g_hub) {
        g_hub->on_capture_resync(reason);
    }
    agent_log("capture needs resync (%s): dropping all replica targets",
              reason == NULL ? "unknown" : reason);
}

int open_replica_listener(const char* ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        agent_log("create replica listener socket failed: %s", strerror(errno));
        return -1;
    }

    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &address.sin_addr) != 1) {
        agent_log("invalid replica listen ip: %s", ip);
        close(fd);
        return -1;
    }
    if (bind(fd, (struct sockaddr*)&address, sizeof(address)) != 0) {
        agent_log("bind replica listener %s:%d failed: %s", ip, port, strerror(errno));
        close(fd);
        return -1;
    }
    if (listen(fd, 16) != 0) {
        agent_log("listen replica channel on %s:%d failed: %s", ip, port, strerror(errno));
        close(fd);
        return -1;
    }
    return fd;
}

void accept_replica_connection(int listen_fd) {
    struct sockaddr_in peer;
    socklen_t peer_size = sizeof(peer);
    int fd = accept(listen_fd, (struct sockaddr*)&peer, &peer_size);
    if (fd < 0) {
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            agent_log("accept replica channel failed: %s", strerror(errno));
        }
        return;
    }

    char payload[256];
    int length = kvs_net_recv_frame(fd, payload, sizeof(payload), kHandshakeTimeoutMs);
    char session_id[KVS_REPL_SESSION_ID_SIZE + 1] = {0};
    if (kvs_repl_parse_repl_inc(payload, length, session_id, sizeof(session_id)) != 0) {
        kvs_net_send_frame(fd, "-ERROR\r\n", strlen("-ERROR\r\n"), kHandshakeTimeoutMs);
        agent_log("rejected invalid replica channel handshake");
        close(fd);
        return;
    }

    if (!g_hub || !g_hub->attach(session_id, fd)) {
        kvs_net_send_frame(fd, "-ERROR\r\n", strlen("-ERROR\r\n"), kHandshakeTimeoutMs);
        agent_log("rejected replica session %s: capacity reached or hub unavailable",
                  session_id);
        close(fd);
        return;
    }
    if (kvs_net_send_frame(fd, "+OK\r\n", strlen("+OK\r\n"), kHandshakeTimeoutMs) != 0) {
        g_hub->detach_fd(fd);
        return;
    }

    char peer_ip[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &peer.sin_addr, peer_ip, sizeof(peer_ip));
    agent_log("increment channel accepted from %s:%d (fd=%d, session=%s)",
              peer_ip, ntohs(peer.sin_port), fd, session_id);
}

// 返回 0 表示本轮服务正常结束（master 断开或停止），1 表示继续运行。
int service_channels(int listen_fd, int master_fd, int timeout_ms) {
    std::vector<int> replica_fds;
    if (g_hub) {
        g_hub->fds(&replica_fds);
    }

    std::vector<struct pollfd> pfds;
    pfds.push_back({master_fd, POLLIN, 0});
    pfds.push_back({listen_fd, POLLIN, 0});
    for (size_t i = 0; i < replica_fds.size(); ++i) {
        pfds.push_back({replica_fds[i], POLLIN, 0});
    }

    int ready = poll(&pfds[0], (nfds_t)pfds.size(), timeout_ms);
    if (ready < 0 && errno != EINTR) {
        agent_log("poll agent channels failed: %s", strerror(errno));
        return 0;
    }
    if (ready > 0 && master_fd >= 0 &&
        (pfds[0].revents & (POLLIN | POLLHUP | POLLERR))) {
        char tmp[64];
        ssize_t n = recv(master_fd, tmp, sizeof(tmp), 0);
        if (n <= 0) {
            return 0;
        }
    }
    if (ready > 0 && (pfds[1].revents & POLLIN)) {
        accept_replica_connection(listen_fd);
    }
    for (size_t i = 0; ready > 0 && i < replica_fds.size(); ++i) {
        if (pfds[i + 2].revents == 0 || g_hub == NULL) {
            continue;
        }
        char tmp[64];
        ssize_t n = recv(replica_fds[i], tmp, sizeof(tmp), MSG_DONTWAIT);
        if (n != 0) {
            agent_log("replica fd=%d closed (%s)", replica_fds[i],
                      n > 0 ? "unexpected data" : strerror(errno));
        }
        g_hub->detach_fd(replica_fds[i]);
    }
    if (g_hub) {
        g_hub->flush();
    }
    return 1;
}

void service_replica_channels_without_master(int listen_fd, int timeout_ms) {
    service_channels(listen_fd, -1, timeout_ms);
}

void log_stats() {
    static unsigned long long last_tick = 0;
    unsigned long long now = (unsigned long long)time(NULL);
    if (now == last_tick || !g_hub) {
        return;
    }
    last_tick = now;
    kvs::ReplicaHubStats stats = g_hub->stats();
    agent_log("stats: targets=%zu waiting=%zu caching=%zu live=%zu cached=%zu queued=%zu "
              "forwarded=%llu failed=%llu ring_dropped=%llu",
              stats.targets, stats.waiting, stats.caching, stats.live,
              stats.cached_records, stats.queued_bytes, stats.forwarded, stats.failed,
              (unsigned long long)kvs_ebpf_ring_dropped());
}

int master_register_loop(volatile sig_atomic_t* running, const char* ring_path,
                         int replica_listen_fd) {
    bool first_registration = true;
    char handshake[128];
    size_t handshake_len = 0;
    if (kvs_repl_build_ebpf_handshake(handshake, sizeof(handshake), &handshake_len) != 0) {
        agent_log("failed to build versioned EBPF handshake");
        return 2;
    }

    while (*running) {
        int fd = kvs_net_connect(g_master_ip, g_master_port, kHandshakeTimeoutMs);
        if (fd < 0) {
            agent_log("connect master %s:%d failed, retry", g_master_ip, g_master_port);
            service_replica_channels_without_master(replica_listen_fd, 1000);
            continue;
        }
        if (kvs_net_send_frame(fd, handshake, handshake_len, kHandshakeTimeoutMs) != 0 ||
            kvs_net_recv_frame_ok(fd, kHandshakeTimeoutMs) != 1) {
            agent_log("master rejected versioned EBPF registration");
            close(fd);
            service_replica_channels_without_master(replica_listen_fd, 1000);
            continue;
        }
        agent_log("registered with master %s:%d", g_master_ip, g_master_port);

        if (!first_registration) {
            if (g_hub) {
                g_hub->clear();
            }
            if (kvs_ebpf_agent_capture_reopen(ring_path) != 0) {
                agent_log("capture could not be re-established after master restart; exiting");
                close(fd);
                return 3;
            }
        }
        first_registration = false;

        while (*running) {
            if (!service_channels(replica_listen_fd, fd, 10)) {
                break;
            }
            log_stats();
        }
        if (g_hub) {
            g_hub->clear();
        }
        close(fd);
        agent_log("master connection lost, re-registering");
        sleep(1);
    }
    return 0;
}

void usage(const char* prog) {
    fprintf(stderr,
            "用法: %s [--config <path>] [--master <ip:port>] [--listen <ip:port>]\n"
            "          [--mode <uprobe|sim>]\n"
            "          [--ring <path>] [--pid <master_pid>] [--binary <master_exe>]\n"
            "          [--cache-max-bytes N] [--cache-max-records N]\n"
            "          [--max-replicas N] [--memory-budget-bytes N]\n"
            "  未指定 --config 时依次读取 ./ebpf-agent.conf、../ebpf-agent.conf；\n"
            "  命令行参数覆盖配置文件中的同名字段。\n"
            "  --mode sim    读取 master 创建的共享内存环（无 CAP_BPF 时使用）\n"
            "  --mode uprobe  在 master 二进制上挂 uprobe + ringbuf（需要 --pid/--binary）\n",
            prog);
}

} // namespace

static volatile sig_atomic_t g_running = 1;

static void on_signal(int) { g_running = 0; }

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        }
    }

    kvs_agent_config_t config;
    kvs_agent_config_init(&config);

    char config_path[512];
    int has_explicit_path = 0;
    if (kvs_agent_config_select_path(argc, argv, config_path, sizeof(config_path),
                                     &has_explicit_path) != 0) {
        fprintf(stderr, "--config 需要且只能指定一个文件路径\n");
        usage(argv[0]);
        return 2;
    }

    int config_rc = has_explicit_path ? kvs_agent_config_load_file(&config, config_path)
                                      : kvs_agent_config_load_default(&config);
    if (config_rc < 0) {
        fprintf(stderr, "无法加载 ebpf-agent 配置%s%s\n",
                has_explicit_path ? ": " : "（默认文件）",
                has_explicit_path ? config_path : "");
        return 2;
    }

    int parse_rc = kvs_agent_config_parse_args(&config, argc, argv);
    if (parse_rc != 0) {
        if (parse_rc < 0) {
            fprintf(stderr, "未知参数或参数值非法\n");
        }
        usage(argv[0]);
        return 2;
    }
    if (kvs_agent_config_validate(&config) != 0) {
        fprintf(stderr, "配置不完整：需要 master 地址、listen 地址及有效复制资源上限");
        if (config.mode == KVS_EBPF_MODE_SIM) {
            fprintf(stderr, "、--ring");
        } else {
            fprintf(stderr, "、--pid 与 --binary");
        }
        fprintf(stderr, "\n");
        usage(argv[0]);
        return 2;
    }

    snprintf(g_master_ip, sizeof(g_master_ip), "%s", config.master_ip);
    g_master_port = config.master_port;
    snprintf(g_listen_ip, sizeof(g_listen_ip), "%s", config.listen_ip);
    g_listen_port = config.listen_port;

    kvs::ReplicaHubConfig hub_config;
    hub_config.max_replicas = (size_t)config.max_replicas;
    hub_config.cache_max_bytes = (size_t)config.cache_max_bytes;
    hub_config.cache_max_records = (size_t)config.cache_max_records;
    hub_config.outbuf_max_bytes = 64u * 1024u * 1024u;
    hub_config.memory_budget_bytes = (size_t)config.memory_budget_bytes;
    g_hub.reset(new kvs::ReplicaHub(hub_config));

    const int mode = config.mode;
    const char* ring_path = config.ring_path[0] == '\0' ? NULL : config.ring_path;
    const char* master_binary = config.master_binary[0] == '\0' ? NULL : config.master_binary;
    const int master_pid = config.master_pid;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    if (kvs_ebpf_agent_capture_start(mode, ring_path, master_pid, master_binary,
                                     on_capture_record, on_capture_resync) != 0) {
        fprintf(stderr, "[EBPF-AGENT] capture start failed\n");
        return 1;
    }
    agent_log("capture started (mode=%s), master=%s:%d listen=%s:%d max_replicas=%zu "
              "memory_budget=%zu",
              mode == KVS_EBPF_MODE_SIM ? "sim" : "uprobe", g_master_ip, g_master_port,
              g_listen_ip, g_listen_port, hub_config.max_replicas,
              hub_config.memory_budget_bytes);

    int replica_listen_fd = open_replica_listener(g_listen_ip, g_listen_port);
    if (replica_listen_fd < 0) {
        kvs_ebpf_agent_capture_stop();
        return 1;
    }
    agent_log("listening for replicas on %s:%d", g_listen_ip, g_listen_port);

    int rc = master_register_loop(&g_running, ring_path, replica_listen_fd);

    close(replica_listen_fd);
    kvs_ebpf_agent_capture_stop();
    g_hub->clear();
    agent_log("stopped");
    return rc;
}
