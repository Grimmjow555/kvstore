#include "agent/agent_service.h"

#include "kvs_ebpf_ring.h"
#include "kvs_netutil.h"
#include "kvs_replica_hub.h"
#include "kvs_replication.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <cstdarg>
#include <string>
#include <vector>

namespace kvs {
namespace agent {

namespace {

pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;
const int kHandshakeTimeoutMs = 3000;

void agent_log(const char* fmt, ...) {
    char timestamp[32];
    time_t now = time(NULL);
    struct tm value;
    memset(&value, 0, sizeof(value));
    localtime_r(&now, &value);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &value);

    pthread_mutex_lock(&g_log_mutex);
    fprintf(stderr, "[%s][EBPF-AGENT] ", timestamp);
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
    fflush(stderr);
    pthread_mutex_unlock(&g_log_mutex);
}

} // namespace

AgentService::AgentService(const kvs_agent_config_t& config)
    : config_(config), capture_(new capture::CaptureRuntime()), hub_() {}

AgentService::~AgentService() {
    if (capture_) {
        capture_->stop();
    }
    if (hub_) {
        hub_->clear();
    }
}

int AgentService::run(volatile sig_atomic_t* running) {
    capture::CaptureConfig capture_config;
    capture_config.mode = config_.mode;
    capture_config.ring_path = config_.ring_path[0] == '\0' ? NULL : config_.ring_path;
    capture_config.master_pid = config_.master_pid;
    capture_config.master_binary = config_.master_binary[0] == '\0' ? NULL : config_.master_binary;

    ReplicaHubConfig hub_config;
    hub_config.max_replicas = (size_t)config_.max_replicas;
    hub_config.cache_max_bytes = (size_t)config_.cache_max_bytes;
    hub_config.cache_max_records = (size_t)config_.cache_max_records;
    hub_config.outbuf_max_bytes = 64u * 1024u * 1024u;
    hub_config.memory_budget_bytes = (size_t)config_.memory_budget_bytes;
    hub_.reset(new ReplicaHub(hub_config));

    if (capture_->start(
            capture_config, [this](int argc, char* argv[]) { on_capture_record(argc, argv); },
            [this](const char* reason) { on_capture_resync(reason); }) != 0) {
        return 1;
    }

    int listen_fd = open_listener();
    if (listen_fd < 0) {
        capture_->stop();
        return 1;
    }
    int rc = master_register_loop(running, listen_fd);
    close(listen_fd);
    capture_->stop();
    hub_->clear();
    return rc;
}

void AgentService::on_capture_record(int argc, char* argv[]) {
    if (!hub_ || argc <= 0 || argv == NULL) {
        return;
    }
    if (argc == 2 && strcmp(argv[0], "SYNC") == 0 && kvs_repl_validate_session_id(argv[1])) {
        hub_->on_sync(argv[1]);
        return;
    }
    if (argc == 2 && strcmp(argv[0], "SYNC FINISHED") == 0 &&
        kvs_repl_validate_session_id(argv[1])) {
        hub_->on_finished(argv[1]);
        return;
    }
    if (argc == 2 && strcmp(argv[0], "SYNC ABORT") == 0 && kvs_repl_validate_session_id(argv[1])) {
        hub_->on_abort(argv[1]);
        return;
    }
    if ((argc == 1 && (strcmp(argv[0], "SYNC") == 0 || strcmp(argv[0], "SYNC FINISHED") == 0 ||
                       strcmp(argv[0], "INVALIDATE") == 0)) ||
        (argc > 1 && (strcmp(argv[0], "SYNC") == 0 || strcmp(argv[0], "SYNC FINISHED") == 0 ||
                      strcmp(argv[0], "SYNC ABORT") == 0))) {
        agent_log("ignored legacy or invalid replication control record");
        return;
    }
    if (hub_->on_write(argc, argv)) {
        agent_log("cache overflow or outbuf overflow dropped a slow target");
    }
}

void AgentService::on_capture_resync(const char* reason) {
    if (hub_) {
        hub_->on_capture_resync(reason);
    }
    agent_log("capture needs resync (%s): dropping all replica targets",
              reason == NULL ? "unknown" : reason);
}

int AgentService::open_listener() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)config_.listen_port);
    if (inet_pton(AF_INET, config_.listen_ip, &address.sin_addr) != 1 ||
        bind(fd, (struct sockaddr*)&address, sizeof(address)) != 0 || listen(fd, 16) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

void AgentService::accept_replica_connection(int listen_fd) {
    struct sockaddr_in peer;
    socklen_t peer_size = sizeof(peer);
    int fd = accept(listen_fd, (struct sockaddr*)&peer, &peer_size);
    if (fd < 0) {
        return;
    }
    char payload[256];
    int length = kvs_net_recv_frame(fd, payload, sizeof(payload), kHandshakeTimeoutMs);
    char session_id[KVS_REPL_SESSION_ID_SIZE + 1] = {0};
    if (kvs_repl_parse_repl_inc(payload, length, session_id, sizeof(session_id)) != 0 ||
        !hub_->attach(session_id, fd)) {
        kvs_net_send_frame(fd, "-ERROR\r\n", strlen("-ERROR\r\n"), kHandshakeTimeoutMs);
        close(fd);
        return;
    }
    if (kvs_net_send_frame(fd, "+OK\r\n", strlen("+OK\r\n"), kHandshakeTimeoutMs) != 0) {
        hub_->detach_fd(fd);
    }
}

int AgentService::service_channels(int listen_fd, int master_fd, int timeout_ms) {
    std::vector<int> replica_fds;
    hub_->fds(&replica_fds);
    std::vector<struct pollfd> pfds;
    pfds.push_back({master_fd, POLLIN, 0});
    pfds.push_back({listen_fd, POLLIN, 0});
    for (size_t i = 0; i < replica_fds.size(); ++i) {
        pfds.push_back({replica_fds[i], POLLIN, 0});
    }
    int ready = poll(&pfds[0], (nfds_t)pfds.size(), timeout_ms);
    if (ready < 0 && errno != EINTR) {
        return 0;
    }
    if (ready > 0 && master_fd >= 0 && (pfds[0].revents & (POLLIN | POLLHUP | POLLERR))) {
        char tmp[64];
        if (recv(master_fd, tmp, sizeof(tmp), 0) <= 0) {
            return 0;
        }
    }
    if (ready > 0 && (pfds[1].revents & POLLIN)) {
        accept_replica_connection(listen_fd);
    }
    for (size_t i = 0; ready > 0 && i < replica_fds.size(); ++i) {
        if (pfds[i + 2].revents != 0) {
            hub_->detach_fd(replica_fds[i]);
        }
    }
    hub_->flush();
    return 1;
}

int AgentService::master_register_loop(volatile sig_atomic_t* running, int listen_fd) {
    char handshake[128];
    size_t handshake_len = 0;
    if (kvs_repl_build_ebpf_handshake(handshake, sizeof(handshake), &handshake_len) != 0) {
        return 2;
    }
    bool first_registration = true;
    while (*running) {
        int master_fd =
            kvs_net_connect(config_.master_ip, config_.master_port, kHandshakeTimeoutMs);
        if (master_fd < 0) {
            service_channels(listen_fd, -1, 1000);
            continue;
        }
        if (kvs_net_send_frame(master_fd, handshake, handshake_len, kHandshakeTimeoutMs) != 0 ||
            kvs_net_recv_frame_ok(master_fd, kHandshakeTimeoutMs) != 1) {
            close(master_fd);
            service_channels(listen_fd, -1, 1000);
            continue;
        }
        if (!first_registration) {
            hub_->clear();
            if (capture_->reopen(config_.ring_path) != 0) {
                close(master_fd);
                return 3;
            }
        }
        first_registration = false;
        while (*running && service_channels(listen_fd, master_fd, 10)) {
            log_stats();
        }
        hub_->clear();
        close(master_fd);
        sleep(1);
    }
    return 0;
}

void AgentService::log_stats() {
    static unsigned long long last_tick = 0;
    unsigned long long now = (unsigned long long)time(NULL);
    if (now == last_tick) {
        return;
    }
    last_tick = now;
    ReplicaHubStats stats = hub_->stats();
    agent_log("stats: targets=%zu waiting=%zu caching=%zu live=%zu cached=%zu queued=%zu "
              "forwarded=%llu failed=%llu ring_dropped=%llu",
              stats.targets, stats.waiting, stats.caching, stats.live, stats.cached_records,
              stats.queued_bytes, stats.forwarded, stats.failed,
              (unsigned long long)kvs_ebpf_ring_dropped());
}

} // namespace agent
} // namespace kvs
