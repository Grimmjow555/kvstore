#include "sync/replica_sync.h"

#include "kvs_netutil.h"
#include "kvs_replication_protocol.h"
#include "sync/sync_support.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

namespace kvs {
namespace sync {

namespace {

const int kConnectTimeoutMs = 3000;
const int kIoTimeoutMs = 120000;
const int kRdmaReadyTimeoutMs = 10000;
const int kBackoffMinMs = 200;
const int kBackoffMaxMs = 5000;

int send_command(int fd, const char* const argv[], int argc) {
    char payload[256];
    size_t length = 0;
    if (kvs_repl_build_resp_array(argv, argc, payload, sizeof(payload), &length) != 0) {
        return -1;
    }
    return kvs_net_send_frame(fd, payload, length, kIoTimeoutMs);
}

} // namespace

ReplicaSyncCoordinator::ReplicaSyncCoordinator(const SyncConfig& config,
                                               const ReplicaDependencies& dependencies)
    : config_(config), dependencies_(dependencies), thread_(), running_(false),
      thread_started_(false), increment_() {}

ReplicaSyncCoordinator::~ReplicaSyncCoordinator() { stop(); }

void ReplicaSyncCoordinator::set_master_addr(const std::string& ip, int port) {
    if (!ip.empty()) {
        config_.master_ip = ip;
    }
    if (port > 0) {
        config_.master_port = port;
    }
}

bool ReplicaSyncCoordinator::start() {
    if (thread_started_ || config_.master_ip.empty() || config_.master_port <= 0 ||
        config_.agent_port <= 0) {
        return false;
    }
    running_ = true;
    if (pthread_create(&thread_, NULL, thread_entry, this) != 0) {
        running_ = false;
        return false;
    }
    thread_started_ = true;
    return true;
}

void ReplicaSyncCoordinator::stop() {
    if (!thread_started_) {
        return;
    }
    running_ = false;
    increment_.stop();
    pthread_join(thread_, NULL);
    thread_started_ = false;
}

bool ReplicaSyncCoordinator::connected() const { return increment_.connected(); }

bool ReplicaSyncCoordinator::wait_closed(int timeout_ms) {
    return increment_.wait_closed(timeout_ms);
}

void* ReplicaSyncCoordinator::thread_entry(void* argument) {
    static_cast<ReplicaSyncCoordinator*>(argument)->run();
    return NULL;
}

void ReplicaSyncCoordinator::run() {
    int backoff = kBackoffMinMs;
    while (running_) {
        int fd = kvs_net_connect(config_.master_ip.c_str(), config_.master_port, kConnectTimeoutMs);
        if (fd < 0) {
            sleep_interruptible(backoff, &running_);
            backoff = backoff < kBackoffMaxMs ? backoff * 2 : kBackoffMaxMs;
            continue;
        }
        int rc = run_once(fd);
        close(fd);
        if (rc == 0) {
            backoff = kBackoffMinMs;
            if (!increment_.connected()) {
                continue;
            }
            if (!increment_.wait_closed(-1)) {
                break;
            }
            increment_.stop();
            continue;
        }
        sleep_interruptible(backoff, &running_);
        backoff = backoff < kBackoffMaxMs ? backoff * 2 : kBackoffMaxMs;
    }
}

int ReplicaSyncCoordinator::run_once(int control_fd) {
    char response[256];
    char session_id[KVS_REPL_SESSION_ID_SIZE + 1] = {0};
    std::string receive_base = replica_receive_base_path((int)getpid(), control_fd);
    std::string ready_path = replica_receive_ready_path((int)getpid(), control_fd);
    std::string server_log = replica_server_log_path((int)getpid(), control_fd);
    ManagedChildProcess server;

    unlink(ready_path.c_str());
    unlink(receive_base.c_str());
    char listen_port[16];
    char timeout_text[16];
    snprintf(listen_port, sizeof(listen_port), "%d", config_.rdma_port);
    snprintf(timeout_text, sizeof(timeout_text), "%d", config_.rdma_transfer_timeout_ms);
    std::vector<std::string> server_args;
    server_args.push_back("--listen-port");
    server_args.push_back(listen_port);
    server_args.push_back("--out");
    server_args.push_back(receive_base);
    server_args.push_back("--ready-file");
    server_args.push_back(ready_path);
    server_args.push_back("--timeout-ms");
    server_args.push_back(timeout_text);
    if (server.start(config_.rdma_server_bin, server_args, server_log) != 0) {
        return -1;
    }
    unsigned long long ready_deadline = now_ms() + kRdmaReadyTimeoutMs;
    while (running_ && access(ready_path.c_str(), F_OK) != 0) {
        if (now_ms() >= ready_deadline) {
            server.stop();
            return -1;
        }
        sleep_ms(20);
    }

    std::string advertised_ip;
    int advertised_port = 0;
    if (resolve_endpoint(control_fd, &advertised_ip, &advertised_port) != 0 ||
        send_sync(control_fd, advertised_ip, advertised_port) != 0) {
        server.stop();
        unlink(ready_path.c_str());
        unlink(receive_base.c_str());
        return -1;
    }
    if (kvs_net_recv_frame(control_fd, response, sizeof(response), kIoTimeoutMs) < 0 ||
        kvs_repl_parse_ok_session(response, (int)strlen(response), session_id,
                                  sizeof(session_id)) != 0) {
        server.stop();
        unlink(ready_path.c_str());
        unlink(receive_base.c_str());
        return -1;
    }

    ChildWaitResult wait_result = server.wait(config_.sync_wait_timeout_ms);
    if (wait_result != ChildExitedSuccess) {
        server.stop();
        unlink(ready_path.c_str());
        unlink(receive_base.c_str());
        return -1;
    }

    std::string session_path = replica_receive_path(session_id);
    if (rename(receive_base.c_str(), session_path.c_str()) != 0) {
        unlink(ready_path.c_str());
        unlink(receive_base.c_str());
        return -1;
    }
    if (!dependencies_.load_snapshot || dependencies_.load_snapshot(session_path.c_str()) != 0) {
        unlink(ready_path.c_str());
        return -1;
    }
    if (!running_) {
        unlink(ready_path.c_str());
        return -1;
    }

    int increment_fd = connect_increment(session_id);
    if (increment_fd < 0 || !increment_.start(increment_fd, dependencies_.replay_command)) {
        if (increment_fd >= 0) {
            close(increment_fd);
        }
        unlink(ready_path.c_str());
        return -1;
    }
    if (send_finished(control_fd, session_id) != 0 ||
        kvs_net_recv_frame(control_fd, response, sizeof(response), kIoTimeoutMs) < 0 ||
        strncmp(response, "+OK", 3) != 0) {
        increment_.stop();
        unlink(ready_path.c_str());
        return -1;
    }
    unlink(ready_path.c_str());
    unlink(session_path.c_str());
    return 0;
}

int ReplicaSyncCoordinator::resolve_endpoint(int fd, std::string* ip, int* port) const {
    if (fd < 0 || ip == NULL || port == NULL) {
        return -1;
    }
    if (!config_.rdma_advertise_host.empty()) {
        struct in_addr address;
        if (inet_pton(AF_INET, config_.rdma_advertise_host.c_str(), &address) != 1) {
            return -1;
        }
        *ip = config_.rdma_advertise_host;
    } else {
        struct sockaddr_in local;
        socklen_t local_size = sizeof(local);
        char buffer[INET_ADDRSTRLEN];
        if (getsockname(fd, (struct sockaddr*)&local, &local_size) != 0 ||
            local.sin_family != AF_INET ||
            inet_ntop(AF_INET, &local.sin_addr, buffer, sizeof(buffer)) == NULL) {
            return -1;
        }
        *ip = buffer;
    }
    *port = config_.rdma_port;
    return *port > 0 && *port <= 65535 ? 0 : -1;
}

int ReplicaSyncCoordinator::send_sync(int fd, const std::string& ip, int port) {
    char port_text[16];
    snprintf(port_text, sizeof(port_text), "%d", port);
    const char* argv[] = {"SYNC", ip.c_str(), port_text};
    return send_command(fd, argv, 3);
}

int ReplicaSyncCoordinator::send_finished(int fd, const std::string& session_id) {
    const char* argv[] = {"SYNC FINISHED", session_id.c_str()};
    return send_command(fd, argv, 2);
}

int ReplicaSyncCoordinator::connect_increment(const std::string& session_id) {
    char payload[256];
    size_t length = 0;
    if (kvs_repl_build_repl_inc(session_id.c_str(), payload, sizeof(payload), &length) != 0) {
        return -1;
    }
    int fd = kvs_net_connect(config_.master_ip.c_str(), config_.agent_port, kConnectTimeoutMs);
    if (fd < 0) {
        return -1;
    }
    if (kvs_net_send_frame(fd, payload, length, kIoTimeoutMs) != 0 ||
        kvs_net_recv_frame_ok(fd, kIoTimeoutMs) != 1) {
        close(fd);
        return -1;
    }
    return fd;
}

} // namespace sync
} // namespace kvs
