#include "sync/master_sync.h"

#include "kvs_io_uring.h"
#include "kvs_replication_protocol.h"
#include "sync/sync_support.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <vector>

namespace kvs {
namespace sync {

namespace {

const char* kMasterControlSync = "SYNC";
const char* kMasterControlFinished = "SYNC FINISHED";
const char* kMasterControlAbort = "SYNC ABORT";

bool parse_event_endpoint(char* tokens[], int count, char* ip, size_t ip_size, int* port) {
    if (count != 3 || tokens == NULL || tokens[1] == NULL || tokens[2] == NULL) {
        return false;
    }
    struct in_addr parsed_address;
    char* end = NULL;
    long parsed_port = strtol(tokens[2], &end, 10);
    if (inet_pton(AF_INET, tokens[1], &parsed_address) != 1 || end == tokens[2] || *end != '\0' ||
        parsed_port <= 0 || parsed_port > 65535 || strlen(tokens[1]) >= ip_size) {
        return false;
    }
    snprintf(ip, ip_size, "%s", tokens[1]);
    *port = (int)parsed_port;
    return true;
}

} // namespace

MasterSyncCoordinator::MasterSyncCoordinator(const SyncConfig& config,
                                             const MasterDependencies& dependencies)
    : config_(config), dependencies_(dependencies), mutex_(PTHREAD_MUTEX_INITIALIZER),
      condition_(PTHREAD_COND_INITIALIZER), next_session_id_(1), running_(false),
      reaper_started_(false), reaper_thread_(), agent_fd_(-1) {}

MasterSyncCoordinator::~MasterSyncCoordinator() { stop(); }

bool MasterSyncCoordinator::start() {
    if (running_) {
        return true;
    }
    running_ = true;
    if (pthread_create(&reaper_thread_, NULL, reaper_entry, this) != 0) {
        running_ = false;
        return false;
    }
    reaper_started_ = true;
    return true;
}

void MasterSyncCoordinator::stop() {
    if (running_) {
        pthread_mutex_lock(&mutex_);
        running_ = false;
        pthread_cond_broadcast(&condition_);
        pthread_mutex_unlock(&mutex_);
    }
    if (reaper_started_) {
        pthread_join(reaper_thread_, NULL);
        reaper_started_ = false;
    }
    abort_all("master shutting down");
}

bool MasterSyncCoordinator::accept_agent(int fd, const char* payload, int length) {
    if (fd < 0 || kvs_repl_parse_ebpf_handshake(payload, length) != 0) {
        return false;
    }
    int previous_agent = agent_fd_.load();
    if (previous_agent >= 0 && previous_agent != fd) {
        abort_all("ebpf-agent re-registered");
    }
    agent_fd_.store(fd);
    if (dependencies_.enable_capture) {
        dependencies_.enable_capture(1);
    }
    return true;
}

bool MasterSyncCoordinator::agent_armed() const { return agent_fd_.load() >= 0; }

void MasterSyncCoordinator::forget_fd(int fd) {
    if (fd < 0) {
        return;
    }
    if (agent_fd_.load() == fd) {
        agent_fd_.store(-1);
        if (dependencies_.enable_capture) {
            dependencies_.enable_capture(0);
        }
        abort_all("ebpf-agent disconnected");
        return;
    }

    std::string session_id;
    pthread_mutex_lock(&mutex_);
    std::unordered_map<int, std::string>::iterator found = control_sessions_.find(fd);
    if (found != control_sessions_.end()) {
        session_id = found->second;
    }
    pthread_mutex_unlock(&mutex_);
    if (!session_id.empty()) {
        abort_session(session_id, "control connection closed");
    }
}

SyncReply MasterSyncCoordinator::handle_sync(int control_fd, char* tokens[], int count) {
    if (!agent_armed() || control_fd < 0) {
        return SyncReply{false, build_sync_reply(false)};
    }

    char replica_ip[INET_ADDRSTRLEN] = {0};
    int replica_port = 0;
    if (!parse_event_endpoint(tokens, count, replica_ip, sizeof(replica_ip), &replica_port)) {
        return SyncReply{false, build_sync_reply(false)};
    }

    Session session;
    char session_id[KVS_REPL_SESSION_ID_SIZE + 1] = {0};
    pthread_mutex_lock(&mutex_);
    if (sessions_.size() >= (size_t)config_.max_replicas) {
        pthread_mutex_unlock(&mutex_);
        return SyncReply{false, build_sync_reply(false)};
    }
    do {
        unsigned long long candidate = next_session_id_.fetch_add(1);
        if (kvs_repl_format_session_id(candidate, session_id, sizeof(session_id)) != 0) {
            pthread_mutex_unlock(&mutex_);
            return SyncReply{false, build_sync_reply(false)};
        }
    } while (sessions_.find(session_id) != sessions_.end());

    session.session_id = session_id;
    session.control_fd = control_fd;
    session.snapshot_path = master_snapshot_path(session.session_id);
    session.deadline_ms = now_ms() + (unsigned long long)config_.session_timeout_ms;
    session.state = SessionPreparing;
    sessions_.insert(std::make_pair(session.session_id, std::move(session)));
    control_sessions_[control_fd] = session_id;
    pthread_mutex_unlock(&mutex_);

    if (dependencies_.lock) {
        dependencies_.lock();
    }
    if (dependencies_.publish_event) {
        dependencies_.publish_event(kMasterControlSync, session_id);
    }
    char* snapshot = NULL;
    size_t snapshot_length = 0;
    int serialize_rc =
        dependencies_.serialize ? dependencies_.serialize(&snapshot, &snapshot_length) : -1;
    if (dependencies_.unlock) {
        dependencies_.unlock();
    }
    if (serialize_rc != 0 || snapshot == NULL) {
        free(snapshot);
        abort_session(session_id, "snapshot serialize failed");
        return SyncReply{false, build_sync_reply(false)};
    }

    if (write_snapshot(session_id, snapshot, snapshot_length).empty()) {
        free(snapshot);
        abort_session(session_id, "snapshot write failed");
        return SyncReply{false, build_sync_reply(false)};
    }
    free(snapshot);

    std::unique_ptr<ManagedChildProcess> client(new ManagedChildProcess());
    std::vector<std::string> arguments;
    arguments.push_back("--file");
    arguments.push_back(master_snapshot_path(session_id));
    arguments.push_back("--peer");
    arguments.push_back(replica_ip);
    char port_text[16];
    snprintf(port_text, sizeof(port_text), "%d", replica_port);
    arguments.push_back("--port");
    arguments.push_back(port_text);
    arguments.push_back("--timeout-ms");
    char timeout_text[16];
    snprintf(timeout_text, sizeof(timeout_text), "%d", config_.rdma_transfer_timeout_ms);
    arguments.push_back(timeout_text);
    std::string log_path = master_snapshot_path(session_id) + ".rdma-client.log";
    if (client->start(config_.rdma_client_bin, arguments, log_path) != 0) {
        abort_session(session_id, "rdma-client spawn failed");
        return SyncReply{false, build_sync_reply(false)};
    }

    pthread_mutex_lock(&mutex_);
    std::unordered_map<std::string, Session>::iterator active = sessions_.find(session_id);
    if (active == sessions_.end()) {
        pthread_mutex_unlock(&mutex_);
        client->stop();
        unlink(master_snapshot_path(session_id).c_str());
        return SyncReply{false, build_sync_reply(false)};
    }
    active->second.client = std::move(client);
    active->second.state = SessionAwaitingFinished;
    pthread_mutex_unlock(&mutex_);
    return SyncReply{true, build_sync_reply(true, session_id)};
}

SyncReply MasterSyncCoordinator::handle_finished(int control_fd, char* tokens[], int count) {
    if (tokens == NULL || count != 2 || !kvs_repl_validate_session_id(tokens[1])) {
        return SyncReply{false, build_sync_reply(false)};
    }

    Session session;
    pthread_mutex_lock(&mutex_);
    std::unordered_map<std::string, Session>::iterator found = sessions_.find(tokens[1]);
    if (found == sessions_.end() || found->second.control_fd != control_fd ||
        found->second.state != SessionAwaitingFinished) {
        pthread_mutex_unlock(&mutex_);
        return SyncReply{false, build_sync_reply(false)};
    }
    session.session_id = found->second.session_id;
    session.snapshot_path = found->second.snapshot_path;
    session.client = std::move(found->second.client);
    sessions_.erase(found);
    control_sessions_.erase(control_fd);
    pthread_cond_broadcast(&condition_);
    pthread_mutex_unlock(&mutex_);

    if (dependencies_.lock) {
        dependencies_.lock();
    }
    if (dependencies_.publish_event) {
        dependencies_.publish_event(kMasterControlFinished, session.session_id);
    }
    if (dependencies_.unlock) {
        dependencies_.unlock();
    }
    if (session.client) {
        session.client->stop();
    }
    unlink(session.snapshot_path.c_str());
    unlink((session.snapshot_path + ".partial").c_str());
    return SyncReply{true, build_sync_reply(true)};
}

size_t MasterSyncCoordinator::session_count() const {
    pthread_mutex_lock(&mutex_);
    size_t count = sessions_.size();
    pthread_mutex_unlock(&mutex_);
    return count;
}

void* MasterSyncCoordinator::reaper_entry(void* argument) {
    static_cast<MasterSyncCoordinator*>(argument)->reaper_loop();
    return NULL;
}

void MasterSyncCoordinator::reaper_loop() {
    pthread_mutex_lock(&mutex_);
    while (running_) {
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += 1;
        pthread_cond_timedwait(&condition_, &mutex_, &deadline);

        unsigned long long current = now_ms();
        std::vector<std::string> expired;
        for (std::unordered_map<std::string, Session>::const_iterator it = sessions_.begin();
             it != sessions_.end(); ++it) {
            if (it->second.deadline_ms <= current) {
                expired.push_back(it->first);
            }
        }
        pthread_mutex_unlock(&mutex_);
        for (size_t i = 0; i < expired.size(); ++i) {
            abort_session(expired[i], "session timeout");
        }
        pthread_mutex_lock(&mutex_);
    }
    pthread_mutex_unlock(&mutex_);
}

void MasterSyncCoordinator::abort_session(const std::string& session_id, const char* reason) {
    Session session;
    pthread_mutex_lock(&mutex_);
    std::unordered_map<std::string, Session>::iterator found = sessions_.find(session_id);
    if (found == sessions_.end()) {
        pthread_mutex_unlock(&mutex_);
        return;
    }
    session.session_id = found->second.session_id;
    session.control_fd = found->second.control_fd;
    session.snapshot_path = found->second.snapshot_path;
    session.client = std::move(found->second.client);
    sessions_.erase(found);
    control_sessions_.erase(session.control_fd);
    pthread_cond_broadcast(&condition_);
    pthread_mutex_unlock(&mutex_);

    if (dependencies_.lock) {
        dependencies_.lock();
    }
    if (dependencies_.publish_event) {
        dependencies_.publish_event(kMasterControlAbort, session.session_id);
    }
    if (dependencies_.unlock) {
        dependencies_.unlock();
    }
    if (session.client) {
        session.client->stop();
    }
    unlink(session.snapshot_path.c_str());
    unlink((session.snapshot_path + ".partial").c_str());
    (void)reason;
}

void MasterSyncCoordinator::abort_all(const char* reason) {
    std::vector<std::string> ids;
    pthread_mutex_lock(&mutex_);
    for (std::unordered_map<std::string, Session>::const_iterator it = sessions_.begin();
         it != sessions_.end(); ++it) {
        ids.push_back(it->first);
    }
    pthread_mutex_unlock(&mutex_);
    for (size_t i = 0; i < ids.size(); ++i) {
        abort_session(ids[i], reason);
    }
}

std::string MasterSyncCoordinator::write_snapshot(const std::string& session_id, char* data,
                                                  size_t length) {
    std::string path = master_snapshot_path(session_id);
    std::string partial = path + ".partial";
    kvs_io_uring_file_t* file =
        kvs_io_uring_open(partial.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (file == NULL) {
        return std::string();
    }
    int rc = kvs_io_uring_write_and_fsync(file, data, length);
    kvs_io_uring_close(file);
    if (rc != 0 || rename(partial.c_str(), path.c_str()) != 0) {
        unlink(partial.c_str());
        return std::string();
    }
    return path;
}

} // namespace sync
} // namespace kvs
