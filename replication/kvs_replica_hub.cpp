#include "kvs_replica_hub.h"
#include "kvs_replication_protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

namespace kvs {

ReplicaHubConfig::ReplicaHubConfig()
    : max_replicas(16),
      cache_max_bytes(256u * 1024u * 1024u),
      cache_max_records(500000),
      outbuf_max_bytes(64u * 1024u * 1024u),
      memory_budget_bytes(1024u * 1024u * 1024u) {}

ReplicaHub::ReplicaHub(const ReplicaHubConfig& config)
    : config_(config), forwarded_(0), failed_(0) {}

ReplicaHub::~ReplicaHub() { clear(); }

bool ReplicaHub::attach(const std::string& session_id, int fd) {
    std::lock_guard<std::mutex> lock(mutex_);
    return attach_locked(session_id, fd);
}

bool ReplicaHub::attach_locked(const std::string& session_id, int fd) {
    if (fd < 0) {
        return false;
    }
    std::unordered_map<std::string, Target>::iterator found = targets_.find(session_id);
    if (found != targets_.end()) {
        if (found->second.fd >= 0 && found->second.fd != fd) {
            close(found->second.fd);
        }
        found->second.fd = fd;
        found->second.outbuf.clear();
        found->second.outbuf_off = 0;
        return true;
    }
    if (targets_.size() >= config_.max_replicas) {
        return false;
    }
    Target target;
    target.state = ReplicaState::AwaitingSync;
    target.fd = fd;
    target.outbuf_off = 0;
    targets_.insert(std::make_pair(session_id, target));
    return true;
}

void ReplicaHub::detach_fd(int fd) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (std::unordered_map<std::string, Target>::iterator it = targets_.begin();
         it != targets_.end(); ++it) {
        if (it->second.fd == fd) {
            std::string session_id = it->first;
            drop_target_locked(session_id, "replica disconnected");
            return;
        }
    }
}

bool ReplicaHub::on_sync(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return on_sync_locked(session_id);
}

bool ReplicaHub::on_sync_locked(const std::string& session_id) {
    std::unordered_map<std::string, Target>::iterator found = targets_.find(session_id);
    if (found == targets_.end()) {
        if (targets_.size() >= config_.max_replicas) {
            return false;
        }
        Target target;
        target.state = ReplicaState::Caching;
        target.fd = -1;
        target.outbuf_off = 0;
        targets_.insert(std::make_pair(session_id, target));
        return true;
    }

    found->second.state = ReplicaState::Caching;
    found->second.cache.clear();
    found->second.outbuf.clear();
    found->second.outbuf_off = 0;
    return true;
}

bool ReplicaHub::on_finished(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return on_finished_locked(session_id);
}

bool ReplicaHub::on_finished_locked(const std::string& session_id) {
    std::unordered_map<std::string, Target>::iterator found = targets_.find(session_id);
    if (found == targets_.end() || found->second.state != ReplicaState::Caching) {
        return false;
    }
    if (found->second.fd < 0) {
        drop_target_locked(session_id, "SYNC FINISHED without replica channel");
        return false;
    }

    std::vector<std::string> cached;
    cached.swap(found->second.cache);
    for (size_t i = 0; i < cached.size(); ++i) {
        if (!enqueue_locked(session_id, cached[i])) {
            return false;
        }
    }

    found = targets_.find(session_id);
    if (found == targets_.end()) {
        return false;
    }
    found->second.state = ReplicaState::Live;
    return true;
}

bool ReplicaHub::on_abort(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return on_abort_locked(session_id);
}

bool ReplicaHub::on_abort_locked(const std::string& session_id) {
    if (targets_.find(session_id) == targets_.end()) {
        return false;
    }
    drop_target_locked(session_id, "SYNC ABORT");
    return true;
}

bool ReplicaHub::encode_command(int argc, char* argv[], std::string* output) {
    if (argc <= 0 || argv == NULL || output == NULL) {
        return false;
    }
    size_t size = 32;
    std::vector<const char*> args;
    args.reserve((size_t)argc);
    for (int i = 0; i < argc; ++i) {
        if (argv[i] == NULL) {
            return false;
        }
        args.push_back(argv[i]);
        size += strlen(argv[i]) + 32;
    }
    output->resize(size);
    size_t length = 0;
    int rc = kvs_repl_build_resp_array(args.data(), argc, &(*output)[0], size, &length);
    output->resize(rc == 0 ? length : 0);
    return rc == 0;
}

bool ReplicaHub::on_write(int argc, char* argv[]) {
    std::lock_guard<std::mutex> lock(mutex_);
    return on_write_locked(argc, argv);
}

bool ReplicaHub::on_write_locked(int argc, char* argv[]) {
    std::string payload;
    if (!encode_command(argc, argv, &payload)) {
        return false;
    }
    bool dropped_target = false;

    std::vector<std::string> ids;
    ids.reserve(targets_.size());
    for (std::unordered_map<std::string, Target>::const_iterator it = targets_.begin();
         it != targets_.end(); ++it) {
        ids.push_back(it->first);
    }

    for (size_t i = 0; i < ids.size(); ++i) {
        std::unordered_map<std::string, Target>::iterator found = targets_.find(ids[i]);
        if (found == targets_.end()) {
            continue;
        }
        Target& target = found->second;
        if (target.state == ReplicaState::Caching) {
            size_t target_bytes = cached_bytes_locked(target);
            if (target.cache.size() >= config_.cache_max_records ||
                target_bytes + payload.size() > config_.cache_max_bytes ||
                queued_bytes_locked() + payload.size() > config_.memory_budget_bytes) {
                ++failed_;
                drop_target_locked(ids[i], "cache overflow");
                dropped_target = true;
                continue;
            }
            target.cache.push_back(payload);
        } else if (target.state == ReplicaState::Live) {
            if (enqueue_locked(ids[i], payload)) {
                ++forwarded_;
            } else {
                ++failed_;
                dropped_target = true;
            }
        }
    }
    return dropped_target;
}

void ReplicaHub::on_capture_resync(const char* reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    (void)reason;
    clear_locked();
}

void ReplicaHub::clear_locked() {
    for (std::unordered_map<std::string, Target>::iterator it = targets_.begin();
         it != targets_.end(); ++it) {
        if (it->second.fd >= 0) {
            close(it->second.fd);
        }
    }
    targets_.clear();
}

void ReplicaHub::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    clear_locked();
}

void ReplicaHub::drop_target_locked(const std::string& session_id, const char* reason) {
    std::unordered_map<std::string, Target>::iterator found = targets_.find(session_id);
    if (found == targets_.end()) {
        return;
    }
    if (found->second.fd >= 0) {
        close(found->second.fd);
    }
    (void)reason;
    targets_.erase(found);
}

bool ReplicaHub::enqueue_locked(const std::string& session_id, const std::string& payload) {
    std::unordered_map<std::string, Target>::iterator found = targets_.find(session_id);
    if (found == targets_.end() || found->second.fd < 0) {
        return false;
    }
    Target& target = found->second;
    size_t pending = target.outbuf.size() - target.outbuf_off;
    if (pending + payload.size() + 4 > config_.outbuf_max_bytes ||
        queued_bytes_locked() + payload.size() + 4 > config_.memory_budget_bytes) {
        drop_target_locked(session_id, "outbuf overflow");
        return false;
    }

    uint32_t net_len = htonl((uint32_t)payload.size());
    target.outbuf.append(reinterpret_cast<const char*>(&net_len), sizeof(net_len));
    target.outbuf.append(payload);
    flush_target_locked(session_id);
    return targets_.find(session_id) != targets_.end();
}

void ReplicaHub::flush_target_locked(const std::string& session_id) {
    std::unordered_map<std::string, Target>::iterator found = targets_.find(session_id);
    if (found == targets_.end() || found->second.fd < 0) {
        return;
    }
    Target& target = found->second;
    size_t offset = target.outbuf_off;
    const size_t total = target.outbuf.size();
    while (offset < total) {
        ssize_t sent =
            send(target.fd, target.outbuf.data() + offset, total - offset,
                 MSG_NOSIGNAL | MSG_DONTWAIT);
        if (sent > 0) {
            offset += (size_t)sent;
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            break;
        }
        std::string id = session_id;
        drop_target_locked(id, "send failed");
        return;
    }

    target.outbuf_off = offset;
    if (offset == total) {
        target.outbuf.clear();
        target.outbuf_off = 0;
    } else if (offset >= (1u << 20)) {
        target.outbuf.erase(0, offset);
        target.outbuf_off = 0;
    }
}

void ReplicaHub::flush_all_locked() {
    std::vector<std::string> ids;
    ids.reserve(targets_.size());
    for (std::unordered_map<std::string, Target>::const_iterator it = targets_.begin();
         it != targets_.end(); ++it) {
        ids.push_back(it->first);
    }
    for (size_t i = 0; i < ids.size(); ++i) {
        flush_target_locked(ids[i]);
    }
}

void ReplicaHub::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    flush_all_locked();
}

size_t ReplicaHub::cached_bytes_locked(const Target& target) const {
    size_t total = 0;
    for (size_t i = 0; i < target.cache.size(); ++i) {
        total += target.cache[i].size();
    }
    return total;
}

size_t ReplicaHub::queued_bytes_locked() const {
    size_t total = 0;
    for (std::unordered_map<std::string, Target>::const_iterator it = targets_.begin();
         it != targets_.end(); ++it) {
        total += cached_bytes_locked(it->second);
        total += it->second.outbuf.size() - it->second.outbuf_off;
    }
    return total;
}

ReplicaState ReplicaHub::state(const std::string& session_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, Target>::const_iterator found = targets_.find(session_id);
    return found == targets_.end() ? ReplicaState::Missing : found->second.state;
}

size_t ReplicaHub::target_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return targets_.size();
}

size_t ReplicaHub::cached_records(const std::string& session_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, Target>::const_iterator found = targets_.find(session_id);
    return found == targets_.end() ? 0 : found->second.cache.size();
}

void ReplicaHub::fds(std::vector<int>* output) const {
    if (output == NULL) {
        return;
    }
    output->clear();
    std::lock_guard<std::mutex> lock(mutex_);
    for (std::unordered_map<std::string, Target>::const_iterator it = targets_.begin();
         it != targets_.end(); ++it) {
        if (it->second.fd >= 0) {
            output->push_back(it->second.fd);
        }
    }
}

ReplicaHubStats ReplicaHub::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    ReplicaHubStats stats_value;
    stats_value.targets = targets_.size();
    stats_value.waiting = 0;
    stats_value.caching = 0;
    stats_value.live = 0;
    stats_value.cached_records = 0;
    stats_value.queued_bytes = queued_bytes_locked();
    stats_value.forwarded = forwarded_;
    stats_value.failed = failed_;
    for (std::unordered_map<std::string, Target>::const_iterator it = targets_.begin();
         it != targets_.end(); ++it) {
        if (it->second.state == ReplicaState::AwaitingSync) {
            ++stats_value.waiting;
        } else if (it->second.state == ReplicaState::Caching) {
            ++stats_value.caching;
        } else if (it->second.state == ReplicaState::Live) {
            ++stats_value.live;
        }
        stats_value.cached_records += it->second.cache.size();
    }
    return stats_value;
}

} // namespace kvs
