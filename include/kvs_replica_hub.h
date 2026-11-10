#pragma once

#include <stddef.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvs {

enum class ReplicaState {
    Missing = 0,
    AwaitingSync,
    Caching,
    Live
};

struct ReplicaHubConfig {
    size_t max_replicas;
    size_t cache_max_bytes;
    size_t cache_max_records;
    size_t outbuf_max_bytes;
    size_t memory_budget_bytes;

    ReplicaHubConfig();
};

struct ReplicaHubStats {
    size_t targets;
    size_t waiting;
    size_t caching;
    size_t live;
    size_t cached_records;
    size_t queued_bytes;
    unsigned long long forwarded;
    unsigned long long failed;
};

// 单 Agent 多目标扇出核心。所有从机 fd、缓存和发送缓冲都由该对象拥有。
// 方法可在采集线程与 Agent 轮询线程之间并发调用。
class ReplicaHub {
public:
    explicit ReplicaHub(const ReplicaHubConfig& config);
    ~ReplicaHub();

    ReplicaHub(const ReplicaHub&) = delete;
    ReplicaHub& operator=(const ReplicaHub&) = delete;

    // 握手可能早于采集线程消费到 SYNC，因此未知 ID 会创建 AwaitingSync 占位目标。
    bool attach(const std::string& session_id, int fd);
    void detach_fd(int fd);

    bool on_sync(const std::string& session_id);
    bool on_finished(const std::string& session_id);
    bool on_abort(const std::string& session_id);
    // 返回 true 表示至少一个目标因队列/内存超限或发送失败被移除。
    bool on_write(int argc, char* argv[]);
    void on_capture_resync(const char* reason);
    void on_invalidate();

    void flush();
    void clear();

    ReplicaState state(const std::string& session_id) const;
    size_t target_count() const;
    size_t cached_records(const std::string& session_id) const;
    void fds(std::vector<int>* output) const;
    ReplicaHubStats stats() const;

private:
    struct Target {
        ReplicaState state;
        int fd;
        std::vector<std::string> cache;
        std::string outbuf;
        size_t outbuf_off;
    };

    bool attach_locked(const std::string& session_id, int fd);
    bool on_sync_locked(const std::string& session_id);
    bool on_finished_locked(const std::string& session_id);
    bool on_abort_locked(const std::string& session_id);
    bool on_write_locked(int argc, char* argv[]);
    void clear_locked();
    void drop_target_locked(const std::string& session_id, const char* reason);
    bool enqueue_locked(const std::string& session_id, const std::string& payload);
    void flush_target_locked(const std::string& session_id);
    void flush_all_locked();
    size_t cached_bytes_locked(const Target& target) const;
    size_t queued_bytes_locked() const;
    static bool encode_command(int argc, char* argv[], std::string* output);

    ReplicaHubConfig config_;
    std::unordered_map<std::string, Target> targets_;
    mutable std::mutex mutex_;
    unsigned long long forwarded_;
    unsigned long long failed_;
};

} // namespace kvs
