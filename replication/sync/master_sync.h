#pragma once

#include "sync/sync_types.h"

#include <pthread.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvs {
namespace sync {

class ManagedChildProcess;

class MasterSyncCoordinator {
    public:
    MasterSyncCoordinator(const SyncConfig& config, const MasterDependencies& dependencies);
    ~MasterSyncCoordinator();

    MasterSyncCoordinator(const MasterSyncCoordinator&) = delete;
    MasterSyncCoordinator& operator=(const MasterSyncCoordinator&) = delete;

    bool start();
    void stop();

    bool accept_agent(int fd, const char* payload, int length);
    bool agent_armed() const;
    void forget_fd(int fd);

    SyncReply handle_sync(int control_fd, char* tokens[], int count);
    SyncReply handle_finished(int control_fd, char* tokens[], int count);
    size_t session_count() const;

    private:
    enum SessionState {
        SessionPreparing = 0,
        SessionAwaitingFinished = 1,
    };

    struct Session {
        std::string session_id;
        int control_fd;
        std::string snapshot_path;
        unsigned long long deadline_ms;
        SessionState state;
        std::unique_ptr<ManagedChildProcess> client;
    };

    static void* reaper_entry(void* argument);
    void reaper_loop();
    void abort_session(const std::string& session_id, const char* reason);
    void abort_all(const char* reason);
    std::string write_snapshot(const std::string& session_id, char* data, size_t length);

    SyncConfig config_;
    MasterDependencies dependencies_;
    std::unordered_map<std::string, Session> sessions_;
    std::unordered_map<int, std::string> control_sessions_;
    mutable pthread_mutex_t mutex_;
    pthread_cond_t condition_;
    std::atomic<unsigned long long> next_session_id_;
    volatile bool running_;
    bool reaper_started_;
    pthread_t reaper_thread_;
    std::atomic<int> agent_fd_;
};

} // namespace sync
} // namespace kvs
