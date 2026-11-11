#pragma once

#include "sync/replica_increment.h"
#include "sync/sync_types.h"

#include <pthread.h>

namespace kvs {
namespace sync {

class ReplicaSyncCoordinator {
    public:
    ReplicaSyncCoordinator(const SyncConfig& config, const ReplicaDependencies& dependencies);
    ~ReplicaSyncCoordinator();

    ReplicaSyncCoordinator(const ReplicaSyncCoordinator&) = delete;
    ReplicaSyncCoordinator& operator=(const ReplicaSyncCoordinator&) = delete;

    void set_master_addr(const std::string& ip, int port);
    bool start();
    void stop();
    bool connected() const;
    bool wait_closed(int timeout_ms);

    private:
    static void* thread_entry(void* argument);
    void run();
    int run_once(int control_fd);
    int resolve_endpoint(int fd, std::string* ip, int* port) const;
    int send_sync(int fd, const std::string& ip, int port);
    int send_finished(int fd, const std::string& session_id);
    int connect_increment(const std::string& session_id);

    SyncConfig config_;
    ReplicaDependencies dependencies_;
    pthread_t thread_;
    volatile int running_;
    bool thread_started_;
    ReplicaIncrementChannel increment_;
};

} // namespace sync
} // namespace kvs
