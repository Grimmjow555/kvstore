#pragma once

#include "sync/sync_types.h"

#include <pthread.h>

namespace kvs {
namespace sync {

class ReplicaIncrementChannel {
    public:
    ReplicaIncrementChannel();
    ~ReplicaIncrementChannel();

    ReplicaIncrementChannel(const ReplicaIncrementChannel&) = delete;
    ReplicaIncrementChannel& operator=(const ReplicaIncrementChannel&) = delete;

    bool start(int fd, const ReplayCommandFn& replay);
    bool wait_closed(int timeout_ms);
    bool connected() const;
    void stop();

    private:
    static void* reader_entry(void* argument);
    void reader_loop();
    int read_exact(void* buffer, size_t length);
    void mark_closed();

    mutable pthread_mutex_t mutex_;
    pthread_cond_t condition_;
    pthread_t thread_;
    volatile bool running_;
    bool thread_started_;
    int fd_;
    ReplayCommandFn replay_;
};

} // namespace sync
} // namespace kvs
