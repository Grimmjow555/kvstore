#pragma once

#include <stddef.h>

#include <functional>
#include <string>

namespace kvs {
namespace sync {

struct SyncConfig {
    int rdma_port;
    int agent_port;
    int max_replicas;
    int session_timeout_ms;
    int rdma_transfer_timeout_ms;
    int sync_wait_timeout_ms;
    std::string rdma_client_bin;
    std::string rdma_server_bin;
    std::string master_ip;
    int master_port;
    std::string rdma_advertise_host;
};

struct SyncReply {
    bool ok;
    std::string payload;
};

typedef std::function<int(char**, size_t*)> SnapshotSerializeFn;
typedef std::function<int(const char*)> SnapshotLoadFn;
typedef std::function<void()> DataLockFn;
typedef std::function<void()> DataUnlockFn;
typedef std::function<void(const char*, const std::string&)> ReplicationEventFn;
typedef std::function<void(int)> CaptureEnableFn;
typedef std::function<void(char*, int)> ReplayCommandFn;

struct MasterDependencies {
    SnapshotSerializeFn serialize;
    DataLockFn lock;
    DataUnlockFn unlock;
    ReplicationEventFn publish_event;
    CaptureEnableFn enable_capture;
};

struct ReplicaDependencies {
    SnapshotLoadFn load_snapshot;
    ReplayCommandFn replay_command;
};

} // namespace sync
} // namespace kvs
