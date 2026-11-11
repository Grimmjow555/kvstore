#include "sync/replica_sync.h"

#include <cstdio>

static int g_failures = 0;

static void expect_true(bool condition, const char* message) {
    if (condition) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s\n", message);
    ++g_failures;
}

int main() {
    kvs::sync::SyncConfig config;
    config.rdma_port = 10000;
    config.agent_port = 11001;
    config.max_replicas = 16;
    config.session_timeout_ms = 1000;
    config.rdma_transfer_timeout_ms = 1000;
    config.sync_wait_timeout_ms = 1000;
    config.rdma_client_bin = "/bin/true";
    config.rdma_server_bin = "/bin/true";
    config.master_port = 0;

    kvs::sync::ReplicaDependencies deps;
    deps.load_snapshot = [](const char*) { return 0; };
    deps.replay_command = [](char*, int) {};
    kvs::sync::ReplicaSyncCoordinator coordinator(config, deps);
    expect_true(!coordinator.start(), "replica does not start without a master address");

    if (g_failures != 0) {
        return 1;
    }
    std::printf("replica sync tests passed\n");
    return 0;
}
