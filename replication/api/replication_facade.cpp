#include "kvs_replication.h"

#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_runtime.h"
#include "kvs_snapshot.h"
#include "sync/master_sync.h"
#include "sync/replica_sync.h"

#include <stdio.h>
#include <string.h>

#include <memory>
#include <string>

extern int kvs_protocol(int client_fd, char* msg, int length, char* response, int response_size);

namespace {

std::unique_ptr<kvs::sync::MasterSyncCoordinator> g_master;
std::unique_ptr<kvs::sync::ReplicaSyncCoordinator> g_replica;

kvs::sync::SyncConfig make_sync_config() {
    kvs::sync::SyncConfig config;
    config.rdma_port = kvs_config_rdma_port();
    config.agent_port = kvs_config_agent_port();
    config.max_replicas = kvs_config_max_replicas();
    config.session_timeout_ms = kvs_config_sync_session_timeout_ms();
    config.rdma_transfer_timeout_ms = kvs_config_rdma_transfer_timeout_ms();
    config.sync_wait_timeout_ms = kvs_config_sync_wait_timeout_ms();
    config.rdma_client_bin = kvs_config_rdma_client_bin();
    config.rdma_server_bin = kvs_config_rdma_server_bin();
    const char* master_ip = kvs_config_master_ip();
    const char* advertise_host = kvs_config_rdma_advertise_host();
    config.master_ip = master_ip != nullptr ? master_ip : "";
    config.master_port = kvs_config_master_port();
    config.rdma_advertise_host = advertise_host != nullptr ? advertise_host : "";
    return config;
}

void publish_replication_event(const char* event, const std::string& session_id) {
    char* argv[] = {const_cast<char*>(event), const_cast<char*>(session_id.c_str())};
    kvs_ebpf_notify_command(2, argv);
}

int copy_reply(const kvs::sync::SyncReply& reply, char* response, int response_size) {
    if (response == nullptr || response_size <= 0) {
        return -1;
    }
    if ((int)reply.payload.size() >= response_size) {
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    memcpy(response, reply.payload.data(), reply.payload.size());
    return (int)reply.payload.size();
}

} // namespace

int kvs_sync_init(kvs_role_t role) {
    g_master.reset();
    g_replica.reset();

    kvs::sync::SyncConfig config = make_sync_config();
    if (role == KVS_ROLE_MASTER) {
        kvs::sync::MasterDependencies dependencies;
        dependencies.serialize = [](char** data, size_t* length) {
            return kvs_snapshot_serialize(data, length);
        };
        dependencies.lock = []() { kvs_data_lock(); };
        dependencies.unlock = []() { kvs_data_unlock(); };
        dependencies.publish_event = publish_replication_event;
        dependencies.enable_capture = [](int enabled) { kvs_ebpf_capture_enable(enabled); };
        g_master.reset(new kvs::sync::MasterSyncCoordinator(config, dependencies));
        return g_master->start() ? 0 : -1;
    }

    kvs::sync::ReplicaDependencies dependencies;
    dependencies.load_snapshot = [](const char* path) {
        kvs_data_lock();
        int rc = kvs_snapshot_load(path);
        kvs_data_unlock();
        return rc;
    };
    dependencies.replay_command = [](char* payload, int length) {
        char response[256] = {0};
        kvs_replication_set_replaying(1);
        kvs_protocol(-1, payload, length, response, sizeof(response));
        kvs_replication_set_replaying(0);
    };
    g_replica.reset(new kvs::sync::ReplicaSyncCoordinator(config, dependencies));
    return 0;
}

void kvs_sync_set_master_addr(const char* ip, int port) {
    if (g_replica && ip != nullptr) {
        g_replica->set_master_addr(ip, port);
    }
}

int kvs_sync_handle_sync(int client_fd, char* tokens[], int count, char* response,
                         int response_size) {
    if (!g_master) {
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    return copy_reply(g_master->handle_sync(client_fd, tokens, count), response, response_size);
}

int kvs_sync_handle_sync_finished(int client_fd, char* tokens[], int count, char* response,
                                  int response_size) {
    if (!g_master) {
        return snprintf(response, response_size, "-ERROR\r\n");
    }
    return copy_reply(g_master->handle_finished(client_fd, tokens, count), response, response_size);
}

int kvs_sync_start() { return g_replica && g_replica->start() ? 0 : -1; }

void kvs_sync_stop() {
    if (g_replica) {
        g_replica->stop();
    }
}

void kvs_sync_destroy() {
    g_replica.reset();
    g_master.reset();
}

int kvs_sync_accept_ebpf_handshake(int fd, const char* data, int length) {
    return g_master && g_master->accept_agent(fd, data, length) ? 1 : 0;
}

int kvs_sync_ebpf_armed(void) { return g_master && g_master->agent_armed() ? 1 : 0; }

void kvs_sync_forget_fd(int fd) {
    if (g_master) {
        g_master->forget_fd(fd);
    }
}
