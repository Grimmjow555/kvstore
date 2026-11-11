#include "sync/master_sync.h"
#include "kvs_replication_protocol.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <string>

static int g_failures = 0;

static void expect_true(bool condition, const char* message) {
    if (condition) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s\n", message);
    ++g_failures;
}

static int fake_serialize(char** data, size_t* length) {
    const char* payload = "snapshot";
    *data = static_cast<char*>(malloc(strlen(payload)));
    if (*data == nullptr) {
        return -1;
    }
    memcpy(*data, payload, strlen(payload));
    *length = strlen(payload);
    return 0;
}

static void test_master_session_lifecycle() {
    char root_template[] = "/tmp/kvs-master-sync-XXXXXX";
    char* root = mkdtemp(root_template);
    expect_true(root != nullptr, "temporary root is created");
    if (root == nullptr) {
        return;
    }
    std::string root_path(root);
    std::string build_path = root_path + "/build";
    std::string data_path = root_path + "/data";
    mkdir(build_path.c_str(), 0700);
    mkdir(data_path.c_str(), 0700);

    char original[4096];
    expect_true(getcwd(original, sizeof(original)) != nullptr, "original cwd is captured");
    expect_true(chdir(build_path.c_str()) == 0, "test enters build-like cwd");

    kvs::sync::SyncConfig config;
    config.rdma_port = 10000;
    config.agent_port = 11001;
    config.max_replicas = 1;
    config.session_timeout_ms = 5000;
    config.rdma_transfer_timeout_ms = 1000;
    config.sync_wait_timeout_ms = 1000;
    config.rdma_client_bin = "/bin/true";

    int events = 0;
    kvs::sync::MasterDependencies deps;
    deps.serialize = fake_serialize;
    deps.lock = []() {};
    deps.unlock = []() {};
    deps.publish_event = [&events](const char* event, const std::string&) {
        ++events;
        (void)event;
    };
    deps.enable_capture = [](int) {};

    kvs::sync::MasterSyncCoordinator coordinator(config, deps);
    expect_true(coordinator.start(), "master coordinator starts");

    char handshake[128];
    size_t handshake_len = 0;
    expect_true(kvs_repl_build_ebpf_handshake(handshake, sizeof(handshake), &handshake_len) == 0,
                "agent handshake builds");
    expect_true(coordinator.accept_agent(17, handshake, (int)handshake_len),
                "valid agent handshake is accepted");

    char sync[] = "SYNC";
    char ip[] = "127.0.0.1";
    char port[] = "12000";
    char* tokens[] = {sync, ip, port, nullptr};
    kvs::sync::SyncReply reply = coordinator.handle_sync(23, tokens, 3);
    expect_true(reply.ok, "first sync is accepted");

    char session_id[KVS_REPL_SESSION_ID_SIZE + 1];
    expect_true(kvs_repl_parse_ok_session(reply.payload.c_str(), (int)reply.payload.size(),
                                          session_id, sizeof(session_id)) == 0,
                "sync response contains a session id");
    expect_true(!coordinator.handle_sync(24, tokens, 3).ok,
                "second sync is rejected at max replica capacity");

    char finished[] = "SYNC FINISHED";
    char* finish_tokens[] = {finished, session_id, nullptr};
    expect_true(coordinator.handle_finished(23, finish_tokens, 2).ok,
                "matching session can finish");
    expect_true(coordinator.session_count() == 0, "finished session is removed");
    expect_true(events == 2, "SYNC and SYNC FINISHED events are published");

    coordinator.forget_fd(17);
    expect_true(!coordinator.agent_armed(), "agent disconnect disarms capture");
    coordinator.stop();

    expect_true(chdir(original) == 0, "working directory is restored");
    rmdir(build_path.c_str());
    rmdir(data_path.c_str());
    rmdir(root_path.c_str());
}

int main() {
    test_master_session_lifecycle();
    if (g_failures != 0) {
        return 1;
    }
    std::printf("master sync tests passed\n");
    return 0;
}
