#pragma once

#include "capture/capture_runtime.h"
#include "kvs_agent_config.h"
#include "kvs_replica_hub.h"

#include <signal.h>

#include <memory>

namespace kvs {
namespace agent {

class AgentService {
    public:
    explicit AgentService(const kvs_agent_config_t& config);
    ~AgentService();

    AgentService(const AgentService&) = delete;
    AgentService& operator=(const AgentService&) = delete;

    int run(volatile sig_atomic_t* running);

    private:
    void on_capture_record(int argc, char* argv[]);
    void on_capture_resync(const char* reason);
    int open_listener();
    void accept_replica_connection(int listen_fd);
    int service_channels(int listen_fd, int master_fd, int timeout_ms);
    int master_register_loop(volatile sig_atomic_t* running, int listen_fd);
    void log_stats();

    kvs_agent_config_t config_;
    std::unique_ptr<capture::CaptureRuntime> capture_;
    std::unique_ptr<kvs::ReplicaHub> hub_;
};

} // namespace agent
} // namespace kvs
