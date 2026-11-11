#pragma once

#include <sys/types.h>

#include <string>
#include <vector>

namespace kvs {
namespace sync {

enum ChildWaitResult {
    ChildTimedOut = 0,
    ChildExitedSuccess = 1,
    ChildExitedFailure = -1,
};

class ManagedChildProcess {
    public:
    ManagedChildProcess();
    ~ManagedChildProcess();

    ManagedChildProcess(const ManagedChildProcess&) = delete;
    ManagedChildProcess& operator=(const ManagedChildProcess&) = delete;

    int start(const std::string& binary, const std::vector<std::string>& arguments,
              const std::string& log_path);
    ChildWaitResult wait(int timeout_ms);
    void stop();
    bool running() const;
    pid_t pid() const;

    private:
    pid_t pid_;
};

std::string master_snapshot_path(const std::string& session_id);
std::string master_partial_path(const std::string& session_id);
std::string replica_receive_base_path(int pid, int control_fd);
std::string replica_receive_ready_path(int pid, int control_fd);
std::string replica_receive_path(const std::string& session_id);
std::string replica_server_log_path(int pid, int control_fd);
std::string build_sync_reply(bool ok, const std::string& session_id = std::string());

unsigned long long now_ms();
void sleep_ms(int milliseconds);
void sleep_interruptible(int milliseconds, volatile int* running);

} // namespace sync
} // namespace kvs
