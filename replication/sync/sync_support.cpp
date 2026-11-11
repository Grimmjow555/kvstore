#include "sync/sync_support.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char** environ;

namespace kvs {
namespace sync {

ManagedChildProcess::ManagedChildProcess() : pid_(-1) {}

ManagedChildProcess::~ManagedChildProcess() { stop(); }

int ManagedChildProcess::start(const std::string& binary, const std::vector<std::string>& arguments,
                               const std::string& log_path) {
    if (pid_ > 0 || binary.empty()) {
        return -1;
    }

    std::vector<char*> argv;
    argv.push_back(const_cast<char*>(binary.c_str()));
    for (size_t i = 0; i < arguments.size(); ++i) {
        argv.push_back(const_cast<char*>(arguments[i].c_str()));
    }
    argv.push_back(NULL);

    posix_spawn_file_actions_t actions;
    if (posix_spawn_file_actions_init(&actions) != 0) {
        return -1;
    }
    if (!log_path.empty()) {
        posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log_path.c_str(),
                                         O_WRONLY | O_CREAT | O_TRUNC, 0644);
        posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, log_path.c_str(),
                                         O_WRONLY | O_CREAT | O_TRUNC, 0644);
    }

    pid_t child = -1;
    int rc = posix_spawn(&child, binary.c_str(), &actions, NULL, argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    if (rc != 0) {
        return -1;
    }
    pid_ = child;
    return 0;
}

ChildWaitResult ManagedChildProcess::wait(int timeout_ms) {
    if (pid_ <= 0) {
        return ChildExitedFailure;
    }
    unsigned long long deadline = now_ms() + (unsigned long long)timeout_ms;
    while (true) {
        int status = 0;
        pid_t result = waitpid(pid_, &status, WNOHANG);
        if (result == pid_) {
            pid_ = -1;
            return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? ChildExitedSuccess
                                                                 : ChildExitedFailure;
        }
        if (result < 0) {
            pid_ = -1;
            return ChildExitedFailure;
        }
        if (timeout_ms >= 0 && now_ms() >= deadline) {
            return ChildTimedOut;
        }
        sleep_ms(10);
    }
}

void ManagedChildProcess::stop() {
    if (pid_ <= 0) {
        return;
    }

    pid_t child = pid_;
    pid_ = -1;
    kill(child, SIGTERM);
    for (int i = 0; i < 40; ++i) {
        int status = 0;
        if (waitpid(child, &status, WNOHANG) == child) {
            return;
        }
        sleep_ms(50);
    }
    kill(child, SIGKILL);
    int status = 0;
    waitpid(child, &status, 0);
}

bool ManagedChildProcess::running() const { return pid_ > 0; }

pid_t ManagedChildProcess::pid() const { return pid_; }

std::string master_snapshot_path(const std::string& session_id) {
    return "../data/kvstore.sync." + session_id + ".tmp";
}

std::string master_partial_path(const std::string& session_id) {
    return master_snapshot_path(session_id) + ".partial";
}

std::string replica_receive_base_path(int pid, int control_fd) {
    char path[256];
    snprintf(path, sizeof(path), "../data/kvstore.sync.replica.%d.%d.tmp.recv", pid, control_fd);
    return path;
}

std::string replica_receive_ready_path(int pid, int control_fd) {
    char path[256];
    snprintf(path, sizeof(path), "../data/kvstore.sync.replica.%d.%d.ready", pid, control_fd);
    return path;
}

std::string replica_receive_path(const std::string& session_id) {
    return "../data/kvstore.sync." + session_id + ".tmp.recv";
}

std::string replica_server_log_path(int pid, int control_fd) {
    char path[256];
    snprintf(path, sizeof(path), "../data/kvstore.sync.replica.%d.%d.rdma-server.log", pid,
             control_fd);
    return path;
}

std::string build_sync_reply(bool ok, const std::string& session_id) {
    if (!ok) {
        return "-ERROR\r\n";
    }
    if (session_id.empty()) {
        return "+OK\r\n";
    }
    return "+OK " + session_id + "\r\n";
}

unsigned long long now_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (unsigned long long)ts.tv_sec * 1000ULL + (unsigned long long)ts.tv_nsec / 1000000ULL;
}

void sleep_ms(int milliseconds) {
    struct timespec ts;
    ts.tv_sec = milliseconds / 1000;
    ts.tv_nsec = (long)(milliseconds % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

void sleep_interruptible(int milliseconds, volatile int* running) {
    while (milliseconds > 0 && *running) {
        int step = milliseconds > 50 ? 50 : milliseconds;
        sleep_ms(step);
        milliseconds -= step;
    }
}

} // namespace sync
} // namespace kvs
