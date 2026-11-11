#include "sync/sync_support.h"

#include <cstdio>
#include <string>

static int g_failures = 0;

static void expect_true(bool condition, const char* message) {
    if (condition) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s\n", message);
    ++g_failures;
}

static void expect_int(int actual, int expected, const char* message) {
    if (actual == expected) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s (expected %d, actual %d)\n", message, expected, actual);
    ++g_failures;
}

static void test_session_paths_include_session_id() {
    std::string master = kvs::sync::master_snapshot_path("00000000000000a1");
    std::string replica = kvs::sync::replica_receive_path("00000000000000a1");
    expect_true(master.find("00000000000000a1") != std::string::npos,
                "master path includes session id");
    expect_true(replica.find("00000000000000a1") != std::string::npos,
                "replica path includes session id");
}

static void test_child_process_success_and_failure() {
    kvs::sync::ManagedChildProcess success;
    expect_int(success.start("/bin/sh", {"-c", "exit 0"}, "/tmp/kvs-child-success.log"), 0,
               "successful child starts");
    expect_int(success.wait(1000), kvs::sync::ChildExitedSuccess,
               "successful child exits with zero");

    kvs::sync::ManagedChildProcess failure;
    expect_int(failure.start("/bin/sh", {"-c", "exit 7"}, "/tmp/kvs-child-failure.log"), 0,
               "failing child starts");
    expect_int(failure.wait(1000), kvs::sync::ChildExitedFailure,
               "failing child reports failure");
}

static void test_child_process_timeout_then_stop() {
    kvs::sync::ManagedChildProcess child;
    expect_int(child.start("/bin/sleep", {"5"}, "/tmp/kvs-child-timeout.log"), 0,
               "long-running child starts");
    expect_int(child.wait(50), kvs::sync::ChildTimedOut,
               "long-running child reaches timeout");
    child.stop();
    expect_true(!child.running(), "stop reaps the child process");
}

int main() {
    test_session_paths_include_session_id();
    test_child_process_success_and_failure();
    test_child_process_timeout_then_stop();
    if (g_failures != 0) {
        return 1;
    }
    std::printf("sync support tests passed\n");
    return 0;
}
