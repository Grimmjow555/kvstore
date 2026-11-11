#include "kvs_replication_protocol.h"

#include <cstdio>
#include <cstring>

static int g_failures = 0;

static void expect_true(bool condition, const char* message) {
    if (condition) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s\n", message);
    ++g_failures;
}

static void test_builder_preserves_empty_bulk_string() {
    const char* argv[] = {"SET", "", "value"};
    char payload[128];
    size_t length = 0;

    expect_true(kvs_repl_build_resp_array(argv, 3, payload, sizeof(payload), &length) == 0,
                "empty bulk string is encoded successfully");
    const char* expected = "*3\r\n$3\r\nSET\r\n$0\r\n\r\n$5\r\nvalue\r\n";
    expect_true(length == strlen(expected), "empty bulk string wire length matches");
    expect_true(memcmp(payload, expected, length) == 0,
                "empty bulk string wire payload matches RESP");
}

static void test_builder_encodes_sync_request() {
    const char* argv[] = {"SYNC", "10.0.0.8", "19100"};
    char payload[128];
    size_t length = 0;

    expect_true(kvs_repl_build_resp_array(argv, 3, payload, sizeof(payload), &length) == 0,
                "SYNC request builds successfully");
    const char* expected =
        "*3\r\n$4\r\nSYNC\r\n$8\r\n10.0.0.8\r\n$5\r\n19100\r\n";
    expect_true(length == strlen(expected), "SYNC request wire length matches");
    expect_true(memcmp(payload, expected, length) == 0,
                "SYNC request uses the shared RESP encoder");
}

int main() {
    test_builder_preserves_empty_bulk_string();
    test_builder_encodes_sync_request();
    if (g_failures != 0) {
        return 1;
    }
    std::printf("replication protocol tests passed\n");
    return 0;
}
