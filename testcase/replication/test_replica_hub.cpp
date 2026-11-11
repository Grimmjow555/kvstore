#include "kvs_replica_hub.h"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

static int g_failures = 0;

static void expect_true(bool condition, const char* message) {
    if (condition) {
        return;
    }
    std::fprintf(stderr, "FAIL: %s\n", message);
    ++g_failures;
}

static bool read_frame(int fd, std::string* payload) {
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    if (poll(&pfd, 1, 500) <= 0) {
        return false;
    }

    unsigned char header[4];
    size_t got = 0;
    while (got < sizeof(header)) {
        ssize_t count = recv(fd, header + got, sizeof(header) - got, 0);
        if (count <= 0) {
            return false;
        }
        got += static_cast<size_t>(count);
    }
    uint32_t length = (static_cast<uint32_t>(header[0]) << 24) |
                      (static_cast<uint32_t>(header[1]) << 16) |
                      (static_cast<uint32_t>(header[2]) << 8) |
                      static_cast<uint32_t>(header[3]);
    payload->assign(length, '\0');
    got = 0;
    while (got < length) {
        ssize_t count = recv(fd, &(*payload)[got], length - got, 0);
        if (count <= 0) {
            return false;
        }
        got += static_cast<size_t>(count);
    }
    return true;
}

static void test_hub_preserves_empty_value_wire_payload() {
    int sockets[2] = {-1, -1};
    expect_true(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0,
                "socket pair is created");
    if (sockets[0] < 0) {
        return;
    }

    kvs::ReplicaHubConfig config;
    config.max_replicas = 1;
    config.cache_max_bytes = 4096;
    config.cache_max_records = 10;
    config.outbuf_max_bytes = 4096;
    config.memory_budget_bytes = 8192;
    kvs::ReplicaHub hub(config);

    const std::string session_id = "00000000000000a1";
    expect_true(hub.attach(session_id, sockets[0]), "target attaches");
    expect_true(hub.on_sync(session_id), "target enters caching");
    expect_true(hub.on_finished(session_id), "target enters live");

    char command[] = "SET";
    char key[] = "key";
    char value[] = "";
    char* argv[] = {command, key, value};
    expect_true(!hub.on_write(3, argv), "empty value does not drop the target");

    std::string payload;
    expect_true(read_frame(sockets[1], &payload), "hub writes one framed command");
    const char* expected = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n";
    expect_true(payload.size() == strlen(expected), "hub wire payload length matches");
    expect_true(memcmp(payload.data(), expected, payload.size()) == 0,
                "hub preserves empty bulk string payload");

    hub.detach_fd(sockets[0]);
    close(sockets[1]);
}

int main() {
    test_hub_preserves_empty_value_wire_payload();
    if (g_failures != 0) {
        return 1;
    }
    std::printf("replica hub tests passed\n");
    return 0;
}
