#include "sync/replica_increment.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
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

static void test_increment_channel_replays_frames_and_wakes_on_close() {
    int sockets[2] = {-1, -1};
    expect_true(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0,
                "increment socket pair is created");
    if (sockets[0] < 0) {
        return;
    }

    std::atomic<int> callbacks(0);
    std::string received;
    kvs::sync::ReplicaIncrementChannel channel;
    expect_true(channel.start(sockets[0], [&](char* payload, int length) {
                    received.assign(payload, (size_t)length);
                    callbacks.fetch_add(1);
                }),
                "increment channel starts");

    const char payload[] = "*1\r\n$4\r\nPING\r\n";
    uint32_t network_length = htonl((uint32_t)strlen(payload));
    expect_true(send(sockets[1], &network_length, sizeof(network_length), 0) ==
                    (ssize_t)sizeof(network_length),
                "frame header is sent");
    expect_true(send(sockets[1], payload, strlen(payload), 0) == (ssize_t)strlen(payload),
                "frame payload is sent");
    for (int i = 0; i < 100 && callbacks.load() == 0; ++i) {
        usleep(10000);
    }
    expect_true(callbacks.load() == 1, "increment payload reaches replay callback");
    expect_true(received == payload, "increment payload is preserved");

    close(sockets[1]);
    expect_true(channel.wait_closed(1000), "peer close wakes channel wait");
    expect_true(!channel.connected(), "closed channel reports disconnected");
    channel.stop();
}

int main() {
    test_increment_channel_replays_frames_and_wakes_on_close();
    if (g_failures != 0) {
        return 1;
    }
    std::printf("replica increment tests passed\n");
    return 0;
}
