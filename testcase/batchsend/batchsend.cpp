#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace {

constexpr size_t kMaxMessageLength = 1024 * 1024;

struct Engine {
    const char* name;
    const char* set_command;
};

const Engine kEngines[] = {
    {"rbtree", "RSET"},
    {"array", "SET"},
    {"hash", "HSET"},
    {"skiptable", "SSET"},
};

const int kBatchSizes[] = {10, 20, 40, 80, 160};

void print_usage(const char* program) {
    std::fprintf(stderr,
                 "用法: %s <ip> <port> <batch-size>\n"
                 "batch-size: 10, 20, 40, 80, 160\n",
                 program);
}

bool parse_long(const char* text, long* value) {
    if (text == nullptr || *text == '\0') {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    long parsed = std::strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return false;
    }

    *value = parsed;
    return true;
}

bool is_supported_batch_size(long batch_size) {
    for (int supported : kBatchSizes) {
        if (batch_size == supported) {
            return true;
        }
    }
    return false;
}

int connect_to_server(const char* ip, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::perror("socket");
        return -1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &server_addr.sin_addr) != 1) {
        std::fprintf(stderr, "无效的 IPv4 地址: %s\n", ip);
        close(fd);
        return -1;
    }

    if (connect(fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        std::perror("connect");
        close(fd);
        return -1;
    }

    return fd;
}

bool send_all(int fd, const void* data, size_t length) {
    const char* cursor = static_cast<const char*>(data);
    size_t sent = 0;
    while (sent < length) {
        ssize_t result = send(fd, cursor + sent, length - sent, MSG_NOSIGNAL);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            std::perror("send");
            return false;
        }
        sent += static_cast<size_t>(result);
    }
    return true;
}

bool recv_all(int fd, void* data, size_t length) {
    char* cursor = static_cast<char*>(data);
    size_t received = 0;
    while (received < length) {
        ssize_t result = recv(fd, cursor + received, length - received, 0);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            if (result == 0) {
                std::fprintf(stderr, "服务端提前关闭连接\n");
            } else {
                std::perror("recv");
            }
            return false;
        }
        received += static_cast<size_t>(result);
    }
    return true;
}

bool send_frame(int fd, const std::string& payload) {
    if (payload.size() > kMaxMessageLength) {
        std::fprintf(stderr, "请求超过 1MB 上限，长度: %zu\n", payload.size());
        return false;
    }

    uint32_t network_length = htonl(static_cast<uint32_t>(payload.size()));
    std::string frame(sizeof(network_length) + payload.size(), '\0');
    std::memcpy(&frame[0], &network_length, sizeof(network_length));
    if (!payload.empty()) {
        std::memcpy(&frame[sizeof(network_length)], payload.data(), payload.size());
    }
    return send_all(fd, frame.data(), frame.size());
}

bool recv_frame(int fd, std::string* payload) {
    uint32_t network_length = 0;
    if (!recv_all(fd, &network_length, sizeof(network_length))) {
        return false;
    }

    size_t data_length = ntohl(network_length);
    if (data_length > kMaxMessageLength) {
        std::fprintf(stderr, "响应超过 1MB 上限，长度: %zu\n", data_length);
        return false;
    }

    payload->assign(data_length, '\0');
    return data_length == 0 || recv_all(fd, &(*payload)[0], data_length);
}

void append_bulk_string(std::string* payload, const char* value) {
    payload->push_back('$');
    payload->append(std::to_string(std::strlen(value)));
    payload->append("\r\n");
    payload->append(value);
    payload->append("\r\n");
}

void append_insert_command(std::string* payload, const Engine& engine, size_t index) {
    std::string number = std::to_string(index);
    std::string key = "Teacher" + number;
    std::string value = "King" + number;

    payload->append("*3\r\n");
    append_bulk_string(payload, engine.set_command);
    append_bulk_string(payload, key.c_str());
    append_bulk_string(payload, value.c_str());
}

std::string build_insert_payload(size_t batch_size) {
    constexpr size_t kEngineCount = sizeof(kEngines) / sizeof(kEngines[0]);
    const size_t count_per_engine = batch_size / kEngineCount;
    const size_t remainder = batch_size % kEngineCount;

    std::string payload;
    payload.reserve(batch_size * 64);

    for (size_t index = 0; index < count_per_engine; ++index) {
        for (const Engine& engine : kEngines) {
            append_insert_command(&payload, engine, index);
        }
    }
    for (size_t engine_index = 0; engine_index < remainder; ++engine_index) {
        append_insert_command(&payload, kEngines[engine_index], count_per_engine);
    }

    return payload;
}

bool validate_insert_responses(const std::string& payload, size_t expected_count, int* ok_count,
                               int* exist_count) {
    static const std::string kOkResponse = "+OK\r\n";
    static const std::string kExistResponse = "+EXIST\r\n";

    *ok_count = 0;
    *exist_count = 0;
    size_t offset = 0;
    while (offset < payload.size()) {
        if (payload.compare(offset, kOkResponse.size(), kOkResponse) == 0) {
            offset += kOkResponse.size();
            ++*ok_count;
        } else if (payload.compare(offset, kExistResponse.size(), kExistResponse) == 0) {
            offset += kExistResponse.size();
            ++*exist_count;
        } else {
            std::fprintf(stderr, "offset %zu 处出现非预期响应: %s\n", offset,
                         payload.c_str() + offset);
            return false;
        }
    }

    size_t total = static_cast<size_t>(*ok_count + *exist_count);
    if (total != expected_count) {
        std::fprintf(stderr, "响应数量不匹配，期望 %zu，实际 %zu\n", expected_count, total);
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc != 4) {
        print_usage(argv[0]);
        return 1;
    }

    long port_value = 0;
    long batch_value = 0;
    if (!parse_long(argv[2], &port_value) || port_value < 1 || port_value > 65535) {
        std::fprintf(stderr, "无效端口: %s\n", argv[2]);
        return 1;
    }
    if (!parse_long(argv[3], &batch_value) || !is_supported_batch_size(batch_value)) {
        std::fprintf(stderr, "无效 batch-size: %s\n", argv[3]);
        print_usage(argv[0]);
        return 1;
    }

    size_t batch_size = static_cast<size_t>(batch_value);
    int fd = connect_to_server(argv[1], static_cast<uint16_t>(port_value));
    if (fd < 0) {
        return 1;
    }

    std::string request = build_insert_payload(batch_size);
    auto begin = std::chrono::steady_clock::now();
    if (!send_frame(fd, request)) {
        close(fd);
        return 1;
    }

    std::string response;
    if (!recv_frame(fd, &response)) {
        close(fd);
        return 1;
    }
    auto end = std::chrono::steady_clock::now();
    close(fd);

    int ok_count = 0;
    int exist_count = 0;
    if (!validate_insert_responses(response, batch_size, &ok_count, &exist_count)) {
        return 1;
    }

    constexpr size_t kEngineCount = sizeof(kEngines) / sizeof(kEngines[0]);
    const size_t count_per_engine = batch_size / kEngineCount;
    const size_t remainder = batch_size % kEngineCount;
    auto elapsed = end - begin;
    long long elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    long long time_used_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    if (elapsed_us < 1) {
        elapsed_us = 1;
    }

    double elapsed_seconds = static_cast<double>(elapsed_us) / 1000000.0;
    double qps = static_cast<double>(batch_size) / elapsed_seconds;
    double average_latency_ms = static_cast<double>(elapsed_us) / batch_size / 1000.0;

    std::printf("PASS batch=%zu sent=%zu responses=%zu (+OK=%d, +EXIST=%d)\n", batch_size,
                batch_size, batch_size, ok_count, exist_count);
    std::printf("time_used: %lld ms, qps: %.0f, avg: %.4f ms/cmd\n", time_used_ms, qps,
                average_latency_ms);
    return 0;
}
