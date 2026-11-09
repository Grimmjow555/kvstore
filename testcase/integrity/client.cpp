#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

static int send_all(int fd, const void* data, size_t size) {
    const char* bytes = (const char*)data;
    size_t sent = 0;
    while (sent < size) {
        ssize_t n = send(fd, bytes + sent, size - sent, 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

static int recv_all(int fd, void* data, size_t size) {
    char* bytes = (char*)data;
    size_t received = 0;
    while (received < size) {
        ssize_t n = recv(fd, bytes + received, size - received, 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return -1;
        }
        received += (size_t)n;
    }
    return 0;
}

static void append_resp_arg(std::string* request, const char* value) {
    size_t length = std::strlen(value);
    *request += "$";
    *request += std::to_string(length);
    *request += "\r\n";
    request->append(value, length);
    *request += "\r\n";
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::fprintf(stderr, "usage: %s <host> <port> <command> [args...]\n", argv[0]);
        return 2;
    }

    const char* host = argv[1];
    int port = std::atoi(argv[2]);
    int arg_count = argc - 3;

    std::string request = "*";
    request += std::to_string(arg_count);
    request += "\r\n";
    for (int i = 3; i < argc; ++i) {
        append_resp_arg(&request, argv[i]);
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::perror("socket");
        return 1;
    }

    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &address.sin_addr) != 1 ||
        connect(fd, (struct sockaddr*)&address, sizeof(address)) != 0) {
        std::perror("connect");
        close(fd);
        return 1;
    }

    uint32_t frame_length = htonl((uint32_t)request.size());
    std::string frame(sizeof(frame_length) + request.size(), '\0');
    std::memcpy(&frame[0], &frame_length, sizeof(frame_length));
    if (!request.empty()) {
        std::memcpy(&frame[sizeof(frame_length)], request.data(), request.size());
    }
    if (send_all(fd, frame.data(), frame.size()) != 0) {
        std::perror("send");
        close(fd);
        return 1;
    }

    uint32_t response_length = 0;
    if (recv_all(fd, &response_length, sizeof(response_length)) != 0) {
        std::perror("recv length");
        close(fd);
        return 1;
    }
    response_length = ntohl(response_length);
    if (response_length > 1024 * 1024) {
        std::fprintf(stderr, "response too large: %u\n", response_length);
        close(fd);
        return 1;
    }

    std::string response(response_length, '\0');
    if (response_length > 0 && recv_all(fd, &response[0], response_length) != 0) {
        std::perror("recv body");
        close(fd);
        return 1;
    }
    close(fd);
    std::fwrite(response.data(), 1, response.size(), stdout);
    return 0;
}
