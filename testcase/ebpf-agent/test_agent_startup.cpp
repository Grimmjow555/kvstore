#include "kvs_ebpf_ring.h"
#include "kvs_netutil.h"
#include "kvs_replication.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>

#ifndef KVS_AGENT_BINARY
#error "KVS_AGENT_BINARY must point to the ebpf-agent executable"
#endif

static bool write_text_file(const std::string& path, const char* contents) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        return false;
    }
    size_t total = strlen(contents);
    size_t written = 0;
    while (written < total) {
        ssize_t n = write(fd, contents + written, total - written);
        if (n <= 0) {
            close(fd);
            return false;
        }
        written += static_cast<size_t>(n);
    }
    return close(fd) == 0;
}

static bool file_contains(const std::string& path, const char* expected) {
    FILE* file = fopen(path.c_str(), "rb");
    if (file == NULL) {
        return false;
    }
    char buffer[8192];
    size_t size = fread(buffer, 1, sizeof(buffer) - 1, file);
    fclose(file);
    buffer[size] = '\0';
    return strstr(buffer, expected) != NULL;
}

static void print_file(const std::string& path) {
    FILE* file = fopen(path.c_str(), "rb");
    if (file == NULL) {
        return;
    }
    char buffer[4096];
    size_t size = 0;
    while ((size = fread(buffer, 1, sizeof(buffer), file)) > 0) {
        fwrite(buffer, 1, size, stderr);
    }
    fclose(file);
}

static int reserve_loopback_port() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(fd, (struct sockaddr*)&address, sizeof(address)) != 0) {
        close(fd);
        return -1;
    }
    socklen_t length = sizeof(address);
    if (getsockname(fd, (struct sockaddr*)&address, &length) != 0) {
        close(fd);
        return -1;
    }
    int port = ntohs(address.sin_port);
    close(fd);
    return port;
}

static int connect_loopback(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons((uint16_t)port);
    if (connect(fd, (struct sockaddr*)&address, sizeof(address)) != 0) {
        std::fprintf(stderr, "DEBUG connect errno=%d (%s)\n", errno, strerror(errno));
        close(fd);
        return -1;
    }
    return fd;
}

int main() {
    char directory_template[] = "/tmp/kvs-agent-startup-XXXXXX";
    char* directory = mkdtemp(directory_template);
    if (directory == NULL) {
        std::fprintf(stderr, "FAIL: cannot create startup fixture directory\n");
        return 1;
    }

    const std::string root(directory);
    const std::string ring_path = root + "/ring";
    const std::string config_path = root + "/ebpf-agent.conf";
    const std::string log_path = root + "/agent.log";
    int listen_port = reserve_loopback_port();
    if (listen_port <= 0) {
        std::fprintf(stderr, "FAIL: cannot reserve listener port\n");
        rmdir(root.c_str());
        return 1;
    }
    const std::string config =
        "master_ip 127.0.0.1\n"
        "master_port 1\n"
        "listen_ip 127.0.0.1\n"
        "listen_port " +
        std::to_string(listen_port) + "\n"
        "cache_max_records 1\n"
        "mode sim\n"
        "ring " +
        ring_path + "\n";

    if (!write_text_file(config_path, config.c_str()) || kvs_ebpf_ring_create(ring_path.c_str()) != 0) {
        std::fprintf(stderr, "FAIL: cannot prepare startup fixture\n");
        unlink(config_path.c_str());
        rmdir(root.c_str());
        return 1;
    }

    pid_t child = fork();
    if (child == 0) {
        if (chdir(root.c_str()) != 0) {
            _exit(126);
        }
        int log_fd = open(log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (log_fd < 0) {
            _exit(126);
        }
        dup2(log_fd, STDERR_FILENO);
        dup2(log_fd, STDOUT_FILENO);
        close(log_fd);
        execl(KVS_AGENT_BINARY, KVS_AGENT_BINARY, static_cast<char*>(NULL));
        _exit(127);
    }
    if (child < 0) {
        std::fprintf(stderr, "FAIL: cannot launch ebpf-agent\n");
        kvs_ebpf_ring_close();
        unlink(config_path.c_str());
        rmdir(root.c_str());
        return 1;
    }

    bool exited_early = false;
    int status = 0;
    for (int i = 0; i < 10; ++i) {
        usleep(50 * 1000);
        if (waitpid(child, &status, WNOHANG) == child) {
            exited_early = true;
            break;
        }
    }

    bool loaded_config = file_contains(log_path, "capture started (mode=sim), master=127.0.0.1:1");
    bool listening = false;
    for (int i = 0; i < 20 && !exited_early; ++i) {
        if (file_contains(log_path, "listening for replica on 127.0.0.1")) {
            listening = true;
            break;
        }
        usleep(50 * 1000);
    }

    bool valid_handshake = false;
    bool invalid_rejected = false;
    if (listening) {
        int fd = connect_loopback(listen_port);
        int send_rc = fd >= 0 ? kvs_net_send_frame(fd, KVS_REPL_HANDSHAKE_REPL_INC,
                                                   strlen(KVS_REPL_HANDSHAKE_REPL_INC), 1000)
                              : -1;
        int recv_rc = send_rc == 0 ? kvs_net_recv_frame_ok(fd, 1000) : -1;
        valid_handshake = fd >= 0 && send_rc == 0 && recv_rc == 1;
        if (fd >= 0) {
            close(fd);
        }
        if (!valid_handshake) {
            std::fprintf(stderr, "DEBUG valid handshake: fd=%d send=%d recv=%d port=%d\n", fd,
                         send_rc, recv_rc, listen_port);
        }

        char arg_sync[] = "SYNC";
        char arg_write1[] = "SET";
        char arg_key1[] = "cache-overflow-1";
        char arg_value1[] = "value-1";
        char arg_write2[] = "SET";
        char arg_key2[] = "cache-overflow-2";
        char arg_value2[] = "value-2";
        char arg_finished[] = "SYNC FINISHED";
        char* sync_args[] = {arg_sync};
        char* write_args1[] = {arg_write1, arg_key1, arg_value1};
        char* write_args2[] = {arg_write2, arg_key2, arg_value2};
        char* finished_args[] = {arg_finished};
        kvs_ebpf_ring_push(1, 1, sync_args);
        kvs_ebpf_ring_push(2, 3, write_args1);
        kvs_ebpf_ring_push(3, 3, write_args2);
        for (int i = 0; i < 40 && !file_contains(log_path, "cache overflow"); ++i) {
            usleep(50 * 1000);
        }

        fd = connect_loopback(listen_port);
        if (fd >= 0 &&
            kvs_net_send_frame(fd, "INVALID", strlen("INVALID"), 1000) == 0) {
            char response[64];
            int length = kvs_net_recv_frame(fd, response, sizeof(response), 1000);
            invalid_rejected = length > 0 && response[0] == '-';
            if (!invalid_rejected) {
                std::fprintf(stderr, "DEBUG invalid handshake: length=%d response=%s\n", length,
                             length > 0 ? response : "(none)");
            }
        }
        if (fd >= 0) {
            close(fd);
        }

        fd = connect_loopback(listen_port);
        int recovered_send =
            fd >= 0 ? kvs_net_send_frame(fd, KVS_REPL_HANDSHAKE_REPL_INC,
                                         strlen(KVS_REPL_HANDSHAKE_REPL_INC), 1000)
                    : -1;
        int recovered_recv =
            recovered_send == 0 ? kvs_net_recv_frame_ok(fd, 1000) : -1;
        bool recovered = fd >= 0 && recovered_send == 0 && recovered_recv == 1;
        invalid_rejected = invalid_rejected && recovered;
        if (!recovered) {
            std::fprintf(stderr, "DEBUG recovered handshake: fd=%d send=%d recv=%d\n", fd,
                         recovered_send, recovered_recv);
            if (fd >= 0) {
                close(fd);
            }
            fd = -1;
        }

        bool invalid_round_closed = false;
        if (fd >= 0) {
            kvs_ebpf_ring_push(4, 1, finished_args);

            struct pollfd closed_check;
            closed_check.fd = fd;
            closed_check.events = POLLIN;
            closed_check.revents = 0;
            for (int i = 0; i < 30; ++i) {
                int ready = poll(&closed_check, 1, 100);
                if (ready > 0) {
                    char byte;
                    invalid_round_closed = recv(fd, &byte, sizeof(byte), 0) == 0;
                    break;
                }
            }
            close(fd);
        }
        invalid_rejected = invalid_rejected && invalid_round_closed;
    }

    if (!exited_early) {
        kill(child, SIGTERM);
        for (int i = 0; i < 50; ++i) {
            if (waitpid(child, &status, WNOHANG) == child) {
                break;
            }
            usleep(100 * 1000);
        }
        if (waitpid(child, &status, WNOHANG) == 0) {
            kill(child, SIGKILL);
            waitpid(child, &status, 0);
        }
    }

    bool passed = !exited_early && loaded_config && listening && valid_handshake &&
                  invalid_rejected;
    if (!passed) {
        std::fprintf(stderr,
                     "FAIL: ebpf-agent did not load config and serve reverse REPL_INC handshakes "
                     "(loaded=%d listening=%d valid=%d invalid=%d exited=%d)\n",
                     loaded_config, listening, valid_handshake, invalid_rejected, exited_early);
        print_file(log_path);
    }

    kvs_ebpf_ring_close();
    unlink(config_path.c_str());
    unlink(log_path.c_str());
    rmdir(root.c_str());

    if (!passed) {
        return 1;
    }
    std::printf("ebpf-agent default startup test passed\n");
    return 0;
}
