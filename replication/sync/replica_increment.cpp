#include "sync/replica_increment.h"

#include "kvs_network.h"

#include <arpa/inet.h>
#include <errno.h>
#include <poll.h>
#include <stdint.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <vector>

namespace kvs {
namespace sync {

ReplicaIncrementChannel::ReplicaIncrementChannel()
    : mutex_(PTHREAD_MUTEX_INITIALIZER), condition_(PTHREAD_COND_INITIALIZER), thread_(),
      running_(false), thread_started_(false), fd_(-1) {}

ReplicaIncrementChannel::~ReplicaIncrementChannel() { stop(); }

bool ReplicaIncrementChannel::start(int fd, const ReplayCommandFn& replay) {
    if (fd < 0 || thread_started_) {
        return false;
    }
    fd_ = fd;
    replay_ = replay;
    running_ = true;
    if (pthread_create(&thread_, NULL, reader_entry, this) != 0) {
        running_ = false;
        fd_ = -1;
        return false;
    }
    thread_started_ = true;
    return true;
}

bool ReplicaIncrementChannel::wait_closed(int timeout_ms) {
    unsigned long long deadline = (unsigned long long)time(NULL) * 1000ULL +
                                  (unsigned long long)(timeout_ms < 0 ? 0 : timeout_ms);
    pthread_mutex_lock(&mutex_);
    while (running_ && fd_ >= 0) {
        if (timeout_ms < 0) {
            pthread_cond_wait(&condition_, &mutex_);
            continue;
        }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 100 * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000L;
        }
        pthread_cond_timedwait(&condition_, &mutex_, &ts);
        unsigned long long now = (unsigned long long)time(NULL) * 1000ULL;
        if (now >= deadline) {
            break;
        }
    }
    bool closed = !running_ || fd_ < 0;
    pthread_mutex_unlock(&mutex_);
    return closed;
}

bool ReplicaIncrementChannel::connected() const {
    pthread_mutex_lock(&mutex_);
    bool connected = running_ && fd_ >= 0;
    pthread_mutex_unlock(&mutex_);
    return connected;
}

void ReplicaIncrementChannel::stop() {
    if (!thread_started_) {
        return;
    }
    running_ = false;
    pthread_mutex_lock(&mutex_);
    if (fd_ >= 0) {
        shutdown(fd_, SHUT_RDWR);
    }
    pthread_cond_broadcast(&condition_);
    pthread_mutex_unlock(&mutex_);
    pthread_join(thread_, NULL);
    thread_started_ = false;
}

void* ReplicaIncrementChannel::reader_entry(void* argument) {
    static_cast<ReplicaIncrementChannel*>(argument)->reader_loop();
    return NULL;
}

void ReplicaIncrementChannel::reader_loop() {
    while (running_) {
        uint32_t network_length = 0;
        if (read_exact(&network_length, sizeof(network_length)) != 1) {
            break;
        }
        uint32_t length = ntohl(network_length);
        if (length == 0 || length > KVS_NET_MAX_MESSAGE_SIZE) {
            break;
        }
        std::vector<char> payload((size_t)length + 1);
        if (read_exact(payload.data(), length) != 1) {
            break;
        }
        payload[length] = '\0';
        if (replay_) {
            replay_(payload.data(), (int)length);
        }
    }
    mark_closed();
}

int ReplicaIncrementChannel::read_exact(void* buffer, size_t length) {
    char* output = static_cast<char*>(buffer);
    size_t received = 0;
    while (received < length && running_) {
        struct pollfd pfd;
        pfd.fd = fd_;
        pfd.events = POLLIN;
        pfd.revents = 0;
        int ready = poll(&pfd, 1, 200);
        if (ready == 0) {
            continue;
        }
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        ssize_t count = recv(fd_, output + received, length - received, 0);
        if (count > 0) {
            received += (size_t)count;
            continue;
        }
        if (count == 0) {
            return -1;
        }
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
        }
        return -1;
    }
    return received == length ? 1 : 0;
}

void ReplicaIncrementChannel::mark_closed() {
    pthread_mutex_lock(&mutex_);
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    running_ = false;
    replay_ = ReplayCommandFn();
    pthread_cond_broadcast(&condition_);
    pthread_mutex_unlock(&mutex_);
}

} // namespace sync
} // namespace kvs
