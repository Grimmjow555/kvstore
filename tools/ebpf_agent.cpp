// ebpf-agent：独立的增量采集与转发进程（重设计阶段 3）。
//
// 用法：
//   ebpf-agent --master 127.0.0.1:9999 --listen 0.0.0.0:11001 \
//              --mode sim --ring ../data/kvstore.ebpf.ring
//   ebpf-agent --master ... --listen ... --mode uprobe --pid <master_pid> \
//              --binary ./kvstore
//
// 职责（设计 §8.5）：
//   1) 启动采集（uprobe 挂 master 二进制的 kvs_ebpf_notify_write；sim 读共享内存环）；
//   2) 连 master 发 EBPF 握手注册——master 只在注册后才受理 SYNC（eBPF 强制）；
//   3) 按记录顺序维护状态机：
//        IDLE    ：尚未同步，写入不转发；
//        CACHING ：读到 SYNC，把后续写命令缓存起来（覆盖整个全量窗口）；
//        LIVE    ：读到 SYNC FINISHED，冲刷缓存并转入实时转发。
//
// 采集流里的 SYNC / SYNC FINISHED 与写命令来自同一条有序流（master 在存储锁内
// 调用挂载点），因此「缓存窗口 ⊇ 快照窗口」由顺序本身保证（设计 §7.1）。
#include "kvs_ebpf_agent.h"
#include "kvs_ebpf_ring.h"
#include "kvs_netutil.h"
#include "kvs_replication.h"
#include "kvs_agent_config.h"

#include <arpa/inet.h>
#include <cstdarg>
#include <deque>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

namespace {

// 采集状态机。
enum agent_state_t { ST_IDLE = 0, ST_CACHING = 1, ST_LIVE = 2 };

struct agent_t {
    agent_state_t state;
    std::deque<std::string> cache; // CACHING 期间缓存的已编码命令
    size_t cache_bytes;
    size_t cache_max_bytes;
    size_t cache_max_records;
    int replica_fd;      // 增量通道；-1 表示未连接
    std::string outbuf;  // 待发字节（含 4 字节长度头）
    size_t outbuf_off;   // outbuf 中已发出的前缀长度（避免每次 erase 造成 O(n²)）
};

agent_t g_agent;
pthread_mutex_t g_agent_mutex = PTHREAD_MUTEX_INITIALIZER;

// 诊断计数：转发成功 / 转发失败（通道不可用）的条数。全量同步窗口内的丢失
// 往往表现为「尾部没有到达」，这两个计数能直接区分是采集丢了还是投递失败。
unsigned long long g_forwarded = 0;
unsigned long long g_forward_failed = 0;

// 本轮全量同步窗口是否已经失效（缓存溢出等）。失效后关闭反向增量通道，
// 让 Replica 观察到断开并重新全量同步——避免「带着缺口进入实时转发」。
bool g_round_invalid = false;

char g_master_ip[64];
int g_master_port = 0;
char g_listen_ip[64];
int g_listen_port = 0;


// 增量通道发送缓冲上限：超过说明 Replica 明显跟不上，断开让它重新全量同步。
const size_t kOutbufLimit = 64u * 1024 * 1024;

// 握手/注册这类控制交互的超时（连接、发包、读 +OK 共用）。
const int kHandshakeTimeoutMs = 3000;

void agent_log(const char* fmt, ...) {
    char ts[32];
    time_t now = time(NULL);
    struct tm tm_value;
    memset(&tm_value, 0, sizeof(tm_value));
    localtime_r(&now, &tm_value);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_value);

    fprintf(stderr, "[%s][EBPF-AGENT] ", ts);
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
    fflush(stderr);
}

// 把命令编码成 RESP 文本（增量通道上承载的就是这个）。
bool encode_command(int argc, char* argv[], std::string* out) {
    if (argc <= 0 || argv == NULL || out == NULL) {
        return false;
    }
    size_t size = 32;
    for (int i = 0; i < argc; ++i) {
        size += strlen(argv[i]) + 32;
    }
    out->resize(size);
    int offset = snprintf(&(*out)[0], size, "*%d\r\n", argc);
    for (int i = 0; i < argc; ++i) {
        offset += snprintf(&(*out)[0] + offset, size - (size_t)offset, "$%zu\r\n%s\r\n",
                           strlen(argv[i]), argv[i]);
    }
    out->resize((size_t)offset);
    return true;
}

void drop_replica_conn_locked(const char* why) {
    if (g_agent.replica_fd >= 0) {
        close(g_agent.replica_fd);
        g_agent.replica_fd = -1;
    }
    g_agent.outbuf.clear();
    g_agent.outbuf_off = 0;
    agent_log("increment channel closed (%s)", why);
}

// 把 outbuf 里能发的都发出去；连接断开则关闭（Replica 会因此重新全量同步）。
void flush_outbuf_locked() {
    if (g_agent.replica_fd < 0 || g_agent.outbuf_off >= g_agent.outbuf.size()) {
        return;
    }
    size_t off = g_agent.outbuf_off;
    size_t total = g_agent.outbuf.size();
    while (off < total) {
        ssize_t n = send(g_agent.replica_fd, g_agent.outbuf.data() + off, total - off,
                         MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n > 0) {
            off += (size_t)n;
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            break;
        }
        drop_replica_conn_locked("send failed");
        return;
    }

    g_agent.outbuf_off = off;
    // 已全部发出：直接清空。否则只在已发前缀足够大时搬移一次，
    // 把「每发一点就 memmove 整个缓冲」的平方级开销摊平。
    if (off == total) {
        g_agent.outbuf.clear();
        g_agent.outbuf_off = 0;
    } else if (off >= (1u << 20)) {
        g_agent.outbuf.erase(0, off);
        g_agent.outbuf_off = 0;
    }
}

// 把一条命令排进增量通道（带 4 字节长度头）。通道不可用时返回 -1。
int enqueue_command_locked(const std::string& payload) {
    if (g_agent.replica_fd < 0) {
        return -1;
    }
    if (g_agent.outbuf.size() - g_agent.outbuf_off + payload.size() + 4 > kOutbufLimit) {
        drop_replica_conn_locked("outbuf overflow");
        return -1;
    }

    uint32_t net_len = htonl((uint32_t)payload.size());
    g_agent.outbuf.append((const char*)&net_len, sizeof(net_len));
    g_agent.outbuf.append(payload);
    flush_outbuf_locked();
    return 0;
}

// ==================== 采集回调（consumer 线程） ====================

void on_capture_record(int argc, char* argv[]) {
    pthread_mutex_lock(&g_agent_mutex);

    // 控制记录：与写命令在同一条有序流里，直接驱动状态机。
    if (argc == 1 && strcmp(argv[0], "SYNC") == 0) {
        g_agent.cache.clear();
        g_agent.cache_bytes = 0;
        g_agent.state = ST_CACHING;
        g_round_invalid = false;
        agent_log("SYNC observed: caching increments for the full-sync window");
        pthread_mutex_unlock(&g_agent_mutex);
        return;
    }
    if (argc == 1 && strcmp(argv[0], "SYNC FINISHED") == 0) {
        if (g_round_invalid) {
            // 这一轮的缓存已因溢出被丢弃：不能把不完整的数据接进实时转发。
            // 关闭已经接受的反向连接，让 Replica 立即观察到断开并重新全量同步。
            drop_replica_conn_locked("full-sync window invalidated");
            g_round_invalid = false;
            g_agent.state = ST_IDLE;
            agent_log("full-sync window was invalidated (cache overflow); skipping this round so "
                      "the replica re-syncs");
            pthread_mutex_unlock(&g_agent_mutex);
            return;
        }
        // 从机在发送 SYNC FINISHED 前已经主动接入；此时可以把缓存按序补发。
        if (g_agent.replica_fd < 0) {
            agent_log("SYNC FINISHED observed without replica channel; this round is invalid");
            g_agent.cache.clear();
            g_agent.cache_bytes = 0;
            g_agent.state = ST_IDLE;
            pthread_mutex_unlock(&g_agent_mutex);
            return;
        }
        size_t flushed = g_agent.cache.size();
        while (!g_agent.cache.empty()) {
            const std::string& payload = g_agent.cache.front();
            if (enqueue_command_locked(payload) != 0) {
                break; // 通道不可用：丢弃缓存，等待下一轮 SYNC 重新对齐
            }
            g_agent.cache.pop_front();
        }
        g_agent.cache_bytes = 0;
        g_agent.cache.clear();
        g_agent.state = ST_LIVE;
        agent_log("SYNC FINISHED observed: flushed %zu cached command(s), now live", flushed);
        pthread_mutex_unlock(&g_agent_mutex);
        return;
    }
    if (argc == 1 && strcmp(argv[0], "INVALIDATE") == 0) {
        // Master 做了整库替换等改变全量数据的事：缓存与已投递的增量都不再
        // 能保证与 Master 一致，断开增量通道让 Replica 重新走一次全量同步。
        agent_log("INVALIDATE observed: dropping increment channel to force a re-sync");
        drop_replica_conn_locked("invalidate from master");
        g_agent.cache.clear();
        g_agent.cache_bytes = 0;
        g_agent.state = ST_IDLE;
        pthread_mutex_unlock(&g_agent_mutex);
        return;
    }

    // 普通写命令。
    if (g_agent.state == ST_CACHING) {
        std::string payload;
        if (encode_command(argc, argv, &payload)) {
            if (g_agent.cache.size() >= g_agent.cache_max_records ||
                g_agent.cache_bytes + payload.size() > g_agent.cache_max_bytes) {
                // 缓存超限：继续同步就会丢命令。断开增量通道让 Replica 重新全量同步，
                // 之后的 SYNC 记录会让状态机重新进入 CACHING。
                agent_log("cache overflow (%zu bytes, %zu records); dropping increment channel "
                          "to force a re-sync",
                          g_agent.cache_bytes, g_agent.cache.size());
                drop_replica_conn_locked("cache overflow");
                g_agent.cache.clear();
                g_agent.cache_bytes = 0;
                g_agent.state = ST_IDLE;
                g_round_invalid = true;
            } else {
                g_agent.cache_bytes += payload.size();
                g_agent.cache.push_back(payload);
            }
        }
    } else if (g_agent.state == ST_LIVE) {
        std::string payload;
        if (encode_command(argc, argv, &payload)) {
            if (enqueue_command_locked(payload) == 0) {
                g_forwarded++;
            } else {
                g_forward_failed++;
            }
        }
    }
    // ST_IDLE：还没有同步目标，丢弃。

    pthread_mutex_unlock(&g_agent_mutex);
}

// 采集丢事件 / 记录损坏：无法安全续传，断开增量通道让 Replica 重新全量同步。
void on_capture_resync(const char* reason) {
    pthread_mutex_lock(&g_agent_mutex);
    agent_log("capture needs resync (%s): dropping increment channel", reason);
    drop_replica_conn_locked(reason);
    g_agent.cache.clear();
    g_agent.cache_bytes = 0;
    g_agent.state = ST_IDLE;
    pthread_mutex_unlock(&g_agent_mutex);
}

// 周期性把积压发出去（Replica 之前写不进去时留下的尾巴）。
void drain_outbuf() {
    pthread_mutex_lock(&g_agent_mutex);
    flush_outbuf_locked();
    pthread_mutex_unlock(&g_agent_mutex);
}

// 周期性诊断：缓存大小、已转发/失败条数、采集环丢弃计数。
void log_stats() {
    static unsigned long long last_tick = 0;
    unsigned long long now = (unsigned long long)time(NULL);
    if (now == last_tick) {
        return;
    }
    last_tick = now;

    pthread_mutex_lock(&g_agent_mutex);
    size_t cached = g_agent.cache.size();
    size_t pending = g_agent.outbuf.size();
    agent_state_t st = g_agent.state;
    pthread_mutex_unlock(&g_agent_mutex);

    agent_log("stats: state=%d cached=%zu outbuf=%zu forwarded=%llu failed=%llu ring_dropped=%llu",
              (int)st, cached, pending, g_forwarded, g_forward_failed,
              (unsigned long long)kvs_ebpf_ring_dropped());
}

// ==================== Replica 反向增量通道 ====================

int open_replica_listener(const char* ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        agent_log("create replica listener socket failed: %s", strerror(errno));
        return -1;
    }

    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &address.sin_addr) != 1) {
        agent_log("invalid replica listen ip: %s", ip);
        close(fd);
        return -1;
    }
    if (bind(fd, (struct sockaddr*)&address, sizeof(address)) != 0) {
        agent_log("bind replica listener %s:%d failed: %s", ip, port, strerror(errno));
        close(fd);
        return -1;
    }
    if (listen(fd, 4) != 0) {
        agent_log("listen replica channel on %s:%d failed: %s", ip, port, strerror(errno));
        close(fd);
        return -1;
    }
    return fd;
}

void reset_round_after_channel_loss() {
    pthread_mutex_lock(&g_agent_mutex);
    drop_replica_conn_locked("master disconnected");
    g_agent.cache.clear();
    g_agent.cache_bytes = 0;
    g_agent.state = ST_IDLE;
    g_round_invalid = false;
    pthread_mutex_unlock(&g_agent_mutex);
}

// Replica 必须在发送 SYNC FINISHED 前先接入这里。握手失败时返回 -ERROR，
// 单从机语义下新的有效连接替换旧连接。
void accept_replica_connection(int listen_fd) {
    struct sockaddr_in peer;
    socklen_t peer_size = sizeof(peer);
    int fd = accept(listen_fd, (struct sockaddr*)&peer, &peer_size);
    if (fd < 0) {
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            agent_log("accept replica channel failed: %s", strerror(errno));
        }
        return;
    }

    char payload[128];
    int length = kvs_net_recv_frame(fd, payload, sizeof(payload), kHandshakeTimeoutMs);
    bool valid = length > 0 &&
                 (size_t)length == strlen(KVS_REPL_HANDSHAKE_REPL_INC) &&
                 memcmp(payload, KVS_REPL_HANDSHAKE_REPL_INC, (size_t)length) == 0;
    if (!valid) {
        kvs_net_send_frame(fd, "-ERROR\r\n", strlen("-ERROR\r\n"), kHandshakeTimeoutMs);
        agent_log("rejected invalid replica channel handshake");
        close(fd);
        return;
    }
    if (kvs_net_send_frame(fd, "+OK\r\n", strlen("+OK\r\n"), kHandshakeTimeoutMs) != 0) {
        close(fd);
        return;
    }

    char peer_ip[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &peer.sin_addr, peer_ip, sizeof(peer_ip));
    int old_fd = -1;
    pthread_mutex_lock(&g_agent_mutex);
    old_fd = g_agent.replica_fd;
    g_agent.replica_fd = fd;
    g_agent.outbuf.clear();
    g_agent.outbuf_off = 0;
    pthread_mutex_unlock(&g_agent_mutex);

    if (old_fd >= 0 && old_fd != fd) {
        close(old_fd);
        agent_log("replica channel replaced: fd %d -> %d", old_fd, fd);
    }
    agent_log("increment channel accepted from %s:%d (fd=%d)", peer_ip, ntohs(peer.sin_port),
              fd);
}

// Master 尚未连上时仍服务监听端口，避免短时重连窗口内 Replica 被拒绝。
void service_replica_channels_without_master(int listen_fd, int timeout_ms) {
    pthread_mutex_lock(&g_agent_mutex);
    int replica_fd = g_agent.replica_fd;
    pthread_mutex_unlock(&g_agent_mutex);

    struct pollfd pfds[2];
    pfds[0].fd = listen_fd;
    pfds[0].events = POLLIN;
    pfds[0].revents = 0;
    pfds[1].fd = replica_fd;
    pfds[1].events = replica_fd >= 0 ? POLLIN : 0;
    pfds[1].revents = 0;

    int pr = poll(pfds, replica_fd >= 0 ? 2 : 1, timeout_ms);
    if (pr < 0 && errno != EINTR) {
        return;
    }
    if (pfds[0].revents & POLLIN) {
        accept_replica_connection(listen_fd);
    }
    if (replica_fd >= 0 && pfds[1].revents != 0) {
        char tmp[64];
        ssize_t n = recv(replica_fd, tmp, sizeof(tmp), MSG_DONTWAIT);
        pthread_mutex_lock(&g_agent_mutex);
        drop_replica_conn_locked(n > 0 ? "unexpected replica data" : "replica disconnected");
        pthread_mutex_unlock(&g_agent_mutex);
    }
    drain_outbuf();
}

// ==================== master 注册连接 ====================

// 注册并保持一条到 master 的控制连接：master 据此把 SYNC 放开（eBPF 强制）。
// 连接断开时重连，期间 master 会拒绝新的 SYNC。
// 返回进程退出码：0 = 正常停止，非 0 = 采集链路无法自愈（需要重启 agent）。
int master_register_loop(volatile sig_atomic_t* running, const char* ring_path,
                         int replica_listen_fd) {
    bool first_registration = true;

    while (*running) {
        int fd = kvs_net_connect(g_master_ip, g_master_port, kHandshakeTimeoutMs);
        if (fd < 0) {
            agent_log("connect master %s:%d failed, retry", g_master_ip, g_master_port);
            service_replica_channels_without_master(replica_listen_fd, 1000);
            continue;
        }
        if (kvs_net_send_frame(fd, KVS_REPL_HANDSHAKE_EBPF, strlen(KVS_REPL_HANDSHAKE_EBPF), kHandshakeTimeoutMs) != 0) {
            close(fd);
            service_replica_channels_without_master(replica_listen_fd, 1000);
            continue;
        }
        if (kvs_net_recv_frame_ok(fd, kHandshakeTimeoutMs) != 1) {
            agent_log("master rejected EBPF registration");
            close(fd);
            service_replica_channels_without_master(replica_listen_fd, 1000);
            continue;
        }
        agent_log("registered with master %s:%d", g_master_ip, g_master_port);

        // 不是第一次注册：说明 Master 重启过。采集通道（sim 的环 / uprobe 的探针）
        // 绑在旧进程上已经失效，必须重建；重建不了就退出，避免「已注册但采不到」
        // 这种会让 Master 误判可以同步的状态。
        if (!first_registration) {
            reset_round_after_channel_loss();
            if (kvs_ebpf_agent_capture_reopen(ring_path) != 0) {
                agent_log("capture could not be re-established after master restart; exiting "
                          "(uprobe mode needs a restart with the new master pid)");
                close(fd);
                return 3;
            }
        }
        first_registration = false;

        // 保持 Master 控制连接，同时接受 Replica 主动接入的反向增量通道。
        while (*running) {
            pthread_mutex_lock(&g_agent_mutex);
            int replica_fd = g_agent.replica_fd;
            pthread_mutex_unlock(&g_agent_mutex);

            struct pollfd pfds[3];
            pfds[0].fd = fd;
            pfds[0].events = POLLIN;
            pfds[0].revents = 0;
            pfds[1].fd = replica_listen_fd;
            pfds[1].events = POLLIN;
            pfds[1].revents = 0;
            pfds[2].fd = replica_fd;
            pfds[2].events = replica_fd >= 0 ? POLLIN : 0;
            pfds[2].revents = 0;

            int poll_count = replica_fd >= 0 ? 3 : 2;
            // 10ms 一轮：及时接受反向连接、清理断线并发送积压。
            int pr = poll(pfds, (nfds_t)poll_count, 10);
            if (pr < 0 && errno != EINTR) {
                agent_log("poll agent channels failed: %s", strerror(errno));
                break;
            }
            if (pr > 0 && (pfds[0].revents & (POLLIN | POLLHUP | POLLERR))) {
                char tmp[64];
                ssize_t n = recv(fd, tmp, sizeof(tmp), 0);
                if (n <= 0) {
                    break;
                }
            }
            if (pfds[1].revents & POLLIN) {
                accept_replica_connection(replica_listen_fd);
            }
            if (replica_fd >= 0 && pfds[2].revents != 0) {
                char tmp[64];
                ssize_t n = recv(replica_fd, tmp, sizeof(tmp), MSG_DONTWAIT);
                pthread_mutex_lock(&g_agent_mutex);
                const char* reason = n > 0 ? "unexpected replica data" : "replica disconnected";
                drop_replica_conn_locked(reason);
                pthread_mutex_unlock(&g_agent_mutex);
            }
            drain_outbuf();
            log_stats();
        }
        reset_round_after_channel_loss();
        close(fd);
        agent_log("master connection lost, re-registering");
        sleep(1);
    }
    return 0;
}

void usage(const char* prog) {
    fprintf(stderr,
            "用法: %s [--config <path>] [--master <ip:port>] [--listen <ip:port>]\n"
            "          [--mode <uprobe|sim>]\n"
            "          [--ring <path>] [--pid <master_pid>] [--binary <master_exe>]\n"
            "          [--cache-max-bytes N] [--cache-max-records N]\n"
            "  未指定 --config 时依次读取 ./ebpf-agent.conf、../ebpf-agent.conf；\n"
            "  命令行参数覆盖配置文件中的同名字段。\n"
            "  --mode sim    读取 master 创建的共享内存环（无 CAP_BPF 时使用）\n"
            "  --mode uprobe  在 master 二进制上挂 uprobe + ringbuf（需要 --pid/--binary）\n",
            prog);
}

} // namespace

static volatile sig_atomic_t g_running = 1;

static void on_signal(int) { g_running = 0; }

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        }
    }

    kvs_agent_config_t config;
    kvs_agent_config_init(&config);

    char config_path[512];
    int has_explicit_path = 0;
    if (kvs_agent_config_select_path(argc, argv, config_path, sizeof(config_path),
                                     &has_explicit_path) != 0) {
        fprintf(stderr, "--config 需要且只能指定一个文件路径\n");
        usage(argv[0]);
        return 2;
    }

    int config_rc = has_explicit_path ? kvs_agent_config_load_file(&config, config_path)
                                      : kvs_agent_config_load_default(&config);
    if (config_rc < 0) {
        fprintf(stderr, "无法加载 ebpf-agent 配置%s%s\n", has_explicit_path ? ": " : "（默认文件）",
                has_explicit_path ? config_path : "");
        return 2;
    }

    int parse_rc = kvs_agent_config_parse_args(&config, argc, argv);
    if (parse_rc != 0) {
        if (parse_rc < 0) {
            fprintf(stderr, "未知参数或参数值非法\n");
        }
        usage(argv[0]);
        return 2;
    }
    if (kvs_agent_config_validate(&config) != 0) {
        fprintf(stderr, "配置不完整：需要 master 地址和 listen 地址");
        if (config.mode == KVS_EBPF_MODE_SIM) {
            fprintf(stderr, "、--ring");
        } else {
            fprintf(stderr, "、--pid 与 --binary");
        }
        fprintf(stderr, "\n");
        usage(argv[0]);
        return 2;
    }

    snprintf(g_master_ip, sizeof(g_master_ip), "%s", config.master_ip);
    g_master_port = config.master_port;
    snprintf(g_listen_ip, sizeof(g_listen_ip), "%s", config.listen_ip);
    g_listen_port = config.listen_port;

    const int mode = config.mode;
    const char* ring_path = config.ring_path[0] == '\0' ? NULL : config.ring_path;
    const char* master_binary = config.master_binary[0] == '\0' ? NULL : config.master_binary;
    const int master_pid = config.master_pid;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    g_agent.state = ST_IDLE;
    g_agent.cache_bytes = 0;
    g_agent.cache_max_bytes = (size_t)config.cache_max_bytes;
    g_agent.cache_max_records = (size_t)config.cache_max_records;
    g_agent.replica_fd = -1;
    g_agent.outbuf_off = 0;

    // 采集必须先就绪：eBPF 强制下，采集失败就没有增量来源，直接退出而不是
    // 让 master 误以为可以同步。
    if (kvs_ebpf_agent_capture_start(mode, ring_path, master_pid, master_binary,
                                     on_capture_record, on_capture_resync) != 0) {
        fprintf(stderr, "[EBPF-AGENT] capture start failed\n");
        return 1;
    }
    agent_log("capture started (mode=%s), master=%s:%d listen=%s:%d",
              mode == KVS_EBPF_MODE_SIM ? "sim" : "uprobe", g_master_ip, g_master_port,
              g_listen_ip, g_listen_port);

    int replica_listen_fd = open_replica_listener(g_listen_ip, g_listen_port);
    if (replica_listen_fd < 0) {
        kvs_ebpf_agent_capture_stop();
        return 1;
    }
    agent_log("listening for replica on %s:%d", g_listen_ip, g_listen_port);

    int rc = master_register_loop(&g_running, ring_path, replica_listen_fd);

    close(replica_listen_fd);
    kvs_ebpf_agent_capture_stop();
    pthread_mutex_lock(&g_agent_mutex);
    drop_replica_conn_locked("agent exiting");
    pthread_mutex_unlock(&g_agent_mutex);
    agent_log("stopped");
    return rc;
}
