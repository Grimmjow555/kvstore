// ebpf-agent：独立的增量采集与转发进程（重设计阶段 3）。
//
// 用法：
//   ebpf-agent --master 127.0.0.1:9999 --replica 127.0.0.1:10001 \
//              --mode sim --ring ../data/kvstore.ebpf.ring
//   ebpf-agent --master ... --replica ... --mode uprobe --pid <master_pid> \
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
#include "kvs_config.h"

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

// 本轮全量同步窗口是否已经失效（缓存溢出等）。失效后不再建立增量通道，
// 让 Replica 的等待超时并重新全量同步——避免「带着缺口进入实时转发」。
bool g_round_invalid = false;

char g_master_ip[64];
int g_master_port = 0;
char g_replica_ip[64];
int g_replica_port = 0;


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

bool parse_host_port(const char* text, char* ip, size_t ip_size, int* port) {
    const char* colon = strrchr(text, ':');
    if (colon == NULL || colon == text) {
        return false;
    }
    size_t len = (size_t)(colon - text);
    if (len + 1 > ip_size) {
        return false;
    }
    memcpy(ip, text, len);
    ip[len] = '\0';
    int p = atoi(colon + 1);
    if (p <= 0 || p > 65535) {
        return false;
    }
    *port = p;
    return true;
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

// 确保增量通道已建立（含 REPL_INC 握手）。
int ensure_replica_conn_locked() {
    if (g_agent.replica_fd >= 0) {
        return 0;
    }

    int fd = kvs_net_connect(g_replica_ip, g_replica_port, kHandshakeTimeoutMs);
    if (fd < 0) {
        return -1;
    }
    if (kvs_net_send_frame(fd, KVS_REPL_HANDSHAKE_REPL_INC, strlen(KVS_REPL_HANDSHAKE_REPL_INC), kHandshakeTimeoutMs) != 0) {
        close(fd);
        return -1;
    }
    // 读握手响应（Replica 回的是 +OK 帧），失败说明对端不认这条通道。
    if (kvs_net_recv_frame_ok(fd, kHandshakeTimeoutMs) != 1) {
        close(fd);
        return -1;
    }

    g_agent.replica_fd = fd;
    g_agent.outbuf.clear();
    g_agent.outbuf_off = 0;
    agent_log("increment channel connected to %s:%d", g_replica_ip, g_replica_port);
    return 0;
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
    if (ensure_replica_conn_locked() != 0) {
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
            // 这里不建立增量通道，Replica 等待超时后会重新走一次全量同步。
            g_round_invalid = false;
            g_agent.state = ST_IDLE;
            agent_log("full-sync window was invalidated (cache overflow); skipping this round so "
                      "the replica re-syncs");
            pthread_mutex_unlock(&g_agent_mutex);
            return;
        }
        // 从机此刻已经加载完快照，可以把缓存按序补发过去了。
        // 即使缓存为空也要先把增量通道建起来：从机在等这条通道接入才会转入
        // 常规回放，否则它会一直停在「waiting for increment channel」。
        ensure_replica_conn_locked();
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

// ==================== master 注册连接 ====================

// 注册并保持一条到 master 的控制连接：master 据此把 SYNC 放开（eBPF 强制）。
// 连接断开时重连，期间 master 会拒绝新的 SYNC。
// 返回进程退出码：0 = 正常停止，非 0 = 采集链路无法自愈（需要重启 agent）。
int master_register_loop(volatile sig_atomic_t* running, const char* ring_path) {
    bool first_registration = true;

    while (*running) {
        int fd = kvs_net_connect(g_master_ip, g_master_port, kHandshakeTimeoutMs);
        if (fd < 0) {
            agent_log("connect master %s:%d failed, retry", g_master_ip, g_master_port);
            sleep(1);
            continue;
        }
        if (kvs_net_send_frame(fd, KVS_REPL_HANDSHAKE_EBPF, strlen(KVS_REPL_HANDSHAKE_EBPF), kHandshakeTimeoutMs) != 0) {
            close(fd);
            sleep(1);
            continue;
        }
        if (kvs_net_recv_frame_ok(fd, kHandshakeTimeoutMs) != 1) {
            agent_log("master rejected EBPF registration");
            close(fd);
            sleep(1);
            continue;
        }
        agent_log("registered with master %s:%d", g_master_ip, g_master_port);

        // 不是第一次注册：说明 Master 重启过。采集通道（sim 的环 / uprobe 的探针）
        // 绑在旧进程上已经失效，必须重建；重建不了就退出，避免「已注册但采不到」
        // 这种会让 Master 误判可以同步的状态。
        if (!first_registration) {
            if (kvs_ebpf_agent_capture_reopen(ring_path) != 0) {
                agent_log("capture could not be re-established after master restart; exiting "
                          "(uprobe mode needs a restart with the new master pid)");
                close(fd);
                return 3;
            }
        }
        first_registration = false;

        // 保持连接：master 从不在这条连接上发数据，读到 EOF 即视为断开。
        while (*running) {
            struct pollfd pfd;
            pfd.fd = fd;
            pfd.events = POLLIN;
            pfd.revents = 0;
            // 10ms 一轮：既及时把积压发给 Replica，也不空转浪费 CPU。
            int pr = poll(&pfd, 1, 10);
            if (pr > 0) {
                char tmp[64];
                ssize_t n = recv(fd, tmp, sizeof(tmp), 0);
                if (n <= 0) {
                    break;
                }
                continue;
            }
            drain_outbuf();
            log_stats();
        }
        close(fd);
        agent_log("master connection lost, re-registering");
        sleep(1);
    }
    return 0;
}

void usage(const char* prog) {
    fprintf(stderr,
            "用法: %s --master <ip:port> --replica <ip:port> --mode <uprobe|sim>\n"
            "          [--ring <path>] [--pid <master_pid>] [--binary <master_exe>]\n"
            "          [--cache-max-bytes N] [--cache-max-records N]\n"
            "  --mode sim    读取 master 创建的共享内存环（无 CAP_BPF 时使用）\n"
            "  --mode uprobe  在 master 二进制上挂 uprobe + ringbuf（需要 --pid/--binary）\n",
            prog);
}

} // namespace

static volatile sig_atomic_t g_running = 1;

static void on_signal(int) { g_running = 0; }

int main(int argc, char* argv[]) {
    int mode = KVS_EBPF_MODE_UPROBE;
    const char* ring_path = NULL;
    const char* master_binary = NULL;
    int master_pid = 0;
    const char* master_addr = NULL;
    const char* replica_addr = NULL;
    long cache_max_bytes = 256L * 1024 * 1024;
    long cache_max_records = 500000;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--master") == 0 && i + 1 < argc) {
            master_addr = argv[++i];
        } else if (strcmp(argv[i], "--replica") == 0 && i + 1 < argc) {
            replica_addr = argv[++i];
        } else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            const char* m = argv[++i];
            if (strcmp(m, "sim") == 0) {
                mode = KVS_EBPF_MODE_SIM;
            } else if (strcmp(m, "uprobe") == 0) {
                mode = KVS_EBPF_MODE_UPROBE;
            } else {
                usage(argv[0]);
                return 2;
            }
        } else if (strcmp(argv[i], "--ring") == 0 && i + 1 < argc) {
            ring_path = argv[++i];
        } else if (strcmp(argv[i], "--binary") == 0 && i + 1 < argc) {
            master_binary = argv[++i];
        } else if (strcmp(argv[i], "--pid") == 0 && i + 1 < argc) {
            master_pid = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--cache-max-bytes") == 0 && i + 1 < argc) {
            cache_max_bytes = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--cache-max-records") == 0 && i + 1 < argc) {
            cache_max_records = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "未知参数: %s\n", argv[i]);
            usage(argv[0]);
            return 2;
        }
    }

    if (master_addr == NULL || replica_addr == NULL ||
        !parse_host_port(master_addr, g_master_ip, sizeof(g_master_ip), &g_master_port) ||
        !parse_host_port(replica_addr, g_replica_ip, sizeof(g_replica_ip), &g_replica_port)) {
        usage(argv[0]);
        return 2;
    }
    if (mode == KVS_EBPF_MODE_UPROBE && (master_pid <= 0 || master_binary == NULL)) {
        fprintf(stderr, "uprobe 模式需要 --pid 与 --binary\n");
        return 2;
    }
    if (mode == KVS_EBPF_MODE_SIM && ring_path == NULL) {
        fprintf(stderr, "sim 模式需要 --ring\n");
        return 2;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    g_agent.state = ST_IDLE;
    g_agent.cache_bytes = 0;
    g_agent.cache_max_bytes = (size_t)(cache_max_bytes > 0 ? cache_max_bytes : 1);
    g_agent.cache_max_records = (size_t)(cache_max_records > 0 ? cache_max_records : 1);
    g_agent.replica_fd = -1;
    g_agent.outbuf_off = 0;

    // 采集必须先就绪：eBPF 强制下，采集失败就没有增量来源，直接退出而不是
    // 让 master 误以为可以同步。
    if (kvs_ebpf_agent_capture_start(mode, ring_path, master_pid, master_binary,
                                     on_capture_record, on_capture_resync) != 0) {
        fprintf(stderr, "[EBPF-AGENT] capture start failed\n");
        return 1;
    }
    agent_log("capture started (mode=%s), master=%s replica=%s",
              mode == KVS_EBPF_MODE_SIM ? "sim" : "uprobe", master_addr, replica_addr);

    int rc = master_register_loop(&g_running, ring_path);

    kvs_ebpf_agent_capture_stop();
    pthread_mutex_lock(&g_agent_mutex);
    drop_replica_conn_locked("agent exiting");
    pthread_mutex_unlock(&g_agent_mutex);
    agent_log("stopped");
    return rc;
}
