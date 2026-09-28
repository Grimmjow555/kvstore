#include "kvs_config.h"
#include "kvs_ebpf.h"
#include "kvs_replication.h"
#include "kvs_snapshot.h"
#include "kvstore.h"
#include <arpa/inet.h>
#include <deque>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

extern int kvs_protocol(char* msg, int length, char* response, int response_size);

// ==================== 设计说明 ====================
//
// 旧实现的问题：
//   1. 写命令在事件循环里对所有 Replica 做阻塞 send，慢副本会拖死整个 Master；
//   2. 全量同步期间 pending 副本直接丢弃增量命令，RDMA 路径存在确定的丢写窗口；
//   3. 断连副本槽位不清理，fd 被复用后复制帧会写进普通客户端连接。
//
// 现实现把「先落增量日志、再按序号投递」作为唯一数据通路：
//
//   Master 写命令 -> （eBPF 可用时先经 uprobe + ringbuf 采集，见 kvs_ebpf.cpp）
//                 -> 追加到全局增量日志 backlog（带递增序号）
//                 -> 独立发送线程按每个副本自己的 next_seq 投递
//   Replica 握手  -> Master 生成一份与序号对齐的全量快照（RDB 二进制）
//                 -> 快照传完后从快照序号继续补发 backlog
//
// 关键不变量：
//   * 任何写命令只需写一次 backlog，对所有副本只有一份数据；
//   * 全量快照与 backlog 序号在同一把存储锁内取得，快照之后落地的命令
//     必然还在 backlog 里，因此不会再出现丢写窗口；
//   * 事件循环只做「编码 + 入队」，网络发送全部在发送线程里用
//     MSG_DONTWAIT 完成，慢副本只会拖慢自己，不会阻塞 Master；
//   * fd 归网络层所有，复制模块只 shutdown() 触发对端断开，绝不 close()。

// Master 最多同时维护的 Replica 连接数。
#define MAX_REPLICAS 16

// 复制通道单帧最大长度（含类型字节）。普通命令上限 1MB，快照分片 256KB。
#define REPL_MAX_FRAME (4u * 1024 * 1024)
// 全量快照分片大小。
#define REPL_SNAPSHOT_CHUNK (256u * 1024)
// 发送线程每次从 backlog 取出的字节上限。
#define REPL_SEND_CHUNK (64u * 1024)
// 增量日志内存上限，超出后丢弃最老记录；落后过多的副本会自动重新全量同步。
#define REPL_BACKLOG_MAX_BYTES (32u * 1024 * 1024)
// 副本多久没有任何发送进展就判定为慢副本并断开。
#define REPL_STALL_TIMEOUT_MS 30000
// RDMA 快照对齐记录的有效期。
#define REPL_RDMA_NOTE_TTL_MS 60000
// 取全量快照前等待 eBPF 采集通道排空的最长时间。
#define KVS_REPL_EBPF_DRAIN_TIMEOUT_MS 200

#define REPL_SLOT_FREE 0
#define REPL_SLOT_PENDING 1 // 已登记握手，尚未开始投递（握手响应还没发出去）
#define REPL_SLOT_SYNCING 2 // 正在发送全量快照 / 增量补齐
#define REPL_SLOT_ONLINE 3  // 已追平过至少一次，进入常规增量投递

struct repl_slot_t {
    int fd;
    int state;
    char peer_ip[INET_ADDRSTRLEN];

    // 已从 backlog 取出、等待写入 socket 的字节（含帧头）。
    std::string out;
    size_t out_sent;
    int prefix_done;             // out 中的全量同步前缀是否已发完
    int fullsync_done_sent;      // 是否已下发 FULLSYNC_DONE
    unsigned long long next_seq; // 下一条待投递的 backlog 序号
    unsigned long long last_progress_ms;
};

struct rdma_note_t {
    int used;
    char peer_ip[INET_ADDRSTRLEN];
    unsigned long long seq;
    unsigned long long ts_ms;
};

static kvs_role_t g_role = KVS_ROLE_MASTER;

static repl_slot_t g_slots[MAX_REPLICAS];
static rdma_note_t g_rdma_notes[MAX_REPLICAS];

// g_repl_mutex 保护：g_slots、backlog、g_rdma_notes、发送线程状态。
// 锁顺序固定为 存储锁(kvs_data_lock) -> g_repl_mutex，任何路径都不得反向获取。
static pthread_mutex_t g_repl_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_repl_cond = PTHREAD_COND_INITIALIZER;

// 增量日志：按写入顺序保存已经编码好的 RESP 命令。
// 队首对应序号 (g_backlog_head - g_backlog.size())。
static std::deque<std::string> g_backlog;
static unsigned long long g_backlog_head = 0; // 已写入的记录总数 = 下一条记录序号
static size_t g_backlog_bytes = 0;

static pthread_t g_sender_tid;
static int g_sender_running = 0;

// Replica 侧状态。
static pthread_t g_replica_tid;
static volatile int replica_running = 0;
static int master_fd = -1;
static char g_master_ip[INET_ADDRSTRLEN] = {0};
static int g_master_port = 0;

// 复制回放标记必须是线程局部量：Replica 的回放线程与本地客户端线程共享
// 同一份全局标志时，回放窗口内本地客户端的写会被误判成回放而不落 AOF。
static __thread int replication_replaying = 0;

#ifdef KVS_ENABLE_RDMA
static int g_rdma_enabled = 0;

int kvs_replication_is_rdma_enabled() { return g_rdma_enabled; }

void kvs_replication_set_rdma_enabled(int enabled) { g_rdma_enabled = enabled ? 1 : 0; }
#endif

static const char* replication_handshake = "*1\r\n$7\r\nREPLICA\r\n";

// ==================== 通用工具 ====================

static unsigned long long repl_now_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (unsigned long long)ts.tv_sec * 1000ull + (unsigned long long)ts.tv_nsec / 1000000ull;
}

static void repl_sleep_ms(int ms) {
    if (ms <= 0) {
        return;
    }
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

// 可被打断的退避睡眠：优雅关闭时不必等整个退避周期睡完。
// running 指向停止标志，被置 0 后最多再等一个分片（50ms）就返回。
static void repl_sleep_ms_interruptible(int ms, const volatile int* running) {
    int waited = 0;
    while (waited < ms) {
        if (running != NULL && *running == 0) {
            return;
        }
        int slice = ms - waited < 50 ? ms - waited : 50;
        struct timespec ts;
        ts.tv_sec = slice / 1000;
        ts.tv_nsec = (long)(slice % 1000) * 1000000L;
        nanosleep(&ts, NULL);
        waited += slice;
    }
}

// 循环发送，直到全部数据发出或出错。
static int send_all(int fd, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
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

static int recv_all(int fd, char* data, size_t len) {
    size_t received = 0;
    while (received < len) {
        ssize_t n = recv(fd, data + received, len - received, 0);
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

// 发送「网络层」帧：4 字节网络序长度 + 裸负载（无类型字节）。
//
// 注意区分两类帧：
//   * 网络层帧（握手、握手 +OK 响应）：裸 RESP 负载，由网络后端收发；
//   * 复制通道帧（append_frame_header 拼装）：负载首字节是类型字段。
// 握手走的是网络层通道，必须用这里的裸帧格式。
static int send_raw_frame(int fd, const char* data, size_t len) {
    if (len > UINT32_MAX) {
        return -1;
    }
    uint32_t net_len = htonl((uint32_t)len);
    size_t total = sizeof(net_len) + len;

    char stack_buf[1024];
    char* frame = stack_buf;
    if (total > sizeof(stack_buf)) {
        frame = (char*)malloc(total);
        if (frame == NULL) {
            return -1;
        }
    }

    memcpy(frame, &net_len, sizeof(net_len));
    if (len > 0) {
        memcpy(frame + sizeof(net_len), data, len);
    }
    int rc = send_all(fd, frame, total);
    if (frame != stack_buf) {
        free(frame);
    }
    return rc;
}

// ==================== backlog（增量日志） ====================

static void backlog_push_locked(const std::string& payload) {
    g_backlog.push_back(payload);
    g_backlog_bytes += payload.size();
    g_backlog_head++;

    while (g_backlog_bytes > REPL_BACKLOG_MAX_BYTES && g_backlog.size() > 1) {
        g_backlog_bytes -= g_backlog.front().size();
        g_backlog.pop_front();
    }
}

static unsigned long long backlog_tail_locked() {
    return g_backlog_head - (unsigned long long)g_backlog.size();
}

static const std::string& backlog_at_locked(unsigned long long seq) {
    return g_backlog[(size_t)(seq - backlog_tail_locked())];
}

unsigned long long kvs_replication_backlog_offset() {
    pthread_mutex_lock(&g_repl_mutex);
    unsigned long long seq = g_backlog_head;
    pthread_mutex_unlock(&g_repl_mutex);
    return seq;
}

// ==================== Master：槽位管理 ====================

static int find_slot_locked(int fd) {
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        if (g_slots[i].fd == fd && g_slots[i].state != REPL_SLOT_FREE) {
            return i;
        }
    }
    return -1;
}

// 复制模块主动放弃一个副本：只 shutdown，不 close。
// shutdown 会让网络层的 recv/epoll 立刻返回，由网络层负责 close 并回调
// kvs_replication_remove_replica()，这样 fd 号不会在复制线程里被提前释放。
static void drop_slot_locked(int slot, const char* reason) {
    repl_slot_t& s = g_slots[slot];
    if (s.fd >= 0) {
        kvs_log(KVS_LOG_WARN, "[REPLICATION] drop replica fd=%d (%s)", s.fd, reason);
        shutdown(s.fd, SHUT_RDWR);
    }
    s.fd = -1;
    s.state = REPL_SLOT_FREE;
    s.next_seq = 0;
    s.out.clear();
    s.out_sent = 0;
    s.prefix_done = 0;
    s.fullsync_done_sent = 0;
    s.peer_ip[0] = '\0';
}

static void fill_peer_ip(int fd, char* out, size_t out_size) {
    out[0] = '\0';
    struct sockaddr_in peer;
    socklen_t peer_len = sizeof(peer);
    if (getpeername(fd, (struct sockaddr*)&peer, &peer_len) != 0) {
        return;
    }
    if (peer.sin_family != AF_INET) {
        return;
    }
    inet_ntop(AF_INET, &peer.sin_addr, out, (socklen_t)out_size);
}

// 打开 TCP keepalive：对端进程被强杀、网络中断等场景下，
// 让网络层能在有限时间内感知并回调 remove_replica，避免槽位长期泄漏。
static void enable_tcp_keepalive(int fd) {
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on));
    int idle = 10;
    int intvl = 3;
    int cnt = 3;
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));
}

int kvs_replication_add_replica(int fd) {
    if (g_role != KVS_ROLE_MASTER || fd < 0) {
        return -1;
    }

    pthread_mutex_lock(&g_repl_mutex);
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        repl_slot_t& s = g_slots[i];
        if (s.state != REPL_SLOT_FREE) {
            continue;
        }
        s.fd = fd;
        s.state = REPL_SLOT_PENDING;
        s.next_seq = g_backlog_head;
        s.out.clear();
        s.out_sent = 0;
        s.prefix_done = 0;
        s.fullsync_done_sent = 0;
        s.last_progress_ms = repl_now_ms();
        fill_peer_ip(fd, s.peer_ip, sizeof(s.peer_ip));
        pthread_mutex_unlock(&g_repl_mutex);

        enable_tcp_keepalive(fd);
        kvs_log(KVS_LOG_INFO, "[REPLICATION] replica connected fd=%d", fd);
        return 0;
    }
    pthread_mutex_unlock(&g_repl_mutex);

    kvs_log(KVS_LOG_WARN, "[REPLICATION] replica slots exhausted, reject fd=%d", fd);
    return -1;
}

void kvs_replication_remove_replica(int fd) {
    if (fd < 0) {
        return;
    }

    pthread_mutex_lock(&g_repl_mutex);
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        repl_slot_t& s = g_slots[i];
        if (s.fd != fd || s.state == REPL_SLOT_FREE) {
            continue;
        }
        s.fd = -1;
        s.state = REPL_SLOT_FREE;
        s.next_seq = 0;
        s.out.clear();
        s.out_sent = 0;
        s.prefix_done = 0;
        s.fullsync_done_sent = 0;
        s.peer_ip[0] = '\0';
        kvs_log(KVS_LOG_INFO, "[REPLICATION] replica removed fd=%d", fd);
        break;
    }
    pthread_cond_broadcast(&g_repl_cond);
    pthread_mutex_unlock(&g_repl_mutex);
}

// Replica -> Master 的握手：*1\r\n$7\r\nREPLICA\r\n
// 实时增量统一走 backlog + 发送线程（TCP/RDMA），握手不再携带任何本地通道信息。
static int parse_handshake(const char* data, int length) {
    static const char legacy[] = "*1\r\n$7\r\nREPLICA\r\n";
    if (data == NULL || length != (int)(sizeof(legacy) - 1)) {
        return 0;
    }
    return memcmp(data, legacy, sizeof(legacy) - 1) == 0;
}

int kvs_replication_accept_handshake(int fd, const char* data, int length) {
    if (!parse_handshake(data, length)) {
        return 0;
    }

    pthread_mutex_lock(&g_repl_mutex);
    int slot = find_slot_locked(fd);
    pthread_mutex_unlock(&g_repl_mutex);
    if (slot >= 0) {
        // 同一条连接重复握手：已经登记过，直接当作已接收。
        return 1;
    }

    return kvs_replication_add_replica(fd) == 0 ? 1 : -1;
}

// ==================== Master：全量同步 ====================

static void append_frame_header(std::string* out, unsigned char type, size_t payload_len) {
    uint32_t net_len = htonl((uint32_t)(payload_len + 1));
    out->append((const char*)&net_len, sizeof(net_len));
    out->push_back((char)type);
}

#ifdef KVS_ENABLE_RDMA

void kvs_replication_note_rdma_snapshot(const char* peer_ip, unsigned long long seq) {
    if (peer_ip == NULL || peer_ip[0] == '\0') {
        return;
    }

    pthread_mutex_lock(&g_repl_mutex);
    unsigned long long now = repl_now_ms();
    int target = 0;
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        if (g_rdma_notes[i].used && now - g_rdma_notes[i].ts_ms > REPL_RDMA_NOTE_TTL_MS) {
            g_rdma_notes[i].used = 0;
        }
        if (!g_rdma_notes[i].used) {
            target = i;
        }
    }
    g_rdma_notes[target].used = 1;
    snprintf(g_rdma_notes[target].peer_ip, sizeof(g_rdma_notes[target].peer_ip), "%s", peer_ip);
    g_rdma_notes[target].seq = seq;
    g_rdma_notes[target].ts_ms = now;
    pthread_mutex_unlock(&g_repl_mutex);
}

// 取出（并消费）某个对端最近一次 RDMA 快照对应的序号。
// 同一 IP 存在多条未消费记录时无法安全区分是哪个副本，返回 0 改走 TCP 全量。
static int take_rdma_note_locked(const char* peer_ip, unsigned long long* seq) {
    int found = -1;
    int duplicates = 0;
    unsigned long long now = repl_now_ms();

    for (int i = 0; i < MAX_REPLICAS; ++i) {
        rdma_note_t& n = g_rdma_notes[i];
        if (!n.used) {
            continue;
        }
        if (now - n.ts_ms > REPL_RDMA_NOTE_TTL_MS) {
            n.used = 0;
            continue;
        }
        if (strcmp(n.peer_ip, peer_ip) != 0) {
            continue;
        }
        if (found < 0) {
            found = i;
            continue;
        }
        duplicates = 1;
    }

    if (found < 0 || duplicates) {
        if (duplicates) {
            // 无法确定对应关系，宁可走一次 TCP 全量，也不能用错序号。
            for (int i = 0; i < MAX_REPLICAS; ++i) {
                if (g_rdma_notes[i].used && strcmp(g_rdma_notes[i].peer_ip, peer_ip) == 0) {
                    g_rdma_notes[i].used = 0;
                }
            }
        }
        return 0;
    }

    *seq = g_rdma_notes[found].seq;
    g_rdma_notes[found].used = 0;
    return 1;
}

#endif

// 生成 TCP 全量同步前缀：RESET + SNAPSHOT 分片 + SNAPSHOT_END。
//
// 关键点：快照序列化与 backlog 序号在同一个存储锁区间内确定，序列化期间
// 不会再有写命令落地，因此「快照状态」与「base_seq」严格对齐，之后 backlog
// 中序号 >= base_seq 的命令正好是快照之后的新增量，补发不会重复也不会丢失。
static int build_tcp_full_sync_prefix(std::string* prefix, unsigned long long* base_seq) {
    kvs_data_lock();

    // 写命令是先经 eBPF ringbuf 采集、再由消费线程落 backlog 的，两者之间有一小段
    // 异步窗口。这里在存储锁内等消费线程追平（锁内不会再产生新命令），保证下面取到的
    // base_seq 与快照内容严格对齐：快照里已有的命令不会被当成增量再补发一遍。
    kvs_ebpf_wait_drained(KVS_REPL_EBPF_DRAIN_TIMEOUT_MS);

    pthread_mutex_lock(&g_repl_mutex);
    unsigned long long base = g_backlog_head;
    pthread_mutex_unlock(&g_repl_mutex);

    char* blob = NULL;
    size_t blob_len = 0;
    int rc = kvs_snapshot_serialize(&blob, &blob_len);

    kvs_data_unlock();

    if (rc != 0 || blob == NULL) {
        free(blob);
        return -1;
    }

    prefix->clear();
    append_frame_header(prefix, KVS_REPL_FRAME_RESET, 0);
    for (size_t off = 0; off < blob_len; off += REPL_SNAPSHOT_CHUNK) {
        size_t n = blob_len - off;
        if (n > REPL_SNAPSHOT_CHUNK) {
            n = REPL_SNAPSHOT_CHUNK;
        }
        append_frame_header(prefix, KVS_REPL_FRAME_SNAPSHOT, n);
        prefix->append(blob + off, n);
    }
    append_frame_header(prefix, KVS_REPL_FRAME_SNAPSHOT_END, 0);

    free(blob);
    *base_seq = base;
    return 0;
}

// 为某个副本安排一次全量同步。调用时需持有 g_repl_mutex。
// have_base_seq=1 表示对端已通过 RDMA 拿到 base_seq 对应的快照，只需补增量；
// 否则生成 TCP 二进制快照（生成过程会临时释放 g_repl_mutex，避免长时间持锁）。
static int start_full_sync_locked(int slot, int have_base_seq, unsigned long long base_seq) {
    repl_slot_t& s = g_slots[slot];
    if (s.fd < 0 || s.state == REPL_SLOT_FREE) {
        return -1;
    }

    if (!have_base_seq) {
        std::string prefix;
        unsigned long long base = 0;
        int rc;
        pthread_mutex_unlock(&g_repl_mutex);
        rc = build_tcp_full_sync_prefix(&prefix, &base);
        pthread_mutex_lock(&g_repl_mutex);

        if (s.fd < 0 || s.state == REPL_SLOT_FREE) {
            return -1;
        }
        if (rc != 0) {
            drop_slot_locked(slot, "build full sync snapshot failed");
            return -1;
        }
        // 注意：不能直接丢弃 s.out。发送线程可能正处在某条命令的帧中间，
        // 一旦截断，Replica 会卡在半个帧上，后续字节全部错位。
        // 这里只回收「已经发出去」的部分，再把新的全量同步前缀追加到队尾：
        // 对端会先看完旧流余下的完整帧，再收到 RESET 清库并加载新快照。
        if (s.out_sent > 0) {
            s.out.erase(0, s.out_sent);
            s.out_sent = 0;
        }
        s.out.append(prefix);
        s.next_seq = base;
        s.prefix_done = 0;
    } else {
        // 该分支只用于「刚登记握手、尚未投递任何字节」的槽位（RDMA 快照后补增量）。
        // 若 out 里仍有未发完的数据，说明调用时机不对，宁可断开让副本重连重同步，
        // 也不能截断半帧或跳过 base_seq 之后的增量。
        if (!s.out.empty()) {
            drop_slot_locked(slot, "unexpected pending data before rdma resume");
            return -1;
        }
        s.out.clear();
        s.out_sent = 0;
        s.next_seq = base_seq;
        s.prefix_done = 1;
    }

    s.fullsync_done_sent = 0;
    s.state = REPL_SLOT_SYNCING;
    s.last_progress_ms = repl_now_ms();
    kvs_log(KVS_LOG_INFO, "[REPLICATION] start full sync fd=%d base_seq=%llu (%s)", s.fd,
            (unsigned long long)s.next_seq, have_base_seq ? "rdma snapshot" : "tcp snapshot");
    pthread_cond_broadcast(&g_repl_cond);
    return 0;
}

void kvs_replication_finish_handshake(int fd) {
    if (g_role != KVS_ROLE_MASTER) {
        return;
    }

    pthread_mutex_lock(&g_repl_mutex);
    int slot = find_slot_locked(fd);
    if (slot < 0 || g_slots[slot].state != REPL_SLOT_PENDING) {
        pthread_mutex_unlock(&g_repl_mutex);
        return;
    }

    unsigned long long base_seq = 0;
    int have_base = 0;
#ifdef KVS_ENABLE_RDMA
    have_base = take_rdma_note_locked(g_slots[slot].peer_ip, &base_seq);
#endif

    if (start_full_sync_locked(slot, have_base, base_seq) != 0) {
        pthread_mutex_unlock(&g_repl_mutex);
        return;
    }
    pthread_mutex_unlock(&g_repl_mutex);
}

int kvs_replication_resync() {
    if (g_role != KVS_ROLE_MASTER) {
        return 0;
    }

    pthread_mutex_lock(&g_repl_mutex);
    kvs_log(KVS_LOG_INFO, "[REPLICATION] resync requested, restart full sync for active replicas");
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        repl_slot_t& s = g_slots[i];
        if (s.fd < 0 || s.state == REPL_SLOT_FREE || s.state == REPL_SLOT_PENDING) {
            continue;
        }
        s.state = REPL_SLOT_PENDING;
        s.prefix_done = 0;
        s.fullsync_done_sent = 0;
        start_full_sync_locked(i, 0, 0);
    }
    pthread_mutex_unlock(&g_repl_mutex);
    return 0;
}

// ==================== Master：增量投递 ====================

// 把一条写命令编码成 RESP 文本（增量日志里存的就是最终要投递给 Replica 的负载）。
static int encode_command(int argc, char* argv[], std::string* out) {
    if (argc <= 0 || argv == NULL || out == NULL) {
        return -1;
    }
    size_t size = 32;
    for (int i = 0; i < argc; ++i) {
        size += strlen(argv[i]) + 32;
    }

    out->resize(size);
    int offset = 0;
    offset += snprintf(&(*out)[0] + offset, size - (size_t)offset, "*%d\r\n", argc);
    for (int i = 0; i < argc; ++i) {
        offset += snprintf(&(*out)[0] + offset, size - (size_t)offset, "$%zu\r\n%s\r\n",
                           strlen(argv[i]), argv[i]);
    }
    if (offset <= 0) {
        return -1;
    }
    out->resize((size_t)offset);
    return 0;
}

// 追加到增量日志并唤醒发送线程。
static void backlog_push_and_wake(const std::string& payload) {
    pthread_mutex_lock(&g_repl_mutex);
    backlog_push_locked(payload);
    // 只有存在正在同步/在线的副本时才需要唤醒发送线程；
    // 没有副本时避免每条写命令都做一次无意义的 futex 唤醒。
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        if (g_slots[i].state >= REPL_SLOT_SYNCING) {
            pthread_cond_broadcast(&g_repl_cond);
            break;
        }
    }
    pthread_mutex_unlock(&g_repl_mutex);
}

// eBPF 采集通道的消费回调：记录里取出的参数在这里重新编码后落 backlog。
// 调用方是采集消费线程，不持有任何锁。
void kvs_replication_on_captured_command(int argc, char* argv[]) {
    if (g_role != KVS_ROLE_MASTER) {
        return;
    }
    std::string payload;
    if (encode_command(argc, argv, &payload) != 0) {
        kvs_log(KVS_LOG_ERROR, "[REPLICATION] encode captured command failed, force resync");
        kvs_replication_resync();
        return;
    }
    backlog_push_and_wake(payload);
}

int kvs_replication_append(int argc, char* argv[]) {
    if (g_role != KVS_ROLE_MASTER) {
        return 0;
    }
    if (argc <= 0 || argv == NULL) {
        return -1;
    }

#if KVS_ENABLE_EBPF_REALTIME
    if (kvs_ebpf_realtime_active()) {
        if (kvs_ebpf_event_fits(argc, argv)) {
            // 交给 eBPF 采集通道：uprobe 把参数写进 ringbuf，消费线程稍后
            // 调用 kvs_replication_on_captured_command() 落 backlog。
            // 序号在存储锁内递增，消费线程据此发现丢失的事件。
            static unsigned int event_seq = 0;
            ++event_seq;
            kvs_ebpf_notify_write(event_seq, argc, argv);
            return 0;
        }
        // 参数超过采集长度上限：直接落 backlog 之前先等采集通道排空，
        // 否则本条命令会插到「已经通知但还没落 backlog」的前序命令前面。
        kvs_ebpf_wait_drained(KVS_REPL_EBPF_DRAIN_TIMEOUT_MS);
    }
#endif

    std::string payload;
    if (encode_command(argc, argv, &payload) != 0) {
        return -1;
    }
    backlog_push_and_wake(payload);
    return 0;
}

// 把 backlog 中 seq 开始的记录拼进 s.out，最多 REPL_SEND_CHUNK 字节。
// 返回 1 表示副本落后超出日志范围，需要重新全量同步。
static int refill_slot_locked(int slot) {
    repl_slot_t& s = g_slots[slot];
    size_t budget = REPL_SEND_CHUNK;
    unsigned long long tail = backlog_tail_locked();

    if (s.next_seq < tail) {
        return 1;
    }

    while (budget > 0 && s.next_seq < g_backlog_head) {
        const std::string& rec = backlog_at_locked(s.next_seq);
        size_t framed = rec.size() + 5;
        if (framed > REPL_SEND_CHUNK && !s.out.empty()) {
            break;
        }
        append_frame_header(&s.out, KVS_REPL_FRAME_COMMAND, rec.size());
        s.out.append(rec);
        s.next_seq++;
        budget = (framed >= budget) ? 0 : budget - framed;
    }
    return 0;
}

// 返回：0 = 本轮无待发数据，1 = 还有数据待发，2 = 需要重新全量同步。
static int flush_slot_locked(int slot) {
    repl_slot_t& s = g_slots[slot];
    if (s.fd < 0 || s.state < REPL_SLOT_SYNCING) {
        return 0;
    }

    unsigned long long now = repl_now_ms();
    int had_work = (s.out_sent < s.out.size()) || (s.next_seq < g_backlog_head);
    if (had_work && now - s.last_progress_ms > REPL_STALL_TIMEOUT_MS) {
        // 长时间没有任何进展：多半是对端不读数据，断开并交由网络层回收。
        drop_slot_locked(slot, "replication send stalled");
        return 0;
    }

    for (;;) {
        if (s.out_sent < s.out.size()) {
            ssize_t n = send(s.fd, s.out.data() + s.out_sent, s.out.size() - s.out_sent,
                             MSG_DONTWAIT | MSG_NOSIGNAL);
            if (n > 0) {
                s.out_sent += (size_t)n;
                s.last_progress_ms = now;
                if (s.out_sent == s.out.size()) {
                    s.out.clear();
                    s.out_sent = 0;
                    if (!s.prefix_done) {
                        s.prefix_done = 1;
                        kvs_log(KVS_LOG_INFO, "[REPLICATION] full snapshot sent fd=%d", s.fd);
                    }
                }
                continue;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                return 1;
            }
            drop_slot_locked(slot, "replication send failed");
            return 0;
        }

        // 全量快照发完、且快照之后落地的增量也补齐后，下发 FULLSYNC_DONE 标记。
        if (s.prefix_done && !s.fullsync_done_sent && s.next_seq == g_backlog_head) {
            append_frame_header(&s.out, KVS_REPL_FRAME_FULLSYNC_DONE, 0);
            s.fullsync_done_sent = 1;
            continue;
        }

        if (refill_slot_locked(slot) != 0) {
            return 2;
        }
        if (s.out.empty()) {
            s.last_progress_ms = now;
            if (s.state == REPL_SLOT_SYNCING && s.fullsync_done_sent) {
                s.state = REPL_SLOT_ONLINE;
            }
            return 0;
        }
    }
}

static void* repl_sender_thread(void*) {
    while (g_sender_running) {
        struct pollfd pfds[MAX_REPLICAS];
        int nfds = 0;
        int need_resync[MAX_REPLICAS];
        int resync_count = 0;
        int pending = 0;

        pthread_mutex_lock(&g_repl_mutex);
        for (int i = 0; i < MAX_REPLICAS; ++i) {
            repl_slot_t& s = g_slots[i];
            if (s.fd < 0 || s.state < REPL_SLOT_SYNCING) {
                continue;
            }
            int rc = flush_slot_locked(i);
            if (rc == 2) {
                need_resync[resync_count++] = i;
            } else if (rc == 1) {
                pending = 1;
                pfds[nfds].fd = s.fd;
                pfds[nfds].events = POLLOUT;
                pfds[nfds].revents = 0;
                ++nfds;
            }
        }

        if (!pending && resync_count == 0) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 50 * 1000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec += 1;
                ts.tv_nsec -= 1000000000L;
            }
            pthread_cond_timedwait(&g_repl_cond, &g_repl_mutex, &ts);
        }
        pthread_mutex_unlock(&g_repl_mutex);

        for (int k = 0; k < resync_count; ++k) {
            // 重新全量同步需要先拿存储锁再拿复制锁，必须脱离复制锁后再启动。
            int slot = need_resync[k];
            pthread_mutex_lock(&g_repl_mutex);
            int valid = (g_slots[slot].fd >= 0 && g_slots[slot].state >= REPL_SLOT_SYNCING);
            pthread_mutex_unlock(&g_repl_mutex);
            if (!valid) {
                continue;
            }
            pthread_mutex_lock(&g_repl_mutex);
            drop_slot_locked(slot, "backlog overrun, force reconnect");
            pthread_mutex_unlock(&g_repl_mutex);
        }

        if (pending) {
            poll(pfds, (nfds_t)nfds, 20);
        }
    }
    return NULL;
}

// ==================== Replica：帧处理 ====================

int kvs_replication_is_replaying() { return replication_replaying; }

void kvs_replication_set_replaying(int value) { replication_replaying = value ? 1 : 0; }

void kvs_replication_set_master_addr(const char* ip, int port) {
    if (ip == NULL || ip[0] == '\0' || port <= 0 || port > 65535) {
        return;
    }
    // ip 可能正好指向 g_master_ip（监督线程传的就是它），必须避免自我拷贝。
    if (ip != g_master_ip) {
        snprintf(g_master_ip, sizeof(g_master_ip), "%s", ip);
    }
    g_master_port = port;
}

// 非阻塞 connect + 超时，避免 Master 不可达时线程长时间卡死。
int kvs_replication_connect_master(const char* ip, int port) {
    if (ip == NULL || strlen(ip) >= INET_ADDRSTRLEN || port <= 0 || port > 65535) {
        return -1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    int rc = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (rc != 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }

    if (rc != 0) {
        struct pollfd pfd;
        pfd.fd = fd;
        pfd.events = POLLOUT;
        pfd.revents = 0;
        if (poll(&pfd, 1, 1000) <= 0) {
            close(fd);
            return -1;
        }
        int so_error = 0;
        socklen_t len = sizeof(so_error);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) != 0 || so_error != 0) {
            close(fd);
            return -1;
        }
    }

    // 恢复阻塞语义：回放循环按「阻塞读」实现，收到 shutdown 时 recv 会返回 0。
    if (flags >= 0) {
        fcntl(fd, F_SETFL, flags);
    }

    // 监督线程传入的 ip 就是 g_master_ip 本身，这里不能再对自己 snprintf，
    // 否则会把自己的地址清空，后续 RDMA/eBPF 路径都会拿到空地址。
    if (ip != g_master_ip) {
        snprintf(g_master_ip, sizeof(g_master_ip), "%s", ip);
    }
    g_master_port = port;
    kvs_log(KVS_LOG_INFO, "[REPLICATION] connected to master %s:%d", ip, port);
    return fd;
}

// 回放一条 Master 下发的写命令（回放期间不重复记录 AOF、不再向外转发）。
static int apply_replication_command(const char* body, size_t len) {
    if (len == 0) {
        return 0;
    }
    char response[256] = {0};
    kvs_replication_set_replaying(1);
    kvs_protocol(const_cast<char*>(body), (int)len, response, sizeof(response));
    kvs_replication_set_replaying(0);
    return 0;
}

// 处理一个复制帧。返回 0 正常，-1 需要断开重连。
static int handle_replication_frame(unsigned char type, const char* body, size_t len,
                                    std::string* snapshot_buf) {
    switch (type) {
    case KVS_REPL_FRAME_COMMAND:
        return apply_replication_command(body, len);

    case KVS_REPL_FRAME_RESET:
        // 清空本地数据，等待后续全量快照；同时丢弃可能残留的半份快照。
        snapshot_buf->clear();
        kvs_data_lock();
        kvs_reset_data();
        kvs_data_unlock();
        kvs_log(KVS_LOG_INFO, "[REPLICATION] replica reset, waiting for full snapshot");
        return 0;

    case KVS_REPL_FRAME_SNAPSHOT:
        if (snapshot_buf->size() + len > (size_t)UINT32_MAX) {
            kvs_log(KVS_LOG_ERROR, "[REPLICATION] snapshot too large, abort sync");
            snapshot_buf->clear();
            return -1;
        }
        snapshot_buf->append(body, len);
        return 0;

    case KVS_REPL_FRAME_SNAPSHOT_END: {
        int rc = -1;
        size_t snapshot_len = snapshot_buf->size();
        if (snapshot_len > 0) {
            // kvs_snapshot_load_buffer 内部会先 kvs_reset_data()，因此这一步
            // 已经包含了「清空旧数据」，RESET 帧只是提前释放内存。
            kvs_data_lock();
            rc = kvs_snapshot_load_buffer(snapshot_buf->data(), snapshot_len);
            kvs_data_unlock();
        }
        kvs_log(rc == 0 ? KVS_LOG_INFO : KVS_LOG_ERROR,
                "[REPLICATION] full snapshot loaded (%zu bytes), rc=%d", snapshot_len, rc);
        snapshot_buf->clear();
        return 0;
    }

    case KVS_REPL_FRAME_FULLSYNC_DONE:
        // 快照与快照之后的增量都已到达，后续只会有常规增量帧。
        kvs_log(KVS_LOG_INFO, "[REPLICATION] full sync completed, now replaying increment only");
        return 0;

    default:
        // 握手的 "+OK" 响应帧没有类型字节，首字节是 '+'，在这里被安全忽略。
        return 0;
    }
}

// 一次会话：持续读取类型化复制帧并应用，直到连接断开或主动停止。
// 实时增量统一从 TCP 复制帧（KVS_REPL_FRAME_COMMAND）读取并回放；
// Master 侧的 eBPF 采集只影响命令如何进入 backlog，不影响本函数。
static void replica_session(int fd) {
    std::string snapshot_buf;
    char* body = NULL;
    size_t body_cap = 0;

    if (send_raw_frame(fd, replication_handshake, strlen(replication_handshake)) != 0) {
        kvs_log(KVS_LOG_WARN, "[REPLICATION] send handshake failed");
        return;
    }

    while (replica_running) {
        uint32_t net_len = 0;
        if (recv_all(fd, (char*)&net_len, sizeof(net_len)) < 0) {
            break;
        }
        uint32_t frame_len = ntohl(net_len);
        if (frame_len == 0 || frame_len > REPL_MAX_FRAME) {
            kvs_log(KVS_LOG_WARN, "[REPLICATION] invalid replication frame len=%u", frame_len);
            break;
        }

        if (frame_len > body_cap) {
            char* new_body = (char*)realloc(body, frame_len);
            if (new_body == NULL) {
                break;
            }
            body = new_body;
            body_cap = frame_len;
        }
        if (recv_all(fd, body, frame_len) < 0) {
            break;
        }

        unsigned char type = (unsigned char)body[0];
        if (handle_replication_frame(type, body + 1, frame_len - 1, &snapshot_buf) != 0) {
            break;
        }
    }

    free(body);
    snapshot_buf.clear();
}

static void* replica_supervisor_thread(void*) {
    int backoff_ms = 200;

    while (replica_running) {
        const char* ip = g_master_ip;
        int port = g_master_port;

        int fd = kvs_replication_connect_master(ip, port);
        if (fd < 0) {
            kvs_log(KVS_LOG_WARN, "[REPLICATION] connect master %s:%d failed, retry in %dms", ip,
                    port, backoff_ms);
            // 用可打断的退避：停止时不必等整个退避周期
            repl_sleep_ms_interruptible(backoff_ms, &replica_running);
            backoff_ms = backoff_ms < 2500 ? backoff_ms * 2 : 5000;
            continue;
        }

        master_fd = fd;
        backoff_ms = 200;

#ifdef KVS_ENABLE_RDMA
        // 优先用 RDMA 拉一次全量快照；Master 会按是否收到 RDMA 快照记录，
        // 决定是「只补增量」还是「发完整 TCP 快照」。
        if (kvs_replication_rdma_full_sync(ip, port) != 0) {
            kvs_log(KVS_LOG_INFO, "[REPLICATION] RDMA full sync unavailable, use TCP snapshot");
        }
#endif

        // 握手与后续增量回放都在会话内部完成。
        replica_session(fd);

        shutdown(fd, SHUT_RDWR);
        close(fd);
        master_fd = -1;

        if (replica_running) {
            kvs_log(KVS_LOG_WARN, "[REPLICATION] master connection lost, reconnect in %dms",
                    backoff_ms);
            repl_sleep_ms_interruptible(backoff_ms, &replica_running);
            backoff_ms = backoff_ms < 2500 ? backoff_ms * 2 : 5000;
        }
    }

    return NULL;
}

int kvs_replication_start() {
    if (g_role != KVS_ROLE_REPLICA || replica_running) {
        return -1;
    }
    if (g_master_ip[0] == '\0' || g_master_port <= 0) {
        kvs_log(KVS_LOG_ERROR, "[REPLICATION] master address is not configured");
        return -1;
    }

    replica_running = 1;
    if (pthread_create(&g_replica_tid, NULL, replica_supervisor_thread, NULL) != 0) {
        kvs_log(KVS_LOG_ERROR, "[REPLICATION] failed to create replication thread");
        replica_running = 0;
        return -1;
    }
    return 0;
}

void kvs_replication_stop() {
    if (!replica_running) {
        return;
    }
    replica_running = 0;
    if (master_fd >= 0) {
        shutdown(master_fd, SHUT_RDWR);
    }
    pthread_join(g_replica_tid, NULL);
    if (master_fd >= 0) {
        close(master_fd);
        master_fd = -1;
    }
    kvs_log(KVS_LOG_INFO, "[REPLICATION] replica sync stopped");
}

void kvs_replication_destroy() {
    if (g_role == KVS_ROLE_REPLICA) {
        kvs_replication_stop();
    }

    pthread_mutex_lock(&g_repl_mutex);
    if (g_sender_running) {
        g_sender_running = 0;
        pthread_cond_broadcast(&g_repl_cond);
        pthread_mutex_unlock(&g_repl_mutex);
        pthread_join(g_sender_tid, NULL);
        pthread_mutex_lock(&g_repl_mutex);
    }
    for (int i = 0; i < MAX_REPLICAS; ++i) {
        g_slots[i].out.clear();
        g_slots[i].fd = -1;
        g_slots[i].state = REPL_SLOT_FREE;
    }
    g_backlog.clear();
    g_backlog_bytes = 0;
    pthread_mutex_unlock(&g_repl_mutex);

#if KVS_ENABLE_EBPF_REALTIME
    // 先停采集线程，再拆 RDMA：采集消费线程会调用 kvs_replication_resync()，
    // 必须确保它已经退出。
    kvs_ebpf_realtime_stop();
#endif

#ifdef KVS_ENABLE_RDMA
    if (g_role == KVS_ROLE_MASTER) {
        kvs_replication_stop_rdma_listener();
    }
#endif
}

int kvs_replication_init(kvs_role_t role) {
    g_role = role;

    // 默认从配置文件取主从地址/端口，main 解析完命令行后会再覆盖一次。
    if (role == KVS_ROLE_REPLICA) {
        const char* cfg_ip = kvs_config_master_ip();
        if (cfg_ip != NULL && cfg_ip[0] != '\0') {
            snprintf(g_master_ip, sizeof(g_master_ip), "%s", cfg_ip);
        }
        g_master_port = kvs_config_master_port();
    }

    for (int i = 0; i < MAX_REPLICAS; ++i) {
        g_slots[i].fd = -1;
        g_slots[i].state = REPL_SLOT_FREE;
        g_slots[i].next_seq = 0;
        g_slots[i].out.clear();
        g_slots[i].out_sent = 0;
        g_slots[i].prefix_done = 0;
        g_slots[i].fullsync_done_sent = 0;
        g_slots[i].peer_ip[0] = '\0';
        g_rdma_notes[i].used = 0;
        g_rdma_notes[i].peer_ip[0] = '\0';
        g_rdma_notes[i].seq = 0;
        g_rdma_notes[i].ts_ms = 0;
    }
    g_backlog.clear();
    g_backlog_bytes = 0;
    g_backlog_head = 0;

    if (role == KVS_ROLE_MASTER && !g_sender_running) {
        g_sender_running = 1;
        if (pthread_create(&g_sender_tid, NULL, repl_sender_thread, NULL) != 0) {
            kvs_log(KVS_LOG_ERROR, "[REPLICATION] failed to create sender thread");
            g_sender_running = 0;
            return -1;
        }
    }

#if KVS_ENABLE_EBPF_REALTIME
    // Master 侧：尝试用 uprobe/kprobe + ringbuf 采集写命令。失败只影响「命令怎么
    // 进 backlog」，不影响复制正确性——写路径会自动改走直接落 backlog。
    if (role == KVS_ROLE_MASTER) {
        if (kvs_ebpf_realtime_start() == 0) {
            kvs_log(KVS_LOG_INFO, "[REPLICATION] realtime increments captured by eBPF ringbuf");
        } else {
            kvs_log(KVS_LOG_WARN,
                    "[REPLICATION] eBPF capture unavailable, write path pushes backlog directly");
        }
    }
#endif
    return 0;
}
