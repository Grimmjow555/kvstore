// RDMA 全量快照搬运的独立进程实现（重设计阶段 2）。
//
// 本文件被编译进 rdma-client 与 rdma-server 两个可执行文件，不进入 kvstore
// 进程；协议与约束见 include/kvs_rdma_transfer.h 顶部注释与
// docs/replication-redesign.md §8.3 / §8.4。
#include "kvs_rdma_transfer.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

// ==================== 控制消息 ====================

// 固定 32 字节布局，双方按同一结构收发。
struct kvs_rdma_xfer_msg {
    uint32_t type;
    uint32_t status;
    uint64_t value; // 快照长度 / ACK 状态承载
    uint64_t addr;  // 对端接收缓冲地址
    uint32_t rkey;
    uint32_t reserved;
};

enum {
    KVS_RDMA_XFER_MSG_SNAPSHOT_LEN = 1,
    KVS_RDMA_XFER_MSG_SNAPSHOT_BUF = 2,
    KVS_RDMA_XFER_MSG_SNAPSHOT_ACK = 3,
    KVS_RDMA_XFER_MSG_SNAPSHOT_DONE = 4,
};

enum {
    // rdma-client（发送方）
    KVS_RDMA_XFER_WR_RECV_BUF = 2000,
    KVS_RDMA_XFER_WR_RECV_ACK = 2001,
    KVS_RDMA_XFER_WR_SEND_LEN = 3000,
    KVS_RDMA_XFER_WR_WRITE = 3001,
    KVS_RDMA_XFER_WR_SEND_DONE = 3002,
    // rdma-server（接收方）
    KVS_RDMA_XFER_WR_RECV_LEN = 4000,
    KVS_RDMA_XFER_WR_RECV_DONE = 4001,
    KVS_RDMA_XFER_WR_SEND_BUF = 5000,
    KVS_RDMA_XFER_WR_SEND_ACK = 5001,
};

// ==================== 日志 ====================
// 两个工具进程各自独立运行，父进程会重定向它们的 stderr 到日志文件，
// 因此这里只做最简的输出，不依赖 kvstore 的配置/日志模块。

static void xfer_log(const char* fmt, ...) {
    char ts[32];
    time_t now = time(NULL);
    struct tm tm_value;
    memset(&tm_value, 0, sizeof(tm_value));
    localtime_r(&now, &tm_value);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_value);

    fprintf(stderr, "[%s][RDMA-XFER] ", ts);
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
    fflush(stderr);
}

// ==================== 文件读写 ====================

static int xfer_read_file(const char* path, char** out_data, size_t* out_len) {
    *out_data = NULL;
    *out_len = 0;

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        xfer_log("open %s failed: %s", path, strerror(errno));
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size <= 0) {
        xfer_log("stat %s failed or empty", path);
        close(fd);
        return -1;
    }

    size_t len = (size_t)st.st_size;
    char* buf = (char*)malloc(len);
    if (buf == NULL) {
        close(fd);
        return -1;
    }

    size_t got = 0;
    while (got < len) {
        ssize_t n = read(fd, buf + got, len - got);
        if (n > 0) {
            got += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        xfer_log("read %s failed at %zu/%zu", path, got, len);
        free(buf);
        close(fd);
        return -1;
    }
    close(fd);

    *out_data = buf;
    *out_len = len;
    return 0;
}

// 原子落盘：先写 <path>.partial 并 fsync，再 rename 到 <path>。
// 父进程以「子进程退出」作为文件就绪信号，rename 保证即使父进程提前观察，
// 也只会看到完整文件。
//
// 说明：Master 侧写 .tmp 有一份等价实现（replication/kvs_sync.cpp 的
// sync_write_tmp_atomic）。两者刻意没有合并：本进程是独立的 rdma-* 可执行文件，
// 只为一次落盘引入 liburing 不划算，因此用裸 write + fsync；Master 侧走 io_uring
// 以与 RDB SAVE 的写盘方式一致。改动其一时请同步核对另一处。
static int xfer_write_file_atomic(const char* path, const void* data, size_t len) {
    char partial[512];
    int n = snprintf(partial, sizeof(partial), "%s.partial", path);
    if (n <= 0 || (size_t)n >= sizeof(partial)) {
        return -1;
    }

    int fd = open(partial, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        xfer_log("open %s failed: %s", partial, strerror(errno));
        return -1;
    }

    size_t put = 0;
    while (put < len) {
        ssize_t w = write(fd, (const char*)data + put, len - put);
        if (w > 0) {
            put += (size_t)w;
            continue;
        }
        if (w < 0 && errno == EINTR) {
            continue;
        }
        xfer_log("write %s failed at %zu/%zu: %s", partial, put, len, strerror(errno));
        close(fd);
        unlink(partial);
        return -1;
    }

    if (fsync(fd) != 0) {
        xfer_log("fsync %s failed: %s", partial, strerror(errno));
        close(fd);
        unlink(partial);
        return -1;
    }
    close(fd);

    if (rename(partial, path) != 0) {
        xfer_log("rename %s -> %s failed: %s", partial, path, strerror(errno));
        unlink(partial);
        return -1;
    }
    return 0;
}

// ==================== RDMA 原语 ====================

static int xfer_wait_cm_event(struct rdma_event_channel* ec, enum rdma_cm_event_type type,
                              struct rdma_cm_event** out_event) {
    if (ec == NULL || out_event == NULL) {
        return -1;
    }

    while (1) {
        struct rdma_cm_event* event = NULL;
        if (rdma_get_cm_event(ec, &event) != 0) {
            return -1;
        }

        if (event->event == type) {
            *out_event = event;
            return 0;
        }

        if (event->event == RDMA_CM_EVENT_REJECTED ||
            event->event == RDMA_CM_EVENT_DISCONNECTED ||
            event->event == RDMA_CM_EVENT_DEVICE_REMOVAL) {
            rdma_ack_cm_event(event);
            return -1;
        }

        rdma_ack_cm_event(event);
    }
}

static int xfer_create_qp(struct rdma_cm_id* id, struct ibv_pd** out_pd, struct ibv_cq** out_cq) {
    if (id == NULL || out_pd == NULL || out_cq == NULL) {
        return -1;
    }

    *out_pd = NULL;
    *out_cq = NULL;

    struct ibv_pd* pd = ibv_alloc_pd(id->verbs);
    if (pd == NULL) {
        xfer_log("ibv_alloc_pd failed: %s", strerror(errno));
        return -1;
    }

    struct ibv_cq* cq = ibv_create_cq(id->verbs, 128, NULL, NULL, 0);
    if (cq == NULL) {
        xfer_log("ibv_create_cq failed: %s", strerror(errno));
        ibv_dealloc_pd(pd);
        return -1;
    }

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 128;
    qp_attr.cap.max_recv_wr = 128;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.sq_sig_all = 0;

    if (rdma_create_qp(id, pd, &qp_attr) != 0) {
        xfer_log("rdma_create_qp failed: %s", strerror(errno));
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        return -1;
    }

    *out_pd = pd;
    *out_cq = cq;
    return 0;
}

struct xfer_pending_wc {
    struct ibv_wc entries[8];
    int count;
};

static int xfer_poll_cq_state(struct ibv_cq* cq, uint64_t wr_id, struct ibv_wc* out_wc,
                              struct xfer_pending_wc* pending) {
    if (cq == NULL || out_wc == NULL) {
        return -1;
    }

    while (1) {
        for (int i = 0; i < pending->count; ++i) {
            if (pending->entries[i].wr_id == wr_id) {
                *out_wc = pending->entries[i];
                pending->entries[i] = pending->entries[pending->count - 1];
                pending->count--;
                return out_wc->status == IBV_WC_SUCCESS ? 0 : -1;
            }
        }

        int n = ibv_poll_cq(cq, 1, out_wc);
        if (n < 0) {
            return -1;
        }
        if (n == 0) {
            usleep(100);
            continue;
        }
        if (out_wc->wr_id == wr_id) {
            return out_wc->status == IBV_WC_SUCCESS ? 0 : -1;
        }

        // 缓存非目标完成事件，后续等待对应 wr_id 时再取出。
        if (pending->count < (int)(sizeof(pending->entries) / sizeof(pending->entries[0]))) {
            pending->entries[pending->count++] = *out_wc;
            continue;
        }
        return -1;
    }
}

static int xfer_post_send(struct ibv_qp* qp, struct ibv_mr* mr, void* buf, size_t len,
                          uint64_t wr_id) {
    if (qp == NULL || mr == NULL || buf == NULL || len > UINT32_MAX) {
        return -1;
    }

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)len;
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.opcode = IBV_WR_SEND;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr* bad_wr = NULL;
    int rc = ibv_post_send(qp, &wr, &bad_wr);
    if (rc != 0) {
        // ibv_post_send 的返回值即错误码（不走 errno）。
        xfer_log("ibv_post_send(SEND) failed: rc=%d", rc);
    }
    return rc;
}

static int xfer_post_recv(struct ibv_qp* qp, struct ibv_mr* mr, void* buf, size_t len,
                          uint64_t wr_id) {
    if (qp == NULL || mr == NULL || buf == NULL || len > UINT32_MAX) {
        return -1;
    }

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)len;
    sge.lkey = mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.num_sge = 1;
    wr.sg_list = &sge;

    struct ibv_recv_wr* bad_wr = NULL;
    int rc = ibv_post_recv(qp, &wr, &bad_wr);
    if (rc != 0) {
        xfer_log("ibv_post_recv failed: rc=%d", rc);
    }
    return rc;
}

// 普通 RDMA 写（不带立即数）。完成通知由调用方另行发一条 SEND。
static int xfer_post_write(struct ibv_qp* qp, struct ibv_mr* local_mr, void* local_buf, size_t len,
                           uint64_t remote_addr, uint32_t remote_rkey, uint64_t wr_id) {
    if (qp == NULL || local_mr == NULL || local_buf == NULL || len > UINT32_MAX) {
        return -1;
    }

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_buf;
    sge.length = (uint32_t)len;
    sge.lkey = local_mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = remote_rkey;

    struct ibv_send_wr* bad_wr = NULL;
    int rc = ibv_post_send(qp, &wr, &bad_wr);
    if (rc != 0) {
        xfer_log("ibv_post_send(RDMA_WRITE) failed: rc=%d", rc);
    }
    return rc;
}

static void xfer_fill_conn_param(struct rdma_conn_param* conn_param) {
    memset(conn_param, 0, sizeof(*conn_param));
    conn_param->responder_resources = 1;
    conn_param->initiator_depth = 1;
    conn_param->retry_count = 7;
    conn_param->rnr_retry_count = 7;
}

// ==================== rdma-client：发送端 ====================

static int xfer_send_over_qp(struct rdma_cm_id* id, struct ibv_pd* pd, struct ibv_cq* cq,
                             char* snapshot, size_t snapshot_len) {
    struct ibv_mr* snapshot_mr = NULL;
    char* ctrl = NULL;
    struct ibv_mr* ctrl_mr = NULL;
    struct kvs_rdma_xfer_msg* len_msg = NULL;
    struct kvs_rdma_xfer_msg* buf_msg = NULL;
    struct kvs_rdma_xfer_msg* ack_msg = NULL;
    struct kvs_rdma_xfer_msg* done_msg = NULL;
    struct ibv_wc wc;
    struct xfer_pending_wc pending;
    memset(&pending, 0, sizeof(pending));
    int ret = -1;

    const size_t msg_size = sizeof(struct kvs_rdma_xfer_msg);
    const size_t ctrl_len = msg_size * 4;

    snapshot_mr = ibv_reg_mr(pd, snapshot, snapshot_len,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ);
    if (snapshot_mr == NULL) {
        xfer_log("reg snapshot MR failed");
        goto cleanup;
    }

    ctrl = (char*)calloc(1, ctrl_len);
    if (ctrl == NULL) {
        goto cleanup;
    }
    ctrl_mr = ibv_reg_mr(pd, ctrl, ctrl_len,
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ);
    if (ctrl_mr == NULL) {
        xfer_log("reg ctrl MR failed");
        goto cleanup;
    }

    // 接收对端回填的「接收缓冲地址」与最终 ACK。
    if (xfer_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 0, msg_size,
                       KVS_RDMA_XFER_WR_RECV_BUF) != 0 ||
        xfer_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 1, msg_size,
                       KVS_RDMA_XFER_WR_RECV_ACK) != 0) {
        goto cleanup;
    }

    // 先告诉对端快照长度，对端据此分配接收缓冲。
    len_msg = (struct kvs_rdma_xfer_msg*)(ctrl + msg_size * 2);
    len_msg->type = KVS_RDMA_XFER_MSG_SNAPSHOT_LEN;
    len_msg->value = (uint64_t)snapshot_len;
    if (xfer_post_send(id->qp, ctrl_mr, len_msg, msg_size, KVS_RDMA_XFER_WR_SEND_LEN) != 0) {
        goto cleanup;
    }
    if (xfer_poll_cq_state(cq, KVS_RDMA_XFER_WR_SEND_LEN, &wc, &pending) != 0 ||
        xfer_poll_cq_state(cq, KVS_RDMA_XFER_WR_RECV_BUF, &wc, &pending) != 0) {
        goto cleanup;
    }

    buf_msg = (struct kvs_rdma_xfer_msg*)(ctrl + msg_size * 0);
    if (buf_msg->type != KVS_RDMA_XFER_MSG_SNAPSHOT_BUF || buf_msg->rkey == 0 ||
        buf_msg->addr == 0) {
        xfer_log("received invalid remote buffer descriptor");
        goto cleanup;
    }

    // 真正的大块搬运走 RDMA_WRITE。
    if (xfer_post_write(id->qp, snapshot_mr, snapshot, snapshot_len, buf_msg->addr,
                        buf_msg->rkey, KVS_RDMA_XFER_WR_WRITE) != 0) {
        goto cleanup;
    }
    if (xfer_poll_cq_state(cq, KVS_RDMA_XFER_WR_WRITE, &wc, &pending) != 0) {
        goto cleanup;
    }

    // 写完成后用一条普通 SEND 通知对端「数据已写完」。
    done_msg = (struct kvs_rdma_xfer_msg*)(ctrl + msg_size * 3);
    memset(done_msg, 0, msg_size);
    done_msg->type = KVS_RDMA_XFER_MSG_SNAPSHOT_DONE;
    done_msg->value = (uint64_t)snapshot_len;
    if (xfer_post_send(id->qp, ctrl_mr, done_msg, msg_size, KVS_RDMA_XFER_WR_SEND_DONE) != 0) {
        goto cleanup;
    }
    if (xfer_poll_cq_state(cq, KVS_RDMA_XFER_WR_SEND_DONE, &wc, &pending) != 0 ||
        xfer_poll_cq_state(cq, KVS_RDMA_XFER_WR_RECV_ACK, &wc, &pending) != 0) {
        goto cleanup;
    }

    ack_msg = (struct kvs_rdma_xfer_msg*)(ctrl + msg_size * 1);
    if (ack_msg->type != KVS_RDMA_XFER_MSG_SNAPSHOT_ACK || ack_msg->status != 0) {
        xfer_log("peer reported failure (status=%u)", ack_msg->status);
        goto cleanup;
    }

    ret = 0;

cleanup:
    if (ctrl_mr != NULL) {
        ibv_dereg_mr(ctrl_mr);
    }
    free(ctrl);
    if (snapshot_mr != NULL) {
        ibv_dereg_mr(snapshot_mr);
    }
    return ret;
}

int kvs_rdma_transfer_send_file(const char* file, const char* peer_ip, unsigned short rdma_port,
                                int timeout_ms) {
    struct rdma_event_channel* ec = NULL;
    struct rdma_cm_id* id = NULL;
    struct ibv_pd* pd = NULL;
    struct ibv_cq* cq = NULL;
    struct rdma_cm_event* event = NULL;
    char* snapshot = NULL;
    size_t snapshot_len = 0;
    const char* step = "参数校验";
    int ret = -1;

    if (file == NULL || peer_ip == NULL || peer_ip[0] == '\0' || rdma_port == 0) {
        xfer_log("invalid arguments");
        return -1;
    }

    if (timeout_ms > 0) {
        // 整体超时兜底：任何一步卡住都由 SIGALRM 默认动作终止进程，
        // 避免残留子进程（父进程只做有限等待，不会替它收尾）。
        alarm((unsigned int)((timeout_ms + 999) / 1000));
    }

    step = "读取快照文件";
    if (xfer_read_file(file, &snapshot, &snapshot_len) != 0) {
        return -1;
    }
    if (snapshot_len > UINT32_MAX) {
        xfer_log("snapshot too large: %zu", snapshot_len);
        free(snapshot);
        return -1;
    }
    xfer_log("sending %s (%zu bytes) to %s:%u", file, snapshot_len, peer_ip, rdma_port);

    step = "rdma_create_event_channel";
    ec = rdma_create_event_channel();
    if (ec == NULL) {
        goto cleanup;
    }

    step = "rdma_create_id";
    if (rdma_create_id(ec, &id, NULL, RDMA_PS_TCP) != 0) {
        goto cleanup;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(rdma_port);
    step = "inet_pton";
    if (inet_pton(AF_INET, peer_ip, &addr.sin_addr) <= 0) {
        goto cleanup;
    }

    step = "rdma_resolve_addr";
    if (rdma_resolve_addr(id, NULL, (struct sockaddr*)&addr, 2000) != 0) {
        goto cleanup;
    }
    step = "等待 ADDR_RESOLVED";
    if (xfer_wait_cm_event(ec, RDMA_CM_EVENT_ADDR_RESOLVED, &event) != 0) {
        goto cleanup;
    }
    rdma_ack_cm_event(event);
    event = NULL;

    step = "rdma_resolve_route";
    if (rdma_resolve_route(id, 2000) != 0) {
        goto cleanup;
    }
    step = "等待 ROUTE_RESOLVED";
    if (xfer_wait_cm_event(ec, RDMA_CM_EVENT_ROUTE_RESOLVED, &event) != 0) {
        goto cleanup;
    }
    rdma_ack_cm_event(event);
    event = NULL;

    step = "创建 QP";
    if (xfer_create_qp(id, &pd, &cq) != 0) {
        goto cleanup;
    }

    struct rdma_conn_param conn_param;
    xfer_fill_conn_param(&conn_param);

    step = "rdma_connect";
    if (rdma_connect(id, &conn_param) != 0) {
        goto cleanup;
    }
    step = "等待 ESTABLISHED";
    if (xfer_wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED, &event) != 0) {
        goto cleanup;
    }
    rdma_ack_cm_event(event);
    event = NULL;

    step = "RDMA 发送快照";
    ret = xfer_send_over_qp(id, pd, cq, snapshot, snapshot_len);
    if (ret != 0) {
        goto cleanup;
    }
    xfer_log("snapshot sent, waiting for peer ack done");
    ret = 0;

cleanup:
    if (ret != 0) {
        int saved_errno = errno;
        xfer_log("send failed at step [%s]: errno=%d(%s)", step, saved_errno,
                 strerror(saved_errno));
    }
    if (id != NULL && id->qp != NULL) {
        rdma_destroy_qp(id);
    }
    if (cq != NULL) {
        ibv_destroy_cq(cq);
    }
    if (pd != NULL) {
        ibv_dealloc_pd(pd);
    }
    if (id != NULL) {
        rdma_destroy_id(id);
    }
    if (ec != NULL) {
        rdma_destroy_event_channel(ec);
    }
    free(snapshot);
    return ret;
}

// ==================== rdma-server：接收端 ====================

static int xfer_recv_over_qp(struct rdma_cm_id* id, struct ibv_pd* pd, struct ibv_cq* cq,
                             const char* out_file) {
    char* ctrl = NULL;
    struct ibv_mr* ctrl_mr = NULL;
    char* snapshot = NULL;
    struct ibv_mr* snapshot_mr = NULL;
    size_t snapshot_len = 0;
    struct kvs_rdma_xfer_msg* len_msg = NULL;
    struct kvs_rdma_xfer_msg* buf_msg = NULL;
    struct kvs_rdma_xfer_msg* ack_msg = NULL;
    struct kvs_rdma_xfer_msg* done_msg = NULL;
    struct ibv_wc wc;
    struct xfer_pending_wc pending;
    memset(&pending, 0, sizeof(pending));
    int write_rc = -1;
    int ret = -1;

    const size_t msg_size = sizeof(struct kvs_rdma_xfer_msg);
    const size_t ctrl_len = msg_size * 4;

    ctrl = (char*)calloc(1, ctrl_len);
    if (ctrl == NULL) {
        goto cleanup;
    }
    ctrl_mr = ibv_reg_mr(pd, ctrl, ctrl_len,
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ);
    if (ctrl_mr == NULL) {
        xfer_log("reg ctrl MR failed");
        goto cleanup;
    }

    // 第一个 RECV 收快照长度，第二个 RECV 收「RDMA 写完成」通知。
    if (xfer_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 0, msg_size,
                       KVS_RDMA_XFER_WR_RECV_LEN) != 0 ||
        xfer_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 1, msg_size,
                       KVS_RDMA_XFER_WR_RECV_DONE) != 0) {
        goto cleanup;
    }
    if (xfer_poll_cq_state(cq, KVS_RDMA_XFER_WR_RECV_LEN, &wc, &pending) != 0) {
        goto cleanup;
    }

    len_msg = (struct kvs_rdma_xfer_msg*)(ctrl + msg_size * 0);
    if (len_msg->type != KVS_RDMA_XFER_MSG_SNAPSHOT_LEN || len_msg->value == 0 ||
        len_msg->value > UINT32_MAX) {
        xfer_log("invalid snapshot length message");
        goto cleanup;
    }

    snapshot_len = (size_t)len_msg->value;
    snapshot = (char*)malloc(snapshot_len);
    if (snapshot == NULL) {
        goto cleanup;
    }
    snapshot_mr = ibv_reg_mr(pd, snapshot, snapshot_len,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ);
    if (snapshot_mr == NULL) {
        xfer_log("reg snapshot MR failed");
        goto cleanup;
    }

    buf_msg = (struct kvs_rdma_xfer_msg*)(ctrl + msg_size * 2);
    memset(buf_msg, 0, msg_size);
    buf_msg->type = KVS_RDMA_XFER_MSG_SNAPSHOT_BUF;
    buf_msg->addr = (uint64_t)(uintptr_t)snapshot;
    buf_msg->rkey = snapshot_mr->rkey;
    buf_msg->value = (uint64_t)snapshot_len;

    if (xfer_post_send(id->qp, ctrl_mr, buf_msg, msg_size, KVS_RDMA_XFER_WR_SEND_BUF) != 0) {
        goto cleanup;
    }
    if (xfer_poll_cq_state(cq, KVS_RDMA_XFER_WR_SEND_BUF, &wc, &pending) != 0) {
        goto cleanup;
    }

    // 等对端把数据 RDMA_WRITE 进来，并等它的「写完成」通知。
    if (xfer_poll_cq_state(cq, KVS_RDMA_XFER_WR_RECV_DONE, &wc, &pending) != 0) {
        goto cleanup;
    }
    done_msg = (struct kvs_rdma_xfer_msg*)(ctrl + msg_size * 1);
    if (done_msg->type != KVS_RDMA_XFER_MSG_SNAPSHOT_DONE ||
        done_msg->value != (uint64_t)snapshot_len) {
        xfer_log("invalid snapshot-done notification");
        goto cleanup;
    }

    // 数据已在快照缓冲里，落盘后再回 ACK。
    write_rc = xfer_write_file_atomic(out_file, snapshot, snapshot_len);
    if (write_rc != 0) {
        xfer_log("write %s failed", out_file);
    }

    ack_msg = (struct kvs_rdma_xfer_msg*)(ctrl + msg_size * 3);
    memset(ack_msg, 0, msg_size);
    ack_msg->type = KVS_RDMA_XFER_MSG_SNAPSHOT_ACK;
    ack_msg->status = (uint32_t)(write_rc == 0 ? 0 : 1);
    if (xfer_post_send(id->qp, ctrl_mr, ack_msg, msg_size, KVS_RDMA_XFER_WR_SEND_ACK) != 0) {
        goto cleanup;
    }
    if (xfer_poll_cq_state(cq, KVS_RDMA_XFER_WR_SEND_ACK, &wc, &pending) != 0) {
        goto cleanup;
    }

    if (write_rc != 0) {
        goto cleanup;
    }
    xfer_log("snapshot received and written to %s (%zu bytes)", out_file, snapshot_len);
    ret = 0;

cleanup:
    if (snapshot_mr != NULL) {
        ibv_dereg_mr(snapshot_mr);
    }
    free(snapshot);
    if (ctrl_mr != NULL) {
        ibv_dereg_mr(ctrl_mr);
    }
    free(ctrl);
    return ret;
}

static int xfer_create_ready_file(const char* ready_file) {
    if (ready_file == NULL || ready_file[0] == '\0') {
        return 0;
    }
    int fd = open(ready_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        xfer_log("create ready file %s failed: %s", ready_file, strerror(errno));
        return -1;
    }
    close(fd);
    return 0;
}

int kvs_rdma_transfer_recv_file(unsigned short listen_port, const char* out_file,
                                const char* ready_file, int timeout_ms) {
    struct rdma_event_channel* ec = NULL;
    struct rdma_cm_id* listen_id = NULL;
    struct rdma_cm_id* conn_id = NULL;
    struct ibv_pd* pd = NULL;
    struct ibv_cq* cq = NULL;
    struct rdma_cm_event* event = NULL;
    struct sockaddr_in addr;
    struct rdma_conn_param conn_param;
    const char* step = "参数校验";
    int ret = -1;

    if (listen_port == 0 || out_file == NULL || out_file[0] == '\0') {
        xfer_log("invalid arguments");
        return -1;
    }

    if (timeout_ms > 0) {
        alarm((unsigned int)((timeout_ms + 999) / 1000));
    }

    step = "rdma_create_event_channel";
    ec = rdma_create_event_channel();
    if (ec == NULL) {
        goto cleanup;
    }

    step = "rdma_create_id";
    if (rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP) != 0) {
        goto cleanup;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(listen_port);

    step = "rdma_bind_addr";
    if (rdma_bind_addr(listen_id, (struct sockaddr*)&addr) != 0) {
        xfer_log("rdma_bind_addr port=%u failed: errno=%d(%s)", listen_port, errno,
                 strerror(errno));
        goto cleanup;
    }
    step = "rdma_listen";
    if (rdma_listen(listen_id, 16) != 0) {
        xfer_log("rdma_listen port=%u failed: errno=%d(%s)", listen_port, errno, strerror(errno));
        goto cleanup;
    }

    // 就绪信号必须在 listen 成功之后创建，父进程据此才会发 SYNC。
    step = "创建就绪文件";
    if (xfer_create_ready_file(ready_file) != 0) {
        goto cleanup;
    }
    xfer_log("listening on rdma port %u, waiting for one connection", listen_port);

    step = "等待连接请求";
    if (xfer_wait_cm_event(ec, RDMA_CM_EVENT_CONNECT_REQUEST, &event) != 0) {
        goto cleanup;
    }
    conn_id = event->id;
    rdma_ack_cm_event(event);
    event = NULL;

    step = "创建 QP";
    if (xfer_create_qp(conn_id, &pd, &cq) != 0) {
        rdma_reject(conn_id, NULL, 0);
        goto cleanup;
    }

    xfer_fill_conn_param(&conn_param);
    step = "rdma_accept";
    if (rdma_accept(conn_id, &conn_param) != 0) {
        goto cleanup;
    }

    step = "等待 ESTABLISHED";
    if (xfer_wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED, &event) != 0) {
        goto cleanup;
    }
    rdma_ack_cm_event(event);
    event = NULL;

    step = "接收快照";
    ret = xfer_recv_over_qp(conn_id, pd, cq, out_file);

cleanup:
    if (ret != 0) {
        int saved_errno = errno;
        xfer_log("recv failed at step [%s]: errno=%d(%s)", step, saved_errno,
                 strerror(saved_errno));
    }
    if (conn_id != NULL) {
        if (conn_id->qp != NULL) {
            rdma_destroy_qp(conn_id);
        }
        rdma_destroy_id(conn_id);
    }
    if (cq != NULL) {
        ibv_destroy_cq(cq);
    }
    if (pd != NULL) {
        ibv_dealloc_pd(pd);
    }
    if (listen_id != NULL) {
        rdma_destroy_id(listen_id);
    }
    if (ec != NULL) {
        rdma_destroy_event_channel(ec);
    }
    return ret;
}
