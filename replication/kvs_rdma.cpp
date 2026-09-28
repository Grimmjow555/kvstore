#ifdef KVS_ENABLE_RDMA

// pthread_timedjoin_np 需要 _GNU_SOURCE（glibc 扩展），必须在任何头文件之前定义。
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "kvs_config.h"
#include "kvs_replication.h"
#include "kvs_snapshot.h"
#include "kvstore.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// RDMA 控制消息使用固定 32 字节结构，便于双方按同一布局收发。
struct kvs_rdma_msg {
    uint32_t type;
    uint32_t status;
    uint64_t value;
    uint64_t addr;
    uint32_t rkey;
    uint32_t reserved;
};

enum {
    KVS_RDMA_MSG_SNAPSHOT_LEN = 1,
    KVS_RDMA_MSG_SNAPSHOT_BUF = 2,
    KVS_RDMA_MSG_SNAPSHOT_ACK = 3,
    // 快照 RDMA 写完成通知。
    //
    // 不使用 IBV_WR_RDMA_WRITE_WITH_IMM：SoftiWARP(siw) 下带立即数的写会在
    // ibv_post_send 处直接返回 ENOSPC，导致整条 RDMA 全量同步失败。改用
    // 「普通 RDMA_WRITE + 一条小 SEND 通知」，大块数据仍然走 RDMA。
    KVS_RDMA_MSG_SNAPSHOT_DONE = 4,
};

enum {
    KVS_RDMA_WR_RECV_BUF = 2000,
    KVS_RDMA_WR_RECV_ACK = 2001,
    KVS_RDMA_WR_SEND_LEN = 3000,
    KVS_RDMA_WR_WRITE = 3001,
    KVS_RDMA_WR_SEND_DONE = 3002,

    KVS_RDMA_WR_RECV_LEN = 4000,
    KVS_RDMA_WR_RECV_DONE = 4001,
    KVS_RDMA_WR_SEND_BUF = 5000,
    KVS_RDMA_WR_SEND_ACK = 5001,
};

// Master 的 RDMA 监听线程状态。
static pthread_t g_rdma_listener_tid;
static volatile int g_rdma_listener_running = 0;
static struct rdma_event_channel* g_rdma_event_channel = NULL;
static struct rdma_cm_id* g_rdma_listen_id = NULL;
static pthread_mutex_t g_rdma_ready_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_rdma_ready_cond = PTHREAD_COND_INITIALIZER;
static int g_rdma_listener_ready = 0;

static void kvs_rdma_signal_listener_state() {
    pthread_mutex_lock(&g_rdma_ready_mutex);
    pthread_cond_broadcast(&g_rdma_ready_cond);
    pthread_mutex_unlock(&g_rdma_ready_mutex);
}

static int kvs_rdma_wait_cm_event(struct rdma_event_channel* ec, enum rdma_cm_event_type type,
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

static int kvs_rdma_create_qp(struct rdma_cm_id* id, struct ibv_pd** out_pd,
                              struct ibv_cq** out_cq) {
    if (id == NULL || out_pd == NULL || out_cq == NULL) {
        return -1;
    }

    *out_pd = NULL;
    *out_cq = NULL;

    struct ibv_pd* pd = ibv_alloc_pd(id->verbs);
    if (pd == NULL) {
        kvs_log(KVS_LOG_WARN, "[RDMA] ibv_alloc_pd failed: errno=%d(%s)", errno, strerror(errno));
        return -1;
    }

    struct ibv_cq* cq = ibv_create_cq(id->verbs, 128, NULL, NULL, 0);
    if (cq == NULL) {
        kvs_log(KVS_LOG_WARN, "[RDMA] ibv_create_cq failed: errno=%d(%s)", errno, strerror(errno));
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
        kvs_log(KVS_LOG_WARN, "[RDMA] rdma_create_qp failed: errno=%d(%s)", errno, strerror(errno));
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        return -1;
    }

    *out_pd = pd;
    *out_cq = cq;
    return 0;
}

struct kvs_rdma_pending_wc {
    struct ibv_wc entries[8];
    int count;
};

static int kvs_rdma_poll_cq_state(struct ibv_cq* cq, uint64_t wr_id, struct ibv_wc* out_wc,
                                  struct kvs_rdma_pending_wc* pending) {
    if (cq == NULL || out_wc == NULL) {
        return -1;
    }

    while (1) {
        for (int i = 0; i < pending->count; ++i) {
            if (pending->entries[i].wr_id == wr_id) {
                *out_wc = pending->entries[i];
                pending->entries[i] = pending->entries[pending->count - 1];
                pending->count--;
                if (out_wc->status != IBV_WC_SUCCESS) {
                    return -1;
                }
                return 0;
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
            if (out_wc->status != IBV_WC_SUCCESS) {
                return -1;
            }
            return 0;
        }

        // 暂时缓存非目标完成事件，后续等待对应 wr_id 时再取出。
        if (pending->count < (int)(sizeof(pending->entries) / sizeof(pending->entries[0]))) {
            pending->entries[pending->count++] = *out_wc;
            continue;
        }
        return -1;
    }
}

static int kvs_rdma_post_send(struct ibv_qp* qp, struct ibv_mr* mr, void* buf, size_t len,
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
        // ibv_post_send 的返回值本身就是错误码（不通过 errno 传递）。
        kvs_log(KVS_LOG_WARN, "[RDMA] ibv_post_send(SEND) failed: rc=%d", rc);
    }
    return rc;
}

static int kvs_rdma_post_recv(struct ibv_qp* qp, struct ibv_mr* mr, void* buf, size_t len,
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
        kvs_log(KVS_LOG_WARN, "[RDMA] ibv_post_recv failed: rc=%d", rc);
    }
    return rc;
}

// 普通 RDMA 写（不带立即数）。完成通知由调用方另行发送一条 SEND 消息。
static int kvs_rdma_post_write(struct ibv_qp* qp, struct ibv_mr* local_mr, void* local_buf,
                               size_t len, uint64_t remote_addr, uint32_t remote_rkey,
                               uint64_t wr_id) {
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
        kvs_log(KVS_LOG_WARN, "[RDMA] ibv_post_send(RDMA_WRITE) failed: rc=%d", rc);
    }
    return rc;
}

static int kvs_rdma_serve_snapshot(struct rdma_cm_id* id, struct ibv_pd* pd,
                                   struct ibv_cq* cq, const char* peer_ip) {
    if (id == NULL || pd == NULL || cq == NULL) {
        return -1;
    }

    char* snapshot = NULL;
    size_t snapshot_len = 0;
    struct ibv_mr* snapshot_mr = NULL;
    char* ctrl = NULL;
    struct ibv_mr* ctrl_mr = NULL;
    struct kvs_rdma_msg* len_msg = NULL;
    struct kvs_rdma_msg* buf_msg = NULL;
    struct kvs_rdma_msg* ack_msg = NULL;
    struct kvs_rdma_msg* done_msg = NULL;
    struct ibv_wc wc;
    struct kvs_rdma_pending_wc pending;
    memset(&pending, 0, sizeof(pending));
    int ret = -1;

    const size_t msg_size = sizeof(struct kvs_rdma_msg);
    const size_t ctrl_len = msg_size * 4;

    const char* step = "序列化快照";
    // 快照序列化与增量日志序号必须在同一把存储锁内确定：序列化期间没有写
    // 命令落地，快照内容与 snapshot_seq 严格对齐，之后按 seq 补发增量即可。
    unsigned long long snapshot_seq = 0;
    kvs_data_lock();
    snapshot_seq = kvs_replication_backlog_offset();
    int serialize_rc = kvs_snapshot_serialize(&snapshot, &snapshot_len);
    kvs_data_unlock();

    if (serialize_rc != 0 || snapshot == NULL || snapshot_len > UINT32_MAX) {
        goto cleanup;
    }

    step = "注册快照 MR";
    snapshot_mr = ibv_reg_mr(pd, snapshot, snapshot_len,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ);
    if (snapshot_mr == NULL) {
        goto cleanup;
    }

    step = "分配控制缓冲";
    ctrl = (char*)calloc(1, ctrl_len);
    if (ctrl == NULL) {
        goto cleanup;
    }

    step = "注册控制 MR";
    ctrl_mr = ibv_reg_mr(pd, ctrl, ctrl_len,
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ);
    if (ctrl_mr == NULL) {
        goto cleanup;
    }

    // 接收 Replica 发来的快照缓冲区地址和最终 ACK。
    step = "投递接收请求";
    if (kvs_rdma_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 0, msg_size,
                           KVS_RDMA_WR_RECV_BUF) != 0 ||
        kvs_rdma_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 1, msg_size,
                           KVS_RDMA_WR_RECV_ACK) != 0) {
        goto cleanup;
    }

    len_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 2);
    len_msg->type = KVS_RDMA_MSG_SNAPSHOT_LEN;
    len_msg->value = (uint64_t)snapshot_len;
    step = "发送快照长度";
    if (kvs_rdma_post_send(id->qp, ctrl_mr, len_msg, msg_size, KVS_RDMA_WR_SEND_LEN) != 0) {
        goto cleanup;
    }

    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_SEND_LEN, &wc, &pending) != 0) {
        goto cleanup;
    }
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_RECV_BUF, &wc, &pending) != 0) {
        goto cleanup;
    }

    step = "等待对端缓冲区地址";
    buf_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 0);
    if (buf_msg->type != KVS_RDMA_MSG_SNAPSHOT_BUF || buf_msg->rkey == 0 ||
        buf_msg->addr == 0) {
        goto cleanup;
    }

    step = "RDMA 写快照";
    if (kvs_rdma_post_write(id->qp, snapshot_mr, snapshot, snapshot_len, buf_msg->addr,
                            buf_msg->rkey, KVS_RDMA_WR_WRITE) != 0) {
        goto cleanup;
    }
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_WRITE, &wc, &pending) != 0) {
        goto cleanup;
    }

    // RDMA 写完成后用一条普通 SEND 通知对端「数据已写完」，对端收到后再加载快照。
    step = "发送写完成通知";
    done_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 3);
    memset(done_msg, 0, msg_size);
    done_msg->type = KVS_RDMA_MSG_SNAPSHOT_DONE;
    done_msg->value = (uint64_t)snapshot_len;
    if (kvs_rdma_post_send(id->qp, ctrl_mr, done_msg, msg_size, KVS_RDMA_WR_SEND_DONE) != 0) {
        goto cleanup;
    }
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_SEND_DONE, &wc, &pending) != 0) {
        goto cleanup;
    }

    step = "等待对端 ACK";
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_RECV_ACK, &wc, &pending) != 0) {
        goto cleanup;
    }

    ack_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 1);
    if (ack_msg->type != KVS_RDMA_MSG_SNAPSHOT_ACK || ack_msg->status != 0) {
        goto cleanup;
    }

    ret = 0;

    // 记录「该对端已持有 snapshot_seq 对应的快照」。Replica 随后的 TCP 握手
    // 会消费这条记录，只补发快照之后落地的增量命令。
    kvs_replication_note_rdma_snapshot(peer_ip, snapshot_seq);

cleanup:
    if (ret != 0) {
        int saved_errno = errno;
        kvs_log(KVS_LOG_WARN, "[RDMA] master 下发快照失败于步骤「%s」: errno=%d(%s)", step,
                saved_errno, strerror(saved_errno));
    }
    if (ctrl_mr != NULL) {
        ibv_dereg_mr(ctrl_mr);
    }
    free(ctrl);
    if (snapshot_mr != NULL) {
        ibv_dereg_mr(snapshot_mr);
    }
    free(snapshot);
    return ret;
}

static int kvs_rdma_receive_snapshot(struct rdma_cm_id* id, struct ibv_pd* pd,
                                     struct ibv_cq* cq) {
    if (id == NULL || pd == NULL || cq == NULL) {
        return -1;
    }

    char* ctrl = NULL;
    struct ibv_mr* ctrl_mr = NULL;
    char* snapshot = NULL;
    struct ibv_mr* snapshot_mr = NULL;
    size_t snapshot_len = 0;
    struct kvs_rdma_msg* len_msg = NULL;
    struct kvs_rdma_msg* buf_msg = NULL;
    struct kvs_rdma_msg* ack_msg = NULL;
    struct kvs_rdma_msg* done_msg = NULL;
    int load_ret = -1;
    struct ibv_wc wc;
    struct kvs_rdma_pending_wc pending;
    memset(&pending, 0, sizeof(pending));
    int ret = -1;

    const size_t msg_size = sizeof(struct kvs_rdma_msg);
    const size_t ctrl_len = msg_size * 4;

    const char* step = "分配控制缓冲";
    ctrl = (char*)calloc(1, ctrl_len);
    if (ctrl == NULL) {
        goto cleanup;
    }

    step = "注册控制 MR";
    ctrl_mr = ibv_reg_mr(pd, ctrl, ctrl_len,
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ);
    if (ctrl_mr == NULL) {
        goto cleanup;
    }

    // 第一个 RECV 接收快照长度，第二个 RECV 接收「RDMA 写完成」通知。
    step = "投递接收请求";
    if (kvs_rdma_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 0, msg_size,
                           KVS_RDMA_WR_RECV_LEN) != 0 ||
        kvs_rdma_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 1, msg_size,
                           KVS_RDMA_WR_RECV_DONE) != 0) {
        goto cleanup;
    }

    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_RECV_LEN, &wc, &pending) != 0) {
        goto cleanup;
    }

    step = "等待快照长度";
    len_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 0);
    if (len_msg->type != KVS_RDMA_MSG_SNAPSHOT_LEN || len_msg->value == 0 ||
        len_msg->value > UINT32_MAX) {
        goto cleanup;
    }

    step = "分配快照缓冲";
    snapshot_len = (size_t)len_msg->value;
    snapshot = (char*)malloc(snapshot_len);
    if (snapshot == NULL) {
        goto cleanup;
    }

    step = "注册快照 MR";
    snapshot_mr = ibv_reg_mr(pd, snapshot, snapshot_len,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ);
    if (snapshot_mr == NULL) {
        goto cleanup;
    }

    step = "发送本地缓冲地址";
    buf_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 2);
    memset(buf_msg, 0, msg_size);
    buf_msg->type = KVS_RDMA_MSG_SNAPSHOT_BUF;
    buf_msg->addr = (uint64_t)(uintptr_t)snapshot;
    buf_msg->rkey = snapshot_mr->rkey;
    buf_msg->value = (uint64_t)snapshot_len;

    if (kvs_rdma_post_send(id->qp, ctrl_mr, buf_msg, msg_size, KVS_RDMA_WR_SEND_BUF) != 0) {
        goto cleanup;
    }
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_SEND_BUF, &wc, &pending) != 0) {
        goto cleanup;
    }

    step = "等待 RDMA 写入完成";
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_RECV_DONE, &wc, &pending) != 0) {
        goto cleanup;
    }
    done_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 1);
    if (done_msg->type != KVS_RDMA_MSG_SNAPSHOT_DONE ||
        done_msg->value != (uint64_t)snapshot_len) {
        goto cleanup;
    }

    step = "加载快照";
    // 回放线程/本地客户端线程可能同时在访问存储引擎，加载快照必须串行化。
    kvs_data_lock();
    load_ret = kvs_snapshot_load_buffer(snapshot, snapshot_len);
    kvs_data_unlock();

    step = "回 ACK";
    ack_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 3);
    memset(ack_msg, 0, msg_size);
    ack_msg->type = KVS_RDMA_MSG_SNAPSHOT_ACK;
    ack_msg->status = (uint32_t)(load_ret == 0 ? 0 : 1);
    if (kvs_rdma_post_send(id->qp, ctrl_mr, ack_msg, msg_size, KVS_RDMA_WR_SEND_ACK) != 0) {
        goto cleanup;
    }
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_SEND_ACK, &wc, &pending) != 0) {
        goto cleanup;
    }

    ret = load_ret;

cleanup:
    if (ret != 0) {
        int saved_errno = errno;
        kvs_log(KVS_LOG_WARN, "[RDMA] replica 接收快照失败于步骤「%s」: errno=%d(%s)", step,
                saved_errno, strerror(saved_errno));
    }
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

static void kvs_rdma_handle_connection(struct rdma_cm_id* id, struct rdma_event_channel* ec) {
    if (id == NULL || ec == NULL) {
        return;
    }

    // 被动连接侧 src_addr 是发起方的地址，用于把 RDMA 快照与后续 TCP 握手对齐。
    char peer_ip[INET_ADDRSTRLEN] = {0};
    struct sockaddr_in* peer = (struct sockaddr_in*)&id->route.addr.src_addr;
    if (peer->sin_family == AF_INET) {
        inet_ntop(AF_INET, &peer->sin_addr, peer_ip, sizeof(peer_ip));
    }

    struct ibv_pd* pd = NULL;
    struct ibv_cq* cq = NULL;
    if (kvs_rdma_create_qp(id, &pd, &cq) != 0) {
        rdma_reject(id, NULL, 0);
        rdma_destroy_id(id);
        return;
    }

    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;

    if (rdma_accept(id, &conn_param) != 0) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_id(id);
        return;
    }

    struct rdma_cm_event* event = NULL;
    if (kvs_rdma_wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED, &event) != 0) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_id(id);
        return;
    }
    rdma_ack_cm_event(event);

    (void)kvs_rdma_serve_snapshot(id, pd, cq, peer_ip);

    rdma_destroy_qp(id);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    rdma_destroy_id(id);
}

static void* kvs_rdma_listener_thread(void* arg) {
    unsigned short tcp_port = *(unsigned short*)arg;
    free(arg);

    unsigned short rdma_port = (unsigned short)(tcp_port + KVS_RDMA_PORT_OFFSET);

    g_rdma_event_channel = rdma_create_event_channel();
    if (g_rdma_event_channel == NULL) {
        g_rdma_listener_running = 0;
        kvs_rdma_signal_listener_state();
        return NULL;
    }

    if (rdma_create_id(g_rdma_event_channel, &g_rdma_listen_id, NULL, RDMA_PS_TCP) != 0) {
        rdma_destroy_event_channel(g_rdma_event_channel);
        g_rdma_event_channel = NULL;
        g_rdma_listener_running = 0;
        kvs_rdma_signal_listener_state();
        return NULL;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(rdma_port);

    if (rdma_bind_addr(g_rdma_listen_id, (struct sockaddr*)&addr) != 0 ||
        rdma_listen(g_rdma_listen_id, 16) != 0) {
        rdma_destroy_id(g_rdma_listen_id);
        g_rdma_listen_id = NULL;
        rdma_destroy_event_channel(g_rdma_event_channel);
        g_rdma_event_channel = NULL;
        g_rdma_listener_running = 0;
        kvs_rdma_signal_listener_state();
        return NULL;
    }

    kvs_log(KVS_LOG_INFO, "[RDMA] master listener ready on port %u", rdma_port);
    pthread_mutex_lock(&g_rdma_ready_mutex);
    g_rdma_listener_ready = 1;
    pthread_cond_broadcast(&g_rdma_ready_cond);
    pthread_mutex_unlock(&g_rdma_ready_mutex);

    while (g_rdma_listener_running) {
        struct rdma_cm_event* event = NULL;
        if (rdma_get_cm_event(g_rdma_event_channel, &event) != 0) {
            break;
        }

        if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            struct rdma_cm_id* child_id = event->id;
            rdma_ack_cm_event(event);
            kvs_rdma_handle_connection(child_id, g_rdma_event_channel);
            continue;
        }

        rdma_ack_cm_event(event);
    }

    if (g_rdma_listen_id != NULL) {
        rdma_destroy_id(g_rdma_listen_id);
        g_rdma_listen_id = NULL;
    }
    if (g_rdma_event_channel != NULL) {
        rdma_destroy_event_channel(g_rdma_event_channel);
        g_rdma_event_channel = NULL;
    }
    g_rdma_listener_running = 0;
    pthread_mutex_lock(&g_rdma_ready_mutex);
    g_rdma_listener_ready = 0;
    pthread_cond_broadcast(&g_rdma_ready_cond);
    pthread_mutex_unlock(&g_rdma_ready_mutex);
    return NULL;
}

int kvs_replication_start_rdma_listener(unsigned short tcp_port) {
    if ((int)tcp_port + KVS_RDMA_PORT_OFFSET > 65535) {
        return -1;
    }
    if (g_rdma_listener_running) {
        return -1;
    }
    kvs_replication_set_rdma_enabled(0);

    unsigned short* port = (unsigned short*)malloc(sizeof(unsigned short));
    if (port == NULL) {
        return -1;
    }
    *port = tcp_port;

    g_rdma_listener_running = 1;
    if (pthread_create(&g_rdma_listener_tid, NULL, kvs_rdma_listener_thread, port) != 0) {
        free(port);
        g_rdma_listener_running = 0;
        kvs_replication_set_rdma_enabled(0);
        kvs_rdma_signal_listener_state();
        return -1;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 2;

    pthread_mutex_lock(&g_rdma_ready_mutex);
    while (!g_rdma_listener_ready && g_rdma_listener_running) {
        int wait_ret = pthread_cond_timedwait(&g_rdma_ready_cond, &g_rdma_ready_mutex, &ts);
        if (wait_ret == ETIMEDOUT) {
            break;
        }
    }
    int ready = g_rdma_listener_ready;
    pthread_mutex_unlock(&g_rdma_ready_mutex);

    if (!ready) {
        kvs_replication_set_rdma_enabled(0);
        return -1;
    }
    kvs_replication_set_rdma_enabled(1);
    return 0;
}

// 等待 listener 线程退出的上限（秒）。
//
// librdmacm 在 SoftiWARP(siw) 等环境下可能卡在内部命令通道的阻塞读写上
// （已实测：listener 线程停在向 rdma_cm event channel 的 write 上不再返回）。
// 那种情况下 pthread_join 会永远等下去，把整个优雅关闭流程钉死在这里，
// 外部现象就是“SIGINT/SIGTERM 按了没反应”。因此这里只做有限等待，
// 超时就放弃该线程（detach），保证主线程还能继续收尾并退出。
// 可用 -DKVS_RDMA_STOP_JOIN_SEC=<秒> 覆盖（0 表示不等待，直接 detach）。
#ifndef KVS_RDMA_STOP_JOIN_SEC
#define KVS_RDMA_STOP_JOIN_SEC 2
#endif

void kvs_replication_stop_rdma_listener() {
    if (!g_rdma_listener_running) {
        kvs_replication_set_rdma_enabled(0);
        return;
    }

    kvs_replication_set_rdma_enabled(0);
    g_rdma_listener_running = 0;

    // 把事件通道改成非阻塞：listener 若正等事件，会立刻以 EAGAIN 返回，
    // 从而看到 running=0 并自行退出（退出路径由它自己销毁 id 与通道）。
    if (g_rdma_event_channel != NULL && g_rdma_event_channel->fd >= 0) {
        int flags = fcntl(g_rdma_event_channel->fd, F_GETFL, 0);
        if (flags >= 0) {
            fcntl(g_rdma_event_channel->fd, F_SETFL, flags | O_NONBLOCK);
        }
    }

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += KVS_RDMA_STOP_JOIN_SEC;

    int rc = pthread_timedjoin_np(g_rdma_listener_tid, NULL, &deadline);
    if (rc == 0) {
        return;
    }

    // 线程仍卡在第三方库内部：detach 掉，不再等它，也不再碰它的 id/通道。
    kvs_log(KVS_LOG_WARN,
            "[RDMA] listener thread did not stop within %ds (rc=%d), detaching it so shutdown "
            "can finish",
            KVS_RDMA_STOP_JOIN_SEC, rc);
    pthread_detach(g_rdma_listener_tid);
}

int kvs_replication_rdma_full_sync(const char* master_ip, int master_port) {
    const char* step = "参数校验";
    struct rdma_event_channel* ec = NULL;
    struct rdma_cm_id* id = NULL;
    struct ibv_pd* pd = NULL;
    struct ibv_cq* cq = NULL;
    struct rdma_cm_event* event = NULL;
    int ret = -1;

    if (master_ip == NULL || master_port <= 0 || master_port + KVS_RDMA_PORT_OFFSET > 65535) {
        kvs_log(KVS_LOG_WARN, "[RDMA] full sync 参数非法: %s:%d", master_ip ? master_ip : "(null)",
                master_port);
        return -1;
    }

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
    addr.sin_port = htons((uint16_t)(master_port + KVS_RDMA_PORT_OFFSET));
    step = "inet_pton";
    if (inet_pton(AF_INET, master_ip, &addr.sin_addr) <= 0) {
        goto cleanup;
    }

    step = "rdma_resolve_addr";
    if (rdma_resolve_addr(id, NULL, (struct sockaddr*)&addr, 2000) != 0) {
        goto cleanup;
    }

    step = "等待 ADDR_RESOLVED";
    if (kvs_rdma_wait_cm_event(ec, RDMA_CM_EVENT_ADDR_RESOLVED, &event) != 0) {
        goto cleanup;
    }
    rdma_ack_cm_event(event);
    event = NULL;

    step = "rdma_resolve_route";
    if (rdma_resolve_route(id, 2000) != 0) {
        goto cleanup;
    }
    step = "等待 ROUTE_RESOLVED";
    if (kvs_rdma_wait_cm_event(ec, RDMA_CM_EVENT_ROUTE_RESOLVED, &event) != 0) {
        goto cleanup;
    }
    rdma_ack_cm_event(event);
    event = NULL;

    step = "创建 QP";
    if (kvs_rdma_create_qp(id, &pd, &cq) != 0) {
        goto cleanup;
    }

    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;

    step = "rdma_connect";
    if (rdma_connect(id, &conn_param) != 0) {
        goto cleanup;
    }
    step = "等待 ESTABLISHED";
    if (kvs_rdma_wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED, &event) != 0) {
        goto cleanup;
    }
    rdma_ack_cm_event(event);
    event = NULL;

    step = "接收快照";
    ret = kvs_rdma_receive_snapshot(id, pd, cq);
    if (ret != 0) {
        goto cleanup;
    }
    ret = 0;

cleanup:
    if (ret != 0) {
        int saved_errno = errno;
        kvs_log(KVS_LOG_WARN, "[RDMA] full sync 失败于步骤「%s」: errno=%d(%s)", step, saved_errno,
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
    return ret;
}

#endif
