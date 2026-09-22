#ifdef KVS_ENABLE_RDMA

#include "kvs_config.h"
#include "kvs_replication.h"
#include "kvs_snapshot.h"

#include <arpa/inet.h>
#include <errno.h>
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
};

enum {
    KVS_RDMA_WR_RECV_BUF = 2000,
    KVS_RDMA_WR_RECV_ACK = 2001,
    KVS_RDMA_WR_SEND_LEN = 3000,
    KVS_RDMA_WR_WRITE_IMM = 3001,

    KVS_RDMA_WR_RECV_LEN = 4000,
    KVS_RDMA_WR_RECV_IMM = 4001,
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
        return -1;
    }

    struct ibv_cq* cq = ibv_create_cq(id->verbs, 128, NULL, NULL, 0);
    if (cq == NULL) {
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
    return ibv_post_send(qp, &wr, &bad_wr);
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
    return ibv_post_recv(qp, &wr, &bad_wr);
}

static int kvs_rdma_post_write_imm(struct ibv_qp* qp, struct ibv_mr* local_mr, void* local_buf,
                                   size_t len, uint64_t remote_addr, uint32_t remote_rkey,
                                   uint32_t imm_data, uint64_t wr_id) {
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
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(imm_data);
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = remote_rkey;

    struct ibv_send_wr* bad_wr = NULL;
    return ibv_post_send(qp, &wr, &bad_wr);
}

static int kvs_rdma_serve_snapshot(struct rdma_cm_id* id, struct ibv_pd* pd,
                                   struct ibv_cq* cq) {
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
    struct ibv_wc wc;
    struct kvs_rdma_pending_wc pending;
    memset(&pending, 0, sizeof(pending));
    int ret = -1;

    const size_t msg_size = sizeof(struct kvs_rdma_msg);
    const size_t ctrl_len = msg_size * 4;

    if (kvs_snapshot_serialize(&snapshot, &snapshot_len) != 0 || snapshot == NULL ||
        snapshot_len > UINT32_MAX) {
        goto cleanup;
    }

    snapshot_mr = ibv_reg_mr(pd, snapshot, snapshot_len,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ);
    if (snapshot_mr == NULL) {
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
        goto cleanup;
    }

    // 接收 Replica 发来的快照缓冲区地址和最终 ACK。
    if (kvs_rdma_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 0, msg_size,
                           KVS_RDMA_WR_RECV_BUF) != 0 ||
        kvs_rdma_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 1, msg_size,
                           KVS_RDMA_WR_RECV_ACK) != 0) {
        goto cleanup;
    }

    len_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 2);
    len_msg->type = KVS_RDMA_MSG_SNAPSHOT_LEN;
    len_msg->value = (uint64_t)snapshot_len;
    if (kvs_rdma_post_send(id->qp, ctrl_mr, len_msg, msg_size, KVS_RDMA_WR_SEND_LEN) != 0) {
        goto cleanup;
    }

    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_SEND_LEN, &wc, &pending) != 0) {
        goto cleanup;
    }
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_RECV_BUF, &wc, &pending) != 0) {
        goto cleanup;
    }

    buf_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 0);
    if (buf_msg->type != KVS_RDMA_MSG_SNAPSHOT_BUF || buf_msg->rkey == 0 ||
        buf_msg->addr == 0) {
        goto cleanup;
    }

    if (kvs_rdma_post_write_imm(id->qp, snapshot_mr, snapshot, snapshot_len, buf_msg->addr,
                                buf_msg->rkey, (uint32_t)snapshot_len,
                                KVS_RDMA_WR_WRITE_IMM) != 0) {
        goto cleanup;
    }
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_WRITE_IMM, &wc, &pending) != 0) {
        goto cleanup;
    }
    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_RECV_ACK, &wc, &pending) != 0) {
        goto cleanup;
    }

    ack_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 1);
    if (ack_msg->type != KVS_RDMA_MSG_SNAPSHOT_ACK || ack_msg->status != 0) {
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
    int load_ret = -1;
    struct ibv_wc wc;
    struct kvs_rdma_pending_wc pending;
    memset(&pending, 0, sizeof(pending));
    int ret = -1;

    const size_t msg_size = sizeof(struct kvs_rdma_msg);
    const size_t ctrl_len = msg_size * 4;

    ctrl = (char*)calloc(1, ctrl_len);
    if (ctrl == NULL) {
        goto cleanup;
    }

    ctrl_mr = ibv_reg_mr(pd, ctrl, ctrl_len,
                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ);
    if (ctrl_mr == NULL) {
        goto cleanup;
    }

    // 第一个 RECV 接收快照长度，第二个 RECV 用于 RDMA_WRITE_WITH_IMM 完成通知。
    if (kvs_rdma_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 0, msg_size,
                           KVS_RDMA_WR_RECV_LEN) != 0 ||
        kvs_rdma_post_recv(id->qp, ctrl_mr, ctrl + msg_size * 1, msg_size,
                           KVS_RDMA_WR_RECV_IMM) != 0) {
        goto cleanup;
    }

    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_RECV_LEN, &wc, &pending) != 0) {
        goto cleanup;
    }

    len_msg = (struct kvs_rdma_msg*)(ctrl + msg_size * 0);
    if (len_msg->type != KVS_RDMA_MSG_SNAPSHOT_LEN || len_msg->value == 0 ||
        len_msg->value > UINT32_MAX) {
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
        goto cleanup;
    }

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

    if (kvs_rdma_poll_cq_state(cq, KVS_RDMA_WR_RECV_IMM, &wc, &pending) != 0) {
        goto cleanup;
    }
    if (wc.opcode != IBV_WC_RECV_RDMA_WITH_IMM || ntohl(wc.imm_data) != (uint32_t)snapshot_len) {
        goto cleanup;
    }

    load_ret = kvs_snapshot_load_buffer(snapshot, snapshot_len);

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

    (void)kvs_rdma_serve_snapshot(id, pd, cq);

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

void kvs_replication_stop_rdma_listener() {
    if (!g_rdma_listener_running) {
        kvs_replication_set_rdma_enabled(0);
        return;
    }

    kvs_replication_set_rdma_enabled(0);
    g_rdma_listener_running = 0;
    if (g_rdma_listen_id != NULL) {
        rdma_destroy_id(g_rdma_listen_id);
        g_rdma_listen_id = NULL;
    }
    pthread_join(g_rdma_listener_tid, NULL);
}

int kvs_replication_rdma_full_sync(const char* master_ip, int master_port) {
    if (master_ip == NULL || master_port <= 0 || master_port + KVS_RDMA_PORT_OFFSET > 65535) {
        return -1;
    }

    struct rdma_event_channel* ec = rdma_create_event_channel();
    if (ec == NULL) {
        return -1;
    }

    struct rdma_cm_id* id = NULL;
    if (rdma_create_id(ec, &id, NULL, RDMA_PS_TCP) != 0) {
        rdma_destroy_event_channel(ec);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)(master_port + KVS_RDMA_PORT_OFFSET));
    if (inet_pton(AF_INET, master_ip, &addr.sin_addr) <= 0) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }

    if (rdma_resolve_addr(id, NULL, (struct sockaddr*)&addr, 2000) != 0) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }

    struct rdma_cm_event* event = NULL;
    if (kvs_rdma_wait_cm_event(ec, RDMA_CM_EVENT_ADDR_RESOLVED, &event) != 0) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }
    rdma_ack_cm_event(event);

    if (rdma_resolve_route(id, 2000) != 0) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }
    if (kvs_rdma_wait_cm_event(ec, RDMA_CM_EVENT_ROUTE_RESOLVED, &event) != 0) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }
    rdma_ack_cm_event(event);

    struct ibv_pd* pd = NULL;
    struct ibv_cq* cq = NULL;
    if (kvs_rdma_create_qp(id, &pd, &cq) != 0) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }

    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;

    if (rdma_connect(id, &conn_param) != 0) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }
    if (kvs_rdma_wait_cm_event(ec, RDMA_CM_EVENT_ESTABLISHED, &event) != 0) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }
    rdma_ack_cm_event(event);

    int ret = kvs_rdma_receive_snapshot(id, pd, cq);

    rdma_destroy_qp(id);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    rdma_destroy_id(id);
    rdma_destroy_event_channel(ec);
    return ret;
}

#endif
