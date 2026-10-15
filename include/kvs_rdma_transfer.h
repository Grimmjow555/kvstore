#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// ==================== RDMA 全量快照搬运（独立进程版，重设计阶段 2） ====================
//
// 设计见 docs/replication-redesign.md §3 / §8.3 / §8.4。RDMA 只承担一次性的
// 大块搬运：Master 侧把 .tmp 快照经由 rdma-client 进程 RDMA 写到 Replica 侧的
// rdma-server 进程，后者落盘后退出。实时增量不经过本模块。
//
// 传输协议（与旧的进程内实现相比，数据属主从「服务端」换成了「客户端」）：
//
//   rdma-client（数据属主）                rdma-server（落盘方）
//     --RDMA 建连-------------------------->
//     SEND SNAPSHOT_LEN(value=len) ------->
//                                            分配 len 缓冲并注册 MR
//     <------- SEND SNAPSHOT_BUF(addr,rkey)
//     RDMA_WRITE 数据到远端缓冲
//     SEND SNAPSHOT_DONE(value=len) ------>
//                                            落盘（.partial + fsync + rename）
//     <------- SEND SNAPSHOT_ACK(status)
//
// 约束：必须使用「普通 IBV_WR_RDMA_WRITE + 单独一条 SEND 通知」，不要改回
// IBV_WR_RDMA_WRITE_WITH_IMM——SoftiWARP(siw) 下带立即数的写会在 ibv_post_send
// 直接返回 ENOSPC(28)。

// rdma-client：把 file 通过 RDMA 发给 peer_ip:rdma_port 上的 rdma-server。
// timeout_ms 是整体超时（到点由 SIGALRM 终止进程）；成功返回 0。
int kvs_rdma_transfer_send_file(const char* file, const char* peer_ip, unsigned short rdma_port,
                                int timeout_ms);

// rdma-server：在 listen_port 上等待一次连接，把收到的内容原子写入 out_file。
// ready_file 非空时，在 listen 成功后就创建它，供父进程判断「已就绪」。
// timeout_ms 是整体超时（到点由 SIGALRM 终止进程）；成功返回 0。
int kvs_rdma_transfer_recv_file(unsigned short listen_port, const char* out_file,
                                const char* ready_file, int timeout_ms);

#ifdef __cplusplus
}
#endif
