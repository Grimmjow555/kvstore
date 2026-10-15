// rdma-server：Replica 侧的全量快照接收进程（重设计阶段 2）。
//
// 由 Replica 在发起 SYNC 之前派生：
//   rdma-server --listen-port <rdma_port> --out <快照> [--ready-file <path>]
//               [--timeout-ms N]
// 它在 RDMA 端口上等待一次搬运，把收到的内容原子写入 --out，然后退出。
// 父进程以「子进程退出」作为文件就绪信号；--ready-file 用于让父进程知道
// 端口已经开始监听（从而可以安全地发 SYNC）。设计与约束见
// docs/replication-redesign.md §8.4 与 include/kvs_rdma_transfer.h。
#include "kvs_rdma_transfer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void usage(const char* prog) {
    fprintf(stderr,
            "用法: %s --listen-port <rdma_port> --out <path> [--ready-file <path>] "
            "[--timeout-ms N]\n"
            "  --listen-port  本进程监听的 RDMA 端口\n"
            "  --out          收到的快照落盘路径（先写 .partial 再 rename）\n"
            "  --ready-file   监听就绪后创建的文件，供父进程判断可发 SYNC\n"
            "  --timeout-ms   整体超时，默认 60000\n",
            prog);
}

int main(int argc, char* argv[]) {
    long listen_port = 0;
    const char* out = NULL;
    const char* ready_file = NULL;
    long timeout_ms = 60000;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--listen-port") == 0 && i + 1 < argc) {
            listen_port = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            out = argv[++i];
        } else if (strcmp(argv[i], "--ready-file") == 0 && i + 1 < argc) {
            ready_file = argv[++i];
        } else if (strcmp(argv[i], "--timeout-ms") == 0 && i + 1 < argc) {
            timeout_ms = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "未知参数: %s\n", argv[i]);
            usage(argv[0]);
            return 2;
        }
    }

    if (listen_port <= 0 || listen_port > 65535 || out == NULL) {
        usage(argv[0]);
        return 2;
    }

    int rc = kvs_rdma_transfer_recv_file((unsigned short)listen_port, out, ready_file,
                                         (int)timeout_ms);
    return rc == 0 ? 0 : 1;
}
