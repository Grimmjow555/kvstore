// rdma-client：Master 侧的全量快照搬运进程（重设计阶段 2）。
//
// 由 Master 在收到 SYNC、写好 .tmp 之后派生：
//   rdma-client --file <快照> --peer <replica_ip> --port <rdma_port> [--timeout-ms N]
// 它把文件通过 RDMA 写给远端 rdma-server，完成后退出。设计与约束见
// docs/replication-redesign.md §8.3 与 include/kvs_rdma_transfer.h。
#include "kvs_rdma_transfer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void usage(const char* prog) {
    fprintf(stderr,
            "用法: %s --file <path> --peer <ip> --port <rdma_port> [--timeout-ms N]\n"
            "  --file        要发送的快照文件（Master 写好的 .tmp）\n"
            "  --peer        对端 rdma-server 所在主机 IP（必须是 RDMA 网卡 IP）\n"
            "  --port        对端 rdma-server 监听端口\n"
            "  --timeout-ms  整体超时，默认 60000\n",
            prog);
}

int main(int argc, char* argv[]) {
    const char* file = NULL;
    const char* peer = NULL;
    long port = 0;
    long timeout_ms = 60000;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--file") == 0 && i + 1 < argc) {
            file = argv[++i];
        } else if (strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            peer = argv[++i];
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = strtol(argv[++i], NULL, 10);
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

    if (file == NULL || peer == NULL || port <= 0 || port > 65535) {
        usage(argv[0]);
        return 2;
    }

    int rc = kvs_rdma_transfer_send_file(file, peer, (unsigned short)port, (int)timeout_ms);
    return rc == 0 ? 0 : 1;
}
