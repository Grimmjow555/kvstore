#include "kvs_ebpf.h"

#include <arpa/inet.h>
#include <errno.h>
#include <linux/bpf.h>
#include <netinet/in.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

// 队列元素在 Master 和 Replica 两侧必须保持完全一致。
struct kvs_ebpf_entry {
    unsigned int len;
    char data[KVS_EBPF_MAX_COMMAND_LEN];
};

static int bpf_syscall(enum bpf_cmd cmd, union bpf_attr* attr) {
    return (int)syscall(__NR_bpf, cmd, attr, sizeof(*attr));
}

static int g_master_map_fd = -1;
static int g_replica_map_fd = -1;
static char g_pin_path[128] = {0};

// 不建议把 pin 文件直接放在 /sys/fs/bpf 根目录。根目录通常为 root 所有，
// 且可能带 sticky bit；普通用户即使有 CAP_BPF/CAP_SYS_ADMIN 也未必能删除
// 上一次运行遗留的 pin 文件，进而出现 BPF_OBJ_PIN 返回 EEXIST。
// 这里使用管理员预先创建、并 chown 给运行用户的专用子目录。
#define KVS_EBPF_PIN_DIR "/sys/fs/bpf/kvstore"

static void build_pin_path(unsigned short port, char* path, size_t size) {
    snprintf(path, size, KVS_EBPF_PIN_DIR "/kvstore_replication_%u", (unsigned int)port);
}

static int ensure_pin_dir() {
    struct stat st;

    if (stat(KVS_EBPF_PIN_DIR, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr,
                    "[EBPF] pin path exists but is not a directory: %s\n",
                    KVS_EBPF_PIN_DIR);
            return -1;
        }
        return 0;
    }

    if (mkdir(KVS_EBPF_PIN_DIR, 0700) != 0) {
        fprintf(stderr,
                "[EBPF] cannot create pin directory %s (errno=%d, %s)\n",
                KVS_EBPF_PIN_DIR, errno, strerror(errno));
        return -1;
    }

    return 0;
}

int kvs_ebpf_master_init(unsigned short master_port) {
    if (g_master_map_fd >= 0) {
        return 0;
    }

    if (ensure_pin_dir() != 0) {
        fprintf(stderr, "[EBPF] realtime sync unavailable; fallback to TCP sync\n");
        return -1;
    }

    build_pin_path(master_port, g_pin_path, sizeof(g_pin_path));

    // 服务重启或上次异常退出后，bpffs 上可能残留同名 pin。
    // 这里优先清理旧 pin；旧文件来自已经不再运行的相同端口实例。
    // 必须检查 unlink 结果：如果清理失败，后续 BPF_OBJ_PIN 会返回 EEXIST，
    // 而不是把原因隐藏在 errno=17 后面。
    if (unlink(g_pin_path) != 0 && errno != ENOENT) {
        fprintf(stderr,
                "[EBPF] cannot remove stale pin %s (errno=%d, %s); fallback to TCP sync\n",
                g_pin_path, errno, strerror(errno));
        return -1;
    }

    union bpf_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.map_type = BPF_MAP_TYPE_QUEUE;
    attr.key_size = 0;
    attr.value_size = sizeof(kvs_ebpf_entry);
    attr.max_entries = KVS_EBPF_MAX_ENTRIES;
    attr.map_flags = 0;

    int fd = bpf_syscall(BPF_MAP_CREATE, &attr);
    if (fd < 0) {
        fprintf(stderr,
                "[EBPF] create replication queue failed (errno=%d, %s); fallback to TCP sync\n",
                errno, strerror(errno));
        return -1;
    }

    memset(&attr, 0, sizeof(attr));
    attr.pathname = (__u64)g_pin_path;
    attr.bpf_fd = (__u32)fd;
    if (bpf_syscall(BPF_OBJ_PIN, &attr) != 0) {
        fprintf(stderr,
                "[EBPF] pin replication queue failed (errno=%d, %s); fallback to TCP sync\n",
                errno, strerror(errno));
        close(fd);
        return -1;
    }

    g_master_map_fd = fd;
    printf("[EBPF] master realtime sync queue ready: %s\n", g_pin_path);
    return 0;
}

void kvs_ebpf_master_destroy() {
    if (g_master_map_fd >= 0) {
        close(g_master_map_fd);
        g_master_map_fd = -1;
    }

    if (g_pin_path[0] != '\0') {
        // bpffs 上的 pin 文件通过 unlink 删除。
        unlink(g_pin_path);
        g_pin_path[0] = '\0';
    }
}

int kvs_ebpf_master_available() {
    return g_master_map_fd >= 0 ? 1 : 0;
}

int kvs_ebpf_peer_is_local(int fd) {
    struct sockaddr_in peer;
    socklen_t peer_len = sizeof(peer);

    if (getpeername(fd, (struct sockaddr*)&peer, &peer_len) != 0) {
        return 0;
    }

    if (peer.sin_family != AF_INET) {
        return 0;
    }

    uint32_t host_addr = ntohl(peer.sin_addr.s_addr);
    return (host_addr >> 24) == 127 ? 1 : 0;
}

int kvs_ebpf_push(const char* data, unsigned int len) {
    if (g_master_map_fd < 0 || data == NULL || len == 0 ||
        len > KVS_EBPF_MAX_COMMAND_LEN) {
        return -1;
    }

    struct kvs_ebpf_entry entry;
    entry.len = len;
    memcpy(entry.data, data, len);

    union bpf_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.map_fd = (__u32)g_master_map_fd;
    attr.value = (__u64)&entry;
    attr.flags = BPF_ANY;

    // BPF_MAP_TYPE_QUEUE 满时会返回 EAGAIN。这里做短暂重试，
    // 尽量保持命令顺序并避免单条命令悄悄切回 TCP 造成乱序。
    for (int i = 0; i < 1000; ++i) {
        if (bpf_syscall(BPF_MAP_UPDATE_ELEM, &attr) == 0) {
            return 0;
        }
        if (errno != EAGAIN && errno != ENOBUFS && errno != ENOSPC) {
            return -1;
        }
        usleep(1000);
    }

    return -1;
}

int kvs_ebpf_pop(char* out, unsigned int* out_len) {
    if (g_replica_map_fd < 0 || out == NULL || out_len == NULL || *out_len == 0) {
        return -1;
    }

    struct kvs_ebpf_entry entry;
    memset(&entry, 0, sizeof(entry));

    union bpf_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.map_fd = (__u32)g_replica_map_fd;
    attr.value = (__u64)&entry;

    // BPF_MAP_TYPE_QUEUE 中，BPF_MAP_LOOKUP_ELEM 是 peek，
    // 必须使用 BPF_MAP_LOOKUP_AND_DELETE_ELEM 才能弹出队头。
    if (bpf_syscall(BPF_MAP_LOOKUP_AND_DELETE_ELEM, &attr) != 0) {
        return -1;
    }

    if (entry.len == 0 || entry.len > *out_len) {
        return -1;
    }

    memcpy(out, entry.data, entry.len);
    *out_len = entry.len;
    return 0;
}

void kvs_ebpf_drain() {
    char discard[KVS_EBPF_MAX_COMMAND_LEN];
    unsigned int len = sizeof(discard);
    while (kvs_ebpf_pop(discard, &len) == 0) {
        len = sizeof(discard);
    }
}

int kvs_ebpf_replica_open(unsigned short master_port) {
    if (g_replica_map_fd >= 0) {
        return 0;
    }

    char path[128];
    build_pin_path(master_port, path, sizeof(path));

    union bpf_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.pathname = (__u64)path;
    attr.file_flags = 0;

    int fd = bpf_syscall(BPF_OBJ_GET, &attr);
    if (fd < 0) {
        fprintf(stderr,
                "[EBPF] replica cannot open realtime queue %s (errno=%d, %s); "
                "fallback to TCP sync\n",
                path, errno, strerror(errno));
        return -1;
    }

    g_replica_map_fd = fd;
    printf("[EBPF] replica realtime sync queue ready: %s\n", path);
    return 0;
}

void kvs_ebpf_replica_close() {
    if (g_replica_map_fd >= 0) {
        close(g_replica_map_fd);
        g_replica_map_fd = -1;
    }
}

int kvs_ebpf_replica_available() {
    return g_replica_map_fd >= 0 ? 1 : 0;
}
