// 采集环：master 生产、ebpf-agent 消费的共享内存环（重设计阶段 3）。
//
// 记录格式与 BPF ringbuf 完全一致（见 include/kvs_ebpf_ring.h 顶部注释），
// 因此消费侧无论走内核 ringbuf 还是本模块，都用同一个 kvs_ebpf_event_parse()。
#include "kvs_ebpf_ring.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define KVS_EBPF_RING_MAGIC 0x4b564552u /* "KVER" */
#define KVS_EBPF_RING_VERSION 1u
#define KVS_EBPF_RING_HEADER_SIZE 64u

// 环头放在共享内存起始处；head/tail 用 64 位原子读写，跨进程用 acquire/release
// 保证「先写记录、后发布 head」以及「先读 head、后读记录」的顺序。
struct kvs_ebpf_ring_header {
    uint32_t magic;
    uint32_t version;
    uint32_t record_size;
    uint32_t capacity;
    uint64_t head;    // 生产者进度（已写入的总条数）
    uint64_t tail;    // 消费者进度（已取走的总条数）
    uint64_t dropped; // 生产端因环满丢弃的条数
    uint8_t pad[KVS_EBPF_RING_HEADER_SIZE - 40];
};

static struct kvs_ebpf_ring_header* g_ring_header = NULL;
static unsigned char* g_ring_data = NULL;
static size_t g_ring_map_size = 0;
static char g_ring_path[512] = {0};
static int g_ring_is_creator = 0;

// ==================== 记录编解码 ====================
// 与 kvs_replication.cpp（Master 侧生产者） / 内核 ringbuf 里的记录布局保持一致。

int kvs_ebpf_event_fits(int argc, char* argv[]) {
    if (argc < 1 || argc > KVS_EBPF_EVENT_MAX_ARGS || argv == NULL) {
        return 0;
    }
    for (int i = 0; i < argc; ++i) {
        if (argv[i] == NULL) {
            return 0;
        }
        size_t n = strlen(argv[i]) + 1; // 含结尾 '\0'
        if (n > KVS_EBPF_EVENT_ARG_MAX) {
            return 0;
        }
    }
    return 1;
}

int kvs_ebpf_event_parse(const unsigned char* data, unsigned int len, unsigned int* out_seq,
                         char* argbuf, size_t argbuf_size, char* argv[], int max_args) {
    if (data == NULL || argbuf == NULL || argv == NULL || len < KVS_EBPF_EVENT_HEADER_SIZE) {
        return -1;
    }

    uint32_t seq = 0;
    uint32_t argc = 0;
    memcpy(&seq, data, 4);
    memcpy(&argc, data + 4, 4);
    if (argc == 0 || argc > (uint32_t)KVS_EBPF_EVENT_MAX_ARGS || (int)argc > max_args) {
        return -1;
    }

    size_t used = 0;
    for (uint32_t i = 0; i < argc; ++i) {
        uint32_t alen = 0;
        memcpy(&alen, data + 8 + i * 4, 4);
        if (alen == 0 || alen > KVS_EBPF_EVENT_ARG_MAX) {
            return -1;
        }
        size_t off = KVS_EBPF_EVENT_HEADER_SIZE + (size_t)i * KVS_EBPF_EVENT_ARG_MAX;
        if (off + alen > len || used + alen > argbuf_size) {
            return -1;
        }
        memcpy(argbuf + used, data + off, alen);
        if (argbuf[used + alen - 1] != '\0') {
            return -1; // 记录被截断：宁可重同步也不能复制半条命令
        }
        argv[i] = argbuf + used;
        used += alen;
    }

    if (out_seq != NULL) {
        *out_seq = seq;
    }
    return (int)argc;
}

// 把 argv 编码成一条记录（定长 KVS_EBPF_RECORD_SIZE）。
static int ring_encode(unsigned int seq, int argc, char* argv[], unsigned char* rec) {
    if (!kvs_ebpf_event_fits(argc, argv)) {
        return -1;
    }

    memset(rec, 0, KVS_EBPF_RECORD_SIZE);
    uint32_t n_seq = seq;
    uint32_t n_argc = (uint32_t)argc;
    memcpy(rec, &n_seq, 4);
    memcpy(rec + 4, &n_argc, 4);
    for (int i = 0; i < argc; ++i) {
        size_t n = strlen(argv[i]) + 1;
        uint32_t alen = (uint32_t)n;
        memcpy(rec + 8 + i * 4, &alen, 4);
        memcpy(rec + KVS_EBPF_EVENT_HEADER_SIZE + (size_t)i * KVS_EBPF_EVENT_ARG_MAX, argv[i], n);
    }
    return 0;
}

// ==================== 生命周期 ====================

int kvs_ebpf_ring_create(const char* path) {
    if (path == NULL || path[0] == '\0') {
        return -1;
    }

    kvs_ebpf_ring_close();

    size_t data_size = (size_t)KVS_EBPF_RING_CAPACITY * KVS_EBPF_RECORD_SIZE;
    g_ring_map_size = KVS_EBPF_RING_HEADER_SIZE + data_size;

    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "[EBPF-RING] open %s failed: %s\n", path, strerror(errno));
        return -1;
    }
    if (ftruncate(fd, (off_t)g_ring_map_size) != 0) {
        fprintf(stderr, "[EBPF-RING] ftruncate %s failed: %s\n", path, strerror(errno));
        close(fd);
        return -1;
    }

    void* map = mmap(NULL, g_ring_map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        fprintf(stderr, "[EBPF-RING] mmap %s failed: %s\n", path, strerror(errno));
        return -1;
    }

    g_ring_header = (struct kvs_ebpf_ring_header*)map;
    g_ring_data = (unsigned char*)map + KVS_EBPF_RING_HEADER_SIZE;

    memset(g_ring_header, 0, sizeof(*g_ring_header));
    g_ring_header->magic = KVS_EBPF_RING_MAGIC;
    g_ring_header->version = KVS_EBPF_RING_VERSION;
    g_ring_header->record_size = KVS_EBPF_RECORD_SIZE;
    g_ring_header->capacity = KVS_EBPF_RING_CAPACITY;
    __atomic_store_n(&g_ring_header->head, 0ull, __ATOMIC_RELEASE);
    __atomic_store_n(&g_ring_header->tail, 0ull, __ATOMIC_RELEASE);
    __atomic_store_n(&g_ring_header->dropped, 0ull, __ATOMIC_RELEASE);

    snprintf(g_ring_path, sizeof(g_ring_path), "%s", path);
    g_ring_is_creator = 1;
    return 0;
}

int kvs_ebpf_ring_open(const char* path) {
    if (path == NULL || path[0] == '\0') {
        return -1;
    }

    kvs_ebpf_ring_close();

    int fd = open(path, O_RDWR);
    if (fd < 0) {
        fprintf(stderr, "[EBPF-RING] open %s failed: %s\n", path, strerror(errno));
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < (off_t)KVS_EBPF_RING_HEADER_SIZE) {
        fprintf(stderr, "[EBPF-RING] %s has invalid size\n", path);
        close(fd);
        return -1;
    }

    size_t map_size = (size_t)st.st_size;
    void* map = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        fprintf(stderr, "[EBPF-RING] mmap %s failed: %s\n", path, strerror(errno));
        return -1;
    }

    struct kvs_ebpf_ring_header* hdr = (struct kvs_ebpf_ring_header*)map;
    if (hdr->magic != KVS_EBPF_RING_MAGIC || hdr->version != KVS_EBPF_RING_VERSION ||
        hdr->record_size != KVS_EBPF_RECORD_SIZE || hdr->capacity == 0) {
        fprintf(stderr, "[EBPF-RING] %s has unexpected layout (magic=0x%x)\n", path, hdr->magic);
        munmap(map, map_size);
        return -1;
    }
    if (KVS_EBPF_RING_HEADER_SIZE + (size_t)hdr->capacity * hdr->record_size > map_size) {
        fprintf(stderr, "[EBPF-RING] %s is truncated\n", path);
        munmap(map, map_size);
        return -1;
    }

    g_ring_header = hdr;
    g_ring_data = (unsigned char*)map + KVS_EBPF_RING_HEADER_SIZE;
    g_ring_map_size = map_size;
    snprintf(g_ring_path, sizeof(g_ring_path), "%s", path);
    g_ring_is_creator = 0;
    return 0;
}

void kvs_ebpf_ring_close(void) {
    if (g_ring_header != NULL) {
        munmap(g_ring_header, g_ring_map_size);
    }
    if (g_ring_is_creator && g_ring_path[0] != '\0') {
        unlink(g_ring_path);
    }
    g_ring_header = NULL;
    g_ring_data = NULL;
    g_ring_map_size = 0;
    g_ring_path[0] = '\0';
    g_ring_is_creator = 0;
}

uint64_t kvs_ebpf_ring_dropped(void) {
    if (g_ring_header == NULL) {
        return 0;
    }
    return __atomic_load_n(&g_ring_header->dropped, __ATOMIC_RELAXED);
}

void kvs_ebpf_ring_reset(void) {
    if (g_ring_header == NULL) {
        return;
    }
    __atomic_store_n(&g_ring_header->head, 0ull, __ATOMIC_RELEASE);
    __atomic_store_n(&g_ring_header->tail, 0ull, __ATOMIC_RELEASE);
    __atomic_store_n(&g_ring_header->dropped, 0ull, __ATOMIC_RELEASE);
}

// ==================== 生产 / 消费 ====================

int kvs_ebpf_ring_push(unsigned int seq, int argc, char* argv[]) {
    if (g_ring_header == NULL || g_ring_data == NULL) {
        return -1;
    }

    unsigned char rec[KVS_EBPF_RECORD_SIZE];
    if (ring_encode(seq, argc, argv, rec) != 0) {
        return -1;
    }

    uint32_t capacity = g_ring_header->capacity;
    unsigned long long head = __atomic_load_n(&g_ring_header->head, __ATOMIC_RELAXED);
    unsigned long long tail = __atomic_load_n(&g_ring_header->tail, __ATOMIC_ACQUIRE);

    if (head - tail >= (unsigned long long)capacity) {
        // 环满：丢弃并记账，消费端靠 seq 缺口发现并按设计触发重同步。
        __atomic_add_fetch(&g_ring_header->dropped, 1ull, __ATOMIC_RELAXED);
        return -1;
    }

    unsigned long long slot = head % capacity;
    memcpy(g_ring_data + (size_t)slot * KVS_EBPF_RECORD_SIZE, rec, KVS_EBPF_RECORD_SIZE);

    // 先写记录、再发布 head。
    __atomic_store_n(&g_ring_header->head, head + 1, __ATOMIC_RELEASE);
    return 0;
}

int kvs_ebpf_ring_pop(unsigned int* out_seq, char* argbuf, size_t argbuf_size, char* argv[],
                      int max_args) {
    if (g_ring_header == NULL || g_ring_data == NULL) {
        return 0;
    }

    // 先读 head（acquire）、再读记录，保证看到的是完整记录。
    unsigned long long head = __atomic_load_n(&g_ring_header->head, __ATOMIC_ACQUIRE);
    unsigned long long tail = __atomic_load_n(&g_ring_header->tail, __ATOMIC_RELAXED);
    if (tail >= head) {
        return 0;
    }

    uint32_t capacity = g_ring_header->capacity;
    unsigned long long slot = tail % capacity;
    const unsigned char* rec = g_ring_data + (size_t)slot * KVS_EBPF_RECORD_SIZE;

    int argc = kvs_ebpf_event_parse(rec, (unsigned int)KVS_EBPF_RECORD_SIZE, out_seq, argbuf,
                                    argbuf_size, argv, max_args);

    // 无论记录是否合法都推进 tail：非法记录只能靠重同步恢复，卡住会一直阻塞。
    __atomic_store_n(&g_ring_header->tail, tail + 1, __ATOMIC_RELEASE);
    return argc;
}
