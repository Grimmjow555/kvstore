#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kvs_config.h"
#include "memorypool.h"

/*
 * Slab 分配器实现
 *
 * 快路径（slab_alloc / slab_free_ptr）只操作线程本地的空闲链表，不加锁；
 * 只有某个 class 的空闲链表为空、需要申请新 chunk 时才取一次全局锁。
 *
 * 布局：
 *   chunk = [mp_chunk_t 头][对齐填充][block0][block1]...
 *   block = [mp_hdr_t(16B)][payload]，返回给调用方的是 payload 指针。
 *
 * 块头同时承担两个职责：
 *   - cls   ：释放时 O(1) 找到所属 size class，无需扫描、无需调用方传 size；
 *   - magic ：区分"已分配 / 已回收"，从而识别 double free 与野指针。
 */

// 每次向系统申请的 chunk 大小。越大越省系统调用，越小越省常驻内存。
#define MP_CHUNK_SIZE (64 * 1024)

// 块头大小。16 字节可以让 payload 与 malloc 一样保持 16 字节对齐。
#define MP_HDR_SIZE 16
#define MP_ALIGN 16

#define MP_MAGIC_ALLOC 0x4B565350u // 'KVSP'：块处于已分配状态
#define MP_MAGIC_FREE 0x4B565346u  // 'KVSF'：块位于空闲链表（用于识别 double free）

#define MP_CLASS_COUNT 7
#define MP_CLASS_LARGE 0xFFFFFFFFu // 超过最大 class 的请求直接走 malloc

typedef struct mp_hdr_s {
    uint32_t magic;
    uint32_t cls;
    struct mp_hdr_s* next; // 只有在该块位于空闲链表时才有意义
} mp_hdr_t;

// 块头必须是 16 字节，才能保证返回给调用方的 payload 保持 16 字节对齐
static_assert(sizeof(mp_hdr_t) == MP_HDR_SIZE, "mp_hdr_t must be 16 bytes");

// 线程本地空闲链表
typedef struct mp_bin_s {
    mp_hdr_t* free_list;
    size_t free_count;
} mp_bin_t;

// chunk 注册信息，用于 slab_dest 与统计
typedef struct mp_chunk_s {
    struct mp_chunk_s* next;
    char* data; // 块区起始地址（16 字节对齐）
    uint32_t cls;
    uint32_t nblocks;
} mp_chunk_t;

static int mp_class_sizes[MP_CLASS_COUNT] = {16, 32, 64, 128, 256, 512, 1024};

// 线程本地空闲链表：快路径不加锁
static thread_local mp_bin_t mp_bins[MP_CLASS_COUNT];

// 全局 chunk 注册表：只在扩容与 slab_dest 时访问
static pthread_mutex_t mp_chunk_lock = PTHREAD_MUTEX_INITIALIZER;
static mp_chunk_t* mp_chunks = NULL;
static size_t mp_chunk_count = 0;
static size_t mp_block_total = 0;

// 统计计数：relaxed 原子累加，避免给无锁快路径引入额外同步
static size_t mp_alloc_total = 0;
static size_t mp_free_total = 0;
static size_t mp_large_total = 0;

static inline void mp_stat_inc(size_t* counter) {
    __atomic_add_fetch(counter, 1, __ATOMIC_RELAXED);
}

static inline size_t mp_stat_load(size_t* counter) {
    return __atomic_load_n(counter, __ATOMIC_RELAXED);
}

// 根据请求大小找到合适的 size class，返回 -1 表示应走大对象路径
static int mp_class_of(size_t size) {
    for (int i = 0; i < MP_CLASS_COUNT; i++) {
        if (size <= (size_t)mp_class_sizes[i]) {
            return i;
        }
    }
    return -1;
}

// 大对象：带块头的 malloc，释放路径与池内块统一
static void* mp_large_alloc(size_t size) {
    if (size > (size_t)-1 - MP_HDR_SIZE) {
        return NULL;
    }

    mp_hdr_t* hdr = (mp_hdr_t*)malloc(MP_HDR_SIZE + size);
    if (hdr == NULL) {
        return NULL;
    }

    hdr->magic = MP_MAGIC_ALLOC;
    hdr->cls = MP_CLASS_LARGE;
    hdr->next = NULL;
    mp_stat_inc(&mp_large_total);
    mp_stat_inc(&mp_alloc_total);

    return (char*)hdr + MP_HDR_SIZE;
}

// 申请一个新 chunk 并切成块，挂到调用线程的空闲链表上
static int mp_refill(mp_bin_t* bin, int cls) {
    size_t block_size = MP_HDR_SIZE + (size_t)mp_class_sizes[cls];

    pthread_mutex_lock(&mp_chunk_lock);

    char* raw = (char*)malloc(MP_CHUNK_SIZE);
    if (raw == NULL) {
        pthread_mutex_unlock(&mp_chunk_lock);
        kvs_log(KVS_LOG_ERROR, "[MEMPOOL] failed to allocate %d bytes chunk", MP_CHUNK_SIZE);
        return -1;
    }

    // chunk 头之后按 16 字节对齐作为块区起点
    uintptr_t base =
        ((uintptr_t)raw + sizeof(mp_chunk_t) + (MP_ALIGN - 1)) & ~(uintptr_t)(MP_ALIGN - 1);
    size_t avail = MP_CHUNK_SIZE - (size_t)(base - (uintptr_t)raw);
    size_t nblocks = avail / block_size;

    if (nblocks == 0) {
        free(raw);
        pthread_mutex_unlock(&mp_chunk_lock);
        kvs_log(KVS_LOG_ERROR, "[MEMPOOL] chunk too small for %zu bytes block", block_size);
        return -1;
    }

    mp_chunk_t* chunk = (mp_chunk_t*)raw;
    chunk->data = (char*)base;
    chunk->cls = (uint32_t)cls;
    chunk->nblocks = (uint32_t)nblocks;
    chunk->next = mp_chunks;
    mp_chunks = chunk;
    mp_chunk_count++;
    mp_block_total += nblocks;

    pthread_mutex_unlock(&mp_chunk_lock);

    // 切块：整段挂到本线程的空闲链表头
    mp_hdr_t* head = bin->free_list;
    for (size_t i = 0; i < nblocks; i++) {
        mp_hdr_t* hdr = (mp_hdr_t*)(chunk->data + i * block_size);
        hdr->magic = MP_MAGIC_FREE;
        hdr->cls = (uint32_t)cls;
        hdr->next = head;
        head = hdr;
    }
    bin->free_list = head;
    bin->free_count += nblocks;

    return 0;
}

// 初始化：空闲链表按线程惰性建立，chunk 按需申请，这里无需预分配。
// 保留该接口是为了让启动流程显式声明"内存池就绪"，幂等、可重复调用。
int slab_init(void) { return 0; }

// 重置统计信息。
// 注意：这里不释放 chunk、不重建空闲链表。kvs_reset_data() 用它替代原来的
// slab_dest() + slab_init()：引擎数据已经被 destroy/init 重建，池自身的空闲
// 链表本来就一致；而把 chunk 交还系统会让其他线程手里正在使用的块变成悬空
// 指针（复制线程会并发回放写命令）。
void slab_reset(void) {
    __atomic_store_n(&mp_alloc_total, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&mp_free_total, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&mp_large_total, 0, __ATOMIC_RELAXED);
}

// 销毁分配器：释放所有 chunk 并清空本线程的空闲链表。
// 只能在确认没有其他线程使用分配器时调用（例如复制线程已 join）。
void slab_dest(void) {
    pthread_mutex_lock(&mp_chunk_lock);
    mp_chunk_t* chunk = mp_chunks;
    size_t chunks = mp_chunk_count;
    mp_chunks = NULL;
    mp_chunk_count = 0;
    mp_block_total = 0;
    pthread_mutex_unlock(&mp_chunk_lock);

    for (int i = 0; i < MP_CLASS_COUNT; i++) {
        mp_bins[i].free_list = NULL;
        mp_bins[i].free_count = 0;
    }

    while (chunk != NULL) {
        mp_chunk_t* next = chunk->next;
        free(chunk);
        chunk = next;
    }

    slab_reset();
    kvs_log(KVS_LOG_DEBUG, "[MEMPOOL] destroyed %zu chunks", chunks);
}

// 从内存池分配
void* slab_alloc(size_t size) {
    if (size == 0) {
        size = 1;
    }

    int cls = mp_class_of(size);
    if (cls < 0) {
        return mp_large_alloc(size);
    }

    mp_bin_t* bin = &mp_bins[cls];
    if (bin->free_list == NULL && mp_refill(bin, cls) != 0) {
        return NULL;
    }

    mp_hdr_t* hdr = bin->free_list;
    bin->free_list = hdr->next;
    bin->free_count--;

    hdr->magic = MP_MAGIC_ALLOC;
    hdr->next = NULL;
    mp_stat_inc(&mp_alloc_total);

    return (char*)hdr + MP_HDR_SIZE;
}

// 分配并清零（只清 payload，块头无需清零）
void* slab_calloc(size_t size) {
    void* ptr = slab_alloc(size);
    if (ptr != NULL) {
        memset(ptr, 0, size);
    }
    return ptr;
}

// 回收内存到内存池
void slab_free_ptr(void* ptr) {
    if (ptr == NULL) {
        return;
    }

    mp_hdr_t* hdr = (mp_hdr_t*)((char*)ptr - MP_HDR_SIZE);

    if (hdr->magic != MP_MAGIC_ALLOC) {
        if (hdr->magic == MP_MAGIC_FREE) {
            // 双重释放：直接忽略，避免空闲链表出现自环、同一个块被分配两次
            kvs_log(KVS_LOG_ERROR, "[MEMPOOL] double free detected: ptr=%p (ignored)", ptr);
        } else {
            kvs_log(KVS_LOG_ERROR, "[MEMPOOL] invalid free, pointer not from slab allocator: %p",
                    ptr);
        }
        return;
    }

    uint32_t cls = hdr->cls;

    if (cls == MP_CLASS_LARGE) {
        hdr->magic = MP_MAGIC_FREE;
        mp_stat_inc(&mp_free_total);
        free(hdr);
        return;
    }

    if (cls >= MP_CLASS_COUNT) {
        kvs_log(KVS_LOG_ERROR, "[MEMPOOL] corrupted block header: ptr=%p cls=%u", ptr, cls);
        return;
    }

    hdr->magic = MP_MAGIC_FREE;

    mp_bin_t* bin = &mp_bins[cls];
    hdr->next = bin->free_list;
    bin->free_list = hdr;
    bin->free_count++;
    mp_stat_inc(&mp_free_total);
}

// 打印分配器统计信息。空闲块数只统计"当前线程"的空闲链表（其他线程的链表
// 无法跨线程访问），chunk 数与分配计数则是所有线程共享的。
void slab_stats(void) {
    size_t free_blocks = 0;
    for (int i = 0; i < MP_CLASS_COUNT; i++) {
        free_blocks += mp_bins[i].free_count;
    }

    pthread_mutex_lock(&mp_chunk_lock);
    size_t chunks = mp_chunk_count;
    size_t blocks_total = mp_block_total;
    pthread_mutex_unlock(&mp_chunk_lock);

    printf("=== Slab Allocator Stats ===\n");
    printf("chunks=%zu blocks_total=%zu blocks_free_in_this_thread=%zu\n", chunks, blocks_total,
           free_blocks);
    printf("alloc=%zu free=%zu large_alloc=%zu\n", mp_stat_load(&mp_alloc_total),
           mp_stat_load(&mp_free_total), mp_stat_load(&mp_large_total));
    for (int i = 0; i < MP_CLASS_COUNT; i++) {
        printf("Slab[%4d bytes]: free=%zu\n", mp_class_sizes[i], mp_bins[i].free_count);
    }
}
