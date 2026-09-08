#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "memorypool.h"

#define MEM_PAGE_SIZE 4096

// 内存池结构体定义（移到头文件之前需要在这里定义，因为 kvstore.h 中引用了它）

// 初始化单个内存池
int mp_init(mempool_t* m, int size) {

    if (!m)
        return -1;
    if (size < 16)
        size = 16;

    m->block_size = size;

    m->mem = (char*)malloc(MEM_PAGE_SIZE);
    if (!m->mem)
        return -1;
    memset(m->mem, 0, MEM_PAGE_SIZE);

    m->free_ptr = m->mem;
    m->total_count = MEM_PAGE_SIZE / size;
    m->free_count = m->total_count;

    int i = 0;
    char* ptr = m->free_ptr;
    for (i = 0; i < m->free_count - 1; i++) {
        *(char**)ptr = ptr + size;
        ptr += size;
    }
    *(char**)ptr = NULL;

    return 0;
}

// 销毁单个内存池
void mp_dest(mempool_t* m) {
    if (!m || !m->mem)
        return;

    free(m->mem);
    m->mem = NULL;
    m->free_ptr = NULL;
    m->free_count = 0;
}

// 从单个内存池分配
void* mp_alloc(mempool_t* m) {

    if (!m || m->free_count == 0)
        return NULL;

    void* ptr = m->free_ptr;

    m->free_ptr = *(char**)ptr;
    m->free_count--;

    return ptr;
}

// 释放到单个内存池
void mp_free(mempool_t* m, void* ptr) {
    if (!m || !ptr)
        return;

    *(char**)ptr = m->free_ptr;
    m->free_ptr = (char*)ptr;
    m->free_count++;
}

// ============================================
// Slab 分配器：支持多种大小的内存分配
// ============================================

// Slab 大小等级（字节）
// 16, 32, 64, 128, 256, 512, 1024
#define SLAB_CLASS_COUNT 7

static int slab_sizes[SLAB_CLASS_COUNT] = {16, 32, 64, 128, 256, 512, 1024};
static mempool_t slab_pools[SLAB_CLASS_COUNT];
static int slab_initialized = 0;

// 根据请求大小找到合适的 slab class
static int get_slab_class(size_t size) {
    for (int i = 0; i < SLAB_CLASS_COUNT; i++) {
        if ((int)size <= slab_sizes[i]) {
            return i;
        }
    }
    return -1; // 超过最大 slab 大小
}

// 初始化 slab 分配器
int slab_init(void) {
    if (slab_initialized)
        return 0;

    for (int i = 0; i < SLAB_CLASS_COUNT; i++) {
        if (mp_init(&slab_pools[i], slab_sizes[i]) < 0) {
            // 回滚已初始化的池
            for (int j = 0; j < i; j++) {
                mp_dest(&slab_pools[j]);
            }
            return -1;
        }
    }

    slab_initialized = 1;
    return 0;
}

// 销毁 slab 分配器
void slab_dest(void) {
    if (!slab_initialized)
        return;

    for (int i = 0; i < SLAB_CLASS_COUNT; i++) {
        mp_dest(&slab_pools[i]);
    }

    slab_initialized = 0;
}

// 从 slab 分配器分配内存
void* slab_alloc(size_t size) {
    if (!slab_initialized) {
        slab_init();
    }

    int class_idx = get_slab_class(size);
    if (class_idx < 0) {
        // 超过最大 slab 大小，回退到 malloc
        return malloc(size);
    }

    void* ptr = mp_alloc(&slab_pools[class_idx]);
    if (ptr == NULL) {
        // 内存池耗尽，回退到 malloc
        return malloc(size);
    }

    return ptr;
}

// 释放内存到 slab 分配器
void slab_free(void* ptr, size_t size) {
    if (!ptr)
        return;

    if (!slab_initialized) {
        free(ptr);
        return;
    }

    int class_idx = get_slab_class(size);
    if (class_idx < 0) {
        // 大于最大 slab，使用 free
        free(ptr);
        return;
    }

    // 检查指针是否在 slab 内存范围内
    mempool_t* pool = &slab_pools[class_idx];
    if (ptr >= (void*)pool->mem && ptr < (void*)(pool->mem + MEM_PAGE_SIZE)) {
        mp_free(pool, ptr);
    } else {
        // 不在 slab 范围内，可能是 malloc 分配的
        free(ptr);
    }
}

// 获取 slab 分配器统计信息
void slab_stats(void) {
    if (!slab_initialized) {
        printf("Slab allocator not initialized\n");
        return;
    }

    printf("=== Slab Allocator Stats ===\n");
    for (int i = 0; i < SLAB_CLASS_COUNT; i++) {
        mempool_t* pool = &slab_pools[i];
        printf("Slab[%d bytes]: total=%d, free=%d, used=%d\n", slab_sizes[i], pool->total_count,
               pool->free_count, pool->total_count - pool->free_count);
    }
}

// 智能释放：无需 size 参数，自动判断释放方式
void slab_free_ptr(void* ptr) {
    if (!ptr)
        return;

    if (!slab_initialized) {
        free(ptr);
        return;
    }

    // 遍历所有 slab class，检查 ptr 是否在某个 pool 的内存范围内
    for (int i = 0; i < SLAB_CLASS_COUNT; i++) {
        mempool_t* pool = &slab_pools[i];
        // 检查 pool->mem 是否已分配，并且 ptr 是否在该页范围内
        // 注意: 这里假设每个 pool 只有一个 page (MEM_PAGE_SIZE)
        if (pool->mem && ptr >= (void*)pool->mem && ptr < (void*)(pool->mem + MEM_PAGE_SIZE)) {
            mp_free(pool, ptr);
            return;
        }
    }

    // 如果不在任何 slab pool 中，则说明是 malloc 分配的
    free(ptr);
}

#if 0
int main() {
    printf("Hello memorypool!\n");

    mempool_t m;
    slab_init();

    void* p1 = slab_alloc(32);
    printf("p1 mp_alloc: %p\n", p1);

    void* p2 = slab_alloc(32);
    printf("p2 mp_alloc: %p\n", p2);

    void* p3 = slab_alloc(32);
    printf("p3 mp_alloc: %p\n", p3);

    slab_free_ptr(p2);

    void* p4 = slab_alloc(32);
    printf("p4 mp_alloc: %p\n", p4);
}

#endif