#pragma once

#include <stdlib.h>

#define ENABLE_MEMORYPOOL 1

// 内存池结构体
typedef struct mempool_s {
    int block_size;
    int free_count;
    int total_count;
    char* free_ptr;
    char* mem;
} mempool_t;

#if ENABLE_MEMORYPOOL

// 单池 API
int mp_init(mempool_t* m, int size);
void mp_dest(mempool_t* m);
void* mp_alloc(mempool_t* m);
void mp_free(mempool_t* m, void* ptr);

// Slab 分配器 API
int slab_init(void);
void slab_free_ptr(void* ptr);
void slab_dest(void);
void* slab_alloc(size_t size);
void slab_free(void* ptr, size_t size);
void slab_stats(void);

#endif