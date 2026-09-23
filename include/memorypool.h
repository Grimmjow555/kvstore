#pragma once

#include <stddef.h>
#include <stdlib.h>

#define ENABLE_MEMORYPOOL 1

#if ENABLE_MEMORYPOOL

/*
 * Slab 内存分配器
 * ---------------------------------------------------------------------------
 * 结构：chunk = [chunk 头][对齐填充][block0][block1]...
 *       block = [16 字节块头][payload]，返回给调用方的是 payload 指针
 *
 * 三条核心约定：
 * 1. 线程本地空闲链表：每个线程有独立的一套 size class 空闲链表，分配/释放
 *    的慢快路径不加锁；只有某个 class 空闲链表为空、需要申请新 chunk 时才取
 *    一次全局锁。因此多线程可以并发使用，不需要调用方额外加锁。
 * 2. chunk 按需增长：不再是"每个 class 只有一页"，空闲链表为空就申请一个新的
 *    64KB chunk 并切块，适配真正的大数据量场景。
 * 3. 每个块带块头（magic + class）：释放时 O(1) 定位所属 size class，并能识别
 *    double free 与野指针，而不是把坏指针挂进空闲链表污染后续分配。
 *
 * 语义与使用约束：
 * - slab_alloc 返回的内存不保证清零，等价于 malloc；需要清零用 slab_calloc。
 * - 只能释放 slab_alloc / slab_calloc 返回的指针，且不要与 free() 混用。
 * - slab_dest() 会释放所有 chunk，必须确保此时没有其他线程仍在使用分配器
 *   （例如复制线程已经 join），因此它只在进程退出路径上调用。
 * - 已知限制：线程退出时其空闲链表（线程本地存储）也随之消失，链上的块要到
 *   进程退出才被回收。当前服务只有主线程与复制线程两个长生命周期线程，不受影响。
 */

// Slab 分配器 API
int slab_init(void);
void slab_dest(void);
void slab_reset(void);
void* slab_alloc(size_t size);
void* slab_calloc(size_t size);
void slab_free_ptr(void* ptr);
void slab_stats(void);

#endif
