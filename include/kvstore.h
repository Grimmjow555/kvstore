#pragma once
#include <iostream>

// 存储引擎全局读写锁。
//
// 复制模块引入后，同一进程内可能出现多个线程同时访问存储引擎：
//   - Master：事件循环线程写数据，RDMA 监听线程序列化全量快照；
//   - Replica：事件循环线程服务本地客户端，复制回放线程应用 Master 命令。
// 存储引擎自身不加锁，因此所有入口都必须通过下面两个函数串行化。
// 锁是可重入的：命令分发内部再调用快照/重置逻辑时不会自锁死。
void kvs_data_lock();
void kvs_data_unlock();

void* kvs_malloc(size_t size);

void* kvs_calloc(size_t size);

void kvs_free(void* ptr);

int kvs_reset_data();
