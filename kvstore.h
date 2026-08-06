#pragma once
#include <cstring>
#include <iostream>
#include <vector>
#define KVS_MAX_TOKENS 128

#define ENABLE_ARRAY 1

// 选择网络框架
enum NETWORK_ARCHITECTURE { NETWORK_REACTOR = 0, NETWORK_NTYCO = 1, NETWORK_PROACTOR = 2 };

// 协议解析函数
typedef int (*msg_handler)(char* msg, int length, char* response);

/*--------------------------------------------------------------------*/
// reactor启动函数：监听的端口，解析协议
extern int reactor_start(unsigned short port, msg_handler handler);

#ifdef __cplusplus // ntyco_start是纯C编写的，用C++编译时需要以兼容C的模式处理
extern "C" {
#endif

// ntyco启动函数：监听的端口，解析协议
extern int ntyco_start(unsigned short port, msg_handler handler);

#ifdef __cplusplus
}
#endif

// uring启动函数：监听的端口，解析协议
extern int proactor_start(unsigned short port, msg_handler handler);
/*--------------------------------------------------------------------*/

void* kvs_malloc(size_t size);

void kvs_free(void* ptr);

#if ENABLE_ARRAY

struct kvs_array_item_t {
    char* key;
    char* value;
    //包含两个指针，共16字节，根据这两个指针可以找到两个字符串
};

#define KVS_ARRAY_SIZE 1024

struct kvs_array_t {
    kvs_array_item_t* table; //指针，同时也是数组的首地址

    // std::vector<kvs_array_item_t> table;

    int max_idx; //最大的索引+1，而非总数，因为删除数据不是连续的，允许中间删除
    int total; // 当前的总数
};

int kvs_array_create(kvs_array_t* inst);
void kvs_array_destory(kvs_array_t* inst);

char* kvs_array_get(kvs_array_t* inst, char* key);
int kvs_array_set(kvs_array_t* inst, char* key, char* value);
int kvs_array_del(kvs_array_t* inst, char* key);
int kvs_array_mod(kvs_array_t* inst, char* key, char* value);
int kvs_array_exist(kvs_array_t* inst, char* key);

#endif
