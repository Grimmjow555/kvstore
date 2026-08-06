#pragma once
#include <cstring>
#include <iostream>
#include <vector>
#define KVS_MAX_TOKENS 128

#define ENABLE_ARRAY 1
#define ENABLE_RBTREE 1

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
void kvs_array_destroy(kvs_array_t* inst);

char* kvs_array_get(kvs_array_t* inst, char* key);
int kvs_array_set(kvs_array_t* inst, char* key, char* value);
int kvs_array_del(kvs_array_t* inst, char* key);
int kvs_array_mod(kvs_array_t* inst, char* key, char* value);
int kvs_array_exist(kvs_array_t* inst, char* key);

#endif

#if ENABLE_RBTREE

#define RED 1
#define BLACK 2
#define ENABLE_KEY_CHAR 1 // 原样例的 key 为 int 型，这里改为char*

#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else
typedef int KEY_TYPE;
#endif

typedef struct _rbtree_node {
    unsigned char color;
    struct _rbtree_node* right;
    struct _rbtree_node* left;
    struct _rbtree_node* parent;
    KEY_TYPE key;
    void* value;
} rbtree_node;

typedef struct _rbtree {
    rbtree_node* root;
    rbtree_node* nil;
} rbtree;

typedef struct _rbtree kvs_rbtree_t;

int kvs_rbtree_create(kvs_rbtree_t* inst);
void kvs_rbtree_destroy(kvs_rbtree_t* inst);
char* kvs_rbtree_get(kvs_rbtree_t* inst, char* key);
int kvs_rbtree_set(kvs_rbtree_t* inst, char* key, char* value);
int kvs_rbtree_del(kvs_rbtree_t* inst, char* key);
int kvs_rbtree_mod(kvs_rbtree_t* inst, char* key, char* value);
int kvs_rbtree_exist(kvs_rbtree_t* inst, char* key);

#endif