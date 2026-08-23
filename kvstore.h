#pragma once
#include <cstring>
#include <iostream>
#include <vector>
#define KVS_MAX_TOKENS 128

#define ENABLE_ARRAY 1
#define ENABLE_RBTREE 1
#define ENABLE_HASH 1
#define ENABLE_SKIPTABLE 1

void* kvs_malloc(size_t size);

void kvs_free(void* ptr);

#if ENABLE_ARRAY

struct kvs_array_item_t {
    char* key;
    char* value;
    //包含两个指针，共16字节，根据这两个指针可以找到两个字符串
};

#define KVS_ARRAY_SIZE 10240

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

int kvs_array_save(kvs_array_t* array, const char* filename);
int kvs_array_load(kvs_array_t* array, const char* filename);

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

int kvs_rbtree_save(kvs_rbtree_t* tree, const char* filename);
int kvs_rbtree_load(kvs_rbtree_t* tree, const char* filename);

#endif

#if ENABLE_HASH

#define MAX_KEY_LEN 128
#define MAX_VALUE_LEN 512
#define MAX_TABLE_SIZE 1024

#define ENABLE_KEY_POINTER 1

typedef struct hashnode_s {
#if ENABLE_KEY_POINTER
    char* key;
    char* value;
#else
    char key[MAX_KEY_LEN];
    char value[MAX_VALUE_LEN];
#endif
    struct hashnode_s* next;

} hashnode_t;

typedef struct hashtable_s {

    hashnode_t** nodes; //* change **,

    int max_slots;
    int count;

} hashtable_t;

typedef struct hashtable_s kvs_hash_t;

int kvs_hash_create(kvs_hash_t* hash);
void kvs_hash_destroy(kvs_hash_t* hash);

int kvs_hash_set(hashtable_t* hash, char* key, char* value);
char* kvs_hash_get(kvs_hash_t* hash, char* key);
int kvs_hash_mod(kvs_hash_t* hash, char* key, char* value);
int kvs_hash_del(kvs_hash_t* hash, char* key);
int kvs_hash_exist(kvs_hash_t* hash, char* key);

int kvs_hash_save(kvs_hash_t* hash, const char* filename);
int kvs_hash_load(kvs_hash_t* hash, const char* filename);

#endif

#if ENABLE_SKIPTABLE

#define HASH_ENABLE_CHAR_KV 1
#define MAX_LEVEL 6

typedef struct Node {
#if HASH_ENABLE_CHAR_KV
    char* key;
    char* value;
#else
    int key;
    int value;
#endif
    struct Node** forward;
} Node;

typedef struct SkipList {
    int level;
    Node* header;
} SkipList;

typedef struct SkipList kvs_skiptable_t;

int kvs_skiptable_create(kvs_skiptable_t* skiptable);
void kvs_skiptable_destroy(kvs_skiptable_t* skiptable);
int kvs_skiptable_set(kvs_skiptable_t* skiptable, char* key, char* value);
char* kvs_skiptable_get(kvs_skiptable_t* skiptable, char* key);
int kvs_skiptable_mod(kvs_skiptable_t* skiptable, char* key, char* value);
int kvs_skiptable_del(kvs_skiptable_t* skiptable, char* key);
int kvs_skiptable_exist(kvs_skiptable_t* skiptable, char* key);

int kvs_skiptable_save(kvs_skiptable_t* skiptable, const char* filename);
int kvs_skiptable_load(kvs_skiptable_t* skiptable, const char* filename);

#endif
