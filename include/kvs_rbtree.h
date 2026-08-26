#pragma once

#define ENABLE_RBTREE 1

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
