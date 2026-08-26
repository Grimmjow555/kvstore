#pragma once

#define ENABLE_SKIPTABLE 1

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
