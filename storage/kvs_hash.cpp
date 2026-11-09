#include "kvs_hash.h"
#include "kvs_runtime.h"
#include <climits>
#include <cstring>

// Key, Value -->
// Modify

kvs_hash_t global_hash = {0};

// 32 位 FNV-1a：避免字符和哈希把 TeacherN 这类连续键挤进少量桶。
static uint32_t _hash_key(const char* key) {
    uint32_t hash = 2166136261u;
    while (*key != '\0') {
        hash ^= (uint8_t)*key++;
        hash *= 16777619u;
    }
    return hash;
}

static int _bucket_index(uint32_t hash, int slot_count) {
    return (int)(hash & (uint32_t)(slot_count - 1));
}

static void _free_node(hashnode_t* node) {
    if (!node) {
        return;
    }
#if ENABLE_KEY_POINTER
    kvs_free(node->key);
    kvs_free(node->value);
#endif
    kvs_free(node);
}

hashnode_t* _create_node(char* key, char* value, uint32_t hash) {

    hashnode_t* node = (hashnode_t*)kvs_malloc(sizeof(hashnode_t));
    if (!node)
        return NULL;

#if ENABLE_KEY_POINTER
    char* kcopy = (char*)kvs_malloc(strlen(key) + 1);
    if (kcopy == NULL) {
        kvs_free(node);
        return NULL;
    }
    memset(kcopy, 0, strlen(key) + 1);
    strncpy(kcopy, key, strlen(key));

    node->key = kcopy;

    char* kvalue = (char*)kvs_malloc(strlen(value) + 1);
    if (kvalue == NULL) {
        kvs_free(kcopy);
        kvs_free(node);
        return NULL;
    }
    memset(kvalue, 0, strlen(value) + 1);
    strncpy(kvalue, value, strlen(value));

    node->value = kvalue;

#else
    strncpy(node->key, key, MAX_KEY_LEN);
    strncpy(node->value, value, MAX_VALUE_LEN);
#endif
    node->hash = hash;
    node->next = NULL;

    return node;
}

static int _grow_table(kvs_hash_t* hash) {
    if (!hash || !hash->nodes || hash->max_slots <= 0 ||
        hash->max_slots > INT_MAX / 2) {
        return -1;
    }

    int new_slots = hash->max_slots * 2;
    size_t allocation_size = sizeof(hashnode_t*) * (size_t)new_slots;
    hashnode_t** new_nodes = (hashnode_t**)kvs_malloc(allocation_size);
    if (!new_nodes) {
        return -1;
    }
    memset(new_nodes, 0, allocation_size);

    // 先完整构建新桶数组；失败时保留旧数组，扩容对所有已有键保持原子可见。
    for (int i = 0; i < hash->max_slots; ++i) {
        hashnode_t* node = hash->nodes[i];
        while (node) {
            hashnode_t* next = node->next;
            int new_idx = _bucket_index(node->hash, new_slots);
            node->next = new_nodes[new_idx];
            new_nodes[new_idx] = node;
            node = next;
        }
    }

    kvs_free(hash->nodes);
    hash->nodes = new_nodes;
    hash->max_slots = new_slots;
    return 0;
}

//
int kvs_hash_create(kvs_hash_t* hash) {

    if (!hash)
        return -1;

    hash->nodes = (hashnode_t**)kvs_malloc(
        sizeof(hashnode_t*) * KVS_HASH_INITIAL_TABLE_SIZE);
    if (!hash->nodes)
        return -1;

    // nodes 数组中的槽位必须初始化为 NULL，否则销毁时会遍历到未初始化指针，
    // 二次快照加载可能触发 double free 或内存破坏。
    memset(hash->nodes, 0, sizeof(hashnode_t*) * KVS_HASH_INITIAL_TABLE_SIZE);

    hash->max_slots = KVS_HASH_INITIAL_TABLE_SIZE;
    hash->count = 0;

    return 0;
}

//
void kvs_hash_destroy(kvs_hash_t* hash) {

    if (!hash)
        return;

    int i = 0;
    for (i = 0; i < hash->max_slots; i++) {
        hashnode_t* node = hash->nodes[i];

        while (node != NULL) {
            hashnode_t* tmp = node;
            node = node->next;

            _free_node(tmp);
        }
        hash->nodes[i] = NULL;
    }

    if (hash->nodes) {
        kvs_free(hash->nodes);
        hash->nodes = NULL;
    }
    hash->max_slots = 0;
    hash->count = 0;
}

// 5 + 2

// mp
int kvs_hash_set(kvs_hash_t* hash, char* key, char* value) {

    if (!hash || !key || !value)
        return -1;

    uint32_t hash_value = _hash_key(key);
    int idx = _bucket_index(hash_value, hash->max_slots);

    hashnode_t* node = hash->nodes[idx];
#if 1
    while (node != NULL) {
        if (node->hash == hash_value && strcmp(node->key, key) == 0) { // exist
            return 1;
        }
        node = node->next;
    }
#endif

    hashnode_t* new_node = _create_node(key, value, hash_value);
    if (!new_node) {
        return -1;
    }

    if (((long long)hash->count + 1) * KVS_HASH_MAX_LOAD_DENOMINATOR >
        (long long)hash->max_slots * KVS_HASH_MAX_LOAD_NUMERATOR) {
        if (_grow_table(hash) != 0) {
            _free_node(new_node);
            return -1;
        }
        idx = _bucket_index(hash_value, hash->max_slots);
    }

    new_node->next = hash->nodes[idx];
    hash->nodes[idx] = new_node;

    hash->count++;

    return 0;
}

char* kvs_hash_get(kvs_hash_t* hash, char* key) {

    if (!hash || !key)
        return NULL;

    uint32_t hash_value = _hash_key(key);
    int idx = _bucket_index(hash_value, hash->max_slots);

    hashnode_t* node = hash->nodes[idx];

    while (node != NULL) {

        if (node->hash == hash_value && strcmp(node->key, key) == 0) {
            return node->value;
        }

        node = node->next;
    }

    return NULL;
}

int kvs_hash_mod(kvs_hash_t* hash, char* key, char* value) {

    if (!hash || !key)
        return -1;

    uint32_t hash_value = _hash_key(key);
    int idx = _bucket_index(hash_value, hash->max_slots);

    hashnode_t* node = hash->nodes[idx];

    while (node != NULL) {

        if (node->hash == hash_value && strcmp(node->key, key) == 0) {
            break;
        }

        node = node->next;
    }

    if (node == NULL) {
        return 1;
    }

    // node -->
    kvs_free(node->value);

    char* kvalue = (char*)kvs_malloc(strlen(value) + 1);
    if (kvalue == NULL)
        return -2;
    memset(kvalue, 0, strlen(value) + 1);
    strncpy(kvalue, value, strlen(value));

    node->value = kvalue;

    return 0;
}

int kvs_hash_count(kvs_hash_t* hash) { return hash->count; }

int kvs_hash_del(kvs_hash_t* hash, char* key) {
    if (!hash || !key)
        return -2;

    uint32_t hash_value = _hash_key(key);
    int idx = _bucket_index(hash_value, hash->max_slots);

    hashnode_t* head = hash->nodes[idx];
    if (head == NULL)
        return -1; // noexist
    // head node
    if (head->hash == hash_value && strcmp(head->key, key) == 0) {
        hashnode_t* tmp = head->next;
        hash->nodes[idx] = tmp;

        _free_node(head);
        hash->count--;

        return 0;
    }

    hashnode_t* cur = head;
    while (cur->next != NULL) {
        if (cur->next->hash == hash_value && strcmp(cur->next->key, key) == 0)
            break; // search node

        cur = cur->next;
    }

    if (cur->next == NULL) {

        return -1;
    }

    hashnode_t* tmp = cur->next;
    cur->next = tmp->next;
    _free_node(tmp);

    hash->count--;

    return 0;
}

int kvs_hash_exist(kvs_hash_t* hash, char* key) {
    char* value = kvs_hash_get(hash, key);
    if (!value)
        return 1;

    return 0;
}

#if 0
void* kvs_malloc(size_t size) { return malloc(size); }

void kvs_free(void* ptr) { return free(ptr); }
int main() {
    kvs_hash_t hash;

    kvs_hash_create(&hash);

    kvs_hash_set(&hash, "Teacher1", "King");
    kvs_hash_set(&hash, "Teacher2", "Darren");
    kvs_hash_set(&hash, "Teacher3", "Mark");
    kvs_hash_set(&hash, "Teacher4", "Vico");
    kvs_hash_set(&hash, "Teacher5", "Nick");

    char* value1 = kvs_hash_get(&hash, "Teacher1");
    printf("Teacher1 : %s\n", value1);

    int ret = kvs_hash_mod(&hash, "Teacher1", "King1");
    printf("mode Teacher1 ret : %d\n", ret);

    char* value2 = kvs_hash_get(&hash, "Teacher1");
    printf("Teacher1 : %s\n", value1);

    ret = kvs_hash_del(&hash, "Teacher1");
    printf("delete Teacher1 ret : %d\n", ret);

    ret = kvs_hash_exist(&hash, "Teacher1");
    printf("Exist Teacher1 ret : %d\n", ret);

    kvs_hash_destroy(&hash);

    return 0;
}

#endif
