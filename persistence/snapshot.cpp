#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_skiptable.h"
#include "kvs_snapshot.h"
#include "kvstore.h"
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

extern kvs_array_t global_array;
extern kvs_rbtree_t global_rbtree;
extern kvs_hash_t global_hash;
extern kvs_skiptable_t global_skiptable;

#define KVS_FILE_MAGIC "KVSDB01"
#define KVS_FILE_VERSION 1
#define KVS_SNAPSHOT_PATH "../data/kvstore.data"

int init_kvengine();
int destroy_kvengine();

typedef struct { //文件头
    char magic[8];
    uint32_t version;
    uint32_t count; //记录存储的份数
} kvs_file_header_t;

typedef struct { //区块头
    uint32_t type;
    uint32_t count;
} kvs_section_header_t;

enum {
    KVS_SNAPSHOT_TYPE_ARRAY = 1,
    KVS_SNAPSHOT_TYPE_RBTREE = 2,
    KVS_SNAPSHOT_TYPE_HASH = 3,
    KVS_SNAPSHOT_TYPE_SKIPTABLE = 4,
};

static int kvs_write_exact(FILE* fp, const void* buf, size_t len) {
    return fwrite(buf, 1, len, fp) == len ? 0 : -1;
}

static int kvs_read_exact(FILE* fp, void* buf, size_t len) {
    return fread(buf, 1, len, fp) == len ? 0 : -1;
}

static int kvs_write_record(FILE* fp, const char* key, const char* value) {
    uint32_t key_len = key ? (uint32_t)strlen(key) : 0;
    uint32_t value_len = value ? (uint32_t)strlen(value) : 0;

    if (kvs_write_exact(fp, &key_len, sizeof(key_len)) != 0)
        return -1;
    if (kvs_write_exact(fp, &value_len, sizeof(value_len)) != 0)
        return -1;
    if (key_len > 0 && kvs_write_exact(fp, key, key_len) != 0)
        return -1;
    if (value_len > 0 && kvs_write_exact(fp, value, value_len) != 0)
        return -1;
    return 0;
}

static int kvs_read_record(FILE* fp, char** out_key, char** out_value) {
    uint32_t key_len = 0;
    uint32_t value_len = 0;
    char* key = nullptr;
    char* value = nullptr;

    if (kvs_read_exact(fp, &key_len, sizeof(key_len)) != 0)
        return -1;
    if (kvs_read_exact(fp, &value_len, sizeof(value_len)) != 0)
        return -1;

    if (key_len > 0) {
        key = (char*)kvs_malloc(key_len + 1);
        if (!key)
            return -1;
        if (kvs_read_exact(fp, key, key_len) != 0) {
            kvs_free(key);
            return -1;
        }
        key[key_len] = '\0';
    }

    if (value_len > 0) {
        value = (char*)kvs_malloc(value_len + 1);
        if (!value) {
            kvs_free(key);
            return -1;
        }
        if (kvs_read_exact(fp, value, value_len) != 0) {
            kvs_free(key);
            kvs_free(value);
            return -1;
        }
        value[value_len] = '\0';
    }

    *out_key = key;
    *out_value = value;
    return 0;
}

#if ENABLE_ARRAY
static uint32_t kvs_array_count(const kvs_array_t* array) {
    if (!array)
        return 0;
    return (uint32_t)array->total;
}

static int kvs_save_array_section(FILE* fp) {
    uint32_t count = kvs_array_count(&global_array);
    kvs_section_header_t section = {KVS_SNAPSHOT_TYPE_ARRAY, count};
    if (kvs_write_exact(fp, &section, sizeof(section)) != 0)
        return -1;

    for (int i = 0; i < global_array.max_idx; ++i) {
        if (global_array.table[i].key == nullptr || global_array.table[i].value == nullptr)
            continue;
        if (kvs_write_record(fp, global_array.table[i].key, global_array.table[i].value) != 0)
            return -1;
    }
    return 0;
}

static int kvs_load_array_section(FILE* fp, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        char* key = nullptr;
        char* value = nullptr;
        if (kvs_read_record(fp, &key, &value) != 0)
            return -1;
        if (key && value) {
            if (kvs_array_set(&global_array, key, value) != 0) {
                kvs_free(key);
                kvs_free(value);
                return -1;
            }
            kvs_free(key);
            kvs_free(value);
        } else {
            kvs_free(key);
            kvs_free(value);
            return -1;
        }
    }
    return 0;
}
#endif

#if ENABLE_RBTREE
static uint32_t kvs_rbtree_count_node(kvs_rbtree_t* tree, rbtree_node* node) {
    if (node == tree->nil) {
        return 0;
    }
    return 1 + kvs_rbtree_count_node(tree, node->left) + kvs_rbtree_count_node(tree, node->right);
}

static int kvs_write_rbtree_node(FILE* fp, kvs_rbtree_t* tree, rbtree_node* node) {
    if (node == tree->nil)
        return 0;

    if (kvs_write_rbtree_node(fp, tree, node->left) != 0)
        return -1;
    if (node->key && node->value && kvs_write_record(fp, (char*)node->key, (char*)node->value) != 0)
        return -1;
    if (kvs_write_rbtree_node(fp, tree, node->right) != 0)
        return -1;

    return 0;
}

static int kvs_save_rbtree_section(FILE* fp) {
    uint32_t count = kvs_rbtree_count_node(&global_rbtree, global_rbtree.root);
    kvs_section_header_t section = {KVS_SNAPSHOT_TYPE_RBTREE, count};
    if (kvs_write_exact(fp, &section, sizeof(section)) != 0)
        return -1;
    return kvs_write_rbtree_node(fp, &global_rbtree, global_rbtree.root);
}

static int kvs_load_rbtree_section(FILE* fp, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        char* key = nullptr;
        char* value = nullptr;
        if (kvs_read_record(fp, &key, &value) != 0)
            return -1;
        if (key && value) {
            if (kvs_rbtree_set(&global_rbtree, key, value) != 0) {
                kvs_free(key);
                kvs_free(value);
                return -1;
            }
            kvs_free(key);
            kvs_free(value);
        } else {
            kvs_free(key);
            kvs_free(value);
            return -1;
        }
    }
    return 0;
}
#endif

#if ENABLE_HASH
static uint32_t kvs_hash_count(const kvs_hash_t* hash) {
    if (!hash)
        return 0;
    uint32_t count = 0;
    for (int i = 0; i < hash->max_slots; ++i) {
        hashnode_t* node = hash->nodes[i];
        while (node) {
            ++count;
            node = node->next;
        }
    }
    return count;
}

static int kvs_save_hash_section(FILE* fp) {
    uint32_t count = kvs_hash_count(&global_hash);
    kvs_section_header_t section = {KVS_SNAPSHOT_TYPE_HASH, count};
    if (kvs_write_exact(fp, &section, sizeof(section)) != 0)
        return -1;

    for (int i = 0; i < global_hash.max_slots; ++i) {
        hashnode_t* node = global_hash.nodes[i];
        while (node) {
            if (node->key == nullptr || node->value == nullptr)
                return -1;
            if (kvs_write_record(fp, node->key, node->value) != 0)
                return -1;
            node = node->next;
        }
    }
    return 0;
}

static int kvs_load_hash_section(FILE* fp, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        char* key = nullptr;
        char* value = nullptr;
        if (kvs_read_record(fp, &key, &value) != 0)
            return -1;
        if (key && value) {
            if (kvs_hash_set(&global_hash, key, value) != 0) {
                kvs_free(key);
                kvs_free(value);
                return -1;
            }
            kvs_free(key);
            kvs_free(value);
        } else {
            kvs_free(key);
            kvs_free(value);
            return -1;
        }
    }
    return 0;
}
#endif

#if ENABLE_SKIPTABLE
static uint32_t kvs_skiptable_count(const kvs_skiptable_t* table) {
    if (table == NULL || table->header == NULL)
        return 0;
    uint32_t count = 0;
    Node* node = table->header->forward[0];
    while (node != NULL) {
        ++count;
        node = node->forward[0];
    }
    return count;
}

static int kvs_save_skiptable_section(FILE* fp) {
    uint32_t count = kvs_skiptable_count(&global_skiptable);
    kvs_section_header_t section = {KVS_SNAPSHOT_TYPE_SKIPTABLE, count};
    if (kvs_write_exact(fp, &section, sizeof(section)) != 0)
        return -1;

    Node* node = global_skiptable.header->forward[0];
    while (node != NULL) {
        if (node->key == nullptr || node->value == nullptr)
            return -1;
        if (kvs_write_record(fp, node->key, node->value) != 0)
            return -1;
        node = node->forward[0];
    }
    return 0;
}

static int kvs_load_skiptable_section(FILE* fp, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        char* key = nullptr;
        char* value = nullptr;
        if (kvs_read_record(fp, &key, &value) != 0)
            return -1;
        if (key && value) {
            if (kvs_skiptable_set(&global_skiptable, key, value) != 0) {
                kvs_free(key);
                kvs_free(value);
                return -1;
            }
            kvs_free(key);
            kvs_free(value);
        } else {
            kvs_free(key);
            kvs_free(value);
            return -1;
        }
    }
    return 0;
}
#endif

int kvs_snapshot_save(const char* filename) {
    FILE* fp = fopen(filename, "wb");
    if (!fp)
        return -1;

    uint32_t section_count = 0;
#if ENABLE_ARRAY
    ++section_count;
#endif
#if ENABLE_RBTREE
    ++section_count;
#endif
#if ENABLE_HASH
    ++section_count;
#endif
#if ENABLE_SKIPTABLE
    ++section_count;
#endif

    kvs_file_header_t header;
    memset(&header, 0, sizeof(header));
    memcpy(header.magic, KVS_FILE_MAGIC, sizeof(header.magic));
    header.version = KVS_FILE_VERSION;
    header.count = section_count;

    if (kvs_write_exact(fp, &header, sizeof(header)) != 0) {
        fclose(fp);
        return -1;
    }

#if ENABLE_ARRAY
    if (kvs_save_array_section(fp) != 0) {
        fclose(fp);
        return -1;
    }
#endif
#if ENABLE_RBTREE
    if (kvs_save_rbtree_section(fp) != 0) {
        fclose(fp);
        return -1;
    }
#endif
#if ENABLE_HASH
    if (kvs_save_hash_section(fp) != 0) {
        fclose(fp);
        return -1;
    }
#endif
#if ENABLE_SKIPTABLE
    if (kvs_save_skiptable_section(fp) != 0) {
        fclose(fp);
        return -1;
    }
#endif

    fclose(fp);
    return 0;
}

int kvs_snapshot_load(const char* filename) {
    FILE* fp = fopen(filename, "rb");
    if (!fp)
        return -1;

    kvs_file_header_t header;
    memset(&header, 0, sizeof(header));
    if (kvs_read_exact(fp, &header, sizeof(header)) != 0) {
        fclose(fp);
        return -1;
    }

    if (memcmp(header.magic, KVS_FILE_MAGIC, sizeof(header.magic)) != 0 ||
        header.version != KVS_FILE_VERSION) {
        fclose(fp);
        return -1;
    }

    kvs_reset_data();

    for (uint32_t i = 0; i < header.count; ++i) {
        kvs_section_header_t section;
        if (kvs_read_exact(fp, &section, sizeof(section)) != 0) {
            fclose(fp);
            return -1;
        }

        switch (section.type) {
#if ENABLE_ARRAY
        case KVS_SNAPSHOT_TYPE_ARRAY:
            if (kvs_load_array_section(fp, section.count) != 0) {
                fclose(fp);
                return -1;
            }
            break;
#endif
#if ENABLE_RBTREE
        case KVS_SNAPSHOT_TYPE_RBTREE:
            if (kvs_load_rbtree_section(fp, section.count) != 0) {
                fclose(fp);
                return -1;
            }
            break;
#endif
#if ENABLE_HASH
        case KVS_SNAPSHOT_TYPE_HASH:
            if (kvs_load_hash_section(fp, section.count) != 0) {
                fclose(fp);
                return -1;
            }
            break;
#endif
#if ENABLE_SKIPTABLE
        case KVS_SNAPSHOT_TYPE_SKIPTABLE:
            if (kvs_load_skiptable_section(fp, section.count) != 0) {
                fclose(fp);
                return -1;
            }
            break;
#endif
        default:
            fclose(fp);
            return -1;
        }
    }

    fclose(fp);
    return 0;
}
