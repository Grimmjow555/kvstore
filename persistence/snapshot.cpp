#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_skiptable.h"
#include "kvstore.h"
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

typedef struct {
    char magic[8];
    uint32_t version;
    uint32_t count;
} kvs_file_header_t;

typedef struct {
    uint32_t type;
    uint32_t count;
} kvs_section_header_t;

typedef struct {
    uint32_t key_len;
    uint32_t value_len;
} kvs_record_header_t;

enum {
    KVS_SNAPSHOT_TYPE_ARRAY = 1,
    KVS_SNAPSHOT_TYPE_RBTREE = 2,
    KVS_SNAPSHOT_TYPE_HASH = 3,
    KVS_SNAPSHOT_TYPE_SKIPTABLE = 4,
};

#if ENABLE_RBTREE
static uint32_t kvs_rbtree_count_node(kvs_rbtree_t* tree, rbtree_node* node) {
    if (node == tree->nil) {
        return 0;
    }

    return 1 + kvs_rbtree_count_node(tree, node->left) + kvs_rbtree_count_node(tree, node->right);
}

#endif

#if ENABLE_SKIPTABLE

static uint32_t kvs_skiptable_count(kvs_skiptable_t* table) {
    if (table == NULL || table->header == NULL) {

        return 0;
    }

    uint32_t count = 0;

    Node* node = table->header->forward[0];

    while (node != NULL) {

        ++count;

        node = node->forward[0];
    }

    return count;
}

#endif

static int kvs_write_snapshot_record(FILE* fp, const char* key, const char* value) {
    if (fp == NULL || key == NULL || value == NULL) {
        return -1;
    }

    kvs_record_header_t record;
    record.key_len = (uint32_t)strlen(key);
    record.value_len = (uint32_t)strlen(value);

    if (record.key_len == 0 || record.key_len > MAX_KEY_LEN) {
        return -1;
    }
    if (record.value_len > MAX_VALUE_LEN) {
        return -1;
    }

    if (fwrite(&record, sizeof(record), 1, fp) != 1) {
        return -1;
    }
    if (fwrite(key, record.key_len, 1, fp) != 1) {
        return -1;
    }
    if (fwrite(value, record.value_len, 1, fp) != 1) {
        return -1;
    }

    return 0;
}

static int kvs_read_snapshot_record(FILE* fp, char** key_out, char** value_out) {
    kvs_record_header_t record;
    char* key = NULL;
    char* value = NULL;

    if (fp == NULL || key_out == NULL || value_out == NULL) {
        return -1;
    }

    if (fread(&record, sizeof(record), 1, fp) != 1) {
        return -1;
    }
    if (record.key_len == 0 || record.key_len > MAX_KEY_LEN) {
        return -1;
    }
    if (record.value_len > MAX_VALUE_LEN) {
        return -1;
    }

    key = (char*)kvs_malloc(record.key_len + 1);
    value = (char*)kvs_malloc(record.value_len + 1);
    if (key == NULL || value == NULL) {
        kvs_free(key);
        kvs_free(value);
        return -1;
    }

    if (fread(key, record.key_len, 1, fp) != 1 || fread(value, record.value_len, 1, fp) != 1) {
        kvs_free(key);
        kvs_free(value);
        return -1;
    }
    key[record.key_len] = '\0';
    value[record.value_len] = '\0';

    *key_out = key;
    *value_out = value;
    return 0;
}

static int kvs_snapshot_write_header(FILE* fp, uint32_t count) {
    kvs_file_header_t header;
    memset(&header, 0, sizeof(header));
    memcpy(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC));
    header.version = KVS_FILE_VERSION;
    header.count = count;
    return fwrite(&header, sizeof(header), 1, fp) == 1 ? 0 : -1;
}

static int kvs_snapshot_read_header(FILE* fp, uint32_t* count_out) {
    kvs_file_header_t header;
    if (fread(&header, sizeof(header), 1, fp) != 1) {
        return -1;
    }
    if (memcmp(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC)) != 0) {
        return -1;
    }
    if (header.version != KVS_FILE_VERSION) {
        return -1;
    }
    if (count_out != NULL) {
        *count_out = header.count;
    }
    return 0;
}

static int kvs_snapshot_write_section_header(FILE* fp, uint32_t type, uint32_t count) {
    kvs_section_header_t section;
    memset(&section, 0, sizeof(section));
    section.type = type;
    section.count = count;
    return fwrite(&section, sizeof(section), 1, fp) == 1 ? 0 : -1;
}

static int kvs_snapshot_read_section_header(FILE* fp, uint32_t* type_out, uint32_t* count_out) {
    kvs_section_header_t section;
    if (fread(&section, sizeof(section), 1, fp) != 1) {
        return -1;
    }
    if (type_out != NULL) {
        *type_out = section.type;
    }
    if (count_out != NULL) {
        *count_out = section.count;
    }
    return 0;
}

static int kvs_snapshot_save_array_records(FILE* fp) {
#if ENABLE_ARRAY
    kvs_snapshot_write_section_header(fp, KVS_SNAPSHOT_TYPE_ARRAY, (uint32_t)global_array.total);
    for (int i = 0; i < global_array.max_idx; ++i) {
        if (global_array.table[i].key == NULL || global_array.table[i].value == NULL) {
            continue;
        }
        if (kvs_write_snapshot_record(fp, global_array.table[i].key, global_array.table[i].value) !=
            0) {
            return -1;
        }
    }
#endif
    return 0;
}

static int kvs_snapshot_save_rbtree_records(FILE* fp, rbtree_node* node) {
#if ENABLE_RBTREE
    uint32_t count = kvs_rbtree_count_node(&global_rbtree, global_rbtree.root);
    if (kvs_snapshot_write_section_header(fp, KVS_SNAPSHOT_TYPE_RBTREE, count) != 0) {
        return -1;
    }
    if (node == NULL || node == global_rbtree.nil) {
        return 0;
    }
    if (kvs_snapshot_save_rbtree_records(fp, node->left) != 0) {
        return -1;
    }
    if (node->key == NULL || node->value == NULL) {
        return -1;
    }
    if (kvs_write_snapshot_record(fp, (const char*)node->key, (const char*)node->value) != 0) {
        return -1;
    }
    if (kvs_snapshot_save_rbtree_records(fp, node->right) != 0) {
        return -1;
    }
#endif
    return 0;
}

static int kvs_snapshot_save_hash_records(FILE* fp) {
#if ENABLE_HASH
    uint32_t count = (uint32_t)global_hash.count;
    if (kvs_snapshot_write_section_header(fp, KVS_SNAPSHOT_TYPE_HASH, count) != 0) {
        return -1;
    }
    for (int i = 0; i < global_hash.max_slots; ++i) {
        for (hashnode_t* node = global_hash.nodes[i]; node != NULL; node = node->next) {
            if (node->key == NULL || node->value == NULL) {
                return -1;
            }
            if (kvs_write_snapshot_record(fp, node->key, node->value) != 0) {
                return -1;
            }
        }
    }
#endif
    return 0;
}

static int kvs_snapshot_save_skiptable_records(FILE* fp) {
#if ENABLE_SKIPTABLE
    uint32_t count = kvs_skiptable_count(&global_skiptable);
    if (kvs_snapshot_write_section_header(fp, KVS_SNAPSHOT_TYPE_SKIPTABLE, count) != 0) {
        return -1;
    }
    for (Node* node = global_skiptable.header->forward[0]; node != NULL; node = node->forward[0]) {
        if (node->key == NULL || node->value == NULL) {
            return -1;
        }
        if (kvs_write_snapshot_record(fp, node->key, node->value) != 0) {
            return -1;
        }
    }
#endif
    return 0;
}

static int kvs_snapshot_save_all_records(FILE* fp) {
    if (kvs_snapshot_save_array_records(fp) != 0) {
        return -1;
    }
#if ENABLE_RBTREE
    if (kvs_snapshot_save_rbtree_records(fp, global_rbtree.root) != 0) {
        return -1;
    }
#endif
    if (kvs_snapshot_save_hash_records(fp) != 0) {
        return -1;
    }
    if (kvs_snapshot_save_skiptable_records(fp) != 0) {
        return -1;
    }
    return 0;
}

static int kvs_snapshot_load_section(FILE* fp, uint32_t type) {
    uint32_t section_count = 0;
    if (kvs_snapshot_read_section_header(fp, NULL, &section_count) != 0) {
        return -1;
    }

    for (uint32_t i = 0; i < section_count; ++i) {
        char* key = NULL;
        char* value = NULL;
        if (kvs_read_snapshot_record(fp, &key, &value) != 0) {
            return -1;
        }

        switch (type) {
#if ENABLE_ARRAY
        case KVS_SNAPSHOT_TYPE_ARRAY:
            if (kvs_array_set(&global_array, key, value) < 0) {
                kvs_free(key);
                kvs_free(value);
                return -1;
            }
            break;
#endif
#if ENABLE_RBTREE
        case KVS_SNAPSHOT_TYPE_RBTREE:
            if (kvs_rbtree_set(&global_rbtree, key, value) < 0) {
                kvs_free(key);
                kvs_free(value);
                return -1;
            }
            break;
#endif
#if ENABLE_HASH
        case KVS_SNAPSHOT_TYPE_HASH:
            if (kvs_hash_set(&global_hash, key, value) < 0) {
                kvs_free(key);
                kvs_free(value);
                return -1;
            }
            break;
#endif
#if ENABLE_SKIPTABLE
        case KVS_SNAPSHOT_TYPE_SKIPTABLE:
            if (kvs_skiptable_set(&global_skiptable, key, value) < 0) {
                kvs_free(key);
                kvs_free(value);
                return -1;
            }
            break;
#endif
        default:
            kvs_free(key);
            kvs_free(value);
            return -1;
        }

        kvs_free(key);
        kvs_free(value);
    }

    return 0;
}

static int kvs_snapshot_load_all_records(FILE* fp) {
    uint32_t count = 0;
    if (kvs_snapshot_read_header(fp, &count) != 0) {
        return -1;
    }

    uint32_t type = 0;
    while (1) {
        if (kvs_snapshot_read_section_header(fp, &type, NULL) != 0) {
            break;
        }
        if (kvs_snapshot_load_section(fp, type) != 0) {
            return -1;
        }
    }

    return 0;
}

int kvs_snapshot_save_all(const char* filename) {
    const char* path = filename != NULL ? filename : KVS_SNAPSHOT_PATH;
    FILE* fp = fopen(path, "wb");
    if (fp == NULL) {
        perror("fopen");
        return -1;
    }

    uint32_t total = 0;
#if ENABLE_ARRAY
    total += global_array.total;
#endif
#if ENABLE_RBTREE
    total += kvs_rbtree_count_node(&global_rbtree, global_rbtree.root);
#endif
#if ENABLE_HASH
    total += (uint32_t)global_hash.count;
#endif
#if ENABLE_SKIPTABLE
    total += kvs_skiptable_count(&global_skiptable);
#endif

    if (kvs_snapshot_write_header(fp, total) != 0) {
        fclose(fp);
        return -1;
    }
    if (kvs_snapshot_save_all_records(fp) != 0) {
        fclose(fp);
        return -1;
    }
    if (fflush(fp) != 0) {
        fclose(fp);
        return -1;
    }
    fclose(fp);
    return 0;
}

int kvs_snapshot_load_all(const char* filename) {
    const char* path = filename != NULL ? filename : KVS_SNAPSHOT_PATH;
    FILE* fp = fopen(path, "rb");
    if (fp == NULL) {
        if (errno == ENOENT) {
            return 0;
        }
        perror("fopen");
        return -1;
    }

    destroy_kvengine();
    init_kvengine();

    int ret = kvs_snapshot_load_all_records(fp);
    fclose(fp);
    if (ret != 0) {
        destroy_kvengine();
        init_kvengine();
    }
    return ret;
}
