#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_io_uring.h"
#include "kvs_rbtree.h"
#include "kvs_skiptable.h"
#include "kvs_snapshot.h"
#include "kvstore.h"
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

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

typedef struct {
    const unsigned char* base;
    size_t size;
    size_t offset;
} kvs_mmap_reader_t;

static int kvs_write_exact(std::vector<char>* fp, const void* buf, size_t len) {
    if (fp == NULL || buf == NULL) {
        return len == 0 ? 0 : -1;
    }
    if (len == 0) {
        return 0;
    }

    const char* bytes = (const char*)buf;
    fp->insert(fp->end(), bytes, bytes + len);
    return 0;
}

static int kvs_mmap_read_exact(kvs_mmap_reader_t* reader, void* buf, size_t len) {
    if (reader == NULL || (buf == NULL && len != 0)) {
        return -1;
    }
    if (reader->offset > reader->size || len > reader->size - reader->offset) {
        return -1;
    }

    if (len > 0) {
        memcpy(buf, reader->base + reader->offset, len);
        reader->offset += len;
    }
    return 0;
}

static int kvs_write_record(std::vector<char>* fp, const char* key, const char* value) {
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

static int kvs_read_record(kvs_mmap_reader_t* reader, char** out_key, char** out_value) {
    uint32_t key_len = 0;
    uint32_t value_len = 0;
    char* key = nullptr;
    char* value = nullptr;

    if (kvs_mmap_read_exact(reader, &key_len, sizeof(key_len)) != 0)
        return -1;
    if (kvs_mmap_read_exact(reader, &value_len, sizeof(value_len)) != 0)
        return -1;

    if (key_len > 0) {
        key = (char*)malloc(key_len + 1);
        if (!key)
            return -1;
        if (kvs_mmap_read_exact(reader, key, key_len) != 0) {
            free(key);
            return -1;
        }
        key[key_len] = '\0';
    }

    if (value_len > 0) {
        value = (char*)malloc(value_len + 1);
        if (!value) {
            free(key);
            return -1;
        }
        if (kvs_mmap_read_exact(reader, value, value_len) != 0) {
            free(key);
            free(value);
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

static int kvs_save_array_section(std::vector<char>* fp) {
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

static int kvs_load_array_section(kvs_mmap_reader_t* reader, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        char* key = nullptr;
        char* value = nullptr;
        if (kvs_read_record(reader, &key, &value) != 0)
            return -1;
        if (key && value) {
            if (kvs_array_set(&global_array, key, value) != 0) {
                free(key);
                free(value);
                return -1;
            }
            free(key);
            free(value);
        } else {
            free(key);
            free(value);
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

static int kvs_write_rbtree_node(std::vector<char>* fp, kvs_rbtree_t* tree, rbtree_node* node) {
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

static int kvs_save_rbtree_section(std::vector<char>* fp) {
    uint32_t count = kvs_rbtree_count_node(&global_rbtree, global_rbtree.root);
    kvs_section_header_t section = {KVS_SNAPSHOT_TYPE_RBTREE, count};
    if (kvs_write_exact(fp, &section, sizeof(section)) != 0)
        return -1;
    return kvs_write_rbtree_node(fp, &global_rbtree, global_rbtree.root);
}

static int kvs_load_rbtree_section(kvs_mmap_reader_t* reader, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        char* key = nullptr;
        char* value = nullptr;
        if (kvs_read_record(reader, &key, &value) != 0)
            return -1;
        if (key && value) {
            if (kvs_rbtree_set(&global_rbtree, key, value) != 0) {
                free(key);
                free(value);
                return -1;
            }
            free(key);
            free(value);
        } else {
            free(key);
            free(value);
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

static int kvs_save_hash_section(std::vector<char>* fp) {
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

static int kvs_load_hash_section(kvs_mmap_reader_t* reader, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        char* key = nullptr;
        char* value = nullptr;
        if (kvs_read_record(reader, &key, &value) != 0)
            return -1;
        if (key && value) {
            if (kvs_hash_set(&global_hash, key, value) != 0) {
                free(key);
                free(value);
                return -1;
            }
            free(key);
            free(value);
        } else {
            free(key);
            free(value);
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

static int kvs_save_skiptable_section(std::vector<char>* fp) {
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

static int kvs_load_skiptable_section(kvs_mmap_reader_t* reader, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        char* key = nullptr;
        char* value = nullptr;
        if (kvs_read_record(reader, &key, &value) != 0)
            return -1;
        if (key && value) {
            if (kvs_skiptable_set(&global_skiptable, key, value) != 0) {
                free(key);
                free(value);
                return -1;
            }
            free(key);
            free(value);
        } else {
            free(key);
            free(value);
            return -1;
        }
    }
    return 0;
}
#endif

static int kvs_snapshot_build(std::vector<char>* buffer) {
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

    // 先完整序列化到内存，避免对持久化文件执行大量小写入和多次 io_uring 提交。
    buffer->reserve(1024 * 1024);

    if (kvs_write_exact(buffer, &header, sizeof(header)) != 0) {
        return -1;
    }

#if ENABLE_ARRAY
    if (kvs_save_array_section(buffer) != 0) {
        return -1;
    }
#endif
#if ENABLE_RBTREE
    if (kvs_save_rbtree_section(buffer) != 0) {
        return -1;
    }
#endif
#if ENABLE_HASH
    if (kvs_save_hash_section(buffer) != 0) {
        return -1;
    }
#endif
#if ENABLE_SKIPTABLE
    if (kvs_save_skiptable_section(buffer) != 0) {
        return -1;
    }
#endif

    return 0;
}

int kvs_snapshot_serialize(char** out_data, size_t* out_len) {
    if (out_data == NULL || out_len == NULL) {
        return -1;
    }

    *out_data = NULL;
    *out_len = 0;

    std::vector<char> buffer;
    if (kvs_snapshot_build(&buffer) != 0) {
        return -1;
    }

    if (buffer.empty()) {
        return -1;
    }

    char* data = (char*)malloc(buffer.size());
    if (data == NULL) {
        return -1;
    }
    memcpy(data, buffer.data(), buffer.size());

    *out_data = data;
    *out_len = buffer.size();
    return 0;
}

int kvs_snapshot_save(const char* filename) {
    std::vector<char> buffer;
    if (kvs_snapshot_build(&buffer) != 0) {
        return -1;
    }

    kvs_io_uring_file_t* fp = kvs_io_uring_open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fp == NULL) {
        return -1;
    }

    int ret = kvs_io_uring_write_and_fsync(fp, buffer.data(), buffer.size());
    kvs_io_uring_close(fp);
    return ret;
}

// 解析内存中的 RDB 格式快照。成功时会先重置全部引擎数据。
static int kvs_snapshot_parse(const void* mapped, size_t mapped_size) {
    kvs_mmap_reader_t reader;
    reader.base = (const unsigned char*)mapped;
    reader.size = mapped_size;
    reader.offset = 0;

    kvs_file_header_t header;
    memset(&header, 0, sizeof(header));
    if (kvs_mmap_read_exact(&reader, &header, sizeof(header)) != 0) {
        return -1;
    }

    if (memcmp(header.magic, KVS_FILE_MAGIC, sizeof(header.magic)) != 0 ||
        header.version != KVS_FILE_VERSION) {
        return -1;
    }

    kvs_reset_data();

    for (uint32_t i = 0; i < header.count; ++i) {
        kvs_section_header_t section;
        if (kvs_mmap_read_exact(&reader, &section, sizeof(section)) != 0) {
            return -1;
        }

        switch (section.type) {
#if ENABLE_ARRAY
        case KVS_SNAPSHOT_TYPE_ARRAY:
            if (kvs_load_array_section(&reader, section.count) != 0) {
                return -1;
            }
            break;
#endif
#if ENABLE_RBTREE
        case KVS_SNAPSHOT_TYPE_RBTREE:
            if (kvs_load_rbtree_section(&reader, section.count) != 0) {
                return -1;
            }
            break;
#endif
#if ENABLE_HASH
        case KVS_SNAPSHOT_TYPE_HASH:
            if (kvs_load_hash_section(&reader, section.count) != 0) {
                return -1;
            }
            break;
#endif
#if ENABLE_SKIPTABLE
        case KVS_SNAPSHOT_TYPE_SKIPTABLE:
            if (kvs_load_skiptable_section(&reader, section.count) != 0) {
                return -1;
            }
            break;
#endif
        default:
            return -1;
        }
    }

    return 0;
}

int kvs_snapshot_load_buffer(const void* data, size_t len) {
    if (data == NULL || len == 0) {
        return -1;
    }
    return kvs_snapshot_parse(data, len);
}

// 使用 mmap 将快照文件映射为只读内存，再按游标顺序解析，避免通过 FILE* 逐段读取。
int kvs_snapshot_load(const char* filename) {
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size <= 0) {
        close(fd);
        return -1;
    }

    void* mapped = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        return -1;
    }
    close(fd);

    int ret = kvs_snapshot_parse(mapped, (size_t)st.st_size);
    munmap(mapped, (size_t)st.st_size);
    return ret;
}
