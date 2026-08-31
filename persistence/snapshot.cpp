#include "kvs_array.h"
#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_skiptable.h"
#include "kvstore.h"
#include <cstring>

#define KVS_FILE_MAGIC "KVSDB01"
#define KVS_FILE_VERSION 1

typedef struct {
    char magic[8];
    uint32_t version;
    uint32_t count;
} kvs_file_header_t;

typedef struct {
    uint32_t key_len;
    uint32_t value_len;
} kvs_record_header_t;

#if ENABLE_ARRAY
int kvs_array_save(kvs_array_t* array, const char* filename) {
    FILE* fp;
    kvs_file_header_t header;

    fp = fopen(filename, "wb");

    if (fp == NULL) {
        perror("fopen");
        return -1;
    }

    memset(&header, 0, sizeof(header));

    memcpy(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC));

    header.version = KVS_FILE_VERSION;
    header.count = array->total;

    fwrite(&header, sizeof(header), 1, fp);

    /*
     * 注意：
     *
     * max_idx 是最大的索引 + 1
     * total 是实际元素数量
     *
     * 所以必须遍历 max_idx
     */
    for (int i = 0; i < array->max_idx; i++) {

        kvs_array_item_t* item = &array->table[i];

        if (item->key == NULL) {
            continue;
        }

        kvs_record_header_t record;

        record.key_len = strlen(item->key);

        record.value_len = strlen(item->value);

        fwrite(&record, sizeof(record), 1, fp);

        fwrite(item->key, record.key_len, 1, fp);

        fwrite(item->value, record.value_len, 1, fp);
    }

    fclose(fp);

    return 0;
}

int kvs_array_load(kvs_array_t* array, const char* filename) {
    if (array == NULL || filename == NULL) {
        return -1;
    }

    FILE* fp = fopen(filename, "rb");
    if (fp == NULL) {
        if (errno == ENOENT) {
            return 0; // 首次启动，无文件
        }
        perror("fopen");
        return -1;
    }

    kvs_file_header_t header;
    if (fread(&header, sizeof(header), 1, fp) != 1) {
        fclose(fp);
        return -1;
    }

    if (memcmp(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC)) != 0) {
        fprintf(stderr, "invalid array file magic\n");
        fclose(fp);
        return -1;
    }

    if (header.version != KVS_FILE_VERSION) {
        fprintf(stderr, "unsupported array file version: %u\n", header.version);
        fclose(fp);
        return -1;
    }

    // 使用临时数组，全部加载成功后再替换旧数组
    kvs_array_t tmp_array;
    memset(&tmp_array, 0, sizeof(tmp_array));
    if (kvs_array_create(&tmp_array) != 0) { // 假设 create 返回 0 表示成功
        fclose(fp);
        return -1;
    }

    for (uint32_t i = 0; i < header.count; i++) {
        kvs_record_header_t record;
        if (fread(&record, sizeof(record), 1, fp) != 1) {
            kvs_array_destroy(&tmp_array);
            fclose(fp);
            return -1;
        }

        // 分配并读取 key
        char* key = (char*)kvs_malloc(record.key_len + 1);
        char* value = (char*)kvs_malloc(record.value_len + 1);
        if (key == NULL || value == NULL) {
            kvs_free(key);
            kvs_free(value);
            kvs_array_destroy(&tmp_array);
            fclose(fp);
            return -1;
        }

        if (fread(key, record.key_len, 1, fp) != 1 || fread(value, record.value_len, 1, fp) != 1) {
            kvs_free(key);
            kvs_free(value);
            kvs_array_destroy(&tmp_array);
            fclose(fp);
            return -1;
        }

        key[record.key_len] = '\0';
        value[record.value_len] = '\0';

        int ret = kvs_array_set(&tmp_array, key, value);

        if (ret != 0) {
            fprintf(stderr, "kvs_array_set failed during load (key=%s)\n", key);
            kvs_free(key);
            kvs_free(value);
            kvs_array_destroy(&tmp_array);
            fclose(fp);
            return -1;
        }
        kvs_free(key);
        kvs_free(value);
    }

    fclose(fp);

    // 成功：替换旧数组
    if (array->table != NULL) {
        kvs_array_destroy(array);
    }
    *array = tmp_array; // 结构体赋值，转移指针所有权

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
static int kvs_rbtree_save_node(FILE* fp, kvs_rbtree_t* tree, rbtree_node* node) {
    if (node == tree->nil) {
        return 0;
    }

    /*
     * 保存左子树
     */
    if (kvs_rbtree_save_node(fp, tree, node->left) != 0) {
        return -1;
    }

    /*
     * 保存当前节点
     */
    char* key = (char*)node->key;
    char* value = (char*)node->value;

    if (key == NULL || value == NULL) {
        return -1;
    }

    kvs_record_header_t record;

    record.key_len = (uint32_t)strlen(key);

    record.value_len = (uint32_t)strlen(value);

    /*
     * 写 record header
     */
    if (fwrite(&record, sizeof(record), 1, fp) != 1) {

        return -1;
    }

    /*
     * 写 key
     */
    if (fwrite(key, record.key_len, 1, fp) != 1) {

        return -1;
    }

    /*
     * 写 value
     */
    if (fwrite(value, record.value_len, 1, fp) != 1) {

        return -1;
    }

    /*
     * 保存右子树
     */
    if (kvs_rbtree_save_node(fp, tree, node->right) != 0) {

        return -1;
    }

    return 0;
}
int kvs_rbtree_save(kvs_rbtree_t* tree, const char* filename) {
    if (tree == NULL || filename == NULL) {

        return -1;
    }

    FILE* fp = fopen(filename, "wb");

    if (fp == NULL) {

        fprintf(stderr, "open %s failed: %s\n", filename, strerror(errno));

        return -1;
    }

    /*
     * 统计节点数量
     */
    uint32_t count = kvs_rbtree_count_node(tree, tree->root);

    /*
     * 文件头
     */
    kvs_file_header_t header;

    memset(&header, 0, sizeof(header));

    memcpy(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC));

    header.version = KVS_FILE_VERSION;

    header.count = count;

    /*
     * 写文件头
     */
    if (fwrite(&header, sizeof(header), 1, fp) != 1) {

        fclose(fp);

        return -1;
    }

    /*
     * 中序遍历保存
     */
    if (kvs_rbtree_save_node(fp, tree, tree->root) != 0) {

        fclose(fp);

        return -1;
    }

    /*
     * 刷新 stdio buffer
     */
    if (fflush(fp) != 0) {

        fclose(fp);

        return -1;
    }

    fclose(fp);

    return 0;
}

int kvs_rbtree_load(kvs_rbtree_t* tree, const char* filename) {
    if (tree == NULL || filename == NULL)
        return -1;

    FILE* fp = fopen(filename, "rb");
    if (fp == NULL) {
        if (errno == ENOENT)
            return 0;
        fprintf(stderr, "open %s failed: %s\n", filename, strerror(errno));
        return -1;
    }

    kvs_file_header_t header;
    if (fread(&header, sizeof(header), 1, fp) != 1) {
        fclose(fp);
        return -1;
    }

    if (memcmp(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC)) != 0) {
        fprintf(stderr, "invalid rbtree file magic\n");
        fclose(fp);
        return -1;
    }

    if (header.version != KVS_FILE_VERSION) {
        fprintf(stderr, "unsupported rbtree file version: %u\n", header.version);
        fclose(fp);
        return -1;
    }

    // 创建临时树，全部成功后替换
    kvs_rbtree_t tmp_tree;
    memset(&tmp_tree, 0, sizeof(tmp_tree));
    if (kvs_rbtree_create(&tmp_tree) != 0) {
        fclose(fp);
        return -1;
    }

    for (uint32_t i = 0; i < header.count; i++) {
        kvs_record_header_t record;
        if (fread(&record, sizeof(record), 1, fp) != 1) {
            fprintf(stderr, "read record header failed\n");
            kvs_rbtree_destroy(&tmp_tree);
            fclose(fp);
            return -1;
        }

        if (record.key_len == 0 || record.key_len > MAX_KEY_LEN) {
            fprintf(stderr, "invalid key length: %u\n", record.key_len);
            kvs_rbtree_destroy(&tmp_tree);
            fclose(fp);
            return -1;
        }
        if (record.value_len > MAX_VALUE_LEN) {
            fprintf(stderr, "invalid value length: %u\n", record.value_len);
            kvs_rbtree_destroy(&tmp_tree);
            fclose(fp);
            return -1;
        }

        char* key = (char*)kvs_malloc(record.key_len + 1);
        char* value = (char*)kvs_malloc(record.value_len + 1);
        if (key == NULL || value == NULL) {
            kvs_free(key);
            kvs_free(value);
            kvs_rbtree_destroy(&tmp_tree);
            fclose(fp);
            return -1;
        }

        if (fread(key, record.key_len, 1, fp) != 1 || fread(value, record.value_len, 1, fp) != 1) {
            kvs_free(key);
            kvs_free(value);
            kvs_rbtree_destroy(&tmp_tree);
            fclose(fp);
            return -1;
        }
        key[record.key_len] = '\0';
        value[record.value_len] = '\0';

        int ret = kvs_rbtree_set(&tmp_tree, key, value);
        if (ret != 0) { // 包括 >0 和 <0
            fprintf(stderr, "rbtree set failed (ret=%d) key=%s\n", ret, key);
            kvs_free(key);
            kvs_free(value);
            kvs_rbtree_destroy(&tmp_tree);
            fclose(fp);
            return -1;
        }
        kvs_free(key);
        kvs_free(value);
    }

    fclose(fp);

    // 成功：替换旧树
    if (tree->root != NULL && tree->root != tree->nil) {
        kvs_rbtree_destroy(tree);
    }
    *tree = tmp_tree; // 结构体赋值，转移指针所有权

    return 0;
}

#endif

#if ENABLE_HASH
static int kvs_hash_save_bucket(FILE* fp, hashnode_t* node) {
    while (node != NULL) {

        if (node->key == NULL || node->value == NULL) {

            return -1;
        }

        /*
         * key/value 长度
         */
        kvs_record_header_t record;

        record.key_len = (uint32_t)strlen(node->key);

        record.value_len = (uint32_t)strlen(node->value);

        /*
         * 检查长度
         */
        if (record.key_len == 0 || record.key_len > MAX_KEY_LEN) {

            return -1;
        }

        if (record.value_len > MAX_VALUE_LEN) {

            return -1;
        }

        /*
         * 写 record header
         */
        if (fwrite(&record, sizeof(record), 1, fp) != 1) {

            return -1;
        }

        /*
         * 写 key
         */
        if (fwrite(node->key, record.key_len, 1, fp) != 1) {

            return -1;
        }

        /*
         * 写 value
         */
        if (fwrite(node->value, record.value_len, 1, fp) != 1) {

            return -1;
        }

        /*
         * 下一个节点
         */
        node = node->next;
    }

    return 0;
}
int kvs_hash_save(kvs_hash_t* hash, const char* filename) {
    if (hash == NULL || filename == NULL) {

        return -1;
    }

    FILE* fp = fopen(filename, "wb");

    if (fp == NULL) {

        perror("fopen");
        return -1;
    }

    /*
     * =====================================
     * 文件头
     * =====================================
     */

    kvs_file_header_t header;

    memset(&header, 0, sizeof(header));

    memcpy(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC));

    header.version = KVS_FILE_VERSION;

    /*
     * Hash 已经维护 count，
     * 不需要重新统计。
     */
    header.count = (uint32_t)hash->count;

    /*
     * 写 header
     */
    if (fwrite(&header, sizeof(header), 1, fp) != 1) {

        fclose(fp);

        return -1;
    }

    /*
     * =====================================
     * 遍历所有 bucket
     * =====================================
     */

    for (int i = 0; i < hash->max_slots; ++i) {

        hashnode_t* node = hash->nodes[i];

        /*
         * 保存当前 bucket 的链表
         */
        if (kvs_hash_save_bucket(fp, node) != 0) {

            fclose(fp);

            return -1;
        }
    }

    /*
     * =====================================
     * 刷新文件缓冲区
     * =====================================
     */

    if (fflush(fp) != 0) {

        fclose(fp);

        return -1;
    }

    fclose(fp);

    return 0;
}

int kvs_hash_load(kvs_hash_t* hash, const char* filename) {
    if (hash == NULL || filename == NULL)
        return -1;

    FILE* fp = fopen(filename, "rb");
    if (fp == NULL) {
        if (errno == ENOENT)
            return 0; // 首次启动，无文件
        perror("fopen");
        return -1;
    }

    kvs_file_header_t header;
    if (fread(&header, sizeof(header), 1, fp) != 1) {
        fclose(fp);
        return -1;
    }

    if (memcmp(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC)) != 0) {
        fprintf(stderr, "invalid hash file magic\n");
        fclose(fp);
        return -1;
    }

    if (header.version != KVS_FILE_VERSION) {
        fprintf(stderr, "unsupported hash file version: %u\n", header.version);
        fclose(fp);
        return -1;
    }

    // 创建临时哈希表
    kvs_hash_t tmp_hash;
    memset(&tmp_hash, 0, sizeof(tmp_hash));
    if (kvs_hash_create(&tmp_hash) != 0) { // 假设返回 0 表示成功
        fclose(fp);
        return -1;
    }

    for (uint32_t i = 0; i < header.count; ++i) {
        kvs_record_header_t record;
        if (fread(&record, sizeof(record), 1, fp) != 1) {
            fprintf(stderr, "read record header failed\n");
            kvs_hash_destroy(&tmp_hash);
            fclose(fp);
            return -1;
        }

        if (record.key_len == 0 || record.key_len > MAX_KEY_LEN) {
            fprintf(stderr, "invalid key length: %u\n", record.key_len);
            kvs_hash_destroy(&tmp_hash);
            fclose(fp);
            return -1;
        }
        if (record.value_len > MAX_VALUE_LEN) {
            fprintf(stderr, "invalid value length: %u\n", record.value_len);
            kvs_hash_destroy(&tmp_hash);
            fclose(fp);
            return -1;
        }

        char* key = (char*)kvs_malloc(record.key_len + 1);
        char* value = (char*)kvs_malloc(record.value_len + 1);
        if (key == NULL || value == NULL) {
            kvs_free(key);
            kvs_free(value);
            kvs_hash_destroy(&tmp_hash);
            fclose(fp);
            return -1;
        }

        if (fread(key, record.key_len, 1, fp) != 1 || fread(value, record.value_len, 1, fp) != 1) {
            kvs_free(key);
            kvs_free(value);
            kvs_hash_destroy(&tmp_hash);
            fclose(fp);
            return -1;
        }
        key[record.key_len] = '\0';
        value[record.value_len] = '\0';

        int ret = kvs_hash_set(&tmp_hash, key, value);
        if (ret != 0) { // 包括 ret > 0（重复键）和 ret < 0（错误）
            fprintf(stderr, "hash set failed (ret=%d) key=%s\n", ret, key);
            kvs_free(key);
            kvs_free(value);
            kvs_hash_destroy(&tmp_hash);
            fclose(fp);
            return -1;
        }
        kvs_free(key);
        kvs_free(value);
    }

    fclose(fp);

    // 成功：替换旧哈希表
    if (hash->nodes != NULL) {
        kvs_hash_destroy(hash);
    }
    *hash = tmp_hash; // 结构体赋值，转移指针所有权

    return 0;
}

#endif

#if ENABLE_SKIPTABLE
static int kvs_skiptable_save_node(FILE* fp, Node* node) {
    if (node == NULL) {
        return 0;
    }

    if (node->key == NULL || node->value == NULL) {

        return -1;
    }

    kvs_record_header_t record;

    record.key_len = (uint32_t)strlen(node->key);

    record.value_len = (uint32_t)strlen(node->value);

    /*
     * 检查长度
     */
    if (record.key_len == 0 || record.key_len > MAX_KEY_LEN) {

        return -1;
    }

    if (record.value_len > MAX_VALUE_LEN) {

        return -1;
    }

    /*
     * 写 record header
     */
    if (fwrite(&record, sizeof(record), 1, fp) != 1) {

        return -1;
    }

    /*
     * 写 key
     */
    if (fwrite(node->key, record.key_len, 1, fp) != 1) {

        return -1;
    }

    /*
     * 写 value
     */
    if (fwrite(node->value, record.value_len, 1, fp) != 1) {

        return -1;
    }

    return 0;
}
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
int kvs_skiptable_save(kvs_skiptable_t* table, const char* filename) {
    if (table == NULL || filename == NULL) {

        return -1;
    }

    FILE* fp = fopen(filename, "wb");

    if (fp == NULL) {

        perror("fopen");

        return -1;
    }

    /*
     * =====================================
     * 统计节点数量
     * =====================================
     */

    uint32_t count = kvs_skiptable_count(table);

    /*
     * =====================================
     * 文件头
     * =====================================
     */

    kvs_file_header_t header;

    memset(&header, 0, sizeof(header));

    memcpy(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC));

    header.version = KVS_FILE_VERSION;

    header.count = count;

    /*
     * 写 header
     */
    if (fwrite(&header, sizeof(header), 1, fp) != 1) {

        fclose(fp);

        return -1;
    }

    /*
     * =====================================
     * 遍历 Level 0
     * =====================================
     */

    Node* node = table->header->forward[0];

    while (node != NULL) {

        if (kvs_skiptable_save_node(fp, node) != 0) {

            fclose(fp);

            return -1;
        }

        node = node->forward[0];
    }

    /*
     * =====================================
     * 刷新
     * =====================================
     */

    if (fflush(fp) != 0) {

        fclose(fp);

        return -1;
    }

    fclose(fp);

    return 0;
}

int kvs_skiptable_load(kvs_skiptable_t* table, const char* filename) {
    if (table == NULL || filename == NULL)
        return -1;

    FILE* fp = fopen(filename, "rb");
    if (fp == NULL) {
        if (errno == ENOENT)
            return 0; // 首次启动，无文件
        perror("fopen");
        return -1;
    }

    kvs_file_header_t header;
    if (fread(&header, sizeof(header), 1, fp) != 1) {
        fclose(fp);
        return -1;
    }

    if (memcmp(header.magic, KVS_FILE_MAGIC, strlen(KVS_FILE_MAGIC)) != 0) {
        fprintf(stderr, "invalid skiptable file magic\n");
        fclose(fp);
        return -1;
    }

    if (header.version != KVS_FILE_VERSION) {
        fprintf(stderr, "unsupported skiptable file version: %u\n", header.version);
        fclose(fp);
        return -1;
    }

    // 创建临时跳表
    kvs_skiptable_t tmp_table;
    memset(&tmp_table, 0, sizeof(tmp_table));
    if (kvs_skiptable_create(&tmp_table) != 0) { // 假设返回 0 成功
        fclose(fp);
        return -1;
    }

    for (uint32_t i = 0; i < header.count; ++i) {
        kvs_record_header_t record;
        if (fread(&record, sizeof(record), 1, fp) != 1) {
            fprintf(stderr, "read record header failed\n");
            kvs_skiptable_destroy(&tmp_table);
            fclose(fp);
            return -1;
        }

        if (record.key_len == 0 || record.key_len > MAX_KEY_LEN) {
            fprintf(stderr, "invalid key length: %u\n", record.key_len);
            kvs_skiptable_destroy(&tmp_table);
            fclose(fp);
            return -1;
        }
        if (record.value_len > MAX_VALUE_LEN) {
            fprintf(stderr, "invalid value length: %u\n", record.value_len);
            kvs_skiptable_destroy(&tmp_table);
            fclose(fp);
            return -1;
        }

        char* key = (char*)kvs_malloc(record.key_len + 1);
        char* value = (char*)kvs_malloc(record.value_len + 1);
        if (key == NULL || value == NULL) {
            kvs_free(key);
            kvs_free(value);
            kvs_skiptable_destroy(&tmp_table);
            fclose(fp);
            return -1;
        }

        if (fread(key, record.key_len, 1, fp) != 1 || fread(value, record.value_len, 1, fp) != 1) {
            kvs_free(key);
            kvs_free(value);
            kvs_skiptable_destroy(&tmp_table);
            fclose(fp);
            return -1;
        }
        key[record.key_len] = '\0';
        value[record.value_len] = '\0';

        int ret = kvs_skiptable_set(&tmp_table, key, value);
        if (ret != 0) { // 包括 ret > 0（重复键）和 ret < 0（错误）
            fprintf(stderr, "skiptable set failed (ret=%d) key=%s\n", ret, key);
            kvs_free(key);
            kvs_free(value);
            kvs_skiptable_destroy(&tmp_table);
            fclose(fp);
            return -1;
        }
        kvs_free(key);
        kvs_free(value);
    }

    fclose(fp);

    // 成功：替换旧跳表
    if (table->header != NULL) {
        kvs_skiptable_destroy(table);
    }
    *table = tmp_table; // 结构体赋值，转移指针所有权

    return 0;
}

#endif

#if 0 
// array
struct kvs_array_item_t {
    char* key;
    char* value;
};

#define KVS_ARRAY_SIZE 10240

struct kvs_array_t {
    kvs_array_item_t* table; //指针，同时也是数组的首地址

    int max_idx; //最大的索引+1，而非总数，因为删除数据不是连续的，允许中间删除
    int total; // 当前的总数
};
kvs_array_t global_array = {0};

memset(&global_array, 0, sizeof(kvs_array_t));
kvs_array_create(&global_array);

// rbtree
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
kvs_rbtree_t global_rbtree = {0};
memset(&global_rbtree, 0, sizeof(kvs_rbtree_t));
kvs_rbtree_create(&global_rbtree);

// hash

#define MAX_KEY_LEN 128
#define MAX_VALUE_LEN 512
#define MAX_TABLE_SIZE 1024

typedef struct hashnode_s {
    char* key;
    char* value;
    struct hashnode_s* next;

} hashnode_t;

typedef struct hashtable_s {
    hashnode_t** nodes; //* change **,
    int max_slots;
    int count;

} hashtable_t;

typedef struct hashtable_s kvs_hash_t;
kvs_hash_t global_hash;

memset(&global_hash, 0, sizeof(kvs_hash_t));
kvs_hash_create(&global_hash);

// skiptable
#define MAX_LEVEL 6

typedef struct Node {

    char* key;
    char* value;

    struct Node** forward;
} Node;

typedef struct SkipList {
    int level;
    Node* header;
} SkipList;

typedef struct SkipList kvs_skiptable_t;
kvs_skiptable_t global_skiptable;

memset(&global_skiptable, 0, sizeof(kvs_skiptable_t));
kvs_skiptable_create(&global_skiptable);
#endif