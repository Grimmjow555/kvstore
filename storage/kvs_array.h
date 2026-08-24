#define ENABLE_ARRAY 1

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
