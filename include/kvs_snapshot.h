#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// 将当前四个存储引擎序列化为与 RDB 文件一致的内存快照。
// 成功时 *out_data 指向新分配的内存，调用方负责 free(*out_data)。
int kvs_snapshot_serialize(char** out_data, size_t* out_len);

// 从内存中的 RDB 格式快照恢复数据。该函数会先调用 kvs_reset_data()。
int kvs_snapshot_load_buffer(const void* data, size_t len);

int kvs_snapshot_load(const char* filename);
int kvs_snapshot_save(const char* filename);

#ifdef __cplusplus
}
#endif
