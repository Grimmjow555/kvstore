#pragma once
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// 将当前四个存储引擎序列化为 KVSDB02 v2 内存快照，末尾附带整文件 SHA-256。
// 成功时 *out_data 指向新分配的内存，调用方负责 free(*out_data)。
int kvs_snapshot_serialize(char** out_data, size_t* out_len);

// 验证并恢复 KVSDB02 快照。校验失败不会调用 kvs_reset_data()。
int kvs_snapshot_load_buffer(const void* data, size_t len);

int kvs_snapshot_load(const char* filename);
int kvs_snapshot_save(const char* filename);

#ifdef __cplusplus
}
#endif
