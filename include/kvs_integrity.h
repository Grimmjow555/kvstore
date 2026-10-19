#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KVS_SHA256_DIGEST_SIZE 32
#define KVS_SHA256_HEX_SIZE 64

// 计算一段连续字节的 SHA-256。
int kvs_sha256_digest(const void* data, size_t len,
                      unsigned char out[KVS_SHA256_DIGEST_SIZE]);

// 计算 AOF 哈希链：
// SHA256(previous || little-endian uint64(record_len) || record)。
int kvs_sha256_chain_digest(const unsigned char previous[KVS_SHA256_DIGEST_SIZE],
                            const void* record, size_t record_len,
                            unsigned char out[KVS_SHA256_DIGEST_SIZE]);

// 输出 64 位小写十六进制，并在 out[64] 写入 '\0'。
int kvs_sha256_hex_encode(const unsigned char digest[KVS_SHA256_DIGEST_SIZE],
                          char out[KVS_SHA256_HEX_SIZE + 1]);

// 解析恰好 64 位十六进制摘要，大小写均可。
int kvs_sha256_hex_decode(const char* text,
                          unsigned char out[KVS_SHA256_DIGEST_SIZE]);

// 摘要一致时返回 1，不一致或参数非法时返回 0。
int kvs_sha256_equal(const unsigned char left[KVS_SHA256_DIGEST_SIZE],
                     const unsigned char right[KVS_SHA256_DIGEST_SIZE]);

#ifdef __cplusplus
}
#endif
