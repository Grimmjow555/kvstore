#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KVS_REPL_PROTOCOL_VERSION "2"
#define KVS_REPL_SESSION_ID_SIZE 16

// 生成/校验 Master 分配的同步会话 ID。ID 固定为 16 位小写十六进制，
// 既作为多从机会话键，也可安全地拼进临时文件名。
int kvs_repl_format_session_id(unsigned long long value, char* out, size_t out_size);
int kvs_repl_validate_session_id(const char* session_id);

// 通用 RESP 数组编码。argv 中每个元素都是非空、无 NUL 的 C 字符串。
int kvs_repl_build_resp_array(const char* const argv[], int argc, char* out, size_t out_size,
                              size_t* out_len);

// 解析 Master 对 SYNC 的成功响应："+OK <16-hex-session>\r\n"。
int kvs_repl_parse_ok_session(const char* response, int length, char* session_id,
                              size_t session_id_size);

// 版本化握手：
//   EBPF 2
//   REPL_INC 2 <session_id>
int kvs_repl_build_ebpf_handshake(char* out, size_t out_size, size_t* out_len);
int kvs_repl_parse_ebpf_handshake(const char* payload, int length);
int kvs_repl_build_repl_inc(const char* session_id, char* out, size_t out_size, size_t* out_len);
int kvs_repl_parse_repl_inc(const char* payload, int length, char* session_id,
                            size_t session_id_size);

#ifdef __cplusplus
}
#endif
