#include "kvs_aof.h"
#include "kvs_config.h"
#include "kvs_integrity.h"
#include "kvs_io_uring.h"
#include "kvs_runtime.h"
#include <climits>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

extern int kvs_filter_protocol(char* tokens[], int count, char* response, int response_size);

#if AOF_ENABLE

#define KVS_AOF_FLUSH_THRESHOLD 1024
#define KVS_AOF_HEADER "#KVSAOF02\r\n"
#define KVS_AOF_HEADER_SIZE (sizeof(KVS_AOF_HEADER) - 1)
#define KVS_AOF_CHECKSUM_SIZE (KVS_SHA256_HEX_SIZE + 2)

// AOF 重放标记按线程隔离：Replica 的复制回放线程与本地客户端线程可能同时
// 处于写入路径，全局标记会让本地客户端的写被误判为「重放中」而漏记 AOF。
static __thread int aof_replaying = 0;

static kvs_io_uring_file_t* aof_fp = NULL;
static char aof_filename[512] = {0};
static std::string aof_buffer;
static unsigned char aof_committed_digest[KVS_SHA256_DIGEST_SIZE] = {0};
static unsigned char aof_tail_digest[KVS_SHA256_DIGEST_SIZE] = {0};

static int kvs_aof_flush(void);

static void kvs_aof_reset_chain(void) {
    memset(aof_committed_digest, 0, sizeof(aof_committed_digest));
    memset(aof_tail_digest, 0, sizeof(aof_tail_digest));
}

static const char* kvs_aof_find_crlf(const char* p, const char* end) {
    if (p == NULL || end == NULL || p >= end) {
        return NULL;
    }
    while (p + 1 < end) {
        if (p[0] == '\r' && p[1] == '\n') {
            return p;
        }
        ++p;
    }
    return NULL;
}

static int kvs_aof_parse_int(const char* p, const char* end, int* out, const char** next) {
    if (p == NULL || out == NULL || next == NULL || p >= end) {
        return -1;
    }

    const char* q = p;
    int sign = 1;
    if (*q == '-') {
        sign = -1;
        ++q;
    }

    if (q >= end || *q < '0' || *q > '9') {
        return -1;
    }

    long value = 0;
    while (q < end && *q >= '0' && *q <= '9') {
        int digit = *q - '0';
        if (value > (LONG_MAX - digit) / 10) {
            return -1;
        }
        value = value * 10 + digit;
        ++q;
    }

    value *= sign;
    if (value < INT_MIN || value > INT_MAX) {
        return -1;
    }

    *out = (int)value;
    *next = q;
    return 0;
}

static char** kvs_aof_parse_command(const char* data, size_t size, int* argc, int* consumed) {
    if (data == NULL || size == 0 || data[0] != '*') {
        return NULL;
    }

    const char* end = data + size;
    const char* p = data + 1;
    int param_count = 0;

    if (kvs_aof_parse_int(p, end, &param_count, &p) != 0 || param_count <= 0) {
        return NULL;
    }

    const char* line_end = kvs_aof_find_crlf(p, end);
    if (line_end == NULL) {
        return NULL;
    }
    p = line_end + 2;

    char** argv = (char**)malloc((param_count + 1) * sizeof(char*));
    if (argv == NULL) {
        return NULL;
    }
    for (int i = 0; i <= param_count; ++i) {
        argv[i] = NULL;
    }

    for (int i = 0; i < param_count; ++i) {
        if (p >= end || *p != '$') {
            goto parse_fail;
        }

        int len = 0;
        if (kvs_aof_parse_int(p + 1, end, &len, &p) != 0) {
            goto parse_fail;
        }
        if (len < 0) {
            goto parse_fail;
        }

        line_end = kvs_aof_find_crlf(p, end);
        if (line_end == NULL) {
            goto parse_fail;
        }

        const char* data_start = line_end + 2;
        if ((size_t)(end - data_start) < (size_t)len) {
            goto parse_fail;
        }

        char* item = (char*)malloc((size_t)len + 1);
        if (item == NULL) {
            goto parse_fail;
        }
        memcpy(item, data_start, (size_t)len);
        item[len] = '\0';
        argv[i] = item;

        p = data_start + len;
        if ((size_t)(end - p) < 2 || p[0] != '\r' || p[1] != '\n') {
            goto parse_fail;
        }
        p += 2;
    }

    *argc = param_count;
    *consumed = (int)(p - data);
    return argv;

parse_fail:
    for (int i = 0; i < param_count; ++i) {
        free(argv[i]);
    }
    free(argv);
    return NULL;
}

int kvs_aof_init(const char* filename) {
    if (filename == NULL) {
        return -1;
    }

    struct stat st;
    int needs_header = 0;
    if (stat(filename, &st) == 0) {
        if (st.st_size < 0) {
            return -1;
        }
        needs_header = st.st_size == 0;
    } else if (errno == ENOENT) {
        needs_header = 1;
    } else {
        return -1;
    }

    aof_fp = kvs_io_uring_open(filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fp == NULL) {
        return -1;
    }

    snprintf(aof_filename, sizeof(aof_filename), "%s", filename);
    aof_buffer.clear();

    if (needs_header) {
        kvs_aof_reset_chain();
        if (kvs_io_uring_write_and_fsync(aof_fp, KVS_AOF_HEADER, KVS_AOF_HEADER_SIZE) != 0) {
            kvs_io_uring_close(aof_fp);
            aof_fp = NULL;
            aof_filename[0] = '\0';
            return -1;
        }
    }

    return 0;
}

int kvs_aof_append(int argc, char* argv[]) {

    if (aof_fp == NULL || argc <= 0) {
        return -1;
    }

    std::string record;
    record += "*";
    record += std::to_string(argc);
    record += "\r\n";

    for (int i = 0; i < argc; ++i) {
        record += "$";
        record += std::to_string(strlen(argv[i]));
        record += "\r\n";
        record += argv[i];
        record += "\r\n";
    }

    unsigned char digest[KVS_SHA256_DIGEST_SIZE];
    if (kvs_sha256_chain_digest(aof_tail_digest, record.data(), record.size(), digest) != 0) {
        return -1;
    }
    char hex[KVS_SHA256_HEX_SIZE + 1] = {0};
    if (kvs_sha256_hex_encode(digest, hex) != 0) {
        return -1;
    }

    aof_buffer += record;
    aof_buffer += hex;
    aof_buffer += "\r\n";
    memcpy(aof_tail_digest, digest, sizeof(aof_tail_digest));

    if (aof_buffer.size() >= KVS_AOF_FLUSH_THRESHOLD) {
        return kvs_aof_flush();
    }

    return 0;
}

static int kvs_aof_flush(void) {
    if (aof_fp == NULL) {
        return aof_buffer.empty() ? 0 : -1;
    }
    if (aof_buffer.empty()) {
        return 0;
    }

    int ret = kvs_io_uring_write_and_fsync(aof_fp, aof_buffer.data(), aof_buffer.size());
    if (ret == 0) {
        memcpy(aof_committed_digest, aof_tail_digest, sizeof(aof_committed_digest));
        aof_buffer.clear();
    }
    return ret;
}

int kvs_aof_close() {
    if (aof_fp != NULL) {

        if (!aof_buffer.empty()) {
            kvs_aof_flush();
        } else {
            kvs_io_uring_fsync(aof_fp);
        }

        kvs_io_uring_close(aof_fp);

        aof_fp = NULL;
    }

    aof_buffer.clear();
    return 0;
}

//读取增量文件，执行命令
int kvs_aof_replay(const char* filename) {

    // 回放前先提交仍在内存中的 AOF 增量，避免读到旧文件后漏掉最近写入。
    if (kvs_aof_flush() != 0) {
        return -1;
    }

    // 将 AOF 文件 mmap 到只读内存后，使用带边界检查的 RESP 解析器逐条回放。
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < 0) {
        close(fd);
        return -1;
    }

    size_t size = (size_t)st.st_size;
    if (size < KVS_AOF_HEADER_SIZE) {
        close(fd);
        kvs_log(KVS_LOG_ERROR,
                "Unsupported AOF format; expected #KVSAOF02 header and SHA-256 chain");
        return -1;
    }

    void* mapped = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        return -1;
    }
    close(fd);

    if (memcmp(mapped, KVS_AOF_HEADER, KVS_AOF_HEADER_SIZE) != 0) {
        munmap(mapped, size);
        kvs_log(KVS_LOG_ERROR,
                "Unsupported AOF format; expected #KVSAOF02 header and SHA-256 chain");
        return -1;
    }

    size_t offset = KVS_AOF_HEADER_SIZE;
    uint64_t record_index = 0;
    unsigned char chain_digest[KVS_SHA256_DIGEST_SIZE] = {0};

    aof_replaying = 1;

    while (offset < size) {

        int argc;
        int consumed;

        char** argv =
            kvs_aof_parse_command((const char*)mapped + offset, size - offset, &argc, &consumed);

        if (argv == NULL || consumed <= 0) {
            kvs_log(KVS_LOG_ERROR,
                    "AOF integrity check failed at record %llu, offset %zu: malformed RESP",
                    (unsigned long long)record_index, offset);
            aof_replaying = 0;
            munmap(mapped, size);
            return -1;
        }

        if ((size_t)consumed > size - offset ||
            size - offset - (size_t)consumed < KVS_AOF_CHECKSUM_SIZE) {
            for (int i = 0; i < argc; ++i) {
                free(argv[i]);
            }
            free(argv);
            kvs_log(KVS_LOG_ERROR,
                    "AOF integrity check failed at record %llu, offset %zu: "
                    "missing checksum",
                    (unsigned long long)record_index, offset);
            aof_replaying = 0;
            munmap(mapped, size);
            return -1;
        }

        const char* command = (const char*)mapped + offset;
        const char* checksum_text = command + consumed;
        unsigned char expected_digest[KVS_SHA256_DIGEST_SIZE];
        char checksum_with_nul[KVS_SHA256_HEX_SIZE + 1] = {0};
        memcpy(checksum_with_nul, checksum_text, KVS_SHA256_HEX_SIZE);
        if (kvs_sha256_hex_decode(checksum_with_nul, expected_digest) != 0 ||
            checksum_text[KVS_SHA256_HEX_SIZE] != '\r' ||
            checksum_text[KVS_SHA256_HEX_SIZE + 1] != '\n') {
            for (int i = 0; i < argc; ++i) {
                free(argv[i]);
            }
            free(argv);
            kvs_log(KVS_LOG_ERROR,
                    "AOF integrity check failed at record %llu, offset %zu: "
                    "invalid checksum framing",
                    (unsigned long long)record_index, offset);
            aof_replaying = 0;
            munmap(mapped, size);
            return -1;
        }

        unsigned char actual_digest[KVS_SHA256_DIGEST_SIZE];
        if (kvs_sha256_chain_digest(chain_digest, command, (size_t)consumed,
                                    actual_digest) != 0) {
            for (int i = 0; i < argc; ++i) {
                free(argv[i]);
            }
            free(argv);
            kvs_log(KVS_LOG_ERROR,
                    "AOF integrity check failed at record %llu, offset %zu: "
                    "cannot compute SHA-256",
                    (unsigned long long)record_index, offset);
            aof_replaying = 0;
            munmap(mapped, size);
            return -1;
        }
        if (!kvs_sha256_equal(expected_digest, actual_digest)) {
            char expected_hex[KVS_SHA256_HEX_SIZE + 1] = {0};
            char actual_hex[KVS_SHA256_HEX_SIZE + 1] = {0};
            kvs_sha256_hex_encode(expected_digest, expected_hex);
            kvs_sha256_hex_encode(actual_digest, actual_hex);
            for (int i = 0; i < argc; ++i) {
                free(argv[i]);
            }
            free(argv);
            kvs_log(KVS_LOG_ERROR,
                    "AOF integrity check failed at record %llu, offset %zu: "
                    "expected sha256=%s actual sha256=%s",
                    (unsigned long long)record_index, offset, expected_hex, actual_hex);
            aof_replaying = 0;
            munmap(mapped, size);
            return -1;
        }
        memcpy(chain_digest, actual_digest, sizeof(chain_digest));

        // 执行命令
        char response[128];

        kvs_filter_protocol(argv, argc, response, sizeof(response));

        for (int i = 0; i < argc; ++i) {
            free(argv[i]);
        }

        free(argv);

        offset += (size_t)consumed + KVS_AOF_CHECKSUM_SIZE;
        ++record_index;
    }

    aof_replaying = 0;
    memcpy(aof_committed_digest, chain_digest, sizeof(aof_committed_digest));
    memcpy(aof_tail_digest, chain_digest, sizeof(aof_tail_digest));

    munmap(mapped, size);

    return 0;
}

int kvs_aof_is_replaying() { return aof_replaying; }

//清除aof文件
int kvs_aof_clear() {
    if (aof_fp == NULL || aof_filename[0] == '\0') {
        return -1;
    }

    // 先落盘当前缓冲区，再关闭并重建空文件。
    if (kvs_aof_flush() != 0) {
        return -1;
    }

    // 关闭当前 AOF 文件
    if (kvs_io_uring_fsync(aof_fp) != 0) {
        return -1;
    }
    kvs_io_uring_close(aof_fp);
    aof_fp = NULL;

    // 以 "w" 模式重新打开，清空原文件
    aof_fp = kvs_io_uring_open(aof_filename, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (aof_fp == NULL) {
        return -1;
    }

    aof_buffer.clear();

    if (kvs_io_uring_write_and_fsync(aof_fp, KVS_AOF_HEADER, KVS_AOF_HEADER_SIZE) != 0) {
        kvs_io_uring_close(aof_fp);
        aof_fp = NULL;
        return -1;
    }
    kvs_aof_reset_chain();

    return 0;
}

#endif
