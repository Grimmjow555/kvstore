#include "aof.h"
#include "kvs_config.h"
#include "kvs_io_uring.h"
#include "kvstore.h"
#include <climits>
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

#define KVS_AOF_FLUSH_THRESHOLD (0)

static int aof_replaying = 0;

static kvs_io_uring_file_t* aof_fp = NULL;
static char aof_filename[512] = {0};
static std::string aof_buffer;

static int kvs_aof_flush(void);

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

    aof_fp = kvs_io_uring_open(filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fp == NULL) {
        return -1;
    }

    snprintf(aof_filename, sizeof(aof_filename), "%s", filename);
    aof_buffer.clear();

    return 0;
}

int kvs_aof_append(int argc, char* argv[]) {

    if (aof_fp == NULL || argc <= 0) {
        return -1;
    }

    aof_buffer += "*";
    aof_buffer += std::to_string(argc);
    aof_buffer += "\r\n";

    for (int i = 0; i < argc; ++i) {
        aof_buffer += "$";
        aof_buffer += std::to_string(strlen(argv[i]));
        aof_buffer += "\r\n";
        aof_buffer += argv[i];
        aof_buffer += "\r\n";
    }

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
    void* mapped = NULL;
    if (size > 0) {
        mapped = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped == MAP_FAILED) {
            close(fd);
            return -1;
        }
    }
    close(fd);

    size_t offset = 0;

    aof_replaying = 1;

    while (offset < size) {

        int argc;
        int consumed;

        char** argv =
            kvs_aof_parse_command((const char*)mapped + offset, size - offset, &argc, &consumed);

        if (argv == NULL || consumed <= 0) {
            break;
        }

        // 执行命令
        char response[128];

        kvs_filter_protocol(argv, argc, response, sizeof(response));

        for (int i = 0; i < argc; ++i) {
            free(argv[i]);
        }

        free(argv);

        offset += consumed;
    }

    aof_replaying = 0;

    if (size > 0) {
        munmap(mapped, size);
    }

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

    if (kvs_io_uring_fsync(aof_fp) != 0) {
        return -1;
    }

    return 0;
}

#endif
