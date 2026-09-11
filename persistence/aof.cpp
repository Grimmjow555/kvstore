#include "aof.h"
#include "kvs_config.h"
#include "kvs_io_uring.h"
#include "kvstore.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>

extern int kvs_filter_protocol(char* tokens[], int count, char* response, int response_size);
extern char** resp_parse_command(char* buffer, int* argc, int* consumed);

#if AOF_ENABLE

#define KVS_AOF_FLUSH_THRESHOLD (64 * 1024)

static int aof_replaying = 0;

static kvs_io_uring_file_t* aof_fp = NULL;
static char aof_filename[512] = {0};
static std::string aof_buffer;

static int kvs_aof_flush(void);

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

    FILE* fp = fopen(filename, "r");

    if (!fp) {
        return -1;
    }

    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    rewind(fp);

    char* buffer = (char*)malloc(size + 1);

    if (!buffer) {
        fclose(fp);
        return -1;
    }

    fread(buffer, 1, size, fp);
    buffer[size] = '\0';

    int offset = 0;

    aof_replaying = 1;

    while (offset < size) {

        int argc;
        int consumed;

        char** argv = resp_parse_command(buffer + offset, &argc, &consumed);

        if (!argv) {
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

    free(buffer);
    fclose(fp);

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
