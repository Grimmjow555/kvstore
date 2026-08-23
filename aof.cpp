#include "aof.h"
#include "kvstore.h"

extern int kvs_filter_protocol(char* tokens[], int count, char* response, int response_size);
extern char** resp_parse_command(char* buffer, int* argc, int* consumed);

#if AOF_ENABLE

static int aof_replaying = 0;

static FILE* aof_fp = NULL;
static char aof_filename[512] = {0};

int kvs_aof_init(const char* filename) {
    if (filename == NULL) {
        return -1;
    }

    aof_fp = fopen(filename, "a+");
    if (aof_fp == NULL) {
        return -1;
    }

    snprintf(aof_filename, sizeof(aof_filename), "%s", filename);

    return 0;
}

int kvs_aof_append(int argc, char* argv[]) {
    if (aof_fp == NULL || argc <= 0) {
        return -1;
    }

    fprintf(aof_fp, "*%d\r\n", argc);

    for (int i = 0; i < argc; ++i) {
        fprintf(aof_fp, "$%zu\r\n%s\r\n", strlen(argv[i]), argv[i]);
    }

    fflush(aof_fp);

    return 0;
}

int kvs_aof_close() {
    if (aof_fp != NULL) {

        fflush(aof_fp);

        fclose(aof_fp);

        aof_fp = NULL;
    }

    return 0;
}

int kvs_aof_replay(const char* filename) {

    FILE* fp = fopen(filename, "r");

    if (!fp) {
        return -1;
    }

    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    rewind(fp);

    char* buffer = (char*)kvs_malloc(size + 1);

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
        char response[1024];

        kvs_filter_protocol(argv, argc, response, sizeof(response));

        for (int i = 0; i < argc; ++i) {
            kvs_free(argv[i]);
        }

        kvs_free(argv);

        offset += consumed;
    }

    aof_replaying = 0;

    kvs_free(buffer);
    fclose(fp);

    return 0;
}

int kvs_aof_is_replaying() { return aof_replaying; }

int kvs_aof_clear() {
    if (aof_fp == NULL || aof_filename[0] == '\0') {
        return -1;
    }

    // 关闭当前 AOF 文件
    fclose(aof_fp);
    aof_fp = NULL;

    // 以 "w" 模式重新打开，清空原文件
    aof_fp = fopen(aof_filename, "w");
    if (aof_fp == NULL) {
        return -1;
    }

    fflush(aof_fp);

    return 0;
}

#endif