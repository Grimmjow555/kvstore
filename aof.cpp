#include "kvstore.h"

#define AOF_ENABLE 1

#if AOF_ENABLE
static FILE* aof_fp = NULL;
int kvs_aof_init(const char* filename) {
    aof_fp = fopen(filename, "a+");
    if (aof_fp == NULL) {
        return -1;
    }

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

// int kvs_aof_replay(const char* filename) {

//     FILE* fp = fopen(filename, "r");

//     if (!fp) {
//         return -1;
//     }

//     fseek(fp, 0, SEEK_END);
//     long size = ftell(fp);
//     rewind(fp);

//     char* buffer = (char*)kvs_malloc(size + 1);

//     if (!buffer) {
//         fclose(fp);
//         return -1;
//     }

//     fread(buffer, 1, size, fp);
//     buffer[size] = '\0';

//     int offset = 0;

//     while (offset < size) {

//         int argc;
//         int consumed;

//         char** argv = resp_parse_command(buffer + offset, &argc, &consumed);

//         if (!argv) {
//             break;
//         }

//         // 执行命令
//         char response[1024];

//         kvs_filter_protocol(argv, argc, response, sizeof(response));

//         for (int i = 0; i < argc; ++i) {
//             kvs_free(argv[i]);
//         }

//         kvs_free(argv);

//         offset += consumed;
//     }

//     kvs_free(buffer);
//     fclose(fp);

//     return 0;
// }
#endif