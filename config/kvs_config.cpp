#include "kvs_config.h"

#include <cstring>
#include <strings.h>

static kvs_persistence_config_t g_persistence_config = {1, 1};

static int parse_on_off(const char* value, int* out) {
    if (value == nullptr || out == nullptr) {
        return -1;
    }

    if (strcasecmp(value, "on") == 0 || strcasecmp(value, "true") == 0 || strcmp(value, "1") == 0) {
        *out = 1;
        return 0;
    }

    if (strcasecmp(value, "off") == 0 || strcasecmp(value, "false") == 0 ||
        strcmp(value, "0") == 0) {
        *out = 0;
        return 0;
    }

    return -1;
}

void kvs_config_set_defaults(void) {
    g_persistence_config.rdb_enabled = 1;
    g_persistence_config.aof_enabled = 1;
}

int kvs_config_parse_switches(int* argc, char*** argv) {
    if (argc == nullptr || argv == nullptr || *argv == nullptr) {
        return -1;
    }

    char** args = *argv;
    int input_count = *argc;
    int output_index = 1; // argv[0] 是程序名，保持不变

    for (int i = 1; i < input_count;) {
        int value = 0;

        if (strcmp(args[i], "--rdb") == 0) {
            if (i + 1 >= input_count || parse_on_off(args[i + 1], &value) != 0) {
                return -1;
            }
            g_persistence_config.rdb_enabled = value;
            i += 2;
            continue;
        }

        if (strcmp(args[i], "--aof") == 0) {
            if (i + 1 >= input_count || parse_on_off(args[i + 1], &value) != 0) {
                return -1;
            }
            g_persistence_config.aof_enabled = value;
            i += 2;
            continue;
        }

        args[output_index++] = args[i++];
    }

    args[output_index] = nullptr;
    *argc = output_index;
    return 0;
}

int kvs_config_rdb_enabled(void) { return g_persistence_config.rdb_enabled; }

int kvs_config_aof_enabled(void) { return g_persistence_config.aof_enabled; }
