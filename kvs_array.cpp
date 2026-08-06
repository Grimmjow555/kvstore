#include "kvstore.h"

// singleton

kvs_array_t global_array = {0};

int kvs_array_create(kvs_array_t* inst) {
    if (inst == nullptr)
        return -1;
    if (inst->table) {
        printf("table has alloc\n");
        return -1;
    }
    inst->table = (kvs_array_item_t*)kvs_malloc(KVS_ARRAY_SIZE * sizeof(kvs_array_item_t));
    if (inst->table == nullptr)
        return -1;

    inst->max_idx = 0;
    inst->total = 0;
    return 0;
}

void kvs_array_destroy(kvs_array_t* inst) {
    if (inst == nullptr)
        return;
    if (inst->table) {
        kvs_free(inst->table);
    }
}

/*
 *@return: (char*)ptr, exist; nullptr, no exist
 */
char* kvs_array_get(kvs_array_t* inst, char* key) {
    if (inst == nullptr || key == nullptr) {
        // printf("kvs_array_get error!\n");
        return nullptr;
    }

    for (int i = 0; i < inst->max_idx; ++i) {
        if (inst->table[i].key == nullptr)
            continue;

        if (strcmp(inst->table[i].key, key) == 0) {
            return inst->table[i].value;
        }
    }

    // printf("kvs_array_get not find!\n");
    return nullptr;
}

/*
 *@return: = 0, success; <0, error; >0, exist
 */
int kvs_array_set(kvs_array_t* inst, char* key, char* value) {
    if (inst == nullptr || key == nullptr || value == nullptr)
        return -1;
    if (inst->max_idx >= KVS_ARRAY_SIZE)
        return -2;

    char* ret = kvs_array_get(inst, key);
    if (ret != nullptr)
        return 1;

    char* kcopy = (char*)kvs_malloc(strlen(key) + 1);
    if (kcopy == nullptr)
        return -3;
    // memset(kcopy, 0, strlen(key) + 1);
    // strncpy(kcopy, key, strlen(key));
    memcpy(kcopy, key, strlen(key) + 1);

    char* vcopy = (char*)kvs_malloc(strlen(value) + 1);
    if (vcopy == nullptr)
        return -4;
    // memset(vcopy, 0, strlen(value) + 1);
    // strncpy(vcopy, value, strlen(value));
    memcpy(vcopy, value, strlen(value) + 1);

    int i = 0;
    for (i = 0; i < inst->max_idx; ++i) {
        if (inst->table[i].key == nullptr) {
            inst->table[i].key = kcopy;
            inst->table[i].value = vcopy;
            inst->total++;
            return 0;
        }
    }

    inst->table[i].key = kcopy;
    inst->table[i].value = vcopy;
    inst->max_idx++;
    inst->total++;
    return 0;
}

/*
 *@return: = 0, success; <0, error; >0, no exist
 */
int kvs_array_del(kvs_array_t* inst, char* key) {
    if (inst == nullptr || key == nullptr)
        return -1;

    for (int i = 0; i < inst->max_idx; ++i) {
        if (inst->table[i].key == nullptr)
            continue;
        if (strcmp(inst->table[i].key, key) == 0) {
            kvs_free(inst->table[i].key);
            inst->table[i].key = nullptr;

            kvs_free(inst->table[i].value);
            inst->table[i].value = nullptr;

            inst->total--;
            if (i == inst->max_idx - 1) {
                while ((inst->max_idx > 0) && (inst->table[inst->max_idx - 1].key == nullptr)) {
                    inst->max_idx--;
                }
            }
            return 0;
        }
    }

    return 1;
}

/*
 *@return: = 0, success; < 0, error; > 0 no exist
 */
int kvs_array_mod(kvs_array_t* inst, char* key, char* value) {
    if (inst == nullptr || key == nullptr || value == nullptr)
        return -1;

    for (int i = 0; i < inst->max_idx; ++i) {
        if (inst->table[i].key == nullptr)
            continue;
        if (strcmp(inst->table[i].key, key) == 0) {
            kvs_free(inst->table[i].value);
            char* kvalue = (char*)kvs_malloc(strlen(value) + 1);
            if (kvalue == nullptr)
                return -2;
            memset(kvalue, 0, strlen(value) + 1);
            strncpy(kvalue, value, strlen(value));

            inst->table[i].value = kvalue;
            return 0;
        }
    }

    return 1;
}

/*
 *@return: = 0, exist; < 0, error; > 0 no exist
 */
int kvs_array_exist(kvs_array_t* inst, char* key) {
    if (inst == nullptr || key == nullptr)
        return -1;

    char* ret = kvs_array_get(inst, key);
    if (ret == nullptr)
        return 1;

    return 0;
}