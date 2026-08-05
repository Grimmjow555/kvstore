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

    inst->total = 0;
    return 0;
}

void kvs_array_destory(kvs_array_t* inst) {
    if (inst == nullptr)
        return;
    if (inst->table) {
        kvs_free(inst->table);
    }
}

/*
 *@return: nullptr, no exist; ptr exist
 */
char* kvs_array_get(kvs_array_t* inst, char* key) {
    if (inst == nullptr || key == nullptr)
        return nullptr;

    for (int i = 0; i < inst->total; ++i) {
        if (inst->table[i].key == nullptr)
            continue;

        if (strcmp(inst->table[i].key, key) == 0) {
            return inst->table[i].value;
        }
    }

    return nullptr;
}

/*
 *@return: <0, error; = 0, success; >0, exist
 */
int kvs_array_set(kvs_array_t* inst, char* key, char* value) {
    if (inst == nullptr || key == nullptr || value == nullptr)
        return -1;
    if (inst->total == KVS_ARRAY_SIZE)
        return -1;

    char* ret = kvs_array_get(inst, key);
    if (ret != nullptr)
        return 1;

    char* kcopy = (char*)kvs_malloc(strlen(key) + 1);
    if (kcopy == nullptr)
        return -2;
    memset(kcopy, 0, strlen(key) + 1);
    strncpy(kcopy, key, strlen(key));

    char* kvalue = (char*)kvs_malloc(strlen(value) + 1);
    if (kvalue == nullptr)
        return -2;
    memset(kvalue, 0, strlen(value) + 1);
    strncpy(kvalue, value, strlen(value));

    int i = 0;
    for (i = 0; i < inst->total; ++i) {
        if (inst->table[i].key == nullptr) {
            inst->table[i].key = kcopy;
            inst->table[i].value = kvalue;
            // inst->total++;
            return 0;
        }
    }

    if (i == inst->total && i < KVS_ARRAY_SIZE) {
        inst->table[i].key = kcopy;
        inst->table[i].value = kvalue;
        inst->total++;
    }

    return 0;
}

/*
 *@return: <0, error; = 0, success; >0, no exist
 */
int kvs_array_del(kvs_array_t* inst, char* key) {
    if (inst == nullptr || key == nullptr)
        return -1;

    for (int i = 0; i < inst->total; ++i) {
        if (inst->table[i].key == nullptr)
            continue;
        if (strcmp(inst->table[i].key, key) == 0) {
            kvs_free(inst->table[i].key);
            inst->table[i].key = nullptr;

            kvs_free(inst->table[i].value);
            inst->table[i].value = nullptr;
            if (i == inst->total - 1) {
                while ((inst->total > 0) && (inst->table[inst->total - 1].key == nullptr)) {
                    inst->total--;
                }
            }
            return 0;
        }
    }

    return 1;
}

/*
 *@return: < 0, error; = 0, success, > 0 no exist
 */
int kvs_array_mod(kvs_array_t* inst, char* key, char* value) {
    if (inst == nullptr || key == nullptr || value == nullptr)
        return -1;

    for (int i = 0; i < inst->total; ++i) {
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
 *@return: = 0, exist; > 0 no exist; < 0, error
 */
int kvs_array_exist(kvs_array_t* inst, char* key) {
    if (inst == nullptr || key == nullptr)
        return -1;

    char* ret = kvs_array_get(inst, key);
    if (ret == nullptr)
        return 1;

    return 0;
}