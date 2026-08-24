#pragma once

#include <iostream>

void* kvs_malloc(size_t size);

void kvs_free(void* ptr);

enum kvs_role { KVS_ROLE_MASTER = 0, KVS_ROLE_REPLICA };