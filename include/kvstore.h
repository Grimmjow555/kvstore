#pragma once

#include <iostream>

void* kvs_malloc(size_t size);

void kvs_free(void* ptr);
int kvs_reset_data();
