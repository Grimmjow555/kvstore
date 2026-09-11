#pragma once

#include <stddef.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kvs_io_uring_file kvs_io_uring_file_t;

kvs_io_uring_file_t* kvs_io_uring_open(const char* filename, int flags, mode_t mode);
int kvs_io_uring_write(kvs_io_uring_file_t* file, const void* buf, size_t len);
int kvs_io_uring_write_and_fsync(kvs_io_uring_file_t* file, const void* buf, size_t len);
int kvs_io_uring_fsync(kvs_io_uring_file_t* file);
int kvs_io_uring_close(kvs_io_uring_file_t* file);

#ifdef __cplusplus
}
#endif
