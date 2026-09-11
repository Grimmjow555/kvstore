#include "kvs_io_uring.h"

#include <climits>
#include <cstddef>
#include <cstdlib>
#include <fcntl.h>
#include <liburing.h>
#include <unistd.h>

// 每个文件对象使用一个独立的小型 io_uring。
// 当前服务端业务路径为单线程，这里采用“提交后同步等待单个 CQE”的方式，
// 避免引入额外的完成队列线程，同时仍由内核 io_uring 完成写盘与 fsync。
#define KVS_URING_QUEUE_DEPTH 64

struct kvs_io_uring_file {
    int fd;                          // 已打开的持久化文件描述符
    struct io_uring ring;            // 该文件独立使用的 io_uring 实例
    unsigned char append_mode;       // 是否使用 O_APPEND 追加写入
    unsigned long long write_offset; // 非追加写模式下的下一次写入偏移，追加模式使用
};

static int kvs_io_uring_wait_one(struct io_uring* ring) {
    struct io_uring_cqe* cqe = NULL;
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret < 0) {
        return ret;
    }

    int res = cqe->res;
    io_uring_cqe_seen(ring, cqe);
    return res;
}

kvs_io_uring_file_t* kvs_io_uring_open(const char* filename, int flags, mode_t mode) {
    if (filename == NULL) {
        return NULL;
    }

    int fd = open(filename, flags, mode);
    if (fd < 0) {
        return NULL;
    }

    kvs_io_uring_file_t* file = (kvs_io_uring_file_t*)calloc(1, sizeof(kvs_io_uring_file_t));
    if (file == NULL) {
        close(fd);
        return NULL;
    }

    if (io_uring_queue_init(KVS_URING_QUEUE_DEPTH, &file->ring, 0) < 0) {
        close(fd);
        free(file);
        return NULL;
    }

    file->fd = fd;
    file->append_mode = (flags & O_APPEND) ? 1 : 0;
    file->write_offset = 0;
    return file;
}

int kvs_io_uring_write(kvs_io_uring_file_t* file, const void* buf, size_t len) {
    // 校验文件对象、缓冲区和写入长度。
    if (file == NULL || buf == NULL || len == 0) {
        return -1;
    }
    if (len > UINT_MAX) {
        return -1;
    }

    // 获取 SQE，准备一次文件写入请求。
    struct io_uring_sqe* sqe = io_uring_get_sqe(&file->ring);
    if (sqe == NULL) {
        return -1;
    }

    __u64 offset = file->append_mode ? 0 : file->write_offset;
    io_uring_prep_write(sqe, file->fd, buf, (unsigned)len, offset);
    sqe->user_data = 0;

    // 提交请求并同步等待对应的完成事件。
    int submitted = io_uring_submit(&file->ring);
    if (submitted < 1) {
        return -1;
    }

    int res = kvs_io_uring_wait_one(&file->ring);
    if (res < 0) {
        return -1;
    }
    if ((size_t)res != len) {
        return -1;
    }

    // 顺序写模式需要记录下一次写入位置；追加模式由 O_APPEND 管理偏移。
    if (!file->append_mode) {
        file->write_offset += (unsigned long long)res;
    }
    return 0;
}

int kvs_io_uring_write_and_fsync(kvs_io_uring_file_t* file, const void* buf, size_t len) {
    // 空缓冲区只需执行一次 fsync，调用方不必额外处理空写场景。
    if (file == NULL || (len == 0 && buf == NULL)) {
        return kvs_io_uring_fsync(file);
    }
    if (buf == NULL || len == 0 || len > UINT_MAX) {
        return -1;
    }

    struct io_uring_sqe* write_sqe = io_uring_get_sqe(&file->ring);
    struct io_uring_sqe* fsync_sqe = io_uring_get_sqe(&file->ring);
    if (write_sqe == NULL || fsync_sqe == NULL) {
        return -1;
    }

    __u64 offset = file->append_mode ? 0 : file->write_offset;
    io_uring_prep_write(write_sqe, file->fd, buf, (unsigned)len, offset);
    write_sqe->flags |= IOSQE_IO_LINK;
    write_sqe->user_data = 0;

    io_uring_prep_fsync(fsync_sqe, file->fd, 0);
    fsync_sqe->user_data = 0;

    int submitted = io_uring_submit(&file->ring);
    if (submitted < 2) {
        return -1;
    }

    int write_res = kvs_io_uring_wait_one(&file->ring);
    int fsync_res = kvs_io_uring_wait_one(&file->ring);

    if (write_res < 0 || (size_t)write_res != len) {
        return -1;
    }
    if (fsync_res < 0) {
        return -1;
    }

    if (!file->append_mode) {
        file->write_offset += (unsigned long long)write_res;
    }
    return 0;
}

int kvs_io_uring_fsync(kvs_io_uring_file_t* file) {
    // 校验文件对象。
    if (file == NULL) {
        return -1;
    }

    // 获取 SQE 并准备文件同步请求。
    struct io_uring_sqe* sqe = io_uring_get_sqe(&file->ring);
    if (sqe == NULL) {
        return -1;
    }

    io_uring_prep_fsync(sqe, file->fd, 0);
    sqe->user_data = 0;

    // 提交请求并等待 fsync 完成。
    int submitted = io_uring_submit(&file->ring);
    if (submitted < 1) {
        return -1;
    }

    int res = kvs_io_uring_wait_one(&file->ring);
    return res < 0 ? -1 : 0;
}

int kvs_io_uring_close(kvs_io_uring_file_t* file) {
    // 校验文件对象。
    if (file == NULL) {
        return -1;
    }

    int ret = 0;
    // 关闭底层文件描述符。
    if (file->fd >= 0) {
        if (close(file->fd) < 0) {
            ret = -1;
        }
    }

    // 销毁 io_uring 队列并释放文件对象。
    io_uring_queue_exit(&file->ring);
    free(file);
    return ret;
}
