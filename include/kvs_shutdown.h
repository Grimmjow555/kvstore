#pragma once

// 优雅关闭的停止标志。
//
// 三个网络后端（reactor / proactor / ntyco）的服务循环都是常驻不返回的，
// 进程收到 SIGINT/SIGTERM 时若不做处理会被默认动作直接杀掉，main 末尾的
// 收尾路径（AOF 落盘、复制线程回收、内存池销毁）永远不会执行。
//
// 这里的做法是：信号处理函数只置位 g_kvs_shutdown（异步信号安全），
// 网络循环在阻塞返回后检查该标志，退出循环并回到 main 完成收尾。
// 注意信号可能被投递到任意一个线程，不能只依赖 EINTR 去唤醒事件循环线程：
// reactor / proactor 改用带超时的等待；ntyco 的 accept/recv 等待无法设置
// 超时（见 network/ntyco.c 的说明），因此由额外的关闭协程负责唤醒。

#include <signal.h>

#ifdef __cplusplus
extern "C" {
#endif

// 各后端事件循环的阻塞上限（毫秒）。必须有限，否则信号落在别的线程时
// 事件循环可能一直等不到唤醒，停止标志迟迟不被观察到。
#ifndef KVS_SHUTDOWN_POLL_MS
#define KVS_SHUTDOWN_POLL_MS 200
#endif

// 非 0 表示已请求关闭。只允许信号处理函数写，服务循环读。
extern volatile sig_atomic_t g_kvs_shutdown;

// 安装 SIGINT/SIGTERM 处理函数，并忽略 SIGPIPE（避免对端断开时进程被杀死）。
void kvs_shutdown_install_handlers(void);

#ifdef __cplusplus
}
#endif
