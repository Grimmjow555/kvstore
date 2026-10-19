#pragma once

#include <signal.h>

// 网络框架不再用宏做编译期选择：三个后端都会编入同一个二进制，启动时由
// kvstore.conf 的 network_architecture（或命令行 --network）决定运行哪一个，
// 未配置时的默认值写在 config/kvs_config.cpp 的 kvs_config_set_defaults() 中。
// 运行时可选的后端名称：reactor(epoll) / ntyco(协程) / proactor(io_uring)，
// 对应 include/kvs_config.h 中的 kvs_network_t。

// 三个后端共用的帧限制。外层协议由 4 字节长度头和负载组成，负载上限为 1MB。
#define KVS_NET_MAX_MESSAGE_SIZE (1024u * 1024u)
#define KVS_NET_INITIAL_BUFFER_SIZE 1024u

// 各后端事件循环的阻塞上限（毫秒）。信号可能落在任意线程，因此等待必须有界，
// 以便事件循环能观察到停止标志并返回 main 完成收尾。
#ifndef KVS_SHUTDOWN_POLL_MS
#define KVS_SHUTDOWN_POLL_MS 200
#endif

// 协议处理回调：msg 不以 '\0' 结尾，长度由 length 指定。
typedef int (*msg_handler)(char* msg, int length, char* response, int response_size);

#ifdef __cplusplus
extern "C" {
#endif

// 启动指定网络后端。成功进入事件循环并正常退出时返回 0，初始化失败返回 -1。
int reactor_start(const char* bind_ip, unsigned short port, msg_handler handler);
int ntyco_start(const char* bind_ip, unsigned short port, msg_handler handler);
int proactor_start(const char* bind_ip, unsigned short port, msg_handler handler);

// 优雅关闭控制。信号处理函数只置位停止标志，服务循环观察到后退出。
extern volatile sig_atomic_t g_kvs_shutdown;
void kvs_shutdown_install_handlers(void);

#ifdef __cplusplus
}
#endif
