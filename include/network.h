#pragma once

// 网络框架不再用宏做编译期选择：三个后端都会编入同一个二进制，启动时由
// kvstore.conf 的 network_architecture（或命令行 --network）决定运行哪一个，
// 未配置时的默认值写在 config/kvs_config.cpp 的 kvs_config_set_defaults() 中。
// 运行时可选的后端名称：reactor(epoll) / ntyco(协程) / proactor(io_uring)，
// 对应 include/kvs_config.h 中的 kvs_network_t。

// 协议解析函数
typedef int (*msg_handler)(char* msg, int length, char* response, int response_sizes);

/*--------------------------------------------------------------------*/
// reactor启动函数：监听的端口，解析协议
extern int reactor_start(const char* bind_ip, unsigned short port, msg_handler handler);

#ifdef __cplusplus // ntyco_start是纯C编写的，用C++编译时需要以兼容C的模式处理
extern "C" {
#endif

// ntyco启动函数：监听的端口，解析协议
extern int ntyco_start(const char* bind_ip, unsigned short port, msg_handler handler);

#ifdef __cplusplus
}
#endif

// uring启动函数：监听的端口，解析协议
extern int proactor_start(const char* bind_ip, unsigned short port, msg_handler handler);
/*--------------------------------------------------------------------*/
