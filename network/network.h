// 选择网络框架
enum NETWORK_ARCHITECTURE { NETWORK_REACTOR = 0, NETWORK_NTYCO = 1, NETWORK_PROACTOR = 2 };

// 协议解析函数
typedef int (*msg_handler)(char* msg, int length, char* response, int response_sizes);

/*--------------------------------------------------------------------*/
// reactor启动函数：监听的端口，解析协议
extern int reactor_start(unsigned short port, msg_handler handler);

#ifdef __cplusplus // ntyco_start是纯C编写的，用C++编译时需要以兼容C的模式处理
extern "C" {
#endif

// ntyco启动函数：监听的端口，解析协议
extern int ntyco_start(unsigned short port, msg_handler handler);

#ifdef __cplusplus
}
#endif

// uring启动函数：监听的端口，解析协议
extern int proactor_start(unsigned short port, msg_handler handler);
/*--------------------------------------------------------------------*/