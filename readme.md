分别基于 reactor(epoll), proactor(io_uring) 以及协程框架（NtyCo）实现的kv存储。

默认监听端口2000

reactor
-
g++ reactor.cpp kvstore.c -o reactor

./reactor

proactor
-
g++ uring.cpp kvstore.c -o uring -luring

./uring

协程
-
gcc hook_tcpserver.c kvstore.c -o hook_tcpserver -I NtyCo-master/core/ -L NtyCo-master/ -lntyco

./hook_tcpserver 2000
