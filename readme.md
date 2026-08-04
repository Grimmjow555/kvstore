分别基于 reactor(epoll), proactor(io_uring) 以及协程框架（NtyCo）实现的kv存储。

编译：

gcc -c ntyco.c -I NtyCo-master/core/ -o ntyco.o && \
g++ -c proactor.cpp -o proactor.o && \
g++ -c reactor.cpp -o reactor.o && \
g++ -c kvstore.cpp -o kvstore.o && \
g++ kvstore.o proactor.o reactor.o ntyco.o -o kvstore -luring -L NtyCo-master/ -lntyco

运行：

./kvstore 端口号 网络架构

网络架构：0代表reactor(epoll)，1代表协程（NtyCo），2代表 proactor(io_uring)

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
