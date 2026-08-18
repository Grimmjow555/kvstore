kvstore
-

网络框架: reactor(epoll), proactor(io_uring) 以及协程框架(NtyCo)

存储引擎: array, rbtree, hash, skiptable

使用jemalloc分配内存

使用RESP协议解析指令，允许key/value中存在特殊符号（如空格、回车），允许一次性接收批量指令



----

编译: 

cd build

cmake ..

make -j$(nproc)

运行: 

./kvstore <端口号> <网络架构>

网络架构: 0 代表 reactor(epoll) , 1 代表协程框架(NtyCo), 2 代表 proactor(io_uring)


