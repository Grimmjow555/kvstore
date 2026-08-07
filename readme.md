kv存储
-

网络框架: reactor(epoll), proactor(io_uring) 以及协程框架(NtyCo)

引擎: array, rbtree, hash 

编译: 

cd build

cmake ..

make -j$(nproc)

运行: 

./kvstore <端口号> <网络架构>

网络架构: 0 代表 reactor(epoll) , 1 代表协程框架(NtyCo), 2 代表 proactor(io_uring)


