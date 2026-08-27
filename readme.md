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

./kvstore <端口号> <网络架构> <角色> [主节点IP] [主节点端口]

网络架构: 0 代表 reactor(epoll) , 1 代表协程框架(NtyCo), 2 代表 proactor(io_uring)

角色: 0 代表主节点, 1 代表从节点

服务器启动时不自动加载数据，主节点收到 `LOAD`、`RLOAD`、`HLOAD` 或
`SLOAD` 指令并成功执行后，会向在线从节点发送重置命令和四类快照，完成全量同步，
随后继续发送实时写命令。`append.aof` 仅用于增量日志，不参与启动恢复。

示例:

./kvstore 19001 0 0
./kvstore 19002 0 1 127.0.0.1 19001


