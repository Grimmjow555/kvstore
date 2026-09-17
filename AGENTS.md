# AGENTS.md

本文档面向在本仓库中工作的 coding agent。它概括项目结构、构建方式、通信协议和跨模块约定；修改代码前请先阅读本文件，避免破坏既有协议和复制/持久化行为。

## 项目概览

`kvstore` 是一个基于 RESP 协议、支持四种内存存储引擎、主从复制以及 RDB/AOF 持久化的 KV 服务端项目。

- 网络框架：reactor（epoll）、proactor（io_uring）、协程框架 NtyCo，通过编译期宏选择。
- 存储引擎：array、rbtree、hash、skiptable，分别对应一套 `SET/GET/DEL/MOD/EXIST` 风格命令。
- 内存分配：业务代码统一走 `kvs_malloc` / `kvs_free`，当前底层为 `malloc` / `free`。
- 持久化：RDB 全量快照 + AOF 增量日志，启动后默认不自动恢复，需通过命令手动触发。
- 复制：Master/Replica 角色由命令行参数决定，支持握手、全量同步和增量同步。

## 构建与运行

依赖：CMake 3.10+、C/C++ 编译器、pthread、liburing、jemalloc，以及通过 Git submodule 接入的 NtyCo。

首次准备 submodule：

```bash
git submodule sync --recursive
git submodule update --init --recursive
```

构建：

```bash
cmake -S . -B build
cmake --build build -j$(nproc)
```

可通过 CMake 变量覆盖 NtyCo 路径：`NTYCO_ROOT`、`NTYCO_INCLUDE_DIR`、`NTYCO_LIBRARY_DIR`。不要使用 README 中的 `URING_ROOT`，该变量未在 [CMakeLists.txt](CMakeLists.txt) 中定义。

产物为 `build/kvstore`。运行时请从 `build` 目录启动，因为持久化路径使用的是 `../data/...`。

```bash
# Master
./kvstore 9999 0

# Replica
./kvstore 9999 1 127.0.0.1 19001
```

参数顺序：`<监听端口> <角色> [主节点IP] [主节点端口]`，角色 `0` 为 Master，`1` 为 Replica。

CMake 只构建主服务，不构建 `testcase/` 下的测试客户端。`testcase/` 中既有源码也有历史编译产物，修改测试代码后需手动重编译对应客户端。

协议级验证可先从 `build/` 启动服务，再手动编译并运行对应的 `testcase/*.cpp` 客户端；客户端的帧封装和接收逻辑可参考 [testcase/testcase.h](testcase/testcase.h) 或 [testcase/AOF/testcase.h](testcase/AOF/testcase.h)。测试客户端不是 CMake target，运行前确认其命令使用当前命令表，不要默认信任目录中的旧二进制。

修改快照格式、`kvs_reset_data()` 或任一存储引擎的 destroy 逻辑后，至少执行一次 RDB SAVE/LOAD 回归；LOAD 会先销毁现有数据，再解析文件，清理路径的问题通常表现为服务启动或 LOAD 时的段错误。

## 目录结构

```text
.
├── CMakeLists.txt          # 主服务构建配置
├── kvstore.cpp             # 入口、RESP 解析、命令分发
├── include/                # 公共头文件与编译期开关
├── network/                # reactor / proactor / ntyco 三种网络后端
├── storage/                # array / rbtree / hash / skiptable
├── persistence/            # snapshot（RDB）与 aof
├── replication/            # 主从复制
├── testcase/               # 测试客户端源码与历史产物
├── data/                   # 运行时持久化文件目录
├── NtyCo-master/           # Git submodule，第三方协程库
├── MASTER_kvstore/         # 独立的旧版 C 实现，不属于当前服务
└── readme.md
```

`build/` 是 CMake 生成目录；`NtyCo-master/` 是第三方 submodule；`MASTER_kvstore/` 使用另一套实现、协议和构建方式。正常修改当前服务时不要改动这些目录中的产物或第三方代码。

## 通信协议

服务端采用两层协议，这一点最容易造成错误，请务必保持一致：

1. 外层是长度前缀帧：4 字节网络字节序的无符号长度，后跟该长度的负载。
2. 内层负载是 RESP 数组/批量字符串格式，例如：

```text
*3\r\n
$3\r\nSET\r\n
$3\r\nkey\r\n
$5\r\nvalue\r\n
```

请求和响应都使用同样的帧格式。`network/` 中的三个后端都实现了半包处理，客户端可参考 `testcase/AOF/testcase.h` 中的 `send_msg` / `recv_msg`。

实现位置：

- `kvstore.cpp` 的 `resp_parse_command` 负责从 RESP 负载中解析 `argv`。
- `kvstore.cpp` 的 `kvs_protocol` 支持一次负载中包含多条命令（粘包），并拼接多条响应。
- `kvstore.cpp` 的 `kvs_filter_protocol` 负责按 `tokens[0]` 分发到具体存储引擎或持久化命令。

消息体上限 `MAX_ALLOWED_LEN` 为 1MB，超限会拒绝或断开连接。

RESP 解析目前只接受完整的数组和 bulk string；不支持 null bulk string。修改协议解析或网络后端时，必须同时考虑半包、粘包、长度前缀和 1MB 限制。

## 命令与响应约定

命令表维护在 `kvstore.cpp` 的 `command[]` 和 `enum KVS_CMD` 中。新增命令时需要同时修改这两处，并在 `kvs_filter_protocol` 中补充分支。

- array：`SET GET DEL MOD EXIST`
- rbtree：`RSET RGET RDEL RMOD REXIST`
- hash：`HSET HGET HDEL HMOD HEXIST`
- skiptable：`SSET SGET SDEL SMOD SEXIST`
- 持久化：`RDB SAVE`、`RDB LOAD`、`AOF LOAD`、`AOF CLEAR`

注意：`RDB SAVE`、`AOF LOAD` 等带空格的命令是一个整体 token，不是两个参数。

响应约定：

- 写成功：`+OK\r\n`；写入时键已存在：`+EXIST\r\n`；错误：`-ERROR\r\n`。
- `GET` 成功：批量字符串，如 `$5\r\nvalue\r\n`；键不存在：`$8\r\nNO EXIST\r\n`。
- `DEL` / `MOD` 不存在：`$8\r\nNO EXIST\r\n`。
- `EXIST`：存在返回 `$5\r\nEXIST\r\n`，不存在返回 `$8\r\nNO EXIST\r\n`。
- `RDB SAVE/LOAD`、`AOF LOAD/CLEAR`：`+OK\r\n` 或 `-ERROR\r\n`。

## 存储引擎约定

每个引擎在对应 `storage/kvs_*.cpp` 中定义自己的全局实例：

- `global_array`
- `global_rbtree`
- `global_hash`
- `global_skiptable`

公共接口语义：

- `set`：`0` 成功，`>0` 键已存在，`<0` 错误。
- `get`：返回内部 value 指针，不存在或参数错误返回 `NULL`；不要释放该指针。
- `del` / `mod`：`0` 成功，`>0` 不存在，`<0` 错误。
- `exist`：`0` 存在，`>0` 不存在，`<0` 错误。

内存所有权：引擎在写入时复制 key/value，由引擎负责释放；调用方传入的字符串不会被接管。因此协议层的 `argv` 在命令执行后仍需单独释放。所有分配应使用 `kvs_malloc` / `kvs_free`。

编译期开关：

- 各存储引擎头部有 `ENABLE_ARRAY` / `ENABLE_RBTREE` / `ENABLE_HASH` / `ENABLE_SKIPTABLE`。
- `include/aof.h` 有 `AOF_ENABLE`。
- `include/network.h` 有 `USE_REACTOR` / `USE_NTYCO` / `USE_PROACTOR`，当前 `USE_NTYCO=1`。CMake 会同时编译三个网络源文件，但运行时只走被启用的那一个；切换后必须重新构建并做协议级验证。

修改这些宏时，要同步考虑对应模块是否被 CMake 编译，以及快照/复制逻辑中的 `#if` 条件。

## 持久化

RDB 实现位于 `persistence/snapshot.cpp`：

- 文件头包含 magic `KVSDB01`、version `1`、区块数。
- 每个存储引擎一个 section，section 头包含 type 和 count。
- record 由 key 长度、value 长度、key 数据、value 数据组成。
- `kvs_snapshot_load` 会先 `kvs_reset_data()`，再按 section 顺序恢复数据。
- `kvs_snapshot_save` 会先在内存中完整序列化快照，再通过 io_uring 一次性写入并 `fsync`。

AOF 实现位于 `persistence/aof.cpp`：

- 写命令在成功后追加为 RESP 文本。
- AOF 增量先写入内存缓冲区，达到阈值或执行 `AOF LOAD` / `AOF CLEAR` / 关闭服务时批量写盘并 `fsync`。
- `AOF LOAD` 读取并重放 `data/append.aof`。
- `AOF CLEAR` 关闭当前 AOF 文件并以 `w` 模式重新打开清空。

重要约束：

- 持久化路径在代码中为 `../data/kvstore.data` 和 `../data/append.aof`，因此应从 `build/` 目录启动进程。
- 服务启动时不会自动加载持久化数据；需要显式执行 `RDB LOAD` 或 `AOF LOAD`。
- `RDB LOAD` / `AOF LOAD` 成功后会调用 `kvs_replication_resync()`，让在线 Replica 重新全量同步。
- RDB 使用原生整数布局，且当前记录长度依赖 `strlen`；不要把嵌入 NUL 的 key/value 或跨主机移植作为已支持行为。
- AOF 重放遇到解析失败可能提前停止但仍返回成功，修改加载逻辑时应保留并覆盖该错误路径。
- AOF 当前使用批量落盘，进程被强制杀死时最多可能丢失未达刷新阈值的内存缓冲数据；优雅关闭或执行 `AOF LOAD`/`AOF CLEAR` 会先刷新缓冲区。

## 复制

实现位于 `replication/kvs_replication.cpp`。

- Master 维护最多 `MAX_REPLICAS`（16）个 Replica 连接。
- Replica 连接后发送握手命令 `*1\r\n$7\r\nREPLICA\r\n`。
- 握手完成后，Master 遍历四个存储引擎，将数据编码为 `SET` / `RSET` / `HSET` / `SSET` 命令下发，完成全量同步。
- 正常写命令通过 `kvs_replication_append` 增量广播给已完成全量同步的 Replica。
- 需要重同步时，Master 发送 `REPLICA_RESET`，随后发送全量快照。
- Replica 侧由独立线程接收帧并调用 `kvs_protocol` 回放；回放期间设置 replaying 标志。

避免复制/AOF 回环的核心条件在 `kvstore.cpp` 的写分支中：

```cpp
if (!kvs_aof_is_replaying() && !kvs_replication_is_replaying()) {
    kvs_aof_append(...);
}
if (!kvs_replication_is_replaying()) {
    kvs_replication_append(...);
}
```

新增写命令时不要绕过这两个判断，否则会导致 AOF 重复记录或 Master/Replica 相互转发。

## 代码风格与工作约定

- 主服务为 C++11，但部分网络/第三方代码为 C；混合编译是项目现状，不要强行统一。
- 注释以中文为主，新增代码建议沿用清晰的中文说明。
- 不要修改 `NtyCo-master/` 内的第三方代码；需要升级时通过 submodule 流程处理，并保持 `.gitmodules` 一致。
- 不要删除或重命名现有公共 API；如需调整，先搜索所有调用点，尤其是 `kvstore.cpp`、`persistence/`、`replication/` 和 `testcase/`。
- 提交前至少完成一次干净构建，并尽量用 `testcase/` 客户端做协议级验证。

## 快速排查提示

- 连接成功但响应异常：先确认客户端是否按“4 字节长度头 + RESP 负载”收发，而不是直接发送裸 RESP。
- 持久化文件找不到：确认进程工作目录是否为 `build/`，或路径是否相对当前目录解析。
- 编译时缺 NtyCo/liburing/jemalloc：按 readme 初始化 submodule 并安装系统依赖。
- 修改命令后测试失败：检查 `command[]`、`KVS_CMD` 枚举和 `kvs_filter_protocol` 三处是否一致。
