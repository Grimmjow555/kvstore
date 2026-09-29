# AGENTS.md

本文档面向在本仓库中工作的 coding agent。它概括项目结构、构建方式、通信协议和跨模块约定；修改代码前请先阅读本文件，避免破坏既有协议和复制/持久化行为。

## 项目概览

`kvstore` 是一个基于 RESP 协议、支持四种内存存储引擎、主从复制以及 RDB/AOF 持久化的 KV 服务端项目。

- 网络框架：reactor（epoll）、proactor（io_uring）、协程框架 NtyCo，通过编译期宏选择。
- 存储引擎：array、rbtree、hash、skiptable，分别对应一套 `SET/GET/DEL/MOD/EXIST` 风格命令。
- 内存分配：业务代码统一走 `kvs_malloc` / `kvs_calloc` / `kvs_free`；由
  `include/memorypool.h` 的编译期宏 `ENABLE_MEMORYPOOL` 选择：`1` 使用内置 slab 内存池，
  `0` 直接使用系统 `malloc` / `calloc` / `free`（若构建时链接了 jemalloc，则由其接管）。
  该选择在编译期确定，运行期不能切换。
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

NtyCo 源码通过 `NTYCO_SOURCES` 直接随 `kvstore` 一起编译，不依赖 submodule 内的
`libntyco.a`（该产物不在版本库中，且 `objs/`、`lib/` 已被 submodule 的 `.gitignore` 忽略）。
因此不需要手工在 `NtyCo/` 下执行 `make`。

可通过 CMake 变量覆盖 NtyCo 路径：`NTYCO_ROOT`、`NTYCO_INCLUDE_DIR`。不要使用 README 中的 `URING_ROOT`，该变量未在 [CMakeLists.txt](CMakeLists.txt) 中定义。

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
├── NtyCo/                  # Git submodule，第三方协程库
├── MASTER_kvstore/         # 独立的旧版 C 实现，不属于当前服务
└── readme.md
```

`build/` 是 CMake 生成目录；`NtyCo/` 是第三方 submodule；`MASTER_kvstore/` 使用另一套实现、协议和构建方式。正常修改当前服务时不要改动这些目录中的产物或第三方代码。

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

内存所有权：引擎在写入时复制 key/value，由引擎负责释放；调用方传入的字符串不会被接管。因此协议层的 `argv` 在命令执行后仍需单独释放。所有分配应使用 `kvs_malloc` / `kvs_calloc` / `kvs_free`，并且必须用同一分配器释放：底层由编译期宏 `ENABLE_MEMORYPOOL` 决定是系统 `malloc`/`free` 还是内置 slab 内存池。

编译期开关：

- 各存储引擎头部有 `ENABLE_ARRAY` / `ENABLE_RBTREE` / `ENABLE_HASH` / `ENABLE_SKIPTABLE`。
- `include/memorypool.h` 的 `ENABLE_MEMORYPOOL` 决定 `kvs_malloc` / `kvs_calloc` /
  `kvs_free` 是否使用内置 slab 内存池：`1` 使用，`0` 直接走系统 `malloc` / `calloc` / `free`。
- `include/aof.h` 有 `AOF_ENABLE`。
- 网络后端不再用编译期宏选择：CMake 会同时编译 `network/` 下三个源文件，运行时由 `kvstore.conf` 的 `network_architecture`（或命令行 `--network`）选择 reactor / ntyco / proactor，未配置时默认 ntyco。切换后端不需要重新编译，但必须做协议级验证。

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

- Master 维护最多 `MAX_REPLICAS`（16）个连接槽位，每个槽位记录 fd、状态和已投递序号。
- Replica 发送握手 `*1\r\n$7\r\nREPLICA\r\n`（外层仍是 4 字节长度前缀帧）；Master 回 `+OK` 后由
  `kvs_replication_finish_handshake` 启动全量同步。
- 每次写成功都会把 RESP 命令追加到全局增量日志 backlog（上限 `REPL_BACKLOG_MAX_BYTES`，
  超出丢弃最老记录，落后过多的副本会被断开重连后重新全量同步）。独立线程
  `repl_sender_thread` 按每个副本的 `next_seq` 用 `MSG_DONTWAIT` 投递，事件循环不做阻塞发送。
- 全量同步复用 `kvs_snapshot_serialize()` 的 RDB 二进制内容分片下发，Replica 用
  `kvs_snapshot_load_buffer()` 加载。快照与 backlog 序号在同一把存储锁内确定
  （`build_tcp_full_sync_prefix`），保证快照之后落地的写命令仍能补发。
- 复制帧负载首字节是类型字段，见 `include/kvs_replication.h` 的 `KVS_REPL_FRAME_*`；
  握手帧与 `+OK` 仍是不带类型的网络层帧，Replica 对未知类型直接忽略（兼容该 `+OK`）。
- fd 归网络层所有：复制模块只 `shutdown()` 触发断开，网络层在各后端断连路径调用
  `kvs_replication_remove_replica()` 后再 `close()`，避免 fd 复用把复制帧写进普通客户端。
- 启动全量同步时不能截断发送线程尚未发完的数据（只能回收已发送部分再追加新前缀），
  否则对端会卡在半个帧上导致整条流错位。
- Replica 侧由监督线程负责连接/重连（指数退避）并在连接内回放命令；回放用线程局部
  `replication_replaying` 标记，避免误伤本地客户端的 AOF 记录。
- 实时增量由 eBPF 采集（`KVS_ENABLE_EBPF_REALTIME=1`），链路是
  `kvs_ebpf_notify_write()`（uprobe 挂载点，在存储锁内调用）-> `BPF_PROG_TYPE_KPROBE` 程序读
  `pt_regs` 里的 `(seq, argc, argv)` 并写 `BPF_MAP_TYPE_RINGBUF` -> 消费线程 -> backlog ->
  发送线程。程序用裸 `bpf(2)` 加载、`perf_event_open` + `PERF_EVENT_IOC_SET_BPF` attach，
  不依赖 libbpf/bpftool。触发点用 uprobe 而不是 kprobe：kprobe 只能挂内核函数，既读不到应用层
  参数，也会在 proactor(io_uring) 后端下漏事件。跨主机可用——eBPF 只负责本机采集，投递仍走
  TCP/RDMA，不再要求两个进程同内核（这是与已被删除的 `BPF_MAP_TYPE_QUEUE` 方案的本质区别）。
  ringbuf 队列满时 `reserve` 会失败导致丢事件，因此记录带自增序号，消费线程发现序号缺口就
  `kvs_replication_resync()`，绝不静默丢命令；参数超过 `KVS_EBPF_EVENT_ARG_MAX`(1024) 或参数
  多于 3 个的命令不走采集、直接落 backlog，但落之前必须先 `kvs_ebpf_wait_drained()` 排队，
  否则会插到前序命令前面（全量快照取 `base_seq` 前同理）。任何 attach 失败都自动回退为
  「写路径直接落 backlog」。改这些常量/接口时需同步 `readme.md`、`setup_ebpf.sh` 与
  `include/kvs_ebpf.h`。
- RDMA 只用于全量快照搬运（`KVS_ENABLE_RDMA`，Master 监听 TCP 端口+1）：Replica 先 RDMA 拉快照，
  再发 TCP 握手，Master 按记录的快照序号补发增量。实现用普通 `IBV_WR_RDMA_WRITE` + 单独一条
  SEND 通知，**不要改回 `IBV_WR_RDMA_WRITE_WITH_IMM`**：SoftiWARP 下 `ibv_post_send` 会返回
  ENOSPC(28)。Replica 的 `master_ip` 必须是 RDMA 网卡 IP，回环地址无法 resolve。

存储引擎没有内部锁，所有存储访问必须经过 `kvstore.cpp` 的 `kvs_data_lock()` /
`kvs_data_unlock()`（可重入锁）。当前加锁点：命令分发 `kvs_filter_protocol`、Replica 回放与
SNAPSHOT 加载、RDMA 快照序列化、全量快照序列化。锁顺序固定为
存储锁 -> 复制锁（`g_repl_mutex`），不得反向获取。

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
- 不要修改 `NtyCo/` 内的第三方代码；需要升级时通过 submodule 流程处理，并保持 `.gitmodules` 一致。
- 不要删除或重命名现有公共 API；如需调整，先搜索所有调用点，尤其是 `kvstore.cpp`、`persistence/`、`replication/` 和 `testcase/`。
- 提交前至少完成一次干净构建，并尽量用 `testcase/` 客户端做协议级验证。

## 快速排查提示

- 连接成功但响应异常：先确认客户端是否按“4 字节长度头 + RESP 负载”收发，而不是直接发送裸 RESP。
- 持久化文件找不到：确认进程工作目录是否为 `build/`，或路径是否相对当前目录解析。
- 编译时缺 NtyCo/liburing/jemalloc：按 readme 初始化 submodule 并安装系统依赖。
- 修改命令后测试失败：检查 `command[]`、`KVS_CMD` 枚举和 `kvs_filter_protocol` 三处是否一致。
