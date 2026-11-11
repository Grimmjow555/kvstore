# AGENTS.md

本文档面向在本仓库中工作的 coding agent。它概括项目结构、构建方式、通信协议和跨模块约定；修改代码前请先阅读本文件，避免破坏既有协议和复制/持久化行为。

## 项目概览

`kvstore` 是一个基于 RESP 协议、支持四种内存存储引擎、主从复制以及 RDB/AOF 持久化的 KV 服务端项目。

- 网络框架：reactor（epoll）、proactor（io_uring）、协程框架 NtyCo。三种后端都编入同一
  个二进制，启动时由 `network_architecture` 或 `--network` 选择，运行期不能切换。
- 存储引擎：array、rbtree、hash、skiptable，分别对应一套 `SET/GET/DEL/MOD/EXIST` 风格命令。
- 内存分配：业务代码统一走 `kvs_malloc` / `kvs_calloc` / `kvs_free`；由
  `memorypool/memorypool.h` 的编译期宏 `ENABLE_MEMORYPOOL` 选择：`1` 使用内置 slab 内存池，
  `0` 直接使用系统 `malloc` / `calloc` / `free`（若构建时链接了 jemalloc，则由其接管）。
  该选择在编译期确定，运行期不能切换。
- 持久化：RDB 全量快照 + AOF 增量日志，二者互斥；启用后服务启动时自动加载，运行期只提供
  `RDB SAVE` 和 `AOF CLEAR`。
- 复制：Master/Replica 角色由命令行参数决定；全量同步由 `SYNC` 驱动、快照经独立
  `rdma-client`/`rdma-server` 搬运，增量由独立 `ebpf-agent` 采集与转发（见「复制」一节）。

## 构建与运行

必需依赖：CMake 3.10+、C/C++ 编译器、pthread、liburing、OpenSSL Crypto，以及通过
Git submodule 接入的 NtyCo。

可选依赖：

- libibverbs / librdmacm：可用时构建 `rdma-client` / `rdma-server`；没有开发库时 CMake
  只跳过这两个目标。
- SoftiWARP：没有 RDMA 硬件时用 `setup_rdma.sh` 创建软件 RDMA 链路。
- Python 3：部分测试脚本用于发送协议帧或注入故障。

Ubuntu/Debian 可安装：

```bash
sudo apt-get install -y build-essential cmake liburing-dev libssl-dev
```

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

主要产物：

- 服务端：`build/kvstore`
- 增量采集：`build/ebpf-agent`
- RDMA 搬运（依赖可用时）：`build/rdma-client`、`build/rdma-server`
- 测试客户端：`build/testcase/<case>/...`

运行时请从 `build` 目录启动主服务，因为持久化路径使用的是 `../data/...`。

```bash
# Master
./kvstore --port 9999 --role master

# Replica
./kvstore --port 9999 --role replica --master-ip 127.0.0.1 --master-port 19001
```

启动参数只接受命名开关或配置文件，不接受旧的位置参数形式。

CMake 会同时构建主要测试目标，产物位于 `build/testcase/` 下。测试脚本、端口、环境变量和
异常场景说明见 [testcase/README.md](testcase/README.md)。

修改测试代码后直接重建对应 CMake target，不要依赖目录中的旧二进制：

```bash
cmake --build build --target rdb_save rdb_load aof_save aof_load -j"$(nproc)"
cmake --build build --target integrity_test integrity_client -j"$(nproc)"
```

测试协议外层是“4 字节网络字节序长度 + RESP 负载”。测试协议辅助函数目前位于对应测试
源码中；如果后续抽取为头文件，应放在对应的 `testcase/<case>/` 目录，不要放进生产 `include/`。

修改快照格式、`kvs_reset_data()` 或任一存储引擎的 destroy 逻辑后，至少执行一次
`testcase/run_rdb_cycle.sh` 和 `testcase/run_persistence_integrity.sh`。快照加载现在先校验
完整性，再重置并解析数据；不要只验证“文件能打开”。

## 目录结构

```text
.
├── CMakeLists.txt          # 主服务构建配置
├── kvstore.cpp             # 入口、RESP 解析、命令分发
├── include/                # 公共头文件与编译期开关
├── network/                # reactor / proactor / ntyco 三种网络后端
├── storage/                # array / rbtree / hash / skiptable
├── persistence/            # snapshot（RDB）与 aof
├── replication/            # 复制控制流、采集环、agent 侧采集、RDMA 搬运
├── tools/                  # 独立进程：rdma-client / rdma-server / ebpf-agent
├── testcase/               # 测试客户端、一键脚本和测试说明
├── docs/                   # 复制设计与补充文档
├── data/                   # 运行时持久化文件目录
├── NtyCo/                  # Git submodule，第三方协程库
├── setup_rdma.sh           # SoftiWARP 链路初始化
└── readme.md
```

`build/` 和 `data/` 是运行时目录；`NtyCo/` 是第三方 submodule。不要手工修改构建产物，
也不要为了测试覆盖仓库中已有的持久化文件。

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

请求和响应都使用同样的帧格式。`network/` 中的三个后端都实现了半包处理；测试客户端的
`send_msg` / `recv_msg` 实现可直接参考对应测试 `.cpp` 文件或
`testcase/integrity/client.cpp`。

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
- 持久化：`RDB SAVE`、`AOF CLEAR`
- 复制控制：`SYNC`、`SYNC FINISHED`

注意：`RDB SAVE`、`AOF CLEAR`、`SYNC FINISHED` 等带空格的命令是一个整体 token，
不是两个参数。

响应约定：

- 写成功：`+OK\r\n`；写入时键已存在：`+EXIST\r\n`；错误：`-ERROR\r\n`。
- `GET` 成功：批量字符串，如 `$5\r\nvalue\r\n`；键不存在：`$8\r\nNO EXIST\r\n`。
- `DEL` / `MOD` 不存在：`$8\r\nNO EXIST\r\n`。
- `EXIST`：存在返回 `$5\r\nEXIST\r\n`，不存在返回 `$8\r\nNO EXIST\r\n`。
- `RDB SAVE`、`AOF CLEAR`、`SYNC FINISHED`：`+OK\r\n` 或 `-ERROR\r\n`。
- `SYNC` 成功返回 `+OK <16 位十六进制 session_id>\r\n`，失败返回 `-ERROR\r\n`。

运行期不再提供 `RDB LOAD` / `AOF LOAD`。恢复只发生在服务启动阶段。

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
- `memorypool/memorypool.h` 的 `ENABLE_MEMORYPOOL` 决定 `kvs_malloc` / `kvs_calloc` /
  `kvs_free` 是否使用内置 slab 内存池：`1` 使用，`0` 直接走系统 `malloc` / `calloc` / `free`。
- `include/kvs_aof.h` 有 `AOF_ENABLE`。
- 网络后端不再用编译期宏选择：CMake 会同时编译 `network/` 下三个源文件，运行时由 `kvstore.conf` 的 `network_architecture`（或命令行 `--network`）选择 reactor / ntyco / proactor，未配置时默认 ntyco。切换后端不需要重新编译，但必须做协议级验证。

修改这些宏时，要同步考虑对应模块是否被 CMake 编译，以及快照/复制逻辑中的 `#if` 条件。

## 持久化

持久化路径固定在 `../data/`：

```text
RDB：../data/kvstore.data
AOF：../data/append.aof
```

因此必须从 `build/` 目录启动主服务。服务启动顺序是：初始化引擎和复制模块、自动加载
持久化文件、初始化 AOF 追加端、启动网络监听或 Replica 同步线程。文件不存在表示首次
启动；文件存在但格式、长度或摘要校验失败时，必须中止启动，不能跳过错误继续提供服务。

RDB 实现位于 `persistence/snapshot.cpp`：

- 当前格式为 `KVSDB02`、version `2`，与旧 `KVSDB01` 不兼容。
- 文件由文件头、每个存储引擎一个 section、若干记录组成。
- section 头包含引擎 type 和 count；record 包含 key 长度、value 长度、key 和 value。
- 所有内容写完后追加 32 字节 SHA-256，覆盖此前的文件头和全部记录。
- 加载时先验证文件长度、magic/version 和整文件 SHA-256，再调用 `kvs_reset_data()`。
- section 解析必须恰好消耗整个 payload；存在未解释的尾部字节时同样失败。
- `kvs_snapshot_save` 在内存中完成序列化后，通过 io_uring 写入并 `fsync`。
- 主从 `SYNC` 使用同一套序列化格式，因此 Replica 加载同步快照时也会验证 SHA-256。

AOF 实现位于 `persistence/aof.cpp`：

- 文件以 `#KVSAOF02\r\n` 开头。
- 每条写命令保存为原始 RESP 字节，随后追加 64 位十六进制 SHA-256 和 CRLF。
- 每条摘要使用哈希链：
  `Hi = SHA256(Hi-1 || uint64_le(record_len) || record_bytes)`。
- `AOF CLEAR` 刷新旧缓冲、截断文件、写入新文件头并重置链状态。
- AOF 增量先进入 1024 字节阈值缓冲；达到阈值或关闭服务时批量写盘并 `fsync`。
- 加载时逐条验证 RESP、摘要长度、摘要链和文件完整结束位置，任何错误都返回失败。
- 重启后必须用最后一条有效记录的摘要继续追加，不能从全零链重新开始。

重要约束：

- `KVSDB01` 快照和无文件头的旧 AOF 会被明确拒绝；当前不提供迁移工具。
- SHA-256 未使用密钥，可发现误写、截断和普通手工篡改，但不能抵御能够重算摘要的攻击者。
- RDB 和 AOF 记录长度依赖 `strlen`，不支持嵌入 NUL 的 key/value；RDB 原生整数布局也不保证
  跨主机字节序移植。
- AOF 批量落盘意味着强制杀死进程可能丢失尚未刷新的缓冲；优雅关闭或 `AOF CLEAR` 会先刷新。

## 测试与验证

主要 CMake 测试目标：

- 持久化：`rdb_save`、`rdb_load`、`aof_save`、`aof_load`
- 完整性：`integrity_test`、`integrity_client`
- 复制：`get10w`、`insert_before5w`、`insert_after5w`
- 协议与性能：`test`、`memptest`、`memptest_pthread`、`batchsend`、`longdata`

常用验证命令：

```bash
# 完整构建
cmake -S . -B build
cmake --build build -j"$(nproc)"

# 完整性单元测试
./build/testcase/integrity/integrity_test

# 持久化与篡改
bash testcase/run_rdb_cycle.sh
bash testcase/run_aof_cycle.sh
bash testcase/run_persistence_integrity.sh

# 三网络后端协议
bash testcase/run_backends_check.sh

# 主从复制
bash testcase/run_sync_fullsync.sh
bash testcase/run_sync_increment.sh
bash testcase/run_sync_multi_replica.sh
bash testcase/run_sync_reverse_inc.sh
bash testcase/run_sync_faults.sh
```

端口、日志、`KVS_TEST_SCENARIOS`、RDMA 前置条件等详细说明见
[testcase/README.md](testcase/README.md)。

测试环境限制必须与代码失败区分：

- 持久化测试需要可用的 `io_uring` 和本地监听套接字。
- `run_backends_check.sh`、`run_sync_faults.sh` 和完整性故障注入需要 Python 3。
- 全量同步和复制异常测试需要可用的 RDMA/SoftiWARP；没有设备时先执行
  `setup_rdma.sh`，不要误判为快照加载失败。
- 复制脚本默认使用 `ebpf-agent --mode sim`，不要求 `CAP_BPF`；uprobe 模式仍需要相应
  capability，且不能 strip Master 二进制。

## 复制

重设计后复制被拆成「kvstore 控制流 + 三个独立进程」，五个阶段已全部完成；设计、阶段记录与
异常矩阵见 [docs/replication-redesign.md](docs/replication-redesign.md)。

进程与产物：

| 可执行文件 | 角色 | 职责 |
| --- | --- | --- |
| `build/kvstore`（role=0） | Master | 处理多会话 `SYNC` / `SYNC FINISHED`、生成会话专用 `.tmp`、派生 `rdma-client` |
| `build/kvstore`（role=1） | Replica | 派生 `rdma-server`、发 `SYNC`、加载 `.tmp`、回放增量 |
| `build/rdma-client` / `build/rdma-server` | 全量搬运 | RDMA 传输 `.tmp`（`kvstore` 自身不再链接 ibverbs/rdmacm） |
| `build/ebpf-agent` | 增量采集 | 单采集流按 session_id 扇出到最多 16 个 Replica（手动启动） |

同步内部边界：

- `replication/api/replication_facade.cpp`：保留给 `kvstore.cpp` 和 C 网络后端的
  `extern "C"` 薄门面。
- `replication/sync/`：`MasterSyncCoordinator`、`ReplicaSyncCoordinator`、
  `ReplicaIncrementChannel`、`ManagedChildProcess` 与同步公共工具。
- `replication/capture/capture_runtime.*`：Agent 侧采集生命周期门面；sim/uprobe 后端由
  `replication/capture/uprobe_capture.cpp` 与 `replication/kvs_ebpf_ring.cpp` 实现。
- `replication/agent/agent_service.*`：Agent 注册、监听、poll 和 Hub 生命周期；
  `tools/ebpf_agent.cpp` 只负责配置、信号和对象启动。

`replication/kvs_replication.cpp` 是写路径采集与复制状态门面；`kvstore.cpp` 里的调用点不要绕过：

- `kvs_replication_append(argc, argv)`：写路径入口，只把命令交给有序采集流
  （`kvs_ebpf_notify_command`）。调用点在 `kvs_filter_protocol` 写分支内，天然持存储锁。
- `kvs_replication_accept_handshake(fd, data, len)`：网络层识别 Master 侧的 `EBPF 2`
  （agent 注册）；三个后端共用这一个入口，返回 1 时后端回 `+OK`。反向 `REPL_INC 2`
  由 Replica 同步线程直接连接 Agent，不经过网络后端。
- `kvs_replication_remove_replica(fd)`：断连清理，转发到 `kvs_sync_forget_fd`。
- `kvs_replication_is_replaying()` / `_set_replaying()`：线程局部回放标记，写分支据此跳过
  AOF 记录（必须是线程局部量，否则会误伤本地客户端）。
全量同步（Replica 主动驱动）：

1. Replica 派生 `rdma-server`，等它创建就绪文件（避免 `rdma-client` 先到被拒）；
2. Replica 发 `SYNC <rdma_ip> <rdma_port>`。Master 校验 agent 已注册后分配 session_id，
   只在该会话的锁内窗口发布 `SYNC <session_id>` 事件并序列化快照，随后在锁外原子写
   `../data/kvstore.sync.<session_id>.tmp`、派生独立 `rdma-client`，返回 `+OK <session_id>`；
3. 文件经 RDMA 落到 Replica 的 `../data/kvstore.sync.replica.<pid>.<fd>.tmp.recv`，
   收到会话 ID 后改名为 `../data/kvstore.sync.<session_id>.tmp.recv` 并校验加载；
4. Replica 加载快照后主动连接 `master_ip:agent_port`，发送 `REPL_INC 2 <session_id>`；
5. Replica 启动增量接收线程，再发 `SYNC FINISHED <session_id>`；Master 只回收该会话资源；
6. 反向通道断开时回到第 1 步重新全量同步。

增量同步：`ebpf-agent` 消费采集流，并为每个 session 独立维护
`AWAITING_SYNC -> CACHING -> LIVE` 状态。普通写命令广播给所有 `LIVE` 目标，同时缓存到
各自的 `CACHING` 目标；`SYNC FINISHED <session_id>` 只把对应目标转成 `LIVE`。

改代码时必须保持的约束：

- **顺序即正确性**：写命令与 `SYNC` / `SYNC FINISHED` 必须在存储锁内经同一挂载点
  （`kvs_ebpf_notify_write`）取号。这是「缓存窗口 ⊇ 快照窗口」的唯一保证，也是不再需要
  `base_seq` 水位和 `kvs_ebpf_wait_drained()` 的原因。
- **增量回放不能回响应**：Agent 只写不读，Replica 专用接收线程必须持续排空 TCP，不能
  为每条命令生成响应，否则发送缓冲区会被堆满并丢掉尾部增量。
- **采集是强制依赖**：`ebpf_capture_mode` 取 `uprobe`（默认，agent 挂探针到 **Master 二进制**
  的 `kvs_ebpf_notify_write` 符号，偏移从 ELF `.symtab` 解析）或 `sim`（Master 写共享内存环）。
  uprobe 要求 Master 二进制不被 strip；agent 采集失败会直接退出，不会静默降级。
- **采集只在 agent 注册后产出**：由 `kvs_ebpf_capture_enable` 控制，注册时重置采集环；否则
  有界环会被注册前的写入灌满并制造 seq 缺口。
- **RDMA endpoint 不静态配置**：Replica 默认通过对 Master 的控制连接
  `getsockname()` 获取本地地址；多网卡/单机测试用 `rdma_advertise_host` 覆盖。
- **缓存溢出只放弃对应目标**：每目标缓存/发送队列或全局内存预算超限时，只关闭该目标的
  反向增量连接；其他 Replica 继续实时转发。
- fd 生命周期按归属清理：Master 的 EBPF 注册 fd 仍归网络层；SYNC 控制 fd 与会话表绑定，
  断连只中止对应 session；Agent 为每个目标独立保存增量 fd，断线时只移除该目标。
- RDMA 实现用普通 `IBV_WR_RDMA_WRITE` + 单独一条 SEND 通知，**不要改回
  `IBV_WR_RDMA_WRITE_WITH_IMM`**：SoftiWARP 下 `ibv_post_send` 返回 ENOSPC(28)。

存储引擎没有内部锁，所有存储访问必须经过 `kvstore.cpp` 的 `kvs_data_lock()` /
`kvs_data_unlock()`（可重入锁）。普通命令在 `kvs_filter_protocol` 内统一加锁；`SYNC` 和
`SYNC FINISHED` 处理器自行加锁，只在发布有序事件和序列化/整理状态时持锁，文件 I/O 与
子进程回收不占用存储锁。旧的复制锁 `g_repl_mutex` 已删除。

避免复制/AOF 回环的判断集中在 `kvstore.cpp` 的私有函数
`kvs_commit_write_side_effects(argc, tokens)` 中。普通写命令只有在校验引擎返回 `0`
成功后，才可在存储锁内调用该函数。不要在命令分支中重新展开 AOF 与复制 replay 判断，
否则容易造成 AOF 重复记录或 Master/Replica 相互转发。

## Agent 验证清单

修改代码后至少检查与改动类型对应的项目：

- 新增或删除命令：同步修改 `command[]`、`KVS_CMD` 和 `kvs_filter_protocol` 三个位置。
- 新增写命令：成功修改数据后调用 `kvs_commit_write_side_effects()`，不要绕过统一的
  AOF/复制回放判断。
- 修改协议或网络后端：对 reactor、ntyco、proactor 都执行协议回归，并覆盖半包、粘包和 1MB 上限。
- 修改持久化：同时验证正常保存/恢复、摘要失败、截断、旧格式拒绝，以及 AOF 重启后链状态续接。
- 修改快照或复制：至少执行 `run_rdb_cycle.sh`、`run_persistence_integrity.sh` 和一项主从同步测试。
- 修改内存所有权或存储引擎：执行对应引擎回归，并确认所有分配和释放继续使用同一套
  `kvs_malloc` / `kvs_calloc` / `kvs_free`。
- 修改测试结构：生产公共接口放在 `include/`；测试专用辅助代码放在对应
  `testcase/<case>/`。共享的 `include/kvs_integrity.h` 不应复制进测试目录。

## 代码风格与工作约定

- 主服务为 C++11，但部分网络/第三方代码为 C；混合编译是项目现状，不要强行统一。
- 注释以中文为主，新增代码建议沿用清晰的中文说明。
- 不要修改 `NtyCo/` 内的第三方代码；需要升级时通过 submodule 流程处理，并保持 `.gitmodules` 一致。
- 不要删除或重命名现有公共 API；如需调整，先搜索所有调用点，尤其是 `kvstore.cpp`、`persistence/`、`replication/` 和 `testcase/`。
- `include/` 中的生产公共头文件统一使用 `kvs_` 前缀，例如 `kvs_runtime.h`、
  `kvs_network.h`、`kvs_aof.h`。新增泛名头文件前应先确认是否只属于单一子模块；
  测试专用辅助代码不要放进生产 `include/`。
- 提交前至少完成一次干净构建，并执行与改动相关的 `testcase/` 脚本或客户端回归。
- 不要用历史测试二进制代替当前构建产物；测试客户端由 CMake target 生成。
- 不要删除或覆盖 `data/` 中的现有持久化文件来“修复”测试；使用测试脚本的临时目录。

## 快速排查提示

- 连接成功但响应异常：先确认客户端是否按“4 字节长度头 + RESP 负载”收发，而不是直接发送裸 RESP。
- 持久化文件找不到：确认进程工作目录是否为 `build/`，或路径是否相对当前目录解析。
- 编译时缺 NtyCo/liburing/OpenSSL：初始化 submodule 并安装 `liburing-dev`、`libssl-dev`；
  jemalloc 当前不是 CMake 强制依赖。
- 修改命令后测试失败：检查 `command[]`、`KVS_CMD` 枚举和 `kvs_filter_protocol` 三处是否一致。
- 日志出现 `Snapshot integrity check failed` 或 `AOF integrity check failed`：文件内容、
  长度或哈希链不匹配；服务会保留文件并拒绝启动，不要自动截断或重写现场。
- 日志出现 `Unsupported snapshot/AOF format`：文件仍是 `KVSDB01` 或无头旧 AOF，当前版本
  明确不兼容，需要清空或从业务数据重建。
- Replica 日志反复出现 `master rejected SYNC: -ERROR`：Master 侧 eBPF 未武装——先起
  `build/ebpf-agent` 并确认它打印 `registered with master`（采集是强制依赖）。
- Replica 日志出现 `connect agent ... failed`：检查 Agent 的 `listen_ip/listen_port`、
  Replica 的 `agent_port` 以及防火墙；通道失败会触发重新全量同步。
- 同步用例失败但 `kvstore` 正常：确认各 Replica 的 `rdma_port` 未被占用、上报地址可路由，
  并检查 `../data/kvstore.sync.<session_id>.tmp.rdma-client.log` 与
  `../data/kvstore.sync.replica.<pid>.<fd>.rdma-server.log`。
- 没有 RDMA 设备：先执行 `setup_rdma.sh` 并确认 `rdma link show` 显示链路 ACTIVE；
  `rdma_create_event_channel`/`No such device` 属于环境问题，优先于业务日志排查。
- 测试脚本按场景拆分：`run_rdb_cycle.sh`、`run_aof_cycle.sh`、
  `run_persistence_integrity.sh`、`run_backends_check.sh`、`run_sync_*.sh`；
  详细用法见 `testcase/README.md`。
