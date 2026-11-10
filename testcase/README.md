# testcase 测试说明

本目录包含 `kvstore` 的协议回归、持久化循环、性能压测、内存分配器测试，以及主从复制使用的辅助客户端。
测试客户端通过 TCP 连接真实服务；服务端和客户端之间使用两层协议：

1. 外层为 4 字节网络字节序长度前缀。
2. 内层为 RESP 数组和 bulk string。

除 mock 测试外，运行前都需要先构建并启动对应服务。测试脚本会创建临时运行目录，不读取或覆盖仓库
`data/` 中的现有持久化文件。

## 构建准备

在仓库根目录执行：

```bash
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

也可以只构建常用测试目标：

```bash
cmake --build build --target \
    kvstore \
    rdb_save rdb_load aof_save aof_load aof_insert10w \
    test memptest memptest_pthread \
    batchsend batchsend_hash longdata \
    get10w insert_before5w insert_after5w \
    -j"$(nproc)"
```

CMake 配置要求所有声明引用的测试源都存在。如果配置阶段报告
`Cannot find source file`，应先补齐或恢复对应测试源，再执行构建；不要使用 `build/` 中遗留的旧
二进制代替当前源码的构建产物。

主要测试产物位于：

```text
build/kvstore
build/testcase/RDB/{save,load}
build/testcase/AOF/{save,load,insert10w}
build/testcase/test/test
build/testcase/memorypool/{memptest,memptest_pthread}
build/testcase/batchsend/{batchsend,batchsend_hash}
build/testcase/longdata/longdata
build/testcase/master-replica/{get10w,insert_before5w,insert_after5w}
```

## 推荐验证顺序

提交前至少根据改动范围选择以下测试：

| 改动类型 | 最小验证 |
| --- | --- |
| 协议、命令分发或存储引擎 | `test`，再分别选择四个 engine mode |
| 网络后端或半包/粘包 | 对 reactor、ntyco、proactor 分别运行 `test` 和 `longdata` |
| RDB、`kvs_reset_data()` 或引擎销毁逻辑 | `RDB/run_rdb_cycle.sh` |
| AOF | `AOF/run_aof_cycle.sh` |
| 批量发送 | `batchsend/test_batchsend.py` 和 `batchsend_hash` |
| 内存池或并发分配 | `memptest` 和 `memptest_pthread` |
| 主从复制客户端 | `insert_before5w`、`insert_after5w`、`get10w`，并配合复制链路脚本 |

## 持久化测试

### RDB 保存与启动恢复

[RDB/run_rdb_cycle.sh](RDB/run_rdb_cycle.sh) 会完成一次完整往返：

1. 在临时目录中启动启用 RDB 的 Master。
2. 使用 [RDB/save.cpp](RDB/save.cpp) 向四个引擎共写入 100,000 条记录。
3. 执行 `RDB SAVE`。
4. 正常停止并重新启动服务。
5. 使用 [RDB/load.cpp](RDB/load.cpp) 逐条读取并校验 100,000 条记录。

```bash
bash testcase/RDB/run_rdb_cycle.sh
```

默认使用 `127.0.0.1:9999`，可通过 `KVSTORE_TEST_PORT` 覆盖：

```bash
KVSTORE_TEST_PORT=19999 bash testcase/RDB/run_rdb_cycle.sh
```

成功时最后输出：

```text
RDB save/auto-load cycle completed.
```

### AOF 写入与启动回放

[AOF/run_aof_cycle.sh](AOF/run_aof_cycle.sh) 使用全新的临时目录启动 AOF 模式 Master：

1. 使用 [AOF/save.cpp](AOF/save.cpp) 向四个引擎共写入 100,000 条记录。
2. 正常停止服务，使 AOF 缓冲刷新并落盘。
3. 重新启动服务并自动回放 AOF。
4. 使用 [AOF/load.cpp](AOF/load.cpp) 逐条校验恢复结果。

```bash
bash testcase/AOF/run_aof_cycle.sh
```

同样支持 `KVSTORE_TEST_PORT`。成功时最后输出：

```text
AOF save/auto-load cycle completed.
```

[AOF/insert10w.cpp](AOF/insert10w.cpp) 单独执行 100,000 条 HSET 并验证结果，主要用于 AOF
开启/关闭时的吞吐对比，目标名为 `aof_insert10w`。该性能测试没有独立的正确性脚本，建议优先使用
上面两个循环脚本验证持久化行为。

不要直接在仓库的 `build/` 目录中手工运行 `RDB/save` 或 `AOF/save` 来测试持久化，否则可能写入
`../data/`。如需手工测试，请像脚本一样切换到独立临时目录后再启动服务。

## 协议与存储引擎测试

先在一个终端中启动服务：

```bash
cd build
./kvstore \
    --bind 127.0.0.1 \
    --port 9999 \
    --role master \
    --network ntyco \
    --rdb off \
    --aof off
```

在另一个终端执行基础协议测试：

```bash
./build/testcase/test/test 127.0.0.1 9999 0
./build/testcase/test/test 127.0.0.1 9999 1
./build/testcase/test/test 127.0.0.1 9999 2
./build/testcase/test/test 127.0.0.1 9999 3
```

第三个参数用于选择引擎：

| 参数 | 引擎 | 命令前缀 |
| --- | --- | --- |
| `0` | rbtree | `RSET/RGET/RDEL/RMOD/REXIST` |
| `1` | array | `SET/GET/DEL/MOD/EXIST` |
| `2` | hash | `HSET/HGET/HDEL/HMOD/HEXIST` |
| `3` | skiptable | `SSET/SGET/SDEL/SMOD/SEXIST` |

当前 [test/testcase.cpp](test/testcase.cpp) 默认启用 `LEVEL3`：每种引擎使用 10 个线程，每个线程
重复 10,000 轮九条命令。需要快速调试时，可在该文件顶部切换为 `LEVEL1` 后重新构建 `test`。

### 网络后端起停切换

三个网络后端都编译进同一个 `kvstore` 二进制，但每次启动只能选择一个。应分别重启服务并运行同一组
客户端回归：

```bash
./kvstore --network reactor ...
./kvstore --network ntyco ...
./kvstore --network proactor ...
```

proactor 依赖可用的 `io_uring`。切换后至少覆盖：

- `SET/GET/DEL/MOD/EXIST` 及其余三个引擎的对应命令。
- 半包、粘包和长度前缀处理。
- `longdata`，以及超过 `MAX_ALLOWED_LEN` 的 1 MB 限制。

## 长消息与内存池

启动无持久化服务后执行：

```bash
./build/testcase/longdata/longdata 127.0.0.1 9999
./build/testcase/memorypool/memptest 127.0.0.1 9999
./build/testcase/memorypool/memptest_pthread 127.0.0.1 9999
```

`longdata` 用包含空格和换行的长 value 测试四个引擎的 SET/MOD/EXIST/DEL 路径。

`memptest` 串行对四个引擎分别执行 25,000 组 SET/MOD/DEL 或对应命令，共 300,000 条命令。
`memptest_pthread` 为四个引擎各启动一个线程并发执行相同规模，主要用于验证内存池和存储锁下的并发
分配行为。

## 批量发送

`batchsend` 把多条命令放入同一个外层帧，验证服务端粘包解析和响应拼接：

```bash
./build/testcase/batchsend/batchsend 127.0.0.1 9999 160
```

支持的 batch size 为 `10`、`20`、`40`、`80`、`160`。每批命令会均匀分配到四个引擎，并检查
响应数量以及 `+OK`/`+EXIST`。

不启动 `kvstore` 也可以使用本地 mock 服务检查客户端构造的帧：

`batchsend_hash` 固定发送 100,000 条 HSET，用 batch size 控制每次外层帧中的命令数量，适合做
批处理吞吐对比：

```bash
./build/testcase/batchsend/batchsend_hash 127.0.0.1 9999 10
./build/testcase/batchsend/batchsend_hash 127.0.0.1 9999 160
```

[batchsend_old.cpp](batchsend/batchsend_old.cpp) 仅保留作历史参考，不属于 CMake 测试目标。

## 主从复制辅助客户端

三个客户端需要配合真实 Master/Replica、`ebpf-agent` 和可用的 RDMA/SoftiWARP 链路使用：

| 客户端 | 参数 | 用途 |
| --- | --- | --- |
| `insert_before5w` | `<ip> <port>` | 写入前半段 50,000 条四引擎数据 |
| `insert_after5w` | `<ip> <port>` | 写入后半段 50,000 条四引擎数据 |
| `get10w` | `<ip> <port>` | 逐条读取并校验完整 100,000 条数据 |

典型数据准备和校验方式：

```bash
./build/testcase/master-replica/insert_before5w 127.0.0.1 9999
./build/testcase/master-replica/insert_after5w 127.0.0.1 9999
./build/testcase/master-replica/get10w 127.0.0.1 9999
```

`insert_after5w.cpp` 通过定义 `START_NUM=50000` 后包含 `insert_before5w.cpp`，两个客户端共同
覆盖 `Teacher0` 到 `Teacher99999`。单独运行 `get10w` 前必须保证目标实例已经写入完整数据。

全量同步由独立的 `rdma-client`/`rdma-server` 搬运快照。CMake 找不到 RDMA 开发库时不会构建这两个
工具，此时无法完成主从全量同步验证。没有硬件时可执行 `sudo ./setup_rdma.sh`，并用
`rdma link show` 确认链路为 `ACTIVE`。复制场景还需要启动 `build/ebpf-agent`；没有复制脚本时不要让
这三个客户端单独承担主从正确性结论。

## ECHO 实验模式

[ECHO/echo.cpp](ECHO/echo.cpp) 用于验证 ntyco 后端的原样回发路径，而不是普通 KV 命令路径。
它要求将 [network/ntyco.c](../network/ntyco.c) 顶部的 `ECHO` 改为 `1` 后重新构建 `kvstore`：

```bash
cmake --build build --target kvstore echo_save -j"$(nproc)"
./build/testcase/ECHO/ECHO 127.0.0.1 9999
```

当前 `ECHO` 默认为 `0`，因此不要在普通服务实例上把该客户端作为 KV 回归。普通协议回归应使用
`test`、`longdata` 和 `batchsend`。

## Redis 对比测试

`redis/` 目录提供 HSET 吞吐对比和 Redis AOF 开/关对比，包含独立的 Redis 启动与清理逻辑。用法、
结果文件和限制见 [redis/README.md](redis/README.md)。

## 环境变量与清理

| 变量 | 适用范围 | 说明 |
| --- | --- | --- |
| `KVSTORE_TEST_PORT` | RDB/AOF 循环脚本 | 覆盖默认测试端口 `9999` |
| `TMPDIR` | 所有临时目录脚本 | 控制临时目录根路径 |

持久化脚本退出时会通过 `trap` 停止服务并删除自己的临时运行目录。若进程被 `SIGKILL` 强制终止，
清理逻辑可能来不及执行，可检查：

```bash
pgrep -a -x kvstore
pgrep -a -x ebpf-agent
pgrep -a -x rdma-client
pgrep -a -x rdma-server
```

## 常见问题

- `missing executable`：先构建对应 CMake target，并确认测试脚本中的路径仍与当前输出目录一致。
- `Connection refused`：确认服务已监听目标 IP/端口，且客户端使用的是“长度头 + RESP”协议。
- 持久化文件加载失败：检查服务工作目录。脚本应在独立临时目录中运行，避免误用仓库 `data/`。
- proactor 启动失败：检查内核和容器是否允许使用 `io_uring`。
- CMake 配置失败：确认 `CMakeLists.txt` 声明的测试源都存在；构建 target 的源码位置以当前
  `CMakeLists.txt` 为准。
- 测试结果异常：优先检查最新构建产物、服务日志和端口冲突，不要使用目录中遗留的旧二进制判断结果。
