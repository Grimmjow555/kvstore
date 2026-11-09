# `testcase` 脚本说明

本目录中的 `.sh` 脚本用于启动真实的 `kvstore`、`ebpf-agent` 和 RDMA 辅助进程，
再通过项目协议或专用测试客户端验证持久化、网络后端和主从复制行为。

脚本不是纯单元测试：运行前需要先完成构建，并准备脚本所需的本地端口、`io_uring`
或 RDMA 环境。

## 构建准备

在仓库根目录执行：

```bash
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

脚本默认使用以下产物：

```text
build/kvstore
build/ebpf-agent
build/rdma-client
build/rdma-server
build/testcase/...
```

缺少对应可执行文件时，脚本通常会在启动前直接退出。

## 脚本一览

| 脚本 | 是否直接运行 | 主要功能 |
| --- | --- | --- |
| `lib_sync_test.sh` | 否 | 同步测试公共函数库 |
| `run_rdb_cycle.sh` | 是 | RDB 保存和启动恢复 |
| `run_aof_cycle.sh` | 是 | AOF 写入、清空和启动回放 |
| `run_persistence_integrity.sh` | 是 | 快照与 AOF 完整性、篡改检测 |
| `run_backends_check.sh` | 是 | 三种网络后端的协议一致性 |
| `ECHO/run_echo_test.sh` | 是 | 四引擎共十万条 ntyco 原样回发测试 |
| `run_sync_fullsync.sh` | 是 | 主从全量同步 |
| `run_sync_increment.sh` | 是 | 主从全量加增量同步 |
| `run_sync_reverse_inc.sh` | 是 | 用本地 mock RDMA 验证 Replica 主动连接 Agent 及断线重同步 |
| `run_sync_faults.sh` | 是 | 主从同步异常和恢复 |

### 协议帧单次发送回归

`test_frame_send_once.py` 使用本地 mock 服务和 `strace` 检查
`batchsend`、`batchsend_hash`、`integrity_client` 是否正确地把“4 字节长度头 +
RESP 负载”放在同一次 socket 发送中。分两次发送会在部分 TCP 环境中触发 Nagle
和延迟 ACK，使每个批次额外等待约 40 ms。

```bash
python3 testcase/test_frame_send_once.py
```

测试依赖已构建的三个客户端，并需要系统中提供 `strace`。

### Hash 引擎回归

`hash_engine_test` 直接使用生产 hash 存储引擎和 slab 分配器，验证动态扩容、
75% 负载因子、FNV-1a 分布、重复键、修改、删除、重新插入，以及节点和扩容分配
失败后的清理语义。

```bash
cmake --build build --target hash_engine_test -j"$(nproc)"
./build/testcase/hash/hash_engine_test
```

`ebpf-agent` 的独立配置测试目标：

```bash
cmake --build build --target agent_config_test agent_startup_test -j"$(nproc)"
./build/testcase/ebpf-agent/agent_config_test
./build/testcase/ebpf-agent/agent_startup_test
```

`agent_config_test` 验证配置字段、默认路径、命令行覆盖和错误校验；
`agent_startup_test` 在临时目录放置 `ebpf-agent.conf`，确认不带参数的
`./ebpf-agent` 能从当前目录加载配置、启动 sim 采集，并正确处理反向
`REPL_INC` 握手和非法握手。

## 公共函数库

### `lib_sync_test.sh`

该脚本只用于被其他脚本 `source`，自身不会启动服务。主要提供：

- 推导仓库根目录、`build/` 和 `data/` 路径。
- 后台启动服务并保存 PID。
- 等待端口监听和日志关键字。
- 使用 `SIGINT`、超时和 `SIGKILL` 停止进程。
- 清理遗留的 `rdma-client`、`rdma-server` 进程。
- 生成复制测试配置。
- 清理同步临时快照、接收文件和 eBPF 共享内存环。

默认网络参数：

```text
Master TCP 端口：9999
Replica TCP 端口：10001
Agent 增量端口：11001
RDMA 端口：10000
测试用 RDMA 上报地址：192.168.234.135
```

这些值可通过环境变量覆盖，具体见下文。

## 持久化测试

### `run_rdb_cycle.sh`

执行完整的 RDB 往返：

1. 在临时目录中启动 RDB 模式的 Master。
2. 使用 `testcase/RDB/save` 写入 10 万条数据。
3. 执行 `RDB SAVE`。
4. 正常停止服务并重新启动。
5. 启动时自动加载 `kvstore.data`。
6. 使用 `testcase/RDB/load` 逐条读取并验证 10 万条数据。

脚本使用独立临时目录，不会覆盖仓库 `data/` 中已有的 `kvstore.data`。

运行：

```bash
bash testcase/run_rdb_cycle.sh
```

成功时最后输出：

```text
***RDB TEST SUCCESS***
RDB save/auto-load cycle completed.
```

### `run_aof_cycle.sh`

执行完整的 AOF 往返：

1. 在临时目录中启动 AOF 模式的 Master。
2. 使用 `testcase/AOF/save` 执行 `AOF CLEAR`。
3. 写入 10 万条命令。
4. 正常停止并重新启动服务。
5. 启动时自动回放 `append.aof`。
6. 使用 `testcase/AOF/load` 验证 10 万条数据。

脚本使用独立临时目录，不会覆盖仓库 `data/` 中已有的 AOF。

运行：

```bash
bash testcase/run_aof_cycle.sh
```

成功时最后输出：

```text
***AOF TEST SUCCESS***
AOF save/auto-load cycle completed.
```

### `run_persistence_integrity.sh`

验证持久化文件的格式和 SHA-256 完整性：

- 生成合法的 `KVSDB02` 快照并确认可以重新加载。
- 修改快照内容、截断快照、追加尾部数据，确认服务拒绝启动。
- 修改为旧 `KVSDB01` 格式，确认旧格式被拒绝。
- 生成合法的 `#KVSAOF02` AOF。
- 修改 AOF 命令、删除中间记录、交换记录、截断摘要和移除新文件头。
- 每次失败后比较文件摘要，确认服务没有自动修复或覆盖损坏文件。
- 验证 AOF 重启后仍能从最后一条有效记录的摘要继续追加。

测试使用以下辅助文件：

```text
testcase/integrity/client.cpp
testcase/integrity/aof_mutate.py
testcase/integrity/test_integrity.cpp
```

运行：

```bash
bash testcase/run_persistence_integrity.sh
```

成功时输出：

```text
persistence integrity tests passed
```

## 网络后端测试

### `run_backends_check.sh`

依次以三种网络后端启动 Master：

```text
reactor
ntyco
proactor
```

对每个后端验证：

- `SET / GET / DEL / EXIST`
- `RSET / RGET / RDEL / REXIST`
- `HSET / HGET / HDEL`
- `SSET / SGET / SDEL`
- 重复写入的 `+EXIST`
- 不存在键和不存在修改的 `NO EXIST`

脚本使用固定端口 `10199`，并通过 Python 正确收发“4 字节长度头 + RESP”协议。

运行：

```bash
bash testcase/run_backends_check.sh
```

成功时输出：

```text
Backends protocol check completed.
```

### `ECHO/run_echo_test.sh`

以 `testcase/AOF/save.cpp` 的四引擎用例为基础，验证 `network/ntyco.c` 的
`ECHO=1` 模式：

- 启动 ntyco 后端的真实 `kvstore` 服务。
- 四引擎各发送 25,000 条，共 100,000 条：
  - array：`SET`
  - rbtree：`RSET`
  - hash：`HSET`
  - skiptable：`SSET`
- 每条请求都逐字节校验回发的长度头、RESP 请求和 payload。
- 输出四引擎各自的命令数、耗时和 QPS，以及总计统计。
- 客户端默认限时 60 秒，可通过 `ECHO_TEST_TIMEOUT` 覆盖。

运行前先构建 `kvstore` 和 `echo_save`：

```bash
cmake --build build --target kvstore echo_save -j"$(nproc)"
bash testcase/ECHO/run_echo_test.sh
```

成功时输出：

```text
***ECHO TEST SUCCESS***
```

## 主从复制测试

### `run_sync_fullsync.sh`

验证主从全量同步链路：

1. Master 写入并自检 10 万条数据。
2. 启动 `ebpf-agent` 的 `sim` 模式。
3. Replica 派生 `rdma-server` 并发送 `SYNC`。
4. Master 生成同步快照并派生 `rdma-client`。
5. Replica 接收快照、验证并加载。
6. Master 收到 `SYNC FINISHED` 后回收辅助进程。
7. 检查 `.tmp`、`.recv`、`.ready`、`.partial` 没有残留。
8. 分别读取 Master 和 Replica 的 10 万条数据并比较。

该脚本需要可用的 RDMA 设备。没有硬件时可使用 SoftiWARP：

```bash
sudo ./setup_rdma.sh
rdma link show
```

运行：

```bash
bash testcase/run_sync_fullsync.sh
```

成功时输出：

```text
Full sync (RDMA process split) cycle completed.
```

### `run_sync_increment.sh`

验证全量同步期间的缓存和后续增量转发：

1. Master 写入前 5 万条。
2. 启动 `ebpf-agent` 并等待其注册。
3. 启动 Replica 触发全量同步。
4. 同步进行期间再写入 5 万条。
5. agent 缓存窗口内的写入，并在 `SYNC FINISHED` 后按序补发。
6. Replica 建立增量通道并追平数据。
7. 逐条校验 Replica 的 10 万条数据。

脚本会检查以下日志状态：

```text
registered with master
SYNC observed
SYNC FINISHED observed
increment channel connected to agent
full sync completed
```

运行：

```bash
bash testcase/run_sync_increment.sh
```

成功时输出：

```text
Increment sync (ebpf-agent process split) cycle completed.
```

### `run_sync_reverse_inc.sh`

在无 RDMA 硬件的环境验证反向增量通道。脚本用本地文件复制包装脚本替代
`rdma-client` / `rdma-server`，但仍启动真实的 Master、Replica 和 `ebpf-agent`。

验证内容：

1. Agent 监听 `11001`，Replica 主动连接并发送 `REPL_INC`。
2. Replica 在 `SYNC` 中动态上报 RDMA endpoint，Master 的 mock `rdma-client`
   必须收到该地址，而不是静态 `replica_host`。
3. 全量窗口内的缓存由 Agent 通过已接受的连接冲刷。
4. 后 5 万条实时增量到达 Replica，最终校验 10 万条。
5. 停止并重启 Agent，确认 Replica 检测到通道断开后重新全量同步，并补齐
   Agent 离线期间写入的数据。

运行：

```bash
bash testcase/run_sync_reverse_inc.sh
```

成功时输出：

```text
Reverse increment sync cycle completed.
```

### `run_sync_faults.sh`

运行复制异常注入和恢复测试。默认依次执行 A、B、C、D：

| 场景 | 注入的异常 | 预期结果 |
| --- | --- | --- |
| A | Replica 请求同步时没有 `ebpf-agent` | Master 拒绝 `SYNC`，启动 agent 后恢复 |
| B | agent 缓存上限设为 1 条并持续写入 | 缓存溢出，Replica 重新全量同步并收敛 |
| C | `rdma-client` 第一次运行直接失败 | Replica 重试后传输成功并收敛 |
| D | `rdma-client` 发送前修改 `.tmp` 快照 | Replica 校验失败，保留已有数据 |

默认运行全部场景：

```bash
bash testcase/run_sync_faults.sh
```

只运行指定场景：

```bash
KVS_TEST_SCENARIOS=A,C bash testcase/run_sync_faults.sh
```

成功时每个场景输出 `A passed`、`B passed` 等，最后输出：

```text
Fault-injection checks completed.
```

## 可配置环境变量

### 持久化测试

```text
KVSTORE_TEST_PORT
```

用于 `run_rdb_cycle.sh`、`run_aof_cycle.sh` 和
`run_persistence_integrity.sh`。

默认值：

```text
RDB/AOF cycle：9999
integrity：19888
```

### 主从复制测试

```text
KVS_TEST_MASTER_PORT
KVS_TEST_REPLICA_PORT
KVS_TEST_RDMA_PORT
KVS_TEST_RDMA_HOST
```

示例：

```bash
KVS_TEST_MASTER_PORT=19001 \
KVS_TEST_REPLICA_PORT=19002 \
KVS_TEST_RDMA_PORT=19003 \
KVS_TEST_RDMA_HOST=192.168.234.135 \
bash testcase/run_sync_fullsync.sh
```

### 异常场景选择

```text
KVS_TEST_SCENARIOS
```

只用于 `run_sync_faults.sh`，支持逗号分隔的 `A`、`B`、`C`、`D`。

## 运行环境限制

- 持久化测试依赖 `io_uring` 和本地 TCP 套接字。
- `run_persistence_integrity.sh` 还需要 `python3` 修改 AOF 测试数据。
- `run_backends_check.sh` 和 `run_sync_faults.sh` 需要 `python3`；缺少时脚本会跳过。
- `run_sync_fullsync.sh`、`run_sync_increment.sh` 和 `run_sync_faults.sh` 需要可用的
  RDMA/SoftiWARP 环境。
- 复制脚本默认使用 `ebpf-agent --mode sim`，不要求 `CAP_BPF`。
- 受限容器如果禁止 socket、`io_uring` 或 RDMA，测试可能因环境限制退出，而不是业务断言失败。

## 日志与清理

脚本把日志放在 `TMPDIR` 或 `/tmp`，通常包含 PID，便于并行运行时隔离。

脚本退出时会通过 `trap`：

- 停止其启动的 Master、Replica 和 agent。
- 回收遗留的 `rdma-client`、`rdma-server`。
- 删除同步临时文件、就绪文件和 eBPF 共享内存环。
- RDB、AOF 和完整性脚本删除自己的临时运行目录。

如果进程被 `SIGKILL` 强制终止，清理函数可能来不及执行，需要手动检查：

```bash
pgrep -a -x kvstore
pgrep -a -x ebpf-agent
pgrep -a -x rdma-client
pgrep -a -x rdma-server
```
