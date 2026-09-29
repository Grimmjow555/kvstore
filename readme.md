# kvstore

一个基于 RESP 协议、支持主从复制与持久化恢复的 KV 存储服务端项目。

## 项目概览

- 网络框架：reactor(epoll)、proactor(io_uring)、协程框架 NtyCo
- 存储引擎：array、rbtree、hash、skiptable
- 内存分配：通过编译期宏 `ENABLE_MEMORYPOOL` 选择内置 slab 内存池或系统 `malloc`/`free`
- 协议：RESP
- 特性：支持特殊字符 key/value、批量命令处理、RDB/AOF 持久化、主从同步

## 依赖说明

### 必需依赖

- CMake 3.10+
- C/C++ 编译器
- pthread
- liburing
- jemalloc
- NtyCo（通过 Git submodule 接入）

### NtyCo 依赖

NtyCo 以 submodule 方式接入，目录如下：

- `NtyCo`

官方仓库：

- https://github.com/wangbojing/NtyCo.git

首次克隆本项目时使用：

```bash
git clone --recurse-submodules <repo-url>
```

如果是已有仓库，执行：

```bash
git submodule sync --recursive
git submodule update --init --recursive
```

也可以在仓库根目录执行一键配置脚本：

```bash
./setup_submodule.sh
```

脚本会同步 `.gitmodules` 配置，并递归初始化和更新所有子模块；重复执行也是安全的。

## 环境准备

Ubuntu/Debian 可安装依赖：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake liburing-dev libjemalloc-dev
```

如果某些环境下库目录不在默认位置，可在 CMake 时手动指定：

```bash
cmake -S . -B build -DURING_ROOT=/usr/lib
```

> 说明：CMake 默认使用仓库根目录下的 submodule 目录 `NtyCo`，因此在仓库根目录直接执行
> `cmake -S . -B build` 即可。只有在自定义路径时才需要显式指定 `-DNTYCO_ROOT=/path/to/NtyCo`。

## 最小编译命令

```bash
cd /path/to/kvstore
git submodule sync --recursive
git submodule update --init --recursive
cmake -S . -B build
cmake --build build -j$(nproc)
```

NtyCo 的源码会随 `kvstore` 一起编译，不依赖 submodule 内生成的 `libntyco.a`，因此不需要
手工在 `NtyCo/` 下执行 `make`。也可以直接执行一键脚本 `./setup_submodule.sh` 完成子模块
同步与初始化，再执行上面的 CMake 编译步骤。

编译完成后，会生成可执行文件：

```bash
./build/kvstore
```

## 运行方式

```bash
./build/kvstore <端口号> <角色> [主节点IP] [主节点端口]
```

参数说明：

- `端口号`：服务监听端口
- `角色`：
  - `0`：主节点
  - `1`：从节点
- `主节点IP`：从节点连接主节点时使用
- `主节点端口`：从节点连接主节点的端口

### 单机主节点示例

```bash
./build/kvstore 9999 0
```

### 从节点示例

```bash
./build/kvstore 9999 1 127.0.0.1 19001
```

### 配置文件方式

服务支持从配置文件读取监听地址、端口、日志级别、主从模式、持久化模式和网络架构。
默认会依次尝试加载 `./kvstore.conf` 和 `../kvstore.conf`，也可以使用 `--config <path>`
显式指定。

```bash
./build/kvstore --config ../kvstore.conf
```

配置文件示例见仓库根目录的 `kvstore.conf`，核心字段如下：

```text
bind 0.0.0.0
port 9999
log_level info
role master
master_ip 127.0.0.1
master_port 19001
persistence_mode none
network_architecture ntyco
```

字段说明：

- `bind` / `port`：服务监听地址与端口。
- `log_level`：`debug`、`info`、`warn`、`error` 或 `off`。
- `role`：`master` 或 `replica`（也接受 `slave`、`0`、`1`）。
- `master_ip` / `master_port`：角色为 `replica` 时使用的主节点地址与端口。
- `persistence_mode`：`none`、`rdb`、`aof` 或 `both`；也可以用 `rdb on/off`、
  `aof on/off` 分别控制。默认为 `none`，即 RDB 与 AOF 都不开启，需要时再显式打开。
- `network_architecture`：网络架构，取值如下。三个后端都会编入同一个二进制，可在配置
  文件中自由选择（也接受 `network`、`net`、`io_engine` 等别名）：
  - `reactor`（也接受 `epoll`）：reactor 后端，基于 epoll 事件循环。
  - `ntyco`（也接受 `coroutine`）：NtyCo 协程后端，也是当前编译期默认值。
  - `proactor`（也接受 `io_uring`）：proactor 后端，基于 io_uring。

  该选项在进程启动时确定，运行期间不能切换；未配置时默认为 `ntyco`。后端选择不再是
  编译期开关，三个后端始终编入同一个二进制，同一台机器上可以用不同配置文件启动不同
  架构的实例。

配置文件加载后，命令行开关可以覆盖其中的值，旧的位置参数方式仍兼容：

```bash
./build/kvstore --config ../kvstore.conf --port 7000 --role replica \
  --master-ip 127.0.0.1 --master-port 19001 --persistence-mode aof \
  --network reactor
```

只切换这一次运行的网络架构时，也可以不用改配置文件：

```bash
./build/kvstore --network reactor
./build/kvstore --network ntyco
./build/kvstore --network proactor
```

## 主从复制与同步

- 从节点启动时只需指定主节点地址与端口，连接、握手、全量同步、断线重连都在后台监督线程里
  完成；Master 尚未启动时从节点也能正常启动，并按指数退避持续重试。
- Master 把每条成功执行的写命令编码为 RESP 追加到全局增量日志 backlog（带递增序号），
  再由独立发送线程按每个 Replica 自己的进度投递，事件循环不做阻塞发送。
- Replica 握手后，Master 生成与 backlog 序号严格对齐的 RDB 二进制全量快照分片下发；
  快照传完再从该序号继续补发增量，因此全量同步期间发生的写命令不会丢失，也不会重复。
- 增量发送使用 `MSG_DONTWAIT` 非阻塞写，慢副本只影响自己：超过 30 秒没有发送进展会被断开，
  由 Replica 重连后重新全量同步，不会阻塞 Master 的事件循环。
- 复制帧负载首字节是帧类型（增量命令 / RESET / 快照分片 / 快照结束 / 全量完成），定义见
  `include/kvs_replication.h`；握手帧与 `+OK` 仍是不带类型的普通网络层帧。
- `RDB LOAD` / `AOF LOAD` 成功后会对在线 Replica 重新发起全量同步。
- fd 生命周期归网络层：复制模块只 `shutdown()` 触发对端断开，由网络层 `close()` 并回调
  `kvs_replication_remove_replica()`，避免 fd 被复用后复制帧写进普通客户端连接。
- 实时增量默认由 **eBPF（kprobe/uprobe + ring buffer）采集**（`KVS_ENABLE_EBPF_REALTIME=1`）：
  Master 把写命令从内核 ringbuf 取到用户态，再进增量日志由发送线程投递；不具备 eBPF 能力时
  写路径直接落增量日志，功能不变。采集只在 Master 本地进行，跨主机复制照常走 TCP/RDMA。

## RDMA 全量同步（可选）

编译时能找到 libibverbs/librdmacm 时会定义 `KVS_ENABLE_RDMA`，此时：

- Master 在 `监听端口 + KVS_RDMA_PORT_OFFSET(=1)` 上额外开一个 RDMA 监听（如 TCP 9999 → RDMA 10000）。
- Replica 每次（重）连接时先尝试用 RDMA 拉取全量快照，成功后再发 TCP 握手；Master 记录该
  快照对应的 backlog 序号，握手时只补发序号之后的增量。RDMA 只承担「一次性大批量搬运」，
  实时增量始终走 TCP（backlog + 发送线程）。
- 任何一步失败都会回退 TCP 全量快照，并在日志里打印失败步骤，例如
  `[RDMA] full sync 失败于步骤「rdma_resolve_addr」: errno=22(Invalid argument)`。

用软件 RDMA 做验证：

```bash
sudo ./setup_rdma.sh          # 默认在 ens33 上创建 SoftiWARP 链路 siw1
rdma link show                # 应显示 siw1/1 state ACTIVE
```

注意事项：

- `rdma link add` 创建的 SoftiWARP 链路**不持久化**，重启后要重新执行 `setup_rdma.sh`。
- Replica 的 `master_ip` 必须是 RDMA 网卡的 IP（例如 192.168.234.135），
  **不能用 127.0.0.1**：RDMA 无法 resolve 回环地址（`rdma_resolve_addr` 返回 EINVAL）。
- 同机部署时注意端口冲突：Master 的 RDMA 端口是 `主节点端口+1`，不能再被其它监听占用。
- 当前实现使用「普通 `IBV_WR_RDMA_WRITE` + 一条 SEND 完成通知」，不使用
  `IBV_WR_RDMA_WRITE_WITH_IMM`：SoftiWARP 下带立即数的写在 `ibv_post_send` 会直接返回
  ENOSPC(28)，导致整条 RDMA 全量同步失败。
- `siw` 是软件 RDMA（底层仍走 TCP/IP 栈），吞吐不会优于 TCP，主要用于在没有 RDMA 硬件的
  环境下跑通并验证 RDMA 代码路径；要拿到真实性能收益需要 RoCE 等硬件。

## eBPF 实时增量采集（kprobe + ring buffer）

实时增量默认由 eBPF 采集（`KVS_ENABLE_EBPF_REALTIME=1`），链路是：

```text
Master 写路径（持有存储锁）
  -> kvs_ebpf_notify_write(seq, argc, argv)      uprobe 挂载点
  -> BPF_PROG_TYPE_KPROBE 程序读取 argv，写 BPF_MAP_TYPE_RINGBUF
  -> 用户态消费线程解析记录、校验序号
  -> backlog（增量日志）+ 发送线程
  -> TCP / RDMA 投递给 Replica
```

关键点：

- **跨主机可用**：eBPF 只负责「本机把写命令采集出来」，跨主机投递仍是 TCP（或 RDMA），
  因此不要求 Master/Replica 在同一内核里。这正是它和旧的 `BPF_MAP_TYPE_QUEUE` 方案的本质区别——
  后者是单内核对象，只能同主机。
- **为什么是 uprobe**：触发点是 kvstore 自身的写路径。kprobe 与 uprobe 共用同一种程序类型
  （`BPF_PROG_TYPE_KPROBE`）和同一套 attach 流程，但 kprobe 只能挂内核函数——既读不到应用层的
  命令参数，也会在 proactor(io_uring) 后端下漏掉事件（该后端不经过 read/write 系统调用）。
- **丢事件可检测**：ringbuf 的 `reserve` 在队列满时会失败，属于「可能丢事件」的通道。每条记录
  带自增序号，消费线程一旦发现序号缺口就打印错误并触发一次全量重同步，绝不静默丢命令。
- **不截断命令**：单条命令最多采集 3 个参数、每个参数最多 1024 字节（含结尾 `\0`）；超过上限
  的命令不采集，由写路径直接落 backlog，并在落之前先等采集通道排空，保证顺序不变。
- **快照对齐**：取全量快照前会等采集通道排空（最多 200ms），保证 `base_seq` 与快照内容严格对齐。
- **自动回退**：任何一步 attach 失败（无 `CAP_BPF`、tracefs 不可写、内核不支持 ringbuf 等）都只
  影响「命令怎么进 backlog」，写路径会退回直接落 backlog，功能与数据一致性不受影响。

### 工作方式

1. Master 启动时加载 kprobe 程序、在自身写路径上注册 uprobe（tracefs `uprobe_events`），
   用 `perf_event_open` + `PERF_EVENT_IOC_SET_BPF` 完成 attach，然后 mmap ringbuf、拉起消费线程。
2. 每条成功的写命令在存储锁内调用 `kvs_ebpf_notify_write()`：内核里的程序读走参数、写进 ringbuf。
3. 消费线程把记录解析成 argv，交给复制模块编码后追加到增量日志 backlog，由发送线程按
   `next_seq` 投递给每个副本。
4. Replica 侧只认复制帧（增量命令 / RESET / 快照分片 / 快照结束 / 全量完成），与采集方式无关，
   所以同主机、跨主机共用同一条投递路径。

### 依赖与内核要求

- 仅支持 Linux x86_64；直接调用 `bpf(2)`，不依赖 libbpf/clang/bpftool。
- 需要内核支持 `BPF_MAP_TYPE_RINGBUF`（Linux 5.8+）以及 `bpf_probe_read_user_str` 等 helper。
- 需要 `CAP_BPF`（或 `CAP_SYS_ADMIN`）加载程序、创建 map，`CAP_PERFMON`（或 `CAP_SYS_ADMIN`）
  打开 perf 事件；写 tracefs 的 `uprobe_events` 需要 root 权限。
- 这一版不需要 bpffs：不再 pin 任何内核对象。

### 运行前一次性配置

```bash
# 1) 确认环境：内核版本、uprobe PMU/tracefs、perf_event_paranoid、当前 capability
./setup_ebpf.sh --check

# 2) 给编译产物加 capability（或直接用 root 运行）
sudo setcap cap_bpf,cap_perfmon,cap_sys_admin+ep ./build/kvstore
```

内核有 uprobe PMU（`/sys/bus/event_source/devices/uprobe/type`，本仓库常见环境都有）时
不需要 tracefs，也不需要 root，只要有上面两个 capability 即可。

仓库根目录的 `setup_ebpf.sh` 会自动检查内核版本、uprobe attach 途径、`perf_event_paranoid`
并执行 `setcap`：

```bash
./setup_ebpf.sh --check
./setup_ebpf.sh
./setup_ebpf.sh --binary build/kvstore --caps cap_bpf,cap_perfmon,cap_sys_admin+ep
./setup_ebpf.sh --help
```

> **每次 `cmake --build` 之后都要重新执行一次 `./setup_ebpf.sh`。** 内核会在文件被写入时
> 清除 `security.capability`（实测：重新链接后 `getcap build/kvstore` 变空），此时启动会打印
> `create ringbuf map failed ... Operation not permitted` 并回退直写 backlog，而不是报错退出。

### 启动与验证

```bash
cd build
./kvstore 9999 0
./kvstore 9999 1 127.0.0.1 9999
```

采集通道生效时，Master 启动日志会看到：

```text
[EBPF] uprobe attached: /path/to/kvstore:0x...
[EBPF] realtime capture active: uprobe ringbuf -> replication backlog (cross-host OK)
[REPLICATION] realtime increments captured by eBPF ringbuf
```

若看到下面这类日志，说明当前环境不具备 eBPF 能力（缺 capability、tracefs 不可写等），
写路径会自动改为直接落 backlog，复制与数据一致性不受影响：

```text
[EBPF] create ringbuf map failed (errno=1, Operation not permitted); fallback to direct backlog ...
[REPLICATION] eBPF capture unavailable, write path pushes backlog directly
```

在没有 BPF 权限的机器上（容器/CI）可以用模拟构建验证整条链路：

```bash
cmake -S . -B build-sim -DCMAKE_CXX_FLAGS=-DKVS_EBPF_SIM -DCMAKE_C_FLAGS=-DKVS_EBPF_SIM
cmake --build build-sim -j$(nproc)
```

该构建把内核部分换成等价的用户态 ringbuf（记录格式与消费代码完全一致），启动日志会显示
`[EBPF] simulation mode: ...`。

### 常见故障排查

| 现象 | 常见原因 | 处理方式 |
| --- | --- | --- |
| 重新编译后 `create ringbuf map failed` EPERM | 内核在文件被写入时清除了 `security.capability` | 重跑 `./setup_ebpf.sh`（每次 build 后都要） |
| `create ringbuf map failed` EPERM | 缺少 `CAP_BPF`/`CAP_SYS_ADMIN` | 重新 `setcap`，或直接用 root 运行 |
| `tracefs not writable` | 容器未暴露 `/sys/kernel/tracing` / debugfs | 授权 tracefs；否则仅采集通道退化为直写 backlog |
| `load kprobe program failed` 且带 verifier 日志 | 内核过老 / helper 不可用 | 看日志里的 verifier 输出；升级内核或接受回退 |
| `perf_event_open failed` EPERM | `perf_event_paranoid` 过高或缺 `CAP_PERFMON` | 调整 `kernel.perf_event_paranoid` 或补 capability |
| 日志出现 `ringbuf lost N event(s)` | 采集通道丢事件（ringbuf 满） | 已自动触发全量重同步；频繁出现可调大 `KVS_EBPF_RINGBUF_SIZE` |

### 移植性说明

- `replication/kvs_ebpf.cpp` 会无条件编译，因此目标平台需要提供 `<linux/bpf.h>` 与
  `<linux/perf_event.h>`。
- 采集程序按 x86_64 `pt_regs` 偏移读取寄存器（seq/argc/argv）；其它架构直接回退到直写 backlog。
- ringbuf 容量由 `KVS_EBPF_RINGBUF_SIZE`（默认 256 KiB）、单条记录大小由
  `KVS_EBPF_EVENT_ARG_MAX`（默认 1024）× 3 个参数决定；调大参数上限会同时放大记录与内核内存占用。

## 数据持久化与恢复

项目支持两种数据恢复方式，分别为全量恢复和增量恢复。服务启动后不会自动加载持久化数据，需要通过相应指令手动执行恢复。

### 1. 全量数据恢复（RDB）

- 主节点收到 `RDB SAVE` 后，会根据当前四种存储引擎状态生成全量快照文件：
  - `data/kvstore.data`
- 主节点收到 `RDB LOAD` 后，会清空当前存储状态，并从 `kvstore.data` 中恢复数据
- 恢复完成后，会同步快照给在线从节点，完成一次全量同步

### 2. 增量数据恢复（AOF）

- 在正常写入过程中，若不处于 AOF 恢复或 Replica 重放阶段，则会将写命令追加到：
  - `data/append.aof`
- 主节点收到 `AOF LOAD` 后，会读取并重放 `append.aof` 中的写命令，完成增量恢复
- 恢复完成后，会同步快照给在线从节点，完成一次全量同步

## 常见问题

### 构建时提示 NtyCo 缺失

```bash
git submodule sync --recursive
git submodule update --init --recursive
```

报 `nty_coroutine.h: No such file or directory` 说明 submodule 还没有初始化，按上面的
`git submodule` 命令同步即可。

如果 `NTYCO_ROOT` 指向的目录里没有 `core/nty_coroutine.h`，CMake 配置阶段会直接报错并
提示先初始化子模块，因此不会出现链接阶段找不到 NtyCo 符号的情况。

### 找不到 liburing / jemalloc

```bash
sudo apt-get install -y liburing-dev libjemalloc-dev
```

liburing 是构建期必需依赖。内存分配由 `include/memorypool.h` 中的编译期宏
`ENABLE_MEMORYPOOL` 决定：置为 `1` 时 `kvs_malloc`/`kvs_calloc`/`kvs_free` 走内置 slab
内存池，置为 `0` 时走系统 `malloc`/`calloc`/`free`。若构建时链接了 jemalloc，则
`ENABLE_MEMORYPOOL=0` 时实际使用的是 jemalloc。

### 找不到头文件或链接库

可检查 CMake 变量是否正确，或在命令行中显式指定依赖目录：

```bash
cmake -S . -B build -DNTYCO_ROOT=/path/to/NtyCo
```

## 目录结构

```text
.
├── CMakeLists.txt
├── kvstore.cpp
├── include/
├── network/
├── storage/
├── persistence/
├── replication/
├── testcase/
├── data/
├── NtyCo/             # git submodule
├── build/
└── readme.md
```

## 备注

- `data/kvstore.data` 是 RDB 全量快照文件
- `data/append.aof` 是 AOF 增量日志文件
- 服务器启动时默认不会自动恢复数据，需通过命令手动触发
