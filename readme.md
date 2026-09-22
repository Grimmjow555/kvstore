# kvstore

一个基于 RESP 协议、支持主从复制与持久化恢复的 KV 存储服务端项目。

## 项目概览

- 网络框架：reactor(epoll)、proactor(io_uring)、协程框架 NtyCo
- 存储引擎：array、rbtree、hash、skiptable
- 运行时内存分配：jemalloc
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

- `NtyCo-master`

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

> 说明：当前项目的 CMake 会优先使用 `NtyCo-master` 目录下的依赖结构，确保 submodule 初始化后可以直接编译。

## 最小编译命令

```bash
cd /path/to/kvstore
git submodule sync --recursive
git submodule update --init --recursive
cmake -S . -B build
cmake --build build -j$(nproc)
```

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

服务支持从配置文件读取监听地址、端口、日志级别、主从模式和持久化模式。默认会依次尝试加载
`./kvstore.conf` 和 `../kvstore.conf`，也可以使用 `--config <path>` 显式指定。

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
persistence_mode both
```

字段说明：

- `bind` / `port`：服务监听地址与端口。
- `log_level`：`debug`、`info`、`warn`、`error` 或 `off`。
- `role`：`master` 或 `replica`（也接受 `slave`、`0`、`1`）。
- `master_ip` / `master_port`：角色为 `replica` 时使用的主节点地址与端口。
- `persistence_mode`：`none`、`rdb`、`aof` 或 `both`；也可以用 `rdb on/off`、
  `aof on/off` 分别控制。

配置文件加载后，命令行开关可以覆盖其中的值，旧的位置参数方式仍兼容：

```bash
./build/kvstore --config ../kvstore.conf --port 7000 --role replica \
  --master-ip 127.0.0.1 --master-port 19001 --persistence-mode aof
```

## 主从复制与同步

- 主节点和从节点通过复制模块进行连接与同步
- 从节点启动时需要指定主节点地址与端口
- 主节点启动后可对在线从节点下发快照和增量同步指令
- 当 Replica 通过回环地址连接同一台主机上的 Master 时，实时增量同步会优先使用 eBPF 队列
  `/sys/fs/bpf/kvstore/kvstore_replication_<master_port>`；内核权限、文件系统权限或跨主机场景下
  会自动回退到 TCP，不影响服务启动。

## eBPF 实时同步配置

项目在“Master 和 Replica 位于同一台主机，且 Replica 通过回环地址连接 Master”时，会尝试用
`BPF_MAP_TYPE_QUEUE` 传输实时增量命令。eBPF 仅用于本地回环场景；跨主机同步仍使用 TCP。
eBPF 初始化失败时会自动回退 TCP，不会阻止 `kvstore` 启动。

### 依赖与内核要求

- 仅支持 Linux；当前实现直接调用 `bpf(2)` 系统调用，不依赖 libbpf。
- 需要内核支持 `BPF_MAP_TYPE_QUEUE` 和 `BPF_MAP_LOOKUP_AND_DELETE_ELEM`，建议 Linux 4.20 或更高版本。
- `BPF_MAP_TYPE_QUEUE` 属于特权 map 类型，通常需要 `CAP_BPF`（Linux 5.8+）或 `CAP_SYS_ADMIN`。
- 某些发行版即使设置了 `kernel.unprivileged_bpf_disabled=0`，仍会禁止非特权进程创建
  queue/stack map，因此建议显式给二进制添加 capability，而不是依赖非特权 BPF 开关。
- 需要 `/sys/fs/bpf`（bpffs）挂载为可写，并允许运行用户创建/删除 pin 文件。

### 运行前一次性配置

1. 确认 bpffs 已挂载且可写：

```bash
mount | grep ' bpf '
sudo mount -t bpf bpf /sys/fs/bpf       # 如果尚未挂载
sudo mount -o remount,rw /sys/fs/bpf    # 如果当前是只读挂载
```

2. 创建 eBPF 队列使用的 pin 子目录，并授权给运行用户：

```bash
sudo mkdir -p /sys/fs/bpf/kvstore
sudo chown "$USER":$(id -gn) /sys/fs/bpf/kvstore
sudo chmod 700 /sys/fs/bpf/kvstore
```

> 当前代码中的 pin 目录固定为 `/sys/fs/bpf/kvstore`。如果需要改到其他路径，
> 请同步修改 `replication/kvs_ebpf.cpp` 中的 `KVS_EBPF_PIN_DIR` 后重新编译。

3. 给编译产物添加 capability：

```bash
sudo setcap cap_bpf,cap_sys_admin+ep ./build/kvstore
```

注意：

- Linux 5.8 以下没有 `CAP_BPF`，可只使用 `cap_sys_admin+ep`。
- 每次重新编译并替换 `build/kvstore` 后，通常需要重新执行一次 `setcap`。
- 如果文件系统不支持 xattr/security.capability 或挂载为 `nosuid`，`setcap` 可能失败；
  此时可先用 `sudo ./build/kvstore` 做临时验证，但正式运行仍建议放到支持 capability 的文件系统上。

### 一键配置脚本

仓库根目录提供了 `setup_ebpf.sh`，可以自动完成 bpffs 检查、pin 目录创建和 `setcap`：

```bash
./setup_ebpf.sh
```

也可以指定二进制路径或 capability：

```bash
./setup_ebpf.sh --binary build/kvstore
./setup_ebpf.sh --caps cap_bpf,cap_sys_admin+ep
```

查看完整选项：

```bash
./setup_ebpf.sh --help
```

### 启动与验证

配置完成后，以普通用户身份启动即可：

```bash
cd build
./kvstore 9999 0
./kvstore 9999 1 127.0.0.1 9999
```

当 eBPF 队列可用时，启动日志中会看到类似输出：

```text
[EBPF] master realtime sync queue ready: /sys/fs/bpf/kvstore/kvstore_replication_9999
[EBPF] replica realtime sync queue ready: /sys/fs/bpf/kvstore/kvstore_replication_9999
```

如果看到 `fallback to TCP sync` 或 `falling back to TCP realtime sync`，说明 eBPF 路径不可用，
服务仍会继续运行，只是实时同步走 TCP。

### 常见故障排查

| 现象 | 常见原因 | 处理方式 |
| --- | --- | --- |
| 创建 queue 返回 `EPERM` | 缺少 `CAP_BPF`/`CAP_SYS_ADMIN`，或内核禁止该 map 类型 | 重新执行 `setcap`，并检查内核版本和安全策略 |
| pin 返回 `EEXIST` | 上一次运行遗留了同名 pin 文件 | 删除 `/sys/fs/bpf/kvstore/kvstore_replication_<port>` 后重启 |
| pin 返回 `EROFS` | `/sys/fs/bpf` 是只读挂载 | `sudo mount -o remount,rw /sys/fs/bpf` |
| pin 返回 `EPERM` 或 `ENOENT` | pin 目录不存在或当前用户无写权限 | 创建并 chown `/sys/fs/bpf/kvstore` |
| 容器/受限环境中始终回退 TCP | 容器未暴露 bpffs、capability 或内核 BPF 能力 | 属预期行为；如必须 eBPF，需要调整容器权限或改用 TCP |

### 移植性说明

- 当前 CMake 会无条件编译 `replication/kvs_ebpf.cpp`，因此目标平台需要提供 `<linux/bpf.h>`。
- eBPF 队列值大小为约 1 MiB，队列容量为 16，Master 侧会额外占用约 16 MiB 内核 map 内存。
- 如果目标环境不能使用 `BPF_MAP_TYPE_QUEUE`，需要修改 `replication/kvs_ebpf.cpp`，
  例如改为 `BPF_MAP_TYPE_ARRAY`/`BPF_MAP_TYPE_HASH` 并在用户态实现 FIFO；否则服务会自动退化为 TCP 同步。

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

### 找不到 liburing / jemalloc

```bash
sudo apt-get install -y liburing-dev libjemalloc-dev
```

### 找不到头文件或链接库

可检查 CMake 变量是否正确，或在命令行中显式指定依赖目录：

```bash
cmake -S . -B build -DNTYCO_ROOT=/path/to/NtyCo-master
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
├── NtyCo-master/      # git submodule
├── build/
└── readme.md
```

## 备注

- `data/kvstore.data` 是 RDB 全量快照文件
- `data/append.aof` 是 AOF 增量日志文件
- 服务器启动时默认不会自动恢复数据，需通过命令手动触发
