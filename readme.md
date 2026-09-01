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

## 主从复制与同步

- 主节点和从节点通过复制模块进行连接与同步
- 从节点启动时需要指定主节点地址与端口
- 主节点启动后可对在线从节点下发快照和增量同步指令

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



