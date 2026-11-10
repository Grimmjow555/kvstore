# 主从同步重设计：RDMA / eBPF 独立进程方案

本文档描述 kvstore 主从同步的整体重做方案：把原本全部运行在 kvstore 进程内的
增量采集与 RDMA 全量搬运，拆分为独立进程；kvstore 进程只保留控制流与快照生成。

本文档为设计定稿，对应「先出方案、暂不动代码」的约定；实现进度另见各阶段验收章节。

---

## 1. 背景与目标

### 1.1 现状

重设计前的实现（`replication/kvs_replication.cpp`、`replication/kvs_rdma.cpp`、
`replication/kvs_ebpf.cpp`；其中 `kvs_ebpf.cpp` 后来并入 `kvs_replication.cpp`）
把三件事都放在 kvstore 进程里：

1. 写命令经 eBPF uprobe 采集后落入进程内 `backlog`，由 `repl_sender_thread`
   按副本 `next_seq` 通过 TCP 投递；
2. Replica 发送 `REPLICA` 握手，Master 在同一进程内序列化 RDB 并通过
   `RESET` + `SNAPSHOT` 分片帧下发全量快照（可选走进程内 RDMA 监听线程加速）；
3. 全量快照与增量的分界靠「同一把存储锁内取快照 + 记录 `base_seq`」保证。

### 1.2 目标

按拓扑时序图重做主从同步，要求：

- **RDMA 独立进程**：全量快照的搬运由 `rdma-client`（主）/ `rdma-server`（从）
  两个独立进程完成，kvstore 进程内不再有 RDMA 监听线程。
- **eBPF 独立进程**：增量采集与向 Replica 的转发由 `ebpf-agent` 独立进程负责，
  kvstore 进程内不再维护 `backlog` 与发送线程。
- **快照落文件**：全量快照不再走 TCP 分片帧，而是由 Master 写成 `.tmp` 文件后
  经 RDMA 搬运，Replica 从本地文件整体加载。
- **同步期间不丢写**：全量同步期间 Master 收到的新写入，由 ebpf-agent 缓存，
  待同步完成后按序补发。

### 1.3 非目标

- 不引入 RDMA 增量通道（RDMA 只承担一次性大块搬运）。
- 不改变存储引擎语义、RESP 协议、RDB/AOF 文件格式。

### 1.4 多从机升级

当前实现已升级为“Master 多会话 + 单 `ebpf-agent` 扇出”：

- Master 为每次全量同步分配 16 位十六进制 `session_id`，每个会话拥有独立临时文件和
  `rdma-client`；并发上限由 `max_replicas` 控制，默认 16。
- Agent 按 `session_id` 维护最多 16 个目标，每目标独立保存 `AWAITING_SYNC /
  CACHING / LIVE` 状态、缓存和发送队列。写命令广播给所有 `LIVE` 目标，并缓存到各自的
  `CACHING` 目标。
- 慢目标或缓存超限只断开该目标；其他 Replica 继续接收实时增量。
- 复制协议只保证同版本节点，旧版裸 `EBPF`、`REPL_INC` 和无会话 ID 的控制命令会被拒绝。

---

## 2. 角色与进程

| 角色 | 可执行文件 | 所在机器 | 职责 |
| --- | --- | --- | --- |
| Master | `kvstore`（role=0） | 主 | KV 服务、处理多会话 `SYNC`/`SYNC FINISHED`、生成会话专用 `.tmp`、启停 `rdma-client` |
| Replica | `kvstore`（role=1） | 从 | KV 服务、启停 `rdma-server`、发 `SYNC`、加载 `.tmp`、发 `SYNC FINISHED`、回放增量 |
| RDMA 客户端 | `rdma-client` | 主 | 独立进程，读 `.tmp` 用 RDMA 写给 `rdma-server`，完成后被 Master 回收 |
| RDMA 服务端 | `rdma-server` | 从 | 独立进程，RDMA 监听、接收并落盘 `.tmp`，收完退出 |
| eBPF 采集 | `ebpf-agent` | 主 | 独立进程，采集一条有序流并扇出到最多 16 个 Replica |

`rdma-client` / `rdma-server` 由各自的 kvstore 进程按需 `fork` + `exec`；
`ebpf-agent` **手动启动**（不随 Master 自动拉起），便于在其未就绪时明确观测到
「采集未武装」的状态。

---

## 3. 拓扑与时序

### 3.1 连接拓扑

```
          +-------------------+                       +-------------------+
          |      Master       |<---- 控制连接(SYNC) ---|      Replica      |
          |   kvstore role=0  |----  +OK / -ERROR  --->|   kvstore role=1  |
          +-------------------+                       +-------------------+
             ^          |                               ^        |        ^
             |          | fork/exec                     |        |        | fork/exec
      EBPF握手|          v                               |        |        v
          +--------+  +--------------+        增量连接   |        |  +--------------+
          |ebpf-   |  | rdma-client  |---------RDMA---->|        +--| rdma-server  |
          |agent   |--+--------------+       (文件)     +-----------+--------------+
          +--------+   (主：.tmp 源)               (从：.tmp 目标)
```

连接清单：

| 连接 | 发起方 → 接收方 | 传输 | 用途 |
| --- | --- | --- | --- |
| 控制连接 | `slave` → `master`（Master 数据端口） | TCP | 发 `SYNC <rdma_ip> <rdma_port>` / `SYNC FINISHED <session_id>` |
| 注册连接 | `ebpf-agent` → `master`（Master 数据端口） | TCP | 握手 `EBPF 2`，登记「采集已就绪」，断线即失效 |
| 增量连接 | `slave` → `ebpf-agent`（Agent 监听端口） | TCP | Replica 握手 `REPL_INC 2 <session_id>`，Agent 单目标推送 |
| RDMA 连接 | `rdma-client` → `rdma-server` | RDMA | 搬运 `.tmp` 快照文件 |

### 3.2 时序（对应拓扑时序图 5 步 + 同步期缓存注解）

```
1. Master 起 → testcase 写 5w

2. ebpf-agent 起 → 解析 Master ELF 符号 → uprobe attach → 连 Master 发 EBPF 握手（armed）
   Replica  起 → 派生 rdma-server → 连 Master 上报自身 RDMA endpoint 并发 SYNC

3. Master: 分配 session_id → [存储锁] notify(SYNC <id>) + kvs_snapshot_serialize → 解锁
          → 写 data/kvstore.sync.<id>.tmp → fork/exec rdma-client
   ebpf-agent: 只把对应 session 转入 CACHING
   rdma-client --RDMA--> rdma-server（从机落独立的接收临时文件）

4. Replica: rdma-server 退出 → kvs_snapshot_load(.tmp) → 握手 REPL_INC 2 <id>
   → 发 SYNC FINISHED <id> → 回收 rdma-server
   Master:  notify(SYNC FINISHED <id>) → unlink 该会话 .tmp → 回收对应 rdma-client
   ebpf-agent: 只冲刷该 session 的缓存
               → 转入 LIVE，后续实时转发

5. testcase_a 写 5w → ebpf-agent 采集 → 增量连接 → Replica 回放
```

同步期间 Master 收到的新写入不丢：ebpf-agent 从读到 `SYNC` 记录起缓存，直到读到
`SYNC FINISHED` 记录才冲刷，覆盖整个「快照生成 + 文件搬运 + 从机加载」窗口。

---

## 4. 锁定的设计决策

| 编号 | 决策 | 取值 |
| --- | --- | --- |
| D1 | 增量通道形式 | **反向专用通道**：Replica 以 `REPL_INC 2 <session_id>` 握手连接 Agent；Agent 按目标推送增量 |
| D2 | 副本规模 | **最多 16 台**；Master 管全量会话，单 Agent 管每目标增量状态 |
| D3 | eBPF 是否强制 | **强制**：Master 仅在 ebpf-agent 已注册（armed）时受理 `SYNC`；采集失效即视为同步不可用 |
| D4 | `.tmp` 位置 | **会话专用**：`data/kvstore.sync.<session_id>.tmp`，由父进程以显式路径传给 RDMA 子进程 |

---

## 5. 通信协议

### 5.1 帧格式

所有 TCP 通道沿用现有两层协议：外层 `4 字节网络序长度 + 负载`，内层为 RESP。
增量连接复用同一外层帧，但语义上只承载「一条 RESP 写命令」。

### 5.2 握手 token

| token | 方向 | 含义 | 响应 |
| --- | --- | --- | --- |
| `EBPF 2` | ebpf-agent → Master | 版本化采集进程注册 | `+OK\r\n` 并武装采集 |
| `REPL_INC 2 <session_id>` | Replica → ebpf-agent | 绑定目标增量通道 | `+OK\r\n`，Agent 保存该目标 fd |
| `REPLICA`（删除） | — | 旧握手 | 移除 |

握手帧必须包含版本；无版本旧握手会被拒绝。

### 5.3 控制命令（Master 命令表新增）

需同时修改 `kvstore.cpp` 的 `command[]`、`enum KVS_CMD` 与 `kvs_filter_protocol`
三处，并沿用 AGENTS.md 的约定。

| 命令 | 方向 | 语义 | 响应 |
| --- | --- | --- | --- |
| `SYNC <rdma_ip> <rdma_port>` | Replica → Master | 请求一次全量同步 | `+OK <session_id>\r\n` 或 `-ERROR\r\n` |
| `SYNC FINISHED <session_id>` | Replica → Master | 从机已加载对应会话 `.tmp` | `+OK\r\n` 或 `-ERROR\r\n` |

注意：带空格的命令是**单个 token**（与现有 `RDB SAVE` 的处理一致），不是两个参数。

### 5.4 增量连接帧

全量改走 RDMA 文件后，增量连接只需携带命令，可简化为与普通客户端一致的
`[uint32 长度][RESP 文本]`。Replica 侧网络层仅在握手分支上区分该连接，
收到负载后按复制回放处理（置 `kvs_replication_set_replaying(1)`）。

---

## 6. 文件与路径

- Master 文件名统一为 `kvstore.sync.<session_id>.tmp`，Replica 接收中的临时文件为
  `kvstore.sync.replica.<pid>.<fd>.tmp.recv`，收到会话 ID 后改名为
  `kvstore.sync.<session_id>.tmp.recv`。
- 两端各自维护一份：Master 侧是源文件，Replica 侧是目标文件；路径由父进程
  `--file` 显式传给 `rdma-client` / `rdma-server`，避免独立进程各自推断 cwd。
- 文件格式沿用现有 RDB 格式（magic `KVSDB01`），复用 `kvs_snapshot_save` /
  `kvs_snapshot_load`，不引入新格式。
- 生命周期：
  - Master：`SYNC` 时创建 → `SYNC FINISHED <session_id>` 或 ABORT/超时时只清理该会话；
  - Replica：加载成功后删除本地副本；加载失败时保留会话 `.recv` 作为完整性故障现场；
  - 各会话路径互不覆盖，多个 Replica 可同时全量同步。

---

## 7. 一致性与正确性

### 7.1 缓存窗口对齐（核心机制）

难点是「缓存从哪一刻开始」。方案是**把 `SYNC` / `SYNC FINISHED` 与写命令放进同一
条有序采集流**：

1. Master 的写路径与 `SYNC` 处理都在存储锁内调用同一个 uprobe 挂载点
   `kvs_ebpf_notify_write(seq, argc, argv)`，`SYNC` 以 `argv = {"SYNC"}` 表示；
2. seq 在同一把锁内单调递增，故 ringbuf 顺序 == 提交顺序，ebpf-agent 消费线程
   读到 `SYNC` 记录一定早于「其之后发生的任何写记录」；
3. Master 处理顺序固定为「锁内 `notify(SYNC)` → 锁内序列化 → 锁外写文件」，
   故快照点必然不早于缓存起点；
4. ebpf-agent 的缓存区间 = `[SYNC 记录, SYNC FINISHED 记录]`，必然覆盖
   `[快照, 从机加载完成]` 这段窗口。

### 7.2 重复无害性论证

上述方案唯一会产生的「重复」落在 `[SYNC, 序列化开始]`：这些命令既在快照中，又被
缓存重放。该窗口内命令的重放是幂等的：

- `SET` / `RSET` / `HSET` / `SSET`：键已存在时 `kvs_*_set` 返回 1 且**不覆盖**，
  与快照中的同名同值结果一致；
- `DEL` / `RDEL` / `HDEL` / `SDEL`：对缺失键返回「不存在」，为空操作；
- `MOD` / `RMOD` / `HMOD` / `SMOD`：对同值重复应用结果一致。

因此重放同批命令后，Replica 最终状态与 Master 一致。

> 严格「零重复」需要额外共享水位（mmap 文件或 BPF map 暴露 `sync_base_seq`），
> 本期不做，留作可选增强（见 §14）。

### 7.3 顺序保证

- 单条增量连接是有序 TCP 流，ebpf-agent 按采集顺序发送，Replica 按接收顺序回放；
- 冲刷缓存与实时转发共用同一条连接，先发缓存、后发实时，不会乱序。

---

## 8. 各进程详细规格

### 8.1 Master（kvstore, role=0）

**命令处理**

- `SYNC` 分支（`kvs_filter_protocol` 内，天然持存储锁）：
  1. 校验 `g_ebpf_armed`，未武装则回 `-ERROR` 并直接返回；
  2. 锁内 `notify(SYNC)`；
  3. 锁内 `kvs_snapshot_serialize()` 到堆缓冲；
  4. 解锁后把「写文件 + 派生 rdma-client」作为任务交给 **sync worker 线程**，
     避免文件 I/O 与 `fork/exec` 阻塞事件循环。
- `SYNC FINISHED` 分支：
  1. 锁内 `notify(SYNC FINISHED)`；
  2. 任务交给 sync worker：`unlink(.tmp)` → `SIGTERM` + `waitpid(rdma-client)`。

**写路径**

- `kvs_replication_append()` 保留函数名与 `kvstore.cpp` 中全部调用点，但实现改为
  「只调用 tap，不再写 backlog」。eBPF 不可用时不再有进程内兜底（D3）。

**eBPF 武装状态**

- `g_ebpf_armed` 由 `EBPF 2` 握手置位，连接断开清零；
- 同步进行中若 armed 变假，记录错误并标记需要重同步。

**删除项**

- `backlog`、`g_backlog_*`、`g_slots`、`repl_sender_thread`、`build_tcp_full_sync_prefix`、
  `start_full_sync_locked`、`REPLICA` 握手、进程内 RDMA 监听线程、`g_rdma_notes`。

### 8.2 Replica（kvstore, role=1）

同步驱动（替代现有 `replica_supervisor_thread`）：

1. 启动监听端口（必须先于 ebpf-agent 连接）；
2. 派生 `rdma-server`，等待子进程退出（带超时，超时视为失败）；
3. 连 Master 发 `SYNC`；
4. 收到 `+OK` 且 `rdma-server` 完成后，`kvs_snapshot_load(<本地 .tmp>)`
   （内部先 `kvs_reset_data()`）；
5. 主动连接 Agent，发送 `REPL_INC 2 <session_id>` 并等待 `+OK`；
6. 启动增量接收线程，同一条控制连接发 `SYNC FINISHED`，回收 `rdma-server`；
7. 在反向连接上持续接收并在专用线程按序回放；
8. **增量连接断开 = 重同步信号**：回到第 2 步再走一遍，退避策略沿用
   200ms 起、上限 5s 的指数退避（可打断）。

### 8.3 rdma-client（主）

- 参数：`--file <.tmp> --peer <replica_ip> --port <rdma_port>`。
- 复用 `replication/kvs_rdma.cpp` 的控制消息与传输原语：读取文件到缓冲 → 注册 MR →
  把长度告知对端 → 等对端回地址/rkey → `IBV_WR_RDMA_WRITE` → 单独一条 SEND 通知。
- **不得改回 `IBV_WR_RDMA_WRITE_WITH_IMM`**：SoftiWARP 下 `ibv_post_send` 会返回
  ENOSPC(28)。
- 完成后正常退出；由 Master 在 `SYNC FINISHED` 时回收（正常情况已退出，仅需 `waitpid`）。

### 8.4 rdma-server（从）

- 参数：`--listen-port <rdma_port> --out <.tmp>`。
- RDMA 监听 → 接收长度与对端缓冲信息 → 注册本地接收缓冲 → 回地址/rkey →
  等 RDMA_WRITE + SEND 通知 → 数据落盘 `fsync` → 退出。
- 退出即表示文件完整，Replica 以子进程退出作为「文件就绪」信号。

### 8.5 ebpf-agent（主，手动启动）

**符号解析**

- 现有 `resolve_self_symbol_offset()` 基于 `dl_iterate_phdr`，只能解析本进程；独立
  进程需改为解析 **Master 二进制的 ELF**：读 `.symtab` 找到 `kvs_ebpf_notify_write`，
  再按 PT_LOAD 段做 vaddr → 文件偏移换算（复用同一套映射数学）。
- 参数：`--pid <master_pid> --binary <master_path> --listen <ip:port>`。

**挂载**

- 首选 uprobe 动态 PMU：`perf_event_open(type=uprobe, pid=master_pid, probe_offset=offset)`；
- 回退 tracefs `uprobe_events`；
- 两者都失败时按 D3 **直接告警退出**，不静默降级。

**状态机**

```
IDLE ──读到 SYNC 记录──> CACHING ──读到 SYNC FINISHED 记录──> FLUSH ──> LIVE
  ^                                                                      |
  +---------------------- 增量连接断开 / seq 缺口 / 缓存溢出 -------------+
```

- `IDLE`：捕获写入但不转发（尚无同步目标）；
- `CACHING`：缓存写入，不向 Replica 发送；
- `FLUSH`：按序把缓存刷给 Replica；
- `LIVE`：实时转发。

**缓存与健康**

- 内存队列上限：默认 **256MB / 50 万条**（两项任一先到即视为满）；
- ringbuf seq 缺口、缓存溢出 → 关闭增量连接（由 Replica 侧触发重同步），
  绝不静默丢命令；
- 定时健康检查沿用现有「通知序号 vs 消费序号」看门狗思路。

**SIM 模式**

- 运行期由 `ebpf_capture_mode` 选择：`uprobe`（默认，agent 在内核挂探针 + ringbuf）或
  `sim`（Master 写 `data/` 下的共享内存环、agent 读该环）。sim 让 Master 与独立的
  ebpf-agent 在无 CAP_BPF 的容器/CI 中也能端到端跑通，两条路径共用同一套记录格式与消费代码。

---

## 9. 异常处理矩阵

| 事件 | 行为 |
| --- | --- |
| 事件 | 行为 | 验证 |
| --- | --- | --- |
| `SYNC` 时 ebpf 未 armed | Master 回 `-ERROR`，不生成快照；Replica 退避后重试 | 已自动化（`run_sync_faults.sh` A） |
| ebpf-agent 同步中掉线 | Master 记录错误并拒绝后续 `SYNC`；Replica 增量连接断开 → 重同步 | 已自动化（A 的未武装分支 + B） |
| rdma-client 启动或传输失败 | Master 记录错误日志；Replica 等文件失败/超时 → 重试 `SYNC` | 已自动化（`run_sync_faults.sh` C） |
| `.tmp` 加载失败 | Replica 不发 `SYNC FINISHED`，回退重同步 | 代码路径复用 C 的重试（未单独注入损坏文件） |
| ringbuf 缺口 / 缓存溢出 | ebpf-agent 断开增量连接 → Replica 重同步 | 已自动化（`run_sync_faults.sh` B） |
| 增量通道迟迟不出现 | Replica 等待上限 10s，超时后重新全量同步 | 已自动化（B 的超时分支） |
| Master 重启 | sim：agent 重开采集环；uprobe：提示需带新 pid 重启 agent（退出码 3） | 代码路径 + 日志提示（未自动化） |
| Replica 重启 | 重新走 §8.2 全流程 | 已由全量同步脚本覆盖 |
| Master 上 RDB/AOF LOAD | 写 `INVALIDATE` 记录 → agent 断开通道 → Replica 重新全量同步 | 已实测（§12.4） |

---

## 10. 配置项（`kvstore.conf` / `kvs_config`）

新增键（沿用「`key value` 或 `key=value`」写法与现有默认加载顺序）。
当前实现支持以下配置：

```
# 主从同步（重设计）
rdma_client_bin          ./rdma-client                 # 已实现
rdma_server_bin          ./rdma-server                 # 已实现
rdma_port                10000                         # 已实现
rdma_advertise_host      (空)                          # 可选覆盖，默认自动检测
replica_host             (已废弃，保留解析兼容)         # 新协议不使用
max_replicas             16                            # Master 未完成全量会话上限
sync_session_timeout_ms  0                             # 0 = RDMA 超时 + 等待超时 + 30s
rdma_transfer_timeout_ms 60000                         # 已实现
sync_wait_timeout_ms     120000                        # 已实现

ebpf_capture_mode        uprobe                        # uprobe / sim
ebpf_ring_file           ../data/kvstore.ebpf.ring     # sim 模式的采集环
max_replicas             16                            # Agent 同时维护的目标数
cache_max_bytes          268435456                     # 每目标缓存上限
cache_max_records        500000                        # 每目标缓存命令数
memory_budget_bytes      1073741824                    # 所有目标共享内存预算
```

说明：

- 全量同步临时快照按会话隔离：Master 使用 `../data/kvstore.sync.<session_id>.tmp`；
- `rdma_port` 显式可配，不复用「数据端口 + 1」（旧进程内监听曾占用该端口，
  见 §12.2）；
- Replica 侧接收路径按进程/控制 fd 隔离，收到 session_id 后改名并校验加载；
- 默认值即为 §8.5 与 §9 采用的取值。

---

## 11. 代码改动清单

**新增**

- `tools/rdma_client.cpp`、`tools/rdma_server.cpp`、`tools/ebpf_agent.cpp`
- `include/kvs_rdma_transfer.h`、`replication/kvs_rdma_transfer.cpp`
  （两个 RDMA 工具共用的搬运实现；阶段 2 已完成）
- `include/kvs_ebpf_ring.h`、`replication/kvs_ebpf_ring.cpp`（采集环 + 记录编解码；
  阶段 3 已完成）
- `include/kvs_ebpf_agent.h`、`replication/kvs_ebpf_agent.cpp`（agent 侧采集；
  阶段 3 已完成）
- `include/kvs_netutil.h`、`replication/kvs_netutil.cpp`：复制相关进程共用的 TCP
  收发工具
- `include/kvs_replication.h`、`replication/kvs_sync.cpp`：复制握手 token、同步接口，
  以及 Master 的 `SYNC`/`SYNC FINISHED` 处理 + `.tmp` 管理 + 子进程生命周期、
  Replica 的同步驱动
- `testcase/run_sync_fullsync.sh`、`testcase/run_sync_increment.sh`、
  `testcase/run_sync_faults.sh`、`testcase/run_backends_check.sh`
  （全量 / 全量+增量 / 异常路径 / 三后端协议验证脚本）与公共库
  `testcase/lib_sync_test.sh`（路径、进程启停、端口与日志等待、配置生成、
  运行时文件清理）

**删除 / 精简**

- `replication/kvs_replication.cpp`：`backlog`、`g_slots`、`repl_sender_thread`、
  `REPLICA` 握手、`build_tcp_full_sync_prefix` / `start_full_sync_locked`、
  RDMA note 逻辑（阶段 4 已完成）
- `replication/kvs_rdma.cpp`（进程内 RDMA 监听，阶段 4 整文件删除）；
  `kvstore` 不再链接 `ibverbs` / `rdmacm`

**保留 / 复用**

- `kvs_replication_is_replaying()` / `kvs_replication_set_replaying()`（Replica 回放）
- `kvs_snapshot_serialize` / `kvs_snapshot_save` / `kvs_snapshot_load` /
  `kvs_snapshot_load_buffer`
- `replication/kvs_rdma.cpp` 的传输原语（RDMA_WRITE + 单独 SEND）
- `kvs_ebpf_notify_write` 挂载点符号（noinline、used）

**修改**

- `kvstore.cpp`：命令表新增 `SYNC` / `SYNC FINISHED`；`main` 中 Replica 分支改为
  启动同步驱动；Master 分支不再启动旧 RDMA 监听；写分支调用点不变
- `network/{reactor,proactor,ntyco}`：握手分支只识别 Master 侧的 `EBPF 2`，
  移除 `REPLICA`；反向 `REPL_INC 2` 不经过这三个后端
- `config/kvs_config.cpp` + `kvstore.conf`：新增 §10 配置键
- `CMakeLists.txt`：新增 `rdma_client` / `rdma_server` / `ebpf_agent` 三个可执行
  目标及 `ibverbs` / `rdmacm` 链接（前两个阶段 2 已完成）
- `readme.md`、`setup_rdma.sh`、`setup_ebpf.sh`、`AGENTS.md`：同步文档与脚本

---

## 12. 实施阶段与验收

| 阶段 | 内容 | 验收标准 | 状态 |
| --- | --- | --- | --- |
| 1 | `SYNC`/`SYNC FINISHED` + `.tmp` 落盘加载 | 单机触发一次全量，Replica 数据与 Master 一致 | 已完成 |
| 2 | RDMA 拆为 `rdma-client` / `rdma-server` | `setup_rdma.sh` 后 `.tmp` 经 RDMA 到达且内容校验一致 | 已完成 |
| 3 | eBPF 拆为 `ebpf-agent` + 有序 SYNC 事件 + 缓存窗口 | 全量期间并发写入，Replica 最终与 Master 一致（即 10w 用例） | 已完成 |
| 4 | 删除旧路径 + 回归 | RDB/AOF 回归通过；reactor / ntyco / proactor 三后端协议验证通过；断线重连正常 | 已完成 |
| 5 | 异常兜底与文档 | 异常矩阵各条目可复现并按预期恢复；日志与文档补齐 | 已完成 |

### 12.1 阶段 1 实现记录

已落地内容（可对照提交内容核对）：

- 新增模块 `replication/kvs_sync.cpp`（接口现已并入 `include/kvs_replication.h`）：
  - Master：`kvs_sync_handle_sync()` 分配 session_id，只在存储锁内发布事件和序列化，
    `.partial` + `fsync` + `rename` 与子进程管理在锁外完成。
  - Replica：`kvs_sync_start()` 拉起后台线程，连接 Master → `SYNC` →
    加载 `.tmp` → `SYNC FINISHED`，失败按 200ms→5s 指数退避重试。
- `kvstore.cpp`：命令表、`KVS_CMD` 枚举、`kvs_filter_protocol` 分支新增
  `SYNC` 与 `SYNC FINISHED`；Replica 分支改走 `kvs_sync_start()`。
- 临时快照路径为 `../data/kvstore.sync.<session_id>.tmp`，不同会话互不覆盖。
- 验证脚本 `testcase/run_sync_fullsync.sh`（阶段 1 时为 `run_sync_phase1.sh`，
  阶段 2 起扩展为同时校验 RDMA 进程链路）：10w 数据 → Replica 全量 → 逐条比对。

阶段 1 的临时实现（阶段 2/3 替换）：

- **文件获取**：单机验证时 Master/Replica 共用 `data/` 目录，Replica 等文件出现后
  直接加载（`sync_acquire_tmp_file()`）。阶段 2 换成「派生 rdma-server 等文件落地」。
- **未做 eBPF 强制**：`SYNC` 暂不校验 `g_ebpf_armed`，也没有往有序采集流写 SYNC
  记录；阶段 3 接入 EBPF 握手后补齐（设计 §7.1）。
- **未做 sync worker**：`SYNC` 的序列化与落盘仍在存储锁内完成，快照较大时会占住
  锁一小段时间；按设计 §8.1 在阶段 5 挪到独立 worker 线程。

### 12.2 阶段 2 实现记录

已落地内容：

- 新增 `include/kvs_rdma_transfer.h` + `replication/kvs_rdma_transfer.cpp`：两个工具
  进程共用的 RDMA 搬运实现（控制消息、CM 事件、QP、recv/send/RDMA_WRITE 原语，
  以及「发送文件」「接收并落盘」两个入口）。
- 新增可执行文件 `tools/rdma_client.cpp`（产物 `build/rdma-client`）与
  `tools/rdma_server.cpp`（产物 `build/rdma-server`），由 CMake 在找到
  libibverbs/librdmacm 时构建。
- `replication/kvs_sync.cpp`：
  - Master：`SYNC` 写完 `.tmp` 后用 `posix_spawn` 派生 `rdma-client`（参数
    `--file/--peer/--port/--timeout-ms`）；`SYNC FINISHED` 时回收该进程并删 `.tmp`。
  - Replica：先派生 `rdma-server`（`--listen-port/--out/--ready-file/--timeout-ms`）
    并等「就绪文件」出现，再发 `SYNC`；等子进程退出（= 文件完整落盘）后加载
    会话专用 `.recv` 文件，最后发 `SYNC FINISHED <session_id>`。
- 配置新增：`rdma_client_bin`、`rdma_server_bin`、`rdma_port`、`rdma_advertise_host`、
  `rdma_transfer_timeout_ms`、`sync_wait_timeout_ms`。
- 子进程 stdout/stderr 重定向到会话/接收端专用日志，
  与 kvstore 自身日志分开，便于定位失败步骤。

阶段 2 的关键改动与注意点：

- **端口冲突**：旧实现的 Master 进程内 RDMA 监听占用「TCP 端口 + 1」，与新
  `rdma_port`（默认 10000）冲突，已停止在 `main` 中启动旧监听；旧监听代码在
  阶段 4 删除。
- **路径区分**：Master 与 Replica 使用会话专用路径，避免单机多从机验证时互相覆盖。
- **RDMA endpoint 动态上报**：Replica 在 `SYNC <rdma_ip> <rdma_port>` 中上报本次
  可访问地址；Master 不再要求静态 `replica_host`。多网卡环境可用
  `rdma_advertise_host` 覆盖，且该地址不能是回环地址。

### 12.3 阶段 3 实现记录

已落地内容：

- 新增 `include/kvs_ebpf_ring.h` + `replication/kvs_ebpf_ring.cpp`：master 生产、
  agent 消费的共享内存采集环，记录布局与 BPF ringbuf 完全一致，
  `kvs_ebpf_event_parse` / `kvs_ebpf_event_fits` 也一并移到这里给两侧共用。
- 新增 `include/kvs_ebpf_agent.h` + `replication/kvs_ebpf_agent.cpp`：agent 侧采集，
  支持 `uprobe`（建 ringbuf、从 **master 二进制** 解析 `kvs_ebpf_notify_write`
  符号偏移并挂探针）与 `sim`（读共享内存环）两种运行期模式。
- 新增可执行文件 `tools/ebpf_agent.cpp`（产物 `build/ebpf-agent`）：注册到 master、
  维护每目标 `AWAITING_SYNC → CACHING → LIVE` 状态，并监听各自的 `REPL_INC 2` 通道。
- `kvstore.cpp`：Master 启动时初始化采集通道；命令表外的 `SYNC` / `SYNC FINISHED`
  处理会往同一条有序流写控制记录。
- 采集挂载点与 sim 生产者最终并入 `replication/kvs_replication.cpp`
  （原 `kvs_ebpf.cpp`，同属 kvstore 侧写路径胶水代码）。
- `replication/kvs_replication.cpp`：`kvs_replication_append()` 改为只调用挂载点；
  网络后端握手入口只识别 Master 侧的 `EBPF 2` 注册。
- Replica 的增量通道由同步线程主动外连并独立收帧，不经过三个网络后端。
- 配置新增：`ebpf_capture_mode`、`ebpf_ring_file`。
- 验证脚本 `testcase/run_sync_increment.sh`。

阶段 3 踩到的两个关键点（都已修）：

- **采集环不能从进程启动就产出**：agent 注册之前写满有界环会丢事件并制造
  seq 缺口。现在只有 agent 注册（armed）后才产出记录，注册时重置环。
- **增量回放不能回响应**：agent 只写不读，逐条回 0 长度帧会把从机发送缓冲区
  堆满、拖住事件循环并丢掉尾部增量。现在回放路径跳过响应，并跳过每命令 1MB
  的响应缓冲分配（旧流程会为每条命令 malloc 一次，回放量大时会把协程饿死）。

### 12.4 阶段 4 实现记录

删除的旧路径：

- `replication/kvs_rdma.cpp`（进程内 RDMA 监听/全量下发）整文件删除；
  `KVSTORE_RDMA_FOUND` 现在只影响两个 RDMA 工具，`kvstore` 不再链接
  `ibverbs` / `rdmacm`（`ldd build/kvstore` 已无相关依赖）。
- `replication/kvs_replication.cpp` 从 1400+ 行收缩为薄接口：backlog、副本槽位
  `g_slots`、`repl_sender_thread`、`REPLICA` 握手、`build_tcp_full_sync_prefix`、
  `start_full_sync_locked`、RDMA note、Replica 监督线程全部移除。
- `include/kvs_replication.h` 同步收缩，只留 6 个接口。

保留的接口与原因：

| 接口 | 保留原因 |
| --- | --- |
| `kvs_replication_append` | 写路径入口，改为只把命令交给有序采集流 |
| `kvs_replication_accept_handshake` | 网络层识别 Master 侧的 EBPF 注册，三个后端共用同一入口 |
| `kvs_replication_remove_replica` | 断连清理，转发到 `kvs_sync_forget_fd` |
| `kvs_replication_finish_handshake` | 维持网络层调用点（新架构下无额外动作） |
| `kvs_replication_is_replaying` / `_set_replaying` | 回放线程标记，决定写分支是否记 AOF |
| `kvs_replication_resync` | RDB/AOF LOAD 之后的重同步入口 |

**重同步链路（新实现）**：旧实现由 Master 主动推快照；新架构下 Master 没有直达
Replica 的通道，因此 `kvs_replication_resync()` 改为往有序采集流写一条
`INVALIDATE` 记录：

```
RDB LOAD / AOF LOAD -> kvs_replication_resync() -> INVALIDATE 记录
  -> ebpf-agent 读到 -> 断开增量通道
  -> Replica 检测到通道断开 -> 重新走 SYNC 全流程
```

另外修正了一处：agent 原先只在「有命令要发」时才建立增量通道，空缓存时
Replica 会一直停在等待通道的状态；现在 `SYNC FINISHED` 会主动建连。

新增验证脚本 `testcase/run_backends_check.sh`：对 reactor / ntyco / proactor
三个后端各起一次服务，按「4 字节长度头 + RESP」验证四引擎命令与异常响应。

### 12.5 阶段 5 实现记录

阶段 5 修掉的两个真实缺口：

1. **缓存溢出必须强制重同步**：溢出会标记本轮失效（`g_round_invalid`），
   `SYNC FINISHED` 时关闭本轮反向通道并放弃缓存，Replica 观察到断开后重新全量同步。
2. **反向通道连接失败或断开必须重同步**：Replica 在发送 `SYNC FINISHED` 前主动连接
   Agent；连接、握手或接收线程失败都会结束本轮并重新全量同步，不再无限等待入站通道。

同时补齐：

- **Master 重启**：agent 重新注册成功后重建采集通道——sim 模式重开采集环
  （`kvs_ebpf_agent_capture_reopen`，停消费线程→重开→重启）；uprobe 模式的探针
  绑在旧进程上无法自动重挂，agent 打印明确指引并以退出码 3 退出，交给外部
  守护进程用新的 `--pid` 重启（不会出现「已注册但采不到」的假健康状态）。
- **子进程回收**：rdma-server 异常退出时已经 `waitpid` 收尸，清理路径不再对
  已回收的 pid 再做一次 kill（省掉 2s 空转与误导性的 SIGKILL 日志）。

新增验证脚本 `testcase/run_sync_faults.sh`，覆盖三条可复现异常：

| 场景 | 注入方式 | 观察到的恢复 |
| --- | --- | --- |
| A 未武装拒绝 | 先起 Replica、后起 agent | Replica 记录 `master rejected SYNC`；agent 起来后完成全量同步 |
| B 缓存溢出 | `--cache-max-records 1` + 同步窗口内持续写入 | agent 记录 `cache overflow` 并放弃本轮；Replica 超时后 `re-running full sync`；最终一致 |
| C rdma-client 失败 | 包装脚本让第一次传输直接失败 | Replica 首次 `sync attempt failed`，重试后 `full sync completed`；最终一致 |

---

## 13. 测试计划

- **全量 + 增量**：`testcase/master-replica/{insert_before5w,insert_after5w,get10w}`，
  即拓扑图中的 5w + 5w 场景；由 ebpf-agent 独立进程承担增量转发。
- **同步期并发写**：全量搬运过程中持续写入，验证 ebpf 缓存窗口与冲刷顺序。
- **SIM 端到端**：`ebpf_capture_mode sim` + 共享内存环（`testcase/run_sync_increment.sh`、
  `run_sync_faults.sh`），在无 `CAP_BPF` 的容器中验证 Master → ebpf-agent → Replica 全链路。
- **RDMA 往返**：`setup_rdma.sh` + `rdma-client` / `rdma-server` 单独联调，
  校验传输文件与源文件一致。
- **异常注入**：ebpf-agent 中途退出、rdma-client 失败、增量连接断开，
  验证 §9 的恢复行为。
- **协议回归**：三种网络后端下，普通客户端命令与复制帧互不干扰。

---

## 14. 风险与后续增强

**风险**

- 独立进程采集依赖 ELF 符号解析，Master 二进制 strip 或函数被内联会破坏 attach；
  需保证 `kvs_ebpf_notify_write` 保持 noinline/used，并在 build 流程中避免 strip。
- eBPF 强制（D3）下，采集进程不可用即无法建立同步；这是刻意取舍，需在运维文档中
  明确「先启动 ebpf-agent 再启动 Replica」。
- 多从机上限默认 16；目标越多，`ebpf-agent` 的缓存和发送缓冲总内存越高，需要结合
  `memory_budget_bytes` 设定容量。
- **`uprobe` 模式未做进程级端到端验证**：本次环境的 `ebpf-agent` 拿不到
  file capability（`setcap` 需要 CAP_SETFCAP），验证全部走 sim 模式；
  `uprobe` 路径已实现并编译通过，建议在有 root 的环境复验一次。
- **大流量增量受既有引擎性能限制**：`kvs_array_set` 是线性扫描（插入 O(n²)），
  隔离测试里 5w 条 `SET` 回放约 15s，而 `RSET` 只要约 1.6s。这是存储引擎既有
  特性，不是复制链路引入；增量回放的收敛等待窗口已按此放宽。
- **子进程健康检查是「被动」的**：Master 只在 `SYNC FINISHED` 时回收
  rdma-client，若它中途失败，靠 Replica 的文件等待失败/超时触发重试，不是
  Master 主动发现并中止。

**可选增强（非本期）**

- 严格零重复：共享水位暴露 `sync_base_seq`，ebpf-agent 只转发 `seq > base_seq`。
- ebpf-agent 由 Master 托管启动与自动重启。
- 更大规模的分片发送或外部增量队列。

---

## 15. 与旧实现的差异速查

| 维度 | 旧实现 | 新实现 |
| --- | --- | --- |
| 增量采集 | kvstore 进程内 uprobe + backlog | 独立 `ebpf-agent` 进程 |
| 增量投递 | kvstore 发送线程 TCP 投递 | Replica → `ebpf-agent` 反向专用握手连接 |
| 全量传输 | TCP 分片帧（可选进程内 RDMA） | `.tmp` 文件 + 独立 `rdma-client`/`rdma-server` |
| 全量对齐 | 锁内取快照 + `base_seq` | 锁内 notify(SYNC) + 有序采集流缓存窗口 |
| 副本规模 | 最多 16 个进程内槽位 | 最多 16 台，Master 多会话 + Agent 多目标扇出 |
| eBPF 定位 | 可选优化，可回退直写 backlog | 强制依赖，失效即同步不可用 |
| 握手 | `REPLICA` | `EBPF 2` + `REPL_INC 2 <session_id>` |
