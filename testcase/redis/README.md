# Redis 与 kvstore HSET 性能测试

本目录脚本使用临时实例和临时数据目录，不会覆盖仓库中的 `data/`。

## 构建

```bash
cmake -S . -B build
cmake --build build --target kvstore aof_insert10w batchsend_hash -j"$(nproc)"
```

## 批量 HSET QPS

```bash
# 两边都固定发送 100,000 条，只改变批大小
bash testcase/redis/run_batch.sh redis 160
bash testcase/redis/run_batch.sh kvstore 160

# 依次测试所有支持的批大小
for batch in 10 20 40 80 160; do
    bash testcase/redis/run_batch.sh redis "$batch"
    bash testcase/redis/run_batch.sh kvstore "$batch"
done
```

每次运行固定发送 100,000 条命令，批次数量由 `100000 / batch-size` 自动计算。
两边都使用单连接；Redis 使用 `-P <batch-size>`，kvstore 使用同一连接连续发送相同大小的批次。
Redis 持久化关闭，kvstore 启动参数固定为 `--rdb off --aof off`。
原始结果、汇总 CSV 和日志默认保存在 `/tmp/kvstore-redis-bench-results`。

## 100,000 条 HSET，AOF 开与关

```bash
bash testcase/redis/run_hash_10w.sh kvstore off
bash testcase/redis/run_hash_10w.sh kvstore on
bash testcase/redis/run_hash_10w.sh redis off
bash testcase/redis/run_hash_10w.sh redis on
```

kvstore 使用 `build/testcase/AOF/insert10w`，启动参数为 `--rdb off --aof <mode>`。
Redis 使用 `redis-benchmark -n 100000 -c 1 -P 1` 执行 HSET。AOF 开启时使用
`appendfsync everysec`，AOF 关闭时同时关闭 RDB。

汇总结果写入：

```text
/tmp/kvstore-redis-bench-results/hash_10w_summary.csv
```

可通过环境变量覆盖端口和结果目录：

```bash
PORT=17379 RESULT_DIR=/tmp/my-results \
    bash testcase/redis/run_hash_10w.sh redis on
```

## 注意事项

- Redis 的 `__rand_int__` 会产生随机字段，100,000 次 HSET 可能出现重复，因此 `HLEN`
  不一定等于 100,000；QPS 仍是执行 100,000 条命令的吞吐量。
- 当前 kvstore AOF 按 `persistence/aof.cpp` 中的字节阈值同步 flush+fsync；Redis
  `appendfsync everysec` 按时间在后台 fsync，两者持久化策略不完全相同。
- `batchsend_hash` 固定发送 100,000 条命令，批次数由批大小自动计算。
