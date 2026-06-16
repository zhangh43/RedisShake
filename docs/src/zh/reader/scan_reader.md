---
outline: deep
---

# Scan Reader

::: tip
`scan_reader` 的性能与数据一致性均不如 [`sync_reader`](sync_reader.md)，应尽可能选择 `sync_reader`。
:::

## 原理介绍

Scan Reader 有 SCAN 和 KSN 两个阶段，SCAN 阶段是全量同步，KSN 阶段是增量同步。

### 全量数据

**SCAN 阶段**：默认开启，可通过 `scan` 配置关闭。`scan_reader` 通过 `SCAN` 命令遍历源端数据库中的所有 Key。默认情况下，它会使用 `DUMP` 获取 Key 对应的 Value，并通过 `RESTORE` 命令写入目标端，完成数据的全量同步。也可以通过 `read_mode = "command"` 切换到标准命令读取模式，此时 RedisShake 会根据 `TYPE` 结果使用 `GET`、`HSCAN`、`LRANGE`、`SSCAN`、`ZSCAN` 等命令读取源端数据，并改写为普通写命令发送到目标端。

1. Redis 的 `SCAN` 命令只保证 <u>SCAN 操作开始至结束期间均存在</u>的 Key 一定会被返回，但是新写入的 Key 有可能会被遗漏，期间删除的 Key 也可能已经被写入目的端。
2. SCAN 阶段期间 RedisShake 会通过 `SCAN 命令` 返回的游标来计算出当前同步进度。该进度有较大误差，仅供参考。对于非 Redis 数据库游标计算方式与 Redis 不同，所以可能会看到进度显示错误的情况，忽略即可。

### 增量数据

**KSN 阶段**：默认关闭，可通过 `ksn` 开启，可以解决 SCAN` 阶段期间遗漏 Key 问题。增量数据同步并不是在 SCAN 阶段结束后才开始，而是与其一同进行，并在 SCAN 阶段结束后持续进行，直到退出 RedisShake。

`ksn` 使用 [Redis keyspace notifications](https://redis.io/docs/manual/keyspace-notifications/)
能力来订阅 Key 的变化。具体来说，RedisShake 会使用 `psubscribe` 命令订阅 `__keyevent@*__:*`，当 Key 发生变化时，RedisShake 会收到发生修改的 Key，然后按 `read_mode` 配置读取源端数据并写入目标端。
1. Redis 在默认情况下不会开启 `notify-keyspace-events` 配置，需要手动开启，保证值中含有 `AE`。
2. 如果在 KSN 阶段出现源端将连接断开，考虑适当调高 `client-output-buffer-limit pubsub` 的值。[802](https://github.com/tair-opensource/RedisShake/issues/802)
3. `Redis keyspace notifications` 不会感知到 `FLUSHALL` 与 `FLUSHDB` 命令，因此在使用 `ksn` 参数时，需要确保源端数据库不会执行这两个命令。


### 性能影响

默认的 `dump` 模式在 SCAN 与 KSN 阶段均使用 DUMP 命令来获取数据，DUMP 命令是 CPU 密集命令，会对源端造成较高压力。需要小心使用，避免影响源端实例的可用性。`command` 模式不会调用 DUMP，但会改用更多标准命令读取数据；对于大集合，它通常会降低单次命令的 CPU 峰值，但会增加源端命令数量和网络往返次数。
* 对于 SCAN 阶段，可以通过调整 `count` 参数来减轻源端压力，建议从 1 开始，逐步增加。
* 对于 KSN 阶段，暂无参数可调整，需要根据源端写请求量来评估是否开启。

### `read_mode = "command"` 的限制

* 当前仅支持 `string`、`hash`、`list`、`set`、`zset` 五类标准数据类型。
* 遇到 `stream`、模块类型或其他特殊类型时，会报错退出；如需跳过，可配合 `skip_unknown_type` 使用。
* 该模式发送的是普通写命令，不使用 `RESTORE`，因此 `rdb_restore_command_behavior` 不生效。

性能影响参考数据：源端实例写 QPS 为 15 万左右时，源端 CPU 使用率为 47%，开启 RedisShake 后，源端 CPU 使用率为 91%。


## 配置

```toml
cluster = false            # set to true if source is a redis cluster
address = "127.0.0.1:6379" # when cluster is true, set address to one of the cluster node
username = ""              # keep empty if not using ACL
password = ""              # keep empty if no authentication is required
tls = false
read_mode = "dump"        # "dump" uses DUMP/RESTORE, "command" reads values via standard Redis commands
dbs = []                   # set you want to scan dbs such as [1,5,7], if you don't want to scan all
scan = true                # set to false if you don't want to scan keys
ksn = false                # set to true to enabled Redis keyspace notifications (KSN) subscription
count = 1                  # number of keys to scan per iteration
scan_high_queue_len = 500000    # pause SCAN when the internal dump queue reaches this high watermark
scan_low_queue_len = 0     # resume SCAN when the queue falls to or below this low watermark
drop_ksn_on_backpressure = false # drop newly received KSN updates while scan backpressure is active
sample_on_start = true     # 在开始同步前，对每个 DB 采样少量 key/value 并打印日志
sample_count_per_db = 3    # 每个 DB 打印的采样 key 数量
sample_value_max_len = 256 # 采样 value 预览的最大长度，超出会截断
```

* `cluster`：源端是否为集群
* `address`：源端地址, 当源端为集群时，`address` 为集群中的任意一个节点即可
* 鉴权：
    * 当源端使用 ACL 账号时，配置 `username` 和 `password`
    * 当源端使用传统账号时，仅配置 `password`
    * 当源端无鉴权时，不配置 `username` 和 `password`
* `tls`：源端是否开启 TLS/SSL，不需要配置证书因为 RedisShake 没有校验服务器证书
* `read_mode`：读取源端 Value 的方式。`dump` 为默认模式，使用 `DUMP`/`RESTORE`；`command` 模式不使用 `DUMP`，而是按 Key 类型使用标准 Redis 命令读取并改写为普通写命令。
* `dbs`：源端为非集群模式时，支持仅同步指定 DB 库。
* `scan`：是否开启 SCAN 阶段，设置为 false 时，RedisShake 会跳过全量同步阶段
* `ksn`：开启 `ksn` 参数后，RedisShake 会订阅源端的 Key 变化，实现增量同步
* `count`：全量同步时每次从源端拉取的 key 的个数，默认为 1，改为较大值可以显著提升同步效率，同时也会提升源端压力。
* `scan_high_queue_len`：仅对 SCAN 阶段生效。当内部待 DUMP 队列长度达到该高水位时，RedisShake 会暂停继续执行 SCAN。默认值为 `500000`。
* `scan_low_queue_len`：仅对 SCAN 阶段生效。当 SCAN 因高水位被暂停后，只有当队列长度回落到该低水位及以下时才会恢复执行。未设置时默认与高水位相同。
* `drop_ksn_on_backpressure`：仅在启用 SCAN 背压时生效。开启后，当 SCAN 背压触发时，新收到的 KSN 更新会被直接丢弃而不入队。该选项会牺牲增量完整性来限制队列增长，默认关闭。
* `sample_on_start`：仅用于观测。开启后，RedisShake 会在正式同步前按 DB 采样少量 key，并打印 key、type 与 value 预览到日志。默认开启。
* `sample_count_per_db`：每个 DB 要打印的采样 key 数量，默认 3。
* `sample_value_max_len`：value 预览的最大字符数，超过后会截断，默认 256。
