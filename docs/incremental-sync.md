# 增量同步说明

增量同步负责在服务运行期间，**周期性从 ES 拉取上一时间窗口的数据并写入 ADB**。无需人工触发，进程启动后自动运行。

---

## 触发条件

同时满足以下条件时，增量同步才会启动：

1. 配置文件或环境变量中 ES、MySQL/ADB 均已配置
2. ES Store、MySQL Store 初始化成功
3. 主进程调用 `StartIncremental`

若 ES 或 MySQL 未就绪，日志会提示：

```text
无配置或不完整，增量同步不会执行
```

---

## 运行节奏

| 阶段 | 行为 |
|------|------|
| 启动瞬间 | **立即**查询上一已结束窗口 |
| 之后 | 每 **`sync.interval` 秒**执行一次 |
| 不等准点 | 不等待 wall-clock 整点，到间隔即执行 |

示例（`interval = 10`，启动时间 `18:50:15`）：

```text
18:50:15  启动 → 查 [18:50:00, 18:50:10)
18:50:25            查 [18:50:10, 18:50:20)
18:50:35            查 [18:50:20, 18:50:30)
...
```

---

## 时间窗口算法

窗口按 **Unix 纪元对齐**划分：

```text
aligned = unix - (unix % interval)
```

上一已结束窗口（`PrevWindow`）：

```text
[end - interval, end)   其中 end = AlignFloor(当前时间, interval)
```

| 当前时间 | interval | 查询窗口 |
|----------|----------|----------|
| 18:50:15 | 10 | `[18:50:00, 18:50:10)` |
| 18:50:25 | 10 | `[18:50:10, 18:50:20)` |
| 18:50:15 | 60 | `[18:49:00, 18:50:00)` |

左闭右开：`[@timestamp >= start, @timestamp < end)`

---

## 单次执行流程

```text
1. 计算 PrevWindow(now)
2. 若与 lastWindow 相同 → 跳过
3. tryClaimWindow（防与补全重复）
4. ES SearchByRange(start, end, max_size)
5. 解析 content 字段 → EventLog
6. ADB REPLACE INTO 批量写入
7. releaseWindow（释放 seen 占用）
8. 更新 lastWindow
```

---

## 配置项

`config.yaml` 中相关配置：

```yaml
sync:
  interval: 10        # 窗口大小 & 执行间隔（秒），可改为 5
  max_size: 10000     # 单次 ES 查询最大条数（不做分页）

es:
  url: "http://..."
  index: "ysh-ysh-app-info*"
  method: "addEventLog"    # term 过滤
  fields: "content"        # 解析字段
  dateField: "@timestamp"  # 时间范围字段
```

环境变量（优先级高于 yaml）：

```bash
ESADB_SYNC_INTERVAL=5
ESADB_SYNC_MAX_SIZE=10000
ESADB_ES_URL=http://...
ESADB_MYSQL_HOST=...
```

---

## ES 查询条件

每次增量查询等价于：

```json
{
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        { "term": { "method.keyword": "addEventLog" } },
        { "range": { "@timestamp": { "gte": "start", "lt": "end" } } }
      ]
    }
  },
  "sort": [{ "@timestamp": { "order": "asc" } }]
}
```

应用层还会过滤 `content` 中包含 `用户事件记录===` 的文档。

---

## ADB 写入

- 使用 **`REPLACE INTO`**（按主键 `id` upsert）
- 每批最多 **200 行** 一条 SQL
- 表不存在时自动建表；缺列时 `ADD COLUMN`

---

## 日志

增量成功时，每窗一条：

```text
[INFO] [incremental] 写入 [18:50:00,18:50:10) fetched=309 written=309 rowsAffected=618
```

失败时：

```text
[ERROR] 增量同步失败 [18:50:00,18:50:10): ...
```

---

## 与补全的关系

| 场景 | 行为 |
|------|------|
| 增量与补全窗口重叠 | `seen` 去重，先 claim 的优先执行 |
| 增量成功后 | 释放 `seen`，不长期占用内存 |
| 补全进行中 | 增量**不会停止**，两者可并行 |
| 补全跳过已处理窗 | 计入 `skipped` |

建议：大规模补全时用 SSE 观察负载；必要时在低峰期操作。

---

## 监控

通过 SSE 观察增量状态：

```bash
curl -N http://localhost:8080/sync/status
```

关注字段：

- `service.incremental`：是否在跑
- `service.lastWindow`：上次成功窗口
- `service.interval`：当前间隔

详见 [sse-status.md](./sse-status.md)。

---

## 常见问题

### Q：改了 interval 要重启吗？

要。interval 在启动时读取，运行中不会热更新。

### Q：单窗超过 max_size 怎么办？

当前不做分页，超出部分**不会写入**。请增大 `max_size` 或减小 `interval`（如 5 秒）。

### Q：服务重启后数据会丢吗？

重启前的窗口若未被增量覆盖，需用 [recovery-backfill.md](./recovery-backfill.md) 中的补全接口恢复。

### Q：为什么不是每 interval/3 秒轮询？

已改为固定每 `interval` 秒查一次上一窗，避免高频空转。
