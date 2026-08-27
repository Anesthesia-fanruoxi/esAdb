# 历史补全（恢复）说明

历史补全用于将 **指定时间范围内** ES 中的事件数据同步到 ADB，适用于：

- 服务停机期间遗漏的数据
- 首次上线需要导入历史
- 增量同步失败后的补偿

---

## 接口

```http
POST /sync/range
Content-Type: application/json
```

### 请求体

```json
{
  "start": "2026-08-27 00:00:00",
  "end":   "2026-08-27 18:00:00"
}
```

| 字段 | 必填 | 格式 | 说明 |
|------|------|------|------|
| `start` | 是 | `2006-01-02 15:04:05` | 补全起始时间（本地时区） |
| `end` | 否 | 同上 | 补全结束时间；**省略时自动补到服务启动时刻的上一周期终点** |

也支持 query 参数：`POST /sync/range?start=2026-08-27%2000:00:00`

### 成功响应（立即返回，异步执行）

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "mode": "range",
    "start": "2026-08-27 00:00:00",
    "end": "2026-08-27 18:50:00",
    "interval": 10,
    "workers": 8,
    "totalWindows": 6780,
    "status": "accepted",
    "startedAt": "2026-08-27 18:50:15"
  }
}
```

| 字段 | 说明 |
|------|------|
| `status: accepted` | 任务已接受，**后台执行中** |
| `totalWindows` | 预估窗口总数 |
| `workers` | 并发数 = CPU 核数 |
| `end`（未传时） | `AlignFloor(服务启动时间, interval)` |

### 错误响应

| 场景 | message 示例 |
|------|----------------|
| 已有补全在进行 | `历史同步正在进行中` |
| ES/ADB 未就绪 | `ES 或 MySQL 未就绪` |
| 时间范围无效 | `时间范围无效 start=... end=...` |

---

## 执行模型

**不是**一次性把全部窗口载入内存，而是 **流式切窗 + 队列 + 多 worker 并发**。

```text
POST /sync/range
    ↓ 对齐 start/end，计算 totalWindows
    ↓ 立即返回 accepted
    ↓
生产者 goroutine ──→ 按 interval 切窗 ──→ 队列 (缓冲 workers×2)
                                              ↓
                         Worker × CPU核数 ──→ ES 查询 + ADB 写入
```

### 关键点

| 项 | 说明 |
|----|------|
| 切窗时机 | 任务开始时按 `[start,end)` **一次性确定**所有窗口边界 |
| 内存 | 队列中同时仅存约 `workers×2` 个窗口，不堆积百万级数组 |
| 并发 | `runtime.NumCPU()` 个 worker |
| 去重 | 与增量共享 `seen`，已 claim 的窗口计入 `skipped` |
| 完成后 | 释放 `seen` 占用，progress 计数清零 |

---

## 时间对齐规则

| 输入 | 处理 |
|------|------|
| `start` | `AlignFloor(start, interval)` 向下对齐 |
| `end`（未传） | `AlignFloor(服务启动时间, interval)` |
| `end`（已传） | `AlignCeil(end, interval)` 向上对齐 |

### 只传 start 的示例

服务 `18:50:15` 启动，`interval = 10`：

```text
end = AlignFloor(18:50:15) = 18:50:10
补全范围 [start, 18:50:10)
最后一窗示例：[18:50:00, 18:50:10)
```

服务 `18:50:15` 启动，`interval = 60`：

```text
end = 18:50:00
```

---

## 窗口示例

`start = 00:00:00`，`end = 00:00:35`，`interval = 10`：

```text
[00:00:00, 00:00:10)
[00:00:10, 00:00:20)
[00:00:20, 00:00:30)
[00:00:30, 00:00:35)   ← 最后一窗可能不足 interval
```

---

## 进度与日志

### 日志（每 5 秒）

```text
[INFO] 历史补全进度 total=3240 done=800 failed=0 skipped=12 remaining=2428 progress=25.0%
```

### 完成时

```text
[INFO] 历史补全完成 total=3240 done=3100 failed=5 skipped=135 remaining=0 progress=100.0%
[INFO] 历史同步汇总 inserted=950000 rowsAffected=950000
```

### SSE 实时监控

```bash
curl -N http://localhost:8080/sync/status
```

详见 [sse-status.md](./sse-status.md)。

---

## 失败重试

单窗失败时：

1. 释放 `seen` 占用
2. 按 `sync.max_retry` 重试，延迟指数退避（`retry_delay` ~ `retry_delay_max`）
3. 仍失败 → 计入 `failed`，继续下一窗

配置：

```yaml
sync:
  max_retry: 3
  retry_delay: 1
  retry_delay_max: 10
```

---

## 推荐使用方式

### 1. 日常：只跑增量

服务启动后自动增量，一般无需补全。

### 2. 停机恢复

```bash
# 补从停机前到本次启动之间的数据（省略 end）
curl -X POST http://localhost:8080/sync/range \
  -H "Content-Type: application/json" \
  -d '{"start":"2026-08-26 18:00:00"}'
```

### 3. 大区间分段补

补 1 年数据窗口量极大，建议 **按月或按周** 分段：

```bash
curl -X POST http://localhost:8080/sync/range \
  -H "Content-Type: application/json" \
  -d '{"start":"2026-01-01 00:00:00","end":"2026-01-31 23:59:59"}'
```

每段完成后再发下一段；**同一时刻只能有一个补全任务**。

### 4. 观察队列堆积

SSE 中 `queue.backlog` 持续偏高说明 worker 消费不及，可：

- 等待低峰再补
- 缩小单次补全范围
- 暂时调大 ES/ADB 资源

---

## 与增量的协作

```text
时间轴 ──────────────────────────────────────────────→

  [==== 补全范围 start ──────────── end ====]
                                      ↑ 启动时刻对齐 end

  增量：启动后立即查上一窗，之后每 interval 秒继续
```

- 重叠窗口：去重，不会双写（`REPLACE INTO` 本身也幂等）
- 补全**不会**暂停增量
- 补全完成后 `seen` 中对应窗口会释放，不影响后续增量

---

## 数据量估算

日增量 ≤ 100 万，9:00–18:00 业务时段，`interval = 10`：

| 指标 | 约值 |
|------|------|
| 每窗平均 | ~309 条 |
| 每天窗口数（9h） | 3,240 |
| 每窗峰值（预估上限） | ≤ 10,000（需 `max_size=10000`） |

`interval = 5` 时窗口数翻倍，单窗条数减半，ES/ADB 查询频率不变（补全并发由 worker 决定）。

---

## 注意事项

1. **异步**：接口返回 `accepted` 不代表完成，需通过日志或 SSE 确认
2. **单任务**：补全进行中再次调用会报错
3. **无分页**：单窗超过 `max_size` 会丢数据，请合理设置 `interval` 与 `max_size`
4. **时区**：时间字符串按 **服务器本地时区** 解析
5. **索引**：ES 使用 `method.keyword` + `@timestamp` range，确保 mapping 正确

---

## 相关文档

- [incremental-sync.md](./incremental-sync.md) — 增量同步机制
- [sse-status.md](./sse-status.md) — SSE 进度监控
