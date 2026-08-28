# API 接口文档

> 服务默认监听 `:8080`（可配置 `server.addr`）。文档以本地调试验证为准。
> 统一响应包：`{ "code": 0, "message": "ok", "data": {...} }`，`code=0` 表示成功。

## 0. 路由总览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 监控可视化页面（`api/web/index.html`） |
| GET | `/health` | 健康检查 |
| GET | `/monitor/sse` | SSE 实时监控流 |
| POST / GET | `/sync/backfill` | 补全（历史回填）同步 |
| POST / GET | `/sync/compare` | ES 与 ADB 条数对比 |

---

## 1. GET /health 健康检查

**响应**

```json
{ "code": 0, "message": "ok", "data": { "status": "ok" } }
```

---

## 2. GET /monitor/sse 实时监控流

Server-Sent Events（`text/event-stream`），保持长连接，每 5 秒心跳推送一次 `pipeline`，并且任何监控事件都会实时推送。

**连接后立即收到的首包**（`event: history`）：

```json
{
  "retentionSec": 3600,
  "incremental": [ "IncrementalPoint..." ],
  "backfill":    [ "BackfillProgressPoint..." ],
  "backfillWindows": [ "BackfillWindowPoint..." ]
}
```

**事件类型**

| event | data 结构 | 说明 |
|---|---|---|
| `history` | `MonitorHistory` | 首包：最近 1 小时历史快照 |
| `pipeline` | `PipelineSnapshot` | 接线图实时状态（含心跳 5s/次） |
| `runtime` | `RuntimeStats` | 服务运行时状态（连接即推 + 随心跳每 5s） |
| `incremental` | `IncrementalPoint` | 单次增量完成 |
| `backfill` | `BackfillProgressPoint` | 补全进度更新（每 5 窗口/结束） |
| `backfill_window` | `BackfillWindowPoint` | 每个补全窗口完成 |

**IncrementalPoint**

```json
{
  "at": 1756281600000, "atStr": "2026-08-27 00:00:00.000",
  "window": { "startMs": 0, "endMs": 0, "start": "...", "end": "..." },
  "hits": 12, "written": 12, "durationMs": 85,
  "success": true, "error": ""
}
```

**PipelineSnapshot**

```json
{
  "at": 0, "atStr": "...",
  "esReady": true, "mysqlReady": true,
  "incrementalRunning": true,
  "intervalSec": 10, "lagSec": 60,
  "targetWindow": { "startMs": 0, "endMs": 0, "start": "...", "end": "..." },
  "lastIncremental": { "IncrementalPoint" },
  "backfillActive": false,
  "backfillProgress": { "BackfillProgressPoint" }
}
```

**RuntimeStats**（`event: runtime`）

```json
{
  "at": 0, "atStr": "...",
  "uptimeSec": 7623,
  "goroutines": 18,
  "numCPU": 8, "goMaxProcs": 8,
  "goVersion": "go1.22.4",
  "allocMB": 42.3, "totalAllocMB": 3580.1,
  "sysMB": 180.5, "heapSysMB": 120.2, "heapObjects": 4092,
  "numGC": 12, "pauseTotalSec": 0.31
}
```

字段含义：`uptimeSec` 进程运行秒数、`goroutines` 协程数、`numCPU/goMaxProcs` 逻辑 CPU/运行容量、`goVersion` Go 版本、`allocMB` 当前堆分配、`totalAllocMB` 累计分配（MB）、`sysMB` 进程从系统获取内存、`heapSysMB` 堆映射内存、`heapObjects` 堆对象数、`numGC` GC 次数、`pauseTotalSec` GC 累计暂停秒数。

**BackfillProgressPoint**

```json
{
  "at": 0, "atStr": "...",
  "totalWindows": 100, "completed": 60, "failed": 2,
  "totalHits": 4580, "totalWritten": 4560, "percent": 62.0,
  "rangeStart": "2026-08-27 00:00:00.000", "rangeEnd": "2026-08-27 01:00:00.000"
}
```

**BackfillWindowPoint**

```json
{
  "at": 0, "atStr": "...",
  "window": { "TimeRangeMs" }, "hits": 10, "written": 10,
  "success": true, "error": ""
}
```

---

## 3. POST/GET /sync/backfill 补全同步

按时间范围回填历史数据（并行）。`body` 与 query 参数二选一。

**请求**

```json
{ "start": "2026-08-27 00:00:00", "end": "2026-08-27 17:00:00" }
```

| 参数 | 必填 | 说明 |
|---|---|---|
| `start` | 是 | 起始时间，支持毫秒时间戳 或 `"2006-01-02 15:04:05"` 等 |
| `end` | 否 | 结束时间；留空则截止到当前 lag 边界 |

**响应 data**

```json
{
  "plan": {
    "hasEnd": false, "intervalMs": 10000, "lagMs": 60000,
    "rangeStart": { "ms": 0, "time": "2026-08-27 00:00:00.000" },
    "rangeEnd":   { "ms": 0, "time": "2026-08-27 15:59:40.000" },
    "firstWindow": { "TimeRangeMs" },
    "lastWindow":  { "TimeRangeMs" },
    "windows": [ "TimeRangeMs..." ],
    "totalWindows": 5758
  },
  "summary": {
    "totalWindows": 5758, "workers": 8,
    "totalHits": 123456, "totalWritten": 123450, "failed": 0,
    "windows": [ { "window": {...}, "hits": 12, "written": 12, "error": "" } ]
  }
}
```

---

## 4. POST/GET /sync/compare 数据对比

对比一个时间范围内 ES 与 ADB（ADB 由 `es_timestamp` 统计）的条数差异。

**请求**

```json
{ "start": "2026-08-27 09:50:00", "end": "2026-08-27 10:00:00" }
```

| 参数 | 说明 |
|---|---|
| `start`、`end` 均空 | 取增量默认窗口 |
| 仅 `start` | 取包含该时刻的单个对齐窗口 |
| `start` + `end` | 对齐后的完整区间 |

**响应 data**

```json
{
  "range": { "TimeRangeMs" },
  "es":    { "startMs": 0, "endMs": 0, "start": "...", "end": "...", "field": "@timestamp", "count": 1234 },
  "mysql": { "startMs": 0, "endMs": 0, "start": "...", "end": "...", "field": "es_timestamp", "count": 1230 },
  "diff": 4,
  "match": false
}
```

说明：`diff = es.count - mysql.count`；`match = diff == 0`。

---

## 5. 通用响应说明

- 成功：`code=0`
- 失败：`code` 为非 0 业务码，`message` 说明原因，HTTP 状态码为 400/404/405/500/503 等
- JSON 序列化不转义 `& < >`

## 6. 页面与 SSE 消费示例

前端页面 `GET /` 直接通过 `EventSource('/monitor/sse')` 订阅：

- `history` → 首屏回放最近 1 小时
- `pipeline` → 更新接线图与 KPI
- `incremental` → 追加增量折线图 / 最近增量表
- `backfill`、`backfill_window` → 更新补全进度卡与进度图