# API 接口文档

> 服务默认监听 `:8080`（可配置 `server.addr`）。文档以本地调试验证为准。
> 统一响应包：`{ "code": 0, "message": "ok", "data": {...} }`，`code=0` 表示成功。

## 0. 路由总览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 监控可视化页面（`api/web/index.html`） |
| GET | `/health` | 健康检查 |
| GET | `/monitor/sse` | SSE 实时监控流（增量 / 补全侧栏 / 接线图） |
| GET | `/monitor/backfill/sse` | 补全详情弹框 SSE（约 1Hz，含 QPS / 运行时序列） |
| GET | `/sync/compare/drilldown/sse` | 三级下钻 SSE（小时→分钟→10秒窗），定位异常时间窗 |
| POST / GET | `/sync/backfill` | 补全（历史回填）：连续范围 或 按窗口列表 |
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

**BackfillProgressPoint**（`event: backfill` / `pipeline.backfillProgress`）

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

## 3. GET /monitor/backfill/sse 补全详情流

打开监控页补全详情弹框时连接。首包 `event: snapshot`，之后约 **1 秒** 推送 `event: detail`，两种事件的 `data` 均为 `BackfillDetail`（结构相同）。连接关闭（弹框关闭）即断开。

**示例返回（完整展开）**

```json
{
  "backfillActive": true,
  "progress": {
    "at": 1756285200000,
    "atStr": "2026-08-27 01:00:00.000",
    "totalWindows": 5758,
    "completed": 3600,
    "failed": 2,
    "totalHits": 45800,
    "totalWritten": 45680,
    "percent": 62.5,
    "rangeStart": "2026-08-27 00:00:00.000",
    "rangeEnd": "2026-08-27 15:59:40.000"
  },
  "session": {
    "startedAtMs": 1756281600000,
    "startedAtStr": "2026-08-27 00:00:00.000",
    "finishedAtMs": 0,
    "finishedAtStr": "",
    "firstWindow": { "startMs": 1756281600000, "endMs": 1756281610000, "start": "00:00:00", "end": "00:00:10" },
    "lastWindow":  { "startMs": 1756343980000, "endMs": 1756343990000, "start": "15:59:40", "end": "15:59:50" }
  },
  "qpsSeries": [
    { "at": 1756281600000, "atStr": "...", "writeQps": 120.0, "windowQps": 2.0, "hitQps": 125.0 },
    { "at": 1756281600500, "atStr": "...", "writeQps": 0.0,   "windowQps": 0.0,  "hitQps": 0.0 }
  ],
  "runtimeSeries": [
    { "at": 1756281600000, "atStr": "...", "heapAllocMB": 42.1, "heapSysMB": 80.0, "sysMB": 95.0, "numGoroutine": 18, "numGC": 12 }
  ]
}
```

**字段说明**

| 字段 | 类型 | 说明 |
|---|---|---|
| `backfillActive` | bool | 补全是否进行中 |
| `progress` | BackfillProgressPoint | 本次补全整体进度（见下表），空闲时为 `null` |
| `session` | BackfillSessionMeta | 本次补全会话元信息 |
| `qpsSeries` | []QpsPoint | 每秒写入/命中/窗口 QPS 采样，最多 600 点（约最近 10 分钟） |
| `runtimeSeries` | []RuntimePoint | 每秒运行时采样（堆内存/Sys/协程/GC），最多 600 点 |

**progress 明细（即弹框「本次进度」）**

| 字段 | 类型 | 说明 |
|---|---|---|
| `totalWindows` | int | 总窗口数 |
| `completed` | int | 成功窗口数 |
| `failed` | int | 失败窗口数 |
| `totalHits` | int | ES 命中总数 |
| `totalWritten` | int | ADB 写入总数 |
| `percent` | float | 完成百分比（0~100） |
| `rangeStart` / `rangeEnd` | string | 本次补全覆盖的时间范围 |

**session 明细**

| 字段 | 类型 | 说明 |
|---|---|---|
| `startedAtMs` / `startedAtStr` | int64 / string | 补全开始时间 |
| `finishedAtMs` / `finishedAtStr` | int64 / string | 补全结束时间（进行中时为空） |
| `firstWindow` / `lastWindow` | TimeRangeMs | 首个 / 最末对齐窗口 |

> `TimeRangeMs = { startMs, endMs, start, end }`，`start/end` 为 `HH:MM:SS`。`percent = (completed+failed)/totalWindows*100`；`done = completed + failed`。

---

## 4. GET /sync/compare/drilldown/sse 三级下钻（定位异常时间窗）

对指定时间范围做 **小时 → 5分钟 → 10秒窗** 三级下钻，定位 ES 与 ADB 条数不一致的异常时间窗。每级只对上一级的**异常父窗口**继续细分，多个异常父窗口的细分窗口合并到同一并行池中并行。每个子窗口算完**立即**以 `progress` 事件流式返回（前端实时填充状态格），每级结束再发 `levelN` 汇总异常窗口。常用于对比后仅差 1~2 条时，精准定位误差时间点。5 分钟块 = 300s = 30 个 10 秒窗，最细仍为 10 秒。

**请求**（GET，query 参数）

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `start` | string | 是 | 范围开始（如 `2026-08-27 00:00:00`，也支持毫秒） |
| `end` | string | 否 | 范围结束（缺省按当前 lag 边界推） |
| `workers` | int | 否 | 并行线程，默认 12，按窗口数自动收敛 |
| `l1` / `l2` / `l3` | int | 否 | 各级粒度（秒），默认 `3600 / 300 / 10` |

SSE 事件：`range` → 每窗口 `progress` → 每级完 `level1` / `level2` / `level3` → `done`（或 `error`）。

**range**

```json
{ "startMs": 1756281600000, "endMs": 1756342400000, "start": "...", "end": "..." }
```

**progress**（每个窗口算完即推一次，前端据此定位各级状态格）

```json
{
  "level": 1, "levelMs": 3600000,
  "parent": { "startMs": 1756281600000, "endMs": 1756285200000, "start": "...", "end": "..." },
  "window": { "startMs": 1756281600000, "endMs": 1756285200000, "start": "...", "end": "..." },
  "es": 3600, "adb": 3599, "diff": 1, "match": false
}
```

`level=1` 时 `parent` 为整体范围（小时格按 `window.startMs` 相对定位）；`level=2` 时 `parent` 为所在小时（5分钟块）；`level=3` 时 `parent` 为所在 5 分钟块（每块 30 个 10 秒格）。`diff=0` 为正常，`diff≠0` 为异常。

**level1 / level2 / level3**（每级完毕的汇总）

```json
{
  "level": 1, "levelMs": 3600000, "total": 24, "abnormal": 1,
  "windows": [
    { "range": { "startMs": 1756281600000, "endMs": 1756285200000, "start": "...", "end": "..." },
      "es": { "count": 3600 }, "mysql": { "count": 3599 }, "diff": 1, "match": false }
  ]
}
```

`windows` 仅包含该级异常窗口（`CompareResult`），按 start 升序；可用最末一级（level3）的 `range.startMs/endMs` 组装回补。

**done**：`{ "abnormal": N }`，`N` 为最末一级（level3）的异常窗口数。

> 异常口径：窗口内 ES 命中数 ≠ ADB 已写入（`es_timestamp`）数量，不区分多少方向。

**与补全衔接**：取 `progress(level=3)` 中 `match=false` 的窗口，或 `event.level3` 里 `windows[]` 的 `range.startMs/endMs`，组装成数组后 POST `/sync/backfill` 的 `windows` 字段即可只回补这些异常窗口（见第 5 章）。

---

## 5. POST/GET /sync/backfill 补全同步

按时间范围回填历史数据（并行）。支持两种入参（二选一）：

1. **连续范围**：`{ start, end }`（`end` 可缺省，内部按 `sync.interval` 切窗）
2. **按窗口列表**（不连续，用于补全三级下钻定位的异常窗口）：`{ windows:[{startMs,endMs}] }`

`body` 与 query 参数二选一。

**请求（按窗口列表补全）**

```json
{ "windows": [ { "startMs": 1756282000000, "endMs": 1756282010000 }, { "startMs": 1756282060000, "endMs": 1756282070000 } ] }
```

**请求（连续范围补全）**

```json
{ "start": "2026-08-27 00:00:00", "end": "2026-08-27 17:00:00" }
```

**连续范围模式参数**（此时请求体/query 含 `start`，不使用 `windows`）

| 参数 | 必填 | 说明 |
|---|---|---|
| `start` | 是 | 起始时间，支持毫秒时间戳 或 `"2006-01-02 15:04:05"` 等 |
| `end` | 否 | 结束时间；留空则截止到当前 lag 边界 |

**窗口列表模式参数**（此时请求体含 `windows`，不使用 `start/end`）

| 参数 | 必填 | 说明 |
|---|---|---|
| `windows` | 是 | 窗口数组 `[{startMs,endMs}]`，去重/排序后直接回补，单次 ≤ 4000 |

**响应 data（连续范围模式含 `plan`；窗口列表模式无 `plan`，多返回 `windows` 数量）**

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
    "totalWindows": 5758, "workers": 2,
    "totalHits": 123456, "totalWritten": 123450, "failed": 0,
    "windows": [ { "window": {...}, "hits": 12, "written": 12, "error": "" } ]
  }
}
```

---

## 6. POST/GET /sync/compare 数据对比

对比一个时间范围内 ES 与 ADB（`es_timestamp`）的条数差异。

**请求**

```json
{ "start": "2026-08-27 09:50:00", "end": "2026-08-27 10:00:00" }
```

| 参数 | 说明 |
|---|---|
| `start`、`end` 均空 | **上一个整点小时**（如 13:45 → `[12:00:00, 13:00:00)`） |
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

## 7. 通用响应说明

- 成功：`code=0`
- 失败：`code` 为非 0 业务码，`message` 说明原因，HTTP 状态码为 400/404/405/500/503 等
- JSON 序列化不转义 `& < >`

## 8. 页面与 SSE 消费示例

前端页面 `GET /`：

- `EventSource('/monitor/sse')` — 主页监控
- `EventSource('/monitor/backfill/sse')` — 补全详情弹框（打开时连接，关闭时断开）

主页 SSE：

- `history` → 首屏回放最近 1 小时
- `pipeline` → 更新接线图与 KPI
- `runtime` → 服务状态卡片
- `incremental` → 增量折线图 / 最近增量表
- `backfill` → 侧栏补全进度

详情 SSE：

- `snapshot` / `detail` → 弹框「本次进度 / 补全吞吐 / 服务信息」