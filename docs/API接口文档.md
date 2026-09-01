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
| GET | `/sync/compare/drilldown/sse` | 四级下钻 SSE（日→小时→5分钟→10秒窗），定位异常时间窗 |
| POST / GET | `/sync/backfill` | 补全（历史回填）：**范围补全**，10 分钟初始窗口 + 命中自适应分裂（10→5→2→1 分钟） |
| POST | `/sync/backfill/windows` | 补全（历史回填）：**窗口补全**，按窗口列表直接回填（可多个、可不连续） |
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

> 计数语义：**范围补全**按"单位窗口"换算（单位窗口 = 增量间隔 `sync.interval`，默认 10 秒）：`totalWindows` 为范围覆盖的单位窗口总数，`completed/failed` 按每个完成窗口覆盖的单位窗口数累加（10 分钟=60、5 分钟=30、2 分钟=12、1 分钟=6），`percent` 反映**时间覆盖比例**；**窗口补全**为字面窗口计数。

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
    { "at": 1756281600000, "atStr": "...", "heapAllocMB": 42.1, "heapSysMB": 80.0, "gcPerSec": 0.5, "numGoroutine": 18, "avgWindowMs": 812.3 }
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
| `runtimeSeries` | []RuntimePoint | 每秒运行时采样（堆内存/协程/GC 次/秒/窗口均耗时），最多 600 点（约最近 10 分钟） |

**progress 明细（即弹框「本次进度」）**

| 字段 | 类型 | 说明 |
|---|---|---|
| `totalWindows` | int | 总窗口数（范围补全 = 单位窗口总数；窗口补全 = 字面窗口数） |
| `completed` | int | 成功数（范围补全按单位窗口累加，见上方计数语义） |
| `failed` | int | 失败数（同上） |
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

## 4. GET /sync/compare/drilldown/sse 四级下钻（定位异常时间窗）

对指定时间范围做 **日 → 小时 → 5分钟 → 10秒窗** 四级金字塔下钻，定位 ES 与 ADB 条数不一致的异常时间窗。每级只对上一级的**异常父窗口**继续细分（正常窗口直接剪枝）；5 分钟块 = 300s = 30 个 10 秒窗，最细仍为 10 秒。

**分析过程保持轻量**：每个窗口算完只推送一条进度计数 `progress {level,done,total}`（不含窗口明细），前端据此把进度条换算为 4 段各 25% 的精确进度，SSE 传输压力小；全部分析完成后由 `done` **一次性**下发全量异常窗口。

**请求**（GET，query 参数）

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `start` | string | 否 | 范围开始（如 `2026-08-01 00:00:00`，也支持毫秒）；缺省回退上一个整点小时 |
| `end` | string | 否 | 范围结束（缺省按当前 lag 边界推） |
| `workers` | int | 否 | 并行线程，默认 12，按窗口数自动收敛 |
| `l1`/`l2`/`l3`/`l4` | int | 否 | 各级粒度（秒），默认 `86400 / 3600 / 300 / 10`；传 `0` 表示该层不下钻 |

SSE 事件：`range` → 每窗口 `progress` → `done`（或 `error`）。

**range**

```json
{ "startMs": 1756281600000, "endMs": 1785369600000, "start": "...", "end": "..." }
```

**progress**（每个窗口算完即推一次，只带计数，用于驱动进度条）

```json
{ "level": 1, "done": 5, "total": 28 }
```

`level` 为当前级（1=日 / 2=小时 / 3=5分钟 / 4=10秒），`done/total` 为该级的处理进度。前端可换算总进度 `= ((level-1) + done/total) / 4 × 100%`。

**done**（分析完成，一次性下发全量异常窗口，不区分层级）

```json
{
  "abnormal": 206,
  "windows": [
    { "level": 1, "s": 1756281600000, "e": 1756368000000, "start": "2026-08-27 00:00:00", "end": "2026-08-28 00:00:00", "diff": 12 },
    { "level": 4, "s": 1756281610000, "e": 1756281620000, "start": "2026-08-27 00:00:10", "end": "2026-08-27 00:00:20", "diff": 1 }
  ]
}
```

- `abnormal`：最末一级（level4）的异常窗口数。
- `windows`：全部各层级的异常窗口（`s/e` 为毫秒，`start/end` 为格式化字符串，`diff` 为 ES−ADB 差值，`level` 为该窗所属层级），按层排序。`diff=0` 为正常，`diff≠0` 为异常；异常窗口（`diff≠0`）才会上抛，正常窗口已剪枝。

> 异常口径：窗口内 ES 命中数 ≠ ADB 已写入（`es_timestamp`）数量，不区分多少方向。
>
> **与补全衔接**：将 `done.windows` 中同层/跨层的异常窗口 `s/e`（毫秒）组装成 `[{startMs:s, endMs:e}, ...]`，POST `/sync/backfill/windows` 即可只回填这些异常窗口（见第 5 章）。

---

## 5. 补全同步（回填历史数据，并行）

4 级下钻定位出异常窗口后，回填缺失/不一致的历史数据。按入参形式分两个独立接口，职责分明、不混用参数：

| 接口 | 方式 | 适用场景 |
|---|---|---|
| `POST / GET /sync/backfill` | **范围补全** | 连续范围，10 分钟初始窗口 + 命中自适应分裂（10→5→2→1 分钟）回填（整体重同步 / 补全天） |
| `POST /sync/backfill/windows` | **窗口补全** | 按窗口列表直接回填，可多个、可不连续（补下钻定位的异常窗） |

### 5.1 范围补全  POST/GET /sync/backfill

入参 `{ start, end }`（`end` 可缺省），`body` 与 query 参数二选一。

**请求**

```json
{ "start": "2026-08-01 00:00:00", "end": "2026-08-29 00:00:00" }
```

**参数**

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `start` | string | 是 | 起始时间，支持毫秒时间戳 或 `"2006-01-02 15:04:05"` 等 |
| `end` | string | 否 | 结束时间；留空则截止到当前 lag 边界 |

**响应 data**（含 `plan` 切片明细 + `summary` 汇总）

```json
{
  "plan": {
    "hasEnd": false, "intervalMs": 600000, "lagMs": 60000,
    "rangeStart": { "ms": 0, "time": "2026-08-01 00:00:00.000" },
    "rangeEnd":   { "ms": 0, "time": "2026-08-29 00:00:00.000" },
    "firstWindow": { "TimeRangeMs" },
    "lastWindow":  { "TimeRangeMs" },
    "windows": [ "TimeRangeMs..." ],
    "totalWindows": 4032
  },
  "summary": {
    "totalWindows": 4032, "workers": 2,
    "totalHits": 123456, "totalWritten": 123450, "failed": 0,
    "windows": [ { "window": {...}, "hits": 12, "written": 12, "durationMs": 85, "error": "" } ]
  }
}
```

> `plan.intervalMs` 固定为 `600000`（10 分钟初始窗口，`store.BackfillBaseIntervalSec`）；`plan.totalWindows` 与 `summary.totalWindows` 均为**初始 10 分钟窗口数**（自适应分裂不改变该值）；`summary.windows` 仅含失败窗口明细。

### 5.2 窗口补全  POST /sync/backfill/windows

入参 `{ windows:[{startMs,endMs}] }`，**直接**按给定窗口回填，不做切窗。仅支持 POST，不支持 query。

**请求**

```json
{ "windows": [ { "startMs": 1782720000000, "endMs": 1782720030000 }, { "startMs": 1782723600000, "endMs": 1782723630000 } ] }
```

**参数**

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `windows` | array | 是 | 窗口数组 `[{startMs,endMs}]`，后端去重/校验后排序回填；单次 ≤ 4000，`endMs` 需大于 `startMs` |

**响应 data**（无 `plan`，多返回实际补全窗口数 `windows`）

```json
{
  "windows": 2,
  "summary": { "totalWindows": 2, "workers": 2, "totalHits": 200, "totalWritten": 199, "failed": 0 }
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