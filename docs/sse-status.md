# SSE 状态推送说明

`GET /sync/status` 使用 **Server-Sent Events (SSE)** 持续推送服务运行状态。连接建立后立即推送一次，之后每 **5 秒**推送一次，直到客户端断开。

---

## 基本信息

| 项 | 值 |
|----|-----|
| 路径 | `GET /sync/status` |
| 协议 | HTTP/1.1，长连接 |
| Content-Type | `text/event-stream; charset=utf-8` |
| 事件名 | `status` |
| 推送间隔 | 5 秒 |
| 数据格式 | JSON（单行） |

---

## 快速试用

### curl

```bash
curl -N http://localhost:8080/sync/status
```

`-N` 禁用输出缓冲，可实时看到流式数据。

### 浏览器 JavaScript

```javascript
const es = new EventSource('http://localhost:8080/sync/status')

es.addEventListener('status', (e) => {
  const data = JSON.parse(e.data)
  console.log(data.timestamp, data.progress?.percent + '%', data.queue?.backlog)
})

es.onerror = () => console.warn('SSE 连接异常或已关闭')
```

---

## SSE 报文格式

每条消息形如：

```
event: status
data: {"timestamp":"2026-08-27 19:30:00","service":{...},"memory":{...},"queue":{...},"progress":{...}}

```

- `event: status`：固定事件名，客户端应用 `addEventListener('status', ...)` 接收
- `data:`：单行 JSON，不含换行
- 空行 `\n\n` 表示一条消息结束

---

## 响应字段说明

### 顶层

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp` | string | 快照时间，`2006-01-02 15:04:05` |
| `service` | object | 服务运行状态 |
| `memory` | object | 进程内存与占用 |
| `queue` | object | 补全生产者队列状态 |
| `progress` | object | 补全进度（无补全任务时计数为 0） |

### service — 服务状态

| 字段 | 类型 | 说明 |
|------|------|------|
| `enabled` | bool | ES 或 MySQL 至少一项就绪 |
| `ready` | bool | 配置完整且可同步 |
| `incremental` | bool | 增量同步是否在运行 |
| `historical` | bool | 历史补全是否在运行 |
| `startedAt` | string | 增量同步启动时间 |
| `interval` | int | 同步窗口间隔（秒），来自 `sync.interval` |
| `lastWindow` | string | 增量上次成功处理的窗口键 |
| `workers` | int | 补全 worker 数（= CPU 核数） |
| `esReady` | bool | ES 是否已初始化 |
| `mysqlReady` | bool | ADB/MySQL 是否已连接 |

### memory — 内存状态

| 字段 | 类型 | 说明 |
|------|------|------|
| `seenWindows` | int | 当前进行中窗口占用数（`seen` map） |
| `heapAllocMB` | float | Go 堆已分配（MB） |
| `heapSysMB` | float | Go 堆向系统申请（MB） |
| `heapInuseMB` | float | Go 堆使用中（MB） |
| `numGoroutine` | int | 当前 goroutine 数 |
| `numGC` | int | GC 累计次数 |
| `progressMapKB` | float | 内存 progress map 估算体积（KB） |

### queue — 生产者队列

仅在历史补全进行中时有意义（`active: true`）。

| 字段 | 类型 | 说明 |
|------|------|------|
| `active` | bool | 是否有活跃补全任务 |
| `capacity` | int | 队列缓冲容量（`workers × 2`） |
| `backlog` | int | 堆积数量 = `enqueued - dequeued` |
| `enqueued` | int | 累计入队窗口数 |
| `dequeued` | int | 累计出队窗口数 |
| `inFlight` | int | 正在处理中的窗口数（≈ `seenWindows`） |
| `producerDone` | bool | 生产者是否已结束入队 |

**backlog 解读：**

- `backlog` 较大：worker 消费慢于生产，ES/ADB 可能是瓶颈
- `backlog = 0` 且 `producerDone = false`：worker 跟得上生产
- `producerDone = true` 且 `backlog > 0`：生产结束，等待 worker 消化剩余

### progress — 补全进度

| 字段 | 类型 | 说明 |
|------|------|------|
| `total` | int | 时间范围内总窗口数 |
| `done` | int | 已成功 |
| `failed` | int | 重试后仍失败 |
| `skipped` | int | 去重跳过（增量已处理等） |
| `remaining` | int | 剩余 = `total - done - failed - skipped` |
| `percent` | string | 进度百分比，`(done+failed)/(total-skipped)` |
| `rangeStart` | string | 当前补全任务起始（有任务时） |
| `rangeEnd` | string | 当前补全任务结束 |
| `rangeWorkers` | int | 当前补全 worker 数 |
| `rangeInterval` | int | 当前补全使用的 interval |

---

## 完整示例

```json
{
  "timestamp": "2026-08-27 19:30:00",
  "service": {
    "enabled": true,
    "ready": true,
    "incremental": true,
    "historical": true,
    "startedAt": "2026-08-27 18:50:15",
    "interval": 10,
    "lastWindow": "1735294200_1735294210",
    "workers": 8,
    "esReady": true,
    "mysqlReady": true
  },
  "memory": {
    "seenWindows": 3,
    "heapAllocMB": 12.5,
    "heapSysMB": 28.0,
    "heapInuseMB": 15.2,
    "numGoroutine": 22,
    "numGC": 5,
    "progressMapKB": 1.2
  },
  "queue": {
    "active": true,
    "capacity": 16,
    "backlog": 8,
    "enqueued": 1200,
    "dequeued": 1192,
    "inFlight": 3,
    "producerDone": false
  },
  "progress": {
    "total": 3240,
    "done": 1192,
    "failed": 0,
    "skipped": 12,
    "remaining": 2036,
    "percent": "36.8",
    "rangeStart": "2026-08-27 00:00:00",
    "rangeEnd": "2026-08-27 18:50:00",
    "rangeWorkers": 8,
    "rangeInterval": 10
  }
}
```

---

## 实现说明

- 服务端在内存中维护 `progress` map，每次推送前调用 `refreshProgress()` 重建快照
- SSE 与日志中的「历史补全进度」均为 5 秒周期，互不依赖
- 客户端断开连接后，服务端 goroutine 自动退出，无额外资源泄漏

---

## 反向代理注意

Nginx 等代理需关闭缓冲，否则 SSE 可能攒批后才输出：

```nginx
location /sync/status {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_cache off;
    chunked_transfer_encoding off;
}
```

响应头已包含 `X-Accel-Buffering: no`，兼容 Nginx。

---

## 相关接口

| 接口 | 说明 |
|------|------|
| `GET /health` | 健康检查，返回 `{"status":"ok"}` |
| `POST /sync/range` | 触发历史补全，见 [recovery-backfill.md](./recovery-backfill.md) |
