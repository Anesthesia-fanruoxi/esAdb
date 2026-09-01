# esAdb

Elasticsearch → AnalyticDB（ADB）日志数据同步服务。按固定时间窗口拉取 ES 数据，解析后幂等写入 ADB，并提供补全、对比、异常下钻与每小时自动修复能力。

```
ES(@timestamp 10s 窗口) ──增量──▶ 解析(content kv) ──REPLACE INTO──▶ ADB(app_event_log)
        │
        └─范围补全(10分钟初始窗+自适应分裂) / 窗口补全 / 对比+4级下钻 / 每小时自动对比修复
```

## 功能特性

| 能力 | 说明 |
|---|---|
| 增量同步 | 每 `sync.interval`（默认 10s）同步上一已结束窗口，滞后 `lag_seconds` 补偿 ES 写入延迟 |
| 范围补全 | `POST /sync/backfill`：10 分钟初始窗口，命中数达 10000 自动沿 10→5→2→1 分钟分裂 |
| 窗口补全 | `POST /sync/backfill/windows`：按异常窗口列表精确回填（配合下钻结果） |
| 条数对比 | `POST /sync/compare`：按 `es_timestamp` 对比 ES 与 ADB 条数差异 |
| 4 级下钻 | 日→小时→5分钟→10秒 金字塔剪枝定位异常窗口 |
| 自动修复 | 每小时整点后自动对比上一整点小时，差异则 2 层下钻（5分钟→10秒）并补全一次 |
| 监控页面 | 内置 Web 页面：增量/补全实时曲线、补全详情弹框（QPS/内存/GC/批次耗时）、异常分析 |

## 快速开始

```powershell
# 构建（Windows 本地）
go build -o es-adb.exe .

# Linux 交叉编译
.\scripts\build-linux.ps1

# 运行（配置默认读 config/config.yaml，可用 -c 指定）
.\es-adb.exe -c config\config.yaml -p 8080
```

启动后访问 `http://localhost:8080` 打开监控页面。也可用根目录 `Dockerfile` 构建镜像，配置挂载到 `/config/config.yaml`。

## 配置说明

完整示例见 [config/config.example.yaml](config/config.example.yaml)，优先级：环境变量（`ESADB_` 前缀）> yaml。

| 配置段 | 关键项 | 说明 |
|---|---|---|
| `server` | `addr` | 监听地址，默认 `:8080` |
| `log` | `level` | debug/info/warn/error/off，默认 off |
| `es` | `url/index/username/password` | ES 连接与索引（支持通配） |
| | `query_string` | 查询串，如 `method:addEventLog` |
| | `fields` / `dateField` / `strip` | 解析字段、时间字段、正文前缀标识 |
| `mysql` | `host/port/user/.../table` | ADB 连接与表名（表需手动创建，见下） |
| `sync` | `interval/lag_seconds` | 增量窗口与滞后补偿 |
| | `max_size/batch_size` | 单窗口最大拉取数 / 单条 REPLACE 行数 |
| | `backfill_workers/backfill_pause_ms` | 补全并行数与节流 |
| `auto_compare` | `enabled/delay_seconds/workers` | 每小时自动对比修复开关/延迟/下钻并行数 |

## ADB 表结构（手动建表）

表结构为数据契约，**手动创建**，代码按固定列写入。`id` 为主键（`REPLACE INTO` 幂等去重的根基），`es_timestamp` 为对比/下钻/补全的查询列（必须建索引）：

```sql
CREATE TABLE IF NOT EXISTS app_event_log (
    id               varchar(64)  NOT NULL,
    type             varchar(64),
    phone_md5        varchar(64),
    customer_id      varchar(64),
    event_id         varchar(64),
    event_name       varchar(256),
    channel_id       varchar(64),
    channel_name     varchar(256),
    client           varchar(64),
    path             varchar(512),
    ip               varchar(64),
    create_time      datetime,
    source           varchar(256),
    app_version_type varchar(64),
    extend           text,
    es_timestamp     bigint       NOT NULL,
    PRIMARY KEY (id),
    KEY idx_es_timestamp (es_timestamp)
) DISTRIBUTED BY HASH(id);
```

字段来源：ES 文档 `content`（`用户事件记录===id:xxx,phoneMd5:xxx,...,extend:{json}`），解析时 camelCase → snake_case；`es_timestamp` 取 ES `@timestamp`（Unix 毫秒），`create_time` 为业务时间。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 监控页面 |
| GET | `/health` | 健康检查 |
| GET | `/monitor/sse` | 实时监控 SSE（5s 心跳） |
| GET | `/monitor/backfill/sse` | 补全详情 SSE（1Hz） |
| GET | `/sync/compare/drilldown/sse` | 4 级下钻 SSE |
| POST/GET | `/sync/backfill` | 范围补全 |
| POST | `/sync/backfill/windows` | 窗口补全 |
| POST/GET | `/sync/compare` | 条数对比 |

出入参明细见 [docs/API接口文档.md](docs/API接口文档.md)。

## 文档索引

- [docs/API接口文档.md](docs/API接口文档.md) — 全部接口出入参
- [docs/增量逻辑.md](docs/增量逻辑.md) — 增量同步窗口计算与重试
- [docs/补全逻辑.md](docs/补全逻辑.md) — 范围/窗口补全、自适应分裂、自动修复链路
- [docs/数据转换逻辑.md](docs/数据转换逻辑.md) — content 解析与字段映射
- [docs/时间计算.md](docs/时间计算.md) — 窗口对齐/边界计算规则

## 注意事项

- **写入幂等**：`REPLACE INTO` 按主键 `id` 覆盖，增量与补全重复执行安全
- **ES 重复文档**：同业务 `id` 的重复文档（MQ 重放）会造成 ES 计数 > ADB 的口径差，属正常现象，补全无法消除
- **时钟基准**：窗口按本地时区对齐（准点 = unix - unix%interval），部署机时区需与业务一致
