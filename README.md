# 设备数据处理服务（事件时间水位线 + 分钟窗口指标）

持续读取 JSONL 设备事件，按**设备 + 事件发生分钟（UTC）**统计有效读数的数量、平均值
和最高值；完整支持乱序、重复、迟到事件，带事件时间水位线、窗口定稿、指标版本化和
崩溃恢复。仅依赖 Python 3.11+ 标准库与 SQLite。

## 快速开始

```bash
# 1. 一键端到端演示（确定性样例，可重复运行；默认结束后服务保持运行）
python3 -I run_demo.py
# 自动退出模式（CI/验证用）：
DEMO_EXIT=1 python3 -I run_demo.py

# 2. 单独启动服务：tail JSONL + 控制台页面
python3 -I run_server.py --db runtime/metrics.db --input runtime/events.jsonl --port 8000
# 浏览器打开 http://127.0.0.1:8000/

# 3. 测试
python3 -I run_tests.py
```

`-I`（隔离模式）用于处理不可信输入：忽略环境变量、用户站点包和当前目录，配合入口
脚本里的 `sys.path` 引导，即使数据目录中存在同名 `json.py` 也不会被误导入。

## 事件格式

每行一个 JSON 对象，必需字段：

| 字段 | 说明 |
| --- | --- |
| `event_id` | 事件唯一标识（去重/冲突判定的身份） |
| `device_id` | 设备 ID |
| `event_time` | 发生时间：ISO 8601（必须带时区，`Z` 或偏移量）或 epoch 秒；**无时区的 naive 时间一律拒绝** |
| `reading` | 读数：JSON 数字，布尔/字符串/NaN/Infinity 一律拒绝 |

所有时间内部统一转为 UTC，窗口按 `event_time` 的分钟取整，**与接收时间无关**。

## 处理语义（与需求逐条对应）

### 水位线

* 有效水位线 = `max(已接受有效事件最大 event_time − 30s, 显式推进覆盖值)`
* **先用当前水位线分类当前事件，再推进水位线**（见
  `processor.EventProcessor.process_event`：分类逻辑在
  `_advance_natural_watermark` 之前）。
* 无效事件、ID 冲突事件、迟到事件都不推动水位线。
* 水位线只增不减；显式推进（HTTP/CLI/页面按钮）同样是单调的，回退请求被拒绝。

### 窗口定稿

* 窗口 `[hh:mm:00, hh:mm+1:00)` 的定稿条件：**水位线 ≥ 窗口结束 + 60s**。
* 定稿在每次有效事件推进水位线、以及每次显式推进后检查，一次推进可同时定稿多个
  到期窗口（同事务完成）。
* 定稿后的窗口行不可变：定稿后到达的事件写入 `late_events` 隔离区，
  `count/avg/max/version` 绝不被回改。

### 去重与冲突

* 同一 `event_id` 且**规范化内容哈希相同**（JSON 键序无关）：只计算一次，
  重复送达只累加 `duplicate_count`。
* 同一 `event_id` 但内容不同：写入 `conflicts`，保留**第一次接受**的读数，
  指标和水位线都不变。
* 去重身份同时覆盖已接受事件与迟到隔离区事件：定稿后迟到事件的重复送达也不会
  在隔离区落两份。

### 迟到的两种含义

1. **定稿前、但 event_time 已落后于当前水位线**：按水位线语义归入 `late_events`，
   不修改任何聚合。
2. **定稿前且 event_time 仍领先水位线**（乱序但未过期的修正）：正常计入窗口，
   指标更新并产生新的版本号（`window_versions` 留有完整历史，可追溯到触发事件）。

### 持久化与恢复

所有状态都在一个 SQLite 库（WAL 模式）中，单行事件的处理与文件偏移提交在同一个
临界区/事务内：

* `files.read_offset`：每个输入文件的字节偏移，重启后精确续读，旧行零重放；
* `events`：去重记录；`windows` + `window_versions`：指标与版本；
* `late_events` / `conflicts` / `invalid_events`：三类旁路数据；
* `state`：最大事件时间与水位线覆盖值；`watermark_log`：水位线变更审计。

## HTTP 接口

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/` | 控制台页面（窗口状态、指标版本、迟到/冲突/无效数据、水位线审计，5 秒自动刷新） |
| GET | `/api/state` | 当前最大事件时间、覆盖值、有效水位线 |
| GET | `/api/windows` | 全部窗口指标（含定稿标记、版本） |
| GET | `/api/versions` | 指标版本历史 |
| GET | `/api/late` `/api/conflicts` `/api/invalid` `/api/events` | 各旁路数据 |
| GET | `/api/watermark-log` | 水位线变更审计 |
| POST | `/api/advance-watermark?to=<ISO8601或epoch>` | **显式推进水位线入口**，同事务定稿到期窗口 |
| GET | `/healthz` | 存活探针 |

CLI 等价入口：

```bash
python3 -I run_cli.py --db runtime/metrics.db show windows
python3 -I run_cli.py --db runtime/metrics.db advance-watermark --to 2026-11-11T10:03:00Z
```

## 演示脚本会展示什么

`run_demo.py` 使用固定 UTC 时间的三批确定性数据：

1. **批次 1**：乱序到达、跨分钟窗口、完全重复 → 验证按事件时间归组与去重；
2. **批次 2**：重复、同 ID 内容冲突、非法时间、非法读数、未过期的迟到修正
   → 验证冲突保留首值、无效事件不推动水位线、修正产生新版本；
3. **显式推进到 10:02:00**（= 10:00 窗口结束 + 60s）→ 观察 10:00 窗口定稿；
4. **批次 3**：推进型事件 + 定稿后迟到数据（含迟到事件重复送达）→ 验证隔离区
   与已定稿指标不可变；
5. **重启服务进程** → 验证偏移续读、去重与指标完整恢复；
6. **再次显式推进到 10:03:00** → 10:01 窗口定稿，而 10:02 窗口（截止 10:04:00）
   保持开放，体现定稿只作用于到期窗口。

## 目录结构

```
device_metrics/
  schema.py      # SQLite 表结构、连接（WAL）、全局访问锁
  models.py      # UTC 时间解析/校验、读数校验、规范化哈希、领域模型
  processor.py   # 核心引擎：水位线、去重/冲突、窗口聚合与版本、定稿、迟到隔离
  tailer.py      # JSONL 轮询读取，偏移持久化，崩溃续读
  server.py      # stdlib HTTP 服务与控制台页面
  cli.py         # serve / advance-watermark / show 子命令
samples/
  sample_data.py # 确定性乱序样例（三批）
tests/
  test_processor.py  # 16 个单元/集成测试
run_demo.py      # 端到端演示
run_server.py    # 服务入口（-I 路径引导）
run_cli.py       # CLI 入口
run_tests.py     # 测试入口
```

## 并发说明

SQLite 连接以 `check_same_thread=False` 在 tailer 线程与 HTTP 线程间共享，
所有数据库访问由 `schema.DB_LOCK`（可重入锁）串行化；WAL 保证提交的持久性，
HTTP 读请求在锁内拿到一致性快照。该设计面向单实例中小吞吐；更高吞吐可改为每
线程独立连接 + WAL，接口层无需变动。
