# vipdoc K 线导入 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把可配置的 vipdoc 日线、5 分钟、1 分钟解析成按年/按日分区的 Parquet，并可用源文件对账。

**Architecture:** `classify` 留下股票和指数，`parse` 按 32 字节流式解码，`normalize` 丢掉占位和空价。第一次导入把每个源文件写成 `_staging` 再折成分区；之后用 manifest 的文件大小和末尾 32 字节区分跳过、只读尾部、整文件重解析。分区用临时文件替换，manifest 最后一次事务提交。

**Tech Stack:** Python 3.11+、pyarrow、sqlite3、pytest

## Global Constraints

- 只处理 `sh`/`sz`/`bj` 的 `lday`、`fzline`、`minline`
- 价格不复权；股票成交量是股，指数是手，数值不换算
- 日线分区 `bars/freq=1d/year=YYYY/part-0.parquet`
- 分钟线分区 `bars/freq=1m|5m/date=YYYY-MM-DD/part-0.parquet`
- `--vipdoc` 和 `--out` 每次传入
- 文件长度不是 32 的倍数时不推进该文件的水位
- 大小和末尾 32 字节都没变则跳过，即使中间某根 K 线被改过
- 日常测试不读真实 vipdoc

---

### Task 1: 代码规则和字节解析

**Files:** `src/tdx_data/classify.py`、`parse.py`、`normalize.py`，`tests/test_classify.py`、`test_parse_day.py`、`test_parse_minute.py`

- [x] 按 spec 的前缀表实现 `classify`
- [x] 日线 `<IIIIIfII`，分钟线 `<HHfffffII`，流式读取，丢掉半条
- [x] `.lc1` 在分钟 ≥ 898 且量额为 0 时丢弃；`.lc5` 保留
- [x] `pytest tests/test_classify.py tests/test_parse_day.py tests/test_parse_minute.py -q`

### Task 2: 发现文件、分区写入、全量与增量

**Files:** `src/tdx_data/discover.py`、`store.py`、`ingest.py`、`cli.py`，`tests/test_ingest.py`

- [x] manifest 记录 relpath、freq、symbol、size、tail、last_datetime、valid_rows
- [x] 全量：按代码写入 `_staging`，按月/年折成最终分区后删除 staging，再提交水位
- [x] 增量：尾部追加只替换该代码从新 bar 起的行；尾部对不上则整文件替换该代码
- [x] `tdx-import ingest --vipdoc --out [--freq]`
- [x] `pytest tests/test_ingest.py -q`

### Task 3: 对账

**Files:** `src/tdx_data/verify.py`，`tests/test_verify.py`

- [x] 按 `(freq, symbol)` 比较行数、首尾时间和 SHA-256
- [x] 检查排序、分区日期、品种和成交量单位
- [x] `--symbol` 逐行比较；日线与 1 分钟成交量差额只打印
- [x] `pytest tests/test_verify.py -q` 然后 `pytest -q`
