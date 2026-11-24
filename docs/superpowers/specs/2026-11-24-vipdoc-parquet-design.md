# vipdoc K 线导入 Parquet

把本机通达信 `vipdoc` 里的日线、5 分钟、1 分钟解析成 Parquet，作为量化行情底座。价格只存不复权原始值。复权因子以后单独建表，用 `symbol` + `trade_date` 连接，不在这一期处理。

## 范围

这一期只处理 K 线。

| 纳入 | 不纳入 |
|---|---|
| `lday/*.day` 日线 | `cw` 财务 |
| `fzline/*.lc5` 5 分钟 | `eday` Level-2 日统计 |
| `minline/*.lc1` 1 分钟 | ETF、基金、可转债、B 股、通达信板块指数 `880` |
| 上海、深圳、北交所的股票和交易所指数 | 查询服务；按代码再存一份分钟线 |

历史有多长就导多长。解析按 32 字节一条流式读取，不把整份 `.lc1` 载入内存。

## 命令

`vipdoc` 和输出目录每次运行传入，不写进代码。

```text
tdx-import ingest --vipdoc /path/to/vipdoc --out /path/to/bars [--freq 1d,5m,1m]
tdx-import verify --vipdoc /path/to/vipdoc --out /path/to/bars [--symbol 600000.SH,000001.SH]
```

`--vipdoc` 是 `vipdoc` 根目录。程序只进入 `sh`、`sz`、`bj` 下的 `lday`、`fzline`、`minline`。`--out` 是 Parquet 和 `manifest.sqlite` 的根目录；换一个输出目录就是另一份库，水位互不影响。

`--freq` 省略时三个周期都跑。取值只能是 `1d`、`5m`、`1m` 的组合。非法取值、缺少必填参数，命令以非 0 退出。没有匹配到任何源文件时记一条日志，以 0 退出。

不支持两个进程同时写同一个 `--out`。

实现用 Python 3.11+。写 Parquet 用 pyarrow，水位用标准库 sqlite3。入口是 `tdx-import`，包名 `tdx_data`。

## 数据流

```text
vipdoc/{sh,sz,bj}/{lday,fzline,minline}
        │  discover：列出源文件
        │  classify：留下股票和指数
        ▼
     parse：32 字节一条
        │  normalize：统一代码、价格、单位；丢掉占位和坏记录
        ▼
     store：按分区归并，原子替换
        ▼
{out}/bars/...
{out}/manifest.sqlite
```

| 模块 | 职责 |
|---|---|
| `discover` | 遍历约定目录，产出市场、代码、周期、路径、大小 |
| `classify` | 判断股票或指数，其余丢掉 |
| `parse` | 解析 `.day` / `.lc1` / `.lc5`，不认识分区 |
| `normalize` | 变成下面的表结构 |
| `store` | 读旧分区、合并、原子替换，维护 manifest。不认识通达信字节布局 |
| `ingest` | 串起上面的模块，实现 `ingest` 子命令 |
| `verify` | 源文件与 Parquet 对账，实现 `verify` 子命令 |

## 落盘布局

日线按年，分钟线按交易日。每个分区只有一个文件。

```text
{out}/bars/freq=1d/year=2024/part-0.parquet
{out}/bars/freq=1m/date=2024-11-22/part-0.parquet
{out}/bars/freq=5m/date=2024-11-22/part-0.parquet
{out}/manifest.sqlite
{out}/_staging/          仅第一次全量期间存在，成功后删除
```

周期、年、日期只出现在目录上，不重复写进文件。分区内按 `symbol, datetime` 排序。压缩用 zstd。`symbol`、`security_type`、`volume_unit` 用 dictionary 编码。`symbol` 列写 bloom filter。

每个 row group 的最大行数固定为下表。数据按代码排序后再切开，一个 row group 里的代码区间因此较窄，只读一只股票时可以跳过其余 row group。停牌或不满一整日的代码不会凑满这个行数，不要求每个 row group 恰好 50 个代码。

| 周期 | row group 最大行数 |
|---|---|
| `1m` | 12000 |
| `5m` | 2400 |
| `1d` | 12000 |

## 表结构

三个周期同一套列。

| 列 | 类型 | 含义 |
|---|---|---|
| `symbol` | string | `600000.SH`、`000001.SZ`、`920001.BJ`。后缀大写，指数同样带后缀 |
| `security_type` | string | `stock` 或 `index` |
| `trade_date` | date32 | 交易日 |
| `datetime` | timestamp[us] | 北京时间，不带时区。分钟线是该根 K 线的结束时刻，第一根 1 分钟是 09:31。日线固定为当日 15:00:00 |
| `open` `high` `low` `close` | float64 | 元。日线由「价格 × 100」的整数除以 100。分钟线源是 float32，升为 float64，精度仍是 float32 |
| `volume` | int64 | 源文件里的数值，不换算 |
| `volume_unit` | string | 股票 `share`（股），指数 `lot`（手） |
| `amount` | float64 | 元。源是 float32，升为 float64。指数等大成交额在源文件里已经损失精度 |

`trade_date` 等于 `datetime` 的日期。高低价颠倒时原样入库，不修正。

## 代码规则

文件名是 `{sh|sz|bj}{6位代码}.{day|lc5|lc1}`。只保留下表，其余文件忽略。

| 市场 | 股票前缀 | 指数前缀 |
|---|---|---|
| `sh` | `600` `601` `603` `605` `688` `689` | `000` |
| `sz` | `000` `001` `002` `003` `300` `301` | `399` |
| `bj` | `920` `43` `82` `83` `87` | `899` |

因此丢弃：上证 `510/511/113/900/880`，深证 `159/127/200`，以及任何不在表内的前缀。`000001.SH`（指数）和 `000001.SZ`（股票）靠后缀区分。

## 记录过滤

日线一条 32 字节，小端：

| 偏移 | 类型 | 含义 |
|---|---|---|
| 0 | uint32 | `YYYYMMDD` |
| 4 | uint32 × 4 | 开、高、低、收，价格 × 100 |
| 20 | float32 | 成交额（元） |
| 24 | uint32 | 成交量 |
| 28 | uint32 | 忽略 |

分钟线一条 32 字节，小端：

| 偏移 | 类型 | 含义 |
|---|---|---|
| 0 | uint16 | 日期：`year = n // 2048 + 2004`，`month = (n % 2048) // 100`，`day = (n % 2048) % 100` |
| 2 | uint16 | 从 0 点起的分钟数，`571` = 09:31，`898` = 14:58，`899` = 14:59 |
| 4 | float32 × 4 | 开、高、低、收（元） |
| 20 | float32 | 成交额（元） |
| 24 | uint32 | 成交量 |
| 28 | uint32 | 忽略 |

丢掉这些记录：

- 日期不是合法公历日期。
- 开、高、低、收全为 0。
- `.lc1` 中分钟数 ≥ 898 且成交量和成交额都为 0。这是收盘占位。盘中量为 0 的分钟保留。`.lc5` 不做这条判断。

文件长度不是 32 的整数倍时，解析前面的完整记录，丢掉末尾半条。该源文件的水位不前移，文件以后被写完整时会再次解析。

## 增量

水位在 `{out}/manifest.sqlite`：

```sql
CREATE TABLE source_file (
  relpath       TEXT PRIMARY KEY,
  freq          TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  size          INTEGER NOT NULL,
  tail          BLOB NOT NULL,    -- 长度为 32，位于文件偏移 size-32
  last_datetime TEXT NOT NULL,
  valid_rows    INTEGER NOT NULL
);
```

| 情况 | 动作 |
|---|---|
| 大小未变，且文件末尾 32 字节等于 `tail` | 跳过 |
| 文件变大，且偏移 `旧size-32` 处的 32 字节仍等于 `tail`，并且旧 size 是 32 的倍数 | 只解析新增尾部 |
| 文件变小、尾部对不上，或旧 size 不是 32 的倍数 | 整文件重解析，用新数据替换该代码在各分区中的行 |

合并键是 `(symbol, datetime)`。一个分区在一轮里只合并一次：收齐本轮有变化的代码，从旧 Parquet 删掉这些代码在该分区的行，拼上新行，排序后写成 `part-0.parquet.tmp`，再在同一目录内替换为 `part-0.parquet`。一天的 1 分钟大约一百多万行，这次合并放在内存里做。

第一次全量不能按「一代码一天一个文件」落临时数据。步骤是：

1. 每个源文件解析成 `{out}/_staging/freq={freq}/symbol={symbol}.parquet`。此期间磁盘大约是最终数据的两倍。
2. 按月从这些归档抽出每个交易日（日线则按年抽出），写成最终分区。
3. 本轮涉及的分区都替换完成后，删除 `_staging`，再用一个事务写入 manifest。

启动时如果 manifest 里还没有行，而 `_staging` 还在，删掉 `_staging` 再从头全量。避免把半成品拼进正式分区。

此后的每日更新只走尾部解析，不再做全量折叠。重写历史的文件走整文件重解析，按日期把该代码的行换掉。

## 失败

`part-0.parquet.tmp` 写完之后才替换。旧文件保留到替换成功。manifest 在本轮所有受影响分区都替换完成、并且 `_staging` 已删除之后，用一个事务更新。

进程中途停掉时，旧 Parquet 还在，水位仍是上一轮的。下次运行会把同一批数据再解析一遍，按代码删除再插入，结果与成功跑完一次相同。

单个源文件解析失败：把路径和原因写到 stderr，跳过该文件，不写它的水位，其他文件继续。只要有跳过的文件，进程以非 0 退出。

## 验证

### 合成样本

`pytest` 不读真实 `vipdoc`，不访问网络，应在几秒内跑完。样本由测试代码按 32 字节布局拼出。

解析：

- 日线样本：日期 `20241122`，开高低收整数 `1050/1080/1040/1075`，成交额 `1234567.5`，成交量 `10000`，保留字段非 0。结果为 `10.50/10.80/10.40/10.75` 元，量额不变，保留字段不出现。同一文件连续两天，顺序和条数不变。
- 分钟：`571 → 09:31`，`900 → 15:00`。`2004-01-01` 可以解析。月份 13、日 0、日 32 整条丢掉。float32 价格 `10.5` 升到 float64 后，误差在 1 个 float32 ulp 以内。
- `.lc1` 与 `.lc5` 用同一组字节：`.lc1` 在 14:58 或 14:59 且量额都为 0 时丢掉；`.lc1` 在 14:58 且量大于 0 时留下；`.lc1` 在 14:57 且量为 0 时留下；`.lc5` 在 14:59 且量为 0 时留下。
- 开高低收全 0 的记录丢掉。高 < 低的记录原样留下。
- 文件长度不是 32 的整数倍时，完整记录保留，水位不前移。

代码与单位，每行一条参数化测试：

| 代码 | 结果 |
|---|---|
| `sh600000` `sh601398` `sh603259` `sh605117` `sh688981` `sh689009` | 股票，`600000.SH` 这种写法，单位股 |
| `sh000001` `sh000300` `sh000688` | 指数，单位手 |
| `sz000001` `sz001979` `sz002594` `sz003816` `sz300750` `sz301269` | 股票，单位股 |
| `sz399001` `sz399006` | 指数，单位手 |
| `bj920001` `bj430047` `bj830000` `bj870000` `bj820000` | 股票，单位股 |
| `bj899050` | 指数，单位手 |
| `sh510050` `sh113050` `sh900901` `sh880001` `sz159915` `sz127001` `sz200011` | 不入库 |

成交量数值不因单位改变：股票文件中的 `10000` 仍是 `10000`，指数文件中的 `10000` 仍是 `10000`。日线 `datetime` 为当日 `15:00:00`。分钟线 `datetime` 为该根结束时刻。`trade_date` 与该时刻的日期相同。

用临时目录扮演 `vipdoc`（2 只股票、1 只指数、日线 2 天、两种分钟线各若干根，并混入一只 ETF）：

- 第一次导入产生约定的 hive 目录，每个分区只有 `part-0.parquet`，分区内按 `symbol, datetime` 排序，manifest 每个源文件一行。ETF 不出现。
- 只给一只股票追加一个交易日：新分区有它，另外两只不在该分区，旧日期行数不变。
- 三只一起追加同一天：该分区一次写完，三只都在且按代码排序。
- 改某一天中间一根 bar 的收盘价：该收盘价更新，其余日期还在，`(symbol, datetime)` 不重复。
- 源文件不变再跑一次：`part-0.parquet` 内容哈希不变。
- 替换前停掉（`.tmp` 已写好，正式文件仍是旧的，manifest 未更新）：再跑后只剩一个 `part-0.parquet`，没有 `.tmp`，数据与一次成功导入相同。
- 替换完成但 manifest 尚未提交时停掉：再跑后行集合相同，无重复行。
- 其中一个文件损坏：其余标的入库，损坏代码不在 Parquet 中，它没有水位，进程非 0 退出。

### 真实数据

仓库不存放真实 `vipdoc`。

第一次导入完成后，人工各对一只股票和一只指数。`600000.SH` 对某一天的开高低收、成交量（股）、成交额（元），且当天第一根 1 分钟是 09:31。`000001.SH` 同样对照，指数成交量按手和客户端比较。每个新环境做一次。

本机安装了 `pytdx` 时，用 `TdxDailyBarReader` 和 `TdxLCMinBarReader` 读同一批源文件。比较时间、开高低收、成交量、成交额；成交量按源文件数值比较，不把指数从手换算成股。`pytdx` 不是安装依赖；没有就跳过。两边不一致时先判断是哪一边的解释问题，这个对照不是唯一标准。

`tdx-import verify` 把源文件和 Parquet 各扫一遍，按 `(freq, symbol)` 对齐。每一侧统计：过滤后的行数、第一根和最后一根时间、全部 bar 的 SHA-256。对不上时把代码、周期和两边的数字打到 stderr。

哈希按 `datetime` 升序，逐行喂给 SHA-256。每一行的字节是：`symbol` 的 UTF-8、`datetime` 的 ASCII `YYYY-MM-DDTHH:MM:SS`、开高低收和 `amount` 各一个 little-endian float64、`volume` 一个 little-endian int64。源文件一侧先走同一套 parse 和 normalize，再哈希，这样和 Parquet 里的 float64 位型一致。

同时检查：

- 分区目录中的日期与行上的 `trade_date` 一致。
- `(symbol, datetime)` 不重复。
- 分区内按 `symbol, datetime` 排序。
- 已丢弃的品种没有出现。
- 股票的 `volume_unit` 是 `share`，指数是 `lot`。

`--symbol 600000.SH,000001.SH` 再把这些代码的每一根 bar 与源文件比较开高低收、量、额。日线价格比较四舍五入到分后的整数。分钟线价格和成交额先转回 float32 再比较。成交量按整数比较。

日线成交量与当天 1 分钟成交量之和只打印差额，不作为失败条件。

`verify` 发现任何不一致时以非 0 退出。对着真实 `vipdoc` 跑完整扫描是第一次全量导入后的验收，不放进日常 `pytest`。`tests/test_verify.py` 只用合成的小型 `vipdoc`，覆盖对得上、行数不符、以及非 0 退出。

## 实现落点

```text
pyproject.toml
src/tdx_data/
  discover.py
  classify.py
  parse.py
  normalize.py
  store.py
  ingest.py
  verify.py
  cli.py
tests/
  test_parse_day.py
  test_parse_minute.py
  test_classify.py
  test_ingest.py
  test_verify.py
```
