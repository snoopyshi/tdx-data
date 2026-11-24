"""源文件和 Parquet 对账。"""

from __future__ import annotations

import hashlib
import struct
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from tdx_data.classify import classify, parse_symbol
from tdx_data.discover import discover
from tdx_data.ingest import parse_freqs
from tdx_data.normalize import iter_bars
from tdx_data.store import iter_partitions

_PRICE = ("open", "high", "low", "close")


@dataclass
class _Agg:
    count: int = 0
    first: datetime | None = None
    last: datetime | None = None
    hasher: "hashlib._Hash" = field(default_factory=hashlib.sha256)

    @property
    def digest(self) -> str:
        return self.hasher.hexdigest()


def _feed(agg: _Agg, symbol: str, when: datetime, open_, high, low, close, amount, volume: int) -> None:
    agg.count += 1
    if agg.first is None or when < agg.first:
        agg.first = when
    if agg.last is None or when > agg.last:
        agg.last = when
    agg.hasher.update(symbol.encode("utf-8"))
    agg.hasher.update(when.strftime("%Y-%m-%dT%H:%M:%S").encode("ascii"))
    agg.hasher.update(struct.pack("<dddddq", open_, high, low, close, amount, volume))


def verify(vipdoc: Path, out: Path, symbols: list[str] | None = None) -> int:
    if not vipdoc.is_dir():
        print(f"vipdoc 目录不存在: {vipdoc}", file=sys.stderr)
        return 1
    if not (out / "bars").is_dir():
        print(f"还没有导入结果: {out}", file=sys.stderr)
        return 1

    problems: list[str] = []
    freqs = list(parse_freqs("1d,5m,1m"))
    source_aggs = _scan_sources(vipdoc, freqs, problems)
    parquet_aggs = _scan_parquet(out, problems)
    _compare_aggs(source_aggs, parquet_aggs, problems)
    if symbols:
        _deep_compare(vipdoc, out, symbols, problems)
    _print_volume_gaps(source_aggs, vipdoc)
    for line in problems[:50]:
        print(line, file=sys.stderr)
    if len(problems) > 50:
        print(f"还有 {len(problems) - 50} 条不一致", file=sys.stderr)
    return 1 if problems else 0


def _scan_sources(vipdoc: Path, freqs: list[str], problems: list[str]) -> dict[tuple[str, str], _Agg]:
    found, errors = discover(vipdoc, freqs)
    for path, message in errors:
        problems.append(f"源文件读不了 {path}: {message}")
    aggs: dict[tuple[str, str], _Agg] = {}
    for source in found:
        agg = aggs.setdefault((source.freq, source.security.symbol), _Agg())
        try:
            for bar in iter_bars(source.path, source.security, source.freq):
                _feed(
                    agg,
                    bar.symbol,
                    bar.datetime,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.amount,
                    bar.volume,
                )
        except OSError as exc:
            problems.append(f"失败 {source.relpath}: {exc}")
    return aggs


def _scan_parquet(out: Path, problems: list[str]) -> dict[tuple[str, str], _Agg]:
    aggs: dict[tuple[str, str], _Agg] = {}
    for key, path in iter_partitions(out):
        table = pq.read_table(path)
        rows = table.to_pylist()
        previous = None
        seen: set[tuple[str, datetime]] = set()
        for row in rows:
            symbol = row["symbol"]
            when = row["datetime"]
            identity = (symbol, when)
            if previous is not None and identity < previous:
                problems.append(f"{path} 没有按 symbol, datetime 排序")
            if identity in seen:
                problems.append(f"重复 bar {symbol} {when.isoformat()}")
            seen.add(identity)
            previous = identity
            trade_date = row["trade_date"]
            if key.freq == "1d" and str(trade_date.year) != key.token:
                problems.append(f"{path} 的交易日 {trade_date} 不在 year={key.token}")
            if key.freq != "1d" and trade_date.isoformat() != key.token:
                problems.append(f"{path} 的交易日 {trade_date} 不是 {key.token}")
            parsed = parse_symbol(symbol)
            security = classify(*parsed) if parsed else None
            if security is None:
                problems.append(f"不该入库的代码 {symbol}")
            else:
                if row["security_type"] != security.security_type:
                    problems.append(f"{symbol} security_type 是 {row['security_type']}")
                if row["volume_unit"] != security.volume_unit:
                    problems.append(f"{symbol} volume_unit 是 {row['volume_unit']}")
            agg = aggs.setdefault((key.freq, symbol), _Agg())
            _feed(
                agg,
                symbol,
                when,
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["amount"],
                row["volume"],
            )
    return aggs


def _compare_aggs(source, parquet, problems: list[str]) -> None:
    keys = set(source) | set(parquet)
    for freq, symbol in sorted(keys):
        left = source.get((freq, symbol))
        right = parquet.get((freq, symbol))
        if left is None or left.count == 0:
            if right and right.count:
                problems.append(f"Parquet 多出 {freq} {symbol} {right.count} 行")
            continue
        if right is None or right.count == 0:
            problems.append(f"Parquet 缺少 {freq} {symbol} {left.count} 行")
            continue
        if left.count != right.count or left.first != right.first or left.last != right.last or left.digest != right.digest:
            problems.append(
                f"不一致 {freq} {symbol}: "
                f"源 {left.count} {left.first} {left.last} {left.digest[:12]} / "
                f"库 {right.count} {right.first} {right.last} {right.digest[:12]}"
            )


def _deep_compare(vipdoc: Path, out: Path, symbols: list[str], problems: list[str]) -> None:
    wanted = set(symbols)
    source_rows: dict[tuple[str, str], list] = {}
    found, _errors = discover(vipdoc, list(parse_freqs("1d,5m,1m")))
    for source in found:
        if source.security.symbol not in wanted:
            continue
        rows = source_rows.setdefault((source.freq, source.security.symbol), [])
        for bar in iter_bars(source.path, source.security, source.freq):
            rows.append(bar)
    parquet_rows: dict[tuple[str, str], list] = {key: [] for key in source_rows}
    for key, path in iter_partitions(out):
        for row in pq.read_table(path).to_pylist():
            if row["symbol"] not in wanted:
                continue
            parquet_rows.setdefault((key.freq, row["symbol"]), []).append(row)
    pairs = [(freq, symbol) for freq in ("1d", "5m", "1m") for symbol in sorted(wanted)]
    for freq, symbol in pairs:
        left = source_rows.get((freq, symbol), [])
        right = parquet_rows.get((freq, symbol), [])
        if not left and not right:
            continue
        if len(left) != len(right):
            problems.append(f"逐行 {freq} {symbol} 行数 {len(left)} != {len(right)}")
            continue
        left_sorted = sorted(left, key=lambda bar: bar.datetime)
        right_sorted = sorted(right, key=lambda row: row["datetime"])
        for bar, row in zip(left_sorted, right_sorted):
            if not _values_equal(freq, bar, row):
                problems.append(f"逐行不一致 {freq} {symbol} {bar.datetime}")


def _values_equal(freq: str, bar, row) -> bool:
    if bar.datetime != row["datetime"] or bar.volume != row["volume"]:
        return False
    for name in (*_PRICE, "amount"):
        if not _number_equal(freq, name, getattr(bar, name), row[name]):
            return False
    return True


def _number_equal(freq: str, name: str, left: float, right: float) -> bool:
    if freq == "1d" and name != "amount":
        return round(left * 100) == round(right * 100)
    return struct.pack("<f", left) == struct.pack("<f", right)


def _print_volume_gaps(source_aggs, vipdoc: Path) -> None:
    # 日线和 1 分钟各自再扫一遍成交量。只打印差额，不影响退出码。
    daily: dict[tuple[str, object], int] = {}
    minute: dict[tuple[str, object], int] = {}
    found, _errors = discover(vipdoc, ["1d", "1m"])
    for source in found:
        if (source.freq, source.security.symbol) not in source_aggs:
            continue
        for bar in iter_bars(source.path, source.security, source.freq):
            bucket = daily if source.freq == "1d" else minute
            key = (bar.symbol, bar.trade_date)
            bucket[key] = bucket.get(key, 0) + bar.volume
    for key in sorted(set(daily) & set(minute)):
        symbol, trade_date = key
        left = daily[key]
        right = minute[key]
        if left != right:
            print(f"成交量差额 {symbol} {trade_date} 日线={left} 1分钟合计={right} 差={left - right}")
