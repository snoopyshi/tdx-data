"""Parquet 分区、原子替换和 manifest。"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tdx_data.normalize import Bar

ROW_GROUP_ROWS = {"1d": 12000, "5m": 2400, "1m": 12000}

PLAIN_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("security_type", pa.string()),
        ("trade_date", pa.date32()),
        ("datetime", pa.timestamp("us")),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
        ("volume_unit", pa.string()),
        ("amount", pa.float64()),
    ]
)

_CREATE = """
CREATE TABLE IF NOT EXISTS source_file (
  relpath       TEXT PRIMARY KEY,
  freq          TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  size          INTEGER NOT NULL,
  tail          BLOB NOT NULL,
  last_datetime TEXT NOT NULL,
  valid_rows    INTEGER NOT NULL
)
"""


@dataclass(frozen=True)
class PartKey:
    freq: str
    token: str

    def path(self, out: Path) -> Path:
        if self.freq == "1d":
            return out / "bars" / "freq=1d" / f"year={self.token}" / "part-0.parquet"
        return out / "bars" / f"freq={self.freq}" / f"date={self.token}" / "part-0.parquet"


def key_for(freq: str, bar: Bar) -> PartKey:
    if freq == "1d":
        return PartKey("1d", str(bar.trade_date.year))
    return PartKey(freq, bar.trade_date.isoformat())


def key_from_path(path: Path) -> PartKey:
    freq = ""
    token = ""
    for part in path.parts:
        if part.startswith("freq="):
            freq = part.split("=", 1)[1]
        elif part.startswith("year=") or part.startswith("date="):
            token = part.split("=", 1)[1]
    if not freq or not token:
        raise ValueError(f"无法识别分区路径: {path}")
    return PartKey(freq, token)


@dataclass(frozen=True)
class ManifestRow:
    relpath: str
    freq: str
    symbol: str
    size: int
    tail: bytes
    last_datetime: str
    valid_rows: int


def connect(out: Path) -> sqlite3.Connection:
    out.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(out / "manifest.sqlite")
    conn.execute(_CREATE)
    return conn


def load_manifest(conn: sqlite3.Connection) -> dict[str, ManifestRow]:
    rows = conn.execute(
        "SELECT relpath, freq, symbol, size, tail, last_datetime, valid_rows FROM source_file"
    ).fetchall()
    found: dict[str, ManifestRow] = {}
    for relpath, freq, symbol, size, tail, last_datetime, valid_rows in rows:
        found[relpath] = ManifestRow(
            relpath=relpath,
            freq=freq,
            symbol=symbol,
            size=size,
            tail=bytes(tail),
            last_datetime=last_datetime,
            valid_rows=valid_rows,
        )
    return found


def manifest_count(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM source_file").fetchone()[0])


def upsert_manifest(conn: sqlite3.Connection, rows: list[ManifestRow]) -> None:
    if not rows:
        return
    with conn:
        conn.executemany(
            """
            INSERT INTO source_file
              (relpath, freq, symbol, size, tail, last_datetime, valid_rows)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(relpath) DO UPDATE SET
              freq=excluded.freq,
              symbol=excluded.symbol,
              size=excluded.size,
              tail=excluded.tail,
              last_datetime=excluded.last_datetime,
              valid_rows=excluded.valid_rows
            """,
            [
                (
                    row.relpath,
                    row.freq,
                    row.symbol,
                    row.size,
                    row.tail,
                    row.last_datetime,
                    row.valid_rows,
                )
                for row in rows
            ],
        )


def bars_to_table(bars: list[Bar]) -> pa.Table:
    if not bars:
        return PLAIN_SCHEMA.empty_table()
    return pa.table(
        {
            "symbol": [bar.symbol for bar in bars],
            "security_type": [bar.security_type for bar in bars],
            "trade_date": [bar.trade_date for bar in bars],
            "datetime": [bar.datetime for bar in bars],
            "open": [bar.open for bar in bars],
            "high": [bar.high for bar in bars],
            "low": [bar.low for bar in bars],
            "close": [bar.close for bar in bars],
            "volume": [bar.volume for bar in bars],
            "volume_unit": [bar.volume_unit for bar in bars],
            "amount": [bar.amount for bar in bars],
        },
        schema=PLAIN_SCHEMA,
    )


def read_plain(path: Path) -> pa.Table:
    return pq.read_table(path).cast(PLAIN_SCHEMA)


def iter_partitions(out: Path):
    root = out / "bars"
    if not root.exists():
        return
    for path in sorted(root.rglob("part-0.parquet")):
        yield key_from_path(path), path


def _write_kwargs(freq: str) -> dict:
    return {
        "compression": "zstd",
        "row_group_size": ROW_GROUP_ROWS[freq],
        "use_dictionary": ["symbol", "security_type", "volume_unit"],
        "write_statistics": True,
        "bloom_filter_options": {"symbol": True},
    }


def publish(table: pa.Table, dest: Path, freq: str, after_tmp=None) -> None:
    """写成临时文件后再替换。空表则删除已有分区。"""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if table.num_rows == 0:
        if dest.exists():
            dest.unlink()
        return
    ordered = table.sort_by([("symbol", "ascending"), ("datetime", "ascending")])
    tmp = dest.parent / f"{dest.name}.tmp"
    pq.write_table(ordered, tmp, **_write_kwargs(freq))
    with tmp.open("rb+") as handle:
        os.fsync(handle.fileno())
    if after_tmp is not None:
        after_tmp(tmp)
    os.replace(tmp, dest)


def drop_symbols(table: pa.Table, symbols: set[str]) -> pa.Table:
    if table.num_rows == 0 or not symbols:
        return table
    mask = pc.invert(pc.is_in(table.column("symbol"), value_set=pa.array(sorted(symbols))))
    return table.filter(mask)


def drop_before_append(
    table: pa.Table,
    floors: dict[tuple[PartKey, str], datetime],
    key: PartKey,
) -> pa.Table:
    """删掉本次尾部会重新写入的行，保留同一分区里更早的 bar。"""
    if table.num_rows == 0 or not floors:
        return table
    symbols = table.column("symbol").to_pylist()
    times = table.column("datetime").to_pylist()
    keep = []
    for symbol, when in zip(symbols, times):
        floor = floors.get((key, symbol))
        keep.append(floor is None or when < floor)
    return table.filter(pa.array(keep))


def concat_tables(tables: list[pa.Table]) -> pa.Table:
    parts = [table for table in tables if table is not None and table.num_rows]
    if not parts:
        return PLAIN_SCHEMA.empty_table()
    return pa.concat_tables(parts, promote_options="default")


def year_bounds(year: int) -> tuple[date, date]:
    return date(year, 1, 1), date(year + 1, 1, 1)


def month_bounds(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    if month == 12:
        return start, date(year + 1, 1, 1)
    return start, date(year, month + 1, 1)


class StagingWriter:
    """按代码把 bar 流式写入临时 Parquet，避免整份分钟线进内存。"""

    def __init__(self, path: Path):
        self.path = path
        self.rows = 0
        self._buffer: list[Bar] = []
        self._writer: pq.ParquetWriter | None = None

    def add(self, bar: Bar) -> None:
        self._buffer.append(bar)
        self.rows += 1
        if len(self._buffer) >= 100_000:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        table = bars_to_table(self._buffer)
        self._buffer.clear()
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(self.path, PLAIN_SCHEMA, compression="zstd")
        self._writer.write_table(table)

    def close(self) -> None:
        self.flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def discard(self) -> None:
        self._buffer.clear()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        if self.path.exists():
            self.path.unlink()
