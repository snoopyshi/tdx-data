"""把 vipdoc 导入成分区 Parquet，并维护水位。"""

from __future__ import annotations

import fcntl
import shutil
import struct
import sys
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

from tdx_data.discover import discover
from tdx_data.normalize import Bar, iter_bars
from tdx_data.parse import RECORD_SIZE, read_tail
from tdx_data.store import (
    ManifestRow,
    PartKey,
    StagingWriter,
    bars_to_table,
    concat_tables,
    connect,
    drop_before_append,
    drop_symbols,
    iter_partitions,
    key_for,
    load_manifest,
    manifest_count,
    month_bounds,
    publish,
    read_plain,
    upsert_manifest,
    year_bounds,
)

ALLOWED_FREQS = ("1d", "5m", "1m")


@dataclass
class IngestHooks:
    """测试用的中断点。正常导入不传。"""

    after_tmp: Callable[[Path], None] | None = None
    before_manifest: Callable[[], None] | None = None


class _Lock:
    def __init__(self, path: Path):
        self._handle = path.open("a+")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._handle.close()
            raise

    def close(self) -> None:
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()


def parse_freqs(text: str) -> list[str]:
    parts = [item.strip() for item in text.split(",") if item.strip()]
    if not parts or any(item not in ALLOWED_FREQS for item in parts):
        allowed = ", ".join(ALLOWED_FREQS)
        raise ValueError(f"freq 只能是 {allowed} 的组合")
    seen: list[str] = []
    for item in parts:
        if item not in seen:
            seen.append(item)
    return seen


def _format_dt(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def _aligned(size: int) -> bool:
    return size >= RECORD_SIZE and size % RECORD_SIZE == 0


def ingest(
    vipdoc: Path,
    out: Path,
    freq: str = "1d,5m,1m",
    hooks: IngestHooks | None = None,
) -> int:
    if not vipdoc.is_dir():
        print(f"vipdoc 目录不存在: {vipdoc}", file=sys.stderr)
        return 1
    try:
        freqs = parse_freqs(freq)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2

    out.mkdir(parents=True, exist_ok=True)
    try:
        lock = _Lock(out / ".ingest.lock")
    except BlockingIOError:
        print("另一个导入进程正在写这个输出目录", file=sys.stderr)
        return 1

    hooks = hooks or IngestHooks()
    try:
        conn = connect(out)
        try:
            fresh = manifest_count(conn) == 0
            if fresh and ((out / "_staging").exists() or (out / "bars").exists()):
                print("manifest 为空，重新全量导入", file=sys.stderr)
            if fresh:
                _remove_tree(out / "_staging")
                _remove_tree(out / "bars")
            else:
                _remove_tree(out / "_staging")
            sources, errors = discover(vipdoc, freqs)
            failures = _report_discover_errors(errors)
            if not sources and not failures:
                print("没有匹配到源文件", file=sys.stderr)
                return 0
            if fresh:
                commits, more = _import_full(out, sources, freqs, hooks)
            else:
                commits, more = _import_incremental(out, sources, load_manifest(conn), hooks)
            failures.extend(more)
            _remove_tree(out / "_staging")
            if hooks.before_manifest is not None:
                hooks.before_manifest()
            upsert_manifest(conn, commits)
        finally:
            conn.close()
    finally:
        lock.close()
    return 1 if failures else 0


def _report_discover_errors(errors: list[tuple[Path, str]]) -> list[tuple[Path, str]]:
    for path, message in errors:
        print(f"失败 {path}: {message}", file=sys.stderr)
    return list(errors)


def _remove_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _stage(out: Path, source, offset: int) -> tuple[int, datetime | None]:
    writer = StagingWriter(
        out / "_staging" / f"freq={source.freq}" / f"symbol={source.security.symbol}.parquet"
    )
    last: datetime | None = None
    try:
        for bar in iter_bars(source.path, source.security, source.freq, offset):
            writer.add(bar)
            last = bar.datetime
        writer.close()
    except Exception:
        writer.discard()
        raise
    return writer.rows, last


def _import_full(out, sources, freqs, hooks: IngestHooks):
    commits: list[ManifestRow] = []
    failures: list[tuple[Path, str]] = []
    for source in sources:
        try:
            rows, last = _stage(out, source, 0)
        except (OSError, ValueError, struct.error) as exc:
            print(f"失败 {source.relpath}: {exc}", file=sys.stderr)
            failures.append((source.path, str(exc)))
            continue
        if _aligned(source.size):
            commits.append(
                ManifestRow(
                    relpath=source.relpath,
                    freq=source.freq,
                    symbol=source.security.symbol,
                    size=source.size,
                    tail=read_tail(source.path, source.size),
                    last_datetime=_format_dt(last),
                    valid_rows=rows,
                )
            )
    for freq in freqs:
        _fold_freq(out, freq, hooks.after_tmp)
    return commits, failures


def _fold_freq(out: Path, freq: str, after_tmp) -> None:
    folder = out / "_staging" / f"freq={freq}"
    files = sorted(folder.glob("symbol=*.parquet")) if folder.is_dir() else []
    if not files:
        return
    if freq == "1d":
        for year in _years(files):
            table = _read_many(files, *_year_filter(year))
            publish(table, PartKey("1d", str(year)).path(out), freq, after_tmp)
        return
    for year, month in _months(files):
        start, end = month_bounds(year, month)
        table = _read_many(
            files,
            ("trade_date", ">=", start),
            ("trade_date", "<", end),
        )
        if table.num_rows == 0:
            continue
        for day in sorted(set(table.column("trade_date").to_pylist())):
            part = table.filter(pc.equal(table.column("trade_date"), day))
            publish(part, PartKey(freq, day.isoformat()).path(out), freq, after_tmp)


def _years(files: list[Path]) -> list[int]:
    found: set[int] = set()
    for path in files:
        column = pq.read_table(path, columns=["trade_date"]).column("trade_date")
        for day in column.to_pylist():
            found.add(day.year)
    return sorted(found)


def _months(files: list[Path]) -> list[tuple[int, int]]:
    found: set[tuple[int, int]] = set()
    for path in files:
        column = pq.read_table(path, columns=["trade_date"]).column("trade_date")
        for day in column.to_pylist():
            found.add((day.year, day.month))
    return sorted(found)


def _year_filter(year: int):
    start, end = year_bounds(year)
    return ("trade_date", ">=", start), ("trade_date", "<", end)


def _read_many(files: list[Path], *filters) -> "pyarrow.Table":
    import pyarrow as pa

    from tdx_data.store import PLAIN_SCHEMA

    tables = []
    for path in files:
        table = pq.read_table(path, filters=list(filters) if filters else None)
        if table.num_rows:
            tables.append(table.cast(PLAIN_SCHEMA))
    if not tables:
        return PLAIN_SCHEMA.empty_table()
    return pa.concat_tables(tables, promote_options="default")


def _import_incremental(out, sources, manifest: dict[str, ManifestRow], hooks: IngestHooks):
    commits: list[ManifestRow] = []
    failures: list[tuple[Path, str]] = []
    append_groups: dict[PartKey, list[Bar]] = defaultdict(list)
    floors: dict[tuple[PartKey, str], datetime] = {}
    replace_symbols: dict[str, set[str]] = defaultdict(set)

    for source in sources:
        previous = manifest.get(source.relpath)
        try:
            kind = _decide(source, previous)
            if kind == "skip":
                continue
            offset = previous.size if kind == "append" else 0
            if kind == "append":
                bars = list(iter_bars(source.path, source.security, source.freq, offset))
                last = bars[-1].datetime if bars else None
                kept = len(bars)
                for bar in bars:
                    key = key_for(source.freq, bar)
                    append_groups[key].append(bar)
                    floor_key = (key, bar.symbol)
                    current = floors.get(floor_key)
                    if current is None or bar.datetime < current:
                        floors[floor_key] = bar.datetime
                base_rows = previous.valid_rows
                base_last = previous.last_datetime
            else:
                kept, last = _stage(out, source, 0)
                replace_symbols[source.freq].add(source.security.symbol)
                base_rows = 0
                base_last = ""
        except (OSError, ValueError, struct.error) as exc:
            print(f"失败 {source.relpath}: {exc}", file=sys.stderr)
            failures.append((source.path, str(exc)))
            continue
        if not _aligned(source.size):
            continue
        final_last = _format_dt(last) if last is not None else base_last
        commits.append(
            ManifestRow(
                relpath=source.relpath,
                freq=source.freq,
                symbol=source.security.symbol,
                size=source.size,
                tail=read_tail(source.path, source.size),
                last_datetime=final_last,
                valid_rows=base_rows + kept if kind == "append" else kept,
            )
        )
    _merge_incremental(out, append_groups, floors, replace_symbols, hooks.after_tmp)
    return commits, failures


def _decide(source, previous: ManifestRow | None) -> str:
    if previous is None:
        return "full"
    if source.size == previous.size and _aligned(source.size):
        if read_tail(source.path, source.size) == previous.tail:
            return "skip"
    if (
        source.size > previous.size
        and _aligned(previous.size)
        and read_tail(source.path, previous.size) == previous.tail
    ):
        return "append"
    return "full"


def _merge_incremental(out, append_groups, floors, replace_symbols, after_tmp) -> None:
    existing = {key: path for key, path in iter_partitions(out)}
    replace_keys = _staging_keys(out, replace_symbols)
    affected = set(append_groups) | set(replace_keys)
    for key, path in existing.items():
        symbols = replace_symbols.get(key.freq)
        if not symbols or key in affected:
            continue
        table = read_plain(path)
        if _contains_any(table, symbols):
            affected.add(key)
    for key in sorted(affected, key=lambda item: (item.freq, item.token)):
        path = existing.get(key, key.path(out))
        table = read_plain(path) if path.exists() else None
        pieces = []
        if table is not None:
            if replace_symbols.get(key.freq):
                table = drop_symbols(table, replace_symbols[key.freq])
            if key in append_groups:
                table = drop_before_append(table, floors, key)
            if table.num_rows:
                pieces.append(table)
        if key in append_groups:
            pieces.append(bars_to_table(append_groups[key]))
        staged = _read_replace_slice(out, replace_symbols.get(key.freq, set()), key)
        if staged.num_rows:
            pieces.append(staged)
        publish(concat_tables(pieces), key.path(out), key.freq, after_tmp)


def _contains_any(table, symbols: set[str]) -> bool:
    if table.num_rows == 0:
        return False
    import pyarrow as pa
    import pyarrow.compute as pc

    mask = pc.is_in(table.column("symbol"), value_set=pa.array(sorted(symbols)))
    return bool(pc.any(mask).as_py())


def _staging_keys(out: Path, replace_symbols: dict[str, set[str]]) -> set[PartKey]:
    keys: set[PartKey] = set()
    for freq, symbols in replace_symbols.items():
        for symbol in symbols:
            path = out / "_staging" / f"freq={freq}" / f"symbol={symbol}.parquet"
            if not path.exists():
                continue
            column = pq.read_table(path, columns=["trade_date"]).column("trade_date")
            for day in column.to_pylist():
                token = str(day.year) if freq == "1d" else day.isoformat()
                keys.add(PartKey(freq, token))
    return keys


def _read_replace_slice(out: Path, symbols: set[str], key: PartKey):
    from tdx_data.store import PLAIN_SCHEMA

    if not symbols:
        return PLAIN_SCHEMA.empty_table()
    files = []
    for symbol in sorted(symbols):
        path = out / "_staging" / f"freq={key.freq}" / f"symbol={symbol}.parquet"
        if path.exists():
            files.append(path)
    if not files:
        return PLAIN_SCHEMA.empty_table()
    if key.freq == "1d":
        return _read_many(files, *_year_filter(int(key.token)))
    day = date.fromisoformat(key.token)
    return _read_many(files, ("trade_date", "==", day))
