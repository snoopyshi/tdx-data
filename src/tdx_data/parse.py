"""按 32 字节一条流式读取通达信日线和分钟线。"""

import struct
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

RECORD_SIZE = 32
_DAY = struct.Struct("<IIIIIfII")
_MINUTE = struct.Struct("<HHfffffII")


@dataclass(frozen=True)
class RawBar:
    """一条已按字节布局解开、尚未做品种过滤的记录。日期非法时 trade_date 为 None。"""

    trade_date: date | None
    when: datetime | None
    minute: int | None
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float


def iter_chunks(path: Path, offset: int = 0):
    """从 offset 起每次产出 32 字节。末尾不足 32 字节的半条丢掉。"""
    if offset < 0 or offset % RECORD_SIZE != 0:
        raise ValueError(f"偏移必须是 {RECORD_SIZE} 的非负倍数: {offset}")
    with path.open("rb") as handle:
        if offset:
            handle.seek(offset)
        while True:
            chunk = handle.read(RECORD_SIZE)
            if len(chunk) < RECORD_SIZE:
                return
            yield chunk


def decode_yyyymmdd(value: int) -> date | None:
    year, month_day = divmod(value, 10000)
    month, day = divmod(month_day, 100)
    try:
        return date(year, month, day)
    except ValueError:
        return None


def decode_minute_date(value: int) -> date | None:
    year = value // 2048 + 2004
    month = (value % 2048) // 100
    day = (value % 2048) % 100
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _clock(minute: int) -> time | None:
    hour, minute_of_hour = divmod(minute, 60)
    if hour > 23 or minute_of_hour > 59:
        return None
    return time(hour, minute_of_hour)


def parse_day_chunk(chunk: bytes) -> RawBar:
    date_n, open_, high, low, close, amount, volume, _reserved = _DAY.unpack(chunk)
    trade_date = decode_yyyymmdd(date_n)
    when = datetime.combine(trade_date, time(15, 0)) if trade_date else None
    return RawBar(
        trade_date=trade_date,
        when=when,
        minute=None,
        open=open_ / 100,
        high=high / 100,
        low=low / 100,
        close=close / 100,
        volume=volume,
        amount=float(amount),
    )


def parse_minute_chunk(chunk: bytes) -> RawBar:
    encoded, minute, open_, high, low, close, amount, volume, _reserved = _MINUTE.unpack(chunk)
    trade_date = decode_minute_date(encoded)
    clock = _clock(minute)
    when = datetime.combine(trade_date, clock) if trade_date and clock else None
    if trade_date and clock is None:
        trade_date = None
    return RawBar(
        trade_date=trade_date,
        when=when,
        minute=minute,
        open=float(open_),
        high=float(high),
        low=float(low),
        close=float(close),
        volume=volume,
        amount=float(amount),
    )


def iter_day_records(path: Path, offset: int = 0):
    for chunk in iter_chunks(path, offset):
        yield parse_day_chunk(chunk)


def iter_minute_records(path: Path, offset: int = 0):
    for chunk in iter_chunks(path, offset):
        yield parse_minute_chunk(chunk)


def read_tail(path: Path, size: int) -> bytes:
    """读取位于 size-32 的那一条记录。size 必须是 32 的正倍数。"""
    if size < RECORD_SIZE or size % RECORD_SIZE != 0:
        raise ValueError(f"无法读取尾部记录: {size}")
    with path.open("rb") as handle:
        handle.seek(size - RECORD_SIZE)
        tail = handle.read(RECORD_SIZE)
    if len(tail) != RECORD_SIZE:
        raise ValueError(f"尾部记录不完整: {path}")
    return tail
