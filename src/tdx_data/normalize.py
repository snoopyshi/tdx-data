"""把原始记录收成统一的 Bar，并丢掉占位和空记录。"""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from tdx_data.classify import Security
from tdx_data.parse import RawBar, iter_day_records, iter_minute_records

_LC1_PLACEHOLDER_MINUTE = 898


@dataclass(frozen=True)
class Bar:
    symbol: str
    security_type: str
    trade_date: date
    datetime: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    volume_unit: str
    amount: float


def ohlc_blank(raw: RawBar) -> bool:
    return raw.open == 0 and raw.high == 0 and raw.low == 0 and raw.close == 0


def is_lc1_placeholder(raw: RawBar) -> bool:
    return (
        raw.minute is not None
        and raw.minute >= _LC1_PLACEHOLDER_MINUTE
        and raw.volume == 0
        and raw.amount == 0
    )


def to_bar(raw: RawBar, security: Security, freq: str) -> Bar | None:
    if raw.trade_date is None or raw.when is None or ohlc_blank(raw):
        return None
    if freq == "1m" and is_lc1_placeholder(raw):
        return None
    return Bar(
        symbol=security.symbol,
        security_type=security.security_type,
        trade_date=raw.trade_date,
        datetime=raw.when,
        open=raw.open,
        high=raw.high,
        low=raw.low,
        close=raw.close,
        volume=raw.volume,
        volume_unit=security.volume_unit,
        amount=raw.amount,
    )


def iter_bars(path: Path, security: Security, freq: str, offset: int = 0):
    records = iter_day_records(path, offset) if freq == "1d" else iter_minute_records(path, offset)
    for raw in records:
        bar = to_bar(raw, security, freq)
        if bar is not None:
            yield bar
