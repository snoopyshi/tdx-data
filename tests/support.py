"""拼测试用的 vipdoc 二进制。"""

import sqlite3
import struct
from datetime import date
from pathlib import Path

FREQ_LAYOUT = {
    "1d": ("lday", ".day"),
    "5m": ("fzline", ".lc5"),
    "1m": ("minline", ".lc1"),
}


def pack_day(day: date, open_, high, low, close, amount, volume, reserved=1) -> bytes:
    ymd = day.year * 10000 + day.month * 100 + day.day
    return struct.pack("<IIIIIfII", ymd, open_, high, low, close, amount, volume, reserved)


def pack_minute(day: date, minute, open_, high, low, close, amount, volume, reserved=1) -> bytes:
    encoded = (day.year - 2004) * 2048 + day.month * 100 + day.day
    return struct.pack(
        "<HHfffffII",
        encoded,
        minute,
        open_,
        high,
        low,
        close,
        amount,
        volume,
        reserved,
    )


def write_source(root: Path, market: str, code: str, freq: str, payload: bytes) -> Path:
    folder, suffix = FREQ_LAYOUT[freq]
    path = root / market / folder / f"{market}{code}{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def manifest(out: Path) -> list[tuple]:
    connection = sqlite3.connect(out / "manifest.sqlite")
    try:
        return connection.execute(
            "SELECT relpath, size, valid_rows FROM source_file ORDER BY relpath"
        ).fetchall()
    finally:
        connection.close()
