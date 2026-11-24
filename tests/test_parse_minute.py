import struct
from datetime import date, datetime

import pytest

from tdx_data.classify import classify
from tdx_data.normalize import iter_bars

DAY = date(2024, 11, 22)
STOCK = classify("sh", "600000")


def encode_date(day: date) -> int:
    return (day.year - 2004) * 2048 + day.month * 100 + day.day


def pack_minute(day, minute, open_, high, low, close, amount, volume, reserved=3) -> bytes:
    return struct.pack(
        "<HHfffffII",
        encode_date(day),
        minute,
        open_,
        high,
        low,
        close,
        amount,
        volume,
        reserved,
    )


def test_clock_and_float32_price(tmp_path):
    path = tmp_path / "a.lc1"
    path.write_bytes(pack_minute(DAY, 571, 10.5, 10.6, 10.4, 10.55, 1000.0, 100))
    bar = list(iter_bars(path, STOCK, "1m"))[0]
    assert bar.datetime == datetime(2024, 11, 22, 9, 31)
    assert bar.trade_date == DAY
    expected = struct.unpack("<f", struct.pack("<f", 10.5))[0]
    assert bar.open == expected
    path.write_bytes(pack_minute(DAY, 900, 10.5, 10.5, 10.5, 10.5, 1.0, 1))
    assert list(iter_bars(path, STOCK, "1m"))[0].datetime == datetime(2024, 11, 22, 15, 0)


def test_epoch_date(tmp_path):
    path = tmp_path / "a.lc1"
    path.write_bytes(pack_minute(date(2004, 1, 1), 571, 1, 1, 1, 1, 1,0, 1))
    assert list(iter_bars(path, STOCK, "1m"))[0].trade_date == date(2004, 1, 1)


@pytest.mark.parametrize(("month", "day_of_month"), [(13, 1), (1, 0), (1, 32)])
def test_invalid_calendar_date_dropped(tmp_path, month, day_of_month):
    encoded = (2024 - 2004) * 2048 + month * 100 + day_of_month
    chunk = struct.pack("<HHfffffII", encoded, 571, 1, 1, 1, 1, 1.0, 1, 0)
    path = tmp_path / "a.lc1"
    path.write_bytes(chunk)
    assert list(iter_bars(path, STOCK, "1m")) == []


@pytest.mark.parametrize(
    ("freq", "minute", "volume", "amount", "kept"),
    [
        ("1m", 899, 0, 0, False),
        ("1m", 898, 0, 0, False),
        ("1m", 898, 5, 0, True),
        ("1m", 897, 0, 0, True),
        ("5m", 899, 0, 0, True),
    ],
)
def test_placeholder_rules(tmp_path, freq, minute, volume, amount, kept):
    path = tmp_path / ("a.lc1" if freq == "1m" else "a.lc5")
    path.write_bytes(pack_minute(DAY, minute, 10.5, 10.5, 10.5, 10.5, amount, volume))
    bars = list(iter_bars(path, STOCK, freq))
    assert (len(bars) == 1) is kept


def test_blank_ohlc_and_inverted_range(tmp_path):
    path = tmp_path / "a.lc5"
    path.write_bytes(
        pack_minute(DAY, 571, 0, 0, 0, 0, 1, 1)
        + pack_minute(DAY, 576, 10, 9, 11, 10, 1, 1)
    )
    bars = list(iter_bars(path, STOCK, "5m"))
    assert len(bars) == 1
    assert bars[0].high == 9
    assert bars[0].low == 11
