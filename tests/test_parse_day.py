import struct
from datetime import date, datetime

from tdx_data.classify import classify
from tdx_data.normalize import iter_bars
from tdx_data.parse import iter_day_records

DAY = date(2024, 11, 22)
NEXT = date(2024, 11, 25)


def pack_day(day, open_, high, low, close, amount, volume, reserved=7) -> bytes:
    ymd = day.year * 10000 + day.month * 100 + day.day
    return struct.pack("<IIIIIfII", ymd, open_, high, low, close, amount, volume, reserved)


def test_day_sample(tmp_path):
    path = tmp_path / "sh600000.day"
    path.write_bytes(pack_day(DAY, 1050, 1080, 1040, 1075, 1234567.5, 10000, reserved=9))
    bars = list(iter_bars(path, classify("sh", "600000"), "1d"))
    assert len(bars) == 1
    bar = bars[0]
    assert bar.trade_date == DAY
    assert bar.datetime == datetime(2024, 11, 22, 15, 0)
    assert round(bar.open * 100) == 1050
    assert round(bar.high * 100) == 1080
    assert round(bar.low * 100) == 1040
    assert round(bar.close * 100) == 1075
    assert bar.open == 1050 / 100
    assert bar.amount == struct.unpack("<f", struct.pack("<f", 1234567.5))[0]
    assert bar.volume == 10000
    assert bar.volume_unit == "share"
    assert not hasattr(bar, "reserved")


def test_two_days_keep_order(tmp_path):
    path = tmp_path / "sh600000.day"
    path.write_bytes(
        pack_day(DAY, 1050, 1080, 1040, 1075, 1.0, 10)
        + pack_day(NEXT, 1100, 1110, 1090, 1105, 2.0, 20)
    )
    bars = list(iter_bars(path, classify("sh", "600000"), "1d"))
    assert [bar.trade_date for bar in bars] == [DAY, NEXT]
    assert bars[1].volume == 20


def test_blank_ohlc_dropped(tmp_path):
    path = tmp_path / "a.day"
    path.write_bytes(pack_day(DAY, 0, 0, 0, 0, 5.0, 8))
    assert list(iter_day_records(path))[0].volume == 8
    assert list(iter_bars(path, classify("sh", "600000"), "1d")) == []


def test_high_below_low_kept(tmp_path):
    path = tmp_path / "a.day"
    path.write_bytes(pack_day(DAY, 1000, 900, 1100, 1000, 1.0, 1))
    bar = list(iter_bars(path, classify("sh", "600000"), "1d"))[0]
    assert bar.high == 9
    assert bar.low == 11


def test_invalid_date_dropped(tmp_path):
    path = tmp_path / "a.day"
    path.write_bytes(struct.pack("<IIIIIfII", 20241301, 100, 100, 100, 100, 1.0, 1, 0))
    assert list(iter_bars(path, classify("sh", "600000"), "1d")) == []


def test_truncated_tail_ignored(tmp_path):
    path = tmp_path / "a.day"
    path.write_bytes(pack_day(DAY, 1050, 1080, 1040, 1075, 1.0, 10000) + b"\x01\x02")
    bars = list(iter_bars(path, classify("sh", "600000"), "1d"))
    assert len(bars) == 1
    assert bars[0].volume == 10000


def test_index_volume_not_rescaled(tmp_path):
    path = tmp_path / "sh000001.day"
    path.write_bytes(pack_day(DAY, 300000, 301000, 299000, 300500, 50.0, 10000))
    bar = list(iter_bars(path, classify("sh", "000001"), "1d"))[0]
    assert bar.symbol == "000001.SH"
    assert bar.volume == 10000
    assert bar.volume_unit == "lot"
