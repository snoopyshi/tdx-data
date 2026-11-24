import hashlib
import shutil
from datetime import date

import pyarrow.parquet as pq

from tdx_data.cli import main
from tdx_data.ingest import IngestHooks, ingest
from tests.support import manifest, pack_day, pack_minute, write_source

DAY = date(2024, 11, 22)
NEXT = date(2024, 11, 25)


class Stop(Exception):
    pass


def _bar_day(day, close=1075, volume=10000, reserved=1):
    return pack_day(day, 1050, 1080, 1040, close, 1000.0, volume, reserved)


def _bar_minute(day, minute=571, close=10.5, volume=100, reserved=1):
    return pack_minute(day, minute, 10.5, 10.6, 10.4, close, 1000.0, volume, reserved)


def build(root):
    """两只股票、一只指数，三个周期各一根；另放一只 ETF。"""
    symbols = (("sh", "600000"), ("sz", "000001"), ("sh", "000001"))
    for market, code in symbols:
        write_source(root, market, code, "1d", _bar_day(DAY))
        write_source(root, market, code, "1m", _bar_minute(DAY))
        write_source(root, market, code, "5m", _bar_minute(DAY, minute=575))
    write_source(root, "sh", "510050", "1d", _bar_day(DAY))
    return root


def _rows(path):
    return pq.read_table(path).sort_by([("symbol", "ascending"), ("datetime", "ascending")]).to_pylist()


def _part(out, freq, token):
    if freq == "1d":
        return out / "bars" / "freq=1d" / f"year={token}" / "part-0.parquet"
    return out / "bars" / f"freq={freq}" / f"date={token}" / "part-0.parquet"


def test_first_import_layout(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    assert ingest(vipdoc, out) == 0
    daily = _part(out, "1d", "2024")
    minute = _part(out, "1m", DAY.isoformat())
    five = _part(out, "5m", DAY.isoformat())
    assert daily.is_file() and minute.is_file() and five.is_file()
    assert not (out / "_staging").exists()
    rows = _rows(minute)
    assert [row["symbol"] for row in rows] == ["000001.SH", "000001.SZ", "600000.SH"]
    assert all(row["symbol"] != "510050.SH" for row in _rows(daily))
    assert len(manifest(out)) == 9
    parquet = pq.ParquetFile(minute)
    symbol_col = parquet.metadata.row_group(0).column(0)
    unit_col = parquet.metadata.row_group(0).column(9)
    assert symbol_col.path_in_schema == "symbol"
    assert symbol_col.has_dictionary_page
    assert unit_col.has_dictionary_page
    assert "zstd" in str(symbol_col.compression).lower()
    assert symbol_col.bloom_filter_offset is not None


def test_append_one_symbol_new_minute_day(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    assert ingest(vipdoc, out) == 0
    before = len(_rows(_part(out, "1m", DAY.isoformat())))
    path = vipdoc / "sh" / "minline" / "sh600000.lc1"
    path.write_bytes(path.read_bytes() + _bar_minute(NEXT))
    assert ingest(vipdoc, out) == 0
    fresh = _rows(_part(out, "1m", NEXT.isoformat()))
    assert [row["symbol"] for row in fresh] == ["600000.SH"]
    assert len(_rows(_part(out, "1m", DAY.isoformat()))) == before


def test_append_all_symbols_same_day_is_sorted(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    ingest(vipdoc, out)
    for market, code in (("sh", "600000"), ("sz", "000001"), ("sh", "000001")):
        path = vipdoc / market / "minline" / f"{market}{code}.lc1"
        path.write_bytes(path.read_bytes() + _bar_minute(NEXT, volume=7))
    assert ingest(vipdoc, out) == 0
    assert [row["symbol"] for row in _rows(_part(out, "1m", NEXT.isoformat()))] == [
        "000001.SH",
        "000001.SZ",
        "600000.SH",
    ]


def test_same_year_daily_append_keeps_earlier_bars(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    ingest(vipdoc, out)
    path = vipdoc / "sh" / "lday" / "sh600000.day"
    path.write_bytes(path.read_bytes() + _bar_day(NEXT, close=1200, volume=50))
    assert ingest(vipdoc, out) == 0
    rows = _rows(_part(out, "1d", "2024"))
    day1 = [row for row in rows if row["trade_date"] == DAY]
    day2 = [row for row in rows if row["trade_date"] == NEXT]
    assert len(day1) == 3
    assert [row["symbol"] for row in day2] == ["600000.SH"]
    assert round(day2[0]["close"] * 100) == 1200


def test_same_day_minute_append_keeps_morning(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    ingest(vipdoc, out)
    path = vipdoc / "sh" / "minline" / "sh600000.lc1"
    path.write_bytes(path.read_bytes() + _bar_minute(DAY, minute=600, close=11.0, volume=8))
    assert ingest(vipdoc, out) == 0
    rows = [row for row in _rows(_part(out, "1m", DAY.isoformat())) if row["symbol"] == "600000.SH"]
    assert [row["datetime"].hour * 60 + row["datetime"].minute for row in rows] == [571, 600]
    assert rows[0]["volume"] == 100
    assert rows[1]["volume"] == 8


def test_rewrite_when_tail_changes(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    path = vipdoc / "sh" / "lday" / "sh600000.day"
    path.write_bytes(_bar_day(DAY, close=1075) + _bar_day(NEXT, close=1100, reserved=4))
    ingest(vipdoc, out)
    # 大小不变，但最后 32 字节变了，所以整文件重解析。
    path.write_bytes(_bar_day(DAY, close=1300) + _bar_day(NEXT, close=1100, reserved=9))
    assert ingest(vipdoc, out) == 0
    rows = [row for row in _rows(_part(out, "1d", "2024")) if row["symbol"] == "600000.SH"]
    assert [row["trade_date"] for row in rows] == [DAY, NEXT]
    assert round(rows[0]["close"] * 100) == 1300
    assert len(rows) == 2


def test_middle_edit_with_same_tail_is_not_detected(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    path = vipdoc / "sh" / "lday" / "sh600000.day"
    original = _bar_day(DAY, close=1075) + _bar_day(NEXT, close=1100, reserved=4)
    path.write_bytes(original)
    ingest(vipdoc, out)
    path.write_bytes(_bar_day(DAY, close=1300) + original[32:])
    assert ingest(vipdoc, out) == 0
    rows = [row for row in _rows(_part(out, "1d", "2024")) if row["symbol"] == "600000.SH"]
    assert round(rows[0]["close"] * 100) == 1075


def test_second_run_does_not_rewrite_parquet(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    ingest(vipdoc, out)
    target = _part(out, "1d", "2024")
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    assert ingest(vipdoc, out) == 0
    assert hashlib.sha256(target.read_bytes()).hexdigest() == digest


def test_truncated_file_does_not_advance_watermark(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    path = vipdoc / "sh" / "lday" / "sh600000.day"
    path.write_bytes(path.read_bytes() + b"\x01\x02\x03")
    out = tmp_path / "out"
    assert ingest(vipdoc, out) == 0
    relpaths = [row[0] for row in manifest(out)]
    assert "sh/lday/sh600000.day" not in relpaths
    assert any(row["symbol"] == "600000.SH" for row in _rows(_part(out, "1d", "2024")))


def test_crash_before_replace_then_rerun(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    ingest(vipdoc, out)
    path = vipdoc / "sh" / "minline" / "sh600000.lc1"
    path.write_bytes(path.read_bytes() + _bar_minute(NEXT))

    def boom(_tmp):
        raise Stop()

    try:
        ingest(vipdoc, out, hooks=IngestHooks(after_tmp=boom))
    except Stop:
        pass
    else:
        raise AssertionError("应当在替换前停下")
    assert list(out.rglob("part-0.parquet.tmp"))
    assert not _part(out, "1m", NEXT.isoformat()).exists()
    assert ingest(vipdoc, out) == 0
    assert not list(out.rglob("part-0.parquet.tmp"))
    assert _rows(_part(out, "1m", NEXT.isoformat()))[0]["symbol"] == "600000.SH"


def test_crash_before_manifest_is_idempotent(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    ingest(vipdoc, out)
    before = manifest(out)
    path = vipdoc / "sh" / "minline" / "sh600000.lc1"
    path.write_bytes(path.read_bytes() + _bar_minute(NEXT, volume=42))

    def boom():
        raise Stop()

    try:
        ingest(vipdoc, out, hooks=IngestHooks(before_manifest=boom))
    except Stop:
        pass
    else:
        raise AssertionError("应当在写入 manifest 前停下")
    assert manifest(out) == before
    assert _part(out, "1m", NEXT.isoformat()).is_file()
    assert ingest(vipdoc, out) == 0
    rows = _rows(_part(out, "1m", NEXT.isoformat()))
    assert len(rows) == 1
    assert rows[0]["volume"] == 42


def test_unreadable_source_does_not_block_others(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    bad = vipdoc / "sh" / "lday" / "sh601398.day"
    bad.mkdir(parents=True)
    out = tmp_path / "out"
    assert ingest(vipdoc, out) == 1
    symbols = {row["symbol"] for row in _rows(_part(out, "1d", "2024"))}
    assert "601398.SH" not in symbols
    assert "600000.SH" in symbols
    assert all("601398" not in relpath for relpath, _size, _rows_n in manifest(out))


def test_empty_vipdoc_exits_zero(tmp_path):
    vipdoc = tmp_path / "vipdoc"
    vipdoc.mkdir()
    assert ingest(vipdoc, tmp_path / "out") == 0


def test_missing_vipdoc_and_bad_freq(tmp_path):
    assert ingest(tmp_path / "missing", tmp_path / "out") == 1
    vipdoc = build(tmp_path / "vipdoc")
    assert ingest(vipdoc, tmp_path / "out2", freq="2d") == 2


def test_cli_missing_args_and_success(tmp_path):
    try:
        main(["ingest"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("缺少参数应当退出")
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    try:
        main(["ingest", "--vipdoc", str(vipdoc), "--out", str(out)])
    except SystemExit as exc:
        assert exc.code == 0
    else:
        raise AssertionError("成功导入应当以 0 退出")


def test_lock_is_exclusive(tmp_path):
    import fcntl

    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    out.mkdir()
    handle = (out / ".ingest.lock").open("a+")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert ingest(vipdoc, out) == 1
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
    shutil.rmtree(out)
