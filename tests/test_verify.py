import pyarrow.parquet as pq

from tdx_data.cli import main
from tdx_data.ingest import ingest
from tdx_data.verify import verify
from tests.support import pack_day, pack_minute, write_source
from tests.test_ingest import DAY, build


def test_verify_clean_fixture(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    assert ingest(vipdoc, out) == 0
    assert verify(vipdoc, out, ["600000.SH", "000001.SH"]) == 0


def test_verify_reports_row_loss(tmp_path, capsys):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    ingest(vipdoc, out)
    path = out / "bars" / "freq=1m" / f"date={DAY.isoformat()}" / "part-0.parquet"
    table = pq.read_table(path)
    pq.write_table(table.slice(0, 1), path)
    assert verify(vipdoc, out) == 1
    error = capsys.readouterr().err
    assert "600000.SH" in error or "000001" in error


def test_cli_verify(tmp_path):
    vipdoc = build(tmp_path / "vipdoc")
    out = tmp_path / "out"
    ingest(vipdoc, out)
    try:
        main(["verify", "--vipdoc", str(vipdoc), "--out", str(out), "--symbol", "600000.SH"])
    except SystemExit as exc:
        assert exc.code == 0
    else:
        raise AssertionError("对账成功应当以 0 退出")


def test_volume_gap_is_not_a_failure(tmp_path, capsys):
    vipdoc = tmp_path / "vipdoc"
    write_source(vipdoc, "sh", "600000", "1d", pack_day(DAY, 1000, 1000, 1000, 1000, 1.0, 10))
    write_source(
        vipdoc,
        "sh",
        "600000",
        "1m",
        pack_minute(DAY, 571, 10, 10, 10, 10, 1.0, 4) + pack_minute(DAY, 572, 10, 10, 10, 10, 1.0, 4),
    )
    out = tmp_path / "out"
    assert ingest(vipdoc, out, freq="1d,1m") == 0
    assert verify(vipdoc, out) == 0
    assert "成交量差额" in capsys.readouterr().out
