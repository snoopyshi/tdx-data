"""命令行入口。"""

from __future__ import annotations

import argparse
from pathlib import Path

from tdx_data.ingest import ingest
from tdx_data.verify import verify


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="tdx-import")
    commands = parser.add_subparsers(dest="command", required=True)

    ingest_parser = commands.add_parser("ingest", help="把 vipdoc K 线写入 Parquet")
    ingest_parser.add_argument("--vipdoc", required=True, type=Path)
    ingest_parser.add_argument("--out", required=True, type=Path)
    ingest_parser.add_argument("--freq", default="1d,5m,1m")

    verify_parser = commands.add_parser("verify", help="用源文件对账 Parquet")
    verify_parser.add_argument("--vipdoc", required=True, type=Path)
    verify_parser.add_argument("--out", required=True, type=Path)
    verify_parser.add_argument("--symbol", default="", help="逗号分隔，额外逐行比较这些代码")

    args = parser.parse_args(argv)
    if args.command == "ingest":
        code = ingest(args.vipdoc, args.out, args.freq)
    else:
        symbols = [item.strip() for item in args.symbol.split(",") if item.strip()]
        code = verify(args.vipdoc, args.out, symbols or None)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
