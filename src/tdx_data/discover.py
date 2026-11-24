"""遍历 vipdoc 里的日线和分钟线文件。"""

from dataclasses import dataclass
from pathlib import Path

from tdx_data.classify import Security, classify

MARKETS = ("sh", "sz", "bj")
FREQ_LAYOUT = {
    "1d": ("lday", ".day"),
    "5m": ("fzline", ".lc5"),
    "1m": ("minline", ".lc1"),
}


@dataclass(frozen=True)
class SourceFile:
    market: str
    code: str
    freq: str
    path: Path
    size: int
    relpath: str
    security: Security


def discover(vipdoc: Path, freqs: list[str]) -> tuple[list[SourceFile], list[tuple[Path, str]]]:
    """返回 (可解析文件, 名字匹配但读不了的文件及原因)。"""
    found: list[SourceFile] = []
    errors: list[tuple[Path, str]] = []
    for market in MARKETS:
        for freq in freqs:
            folder_name, suffix = FREQ_LAYOUT[freq]
            folder = vipdoc / market / folder_name
            if not folder.is_dir():
                continue
            for path in sorted(folder.iterdir()):
                name = path.name
                if not name.endswith(suffix):
                    continue
                stem = name[: -len(suffix)]
                if not (stem.startswith(market) and len(stem) == len(market) + 6):
                    continue
                code = stem[len(market) :]
                if not code.isdigit():
                    continue
                security = classify(market, code)
                if security is None:
                    continue
                relpath = path.relative_to(vipdoc).as_posix()
                if not path.is_file():
                    errors.append((path, "不是普通文件"))
                    continue
                try:
                    size = path.stat().st_size
                except OSError as exc:
                    errors.append((path, str(exc)))
                    continue
                found.append(
                    SourceFile(
                        market=market,
                        code=code,
                        freq=freq,
                        path=path,
                        size=size,
                        relpath=relpath,
                        security=security,
                    )
                )
    return found, errors
