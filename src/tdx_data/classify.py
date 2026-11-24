"""按代码规则区分股票和交易所指数。"""

from dataclasses import dataclass

_SUFFIX = {"sh": "SH", "sz": "SZ", "bj": "BJ"}

# 每个市场里，股票前缀和指数前缀互不重叠。匹配时先看更长的前缀。
_RULES: dict[str, tuple[tuple[tuple[str, ...], str], ...]] = {
    "sh": (
        (("600", "601", "603", "605", "688", "689"), "stock"),
        (("000",), "index"),
    ),
    "sz": (
        (("000", "001", "002", "003", "300", "301"), "stock"),
        (("399",), "index"),
    ),
    "bj": (
        (("920", "43", "82", "83", "87"), "stock"),
        (("899",), "index"),
    ),
}


@dataclass(frozen=True)
class Security:
    symbol: str
    security_type: str
    volume_unit: str


def classify(market: str, code: str) -> Security | None:
    """返回股票或指数。ETF、债券、B 股、板块指数等返回 None。"""
    rules = _RULES.get(market)
    if rules is None or len(code) != 6 or not code.isdigit():
        return None
    for prefixes, kind in rules:
        for prefix in sorted(prefixes, key=len, reverse=True):
            if code.startswith(prefix):
                unit = "share" if kind == "stock" else "lot"
                return Security(f"{code}.{_SUFFIX[market]}", kind, unit)
    return None


def parse_symbol(symbol: str) -> tuple[str, str] | None:
    """`600000.SH` → (`sh`, `600000`)。格式不对时返回 None。"""
    code, dot, suffix = symbol.partition(".")
    market = {value: key for key, value in _SUFFIX.items()}.get(suffix)
    if not dot or market is None or len(code) != 6 or not code.isdigit():
        return None
    return market, code
