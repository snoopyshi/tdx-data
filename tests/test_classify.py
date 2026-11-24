from datetime import date

import pytest

from tdx_data.classify import classify

STOCKS = [
    "sh600000",
    "sh601398",
    "sh603259",
    "sh605117",
    "sh688981",
    "sh689009",
    "sz000001",
    "sz001979",
    "sz002594",
    "sz003816",
    "sz300750",
    "sz301269",
    "bj920001",
    "bj430047",
    "bj830000",
    "bj870000",
    "bj820000",
]
INDEXES = [
    "sh000001",
    "sh000300",
    "sh000688",
    "sz399001",
    "sz399006",
    "bj899050",
]
DROPPED = [
    "sh510050",
    "sh113050",
    "sh900901",
    "sh880001",
    "sz159915",
    "sz127001",
    "sz200011",
]


def _split(token: str) -> tuple[str, str]:
    return token[:2], token[2:]


@pytest.mark.parametrize("token", STOCKS)
def test_stocks(token):
    market, code = _split(token)
    got = classify(market, code)
    assert got is not None
    assert got.security_type == "stock"
    assert got.volume_unit == "share"
    assert got.symbol == f"{code}.{market.upper()}"


@pytest.mark.parametrize("token", INDEXES)
def test_indexes(token):
    market, code = _split(token)
    got = classify(market, code)
    assert got is not None
    assert got.security_type == "index"
    assert got.volume_unit == "lot"
    assert got.symbol == f"{code}.{market.upper()}"


@pytest.mark.parametrize("token", DROPPED)
def test_dropped(token):
    market, code = _split(token)
    assert classify(market, code) is None


def test_rejects_bad_code_shape():
    assert classify("sh", "60000") is None
    assert classify("hk", "600000") is None
    assert classify("sh", "60000A") is None
