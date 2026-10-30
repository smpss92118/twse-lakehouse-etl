"""Cleaning logic on real recorded TWSE payloads: English columns, types, thousands separators, signs."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import polars as pl
import pytest
from conftest import load_fixture

from twse_etl import transform

D = date(2026, 10, 29)


@pytest.fixture(scope="module")
def quotes() -> pl.DataFrame:
    return transform.parse_quotes(load_fixture("MI_INDEX", D), D)


def test_quotes_types_and_thousands_separators(quotes):
    assert quotes.height == 1388
    assert quotes.schema["close"] == pl.Decimal(12, 2)
    assert quotes.schema["volume_shares"] == pl.Int64
    tsmc = quotes.filter(pl.col("stock_id") == "2330").row(0, named=True)
    assert tsmc["volume_shares"] == 18_403_916  # "18,403,916" in the payload
    assert tsmc["close"] == Decimal("2415.00")
    assert quotes.filter(pl.col("stock_id") == "0050").height == 1  # leading zero kept


def test_change_sign_is_parsed_from_html_and_applied(quotes):
    assert set(quotes["change_sign"].unique()) == {"+", "-", "0", "X"}
    down = quotes.filter(pl.col("change_sign") == "-")
    assert (down["price_change"] < 0).all()
    # "X" = not comparable (ex-rights etc.): TWSE prints 0.00, we store null instead of a fake 0.
    assert quotes.filter(pl.col("change_sign") == "X")["price_change"].is_null().all()


def test_no_trade_prices_are_null_not_zero(quotes):
    no_close = quotes.filter(pl.col("close").is_null())
    assert no_close.height > 0
    assert no_close["open"].is_null().all()
    assert quotes.filter(pl.col("stock_id") == "0050")["pe_ratio"].is_null().all()  # 0.00 = no P/E


def test_institutional_columns_are_english_ints():
    df = transform.parse_institutional(load_fixture("T86", D), D)
    assert df.height == 1340
    assert "foreign_net" in df.columns and "三大法人買賣超股數" not in df.columns
    assert df["stock_name"].str.ends_with(" ").sum() == 0  # TWSE pads names with spaces
    assert all(df.schema[c] == pl.Int64 for c in df.columns[3:])


def test_renamed_twse_column_fails_loudly_instead_of_shifting_values():
    doc = load_fixture("T86", D)
    doc["fields"][4] = "外資買賣超股數"  # TWSE renames a column
    with pytest.raises(transform.SchemaDriftError, match="外陸資買賣超股數"):
        transform.parse_institutional(doc, D)


def test_typhoon_day_payload_is_not_published():
    doc = load_fixture("MI_INDEX", date(2026, 10, 23))
    assert not transform.is_complete("MI_INDEX", doc, date(2026, 10, 23))


def test_corporate_action_adj_factor():
    actions = transform.parse_corporate_actions(load_fixture("TWT49U", date(2026, 3, 17)))
    tsmc = actions.filter(pl.col("stock_id") == "2330").row(0, named=True)
    assert tsmc["trade_date"] == date(2026, 3, 17)  # ROC date "115年03月17日"
    assert tsmc["ref_price"] == Decimal("1838.99")
    assert tsmc["adj_factor"] == pytest.approx(float(tsmc["ref_price"]) / float(tsmc["before_close"]))
    assert tsmc["adj_factor"] < 1
