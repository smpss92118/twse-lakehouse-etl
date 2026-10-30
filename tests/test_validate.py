"""Audit checks: pass on real data, catch the corruptions they exist for."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from functools import partial

import polars as pl
import pytest
from conftest import load_fixture

from twse_etl import contracts, corporate_actions, transform
from twse_etl.config import REPO_ROOT
from twse_etl.validate import audit_cross_table, audit_institutional, audit_price_continuity, audit_quotes

D = date(2026, 10, 29)
PREV = date(2026, 10, 28)


def _failed(checks) -> set[str]:
    return {c.name for c in checks if not c.passed}


@pytest.fixture(scope="module")
def real_day() -> dict[str, pl.DataFrame]:
    """2026-10-29 as TWSE published it, with 10-28 as the previous trading day."""
    actions = transform.parse_corporate_actions(load_fixture("TWT49U", D))
    vendor = corporate_actions.load_vendor(REPO_ROOT / "fixtures" / "vendor", PREV + timedelta(days=1), D)
    return {
        "quotes": transform.parse_quotes(load_fixture("MI_INDEX", D), D),
        "inst": transform.parse_institutional(load_fixture("T86", D), D),
        "prev_close": transform.parse_quotes(load_fixture("MI_INDEX", PREV), PREV).select("stock_id", "close"),
        "actions": corporate_actions.attach_vendor(actions, vendor),
    }


def _every_check(quotes, inst, prev_close, actions):
    return (
        audit_quotes(quotes, D)
        + audit_price_continuity(quotes, prev_close, actions)
        + audit_institutional(inst, D)
        + audit_cross_table(quotes, inst)
        + corporate_actions.audit_corporate_actions(actions, quotes, prev_close, D)
    )


def _set(df: pl.DataFrame, stock: str, **values) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("stock_id") == stock).then(pl.lit(v, dtype=df.schema[col])).otherwise(pl.col(col)).alias(col)
        for col, v in values.items()
    )


def _bump(df: pl.DataFrame, stock: str, col: str, delta) -> pl.DataFrame:
    shifted = (pl.col(col) + delta).cast(df.schema[col])
    return df.with_columns(pl.when(pl.col("stock_id") == stock).then(shifted).otherwise(pl.col(col)).alias(col))


def _orphan(inst: pl.DataFrame) -> pl.DataFrame:
    return pl.concat([inst, inst.head(1).with_columns(pl.lit("9999").alias("stock_id"))])


# One corruption per blocking check, applied to real data: (check that must fail, frame, corruption).
# 2330 on 10-29: open 2420, high 2425, low 2390, close 2415, sign "-", change -10.00.
CORRUPTIONS = [
    ("quotes_min_rows", "quotes", lambda q: q.head(800)),
    ("quotes_trade_date", "quotes", partial(_set, stock="2330", trade_date=PREV)),
    ("quotes_unique_key", "quotes", lambda q: pl.concat([q, q.head(1)])),
    ("quotes_ohlc_bounds", "quotes", partial(_set, stock="2330", high=Decimal("2400"))),
    ("quotes_ohlc_bounds", "quotes", partial(_set, stock="2330", open=None)),
    ("quotes_close_implies_volume", "quotes", partial(_set, stock="2330", volume_shares=0)),
    ("quotes_change_sign_known", "quotes", partial(_set, stock="2330", change_sign="?")),
    ("quotes_change_sign_known", "quotes", partial(_set, stock="2330", change_sign=None)),
    ("quotes_sign_matches_change", "quotes", partial(_set, stock="2330", change_sign="+")),
    ("quotes_sign_matches_change", "quotes", partial(_set, stock="2330", price_change=None)),
    # Close moved by 5 but the change did not: still inside high/low, only the day-over-day check sees it.
    ("quotes_price_continuity", "quotes", partial(_bump, stock="2330", col="close", delta=5)),
    ("institutional_min_rows", "inst", lambda i: i.head(400)),
    ("institutional_trade_date", "inst", partial(_set, stock="2330", trade_date=PREV)),
    ("institutional_unique_key", "inst", lambda i: pl.concat([i, i.head(1)])),
    *[
        (f"institutional_{g}_buy_minus_sell", "inst", partial(_bump, stock="2330", col=f"{g}_buy", delta=1))
        for g in contracts.INSTITUTIONAL_GROUPS
    ],
    ("institutional_foreign_buy_minus_sell", "inst", partial(_set, stock="2330", foreign_net=None)),
    ("institutional_dealer_split", "inst", partial(_bump, stock="2330", col="dealer_net", delta=1)),
    ("institutional_total_net", "inst", partial(_bump, stock="2330", col="total_net", delta=1)),
    ("institutional_ids_in_quotes", "inst", _orphan),
    ("institutional_coverage", "inst", lambda i: i.head(800)),
    ("ca_adj_factor_range", "actions", lambda a: a.with_columns(pl.lit(1.5).alias("adj_factor"))),
]


def test_real_day_passes_every_check(real_day):
    assert _failed(_every_check(**real_day)) == set()


@pytest.mark.parametrize(
    ("check", "frame", "corrupt"), CORRUPTIONS, ids=[f"{i:02d}-{c}" for i, (c, _, _) in enumerate(CORRUPTIONS)]
)
def test_each_blocking_check_catches_its_corruption(real_day, check, frame, corrupt):
    frames = {**real_day, frame: corrupt(real_day[frame])}
    assert check in _failed(_every_check(**frames))


def test_duplicates_and_wrong_sign_are_caught():
    quotes = transform.parse_quotes(load_fixture("MI_INDEX", D), D)
    corrupted = pl.concat([quotes, quotes.head(1)]).with_columns(
        pl.when(pl.col("stock_id") == "2330").then(pl.lit("+")).otherwise(pl.col("change_sign")).alias("change_sign")
    )
    assert {"quotes_unique_key", "quotes_sign_matches_change"} <= _failed(audit_quotes(corrupted, D))


def test_partial_institutional_upload_is_blocked_before_merge():
    """Scenario 2 (merged table 800 rows vs 1,000+ quotes) is stopped at the audit gate."""
    quotes = transform.parse_quotes(load_fixture("MI_INDEX", D), D)
    inst = transform.parse_institutional(load_fixture("T86", D), D).head(800)
    assert "institutional_coverage" in _failed(audit_cross_table(quotes, inst))


def test_price_continuity_needs_events_dated_on_the_typhoon_day():
    """10-23 was a typhoon closure. Ex-dividend events dated 10-23 take effect on 10-27, where TWSE
    measures the change from the opening reference price instead of the 10-22 close."""
    prev = transform.parse_quotes(load_fixture("MI_INDEX", date(2026, 10, 22)), date(2026, 10, 22))
    today = transform.parse_quotes(load_fixture("MI_INDEX", date(2026, 10, 27)), date(2026, 10, 27))
    window = pl.concat([transform.parse_corporate_actions(load_fixture("TWT49U", date(2026, 10, d))) for d in (23, 27)])
    prev_close = prev.select("stock_id", "close")

    assert _failed(audit_price_continuity(today, prev_close, window)) == set()
    naive = audit_price_continuity(today, prev_close, window.clear())
    assert _failed(naive) == {"quotes_price_continuity"}
    assert "2028" in naive[0].detail and "6931" in naive[0].detail
