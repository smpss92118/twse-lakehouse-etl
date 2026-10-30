"""Audit checks: pass on real data, catch the corruptions they exist for."""

from __future__ import annotations

from datetime import date

import polars as pl
from conftest import load_fixture

from twse_etl import transform
from twse_etl.validate import audit_cross_table, audit_institutional, audit_price_continuity, audit_quotes

D = date(2026, 10, 29)


def _failed(checks) -> set[str]:
    return {c.name for c in checks if not c.passed}


def test_real_day_passes_every_check():
    quotes = transform.parse_quotes(load_fixture("MI_INDEX", D), D)
    inst = transform.parse_institutional(load_fixture("T86", D), D)
    assert _failed(audit_quotes(quotes, D) + audit_institutional(inst, D) + audit_cross_table(quotes, inst)) == set()


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
