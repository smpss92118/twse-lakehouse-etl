"""Corporate actions: every official reference price is reproduced from the event's own terms, and
adjusted prices are derived from it at query time without rewriting history."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import polars as pl
import pytest
from conftest import load_fixture, staged_actions

from twse_etl import adjust, corporate_actions, pipeline, transform
from twse_etl.validate import audit_price_continuity

# Every recorded window as (start, end): 92 cash dividends, a stock dividend and two rights issues,
# three capital reductions (TWTAUU) and the 0050 1:4 split (TWTCAU).
WINDOWS = [
    (date(2026, 3, 16), date(2026, 3, 16)),
    (date(2026, 3, 17), date(2026, 3, 17)),
    (date(2026, 10, 22), date(2026, 10, 22)),
    (date(2026, 10, 23), date(2026, 10, 27)),  # typhoon closure: 10-23 events take effect on 10-27
    (date(2026, 10, 28), date(2026, 10, 28)),
    (date(2026, 10, 29), date(2026, 10, 29)),
    (date(2025, 6, 10), date(2025, 6, 10)),
    (date(2025, 6, 17), date(2025, 6, 17)),
    (date(2025, 6, 18), date(2025, 6, 18)),
    (date(2025, 6, 23), date(2025, 6, 23)),
]
SPLIT_DAY = date(2025, 6, 18)  # 0050 resumes after its 1:4 split: 188.65 -> 47.16


@pytest.fixture(scope="module")
def recorded_events(tmp_path_factory) -> pl.DataFrame:
    return pl.concat(staged_actions(tmp_path_factory.mktemp("lake"), end, start=start) for start, end in WINDOWS)


def test_every_recorded_reference_price_is_reproduced_from_its_terms(recorded_events):
    assert set(recorded_events["source_report"]) == {"TWT49U", "TWTAUU", "TWTCAU"}
    assert recorded_events["formula_agrees"].null_count() == 0  # every event's term page was found
    d = date(2026, 10, 29)
    quotes = transform.parse_quotes(load_fixture("MI_INDEX", d), d)
    checks = corporate_actions.audit_corporate_actions(recorded_events, quotes, None, d)
    assert [c.name for c in checks if not c.passed] == []


@pytest.mark.parametrize(
    ("cash_delta", "ref_delta", "agrees"),
    [(0.0, "0", True), (1.0, "0", False), (0.0, "10", False)],
    ids=["as_published", "wrong_term_page", "misparsed_reference_price"],
)
def test_formula_flags_a_reference_price_its_terms_do_not_explain(cash_delta, ref_delta, agrees):
    """2330 on 2026-03-17: (1845 - 6.000035) / 1 = 1838.999965, published as 1838.99."""
    d, key = date(2026, 3, 17), "2330,20260317"
    events = transform.parse_corporate_actions(load_fixture("TWT49U", d)).filter(pl.col("stock_id") == "2330")
    events = events.with_columns(pl.col("ref_price") + Decimal(ref_delta))
    terms = transform.parse_event_terms(load_fixture("TWT49UDetail", d, key), pipeline.DETAIL_FIELDS["TWT49UDetail"])
    terms["cash_dividend"] += cash_delta
    checked = corporate_actions.attach_formula(events, corporate_actions.terms_frame({("TWT49U", key): terms}))
    assert checked["formula_agrees"].to_list() == [agrees]


def _split_day(tmp_path) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    quotes = transform.parse_quotes(load_fixture("MI_INDEX", SPLIT_DAY), SPLIT_DAY)
    halted = date(2025, 6, 17)  # previous trading day; 0050 has no quote while halted
    prev_close = transform.parse_quotes(load_fixture("MI_INDEX", halted), halted).select("stock_id", "close")
    return quotes, prev_close, staged_actions(tmp_path, SPLIT_DAY)


def test_split_resumption_is_checked_against_the_reference_price(tmp_path):
    quotes, prev_close, actions = _split_day(tmp_path)
    assert audit_price_continuity(quotes, prev_close, actions)[0].passed  # 47.57 - 0.41 == 47.16

    off = pl.when(pl.col("stock_id") == "0050").then(pl.col("close") + 1).otherwise(pl.col("close"))
    failed = audit_price_continuity(quotes.with_columns(off.alias("close")), prev_close, actions)[0]
    assert not failed.passed and "0050" in failed.detail


@pytest.mark.parametrize("method", ["backward", "forward"])
def test_adjusted_prices_remove_the_split_gap(tmp_path, method):
    _, _, actions = _split_day(tmp_path)
    days = [date(2025, 6, 10), SPLIT_DAY]  # last close before the halt, first close after
    quotes = pl.concat(transform.parse_quotes(load_fixture("MI_INDEX", d), d) for d in days)
    wide = quotes.filter(pl.col("stock_id") == "0050").join(
        actions.select("trade_date", "stock_id", "adj_factor"), on=["trade_date", "stock_id"], how="left"
    )
    out = adjust.adjust(wide, method).sort("trade_date")
    raw, adj = out["close"].cast(pl.Float64).to_list(), out["close_adj"].to_list()

    assert raw[1] / raw[0] - 1 == pytest.approx(-0.7478, abs=1e-4)  # what an unadjusted series shows
    assert adj[1] / adj[0] - 1 == pytest.approx(0.41 / 47.16, abs=1e-4)  # TWSE's own change / reference
    kept = 1 if method == "backward" else 0  # backward keeps today's price, forward the first day's
    assert adj[kept] == raw[kept]
