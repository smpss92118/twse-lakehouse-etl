"""Corporate actions (ex-rights / ex-dividend, capital reductions, ETF splits) and their adjustment
factors, taken from TWSE as published.

Each event has an official reference price (TWT49U / TWTAUU / TWTCAU) and an adjustment factor
adj_factor = ref_price / before_close. The factor removes the price gap the exchange itself opens on
the effective day, so TWSE's reference price is the definition, not a figure to re-derive. What can
go wrong is our reading of it, and that is what the checks look for:

1. adj_factor in range (blocking): dividends only lower the price; anything else is a parsing error.
2. TWT49U row identity (alert): ref_price = before_close - 權值+息值, truncated to the cent. Three
   columns of one row agreeing means none of them was read from the wrong column.
3. before_close = our own Silver close of the previous trading day (alert): one value, two reports.
4. On a non-"X" effective day, close - change = the opening reference price (validate.py, blocking):
   the exchange's own price series confirms the event.

2 and 3 alert instead of blocking: tomorrow's quotes and flows must not wait on an enrichment column,
and TWSE's published number stays the one in force.
"""

from __future__ import annotations

from decimal import Decimal

import polars as pl

from twse_etl import contracts as c
from twse_etl.validate import Check, row_check

CENT = Decimal("0.01")
MAX_REVERSE_FACTOR = 100.0  # a 1-for-100 reverse split; anything larger is a parsing error


def factor_in_range() -> pl.Expr:
    """Dividends only ever lower the price; reductions and reverse splits raise it."""
    factor = pl.col("adj_factor")
    return (
        pl.when(pl.col("action_type").is_in(c.DIVIDEND_ACTIONS))
        .then(factor.is_between(0.0, 1.0, closed="right"))
        .otherwise(factor.is_between(0.0, MAX_REVERSE_FACTOR, closed="right"))
        .fill_null(False)
    )


def row_identity_breaks(actions: pl.DataFrame) -> pl.DataFrame:
    """TWT49U rows whose reference price is not before_close - 權值+息值 truncated to the cent.
    Exact decimal arithmetic: 0 <= gap < 0.01 is the truncation, nothing looser."""
    gap = pl.col("before_close") - pl.col("dividend_value") - pl.col("ref_price")
    return actions.filter(pl.col("dividend_value").is_not_null() & ~((gap >= 0) & (gap < CENT)))


def audit_corporate_actions(actions: pl.DataFrame, prev_close: pl.DataFrame | None) -> list[Check]:
    checks = [
        row_check("ca_adj_factor_range", actions.filter(~factor_in_range())),
        row_check("ca_ref_price_matches_row", row_identity_breaks(actions), blocking=False),
    ]
    if prev_close is not None and prev_close.height:
        moved = actions.join(prev_close.select("stock_id", "close"), on="stock_id", how="inner").filter(
            pl.col("before_close") != pl.col("close")
        )
        checks.append(row_check("ca_before_close_matches_silver", moved, blocking=False))
    return checks
