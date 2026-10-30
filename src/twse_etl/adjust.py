"""Adjusted prices, computed at query time from Gold's per-day adjustment factors.

History is never rewritten: every snapshot of daily_market_wide keeps the prices TWSE printed plus
the day's adj_factor (ref_price / before_close of the events taking effect that day). Rewriting
years of closes at each ex-date would make every old snapshot disagree with today's, and the
question "which data did last week's signal use" would lose its answer.

    backward (前復權)  the last day keeps its printed price; each earlier price is multiplied by
                       every factor that takes effect after it. Charts, returns ending today.
    forward  (後復權)  the first day keeps its printed price; each later price is divided by every
                       factor since. Levels never change as days are appended, so a backtest that
                       reads only days up to its signal date stays point-in-time.

The factor on the window's first day is ignored: it links to a close outside the window.
"""

from __future__ import annotations

from typing import Literal

import polars as pl

PRICE_COLUMNS = ("open", "high", "low", "close")


def adjust(wide: pl.DataFrame, method: Literal["backward", "forward"] = "backward") -> pl.DataFrame:
    """Rows of daily_market_wide (any window, any stocks) + <price>_adj columns."""
    by_stock = pl.col("trade_date").min().over("stock_id")
    factor = pl.when(pl.col("trade_date") == by_stock).then(1.0).otherwise(pl.col("adj_factor").fill_null(1.0))
    out = wide.sort("stock_id", "trade_date").with_columns(factor.cum_prod().over("stock_id").alias("_cum"))
    scale = pl.col("_cum").last().over("stock_id") / pl.col("_cum") if method == "backward" else 1 / pl.col("_cum")
    adjusted = [(pl.col(col).cast(pl.Float64) * scale).round(4).alias(f"{col}_adj") for col in PRICE_COLUMNS]
    return out.with_columns(adjusted).drop("_cum")
