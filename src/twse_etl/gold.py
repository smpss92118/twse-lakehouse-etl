"""Gold: one row per listed stock per day, quotes + institutional net flows + ex-rights flag."""

from __future__ import annotations

from datetime import datetime

import polars as pl

from twse_etl import contracts as c

QUOTE_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "open",
    "high",
    "low",
    "close",
    "change_sign",
    "price_change",
    "volume_shares",
    "turnover_twd",
]


def build(
    quotes: pl.DataFrame,
    institutional: pl.DataFrame,
    actions: pl.DataFrame,
    *,
    quotes_snapshot_id: int,
    institutional_snapshot_id: int,
    run_id: str,
    built_at: datetime,
) -> pl.DataFrame:
    """LEFT join from quotes: a stock never disappears because T86 has no row for it.
    T86 omits stocks with no institutional trades; those get 0 and institutional_reported=False.
    `actions` covers (previous trading day, trade_date], so an event dated on a closure day
    is flagged on the first trading day it takes effect."""
    flows = institutional.select("trade_date", "stock_id", *c.NET_COLUMNS).with_columns(
        pl.lit(True).alias("institutional_reported")
    )
    events = (
        actions.group_by("stock_id").agg(pl.col("adj_factor").product()).with_columns(pl.lit(True).alias("is_ex_event"))
    )
    wide = (
        quotes.select(QUOTE_COLUMNS)
        .join(flows, on=["trade_date", "stock_id"], how="left")
        .join(events, on="stock_id", how="left")
        .with_columns(
            pl.col(list(c.NET_COLUMNS)).fill_null(0),
            pl.col("institutional_reported").fill_null(False),
            pl.col("is_ex_event").fill_null(False),
            pl.lit(quotes_snapshot_id, pl.Int64).alias("quotes_snapshot_id"),
            pl.lit(institutional_snapshot_id, pl.Int64).alias("institutional_snapshot_id"),
            pl.lit(run_id).alias("run_id"),
            pl.lit(built_at).dt.cast_time_unit("us").alias("built_at"),
        )
    )
    return wide.select(c.GOLD_SCHEMA.names)
