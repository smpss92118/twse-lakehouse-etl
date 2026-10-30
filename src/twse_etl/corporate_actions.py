"""Ex-rights / ex-dividend events, cross-validated across three independent sources.

1. TWT49U (official events)  vs  MI_INDEX change sign: every event on its ex-date must show "X".
2. TWT49U before_close        vs  our own Silver close of the previous trading day.
3. TWT49U reference price     vs  a third-party vendor (FinMind TaiwanStockDividendResult).
The opening reference price also feeds the price-continuity check in validate.py.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import polars as pl

from twse_etl.validate import Check, row_check

PRICE_TOLERANCE = Decimal("0.01")


def load_vendor(vendor_dir: Path, start: date, end: date) -> pl.DataFrame:
    rows: dict[tuple[str, str], dict] = {}
    for path in sorted(vendor_dir.glob("TaiwanStockDividendResult_*.json")):
        for row in json.loads(path.read_text()).get("data", []):
            if start.isoformat() <= row.get("date", "") <= end.isoformat():
                rows[(row["date"], row["stock_id"])] = row
    return pl.DataFrame(
        {
            "trade_date": [date.fromisoformat(r["date"]) for r in rows.values()],
            "stock_id": [str(r["stock_id"]) for r in rows.values()],
            "vendor_ref_price": [Decimal(str(r["reference_price"])) for r in rows.values()],
        },
        schema={"trade_date": pl.Date, "stock_id": pl.String, "vendor_ref_price": pl.Decimal(12, 2)},
    )


def attach_vendor(actions: pl.DataFrame, vendor: pl.DataFrame) -> pl.DataFrame:
    """sources_agreed is null when the vendor has no record (coverage gap, not a disagreement)."""
    out = actions.join(vendor, on=["trade_date", "stock_id"], how="left")
    diff = (pl.col("ref_price") - pl.col("vendor_ref_price")).abs()
    return out.with_columns(
        pl.when(pl.col("vendor_ref_price").is_null())
        .then(None)
        .otherwise(diff <= PRICE_TOLERANCE)
        .alias("sources_agreed")
    )


def audit_corporate_actions(
    actions: pl.DataFrame, quotes: pl.DataFrame, prev_close: pl.DataFrame | None, trade_date: date
) -> list[Check]:
    on_day = actions.filter(pl.col("trade_date") == trade_date)
    traded = quotes.filter(pl.col("close").is_not_null())
    not_x = on_day.join(traded.filter(pl.col("change_sign") != "X"), on="stock_id", how="semi")
    checks = [
        row_check("ca_adj_factor_range", actions.filter(~pl.col("adj_factor").is_between(0.0, 1.0, closed="right"))),
        row_check("ca_event_marked_x_in_quotes", not_x, blocking=False),
        row_check("ca_vendor_agrees", actions.filter(pl.col("sources_agreed") == False), blocking=False),  # noqa: E712
    ]
    if prev_close is not None and prev_close.height:
        mismatch = actions.join(prev_close.select("stock_id", "close"), on="stock_id", how="inner").filter(
            pl.col("before_close") != pl.col("close")
        )
        checks.append(row_check("ca_before_close_matches_silver", mismatch, blocking=False))
    return checks


def unexplained_x(quotes: pl.DataFrame, actions: pl.DataFrame) -> list[str]:
    """X rows without a TWT49U event: preferred shares, ETNs, special trading methods, splits.
    Informational only; they are common every day and would cause alert fatigue."""
    x_rows = quotes.filter(pl.col("change_sign") == "X")
    return x_rows.join(actions, on="stock_id", how="anti")["stock_id"].sort().to_list()
