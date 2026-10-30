"""Corporate actions (ex-rights / ex-dividend, capital reductions, ETF splits) and their adjustment
factors, cross-validated before any factor is trusted.

Each event has an official reference price (TWT49U / TWTAUU / TWTCAU) and an adjustment factor
adj_factor = ref_price / before_close. The factor is checked from independent angles:

1. Official formula, recomputed from the event's terms (TWT49UDetail / TWTAVUDetail pages):
       ref = (before_close - cash - refund + subscription_price * s) / (k + s)
   k = new shares per old share before any subscription (1 + bonus ratio for dividends, the exchange
   ratio for reductions, the split ratio for splits); s = new shares of a cash capital increase per
   old share. TWSE truncates the result to the cent, so ref <= calc < ref + 0.01 -> formula_agrees.
   A tick-wide tolerance would be too loose: at 1000+ a tick is 5, wider than a wrong dividend.
2. before_close vs our own Silver close of the previous trading day.
3. The opening reference price is the official price rounded to a tick (for a rights issue, the
   price without the cash capital increase: TWT49U 減除股利參考價).
4. On a non-"X" effective day, close - change must equal the opening reference price (validate.py).
   On an "X" day TWSE prints 0.00 as the change, so only 1-3 apply.
5. Ex-dividend reference price vs a third-party vendor (FinMind TaiwanStockDividendResult).
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import polars as pl

from twse_etl import contracts as c
from twse_etl.validate import Check, row_check

PRICE_TOLERANCE = Decimal("0.01")
MAX_REVERSE_FACTOR = 100.0  # a 1-for-100 reverse split; anything larger is a parsing error

# TWSE tick sizes: (price below, tick), then the tick for everything above.
STOCK_TICKS = ((10, 0.01), (50, 0.05), (100, 0.1), (500, 0.5), (1000, 1.0)), 5.0
ETF_TICKS = ((50, 0.01),), 0.05  # ETFs are coded 00xx


def _ticks(price: pl.Expr, table: tuple) -> pl.Expr:
    steps, top = table
    expr = pl.lit(top)
    for below, tick in reversed(steps):
        expr = pl.when(price < below).then(tick).otherwise(expr)
    return expr


def tick_size(price: pl.Expr, stock_id: pl.Expr) -> pl.Expr:
    return pl.when(stock_id.str.starts_with("00")).then(_ticks(price, ETF_TICKS)).otherwise(_ticks(price, STOCK_TICKS))


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


TERM_COLUMNS = (
    "cash_dividend",
    "bonus_shares_per_1000",
    "new_shares_per_1000",
    "refund_per_share",
    "subscription_price",
    "subscribed_shares_per_1000",
    "cash_increase_shares",
    "shareholder_subscribed_shares",
)


def terms_frame(terms: dict[tuple[str, str], dict[str, float]]) -> pl.DataFrame:
    """{(source_report, detail_key): parsed detail page} as rows to join onto the events."""
    rows = [{"source_report": src, "detail_key": key, **t} for (src, key), t in terms.items()]
    schema = {"source_report": pl.String, "detail_key": pl.String, **dict.fromkeys(TERM_COLUMNS, pl.Float64)}
    return pl.DataFrame(rows, schema=schema)


def attach_formula(actions: pl.DataFrame, terms: pl.DataFrame) -> pl.DataFrame:
    """calc_ref_price from the official formula; formula_agrees when it truncates to ref_price.
    Both are null when the event's terms are unavailable (an unknown is not a disagreement)."""
    col = pl.col
    out = actions.join(terms, on=["source_report", "detail_key"], how="left")
    before, ref = col("before_close").cast(pl.Float64), col("ref_price").cast(pl.Float64)
    t = {name: col(name).fill_null(0.0) for name in TERM_COLUMNS}
    # TWSE's subscription rate counts every new share of the cash increase (public offering and
    # employees too), not only the shareholders' allotment that the per-1000 figure describes.
    allotted = t["subscribed_shares_per_1000"] / 1000
    total, offered = t["cash_increase_shares"], t["shareholder_subscribed_shares"]
    s = pl.when(offered > 0).then(allotted * total / offered).otherwise(allotted)
    ratio = before / ref  # splits: the ratio is a whole number (or its inverse) by definition
    k = (
        pl.when(col("source_report") == "TWT49U")
        .then(1 + t["bonus_shares_per_1000"] / 1000)
        .when(col("source_report") == "TWTAUU")
        .then(t["new_shares_per_1000"] / 1000)
        .otherwise(pl.when(ratio >= 1).then(ratio.round(0)).otherwise(1 / (1 / ratio).round(0)))
    )
    value = before - t["cash_dividend"] - t["refund_per_share"] + t["subscription_price"] * s
    has_terms = (col("source_report") == "TWTCAU") | col("cash_dividend").is_not_null()
    calc = pl.when(has_terms & (k + s > 0)).then(value / (k + s))
    out = out.with_columns(calc.alias("_calc"))
    agrees = (col("_calc") - ref).is_between(-1e-6, float(PRICE_TOLERANCE) + 1e-6)  # 1e-6: float error
    return out.with_columns(
        col("_calc").round(4).cast(pl.Decimal(14, 4)).alias("calc_ref_price"),
        pl.when(col("_calc").is_null()).then(None).otherwise(agrees).alias("formula_agrees"),
    ).drop("_calc", *TERM_COLUMNS)


def factor_in_range() -> pl.Expr:
    """Dividends only ever lower the price; reductions and reverse splits raise it."""
    factor = pl.col("adj_factor")
    return (
        pl.when(pl.col("action_type").is_in(c.DIVIDEND_ACTIONS))
        .then(factor.is_between(0.0, 1.0, closed="right"))
        .otherwise(factor.is_between(0.0, MAX_REVERSE_FACTOR, closed="right"))
        .fill_null(False)
    )


def disagreements(
    actions: pl.DataFrame, quotes: pl.DataFrame, prev_close: pl.DataFrame | None, trade_date: date
) -> dict[str, pl.DataFrame]:
    """The cross-checks that alert instead of block: check name -> events failing it."""
    col = pl.col
    ref = pl.coalesce("dividend_ref_price", "ref_price").cast(pl.Float64)
    open_ref = col("open_ref_price").cast(pl.Float64)
    open_ref_ok = ((open_ref - ref).abs() <= tick_size(ref, col("stock_id")) + 1e-9).fill_null(True)
    traded = quotes.filter(col("close").is_not_null())
    dividends_today = actions.filter((col("trade_date") == trade_date) & col("action_type").is_in(c.DIVIDEND_ACTIONS))
    found = {
        "ca_formula_agrees": actions.filter(col("formula_agrees") == False),  # noqa: E712
        "ca_open_ref_within_tick": actions.filter(~open_ref_ok),
        "ca_event_marked_x_in_quotes": dividends_today.join(
            traded.filter(col("change_sign") != "X"), on="stock_id", how="semi"
        ),
        "ca_vendor_agrees": actions.filter(col("sources_agreed") == False),  # noqa: E712
    }
    if prev_close is not None and prev_close.height:
        found["ca_before_close_matches_silver"] = actions.join(
            prev_close.select("stock_id", "close"), on="stock_id", how="inner"
        ).filter(col("before_close") != col("close"))
    return found


def audit_corporate_actions(
    actions: pl.DataFrame, quotes: pl.DataFrame, prev_close: pl.DataFrame | None, trade_date: date
) -> list[Check]:
    found = disagreements(actions, quotes, prev_close, trade_date)
    alerts = [row_check(name, rows, blocking=False) for name, rows in found.items()]
    return [row_check("ca_adj_factor_range", actions.filter(~factor_in_range())), *alerts]


def events_to_review(found: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """One row per event that failed any cross-check: trade_date, stock_id, checks ("a,b")."""
    rows = [df.select("trade_date", "stock_id", pl.lit(name).alias("check")) for name, df in found.items()]
    schema = {"trade_date": pl.Date, "stock_id": pl.String, "check": pl.String}
    return (
        pl.concat([pl.DataFrame(schema=schema), *rows])
        .group_by("trade_date", "stock_id")
        .agg(pl.col("check").unique().sort().str.join(",").alias("checks"))
        .sort("trade_date", "stock_id")
    )


def apply_decisions(actions: pl.DataFrame, decisions: pl.DataFrame) -> pl.DataFrame:
    """The latest human decision per event: "override" replaces adj_factor with the reviewer's
    ref_price / before_close; either decision marks the event as reviewed (review_decision_id).
    The TWSE columns are left as published, so the table shows both what TWSE said and what is used."""
    latest = (
        decisions.sort("reviewed_at")
        .group_by("trade_date", "stock_id")
        .last()
        .select("trade_date", "stock_id", "decision", "decision_id", pl.col("adj_factor").alias("_reviewed"))
    )
    out = actions.join(latest, on=["trade_date", "stock_id"], how="left")
    factor = pl.when(pl.col("decision") == "override").then(pl.col("_reviewed")).otherwise(pl.col("adj_factor"))
    return out.with_columns(factor.alias("adj_factor"), pl.col("decision_id").alias("review_decision_id")).drop(
        "decision", "decision_id", "_reviewed"
    )


def unexplained_x(quotes: pl.DataFrame, actions: pl.DataFrame) -> list[str]:
    """X rows without any corporate action: preferred shares, ETNs, special trading methods, par
    value changes. Informational only; they are common every day and would cause alert fatigue."""
    x_rows = quotes.filter(pl.col("change_sign") == "X")
    return x_rows.join(actions, on="stock_id", how="anti")["stock_id"].sort().to_list()
