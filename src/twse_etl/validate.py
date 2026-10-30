"""Audit step of Write-Audit-Publish. Every check runs on staged data before any Iceberg commit.

Blocking failures stop the publish; non-blocking ones raise an alert and are kept in the manifest.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date

import polars as pl

from twse_etl import contracts as c
from twse_etl.alerts import alert


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str = ""
    blocking: bool = True

    def to_dict(self) -> dict:
        return asdict(self)


class DataQualityError(Exception):
    def __init__(self, failures: list[Check]):
        self.failures = failures
        super().__init__("; ".join(f"{f.name}: {f.detail}" for f in failures))


def row_check(name: str, bad: pl.DataFrame, blocking: bool = True) -> Check:
    if bad.height == 0:
        return Check(name, True, blocking=blocking)
    sample = bad["stock_id"].head(5).to_list() if "stock_id" in bad.columns else []
    return Check(name, False, f"{bad.height} rows, e.g. {sample}", blocking)


def enforce(checks: list[Check], **context: object) -> None:
    for check in checks:
        if not check.passed and not check.blocking:
            alert("data_quality_warning", check=check.name, detail=check.detail, **context)
    blocking = [ch for ch in checks if not ch.passed and ch.blocking]
    if blocking:
        alert("data_quality_blocked", failures=[ch.to_dict() for ch in blocking], **context)
        raise DataQualityError(blocking)


def audit_quotes(df: pl.DataFrame, trade_date: date) -> list[Check]:
    sign, change = pl.col("change_sign"), pl.col("price_change")
    o, h, lo, cl = pl.col("open"), pl.col("high"), pl.col("low"), pl.col("close")
    return [
        Check("quotes_min_rows", df.height >= c.MIN_QUOTE_ROWS, f"{df.height} rows (min {c.MIN_QUOTE_ROWS})"),
        row_check("quotes_trade_date", df.filter(pl.col("trade_date") != trade_date)),
        row_check("quotes_unique_key", df.filter(pl.col("stock_id").is_duplicated() | pl.col("stock_id").is_null())),
        row_check("quotes_ohlc_bounds", df.filter(cl.is_not_null() & ((lo > o) | (lo > cl) | (h < o) | (h < cl)))),
        # Odd-lot-only days carry volume with no board-lot close, so only this direction holds.
        row_check("quotes_close_implies_volume", df.filter(cl.is_not_null() & (pl.col("volume_shares") <= 0))),
        row_check("quotes_change_sign_known", df.filter(~sign.is_in(["+", "-", "0", "X"]))),
        row_check(
            "quotes_sign_matches_change",
            df.filter(
                ((sign == "+") & (change <= 0))
                | ((sign == "-") & (change >= 0))
                | ((sign == "0") & (change != 0))
                | ((sign == "X") & change.is_not_null())
            ),
        ),
    ]


def audit_price_continuity(df: pl.DataFrame, prev_close: pl.DataFrame | None, actions: pl.DataFrame) -> list[Check]:
    """close - signed change must equal the previous close, or the TWT49U opening reference price
    when an ex-rights event fell between the two trading days (e.g. dated on a typhoon closure).
    "X" rows are excluded: TWSE marks those moves as not comparable."""
    if prev_close is None or prev_close.height == 0:
        return [Check("quotes_price_continuity", True, "skipped: no previous trading day in Silver")]
    base = (
        prev_close.select("stock_id", pl.col("close").alias("prev_close"))
        .join(actions.select("stock_id", "open_ref_price"), on="stock_id", how="left")
        .select("stock_id", pl.coalesce("open_ref_price", "prev_close").alias("base"))
    )
    joined = df.filter((pl.col("change_sign") != "X") & pl.col("close").is_not_null()).join(
        base.filter(pl.col("base").is_not_null()), on="stock_id", how="inner"
    )
    bad = joined.filter((pl.col("close") - pl.col("price_change")) != pl.col("base"))
    check = row_check("quotes_price_continuity", bad)
    return [Check(check.name, check.passed, check.detail or f"{joined.height} rows checked", check.blocking)]


def audit_institutional(df: pl.DataFrame, trade_date: date) -> list[Check]:
    col = pl.col
    checks = [
        Check(
            "institutional_min_rows",
            df.height >= c.MIN_INSTITUTIONAL_ROWS,
            f"{df.height} rows (min {c.MIN_INSTITUTIONAL_ROWS})",
        ),
        row_check("institutional_trade_date", df.filter(col("trade_date") != trade_date)),
        row_check("institutional_unique_key", df.filter(col("stock_id").is_duplicated() | col("stock_id").is_null())),
    ]
    for group in c.INSTITUTIONAL_GROUPS:
        bad = df.filter(col(f"{group}_buy") - col(f"{group}_sell") != col(f"{group}_net"))
        checks.append(row_check(f"institutional_{group}_buy_minus_sell", bad))
    checks.append(
        row_check(
            "institutional_dealer_split",
            df.filter(col("dealer_self_net") + col("dealer_hedge_net") != col("dealer_net")),
        )
    )
    total = col("foreign_net") + col("foreign_dealer_net") + col("trust_net") + col("dealer_net")
    checks.append(row_check("institutional_total_net", df.filter(total != col("total_net"))))
    return checks


def audit_cross_table(quotes: pl.DataFrame, institutional: pl.DataFrame) -> list[Check]:
    """The checks that would have caught 'merged table has 800 rows, quotes has 1,000+, DAG green'."""
    orphans = institutional.join(quotes, on="stock_id", how="anti")
    matched = quotes.join(institutional, on="stock_id", how="semi").height
    coverage = matched / quotes.height if quotes.height else 0.0
    return [
        row_check("institutional_ids_in_quotes", orphans),
        Check(
            "institutional_coverage",
            coverage >= c.MIN_INSTITUTIONAL_COVERAGE,
            f"{matched}/{quotes.height} = {coverage:.1%} (min {c.MIN_INSTITUTIONAL_COVERAGE:.0%})",
        ),
    ]
