"""Raw TWSE JSON -> typed Polars frames. Pure functions: no IO, no clock."""

from __future__ import annotations

import re
from datetime import date

import polars as pl

from twse_etl import contracts as c

NULL_TOKENS = ["", "--", "---", "----", "N/A"]
_HTML_TAG = re.compile(r"<[^>]*>")
_ROC_DATE = re.compile(r"(\d+)[年/](\d+)[月/](\d+)")  # "115年03月17日" (TWT49U) or "115/03/17" (TWTAUU)


class SchemaDriftError(ValueError):
    """TWSE changed the shape of a report; retrying will not help, a human must look."""


def find_quote_table(doc: dict) -> dict | None:
    for table in doc.get("tables", []):
        fields = table.get("fields") or []
        if "證券代號" in fields and "收盤價" in fields:
            return table
    return None


def row_count(report: str, doc: dict) -> int:
    if doc.get("stat") != "OK":
        return 0
    if report == "MI_INDEX":
        table = find_quote_table(doc)
        return len(table.get("data", [])) if table else 0
    return len(doc.get("data", []))


def is_complete(report: str, doc: dict, trade_date: date) -> bool:
    """Published, for the requested date, and not a partial upload."""
    if doc.get("date") not in (None, trade_date.strftime("%Y%m%d")):
        return False
    minimum = {"MI_INDEX": c.MIN_QUOTE_ROWS, "T86": c.MIN_INSTITUTIONAL_ROWS}[report]
    return row_count(report, doc) >= minimum


def unexpected_fields(fields: list[str], mapping: dict[str, str]) -> list[str]:
    return [f for f in fields if f not in mapping]


def _frame(fields: list[str], rows: list[list[str]], mapping: dict[str, str]) -> pl.DataFrame:
    missing = [h for h in mapping if h not in fields]
    if missing:
        raise SchemaDriftError(f"missing TWSE columns: {missing}")
    idx = [fields.index(h) for h in mapping]
    data = [[row[i] for i in idx] for row in rows]
    return pl.DataFrame(data, schema=dict.fromkeys(mapping.values(), pl.String), orient="row")


def _text(col: str) -> pl.Expr:
    return pl.col(col).str.strip_chars()


def _num(col: str) -> pl.Expr:
    s = pl.col(col).str.strip_chars().str.replace_all(",", "")
    return pl.when(s.is_in(NULL_TOKENS)).then(None).otherwise(s)


def _int(col: str) -> pl.Expr:
    return _num(col).cast(pl.Int64).alias(col)


def _price(col: str) -> pl.Expr:
    return _num(col).cast(pl.Decimal(12, 2)).alias(col)


def parse_quotes(doc: dict, trade_date: date) -> pl.DataFrame:
    """MI_INDEX per-stock table. Sign comes as HTML (<p style=color:red>+</p>), magnitude separately."""
    table = find_quote_table(doc)
    if table is None:
        raise SchemaDriftError("MI_INDEX payload has no per-stock quote table")
    raw = _frame(table["fields"], table["data"], c.QUOTE_FIELDS)

    sign = pl.col("change_sign").str.replace_all(_HTML_TAG.pattern, "").str.strip_chars()
    sign = pl.when(sign == "").then(pl.lit("0")).otherwise(sign)
    magnitude = _num("price_change").cast(pl.Decimal(12, 2))
    signed_change = (
        pl.when(sign == "+")
        .then(magnitude)
        .when(sign == "-")
        .then(-magnitude)
        .when(sign == "0")
        .then(magnitude)
        .otherwise(None)  # "X": TWSE prints 0.00 but the move is not comparable (ex-rights, etc.)
    )
    pe = _num("pe_ratio").cast(pl.Decimal(12, 2))
    return raw.select(
        pl.lit(trade_date).alias("trade_date"),
        _text("stock_id"),
        _text("stock_name"),
        _int("volume_shares"),
        _int("trade_count"),
        _int("turnover_twd"),
        _price("open"),
        _price("high"),
        _price("low"),
        _price("close"),
        sign.alias("change_sign"),
        signed_change.alias("price_change"),
        _price("last_bid_price"),
        _int("last_bid_volume_lots"),
        _price("last_ask_price"),
        _int("last_ask_volume_lots"),
        pl.when(pe == 0).then(None).otherwise(pe).alias("pe_ratio"),
    )


def parse_institutional(doc: dict, trade_date: date) -> pl.DataFrame:
    raw = _frame(doc.get("fields", []), doc.get("data", []), c.INSTITUTIONAL_FIELDS)
    numeric = list(c.INSTITUTIONAL_FIELDS.values())[2:]
    return raw.select(
        pl.lit(trade_date).alias("trade_date"),
        _text("stock_id"),
        _text("stock_name"),
        *[_int(col) for col in numeric],
    )


def _roc_to_date(text: str) -> date:
    m = _ROC_DATE.search(text)
    if not m:
        raise SchemaDriftError(f"unrecognised TWSE date {text!r}")
    year, month, day = (int(g) for g in m.groups())
    return date(year + 1911, month, day)


_CA_SCHEMA = {
    "trade_date": pl.Date,
    "stock_id": pl.String,
    "stock_name": pl.String,
    "action_type": pl.String,
    "source_report": pl.String,
    "note": pl.String,
    "before_close": pl.Decimal(12, 2),
    "ref_price": pl.Decimal(12, 2),
    "dividend_value": pl.Decimal(16, 6),
    "open_ref_price": pl.Decimal(12, 2),
    "dividend_ref_price": pl.Decimal(12, 2),
    "adj_factor": pl.Float64,
    "detail_key": pl.String,
}


def _corporate_action_frame(doc: dict, mapping: dict[str, str], source: str, **constants: str) -> pl.DataFrame:
    """One corporate-action table onto the shared shape. 'No data' is a normal day without events."""
    if str(doc.get("stat", "")).upper() != "OK" or not doc.get("data"):
        return pl.DataFrame(schema=_CA_SCHEMA)
    raw = _frame(doc["fields"], doc["data"], mapping)
    optional = ("dividend_value", "dividend_ref_price", "note", "detail_key")
    absent = [col for col in optional if col not in raw.columns]
    raw = raw.with_columns(
        pl.col("data_date").map_elements(_roc_to_date, return_dtype=pl.Date).alias("trade_date"),
        *[pl.lit(value).alias(col) for col, value in constants.items()],
        *[pl.lit(None, pl.String).alias(col) for col in absent],
    )
    return raw.select(
        "trade_date",
        _text("stock_id"),
        _text("stock_name"),
        _text("action_type").replace(c.ACTION_TYPES).alias("action_type"),
        pl.lit(source).alias("source_report"),
        _text("note"),
        _price("before_close"),
        _price("ref_price"),
        _num("dividend_value").cast(pl.Decimal(16, 6)).alias("dividend_value"),
        _price("open_ref_price"),
        _price("dividend_ref_price"),
        (_num("ref_price").cast(pl.Float64) / _num("before_close").cast(pl.Float64)).alias("adj_factor"),
        _text("detail_key").str.replace_all(r"\s+", ""),
    )


def parse_corporate_actions(doc: dict) -> pl.DataFrame:
    """TWT49U ex-rights / ex-dividend results; trade_date is the ex-date."""
    return _corporate_action_frame(doc, c.CORPORATE_ACTION_FIELDS, "TWT49U")


def parse_capital_reductions(doc: dict) -> pl.DataFrame:
    """TWTAUU capital reductions; trade_date is the day trading resumes at the reference price."""
    return _corporate_action_frame(doc, c.CAPITAL_REDUCTION_FIELDS, "TWTAUU", action_type=c.CAPITAL_REDUCTION)


def parse_etf_splits(doc: dict) -> pl.DataFrame:
    """TWTCAU ETF splits / reverse splits; trade_date is the day trading resumes."""
    return _corporate_action_frame(doc, c.ETF_SPLIT_FIELDS, "TWTCAU")


_LEADING_NUMBER = re.compile(r"-?[\d,]*\.?\d+")


def parse_event_terms(doc: dict, mapping: dict[str, str]) -> dict[str, float] | None:
    """A TWT49UDetail / TWTAVUDetail page ("6.000035 元／股", "950.00000000 股") as numbers.
    None when the page is unavailable: the formula check then reports 'unknown', not 'wrong'."""
    if str(doc.get("stat", "")).upper() != "OK" or not doc.get("data"):
        return None
    fields = [f.strip() for f in doc["fields"]]
    missing = [f for f in mapping if f not in fields]
    if missing:
        raise SchemaDriftError(f"missing detail fields: {missing}")
    row = doc["data"][0]
    terms = {}
    for field, name in mapping.items():
        m = _LEADING_NUMBER.search(row[fields.index(field)])
        terms[name] = float(m.group().replace(",", "")) if m else 0.0
    return terms
