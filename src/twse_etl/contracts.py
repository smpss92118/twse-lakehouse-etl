"""Data contracts: TWSE header -> English column, Silver/Gold Arrow schemas, quality thresholds.

TWSE columns are mapped by header text, never by position: a renamed or dropped column raises
SchemaDriftError instead of silently shifting values into the wrong field.
"""

from __future__ import annotations

import pyarrow as pa

PRICE = pa.decimal128(12, 2)

# Thresholds. Normal days: ~1,380 quote rows and ~1,340 institutional rows (2026-10).
MIN_QUOTE_ROWS = 900
MIN_INSTITUTIONAL_ROWS = 500
MIN_INSTITUTIONAL_COVERAGE = 0.85

QUOTE_FIELDS = {
    "證券代號": "stock_id",
    "證券名稱": "stock_name",
    "成交股數": "volume_shares",
    "成交筆數": "trade_count",
    "成交金額": "turnover_twd",
    "開盤價": "open",
    "最高價": "high",
    "最低價": "low",
    "收盤價": "close",
    "漲跌(+/-)": "change_sign",
    "漲跌價差": "price_change",
    "最後揭示買價": "last_bid_price",
    "最後揭示買量": "last_bid_volume_lots",
    "最後揭示賣價": "last_ask_price",
    "最後揭示賣量": "last_ask_volume_lots",
    "本益比": "pe_ratio",
}

INSTITUTIONAL_FIELDS = {
    "證券代號": "stock_id",
    "證券名稱": "stock_name",
    "外陸資買進股數(不含外資自營商)": "foreign_buy",
    "外陸資賣出股數(不含外資自營商)": "foreign_sell",
    "外陸資買賣超股數(不含外資自營商)": "foreign_net",
    "外資自營商買進股數": "foreign_dealer_buy",
    "外資自營商賣出股數": "foreign_dealer_sell",
    "外資自營商買賣超股數": "foreign_dealer_net",
    "投信買進股數": "trust_buy",
    "投信賣出股數": "trust_sell",
    "投信買賣超股數": "trust_net",
    "自營商買賣超股數": "dealer_net",
    "自營商買進股數(自行買賣)": "dealer_self_buy",
    "自營商賣出股數(自行買賣)": "dealer_self_sell",
    "自營商買賣超股數(自行買賣)": "dealer_self_net",
    "自營商買進股數(避險)": "dealer_hedge_buy",
    "自營商賣出股數(避險)": "dealer_hedge_sell",
    "自營商買賣超股數(避險)": "dealer_hedge_net",
    "三大法人買賣超股數": "total_net",
}

# Corporate actions: three TWSE tables, one row per event, all mapped onto CORPORATE_ACTIONS_SCHEMA.
# trade_date is the day the event takes effect (ex-date, or the resumption day after a halt).
# TWT49U: ex-rights / ex-dividend results.
CORPORATE_ACTION_FIELDS = {
    "資料日期": "data_date",
    "股票代號": "stock_id",
    "股票名稱": "stock_name",
    "除權息前收盤價": "before_close",
    "除權息參考價": "ref_price",
    "權值+息值": "dividend_value",
    "權/息": "action_type",
    "開盤競價基準": "open_ref_price",
    "減除股利參考價": "dividend_ref_price",
    "詳細資料": "detail_key",
}
# TWTAUU: capital reduction, trading resumes at a reference price.
CAPITAL_REDUCTION_FIELDS = {
    "恢復買賣日期": "data_date",
    "股票代號": "stock_id",
    "名稱": "stock_name",
    "停止買賣前收盤價格": "before_close",
    "恢復買賣參考價": "ref_price",
    "開盤競價基準": "open_ref_price",
    "減資原因": "note",
    "詳細資料": "detail_key",
}
# TWTCAU: ETF split / reverse split, trading resumes at a reference price.
ETF_SPLIT_FIELDS = {
    "恢復買賣日期": "data_date",
    "ETF代號": "stock_id",
    "名稱": "stock_name",
    "分割(反分割)": "action_type",
    "停止買賣前收盤價格": "before_close",
    "恢復買賣參考價": "ref_price",
    "開盤競價基準": "open_ref_price",
}
# Per-event terms behind the official reference price (TWT49UDetail / TWTAVUDetail pages).
DIVIDEND_DETAIL_FIELDS = {
    "(每股配發現金股利)除息": "cash_dividend",
    "A. 按普通股股東持股比例每千股無償配股": "bonus_shares_per_1000",
    "C. (有償) 現金增資": "cash_increase_shares",
    "c. 原股東認購": "shareholder_subscribed_shares",
    "每股認購金額": "subscription_price",
    "按股東持股比例每千股認購": "subscribed_shares_per_1000",
}
REDUCTION_DETAIL_FIELDS = {
    "每壹仟股換發新股票：": "new_shares_per_1000",
    "每股退還股款：": "refund_per_share",
    "原股每股配發現金股利：": "cash_dividend",
    "減資並(有償)現金增資：": "cash_increase_shares",
    "c. 原股東認購：": "shareholder_subscribed_shares",
    "每股認購金額：": "subscription_price",
    "按股東持股比例每千股認購：": "subscribed_shares_per_1000",
}

ACTION_TYPES = {
    "息": "cash_dividend",
    "權": "stock_dividend",
    "權息": "cash_and_stock_dividend",
    "分割": "split",
    "反分割": "reverse_split",
}
DIVIDEND_ACTIONS = ("cash_dividend", "stock_dividend", "cash_and_stock_dividend")
CAPITAL_REDUCTION = "capital_reduction"

INSTITUTIONAL_GROUPS = ("foreign", "foreign_dealer", "trust", "dealer_self", "dealer_hedge")
NET_COLUMNS = ("foreign_net", "foreign_dealer_net", "trust_net", "dealer_net", "total_net")

LINEAGE = [
    pa.field("run_id", pa.string(), nullable=False),
    pa.field("source_sha256", pa.string(), nullable=False),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
]

_KEY = [
    pa.field("trade_date", pa.date32(), nullable=False),
    pa.field("stock_id", pa.string(), nullable=False),
    pa.field("stock_name", pa.string()),
]

QUOTES_SCHEMA = pa.schema(
    [
        *_KEY,
        pa.field("volume_shares", pa.int64()),
        pa.field("trade_count", pa.int64()),
        pa.field("turnover_twd", pa.int64()),
        pa.field("open", PRICE),
        pa.field("high", PRICE),
        pa.field("low", PRICE),
        pa.field("close", PRICE),
        pa.field("change_sign", pa.string()),  # "+", "-", "0" (flat), "X" (not comparable)
        pa.field("price_change", PRICE),  # signed; null when change_sign == "X"
        pa.field("last_bid_price", PRICE),
        pa.field("last_bid_volume_lots", pa.int64()),
        pa.field("last_ask_price", PRICE),
        pa.field("last_ask_volume_lots", pa.int64()),
        pa.field("pe_ratio", PRICE),  # null when TWSE reports 0.00 (no meaningful P/E)
        *LINEAGE,
    ]
)

INSTITUTIONAL_SCHEMA = pa.schema(
    [*_KEY, *[pa.field(col, pa.int64()) for col in list(INSTITUTIONAL_FIELDS.values())[2:]], *LINEAGE]
)

CORPORATE_ACTIONS_SCHEMA = pa.schema(
    [
        *_KEY,  # trade_date = the day the event takes effect (ex-date or resumption day)
        pa.field("action_type", pa.string()),  # ACTION_TYPES values or capital_reduction
        pa.field("before_close", PRICE),
        pa.field("ref_price", PRICE),
        pa.field("dividend_value", pa.decimal128(16, 6)),
        pa.field("open_ref_price", PRICE),
        pa.field("adj_factor", pa.float64()),  # ref_price / before_close
        pa.field("vendor_ref_price", PRICE),  # third-party (FinMind) reference price
        pa.field("sources_agreed", pa.bool_()),  # null when the vendor has no record
        *LINEAGE,
        # Added later; Iceberg appends evolved columns at the end, so new tables match old ones.
        pa.field("source_report", pa.string()),  # TWT49U / TWTAUU / TWTCAU
        pa.field("note", pa.string()),  # e.g. the reason of a capital reduction
        pa.field("calc_ref_price", pa.decimal128(14, 4)),  # official formula recomputed from the terms
        pa.field("formula_agrees", pa.bool_()),  # truncates to ref_price; null when terms unavailable
        # TWT49U only: the reference price without a cash capital increase. TWSE opens (and sets the
        # price limits) from it, so for a rights issue open_ref_price follows it, not ref_price.
        pa.field("dividend_ref_price", PRICE),
        # Set when a person reviewed the event (review_decisions); adj_factor then follows the decision.
        pa.field("review_decision_id", pa.string()),
    ]
)

# Append-only log of human reviews of corporate actions that the cross-checks could not reconcile.
# The latest decision per (trade_date, stock_id) is applied every time that event is staged, so
# reruns and backfills keep the reviewed factor; earlier decisions stay as the audit trail.
REVIEW_DECISIONS_SCHEMA = pa.schema(
    [
        pa.field("decision_id", pa.string(), nullable=False),
        pa.field("trade_date", pa.date32(), nullable=False),  # the event's effective date
        pa.field("stock_id", pa.string(), nullable=False),
        pa.field("run_date", pa.date32(), nullable=False),  # pipeline day re-run with the decision
        pa.field("checks", pa.string(), nullable=False),  # what disagreed, or "manual_request"
        pa.field("decision", pa.string(), nullable=False),  # accept_twse | override
        pa.field("twse_ref_price", PRICE),
        pa.field("twse_before_close", PRICE),
        pa.field("twse_adj_factor", pa.float64()),
        pa.field("ref_price", PRICE),  # values in force after the decision
        pa.field("before_close", PRICE),
        pa.field("adj_factor", pa.float64()),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("reviewed_by", pa.string(), nullable=False),  # authenticated Airflow user
        pa.field("reviewed_by_id", pa.string(), nullable=False),
        pa.field("reviewed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("review_run_id", pa.string(), nullable=False),
        pa.field("evidence", pa.string()),  # JSON: every source's values as the reviewer saw them
    ]
)

GOLD_SCHEMA = pa.schema(
    [
        *_KEY,
        pa.field("open", PRICE),
        pa.field("high", PRICE),
        pa.field("low", PRICE),
        pa.field("close", PRICE),
        pa.field("change_sign", pa.string()),
        pa.field("price_change", PRICE),
        pa.field("volume_shares", pa.int64()),
        pa.field("turnover_twd", pa.int64()),
        *[pa.field(col, pa.int64()) for col in NET_COLUMNS],
        pa.field("institutional_reported", pa.bool_(), nullable=False),
        pa.field("is_ex_event", pa.bool_(), nullable=False),
        pa.field("adj_factor", pa.float64()),
        pa.field("quotes_snapshot_id", pa.int64(), nullable=False),
        pa.field("institutional_snapshot_id", pa.int64(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("built_at", pa.timestamp("us", tz="UTC"), nullable=False),
        # Added later (appended, see CORPORATE_ACTIONS_SCHEMA): events taking effect that day, e.g. "split".
        pa.field("corporate_action", pa.string()),
    ]
)
