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

# TWT49U (ex-rights / ex-dividend results). Only the columns the pipeline uses.
CORPORATE_ACTION_FIELDS = {
    "資料日期": "data_date",
    "股票代號": "stock_id",
    "股票名稱": "stock_name",
    "除權息前收盤價": "before_close",
    "除權息參考價": "ref_price",
    "權值+息值": "dividend_value",
    "權/息": "action_type",
    "開盤競價基準": "open_ref_price",
}

ACTION_TYPES = {"息": "cash_dividend", "權": "stock_dividend", "權息": "cash_and_stock_dividend"}

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
        *_KEY,  # trade_date = ex-rights / ex-dividend date
        pa.field("action_type", pa.string()),
        pa.field("before_close", PRICE),
        pa.field("ref_price", PRICE),
        pa.field("dividend_value", pa.decimal128(16, 6)),
        pa.field("open_ref_price", PRICE),
        pa.field("adj_factor", pa.float64()),  # ref_price / before_close
        pa.field("vendor_ref_price", PRICE),  # third-party (FinMind) reference price
        pa.field("sources_agreed", pa.bool_()),  # null when the vendor has no record
        *LINEAGE,
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
    ]
)
