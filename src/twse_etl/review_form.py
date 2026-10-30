"""The review form: plain Python, so the review DAG can import it while Airflow parses DAGs."""

from __future__ import annotations

ACCEPT = "採用 TWSE 公布值"
OVERRIDE = "改用我填的數值"
DECISIONS = {ACCEPT: "accept_twse", OVERRIDE: "override"}
MIN_REASON_CHARS = 4
# Form fields (JSON schema for Airflow Params). A blank price keeps TWSE's value.
FORM_FIELDS = {
    "ref_price": {"type": ["null", "number"], "exclusiveMinimum": 0, "description": "正確的參考價；留空 = 沿用 TWSE"},
    "before_close": {
        "type": ["null", "number"],
        "exclusiveMinimum": 0,
        "description": "正確的事件前收盤；留空 = 沿用 TWSE",
    },
    "reason": {"type": ["null", "string"], "description": f"為什麼（必填，至少 {MIN_REASON_CHARS} 個字，永久記錄）"},
}
CHECK_LABELS = {
    "ca_formula_agrees": "官方參考價無法由明細頁條件重算",
    "ca_open_ref_within_tick": "開盤競價基準與參考價差超過一檔",
    "ca_event_marked_x_in_quotes": "除權息當天行情沒有標 X",
    "ca_vendor_agrees": "與 FinMind 參考價不同",
    "ca_before_close_matches_silver": "事件前收盤與我們的前一日收盤不同",
    "manual_request": "人工要求覆核",
}
