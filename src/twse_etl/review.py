"""Human review of corporate actions whose sources the cross-checks could not reconcile.

    twse_daily_etl ── events_to_review ──> twse_corporate_action_review: one form per event under
    Airflow UI "Required Actions" ──> record_decision ──> the day re-runs from its Bronze files

The reviewer sees every source's numbers, keeps TWSE's value or types the correct ones, and must
give a reason. Who answered and when come from Airflow (the authenticated user), never from a
typed name. Decisions are appended to Iceberg review_decisions and never edited; the latest one
per event is applied whenever the event is staged, so reruns and backfills keep it. The re-run
writes new snapshots of corporate_actions and Gold, so the factor in force before the review
stays queryable by time travel.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import polars as pl
import pyarrow as pa

from twse_etl import contracts, corporate_actions, iceberg_io, pipeline
from twse_etl.config import Settings
from twse_etl.review_form import ACCEPT, CHECK_LABELS, DECISIONS, MIN_REASON_CHARS, OVERRIDE

EVIDENCE_COLUMNS = (
    "stock_name",
    "action_type",
    "source_report",
    "note",
    "before_close",
    "ref_price",
    "calc_ref_price",
    "formula_agrees",
    "open_ref_price",
    "dividend_ref_price",
    "vendor_ref_price",
    "adj_factor",
    "review_decision_id",
)


class InvalidReviewError(ValueError):
    """The response cannot be applied; the reviewer answers the form again."""


def _plain(value):
    return str(value) if isinstance(value, Decimal | date) else value


def run_date_for(trade_date: date, settings: Settings) -> date:
    """The pipeline day whose corporate-action window holds the event (an event dated on a closure
    day takes effect on the next loaded trading day)."""
    loaded = iceberg_io.loaded_dates(iceberg_io.get_table(settings, "daily_quotes"))
    later = [d for d in loaded if d >= trade_date]
    if not later:
        raise LookupError(f"no loaded trading day on or after {trade_date}")
    return later[0]


def evidence(item: dict, settings: Settings) -> dict:
    """Every source's numbers for one event, as the reviewer sees them (also stored with the decision)."""
    d = date.fromisoformat(item["trade_date"])
    actions = iceberg_io.read(iceberg_io.get_table(settings, "corporate_actions"), d)
    rows = actions.filter(pl.col("stock_id") == item["stock_id"])
    if rows.is_empty():
        raise LookupError(f"no corporate action for {item['stock_id']} on {d}")
    event = rows.row(0, named=True)
    out = {col: _plain(event[col]) for col in EVIDENCE_COLUMNS}
    quotes = iceberg_io.get_table(settings, "daily_quotes")
    prev = iceberg_io.latest_date_before(quotes, d)
    closes = iceberg_io.read(quotes, prev).filter(pl.col("stock_id") == item["stock_id"]) if prev else None
    out["silver_prev_date"] = _plain(prev)
    out["silver_prev_close"] = _plain(closes["close"][0]) if closes is not None and closes.height else None
    return out


def form(item: dict, ev: dict) -> dict[str, str]:
    """Subject and body of the review form (markdown in the Airflow UI)."""
    checks = "、".join(CHECK_LABELS.get(name, name) for name in item["checks"].split(","))
    reviewed = f"（已有人審過：{ev['review_decision_id']}）" if ev["review_decision_id"] else ""
    rows = [
        ("TWSE 公告（" + str(ev["source_report"]) + "）", ev["before_close"], ev["ref_price"]),
        ("明細頁條件重算", None, ev["calc_ref_price"]),
        ("開盤競價基準", None, ev["open_ref_price"]),
        ("減除股利參考價", None, ev["dividend_ref_price"]),
        ("FinMind", None, ev["vendor_ref_price"]),
        (f"我們的 Silver 收盤（{ev['silver_prev_date']}）", ev["silver_prev_close"], None),
    ]
    table = "\n".join(f"| {name} | {before or ''} | {ref or ''} |" for name, before, ref in rows if before or ref)
    body = (
        f"**對不上的地方**：{checks}\n\n"
        f"| 來源 | 事件前收盤 | 參考價 |\n|---|---|---|\n{table}\n\n"
        f"目前使用的調整因子：**{ev['adj_factor']}**{reviewed}\n\n"
        f"選「{ACCEPT}」，或填入正確的參考價／事件前收盤後選「{OVERRIDE}」。理由必填，連同你的帳號與時間永久記錄。"
    )
    subject = f"{item['stock_id']} {ev['stock_name']} {item['trade_date']} {ev['action_type']}：{checks}"
    return {"subject": subject, "body": body}


def _price(value, fallback: Decimal) -> Decimal:
    return fallback if value is None else Decimal(str(value)).quantize(Decimal("0.01"))


def _decision(item: dict, response: dict, settings: Settings, review_run_id: str) -> dict:
    """One HITL response as a review_decisions row, or InvalidReviewError."""
    choice = (response.get("chosen_options") or [None])[0]
    if choice not in DECISIONS:
        raise InvalidReviewError(f"unknown option {choice!r}")
    answers = response.get("params_input") or {}
    reason = (answers.get("reason") or "").strip()
    if len(reason) < MIN_REASON_CHARS:
        raise InvalidReviewError(f"a reason of at least {MIN_REASON_CHARS} characters is required")

    ev = evidence(item, settings)
    twse_ref, twse_before = Decimal(ev["ref_price"]), Decimal(ev["before_close"])
    ref, before = twse_ref, twse_before
    if DECISIONS[choice] == "override":
        if answers.get("ref_price") is None and answers.get("before_close") is None:
            raise InvalidReviewError("enter the correct ref_price and/or before_close to override")
        ref, before = _price(answers.get("ref_price"), twse_ref), _price(answers.get("before_close"), twse_before)
    factor = float(ref) / float(before)
    in_range = pl.DataFrame({"action_type": [ev["action_type"]], "adj_factor": [factor]}).select(
        corporate_actions.factor_in_range()
    )
    if not in_range.item():
        raise InvalidReviewError(f"adj_factor {factor:.6f} is out of range for {ev['action_type']}")

    user = response["responded_by_user"]
    responded_at = response["responded_at"]
    if isinstance(responded_at, str):
        responded_at = datetime.fromisoformat(responded_at)
    row = {
        "decision_id": uuid.uuid4().hex,
        "trade_date": date.fromisoformat(item["trade_date"]),
        "stock_id": item["stock_id"],
        "run_date": date.fromisoformat(item["run_date"]),
        "checks": item["checks"],
        "decision": DECISIONS[choice],
        "twse_ref_price": twse_ref,
        "twse_before_close": twse_before,
        "twse_adj_factor": float(twse_ref) / float(twse_before),
        "ref_price": ref,
        "before_close": before,
        "adj_factor": factor,
        "reason": reason,
        "reviewed_by": user["name"],
        "reviewed_by_id": str(user["id"]),
        "reviewed_at": responded_at.astimezone(UTC),
        "review_run_id": review_run_id,
        "evidence": json.dumps(ev, ensure_ascii=False),
    }
    return row


def record_decisions(answered: list[tuple[dict, dict]], settings: Settings, review_run_id: str) -> list[dict]:
    """Validate every (item, HITL response) first, then append them all in one snapshot: an
    invalid answer writes nothing, so answering the form again never duplicates a decision."""
    rows = [_decision(item, response, settings, review_run_id) for item, response in answered]
    tbl = iceberg_io.get_table(settings, "review_decisions")
    tbl.append(
        pa.Table.from_pylist(rows, schema=contracts.REVIEW_DECISIONS_SCHEMA),
        snapshot_properties={"review_run_id": review_run_id},
    )
    return [
        {key: _plain(value) if key != "reviewed_at" else value.isoformat() for key, value in r.items()} for r in rows
    ]


def apply(decisions: list[dict], settings: Settings, review_run_id: str) -> dict[str, dict]:
    """Re-run each affected day once, from the payloads it last loaded."""
    days = sorted({d["run_date"] for d in decisions})
    return {
        day: pipeline.rerun_from_bronze(date.fromisoformat(day), settings, run_id=f"review__{review_run_id}")
        for day in days
    }
