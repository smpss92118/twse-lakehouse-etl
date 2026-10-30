"""Human review: a disputed corporate action is opened once, the reviewer's answer is recorded with
who / when / why, and the reviewed factor flows into Silver and Gold as new versions."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from conftest import make_settings

from twse_etl import iceberg_io, pipeline, review
from twse_etl.review_form import ACCEPT, OVERRIDE

D = date(2026, 10, 29)  # 2762 goes ex-dividend: before_close 80.00, TWSE reference price 78.19


@pytest.fixture
def disputed(tmp_path):
    """10-29 loaded while the vendor reports 78.50 for 2762 instead of TWSE's 78.19."""
    vendor = tmp_path / "vendor"
    vendor.mkdir()
    rows = [{"date": D.isoformat(), "stock_id": "2762", "reference_price": 78.50}]
    (vendor / "TaiwanStockDividendResult_test.json").write_text(json.dumps({"data": rows}))
    settings = make_settings(tmp_path / "lake").with_overrides(vendor_dir=vendor)
    pipeline.run_day(D, settings)
    return settings


def _answer(option: str, reason: str = "公司重訊更正參考價", **values) -> dict:
    form = {"ref_price": None, "before_close": None, "reason": reason, **values}
    user = {"id": "trader", "name": "trader"}
    return {
        "chosen_options": [option],
        "params_input": form,
        "responded_by_user": user,
        "responded_at": datetime.now(UTC),
    }


def _event(settings, table: str, snapshot_id: int | None = None) -> dict:
    rows = iceberg_io.read(iceberg_io.get_table(settings, table), D, snapshot_id=snapshot_id)
    return rows.filter(rows["stock_id"] == "2762").row(0, named=True)


def test_override_is_recorded_and_flows_into_new_versions(disputed):
    settings = disputed
    items = pipeline.events_to_review(D, settings)
    assert items == [
        {"trade_date": "2026-10-29", "stock_id": "2762", "checks": "ca_vendor_agrees", "run_date": "2026-10-29"}
    ]
    before = iceberg_io.current_version(iceberg_io.get_table(settings, "daily_market_wide"), D).snapshot_id

    [decision] = review.record_decisions([(items[0], _answer(OVERRIDE, ref_price=78.5))], settings, "review-1")
    review.apply([decision], settings, "review-1")

    assert decision["decision"] == "override"
    assert (decision["reviewed_by"], decision["reason"]) == ("trader", "公司重訊更正參考價")  # who, why
    silver = _event(settings, "corporate_actions")
    assert silver["ref_price"] == Decimal("78.19")  # TWSE's number is kept as published
    assert silver["adj_factor"] == pytest.approx(78.5 / 80)  # the factor in use is the reviewer's
    assert silver["review_decision_id"] == decision["decision_id"]
    assert _event(settings, "daily_market_wide")["adj_factor"] == pytest.approx(78.5 / 80)
    assert _event(settings, "daily_market_wide", before)["adj_factor"] == pytest.approx(78.19 / 80)  # time travel
    assert pipeline.events_to_review(D, settings) == []  # still disputed, but decided: never reopened


def test_accepting_twse_keeps_the_factor_and_closes_the_review(disputed):
    settings = disputed
    [item] = pipeline.events_to_review(D, settings)
    review.apply(review.record_decisions([(item, _answer(ACCEPT))], settings, "review-1"), settings, "review-1")
    assert _event(settings, "corporate_actions")["adj_factor"] == pytest.approx(78.19 / 80)
    assert pipeline.events_to_review(D, settings) == []


@pytest.mark.parametrize(
    "bad",
    [
        _answer(ACCEPT, reason=""),
        _answer(OVERRIDE),  # nothing entered to override with
        _answer(OVERRIDE, ref_price=95.0),  # a dividend cannot raise the price
        _answer("approve"),
    ],
    ids=["no_reason", "override_without_values", "factor_out_of_range", "unknown_option"],
)
def test_an_invalid_answer_writes_nothing_even_after_a_valid_one(disputed, bad):
    settings = disputed
    [item] = pipeline.events_to_review(D, settings)
    with pytest.raises(review.InvalidReviewError):
        review.record_decisions([(item, _answer(ACCEPT)), (item, bad)], settings, "review-1")
    assert pipeline.review_decisions(settings).is_empty()
