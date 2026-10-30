"""
### twse_corporate_action_review

Opened by `twse_daily_etl` when a corporate action's sources disagree, or by hand for any event:
`make review STOCK=2762 DATE=2026-10-29`.

```
prepare ─> review (one form per event, Airflow UI "Required Actions") ─> apply
```

* **review**: the reviewer sees TWSE's numbers next to the formula, the vendor and our own close,
  then keeps TWSE's value or enters the correct reference price / previous close, with a reason.
  Airflow records who answered and when (the logged-in user).
* **apply**: validates the answer, appends it to Iceberg `review_decisions` (never edited) and
  re-runs the day from its Bronze files. corporate_actions and Gold get new snapshots with the
  reviewed factor; adjusted prices follow it from then on, including future reruns and backfills.

An invalid answer (no reason, factor out of range) fails `apply` without writing anything:
clear the `review` task to ask again.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from twse_etl.review_form import ACCEPT, FORM_FIELDS, OVERRIDE  # plain Python: cheap to parse

TPE = pendulum.timezone("Asia/Taipei")


def _settings():
    from twse_etl.config import Settings

    return Settings.from_env()


def _form_kwargs(prepared: dict) -> dict:
    return {"subject": prepared["subject"], "body": prepared["body"]}


@dag(
    dag_id="twse_corporate_action_review",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz=TPE),
    catchup=False,
    is_paused_upon_creation=False,  # triggered by twse_daily_etl; a paused DAG would queue reviews silently
    params={"items": Param([], type="array", description='[{"trade_date": "2026-10-29", "stock_id": "2762"}]')},
    default_args={"owner": "data-eng", "retries": 0},
    tags=["twse", "review"],
    doc_md=__doc__,
)
def twse_corporate_action_review():
    @task
    def prepare() -> list[dict]:
        from datetime import date

        from twse_etl import review

        settings = _settings()
        prepared = []
        for raw in get_current_context()["params"]["items"]:
            item = {"checks": "manual_request", **raw}
            if "run_date" not in item:
                item["run_date"] = review.run_date_for(date.fromisoformat(item["trade_date"]), settings).isoformat()
            prepared.append({"item": item, **review.form(item, review.evidence(item, settings))})
        if not prepared:
            raise AirflowFailException("no items to review")
        return prepared

    @task
    def apply(prepared: list[dict], responses: list[dict]) -> list[dict]:
        from twse_etl import review

        settings, run_id = _settings(), get_current_context()["run_id"]
        try:
            answered = [(p["item"], r) for p, r in zip(prepared, responses, strict=True)]
            decisions = review.record_decisions(answered, settings, run_id)
        except review.InvalidReviewError as exc:
            raise AirflowFailException(f"{exc}; clear the review task to answer again") from exc
        review.apply(decisions, settings, run_id)
        return decisions

    prepared = prepare()
    reviews = HITLOperator.partial(
        task_id="review",
        options=[ACCEPT, OVERRIDE],
        params={name: Param(None, **schema) for name, schema in FORM_FIELDS.items()},
    ).expand_kwargs(prepared.map(_form_kwargs))
    apply(prepared, reviews.output)


twse_corporate_action_review()
