"""
### twse_daily_etl

Every weekday 15:30 Asia/Taipei: TWSE closing quotes (MI_INDEX) + institutional flows (T86)
-> Bronze raw JSON -> staged parquet -> audit -> Iceberg Silver -> Gold merged table -> row-count check.

```
check_trading_day ─> wait_for_reports ─> route ─┬─> extract_* ─> stage_* ─> audit
                                                │      ─> publish_silver ─> build_gold ─> reconcile
                                                └─> check_unscheduled_closure   (cutoff passed, no data)
```

* **Not a trading day** (weekend / official holiday): `check_trading_day` short-circuits, the run is green.
* **Late data**: the sensor re-checks every 10 min, alerts after 17:00, gives up at 19:00. A TWSE
  outage while waiting is "not ready yet", not a task failure.
* **No data at 19:00 on a 'trading day'** (typhoon closures are never in the calendar): TWSE's own
  intraday statistics (MI_5MINS) decide. No intraday records -> closure, recorded, run green;
  the market traded -> fail + alert.
* **Bad data** (audit failure, schema drift): fail fast, no retries, alert. Iceberg is untouched,
  because publishing only starts after every check passed. Corporate actions are the exception:
  a reshaped table or an unusable row is dropped with an alert, so quotes and flows still load.
* **Counts off after publishing** (reconcile): the run is undone in every table, Gold first, then
  the task fails and alerts. Readers stay on the last good version.

Manual run for a given day: trigger with config `{"trade_date": "2026-10-29"}`.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, PokeReturnValue, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.trigger import CronTriggerTimetable

TPE = pendulum.timezone("Asia/Taipei")
REPORTS = {"MI_INDEX": "quotes", "T86": "institutional", "CORPORATE_ACTIONS": "corporate_actions"}

log = logging.getLogger(__name__)


def _settings():
    from twse_etl.config import Settings  # heavy imports stay out of DAG parsing

    return Settings.from_env()


def _run(fn, *args):
    """Data problems are not fixed by retrying: convert them to AirflowFailException (no retries)."""
    from twse_etl.pipeline import MissingDataError
    from twse_etl.transform import SchemaDriftError
    from twse_etl.validate import DataQualityError

    try:
        return fn(*args)
    except (DataQualityError, SchemaDriftError, MissingDataError) as exc:
        raise AirflowFailException(f"{type(exc).__name__}: {exc}") from exc


def _on_failure(context) -> None:
    """Runs once retries are exhausted (or on AirflowFailException). Locally: an ALERT log line."""
    from twse_etl.alerts import alert

    ti = context["ti"]
    alert(
        "task_failed",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        try_number=ti.try_number,
        error=repr(context.get("exception")),
    )


def _on_dag_run_failure(context) -> None:
    """Also fires when the whole run times out, which no task callback would report."""
    from twse_etl.alerts import alert

    run = context["dag_run"]
    alert("dag_run_failed", dag_id=run.dag_id, run_id=run.run_id, reason=context.get("reason"))


@dag(
    dag_id="twse_daily_etl",
    schedule=CronTriggerTimetable("30 15 * * 1-5", timezone="Asia/Taipei"),
    start_date=pendulum.datetime(2026, 10, 1, tz=TPE),
    catchup=False,
    max_active_runs=1,  # days load in order: price continuity reads the previous day from Silver
    # With one active run, a hung run would block the next trading day; fail it (and alert) first.
    dagrun_timeout=timedelta(hours=8),
    on_failure_callback=_on_dag_run_failure,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _on_failure,
    },
    params={"trade_date": Param("", type="string", description="YYYY-MM-DD; empty = the scheduled day")},
    tags=["twse", "iceberg"],
    doc_md=__doc__,
)
def twse_daily_etl():
    @task.short_circuit(retries=3, retry_delay=timedelta(minutes=2), retry_exponential_backoff=True)
    def check_trading_day() -> str | bool:
        from twse_etl.pipeline import trading_day_status

        ctx = get_current_context()
        override = ctx["params"].get("trade_date")
        when = ctx.get("logical_date") or ctx["dag_run"].run_after
        d = date.fromisoformat(override) if override else pendulum.instance(when).in_tz(TPE).date()
        status = trading_day_status(d, _settings())
        log.info("%s: %s", d, status.reason)
        return d.isoformat() if status.is_trading_day else False

    @task.sensor(poke_interval=600, timeout=6 * 3600, mode="reschedule")
    def wait_for_reports(trade_date: str) -> PokeReturnValue:
        """A poke that cannot reach TWSE is 'not ready yet' until the cutoff: the data normally
        arrives long before 19:00, and failing the run over a 15-minute outage would need a person."""
        import requests

        from twse_etl.alerts import alert
        from twse_etl.pipeline import CUTOFF_HOUR, DELAY_ALERT_HOUR, reports_ready

        d = date.fromisoformat(trade_date)
        now = pendulum.now(TPE)
        day_start = pendulum.datetime(d.year, d.month, d.day, tz=TPE)
        try:
            counts = reports_ready(d, _settings())
        except requests.RequestException as exc:
            if now >= day_start.add(hours=CUTOFF_HOUR):
                raise
            log.warning("%s: TWSE unreachable (%s); will poke again", trade_date, exc)
            return PokeReturnValue(is_done=False)
        if all(counts.values()):
            return PokeReturnValue(is_done=True, xcom_value="ready")
        if now >= day_start.add(hours=CUTOFF_HOUR):
            alert("data_wait_cutoff", trade_date=trade_date, row_counts=counts)
            return PokeReturnValue(is_done=True, xcom_value="missing")
        if now >= day_start.add(hours=DELAY_ALERT_HOUR):
            alert("data_delayed", trade_date=trade_date, row_counts=counts)
        return PokeReturnValue(is_done=False)

    @task.branch
    def route(status: str) -> list[str] | str:
        return [f"extract_{slug}" for slug in REPORTS.values()] if status == "ready" else "check_unscheduled_closure"

    @task(retries=2, retry_delay=timedelta(minutes=2))  # network only: MissingDataError skips retries
    def check_unscheduled_closure(trade_date: str) -> str:
        from twse_etl.pipeline import resolve_missing_data

        reason = _run(resolve_missing_data, date.fromisoformat(trade_date), _settings())
        log.info("%s treated as market closure: %s", trade_date, reason)
        return reason

    @task(retries=3, retry_delay=timedelta(minutes=2), retry_exponential_backoff=True)
    def extract(report: str, trade_date: str) -> str:
        from twse_etl.pipeline import extract as extract_report

        return extract_report(report, date.fromisoformat(trade_date), _settings())

    @task
    def stage(report: str, trade_date: str, bronze_uri: str) -> dict:
        from twse_etl.pipeline import stage as stage_report

        run_id = get_current_context()["run_id"]
        return _run(stage_report, report, date.fromisoformat(trade_date), bronze_uri, _settings(), run_id)

    @task(retries=0)
    def audit(trade_date: str, quotes: dict, institutional: dict, corporate_actions: dict) -> dict:
        from twse_etl.pipeline import audit as audit_day

        staged = {"MI_INDEX": quotes, "T86": institutional, "CORPORATE_ACTIONS": corporate_actions}
        return _run(audit_day, date.fromisoformat(trade_date), staged, _settings())

    @task
    def publish_silver(trade_date: str, quotes: dict, institutional: dict, corporate_actions: dict, _audit: dict):
        """One task writes all Silver tables: the SQLite catalog allows a single writer at a time."""
        from twse_etl.pipeline import publish

        staged = {"MI_INDEX": quotes, "T86": institutional, "CORPORATE_ACTIONS": corporate_actions}
        return publish(date.fromisoformat(trade_date), staged, _settings(), get_current_context()["run_id"])

    @task
    def build_gold(trade_date: str, _published: dict) -> dict:
        from twse_etl.pipeline import build_gold as build

        return build(date.fromisoformat(trade_date), _settings(), get_current_context()["run_id"])

    @task(retries=0)
    def reconcile(
        trade_date: str, quotes: dict, institutional: dict, corporate_actions: dict, audit_report: dict, _gold: dict
    ) -> dict:
        from twse_etl.pipeline import reconcile as check

        staged = {"MI_INDEX": quotes, "T86": institutional, "CORPORATE_ACTIONS": corporate_actions}
        run_id = get_current_context()["run_id"]
        manifest = _run(check, date.fromisoformat(trade_date), _settings(), run_id, staged, audit_report)
        return {"snapshots": manifest["snapshots"], "row_counts": manifest["row_counts"]}

    trade_date = check_trading_day()
    routed = route(wait_for_reports(trade_date))
    routed >> check_unscheduled_closure(trade_date)

    staged = {}
    for report, slug in REPORTS.items():
        bronze = extract.override(task_id=f"extract_{slug}")(report, trade_date)
        routed >> bronze
        staged[slug] = stage.override(task_id=f"stage_{slug}")(report, trade_date, bronze)

    audit_report = audit(trade_date, **staged)
    published = publish_silver(trade_date, **staged, _audit=audit_report)
    gold = build_gold(trade_date, published)
    reconcile(trade_date, **staged, audit_report=audit_report, _gold=gold)


twse_daily_etl()
