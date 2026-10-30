"""CI job 'dag-integrity': the DAG imports cleanly and has no cycles. Runs inside the Airflow image."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.airflow
airflow = pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


def test_dags_import_without_errors_and_have_no_cycles():
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS_DIR))
    assert bag.import_errors == {}
    assert {"twse_daily_etl", "twse_corporate_action_review"} <= set(bag.dags)
    for dag in bag.dags.values():
        dag.check_cycle()  # raises on a cycle


def test_task_chain_matches_the_spec():
    from airflow.dag_processing.dagbag import DagBag

    dag = DagBag(dag_folder=str(DAGS_DIR)).dags["twse_daily_etl"]
    order = [
        "check_trading_day",
        "wait_for_reports",
        "route",
        "extract_quotes",
        "stage_quotes",
        "audit",
        "publish_silver",
        "build_gold",
        "reconcile",
        "request_review",
        "open_review",
    ]
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids, f"{upstream} -> {downstream}"
    assert "check_unscheduled_closure" in dag.get_task("route").downstream_task_ids
    assert dag.default_args["retries"] > 0
    assert dag.dagrun_timeout is not None  # one active run: a hung run must not block the next day
