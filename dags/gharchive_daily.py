"""Daily GH Archive DAG: rebuild gold marts, then prune retention.

Gold is a full rebuild from whatever silver exists — trivially idempotent —
so this DAG needs no catchup: any run produces the current best gold.
Retention pruning is wall-clock-relative housekeeping and rides along here
(once a day is enough; see pipeline/cleanup.py for why wall-clock).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from pipeline.alerts import notify_failure


def _build_gold(*, data_interval_start, **_):
    from pipeline.aggregate import build_gold
    from pipeline.metrics import record

    r = build_gold()
    record(
        "build_gold",
        data_interval_start.replace(tzinfo=None),
        duration_seconds=r.duration_seconds,
        **{f"rows_{mart}": n for mart, n in r.rows_per_mart.items()},
    )


def _prune_retention(*, data_interval_start, **_):
    from pipeline.cleanup import prune_retention
    from pipeline.metrics import record

    r = prune_retention()
    record(
        "prune_retention",
        data_interval_start.replace(tzinfo=None),
        partitions_removed=len(r.removed),
    )


with DAG(
    dag_id="gharchive_daily",
    description="Rebuild gold marts from the full silver window; prune retention",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 9, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["gharchive"],
    default_args={
        "owner": "data-eng",
        "on_failure_callback": notify_failure,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:
    build_gold = PythonOperator(task_id="build_gold", python_callable=_build_gold)
    prune_retention = PythonOperator(
        task_id="prune_retention", python_callable=_prune_retention
    )

    build_gold >> prune_retention
