"""Hourly GH Archive DAG: ingest -> transform -> quality_gate.

Each run owns exactly one UTC hour (its data interval); every task is
idempotent (atomic writes, overwrite-partition), so retries, manual
re-runs, concurrent runs, and catchup backfills are safe by construction.

Retry policy is per failure class:
- ingest: most retries, spaced out — a 404 means GH Archive hasn't
  published the hour yet, which heals on the orchestrator's timescale.
- transform: default retries for transient infra failures.
- quality_gate: retries=0 — a hard data-quality failure is never
  self-healing; retrying re-checks the same bad data. Fail once, alert,
  wait for a human.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from pipeline.alerts import notify_failure


def _hour(data_interval_start):
    # pipeline functions take naive UTC datetimes
    return data_interval_start.replace(tzinfo=None)


def _ingest(*, data_interval_start, **_):
    from pipeline.ingest import ingest_hour

    ingest_hour(_hour(data_interval_start))


def _transform(*, data_interval_start, **_):
    from pipeline.transform import transform_hour

    transform_hour(_hour(data_interval_start))


def _quality_gate(*, data_interval_start, **_):
    from pipeline.quality import check_hour

    check_hour(_hour(data_interval_start))


with DAG(
    dag_id="gharchive_hourly",
    description="Ingest, transform and quality-check one GH Archive hour per run",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 7, 9, tz="UTC"),
    catchup=True,
    max_active_runs=4,
    dagrun_timeout=timedelta(hours=2),
    tags=["gharchive"],
    default_args={
        "owner": "data-eng",
        "on_failure_callback": notify_failure,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
) as dag:
    ingest = PythonOperator(
        task_id="ingest",
        python_callable=_ingest,
        retries=6,
        retry_delay=timedelta(minutes=2),
    )
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=_quality_gate,
        retries=0,
    )

    ingest >> transform >> quality_gate
