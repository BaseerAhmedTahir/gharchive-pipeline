"""DAG integrity tests. These require Airflow, which doesn't run on Windows,
so they skip on the host and run inside the Airflow container:

    docker compose run --rm airflow-cli bash -c "cd /opt/airflow && python -m pytest tests/test_dags.py -q"
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="requires Airflow (run inside the container)")

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    # Airflow 3.3 DagBag has no include_examples param (examples are a
    # config concern now); our compose sets AIRFLOW__CORE__LOAD_EXAMPLES=false
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_present(dagbag):
    assert {"gharchive_hourly", "gharchive_daily"} <= set(dagbag.dag_ids)


def test_hourly_wiring_and_retry_policies(dagbag):
    dag = dagbag.get_dag("gharchive_hourly")

    assert dag.catchup is True
    assert dag.max_active_runs == 2  # sized from measured VM memory budget
    assert dag.get_task("transform").upstream_task_ids == {"ingest"}
    assert dag.get_task("quality_gate").upstream_task_ids == {"transform"}
    # per-failure-class retry policy
    assert dag.get_task("ingest").retries == 6
    assert dag.get_task("transform").retries == 3
    assert dag.get_task("quality_gate").retries == 0  # data bugs don't self-heal


def test_daily_wiring(dagbag):
    dag = dagbag.get_dag("gharchive_daily")

    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.get_task("prune_retention").upstream_task_ids == {"build_gold"}


def test_all_tasks_have_failure_callbacks(dagbag):
    for dag_id in ("gharchive_hourly", "gharchive_daily"):
        for task in dagbag.get_dag(dag_id).tasks:
            assert task.on_failure_callback, f"{dag_id}.{task.task_id} has no callback"
