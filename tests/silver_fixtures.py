"""Helpers to write silver Parquet partitions directly for quality/aggregate tests."""

import csv
from datetime import datetime

import duckdb

from pipeline.config import hour_partition
from pipeline.transform import SILVER_COLUMNS, SILVER_FILENAME

_SCHEMA = """(
    id VARCHAR, type VARCHAR, created_at TIMESTAMP, public BOOLEAN,
    actor_login VARCHAR, repo_id BIGINT, repo_name VARCHAR, org_login VARCHAR,
    payload_action VARCHAR, pr_number BIGINT, pr_merged_at TIMESTAMP,
    push_commits INTEGER
)"""


def silver_row(event_id, created_at, **overrides):
    row = {
        "id": event_id,
        "type": "PushEvent",
        "created_at": created_at,
        "public": True,
        "actor_login": "octocat",
        "repo_id": 99,
        "repo_name": "octo/hello",
        "org_login": None,
        "payload_action": None,
        "pr_number": None,
        "pr_merged_at": None,
        "push_commits": 1,
    }
    row.update(overrides)
    return row


def _csv_value(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return value


def write_silver(cfg, hour_dt: datetime, rows):
    partition = hour_partition(cfg.silver_events_dir, hour_dt)
    partition.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    # route through CSV: both executemany and big multi-row VALUES lists are
    # pathologically slow in duckdb (planner cost per tuple/parameter)
    csv_path = partition / "_fixture.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow([_csv_value(row[c]) for c in SILVER_COLUMNS])
    con.execute(f"CREATE TABLE t {_SCHEMA}")
    con.execute(f"COPY t FROM '{csv_path.as_posix()}' (HEADER false, NULL '')")
    csv_path.unlink()
    con.execute(f"COPY t TO '{(partition / SILVER_FILENAME).as_posix()}' (FORMAT PARQUET)")
    con.close()
