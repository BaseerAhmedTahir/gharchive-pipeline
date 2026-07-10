import gzip
import json
from datetime import datetime

import duckdb
import pytest

from pipeline.config import Config, hour_partition
from pipeline.ingest import BRONZE_FILENAME
from pipeline.transform import BronzeNotFoundError, SILVER_FILENAME, transform_hour

HOUR = datetime(2026, 7, 9, 15)


@pytest.fixture
def cfg(tmp_path):
    return Config(data_root=tmp_path, duckdb_memory_limit="512MB", duckdb_threads=2)


def make_event(event_id, type="PushEvent", created="2026-07-09T15:00:01Z", **overrides):
    event = {
        "id": event_id,
        "type": type,
        "created_at": created,
        "public": True,
        "actor": {"id": 1, "login": "octocat"},
        "repo": {"id": 99, "name": "octo/hello"},
        "payload": {"size": 3},
    }
    event.update(overrides)
    return event


FIXTURE_EVENTS = [
    make_event("1"),
    make_event("1"),  # exact duplicate -> dedup keeps one
    make_event(
        "2",
        type="PullRequestEvent",
        created="2026-07-09T15:05:00Z",
        org={"id": 7, "login": "duckdb"},
        payload={
            "action": "closed",
            "pull_request": {"number": 42, "merged_at": "2026-07-09T15:03:00Z"},
        },
    ),
    # minimal event: no org, unknown top-level field, empty payload
    make_event("3", type="WatchEvent", payload={}, some_future_field={"x": 1}),
]


def write_bronze(cfg, events, hour=HOUR):
    path = hour_partition(cfg.bronze_dir, hour) / BRONZE_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    return path


def read_silver(result):
    con = duckdb.connect()
    cur = con.execute(
        f"SELECT * FROM read_parquet('{result.path.as_posix()}') ORDER BY id, created_at"
    )
    columns = [d[0] for d in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    con.close()
    return rows


def test_typed_projection_and_schema(cfg):
    write_bronze(cfg, FIXTURE_EVENTS)

    result = transform_hour(HOUR, cfg=cfg)
    rows = read_silver(result)

    push = rows[0]
    assert push["type"] == "PushEvent"
    assert push["actor_login"] == "octocat"
    assert push["repo_id"] == 99
    assert push["repo_name"] == "octo/hello"
    assert push["created_at"] == datetime(2026, 7, 9, 15, 0, 1)  # real timestamp, not string
    assert push["push_commits"] == 3
    assert push["org_login"] is None

    pr = rows[1]
    assert pr["payload_action"] == "closed"
    assert pr["pr_number"] == 42
    assert pr["pr_merged_at"] == datetime(2026, 7, 9, 15, 3, 0)
    assert pr["org_login"] == "duckdb"

    minimal = rows[2]
    assert minimal["type"] == "WatchEvent"
    assert minimal["push_commits"] is None  # empty payload -> NULLs, not a crash


def test_deduplicates_exact_duplicate_events(cfg):
    write_bronze(cfg, FIXTURE_EVENTS)

    result = transform_hour(HOUR, cfg=cfg)

    assert result.rows_in == 4
    assert result.rows_out == 3
    assert [r["id"] for r in read_silver(result)] == ["1", "2", "3"]


def test_id_collision_with_different_content_is_preserved(cfg):
    # Same id but genuinely different events: NOT silently collapsed —
    # both rows survive so the quality gate can fail loudly on it.
    write_bronze(
        cfg,
        [
            make_event("1", created="2026-07-09T15:00:01Z"),
            make_event("1", created="2026-07-09T15:00:02Z"),
        ],
    )

    result = transform_hour(HOUR, cfg=cfg)

    assert result.rows_out == 2


def test_rerun_overwrites_partition_identically(cfg):
    write_bronze(cfg, FIXTURE_EVENTS)

    first = transform_hour(HOUR, cfg=cfg)
    rows_first = read_silver(first)
    second = transform_hour(HOUR, cfg=cfg)
    rows_second = read_silver(second)

    assert rows_first == rows_second
    assert second.rows_out == first.rows_out
    # no .tmp partitions left behind
    assert list(cfg.silver_events_dir.rglob("*.tmp")) == []


def test_missing_bronze_raises(cfg):
    with pytest.raises(BronzeNotFoundError):
        transform_hour(HOUR, cfg=cfg)


def test_missing_and_malformed_fields_do_not_fail_the_hour(cfg):
    write_bronze(
        cfg,
        [
            make_event("1", created="not-a-timestamp"),  # TRY_CAST -> NULL
            {"id": "2", "type": "GollumEvent", "created_at": "2026-07-09T15:10:00Z"},
        ],
    )

    result = transform_hour(HOUR, cfg=cfg)
    rows = read_silver(result)

    assert result.rows_out == 2
    assert rows[0]["created_at"] is None
    assert rows[1]["actor_login"] is None


def test_spills_to_disk_under_low_memory_limit(cfg):
    # 500k realistic rows whose in-flight working set exceeds the 64 MB
    # limit: the dedup hash aggregate must spill to temp_directory rather
    # than OOM. (This test is what forced the two-phase transform design —
    # DuckDB's window operator cannot spill, so dedup uses DISTINCT.)
    path = hour_partition(cfg.bronze_dir, HOUR) / BRONZE_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=1) as f:
        for i in range(500_000):
            f.write(
                '{"id":"%d","type":"PushEvent","created_at":"2026-07-09T15:00:01Z",'
                '"public":true,"actor":{"login":"user%d"},'
                '"repo":{"id":%d,"name":"org%d/repo%d"},"payload":{"size":1}}\n'
                % (i, i % 1000, i % 5000, i % 500, i % 5000)
            )
    low_mem = Config(data_root=cfg.data_root, duckdb_memory_limit="64MB", duckdb_threads=1)

    result = transform_hour(HOUR, cfg=low_mem)

    assert result.rows_in == 500_000
    assert result.rows_out == 500_000
    assert low_mem.duckdb_tmp_dir.exists()  # spill directory was provisioned
