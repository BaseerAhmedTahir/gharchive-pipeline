import json
from datetime import datetime

import pytest

from dashboard import queries
from pipeline.config import Config
from pipeline.metrics import record

HOUR = datetime(2026, 7, 9, 15)


@pytest.fixture
def cfg(tmp_path):
    return Config(data_root=tmp_path)


def test_record_writes_one_json_per_kind_hour(cfg):
    path = record("transform", HOUR, cfg=cfg, rows_in=100, rows_out=99, duration_seconds=2.5)

    assert path == cfg.metrics_dir / "date=2026-07-09" / "transform_hour=15.json"
    payload = json.loads(path.read_text())
    assert payload["kind"] == "transform"
    assert payload["rows_in"] == 100
    assert "recorded_at" in payload


def test_rerun_overwrites_instead_of_duplicating(cfg):
    record("ingest", HOUR, cfg=cfg, bytes_downloaded=1, duration_seconds=1.0, skipped=False)
    record("ingest", HOUR, cfg=cfg, bytes_downloaded=2, duration_seconds=1.0, skipped=False)

    files = list(cfg.metrics_dir.rglob("*.json"))
    assert len(files) == 1
    assert json.loads(files[0].read_text())["bytes_downloaded"] == 2
    assert list(cfg.metrics_dir.rglob("*.tmp")) == []


def test_queries_read_metrics_history(cfg):
    record("ingest", HOUR, cfg=cfg, bytes_downloaded=20_000_000,
           duration_seconds=15.0, skipped=False)
    record("transform", HOUR, cfg=cfg, rows_in=160_000, rows_out=159_990,
           bytes_out=4_000_000, duration_seconds=2.0)
    record("quality_gate", HOUR, cfg=cfg, rows=159_990, soft_warnings=0)

    con = queries.open_connection(cfg)
    try:
        runs = queries.run_metrics(con, cfg)
        summary = queries.throughput_summary(con, cfg)
    finally:
        con.close()

    assert [r["kind"] for r in runs] == ["ingest", "quality_gate", "transform"]
    assert summary["transform_runs"] == 1
    assert summary["median_events_per_sec"] == pytest.approx(80_000)
    assert summary["gb_ingested"] == pytest.approx(0.02)


def test_queries_are_empty_safe_without_metrics(cfg):
    con = queries.open_connection(cfg)
    try:
        assert queries.run_metrics(con, cfg) == []
        assert queries.throughput_summary(con, cfg) is None
    finally:
        con.close()
