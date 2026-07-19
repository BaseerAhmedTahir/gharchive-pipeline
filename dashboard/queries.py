"""Data access for the dashboard: plain functions over gold/silver Parquet.

No streamlit imports — everything here is unit-testable. Rows come back as
list[dict] so the UI layer decides about dataframes.
"""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from pipeline.config import Config, get_config
from pipeline.transform import connect


def dashboard_config() -> Config:
    """Host-side config with a small explicit DuckDB budget: the dashboard
    must not inherit the 2GB pipeline default on an 8GB laptop (same
    exit-137 logic as in the containers, host edition)."""
    return replace(
        get_config(),
        duckdb_memory_limit=os.environ.get("DASHBOARD_DUCKDB_MEMORY_LIMIT", "256MB"),
        duckdb_threads=2,
    )


def open_connection(cfg: Config) -> duckdb.DuckDBPyConnection:
    return connect(cfg)


def _mart(cfg: Config, name: str) -> str:
    return (cfg.gold_dir / f"{name}.parquet").as_posix()


def _rows(cur) -> list[dict]:
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def summary(con, cfg: Config) -> dict:
    events = con.execute(
        f"""SELECT sum(events), sum(bot_events), count(DISTINCT event_date),
                   min(event_date), max(event_date)
            FROM read_parquet('{_mart(cfg, "event_type_daily")}')"""
    ).fetchone()
    repos, actors_proxy = con.execute(
        f"""SELECT count(DISTINCT repo_name), sum(human_actors)
            FROM read_parquet('{_mart(cfg, "repo_activity_daily")}')"""
    ).fetchone()
    total, bots, days, first_day, last_day = events
    return {
        "total_events": total or 0,
        "bot_share": (bots / total) if total else 0.0,
        "days": days or 0,
        "first_day": first_day,
        "last_day": last_day,
        "events_per_day": (total / days) if days else 0,
        "distinct_repos": repos or 0,
    }


def daily_type_mix(con, cfg: Config, top_n: int = 6) -> list[dict]:
    """Events per day per type; types beyond the top N fold into 'Other'
    (fixed categorical slots — never more series than slots)."""
    return _rows(
        con.execute(
            f"""
            WITH ranked AS (
                SELECT type, sum(events) AS total,
                       row_number() OVER (ORDER BY sum(events) DESC) AS rank
                FROM read_parquet('{_mart(cfg, "event_type_daily")}')
                GROUP BY type
            )
            SELECT m.event_date,
                   CASE WHEN r.rank <= {top_n} THEN m.type ELSE 'Other' END AS type,
                   sum(m.events) AS events,
                   sum(m.bot_events) AS bot_events,
                   sum(m.human_events) AS human_events
            FROM read_parquet('{_mart(cfg, "event_type_daily")}') m
            JOIN ranked r USING (type)
            GROUP BY 1, 2
            ORDER BY event_date, events DESC
            """
        )
    )


def type_order(con, cfg: Config, top_n: int = 6) -> list[str]:
    """Stable series order (by overall volume) for fixed color-slot assignment."""
    rows = _rows(
        con.execute(
            f"""SELECT type, sum(events) AS total
                FROM read_parquet('{_mart(cfg, "event_type_daily")}')
                GROUP BY type ORDER BY total DESC LIMIT {top_n}"""
        )
    )
    order = [r["type"] for r in rows]
    return order + ["Other"]


def top_repos(con, cfg: Config, limit: int = 15) -> list[dict]:
    """Most active repos by human events, multi-actor only (the behavioral
    bot guard from the gold layer, applied consistently here)."""
    return _rows(
        con.execute(
            f"""
            SELECT repo_name,
                   sum(human_events) AS human_events,
                   sum(events) AS events,
                   max(human_actors) AS peak_daily_human_actors
            FROM read_parquet('{_mart(cfg, "repo_activity_daily")}')
            WHERE human_actors >= 2
            GROUP BY repo_name
            ORDER BY human_events DESC
            LIMIT {limit}
            """
        )
    )


def trending(con, cfg: Config, limit: int = 20) -> list[dict]:
    return _rows(
        con.execute(
            f"""
            SELECT event_date, repo_name, human_events, human_actors,
                   round(baseline_7d, 1) AS baseline_7d,
                   round(trend_score, 2) AS trend_score
            FROM read_parquet('{_mart(cfg, "trending_repos")}')
            WHERE trend_score IS NOT NULL
            ORDER BY trend_score DESC
            LIMIT {limit}
            """
        )
    )


def trending_first_scored_date(con, cfg: Config):
    """When will/did scores start? min(event_date) + 7 days."""
    row = con.execute(
        f"""SELECT min(event_date) + INTERVAL 7 DAY
            FROM read_parquet('{_mart(cfg, "repo_activity_daily")}')"""
    ).fetchone()
    return row[0]


def pr_daily(con, cfg: Config) -> list[dict]:
    return _rows(
        con.execute(
            f"""SELECT * FROM read_parquet('{_mart(cfg, "pr_stats_daily")}')
                ORDER BY event_date"""
        )
    )


def hourly_volume(con, cfg: Config) -> list[dict]:
    silver_glob = f"{cfg.silver_events_dir.as_posix()}/date=*/hour=*/events.parquet"
    return _rows(
        con.execute(
            f"""SELECT date_trunc('hour', created_at) AS hour, count(*) AS events
                FROM read_parquet('{silver_glob}')
                WHERE created_at IS NOT NULL
                GROUP BY 1 ORDER BY 1"""
        )
    )


def _metrics_glob(cfg: Config) -> str | None:
    if not any(cfg.metrics_dir.glob("date=*/*.json")):
        return None
    return f"{cfg.metrics_dir.as_posix()}/date=*/*.json"


# Explicit schema for metric records — same principle as the transform's
# bronze read: fields a record lacks become NULL, unknown fields are ignored.
# (union_by_name=true over hundreds of small files OOMs: duckdb holds
# per-file schema-union state; an explicit schema keeps memory flat.)
_METRICS_COLUMNS = """{
    'kind': 'VARCHAR', 'hour': 'VARCHAR', 'recorded_at': 'VARCHAR',
    'duration_seconds': 'DOUBLE', 'rows_in': 'BIGINT', 'rows_out': 'BIGINT',
    'bytes_out': 'BIGINT', 'bytes_downloaded': 'BIGINT', 'skipped': 'BOOLEAN',
    'rows': 'BIGINT', 'soft_warnings': 'BIGINT'
}"""


def _read_metrics(glob: str) -> str:
    return (
        f"read_json('{glob}', format='auto', maximum_object_size=32768, "
        f"columns={_METRICS_COLUMNS})"
    )


def run_metrics(con, cfg: Config) -> list[dict]:
    """Per-run task metrics history (empty before the first instrumented run)."""
    glob = _metrics_glob(cfg)
    if glob is None:
        return []
    return _rows(
        con.execute(
            f"""SELECT kind, CAST(hour AS TIMESTAMP) AS hour, duration_seconds,
                       rows_in, rows_out, bytes_downloaded
                FROM {_read_metrics(glob)}
                WHERE kind IN ('ingest', 'transform', 'quality_gate')
                ORDER BY hour, kind"""
        )
    )


def throughput_summary(con, cfg: Config) -> dict | None:
    glob = _metrics_glob(cfg)
    if glob is None:
        return None
    row = con.execute(
        f"""
        SELECT
            count(*) FILTER (kind = 'transform') AS transform_runs,
            median(rows_in / duration_seconds) FILTER (kind = 'transform'),
            median(duration_seconds) FILTER (kind = 'transform'),
            sum(bytes_downloaded) FILTER (kind = 'ingest') / 1e9,
            count(*) FILTER (kind = 'ingest' AND skipped = false)
        FROM {_read_metrics(glob)}
        """
    ).fetchone()
    return {
        "transform_runs": row[0],
        "median_events_per_sec": row[1],
        "median_transform_seconds": row[2],
        "gb_ingested": row[3],
        "download_runs": row[4],
    }


def ops_snapshot(cfg: Config) -> dict:
    """Filesystem view of the lake: partition counts, sizes, freshness."""

    def layer_stats(base: Path, pattern: str):
        partitions = sorted(base.glob(pattern))
        size = sum(f.stat().st_size for p in partitions for f in p.rglob("*") if f.is_file())
        return partitions, size

    bronze_parts, bronze_bytes = layer_stats(cfg.bronze_dir, "date=*/hour=*")
    silver_parts, silver_bytes = layer_stats(cfg.silver_events_dir, "date=*/hour=*")
    gold_files = sorted(cfg.gold_dir.glob("*.parquet")) if cfg.gold_dir.exists() else []

    latest_hour = None
    freshness_hours = None
    if silver_parts:
        last = silver_parts[-1]
        latest_hour = datetime.strptime(
            f"{last.parent.name.removeprefix('date=')} {last.name.removeprefix('hour=')}",
            "%Y-%m-%d %H",
        )
        interval_end = latest_hour + timedelta(hours=1)
        freshness_hours = (
            datetime.now(timezone.utc).replace(tzinfo=None) - interval_end
        ).total_seconds() / 3600

    return {
        "bronze_partitions": len(bronze_parts),
        "bronze_mb": bronze_bytes / 1e6,
        "silver_partitions": len(silver_parts),
        "silver_mb": silver_bytes / 1e6,
        "gold_marts": [(f.stem, f.stat().st_size / 1e6) for f in gold_files],
        "latest_hour": latest_hour,
        "freshness_hours": freshness_hours,
    }
