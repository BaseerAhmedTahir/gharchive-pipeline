"""Build the gold marts from the full silver window.

Marts (all daily grain — measured PR volume is ~150-210/hour, far too thin
for hourly PR aggregates, and daily is the honest resolution for trends):

- event_type_daily:    events per day per type, with bot share
- repo_activity_daily: per-repo daily activity (events, pushes, actors, bots)
- trending_repos:      daily human (non-bot) activity vs trailing 7-day
                       baseline; scored only where the trailing window is
                       fully inside the silver retention window
- pr_stats_daily:      PR lifecycle per day: opened/closed counts + hours-open
                       latency (join: opened event -> closed event on
                       repo+number). NOT merge stats: 2026 GH Archive slimmed
                       PR payloads to {url,id,number,head,base}, so
                       merged/merged_at no longer exist in the data —
                       verified empirically before this mart was (re)designed.

Design notes:
- Full rebuild every run from whatever silver partitions exist: trivially
  idempotent, and cheap because inputs are pre-aggregated or columnar.
- Bot heuristic, two layers (both from measured data): GitHub app accounts
  carry a literal "[bot]" login marker, but the highest-volume repos turned
  out to be single-actor auto-committers WITHOUT the marker (bots by
  behavior, not by name). So marts expose [bot]-based splits, and trending
  additionally requires >= 2 distinct human actors — multi-actor activity
  is the strongest cheap signal of genuine human interest.
- Gold writes are single files, so atomicity is plain tmp + os.replace.
- Window functions ARE used here (trailing 7-day baseline) although they
  can't spill: safe because the input is the pre-aggregated daily mart,
  orders of magnitude smaller than silver.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime

import duckdb

from pipeline.config import Config, get_config
from pipeline.transform import connect

log = logging.getLogger(__name__)


class SilverEmptyError(Exception):
    """No silver partitions exist yet — nothing to aggregate."""


@dataclass(frozen=True)
class GoldBuildResult:
    rows_per_mart: dict[str, int]
    duration_seconds: float


def build_gold(cfg: Config | None = None) -> GoldBuildResult:
    cfg = cfg or get_config()
    if not list(cfg.silver_events_dir.glob("date=*/hour=*/events.parquet")):
        raise SilverEmptyError(f"no silver partitions under {cfg.silver_events_dir}")
    cfg.gold_dir.mkdir(parents=True, exist_ok=True)
    silver_glob = f"{cfg.silver_events_dir.as_posix()}/date=*/hour=*/events.parquet"

    start = time.monotonic()
    con = connect(cfg)
    rows: dict[str, int] = {}
    try:
        # NULL created_at rows are excluded (soft-checked by the quality
        # gate); they can't be placed on the daily grain.
        con.execute(
            f"""
            CREATE TEMP VIEW events AS
            SELECT *,
                   CAST(created_at AS DATE) AS event_date,
                   actor_login LIKE '%[bot]%' AS is_bot
            FROM read_parquet('{silver_glob}')
            WHERE created_at IS NOT NULL
            """
        )

        rows["event_type_daily"] = _write_mart(
            con, cfg, "event_type_daily",
            """
            SELECT event_date, type,
                   count(*) AS events,
                   count(*) FILTER (is_bot) AS bot_events,
                   count(*) FILTER (NOT is_bot) AS human_events
            FROM events
            GROUP BY event_date, type
            """,
        )

        rows["repo_activity_daily"] = _write_mart(
            con, cfg, "repo_activity_daily",
            """
            SELECT event_date, repo_name,
                   any_value(repo_id) AS repo_id,
                   count(*) AS events,
                   count(*) FILTER (type = 'PushEvent') AS pushes,
                   count(*) FILTER (type = 'PullRequestEvent') AS pr_events,
                   count(DISTINCT actor_login) AS actors,
                   count(DISTINCT actor_login) FILTER (NOT is_bot) AS human_actors,
                   count(*) FILTER (is_bot) AS bot_events,
                   count(*) FILTER (NOT is_bot) AS human_events
            FROM events
            GROUP BY event_date, repo_name
            """,
        )

        # Trailing 7-day baseline over the pre-aggregated daily mart.
        # Missing days count as zero activity (sum/7, not avg of present
        # rows). Scores are NULL until the full 7-day window fits inside
        # the retained data — retention (10d) must exceed the window (7d).
        activity = (cfg.gold_dir / "repo_activity_daily.parquet").as_posix()
        rows["trending_repos"] = _write_mart(
            con, cfg, "trending_repos",
            f"""
            WITH daily AS (
                SELECT event_date, repo_name, human_events, human_actors
                FROM read_parquet('{activity}')
            ),
            scored AS (
                SELECT *,
                    sum(human_events) OVER w / 7.0 AS baseline_7d,
                    date_diff('day', (SELECT min(event_date) FROM daily), event_date) >= 7
                        AS window_complete
                FROM daily
                WINDOW w AS (
                    PARTITION BY repo_name ORDER BY event_date
                    RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND INTERVAL 1 DAY PRECEDING
                )
            )
            SELECT event_date, repo_name, human_events, human_actors,
                   baseline_7d, window_complete,
                   CASE WHEN window_complete AND baseline_7d > 0
                        THEN human_events / baseline_7d END AS trend_score
            FROM scored
            WHERE human_events >= {cfg.trending_min_daily_events}
              -- behavioral bot guard: single-actor firehoses (cron
              -- committers without a "[bot]" login) dominate raw volume
              AND human_actors >= 2
            """,
        )

        rows["pr_stats_daily"] = _write_mart(
            con, cfg, "pr_stats_daily",
            """
            WITH pr AS (
                SELECT * FROM events WHERE type = 'PullRequestEvent'
            ),
            counts AS (
                SELECT event_date,
                       count(*) FILTER (payload_action = 'opened') AS prs_opened,
                       count(*) FILTER (payload_action = 'closed') AS prs_closed
                FROM pr
                GROUP BY event_date
            ),
            opened AS (
                SELECT repo_name, pr_number, min(created_at) AS opened_at
                FROM pr WHERE payload_action = 'opened' AND pr_number IS NOT NULL
                GROUP BY repo_name, pr_number
            ),
            closed AS (
                SELECT repo_name, pr_number, min(created_at) AS closed_at
                FROM pr WHERE payload_action = 'closed' AND pr_number IS NOT NULL
                GROUP BY repo_name, pr_number
            ),
            lifecycle AS (
                SELECT CAST(c.closed_at AS DATE) AS event_date,
                       count(*) AS closed_with_known_open,
                       quantile_cont(date_diff('minute', o.opened_at, c.closed_at) / 60.0,
                                     0.5) AS median_hours_open,
                       quantile_cont(date_diff('minute', o.opened_at, c.closed_at) / 60.0,
                                     0.9) AS p90_hours_open
                FROM closed c
                JOIN opened o USING (repo_name, pr_number)
                WHERE c.closed_at >= o.opened_at
                GROUP BY 1
            )
            SELECT counts.*,
                   coalesce(lifecycle.closed_with_known_open, 0)
                       AS closed_with_known_open,
                   lifecycle.median_hours_open,
                   lifecycle.p90_hours_open
            FROM counts
            LEFT JOIN lifecycle USING (event_date)
            """,
        )
    finally:
        con.close()

    duration = time.monotonic() - start
    log.info("gold rebuilt in %.1fs: %s", duration, rows)
    return GoldBuildResult(rows_per_mart=rows, duration_seconds=duration)


def _write_mart(
    con: duckdb.DuckDBPyConnection, cfg: Config, name: str, sql: str
) -> int:
    """COPY a mart query to gold/<name>.parquet atomically. Returns row count."""
    final = cfg.gold_dir / f"{name}.parquet"
    tmp = cfg.gold_dir / f"{name}.parquet.tmp"
    row_count = con.execute(
        f"COPY ({sql}) TO '{tmp.as_posix()}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {cfg.parquet_row_group_size})"
    ).fetchone()[0]
    os.replace(tmp, final)
    return row_count
