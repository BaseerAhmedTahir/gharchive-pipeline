from datetime import datetime, timedelta

import pytest

from dashboard import queries
from pipeline.aggregate import build_gold
from pipeline.config import Config
from tests.silver_fixtures import silver_row, write_silver

DAY0 = datetime(2026, 7, 1, 12)


@pytest.fixture
def cfg(tmp_path):
    cfg = Config(data_root=tmp_path, trending_min_daily_events=20)
    write_silver(
        cfg,
        DAY0,
        [
            silver_row("1", DAY0, repo_name="octo/hello", actor_login="alice"),
            silver_row("2", DAY0, repo_name="octo/hello", actor_login="bob"),
            silver_row("3", DAY0, repo_name="octo/hello", actor_login="ci[bot]"),
            silver_row("4", DAY0, repo_name="solo/firehose", actor_login="cron-user"),
            silver_row("5", DAY0, type="PullRequestEvent", payload_action="opened",
                       pr_number=1, repo_name="octo/hello", actor_login="alice"),
        ],
    )
    write_silver(
        cfg,
        DAY0 + timedelta(days=1),
        [
            silver_row("6", DAY0 + timedelta(days=1), repo_name="octo/hello",
                       actor_login="alice"),
        ],
    )
    build_gold(cfg=cfg)
    return cfg


@pytest.fixture
def con(cfg):
    con = queries.open_connection(cfg)
    yield con
    con.close()


def test_dashboard_config_has_small_memory_budget(monkeypatch):
    monkeypatch.delenv("DASHBOARD_DUCKDB_MEMORY_LIMIT", raising=False)
    assert queries.dashboard_config().duckdb_memory_limit == "256MB"


def test_summary(con, cfg):
    s = queries.summary(con, cfg)

    assert s["total_events"] == 6
    assert s["days"] == 2
    assert s["distinct_repos"] == 2
    assert 0 < s["bot_share"] < 1


def test_daily_type_mix_and_order(con, cfg):
    mix = queries.daily_type_mix(con, cfg)
    order = queries.type_order(con, cfg)

    assert order[0] == "PushEvent"  # most voluminous gets slot 1
    assert order[-1] == "Other"
    day1 = [r for r in mix if str(r["event_date"]) == "2026-07-01"]
    push = next(r for r in day1 if r["type"] == "PushEvent")
    assert push["events"] == 4
    assert push["bot_events"] == 1


def test_top_repos_applies_multi_actor_guard(con, cfg):
    repos = queries.top_repos(con, cfg)

    names = [r["repo_name"] for r in repos]
    assert "octo/hello" in names
    assert "solo/firehose" not in names  # single actor -> excluded


def test_trending_empty_is_safe_and_first_date_is_computed(con, cfg):
    assert queries.trending(con, cfg) == []
    first = queries.trending_first_scored_date(con, cfg)
    assert str(first).startswith("2026-07-08")


def test_pr_daily(con, cfg):
    rows = queries.pr_daily(con, cfg)

    assert rows[0]["prs_opened"] == 1
    assert rows[0]["prs_closed"] == 0


def test_hourly_volume(con, cfg):
    volume = queries.hourly_volume(con, cfg)

    assert len(volume) == 2
    assert volume[0]["events"] == 5


def test_ops_snapshot(cfg):
    ops = queries.ops_snapshot(cfg)

    assert ops["silver_partitions"] == 2
    assert ops["bronze_partitions"] == 0
    assert ops["latest_hour"] == datetime(2026, 7, 2, 12)
    assert ops["freshness_hours"] is not None
    assert [m[0] for m in ops["gold_marts"]] == [
        "event_type_daily", "pr_stats_daily", "repo_activity_daily", "trending_repos",
    ]
