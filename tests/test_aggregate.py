from datetime import date, datetime, timedelta

import duckdb
import pytest

from pipeline.aggregate import SilverEmptyError, build_gold
from pipeline.config import Config
from tests.silver_fixtures import silver_row, write_silver

DAY0 = datetime(2026, 7, 1, 12)


@pytest.fixture
def cfg(tmp_path):
    return Config(data_root=tmp_path, trending_min_daily_events=20)


def read_mart(cfg, name, order_by):
    con = duckdb.connect()
    cur = con.execute(
        f"SELECT * FROM read_parquet('{(cfg.gold_dir / name).as_posix()}.parquet') "
        f"ORDER BY {order_by}"
    )
    columns = [d[0] for d in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    con.close()
    return rows


def test_empty_silver_raises(cfg):
    with pytest.raises(SilverEmptyError):
        build_gold(cfg=cfg)


def test_repo_activity_daily_counts_and_bot_split(cfg):
    ts = DAY0
    write_silver(
        cfg,
        DAY0,
        [
            silver_row("1", ts, repo_name="octo/hello", actor_login="alice"),
            silver_row("2", ts, repo_name="octo/hello", actor_login="bob",
                       type="PullRequestEvent", payload_action="opened", pr_number=1),
            silver_row("3", ts, repo_name="octo/hello", actor_login="ci[bot]"),
            silver_row("4", ts, repo_name="other/repo", actor_login="alice"),
        ],
    )

    result = build_gold(cfg=cfg)
    rows = read_mart(cfg, "repo_activity_daily", "repo_name")

    assert result.rows_per_mart["repo_activity_daily"] == 2
    hello = rows[0]
    assert hello["repo_name"] == "octo/hello"
    assert hello["events"] == 3
    assert hello["pushes"] == 2
    assert hello["pr_events"] == 1
    assert hello["actors"] == 3
    assert hello["bot_events"] == 1
    assert hello["human_events"] == 2


def test_event_type_daily_splits_bots(cfg):
    ts = DAY0
    write_silver(
        cfg,
        DAY0,
        [
            silver_row("1", ts, actor_login="alice"),
            silver_row("2", ts, actor_login="github-actions[bot]"),
        ],
    )

    build_gold(cfg=cfg)
    rows = read_mart(cfg, "event_type_daily", "type")

    assert rows == [
        {
            "event_date": date(2026, 7, 1),
            "type": "PushEvent",
            "events": 2,
            "bot_events": 1,
            "human_events": 1,
        }
    ]


def test_trending_scores_only_complete_windows(cfg):
    # 10 days of steady 30 human events/day, then a 90-event spike on day 9
    for d in range(10):
        day = DAY0 + timedelta(days=d)
        n = 90 if d == 9 else 30
        write_silver(
            cfg, day,
            [silver_row(f"{d}-{i}", day, actor_login=f"user{i}") for i in range(n)],
        )

    build_gold(cfg=cfg)
    rows = read_mart(cfg, "trending_repos", "event_date")

    assert len(rows) == 10  # all days above the 20-event floor
    # days 0-6: trailing 7-day window extends before retained data -> unscored
    for row in rows[:7]:
        assert not row["window_complete"]
        assert row["trend_score"] is None
    # steady days with complete window: score == 1.0
    assert rows[7]["window_complete"]
    assert rows[7]["trend_score"] == pytest.approx(1.0)
    assert rows[8]["trend_score"] == pytest.approx(1.0)
    # spike day: 90 events vs steady 30 baseline
    assert rows[9]["trend_score"] == pytest.approx(3.0)


def test_trending_excludes_bot_activity_and_tiny_repos(cfg):
    # 50 bot events + 5 human events/day: below the 20 human-event floor
    for d in range(9):
        day = DAY0 + timedelta(days=d)
        rows = [
            silver_row(f"{d}-b{i}", day, actor_login="ci[bot]") for i in range(50)
        ] + [silver_row(f"{d}-h{i}", day, actor_login=f"user{i}") for i in range(5)]
        write_silver(cfg, day, rows)

    build_gold(cfg=cfg)

    assert read_mart(cfg, "trending_repos", "event_date") == []


def test_pr_lifecycle_counts_and_hours_open(cfg):
    # NOTE: lifecycle, not merge stats — 2026 GH Archive PR payloads no
    # longer carry merged/merged_at (verified against real data).
    day1, day2 = DAY0, DAY0 + timedelta(days=1)
    write_silver(
        cfg,
        day1,
        [
            # PR 1 opened day1 12:00
            silver_row("1", day1, type="PullRequestEvent", payload_action="opened",
                       pr_number=1),
            # PR 2 opened day1, closed 2h later
            silver_row("2", day1, type="PullRequestEvent", payload_action="opened",
                       pr_number=2),
            silver_row("3", day1 + timedelta(hours=2), type="PullRequestEvent",
                       payload_action="closed", pr_number=2),
        ],
    )
    # PR 1 closed day2 14:00 -> 26h open
    write_silver(
        cfg,
        day2,
        [
            silver_row("4", day2 + timedelta(hours=2), type="PullRequestEvent",
                       payload_action="closed", pr_number=1),
        ],
    )

    build_gold(cfg=cfg)
    rows = read_mart(cfg, "pr_stats_daily", "event_date")

    d1, d2 = rows
    assert d1["prs_opened"] == 2
    assert d1["prs_closed"] == 1
    assert d1["closed_with_known_open"] == 1
    assert d1["median_hours_open"] == pytest.approx(2.0)
    assert d2["prs_closed"] == 1
    assert d2["closed_with_known_open"] == 1
    assert d2["median_hours_open"] == pytest.approx(26.0)


def test_trending_leaves_newcomers_unscored(cfg):
    # An established repo provides 10 days of history (fixes the global
    # min-date); a newcomer appears only on day 9 with a big spike. The
    # newcomer's window is "complete" but its baseline (~0) is below the
    # floor: it must appear in the mart WITHOUT a trend score, not at 100x.
    for d in range(10):
        day = DAY0 + timedelta(days=d)
        rows = [
            silver_row(f"{d}-{i}", day, repo_name="steady/repo",
                       actor_login=f"user{i}") for i in range(30)
        ]
        if d == 9:
            rows += [
                silver_row(f"new-{i}", day, repo_name="brand/new",
                           actor_login=f"newbie{i % 3}") for i in range(25)
            ]
        write_silver(cfg, day, rows)

    build_gold(cfg=cfg)
    rows = read_mart(cfg, "trending_repos", "event_date, repo_name")

    newcomer = next(r for r in rows if r["repo_name"] == "brand/new")
    assert newcomer["window_complete"]
    assert newcomer["trend_score"] is None  # baseline below floor
    steady_day9 = next(
        r for r in rows
        if r["repo_name"] == "steady/repo" and str(r["event_date"]) == "2026-07-10"
    )
    assert steady_day9["trend_score"] == pytest.approx(1.0)


def test_trending_excludes_single_actor_firehoses(cfg):
    # 9 days x 30 events/day, all from ONE non-[bot] login: a bot by
    # behavior. The >= 2 human actors guard must exclude it.
    for d in range(9):
        day = DAY0 + timedelta(days=d)
        write_silver(
            cfg, day,
            [silver_row(f"{d}-{i}", day, actor_login="cron-committer")
             for i in range(30)],
        )

    build_gold(cfg=cfg)

    assert read_mart(cfg, "trending_repos", "event_date") == []


def test_rebuild_is_idempotent(cfg):
    write_silver(cfg, DAY0, [silver_row(str(i), DAY0) for i in range(5)])

    first = build_gold(cfg=cfg)
    rows_first = read_mart(cfg, "repo_activity_daily", "repo_name")
    second = build_gold(cfg=cfg)
    rows_second = read_mart(cfg, "repo_activity_daily", "repo_name")

    assert first.rows_per_mart == second.rows_per_mart
    assert rows_first == rows_second
    assert list(cfg.gold_dir.glob("*.tmp")) == []
