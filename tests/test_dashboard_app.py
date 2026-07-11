"""Headless render tests: every dashboard page must execute without
exceptions against a fixture lake (streamlit AppTest runs the real script)."""

from datetime import datetime, timedelta

import pytest
from streamlit.testing.v1 import AppTest

from pipeline.aggregate import build_gold
from pipeline.config import Config
from tests.silver_fixtures import silver_row, write_silver

DAY0 = datetime(2026, 7, 1, 12)
PAGES = ["Overview", "Trending repos", "Pull requests", "Pipeline ops"]


@pytest.fixture
def fixture_lake(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path)
    for d in range(2):
        day = DAY0 + timedelta(days=d)
        write_silver(
            cfg, day,
            [silver_row(f"{d}-{i}", day, actor_login=f"user{i}") for i in range(3)]
            + [silver_row(f"{d}-pr", day, type="PullRequestEvent",
                          payload_action="opened", pr_number=d)],
        )
    build_gold(cfg=cfg)
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    return cfg


@pytest.mark.parametrize("page", PAGES)
def test_page_renders_without_exception(fixture_lake, page):
    at = AppTest.from_file("dashboard/app.py", default_timeout=30)
    at.run()
    at.sidebar.radio[0].set_value(page).run()

    assert not at.exception, f"{page} raised: {at.exception}"


def test_overview_shows_measured_metrics(fixture_lake):
    at = AppTest.from_file("dashboard/app.py", default_timeout=30)
    at.run()

    values = [m.value for m in at.metric]
    assert "8" in values  # 8 fixture events processed
