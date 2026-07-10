from datetime import datetime

import pytest

from pipeline.config import Config
from pipeline.quality import QualityGateError, check_hour
from tests.silver_fixtures import silver_row, write_silver

HOUR = datetime(2026, 7, 9, 15)
IN_HOUR = datetime(2026, 7, 9, 15, 30)


@pytest.fixture
def cfg(tmp_path):
    # low row floor so fixtures stay small
    return Config(data_root=tmp_path, quality_min_rows=3)


def good_rows(n=5):
    return [silver_row(str(i), IN_HOUR) for i in range(n)]


def test_clean_hour_passes(cfg):
    write_silver(cfg, HOUR, good_rows())

    report = check_hour(HOUR, cfg=cfg)

    assert report.passed
    assert report.rows == 5
    assert report.soft_warnings == ()


def test_missing_partition_is_hard_failure(cfg):
    with pytest.raises(QualityGateError, match="missing"):
        check_hour(HOUR, cfg=cfg)


def test_row_count_below_floor_is_hard_failure(cfg):
    write_silver(cfg, HOUR, good_rows(2))

    with pytest.raises(QualityGateError, match="below floor"):
        check_hour(HOUR, cfg=cfg)


def test_null_id_is_hard_failure(cfg):
    write_silver(cfg, HOUR, good_rows() + [silver_row(None, IN_HOUR)])

    with pytest.raises(QualityGateError, match="NULL event ids"):
        check_hour(HOUR, cfg=cfg)


def test_duplicate_id_is_hard_failure(cfg):
    # same id, different content — preserved by the transform on purpose,
    # so the gate must be what catches it
    write_silver(
        cfg,
        HOUR,
        good_rows()
        + [
            silver_row("dup", IN_HOUR, actor_login="a"),
            silver_row("dup", IN_HOUR, actor_login="b"),
        ],
    )

    with pytest.raises(QualityGateError, match="duplicate event ids"):
        check_hour(HOUR, cfg=cfg)


def test_boundary_timestamp_is_soft_warning_not_failure(cfg):
    rows = good_rows() + [
        # 90 min after the hour starts +60 min tolerance: outside window
        silver_row("late", datetime(2026, 7, 9, 17, 31)),
        # NULL created_at: malformed upstream timestamp
        silver_row("null-ts", None),
    ]
    write_silver(cfg, HOUR, rows)

    report = check_hour(HOUR, cfg=cfg)

    assert report.passed  # soft checks never fail the run
    assert len(report.soft_warnings) == 2
    assert any("outside collection hour" in w for w in report.soft_warnings)
    assert any("NULL created_at" in w for w in report.soft_warnings)


def test_slight_boundary_spill_within_tolerance_is_clean(cfg):
    # 30 min into the next hour, tolerance is 60: normal GH Archive behavior
    write_silver(
        cfg, HOUR, good_rows() + [silver_row("spill", datetime(2026, 7, 9, 16, 30))]
    )

    report = check_hour(HOUR, cfg=cfg)

    assert report.passed
    assert report.soft_warnings == ()


def test_gate_error_carries_report(cfg):
    write_silver(cfg, HOUR, good_rows(1))

    with pytest.raises(QualityGateError) as exc_info:
        check_hour(HOUR, cfg=cfg)

    assert exc_info.value.report.rows == 1
    assert not exc_info.value.report.passed
