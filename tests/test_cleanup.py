from datetime import date

import pytest

from pipeline.cleanup import prune_retention
from pipeline.config import Config

TODAY = date(2026, 7, 10)


@pytest.fixture
def cfg(tmp_path):
    return Config(data_root=tmp_path, bronze_retention_days=3, silver_retention_days=10)


def make_partitions(base, days):
    for d in days:
        (base / f"date=2026-07-{d:02d}" / "hour=12").mkdir(parents=True)


def test_prunes_beyond_retention_per_layer(cfg):
    make_partitions(cfg.bronze_dir, range(1, 11))          # Jul 1-10
    make_partitions(cfg.silver_events_dir, range(1, 11))

    result = prune_retention(cfg=cfg, today=TODAY)

    # bronze keeps >= Jul 7 (3 days), silver keeps everything (10 days)
    bronze_left = sorted(p.name for p in cfg.bronze_dir.glob("date=*"))
    assert bronze_left == [f"date=2026-07-{d:02d}" for d in (7, 8, 9, 10)]
    assert len(list(cfg.silver_events_dir.glob("date=*"))) == 10
    assert len(result.removed) == 6


def test_unrecognized_dirs_are_left_alone(cfg):
    weird = cfg.bronze_dir / "date=not-a-date"
    weird.mkdir(parents=True)
    make_partitions(cfg.bronze_dir, [1])

    result = prune_retention(cfg=cfg, today=TODAY)

    assert weird.exists()
    assert len(result.removed) == 1


def test_missing_layers_are_fine(cfg):
    assert prune_retention(cfg=cfg, today=TODAY).removed == ()
