# GH Archive Data Pipeline

An orchestrated, fault-tolerant data pipeline over the [GH Archive](https://www.gharchive.org/)
public dataset (every public GitHub event, published as one gzipped JSON file per hour).
Ingests millions of events per day, transforms them with DuckDB into a partitioned
Parquet lakehouse, orchestrates everything with Airflow 3 on Docker Compose, and serves
a Streamlit dashboard — including a page where the pipeline reports on itself.

```
GH Archive (HTTPS, hourly .json.gz)
        |  ingest: streaming download, retries w/ backoff, gzip-verified atomic writes
        v
BRONZE  data/bronze/date=YYYY-MM-DD/hour=HH/events.json.gz      (raw, immutable, 3-day retention)
        |  transform: DuckDB two-phase (stream -> staging parquet -> DISTINCT)
        v
SILVER  data/silver/events/date=.../hour=.../events.parquet     (typed, deduplicated, 10-day retention)
        |  quality gate: hard checks fail the run, soft checks warn
        |  aggregate: joins + window functions, full rebuild
        v
GOLD    data/gold/{event_type_daily, repo_activity_daily, trending_repos, pr_stats_daily}.parquet
        |
        v
Streamlit + DuckDB dashboard  (insights + pipeline-ops pages)

Airflow 3 (Docker Compose, LocalExecutor):
  gharchive_hourly:  ingest -> transform -> quality_gate     (@hourly, catchup=True)
  gharchive_daily:   build_gold -> prune_retention           (@daily)
```

## Measured numbers (not estimates)

All numbers below are measured by the pipeline itself (`pipeline/metrics.py` records
per-run stats; the dashboard's ops page charts them).

| Metric | Measured value |
|---|---|
| Volume | ~3.5-4.0M events/day, ~2.4 GB raw JSON/day (measured over 128 hours) |
| One hour of data | ~165k events, ~20 MB gzipped / ~100 MB raw JSON |
| End-to-end hourly run | ~30 s (ingest ~15-19 s, transform ~6-14 s, quality gate ~6 s) |
| Transform throughput | ~46k events/s median across 128 hours (~70k/s on a warm host, ~28k/s in-container under the memory cap) |
| Compression, raw JSON -> silver Parquet | 24.8x (7.8x from columnar+zstd, 3.2x from dropping unused payload) |
| Backfill | 128 hours of history (21.2M events, 2.8 GB raw) in ~20 min of compute at `max_active_runs=2` |
| Idempotent re-run of an ingested hour | 3.6 s (skip path) vs 15 s (download) |
| Gold rebuild (4 marts) | ~2 s over 2 days; ~80 s over the full 9-day window (6.1M repo-day rows) |
| Test suite | 61 tests on host + 5 DAG-integrity tests in-container |

## Fault tolerance — the actual design, not a checkbox

**Retries live at the layer whose timescale matches the failure.** Connection resets
and truncated downloads heal in seconds — retried in-process with exponential backoff
and jitter (`tenacity`). A 404 means GH Archive hasn't published the hour yet, which
heals in minutes — raised immediately so Airflow retries the task on its schedule
(ingest: 6 retries). A hard data-quality failure never heals by retrying — the
quality gate has `retries=0`: fail once, alert, wait for a human.

**Idempotency by construction.** Every run owns exactly one hour partition. Writes are
atomic (temp file/dir + `os.replace`, always same-filesystem); bronze files are
gzip-verified before the rename, so *a partition existing is proof it is complete* —
which is what makes skip-if-exists a safe idempotency check. Transforms use
overwrite-partition semantics. Consequence: crash recovery is "re-run it", backfill
is `airflow backfill create`, and concurrent duplicate runs are safe (both download
identical immutable source data; only verified content is ever renamed into place).

**Quality gate between transform and aggregate** — a separate DAG task, so a data
failure is visibly distinct from an infra failure and blocks gold from being poisoned.
Hard checks (fail): partition exists, schema matches, row floor (50k, calibrated from
the measured ~160k/hour baseline), no NULL ids, no duplicate ids. Soft checks (warn
only): event timestamps outside the collection hour +/-60min (GH Archive names files
by *collection* hour; events legitimately spill), NULL timestamps.

**Field-tested, involuntarily.** Development happened on an 8 GB laptop, and the
pipeline's fault tolerance got exercised for real. Docker's engine dropped three
times under sustained backfill load; the root cause was the uncapped WSL2 VM growing
to fight Windows for physical RAM until the host had <0.6 GB free and the whole VM
paged out. The fix was measured, not guessed: cap the VM at 3 GB (`.wslconfig`),
which left it responsive through a 120-run backfill. Every time the engine came back,
recovery was mechanical — the scheduler reaped interrupted runs as failed, transform's
`retries=3` auto-healed most, catchup rescheduled missing hours, and `airflow tasks
clear` re-ran the rest; ingest hit its idempotent skip path each time. One failure
looked like a data problem but its log simply stopped mid-run; running the identical
gate on the host passed 165k rows, proving an environment kill, not bad data.
Distinguishing those two failure classes quickly is the entire point of the gate
being a separate task with `retries=0`.

## Design decisions and their tradeoffs

**DuckDB, not Spark.** Single-node columnar SQL is right-sized for GBs/hour, and the
SQL is portable if outgrown. The concrete switch threshold: DuckDB handles up to
~100 GB working sets on one machine via out-of-core execution; you reach for
distributed compute when a single job's working set is multi-hundred-GB+ or the
hourly batch can't finish within the hour on one node. We are 3 orders of magnitude
below that.

**The two-phase transform** (the best bug of the project). The obvious design —
`read_json -> QUALIFY row_number() -> COPY TO parquet` — OOMs under a memory cap
instead of spilling. Probing operators in isolation showed **DuckDB 1.5 window
functions cannot spill to disk** (they materialize all partitions in memory), and
blocking operators sharing a pipeline with the gzip JSON reader exhaust memory before
spill machinery engages. The fix: stream JSON -> staging Parquet (nothing blocking),
then `SELECT DISTINCT` staging -> final (hash aggregate, which spills). A regression
test transforms 500k rows under a 64 MB memory limit to prove it. Bonus: the JSON is
parsed once, not twice.

**Dedup semantics: exact duplicates only.** `DISTINCT *` removes GH Archive's
occasional identical re-deliveries. A same-id-but-different-content collision is
*preserved* and the quality gate fails loudly on it — never silently pick a winner at
the moment you have the least information. (If one-row-per-id were required, the
spillable implementation is sort-based `DISTINCT ON`, not a window function.)

**Parquet lakehouse, not Postgres.** Single-writer batch + read-heavy analytics =
columnar files win: 24.8x smaller than raw JSON, directly queryable by DuckDB and the
dashboard, partition dirs make idempotent overwrite trivial. No concurrent-writer
transactions needed. Honest decomposition of that 24.8x: 7.8x from format+zstd,
3.2x from schema-on-write dropping payload fields nothing downstream uses.

**zstd, not snappy.** Write-once read-many favors zstd's better ratio at comparable
decode speed. `ROW_GROUP_SIZE` is set explicitly (20k) because the Parquet writer
buffers a full row group in memory — bounded writer memory beats default-sized row
groups in memory-capped containers.

**Airflow 3 (deliberately), LocalExecutor (deliberately).** Airflow 3 is the current
major; its data-interval model maps 1:1 onto GH Archive's immutable hourly files,
which is what makes catchup/backfill free. LocalExecutor because Celery's brokers and
workers add distributed-systems overhead with zero benefit on one machine — process
parallelism is real parallelism (and the honest answer beats the impressive one).

**Memory budgets are explicit everywhere** because DuckDB sees the machine's RAM, not
its container's share. In containers: `memory_limit=256MB`, `threads=2`,
`temp_directory` set (in-memory DuckDB has *no* spill location unless you give it
one — cap without spill dir = clean OOM exception with an idle disk). Concurrency
(`max_active_runs=2`) was sized from measured numbers: ~3.7 GiB Docker VM, ~1.9 GiB
headroom after Airflow's own services. Without the explicit limit, the container is
OOM-killed: exit 137 (SIGKILL), no traceback, a vanished process — the worst kind of
failure to debug.

**Retention exceeds the aggregation window** (silver 10 days > trending's trailing
7-day baseline), or edge dates would get silently-wrong scores. Trend scores are NULL
until the full window is inside retained data (`window_complete`), and the baseline
is `sum/7`, not `avg` — `avg` over only-active days inflates the denominator for
bursty repos and suppresses exactly the spikes a trending mart exists to catch.

## What the data forced (findings from looking, not assuming)

- **2026 GH Archive slimmed PR payloads** to `{url, id, number, head, base}` — the
  `merged`/`merged_at` fields are gone, so the planned PR-merge mart is impossible
  from this data. Redesigned as PR *lifecycle* (open->close latency from event
  timestamps), with the limitation stated on the dashboard.
- **Bots by behavior, not by name.** Only ~9% of events carry a `[bot]` login, but
  the top-volume "human" repos were single-actor cron committers. Trending requires
  >= 2 distinct human actors — one line of SQL that flipped the leaderboard from
  auto-commit junk to PostHog and Odoo.
- **PR events are ~0.1% of the firehose** (~150-210/hour, measured across the day) —
  PR marts run at daily grain; hourly would be noise. Quality-gate thresholds were
  calibrated from these measurements, not guessed.

## Running it

```bash
# Prereqs: Docker Desktop (WSL2), Python 3.12 (see .python-version)
py -3.12 -m venv .venv && .venv\Scripts\python -m pip install -r requirements-dev.txt

# Tests (host: pipeline + dashboard; DAG tests auto-skip without Airflow)
.venv\Scripts\python -m pytest

# Airflow stack (custom image bakes duckdb — no per-start pip installs)
docker build -t gharchive-airflow:local .
docker compose up -d          # UI at localhost:8080 (airflow/airflow)
# unpause gharchive_hourly -> catchup backfills from start_date automatically

# DAG-integrity tests, in the same environment the DAGs run in
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && python -m pytest tests/test_dags.py -q"

# Dashboard (host)
.venv\Scripts\python -m streamlit run dashboard/app.py    # localhost:8501
```

Configuration is env-driven with sane defaults (`.env.example`): one `DATA_ROOT`
variable is the only thing that differs between host and container execution.

## Repo layout

```
pipeline/     pure-Python pipeline logic (no Airflow imports; unit-testable)
dags/         thin Airflow wrappers: data interval in, pipeline functions called
dashboard/    Streamlit app + testable query layer (no streamlit imports)
tests/        66 tests: unit, idempotency, spill-under-memory-cap, DAG integrity,
              headless dashboard renders (streamlit AppTest)
```
