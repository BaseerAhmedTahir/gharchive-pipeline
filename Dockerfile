# Airflow image with the pipeline's Python dependencies baked in.
# (The _PIP_ADDITIONAL_REQUIREMENTS route re-installs on every container
# start — fine for a smoke test, wrong for anything recurring.)
FROM apache/airflow:3.3.0

# pipeline runtime deps + test-only deps (tests also run in-container,
# since Airflow itself can't run on the Windows host)
RUN pip install --no-cache-dir "duckdb>=1.1" "tenacity>=8.2" pytest responses
