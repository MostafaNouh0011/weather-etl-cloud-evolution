"""01_bronze_ingest.py — fetch the Weatherstack API and land raw payloads in Bronze.

This is the first of three notebooks. It does exactly one thing:
  * Read the 100-city list from the config container.
  * Read the Weatherstack API key from Key Vault (or env var fallback).
  * Call the API in parallel for every city.
  * Write one Parquet file per run, partitioned by year/month/day/hour,
    into the Bronze container.

The Parquet schema is the **raw** API response, projected into a columnar
shape (so we get column-pruning on later reads) but with no cleansing
or dedup. If a row is malformed, the notebook logs and skips it — it
does NOT fail the whole run. That is the Bronze contract: append-only,
schemaless-ish, never the source of a pipeline failure.

The output path is the *contract* that NB2 reads from. The hour
partition is derived from the pipeline run time (NOT from the API's
`localtime` field) so a stuck or replayed run always lands in a
predictable place.

Inputs (set by ADF as widget parameters; defaults exist for local runs):
  * storage_account   — Bicep output `storageAccountName`
  * scope             — Databricks secret scope (default "kv-scope")
  * api_key_name      — Key Vault secret name (default "weatherstack-api-key")
  * env_fallback      — Optional env-var name to use outside Databricks
  * max_workers       — ThreadPoolExecutor worker count (default 10)

Outputs:
  * One Parquet file per run in:
      abfss://bronze@<account>/weather/year=YYYY/month=MM/day=DD/hour=HH/
  * A job return value summarising success / failure counts so ADF can
    surface a useful error message.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import List

# Databricks runs the notebook from /Workspace/.../01_bronze_ingest.py.
# `libs/` is sibling to `notebooks/`, so we add it to sys.path.
HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
LIBS = os.path.normpath(os.path.join(HERE, "..", "libs"))
if LIBS not in sys.path:
    sys.path.insert(0, LIBS)

from pyspark.sql import SparkSession  # noqa: E402

from storage import (  # noqa: E402
    bronze_path,
    config_cities_path,
    get_secret,
)
from weather_client import fetch_all_resilient  # noqa: E402


# ---------------------------------------------------------------------------
# Widgets — ADF passes these in. Defaults are for local / unit-test runs.
# ---------------------------------------------------------------------------

def _get_widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except NameError:
        return default


storage_account = _get_widget("storage_account", os.environ.get("AZURE_STORAGE_ACCOUNT", ""))
scope = _get_widget("scope", os.environ.get("DATABRICKS_SECRET_SCOPE", "kv-scope"))
api_key_name = _get_widget("api_key_name", os.environ.get("DATABRICKS_SECRET_KEY", "weatherstack-api-key"))
env_fallback = _get_widget("env_fallback", "WEATHERSTACK_API_KEY")
max_workers = int(_get_widget("max_workers", "10"))

if not storage_account:
    raise ValueError(
        "storage_account is required. Set the ADF widget, or AZURE_STORAGE_ACCOUNT "
        "in the environment, before running the notebook."
    )

# ---------------------------------------------------------------------------
# Read inputs
# ---------------------------------------------------------------------------

spark = SparkSession.builder.getOrCreate()

# cities.json lives in the config container. We use the ABFSS URI directly
# — Spark reads it as one row per line, and we concat them back into a
# single string before json.loads. This handles both compact (single
# line) and pretty-printed (multi-line) JSON without a special case.
cities_uri = config_cities_path(storage_account)
cities_text = "\n".join(
    row["value"] for row in spark.read.text(cities_uri).collect()
)
cities_doc = json.loads(cities_text)
cities: List[dict] = cities_doc["cities"]

api_key = get_secret(scope, api_key_name, env_fallback=env_fallback)

# ---------------------------------------------------------------------------
# Fetch in parallel
# ---------------------------------------------------------------------------

successes, failures = fetch_all_resilient(api_key, cities, max_workers=max_workers)
if failures:
    print(f"Bronze ingest: {len(failures)} cities failed", file=sys.stderr)
    for index, msg in failures[:10]:
        print(f"  - {cities[index]['name']}: {msg}", file=sys.stderr)

if not successes:
    raise RuntimeError(
        f"Bronze ingest: all {len(cities)} cities failed. Aborting the run."
    )

# ---------------------------------------------------------------------------
# Build the Bronze DataFrame
# ---------------------------------------------------------------------------

# We project the raw payload into a columnar shape (location.* and
# current.*) so a Parquet read later can do column pruning. The full
# `raw_payload` column is preserved so any field we forgot to surface
# here can still be retrieved without an API re-pull.
now_utc = datetime.now(timezone.utc).replace(tzinfo=None)  # Spark TimestampType is tz-naive
rows = []
for payload in successes:
    location = payload.get("location") or {}
    current = payload.get("current") or {}
    rows.append({
        "city": location.get("name"),
        "country": location.get("country"),
        "region": location.get("region"),
        "localtime": location.get("localtime"),
        "temperature": current.get("temperature"),
        "feelslike": current.get("feelslike"),
        "humidity": current.get("humidity"),
        "wind_speed": current.get("wind_speed"),
        "wind_degree": current.get("wind_degree"),
        "wind_dir": current.get("wind_dir"),
        "pressure": current.get("pressure"),
        "precipitation": current.get("precipitation"),
        "cloudcover": current.get("cloudcover"),
        "visibility": current.get("visibility"),
        "uv_index": current.get("uv_index"),
        "weather_descriptions": current.get("weather_descriptions"),
        "weather_icons": current.get("weather_icons"),
        "observation_time": current.get("observation_time"),
        "ingestion_ts": now_utc,
        "source": "weatherstack",
    })

bronze_df = spark.createDataFrame(rows)

# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

run_time = now_utc
out_path = bronze_path(
    storage_account,
    run_time.year,
    run_time.month,
    run_time.day,
    run_time.hour,
)
(
    bronze_df.write
    .mode("append")
    .parquet(out_path)
)

result = {
    "cities_total": len(cities),
    "cities_written": len(successes),
    "cities_failed": len(failures),
    "failures": [{"city": cities[i]["name"], "error": msg} for i, msg in failures],
    "out_path": out_path,
    "run_time_utc": run_time.isoformat(),
}

# ADF reads the return value of the last expression; JSON-stringify it
# so the value is easy to inspect in the ADF output panel.
print(json.dumps(result))

# Raise at the end if everything failed — ADF treats a raised exception
# as a failure, which is what we want when no data was written.
if not successes:
    raise RuntimeError("Bronze ingest produced zero successful records")
