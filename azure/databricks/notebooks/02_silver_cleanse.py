"""02_silver_cleanse.py — read Bronze, apply the Silver schema, MERGE into silver/weather.

This is the second of three notebooks. It does three things, in order:
  1. Read every Bronze file written for the current hour partition
     (the one NB1 just landed). Reading is restricted to the hour
     because the Bronze layout is hour-partitioned; reading only the
     just-written hour keeps Silver runtimes short.
  2. Coerce every row to the explicit Silver schema (see libs/schemas.py).
     Rows that fail coercion (missing core fields) are dropped. Rows
     that are valid but missing optional measurements are kept.
  3. MERGE the cleansed rows into the silver/weather Delta table, keyed
     on (city, local_time). MERGE = idempotent: re-running this notebook
     for the same hour is a no-op.

The dedup / MERGE key is (city, local_time) — the same pair v1 used
for raw.weather_raw. Adding a new partition column is a schema change;
this is the locked-in contract.

Inputs (ADF widgets; defaults for local runs):
  * storage_account  — Bicep output
  * run_year / run_month / run_day / run_hour — the partition to process
  * (no other knobs)

Outputs:
  * Updated silver/weather Delta table at abfss://silver@<account>/weather
  * Job return value: {rows_in, rows_merged, rows_dropped}
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from typing import List

HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
LIBS = os.path.normpath(os.path.join(HERE, "..", "libs"))
if LIBS not in sys.path:
    sys.path.insert(0, LIBS)

from pyspark.sql import SparkSession  # noqa: E402

from schemas import SILVER, weatherstack_to_silver_row  # noqa: E402
from storage import bronze_path, silver_table_path  # noqa: E402


def _get_widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except NameError:
        return default


storage_account = _get_widget("storage_account", os.environ.get("AZURE_STORAGE_ACCOUNT", ""))
run_year = int(_get_widget("run_year", str(datetime.utcnow().year)))
run_month = int(_get_widget("run_month", str(datetime.utcnow().month)))
run_day = int(_get_widget("run_day", str(datetime.utcnow().day)))
run_hour = int(_get_widget("run_hour", str(datetime.utcnow().hour)))

if not storage_account:
    raise ValueError("storage_account is required")

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# 1. Read Bronze for the current hour
# ---------------------------------------------------------------------------

# Parquet is self-describing, so we don't need to pre-declare a schema
# here. The Bronze file's columns are whatever NB1 wrote; we project
# to the Silver columns next.
in_path = bronze_path(storage_account, run_year, run_month, run_day, run_hour)
bronze_df = spark.read.parquet(in_path)
rows_in = bronze_df.count()
print(f"Bronze rows read: {rows_in} from {in_path}")

# ---------------------------------------------------------------------------
# 2. Coerce to Silver
# ---------------------------------------------------------------------------

# We do the coercion in Python (not in Spark expressions) so the rules
# live in libs/schemas.py and can be unit-tested without a cluster.
# The price is a .collect() round-trip, which is fine for ~100 rows.
def _to_silver_row(r) -> dict | None:
    # Build a payload-shaped dict that the pure function expects.
    payload = {
        "location": {
            "name": r.city,
            "country": r.country,
            "region": r.region,
            "localtime": _localtime_to_str(r.localtime),
        },
        "current": {
            "temperature": r.temperature,
            "feelslike": r.feelslike,
            "humidity": r.humidity,
            "wind_speed": r.wind_speed,
            "wind_degree": r.wind_degree,
            "pressure": r.pressure,
            "precipitation": r.precipitation,
            "cloudcover": r.cloudcover,
            "visibility": r.visibility,
            "uv_index": r.uv_index,
            "weather_descriptions": list(r.weather_descriptions) if r.weather_descriptions else [],
        },
    }
    return weatherstack_to_silver_row(payload, ingestion_ts=r.ingestion_ts)


def _localtime_to_str(value) -> str:
    """Reconstruct 'YYYY-MM-DD HH:MM' from whatever Bronze stored.

    Bronze stores `localtime` as the raw string from the API; the value
    may have been re-serialised as a string, a Timestamp, or a
    YYYY-MM-DD HH:MM datetime depending on the writer. We normalise.
    """
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M")
    s = str(value)
    # 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DDTHH:MM:SS' — both common.
    if "T" in s:
        s = s.replace("T", " ")
    return s[:16]


silver_rows: List[dict] = []
for r in bronze_df.collect():
    coerced = _to_silver_row(r)
    if coerced is not None:
        silver_rows.append(coerced)

rows_dropped = rows_in - len(silver_rows)
print(f"Silver rows after coercion: {len(silver_rows)} (dropped {rows_dropped})")

if not silver_rows:
    # Nothing to merge. Return early so the job doesn't create an empty
    # Delta table that confuses downstream consumers.
    print("No Silver rows to write; skipping MERGE.")
    raise SystemExit(0)

# Build the Silver DataFrame with the explicit schema. This is what
# guarantees column order + nullability match the contract.
silver_df = spark.createDataFrame(silver_rows, schema=SILVER)

# ---------------------------------------------------------------------------
# 3. MERGE into silver/weather
# ---------------------------------------------------------------------------

target_path = silver_table_path(storage_account)

# Delta MERGE: WHEN MATCHED UPDATE, WHEN NOT MATCHED INSERT.
# This makes the notebook idempotent — re-running for the same hour
# updates the existing rows in place rather than duplicating them.
from delta.tables import DeltaTable  # type: ignore  # noqa: E402

if DeltaTable.isDeltaTable(spark, target_path):
    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("t")
        .merge(
            silver_df.alias("s"),
            "t.city = s.city AND t.local_time = s.local_time",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # First run for this environment — create the Delta table by writing.
    silver_df.write.format("delta").mode("overwrite").save(target_path)

rows_merged = len(silver_rows)
print(json.dumps({
    "rows_in": rows_in,
    "rows_merged": rows_merged,
    "rows_dropped": rows_dropped,
    "target_path": target_path,
}))
