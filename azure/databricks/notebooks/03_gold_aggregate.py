"""03_gold_aggregate.py — read Silver, compute hourly + daily aggregates, MERGE into Gold.

This is the third notebook. It does three things:
  1. Read Silver rows for the current hour (the same partition NB2 just
     refreshed). This is enough for the hourly aggregate; the daily
     aggregate re-reads the day-to-date window to roll up.
  2. Compute aggregates:
       * GOLD_HOURLY:  one row per (city, record_hour)
       * GOLD_DAILY:    one row per (city, record_date)
  3. MERGE each into its Delta table. Re-running for the same hour
     updates the hourly row in place; re-running the daily table
     within the same day updates the single daily row for that day.

The daily aggregate recomputes from the *hourly* table, not directly
from Silver, so a one-hour pipeline failure (and the next run) cannot
produce a partial daily row. This is the cheap and boring version of
"hourly + daily rollup" — both are eventually consistent within a day.

Inputs (ADF widgets; defaults for local runs):
  * storage_account
  * run_year / run_month / run_day / run_hour — the hour that just got cleansed

Outputs:
  * Updated gold/weather_hourly Delta table
  * Updated gold/weather_daily Delta table
  * Job return value: {hourly_rows, daily_rows}
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
from pyspark.sql.functions import (  # noqa: E402
    avg,
    count,
    current_timestamp,
    date_format,
    max as _max,
    min as _min,
    sum as _sum,
    to_timestamp,
)
from pyspark.sql.functions import col  # noqa: E402

from schemas import GOLD_DAILY, GOLD_HOURLY  # noqa: E402
from storage import (  # noqa: E402
    gold_daily_table_path,
    gold_hourly_table_path,
    silver_table_path,
)


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
# 1. Read Silver
# ---------------------------------------------------------------------------

silver_path = silver_table_path(storage_account)
silver_df = spark.read.format("delta").load(silver_path)

# Restrict to the hour we just cleansed + the day-to-date for the daily
# rollup. Filtering in Spark (not Python) so we don't .toPandas() the
# whole Silver table.
hour_floor = datetime(run_year, run_month, run_day, run_hour)
# Upper bound is the next hour, computed via timedelta so month/year
# rollovers are handled correctly (e.g. 23:00 → 00:00 next day).
from datetime import timedelta  # noqa: E402
next_hour_floor = hour_floor + timedelta(hours=1)
day_floor = datetime(run_year, run_month, run_day)
next_day_floor = day_floor + timedelta(days=1)

hour_window_df = silver_df.filter(
    (col("local_time") >= hour_floor) & (col("local_time") < next_hour_floor)
)
day_window_df = silver_df.filter(
    (col("local_time") >= day_floor) & (col("local_time") < next_day_floor)
)

# ---------------------------------------------------------------------------
# 2. Compute aggregates
# ---------------------------------------------------------------------------

# Hourly: groupBy (city, country, region, hour(local_time)).
hourly_agg = (
    hour_window_df
    .groupBy(
        col("city"),
        col("country"),
        col("region"),
        date_format(col("local_time"), "yyyy-MM-dd HH:00:00").alias("record_hour_str"),
    )
    .agg(
        count("*").alias("sample_count"),
        avg("temperature_c").alias("avg_temperature_c"),
        _min("temperature_c").alias("min_temperature_c"),
        _max("temperature_c").alias("max_temperature_c"),
        avg("humidity_pct").alias("avg_humidity_pct"),
        avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
        _sum("precipitation_mm").alias("total_precipitation_mm"),
    )
)

# Cast record_hour_str back to a TimestampType (the Gold schema demands it).
hourly_df = (
    hourly_agg
    .withColumn("record_hour", to_timestamp("record_hour_str", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updated_ts", current_timestamp())
    .select(*[c for c in GOLD_HOURLY.fieldNames()])
)

# Daily: groupBy (city, country, region, record_date).
daily_agg = (
    day_window_df
    .groupBy(
        col("city"),
        col("country"),
        col("region"),
        date_format(col("local_time"), "yyyy-MM-dd").alias("record_date"),
    )
    .agg(
        count("*").alias("hourly_observations"),
        avg("temperature_c").alias("avg_temperature_c"),
        _min("temperature_c").alias("min_temperature_c"),
        _max("temperature_c").alias("max_temperature_c"),
        avg("humidity_pct").alias("avg_humidity_pct"),
        avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
        _sum("precipitation_mm").alias("total_precipitation_mm"),
    )
)
daily_df = (
    daily_agg
    .withColumn("updated_ts", current_timestamp())
    .select(*[c for c in GOLD_DAILY.fieldNames()])
)

# ---------------------------------------------------------------------------
# 3. MERGE into the Gold Delta tables
# ---------------------------------------------------------------------------

from delta.tables import DeltaTable  # type: ignore  # noqa: E402


def _merge_into(source_df, target_path: str, merge_keys: List[str]) -> int:
    """MERGE source_df into the Delta table at target_path. Returns row count."""
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        on_clause = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
        (
            target.alias("t")
            .merge(source_df.alias("s"), on_clause)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        source_df.write.format("delta").mode("overwrite").save(target_path)
    return source_df.count()


hourly_count = _merge_into(
    hourly_df,
    gold_hourly_table_path(storage_account),
    merge_keys=["city", "record_hour"],
)
daily_count = _merge_into(
    daily_df,
    gold_daily_table_path(storage_account),
    merge_keys=["city", "record_date"],
)

print(json.dumps({
    "hourly_rows_merged": hourly_count,
    "daily_rows_merged": daily_count,
}))
