"""Explicit Spark StructType definitions for the v2 Medallion tables.

The whole point of these schemas is to make the cleansing rules visible
and reviewable, and to make the Silver/Gold MERGE keys contract-stable.
If a field's type or nullable flag changes here, the next notebook run
will fail loudly instead of silently coercing and corrupting the
downstream aggregate.

Three schemas:
  * BRONZE_RAW    — what we keep from the Weatherstack payload. Loose
                    (most fields nullable) so a partial API response
                    does not drop the whole record.
  * SILVER        — what survives cleansing. Strict (core fields
                    non-nullable). The (city, local_time) pair is the
                    dedup / MERGE key.
  * GOLD_HOURLY   — one row per (city, record_hour). Hourly aggregates
                    over the Silver records.
  * GOLD_DAILY    — one row per (city, record_date). Daily rollup of
                    the hourly aggregates.
"""
from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Silver schema — the contract every downstream stage agrees on.
# ---------------------------------------------------------------------------

SILVER: StructType = StructType([
    # Identifiers / keys
    StructField("city", StringType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("region", StringType(), nullable=True),
    StructField("local_time", TimestampType(), nullable=False),

    # Weather measurements
    StructField("temperature_c", DoubleType(), nullable=True),
    StructField("feels_like_c", DoubleType(), nullable=True),
    StructField("humidity_pct", IntegerType(), nullable=True),
    StructField("wind_speed_kmh", DoubleType(), nullable=True),
    StructField("wind_direction_deg", IntegerType(), nullable=True),
    StructField("pressure_mb", DoubleType(), nullable=True),
    StructField("precipitation_mm", DoubleType(), nullable=True),
    StructField("cloud_cover_pct", IntegerType(), nullable=True),
    StructField("visibility_km", DoubleType(), nullable=True),
    StructField("uv_index", IntegerType(), nullable=True),
    StructField("weather_description", StringType(), nullable=True),

    # Lineage
    StructField("ingestion_ts", TimestampType(), nullable=False),
    StructField("source", StringType(), nullable=False),
])


# ---------------------------------------------------------------------------
# Gold hourly — one row per (city, record_hour).
# ---------------------------------------------------------------------------

GOLD_HOURLY: StructType = StructType([
    StructField("city", StringType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("region", StringType(), nullable=True),
    StructField("record_hour", TimestampType(), nullable=False),

    # Aggregates
    StructField("sample_count", LongType(), nullable=False),
    StructField("avg_temperature_c", DoubleType(), nullable=True),
    StructField("min_temperature_c", DoubleType(), nullable=True),
    StructField("max_temperature_c", DoubleType(), nullable=True),
    StructField("avg_humidity_pct", DoubleType(), nullable=True),
    StructField("avg_wind_speed_kmh", DoubleType(), nullable=True),
    StructField("total_precipitation_mm", DoubleType(), nullable=True),

    # Lineage
    StructField("updated_ts", TimestampType(), nullable=False),
])


# ---------------------------------------------------------------------------
# Gold daily — one row per (city, record_date).
# ---------------------------------------------------------------------------

GOLD_DAILY: StructType = StructType([
    StructField("city", StringType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("region", StringType(), nullable=True),
    StructField("record_date", StringType(), nullable=False),  # YYYY-MM-DD, partition-friendly

    StructField("hourly_observations", LongType(), nullable=False),
    StructField("avg_temperature_c", DoubleType(), nullable=True),
    StructField("min_temperature_c", DoubleType(), nullable=True),
    StructField("max_temperature_c", DoubleType(), nullable=True),
    StructField("avg_humidity_pct", DoubleType(), nullable=True),
    StructField("avg_wind_speed_kmh", DoubleType(), nullable=True),
    StructField("total_precipitation_mm", DoubleType(), nullable=True),

    StructField("updated_ts", TimestampType(), nullable=False),
])


# ---------------------------------------------------------------------------
# Coercion helpers — turn a raw Weatherstack payload into a Silver row.
# ---------------------------------------------------------------------------

def weatherstack_to_silver_row(payload: dict, ingestion_ts) -> dict | None:
    """Project a Weatherstack payload to a Silver-row dict.

    Returns None when the payload is missing the *core* fields (city,
    local_time). The notebook should treat None as "drop this record"
    rather than write a half-empty row.

    Pure function — no Spark calls, easy to unit-test.
    """
    location = payload.get("location") or {}
    current = payload.get("current") or {}
    city = (location.get("name") or "").strip()
    if not city:
        return None
    local_time_str = (location.get("localtime") or "").strip()
    if not local_time_str:
        return None
    # Weatherstack returns "YYYY-MM-DD HH:MM" without seconds or timezone.
    # We treat it as UTC for storage; the (city, local_time) pair is the
    # dedup key so consistent UTC semantics are required.
    from datetime import datetime
    try:
        local_time = datetime.strptime(local_time_str, "%Y-%m-%d %H:%M")
    except ValueError:
        return None
    country = (location.get("country") or "").strip()
    if not country:
        return None

    descriptions = current.get("weather_descriptions") or []
    description = descriptions[0] if descriptions else None

    return {
        "city": city,
        "country": country,
        "region": (location.get("region") or "").strip() or None,
        "local_time": local_time,

        "temperature_c": _to_float(current.get("temperature")),
        "feels_like_c": _to_float(current.get("feelslike")),
        "humidity_pct": _to_int(current.get("humidity")),
        "wind_speed_kmh": _to_float(current.get("wind_speed")),
        "wind_direction_deg": _to_int(current.get("wind_degree")),
        "pressure_mb": _to_float(current.get("pressure")),
        "precipitation_mm": _to_float(current.get("precipitation")),
        "cloud_cover_pct": _to_int(current.get("cloudcover")),
        "visibility_km": _to_float(current.get("visibility")),
        "uv_index": _to_int(current.get("uv_index")),
        "weather_description": (description or "").strip() or None,

        "ingestion_ts": ingestion_ts,
        "source": "weatherstack",
    }


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    f = _to_float(value)
    if f is None:
        return None
    return int(f)
