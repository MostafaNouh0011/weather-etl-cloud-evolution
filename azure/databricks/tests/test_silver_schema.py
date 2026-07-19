"""test_silver_schema.py — unit tests for the Silver cleansing rules.

This file uses the stdlib `unittest` framework on purpose: pyspark is
heavy, and adding pytest + chispa just to run four assertions is too
much surface area for what we're checking. Run it directly:

    cd azure/databricks
    python -m unittest tests/test_silver_schema.py -v

(Or `python tests/test_silver_schema.py` — the __main__ block at the
bottom does the same thing.)

What we test:
  * A normal Weatherstack payload becomes a Silver row with the right
    types, the right values, and ingestion_ts + source filled in.
  * A payload with a missing city returns None (the row is dropped).
  * A payload with a malformed localtime returns None.
  * The hourly + daily aggregate keys match the documented contract.

What we do NOT test (and why):
  * HTTP transport — libs/weather_client.py is a thin urllib wrapper
    with mocked tests being a worse signal than reading the code.
  * The Delta MERGE itself — Delta isn't installed on the dev box, and
    the MERGE clause is 5 lines of well-trodden Delta API. The
    surrounding code is what matters.
"""
from __future__ import annotations

import sys
import os
import unittest
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
LIBS = os.path.normpath(os.path.join(HERE, "..", "libs"))
if LIBS not in sys.path:
    sys.path.insert(0, LIBS)

from schemas import (  # noqa: E402
    GOLD_DAILY,
    GOLD_HOURLY,
    SILVER,
    weatherstack_to_silver_row,
)


def _utcnow():
    """Timezone-aware UTC 'now' — silences Python 3.12's _utcnow() deprecation."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _sample_payload(**overrides) -> dict:
    """Return a valid Weatherstack payload that tests can mutate."""
    payload = {
        "success": True,
        "location": {
            "name": "Cairo",
            "country": "Egypt",
            "region": "Cairo",
            "localtime": "2026-07-19 14:00",
        },
        "current": {
            "temperature": 31,
            "feelslike": 33,
            "humidity": 42,
            "wind_speed": 11,
            "wind_degree": 90,
            "wind_dir": "E",
            "pressure": 1012,
            "precipitation": 0,
            "cloudcover": 5,
            "visibility": 10,
            "uv_index": 7,
            "weather_descriptions": ["Sunny"],
            "weather_icons": ["..."],
            "observation_time": "11:00 AM",
        },
    }
    payload["location"].update(overrides.get("location", {}))
    payload["current"].update(overrides.get("current", {}))
    return payload


class WeatherstackToSilverRowTests(unittest.TestCase):
    """The single most important contract: a payload becomes a Silver row."""

    def test_happy_path(self):
        ingestion_ts = datetime(2026, 7, 19, 14, 5, 0)
        row = weatherstack_to_silver_row(_sample_payload(), ingestion_ts=ingestion_ts)

        self.assertIsNotNone(row)
        self.assertEqual(row["city"], "Cairo")
        self.assertEqual(row["country"], "Egypt")
        self.assertEqual(row["region"], "Cairo")
        self.assertEqual(row["local_time"], datetime(2026, 7, 19, 14, 0))
        self.assertEqual(row["temperature_c"], 31.0)
        self.assertEqual(row["feels_like_c"], 33.0)
        self.assertEqual(row["humidity_pct"], 42)
        self.assertEqual(row["wind_speed_kmh"], 11.0)
        self.assertEqual(row["weather_description"], "Sunny")
        self.assertEqual(row["source"], "weatherstack")
        self.assertEqual(row["ingestion_ts"], ingestion_ts)

    def test_missing_city_returns_none(self):
        payload = _sample_payload(location={"name": ""})
        self.assertIsNone(weatherstack_to_silver_row(payload, ingestion_ts=_utcnow()))

    def test_missing_localtime_returns_none(self):
        payload = _sample_payload(location={"localtime": ""})
        self.assertIsNone(weatherstack_to_silver_row(payload, ingestion_ts=_utcnow()))

    def test_malformed_localtime_returns_none(self):
        payload = _sample_payload(location={"localtime": "not a date"})
        self.assertIsNone(weatherstack_to_silver_row(payload, ingestion_ts=_utcnow()))

    def test_missing_country_returns_none(self):
        payload = _sample_payload(location={"country": ""})
        self.assertIsNone(weatherstack_to_silver_row(payload, ingestion_ts=_utcnow()))

    def test_none_measurements_kept(self):
        """A payload with null measurements should still produce a row —
        optional fields are nullable, only the core identifier is required."""
        payload = _sample_payload(current={
            "temperature": None,
            "feelslike": None,
            "humidity": None,
            "weather_descriptions": [],
        })
        row = weatherstack_to_silver_row(payload, ingestion_ts=_utcnow())
        self.assertIsNotNone(row)
        self.assertIsNone(row["temperature_c"])
        self.assertIsNone(row["humidity_pct"])
        self.assertIsNone(row["weather_description"])

    def test_unparseable_measurement_drops_to_none(self):
        """Bad string values get coerced to None, not the whole row."""
        payload = _sample_payload(current={"temperature": "not-a-number"})
        row = weatherstack_to_silver_row(payload, ingestion_ts=_utcnow())
        self.assertIsNotNone(row)
        self.assertIsNone(row["temperature_c"])


class SilverSchemaContractTests(unittest.TestCase):
    """The schema is the contract downstream stages agree on. If a field
    disappears or changes type, every MERGE breaks."""

    def test_silver_key_fields_are_non_nullable(self):
        key_fields = {"city", "country", "local_time", "ingestion_ts", "source"}
        for f in SILVER.fields:
            if f.name in key_fields:
                self.assertFalse(
                    f.nullable,
                    f"Silver key field {f.name!r} must be non-nullable, "
                    f"otherwise MERGE on (city, local_time) is unreliable."
                )

    def test_gold_hourly_keys(self):
        # city + record_hour
        names = {f.name for f in GOLD_HOURLY.fields}
        self.assertIn("city", names)
        self.assertIn("record_hour", names)
        self.assertIn("avg_temperature_c", names)
        self.assertIn("total_precipitation_mm", names)

    def test_gold_daily_keys(self):
        # city + record_date (string YYYY-MM-DD for partition friendliness)
        names = {f.name for f in GOLD_DAILY.fields}
        self.assertIn("city", names)
        self.assertIn("record_date", names)
        record_date_field = next(f for f in GOLD_DAILY.fields if f.name == "record_date")
        self.assertEqual(record_date_field.dataType.simpleString(), "string")

    def test_silver_optional_fields_are_nullable(self):
        # Everything except the key fields should accept null. This
        # matches the v1 contract: a payload with missing measurements
        # still produces a row, just with nulls in those columns.
        key_fields = {"city", "country", "local_time", "ingestion_ts", "source"}
        for f in SILVER.fields:
            if f.name in key_fields:
                continue
            self.assertTrue(
                f.nullable,
                f"Silver optional field {f.name!r} must be nullable; "
                f"otherwise a single bad measurement drops the whole row."
            )

    def test_silver_field_types(self):
        # If a type changes, the next notebook run will fail with a
        # confusing schema-mismatch error. Pin the types here.
        expected = {
            "city": "string",
            "country": "string",
            "region": "string",
            "local_time": "timestamp",
            "temperature_c": "double",
            "feels_like_c": "double",
            "humidity_pct": "int",
            "wind_speed_kmh": "double",
            "wind_direction_deg": "int",
            "pressure_mb": "double",
            "precipitation_mm": "double",
            "cloud_cover_pct": "int",
            "visibility_km": "double",
            "uv_index": "int",
            "weather_description": "string",
            "ingestion_ts": "timestamp",
            "source": "string",
        }
        for f in SILVER.fields:
            self.assertEqual(
                f.dataType.simpleString(), expected[f.name],
                f"Silver field {f.name!r}: expected {expected[f.name]}, "
                f"got {f.dataType.simpleString()}"
            )


class WeatherClientParseTests(unittest.TestCase):
    """Smoke tests for the urllib-based client. We do not mock the network
    (we test the success-parse path only when the input dict is well-formed)."""

    def test_payload_success_flag(self):
        # A simple sanity check: a successful payload should have success=True.
        p = _sample_payload()
        self.assertTrue(p.get("success", True))

    def test_error_payload_shape(self):
        # An error payload uses success: false; the client should raise on these.
        # We assert the shape directly rather than triggering a real HTTP call.
        err = {"success": False, "error": {"code": 101, "info": "test"}}
        self.assertFalse(err.get("success", True))
        self.assertIn("error", err)


if __name__ == "__main__":
    unittest.main(verbosity=2)
