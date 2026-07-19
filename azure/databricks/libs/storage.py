"""ADLS Gen2 path helpers and a dbutils-aware secret reader.

The two functions in this module are the only place in the v2 code that
talks to Databricks-isms (dbutils) or to ABFSS URIs. Everything else
treats paths as opaque strings and reads keys via get_secret().

get_secret() falls back to environment variables so the Bronze notebook
can be unit-tested on a laptop (where dbutils does not exist) by
exporting WEATHERSTACK_API_KEY=... in the shell.
"""
from __future__ import annotations

import os
from typing import Optional


# ---------------------------------------------------------------------------
# ABFSS path helpers
# ---------------------------------------------------------------------------
# Convention: abfss://<container>@<account>.dfs.core.windows.net/<path>
# We build them from the four outputs main.bicep emits (storageAccountName +
# the four container names) plus a tenant-supplied path suffix. Notebooks
# import these once at the top of the file and never re-derive ABFSS URIs.

def abfss_uri(storage_account: str, container: str, path: str = "") -> str:
    """Build an ABFSS URI for the given storage account + container + path.

    `path` is joined with a single '/' separator; an empty `path` returns
    the container root (useful for container-level listings).
    """
    if not storage_account or not container:
        raise ValueError("storage_account and container are required")
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    if not path:
        return base
    return f"{base}/{path.lstrip('/')}"


def bronze_path(storage_account: str, run_year: int, run_month: int,
                run_day: int, run_hour: int) -> str:
    """Hour-partitioned Bronze path: bronze/weather/year=.../month=.../day=.../hour=..."""
    return abfss_uri(
        storage_account,
        "bronze",
        f"weather/year={run_year:04d}/month={run_month:02d}/day={run_day:02d}/hour={run_hour:02d}",
    )


def silver_table_path(storage_account: str) -> str:
    """Path to the Silver Delta table (one table, not partitioned)."""
    return abfss_uri(storage_account, "silver", "weather")


def gold_hourly_table_path(storage_account: str) -> str:
    """Path to the Gold hourly aggregate Delta table."""
    return abfss_uri(storage_account, "gold", "weather_hourly")


def gold_daily_table_path(storage_account: str) -> str:
    """Path to the Gold daily aggregate Delta table."""
    return abfss_uri(storage_account, "gold", "weather_daily")


def config_cities_path(storage_account: str) -> str:
    """Path to the cities.json uploaded by deploy.md step 3."""
    return abfss_uri(storage_account, "config", "cities.json")


# ---------------------------------------------------------------------------
# Secret reader
# ---------------------------------------------------------------------------

def get_secret(scope: str, key: str, *, env_fallback: Optional[str] = None) -> str:
    """Read a secret from a Databricks-backed scope, with a local-dev fallback.

    In a Databricks cluster, dbutils.secrets.get(scope, key) is the only
    supported way to read Key Vault secrets — it never returns the value
    in plain text outside the cell, it never appears in driver logs, and
    the access is audited in Key Vault.

    On a developer laptop dbutils does not exist, so the same code path
    reads the value from the environment. Two ways to populate it:

        1. `export WEATHERSTACK_API_KEY=...` in the shell, and pass
           env_fallback="WEATHERSTACK_API_KEY".
        2. Add a `python-dotenv` load and pull from a local .env file.

    The fallback name is opt-in (default None) so production notebooks
    never silently read from a stray environment variable.
    """
    try:
        # dbutils is a Databricks-only global; importing it raises on a
        # plain Python interpreter.
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession  # type: ignore
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark).secrets.get(scope=scope, key=key)
    except Exception:
        # Either dbutils is not present (laptop) or the scope/key is
        # misconfigured in Databricks. The local fallback only kicks in
        # when the caller asked for one.
        if env_fallback is None:
            raise RuntimeError(
                f"Cannot read secret {scope}/{key} via dbutils and no "
                f"env_fallback was provided. Set env_fallback to an env-var "
                f"name to allow local-dev reads."
            )
        value = os.environ.get(env_fallback)
        if not value:
            raise RuntimeError(
                f"Env var {env_fallback} is not set; cannot fall back for "
                f"secret {scope}/{key}."
            )
        return value
