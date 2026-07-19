# Azure Databricks code

PySpark notebooks and reusable Python libraries for the v2 Azure-native
pipeline. The structure mirrors v1 (`src/extract.py`, `transform.py`,
`load.py`) but the runtime is Databricks + Delta, not Pandas + Postgres.

## Status

✅ **Scaffolded.** Three notebooks + three libs + one test file. The
schema and the Silver cleansing rules are covered by 14 unit tests that
run on any laptop with PySpark installed (no Azure or Databricks
required):

```bash
cd azure/databricks
python tests/test_silver_schema.py
```

## Layout

```
databricks/
├── notebooks/
│   ├── 01_bronze_ingest.py     # NB1: parallel API → Parquet in bronze/
│   ├── 02_silver_cleanse.py    # NB2: typed, deduped → Delta in silver/
│   └── 03_gold_aggregate.py    # NB3: hourly + daily → Delta in gold/
├── libs/
│   ├── weather_client.py       # ThreadPoolExecutor-based Weatherstack client
│   ├── schemas.py              # Explicit Spark StructType + Silver coercion
│   └── storage.py              # ABFSS path helpers + dbutils-aware secret reader
├── jobs/
│   └── (empty — ADF owns the schedule)
└── tests/
    └── test_silver_schema.py   # stdlib unittest, runs locally
```

## Notebooks in one paragraph each

### 01_bronze_ingest.py

Reads `cities.json` from the `config` container, reads the Weatherstack
API key from Key Vault (or env var in local dev), calls the API in
parallel with a `ThreadPoolExecutor(10)`, and writes a single Parquet
file per run to `abfss://bronze@<account>/weather/year=YYYY/month=MM/day=DD/hour=HH/`.
Bronze is append-only and never the source of a pipeline failure — bad
cities are logged and skipped. If *every* city fails, the notebook
raises so ADF surfaces a useful error.

### 02_silver_cleanse.py

Reads Bronze for the current hour, projects each row through
`weatherstack_to_silver_row()` (the pure function in `libs/schemas.py`),
and `MERGE`s the result into `silver/weather` on `(city, local_time)`.
Re-running for the same hour is a no-op. Rows missing core fields
(city, country, local_time) are dropped; rows missing optional
measurements are kept with nulls.

### 03_gold_aggregate.py

Reads Silver for the current hour + the day-to-date window, computes
hourly and daily aggregates, and `MERGE`s into `gold/weather_hourly` on
`(city, record_hour)` and `gold/weather_daily` on `(city, record_date)`.
The daily table is recomputed from the day-to-date Silver rows, not
from the hourly Gold table, so a single-hour failure cannot produce a
partial daily row.

## MERGE keys (do not change without a migration)

| Table | Key |
|---|---|
| `silver/weather` | `(city, local_time)` |
| `gold/weather_hourly` | `(city, record_hour)` |
| `gold/weather_daily` | `(city, record_date)` |

## Libraries

### `libs/weather_client.py`

Thin `urllib` + `ThreadPoolExecutor` wrapper. `fetch_all_resilient()`
returns `(successes, failures)` — a single bad city does not blank the
run. The function is pure: it takes a list of city dicts and an API
key, returns a list of payloads, has no Spark / dbutils / filesystem
imports. That's what makes it unit-testable.

### `libs/schemas.py`

The three Medallion schemas (`SILVER`, `GOLD_HOURLY`, `GOLD_DAILY`) and
the `weatherstack_to_silver_row()` pure function that projects an API
payload to a Silver-row dict. Type-coercion rules (string → int / float
with `None` on parse failure) live in `_to_int` / `_to_float`; they
are the only place that knows the API can return a string where the
schema expects a number.

### `libs/storage.py`

ABFSS path helpers (`bronze_path`, `silver_table_path`,
`gold_hourly_table_path`, `gold_daily_table_path`, `config_cities_path`)
plus a `get_secret()` function that reads via `dbutils.secrets.get`
when available and falls back to an env var when not. The fallback
name is **opt-in** (`env_fallback=...`); production notebooks never
silently read from a stray environment variable.

## Local development

The libs and notebooks are written so the pure parts run without
Databricks:

```bash
# 1. Install dependencies (one-time).
pip install pyspark

# 2. Run the schema tests.
cd azure/databricks
python tests/test_silver_schema.py
# -> Ran 14 tests in 0.002s, OK

# 3. Local Spark (slow on Windows because of the JVM warmup) can
#    actually exercise the createDataFrame path. The unit tests are
#    the cheap check; local Spark is the expensive one.
```

The Bronze notebook can be run end-to-end locally against the real
Weatherstack API by exporting `WEATHERSTACK_API_KEY` and
`AZURE_STORAGE_ACCOUNT`. (You'll also need the `hadoop-azure` and
`azure-storage` jars on the classpath for ABFSS to work outside
Databricks — see `docs/local-dev.md` if/when it exists.)

## What is NOT here

- **The ADF pipeline JSON.** ADF owns orchestration; that's in
  `azure/adf/`. The notebooks are designed to work as Databricks
  Notebook activities, with the four widgets per notebook
  (`storage_account`, plus year/month/day/hour for Silver + Gold).
- **The Bronze raw schema** is not declared in `libs/schemas.py`
  because Bronze is intentionally loose — Parquet self-describes it
  and the Silver notebook re-projects every column. Pinning a Bronze
  schema would force every API change to require a Bicep / notebook
  change.
- **A `gold_weather_weekly` or higher rollup.** Daily is enough for
  v1 of v2; weekly / monthly can be added by copying the pattern in
  `03_gold_aggregate.py`.
- **Streaming / Auto Loader.** The Bronze write is a batch Parquet
  file per hour; Auto Loader would only matter at sub-minute cadence.

## Reference

- [`../README.md`](../README.md) — architecture overview
- [`../deploy.md`](../deploy.md) — how to import these notebooks
- [`../../../docs/evolution.md`](../../../docs/evolution.md) — design rationale
