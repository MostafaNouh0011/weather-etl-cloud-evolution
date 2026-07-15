# Architecture Comparison: v1 (Local) vs v2 (Azure)

A side-by-side look at the same ETL problem solved in two very different
runtimes. Use this as a quick reference when reviewing the code.

## High-level

```
┌─────────────────────────────── v1 ───────────────────────────────┐
│                                                                    │
│  Weatherstack  ──►  Airflow (Docker)  ──►  PostgreSQL               │
│  API               ├─ extract.py         ├─ raw.weather_raw         │
│  (sequential)      ├─ transform.py       └─ analytics.weather_     │
│                    └─ load.py              summary                 │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────── v2 ───────────────────────────────┐
│                                                                    │
│  Weatherstack  ──►  ADF  ──►  Databricks (PySpark)  ──►  ADLS     │
│  API               trigger   ├─ NB1 Bronze            Gen2         │
│  (parallel)        + 3 NB    ├─ NB2 Silver          ├─ bronze/    │
│                    activities └─ NB3 Gold           ├─ silver/    │
│                                                       └─ gold/     │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

## Stage-by-stage

### Extract

| | v1 | v2 |
|---|---|---|
| Where it runs | Airflow worker (Docker) | Databricks job cluster |
| How cities are listed | `CITIES = [...]` constant in `extract.py` | `shared/cities.json` (or `azure/config/cities.json`) read from ADLS |
| API call pattern | Sequential `for city in CITIES` | `ThreadPoolExecutor(max_workers=10)` in `weather_client.py` |
| Retry / timeout | `requests` default, 10s timeout | Same `requests` call, plus ADF activity-level retry (3 attempts) |
| Output | Python list of dicts in memory → XCom | Parquet files in `bronze/weather/year=.../month=.../day=.../hour=.../` |
| Failure mode | If one city fails, others still proceed (`None` is filtered). DAG task fails only if the entire list is empty. | Same logic — failed cities get a `None` row in Bronze, not a pipeline failure. A second-pass alert job can flag them. |

### Transform

| | v1 | v2 |
|---|---|---|
| Engine | Pandas DataFrame | PySpark DataFrame |
| Schema | Inferred from `dict` keys | Explicit `StructType` in `azure/databricks/libs/schemas.py` |
| Cleansing rules | `parse_response` in `transform.py` | `silver_cleanse()` UDF in `02_silver_cleanse.py` — same rules |
| Null handling | `dropna(subset=["city", "temperature_c"])` | `filter(col("city").isNotNull() & col("temperature_c").isNotNull())` |
| Deduplication | None at the row level (Bronze-equivalent `raw.weather_raw` has no natural key) | `MERGE` on `(city, local_time)` in Silver — late arrivals overwrite, duplicates drop |
| Output | Pandas DataFrame in XCom | Delta table at `silver/weather/` |

### Load

| | v1 | v2 |
|---|---|---|
| Raw / bronze layer | Postgres `raw.weather_raw` (append) | Parquet in `bronze/weather/` (append, partitioned by ingestion date/hour) |
| Aggregation table | `analytics.weather_summary` (one row per city per day) | `gold/weather_hourly` (per city per hour) **and** `gold/weather_daily` (per city per day) |
| Upsert mechanism | `INSERT ... ON CONFLICT (city, record_date) DO UPDATE` | Delta `MERGE INTO gold.weather_daily USING (...) ON city AND record_date` |
| What gets re-aggregated each run | The whole `raw` table | Only the current hour's worth of data → still day-level `MERGE` is correct because Delta `MERGE` is idempotent |
| Consumer access | `psql` / DBeaver / `pandas.read_sql` | Power BI / Synapse / `spark.read` / `pandas.read_parquet` after mounting ADLS |

## Storage

| | v1 (Postgres) | v2 (ADLS Gen2 + Delta) |
|---|---|---|
| Format | Row store, normalized | Columnar, partitioned by date |
| File format | n/a (binary) | Parquet (Bronze) + Delta (Silver, Gold) |
| Schema evolution | Manual `ALTER TABLE` | Delta: add column, no migration |
| Time travel | None | `DESCRIBE HISTORY silver.weather` |
| Indexing | B-tree on `(city, record_date)` | Partition pruning + Z-order (optional) |
| Cost model | Self-hosted (free) | Pay-per-GB-month + transactions |
| Backup | `pg_dump` | ADLS soft delete + versioning + GRS replication |

## Orchestration

| | v1 (Airflow) | v2 (ADF) |
|---|---|---|
| Where it runs | Local Docker (webserver + scheduler) | Azure-managed |
| Schedule definition | `schedule_interval="0 */6 * * *"` in DAG file | "Schedule trigger" in ADF with cron expression |
| Task definition | `PythonOperator` per task | "Notebook" activity per stage, in a pipeline JSON |
| Retry / alerting | `default_args` in DAG | Activity-level retry policy + Azure Monitor alerts |
| UI for ops | Airflow webserver on `:8080` | ADF Studio + Azure portal |
| Dependency management | `requirements.txt` baked into custom Airflow image | Databricks Runtime + cluster libraries; no custom image required |
| Who runs the upgrade | You | Microsoft |

## Compute

| | v1 | v2 |
|---|---|---|
| Engine | Python 3.11 + Pandas 2.1.4 | PySpark on Databricks Runtime |
| Cluster | None — single Airflow worker | Job cluster, auto-terminated after run |
| Sizing | Fixed (the box Airflow runs on) | Configurable per ADF activity (`Standard_DS3_v2` worker type, 1–4 workers) |
| Cold start | Negligible | ~3–5 min for cluster spin-up |
| Cost when idle | Zero (local) | Zero (job cluster terminates) |
| Cost when running | Free | ~DBU/hour (Databricks Units) — roughly $0.15/DBU on standard tier |
| Distributed? | No | Yes — Spark's whole point |

## Secrets

| | v1 | v2 |
|---|---|---|
| Where the API key lives | `.env` (gitignored) | Azure Key Vault |
| How code reads it | `os.getenv("WEATHERSTACK_API_KEY")` | `dbutils.secrets.get(scope="kv-scope", key="weatherstack-api-key")` |
| Rotation | Edit `.env` and restart | Rotate in Key Vault; no restart needed |
| Audit | None | Azure Activity Log + Key Vault diagnostic logs |
| Risk if repo is public | `.env` is gitignored, but if it ever leaks, key is exposed | Key never leaves Key Vault |

## Observability

| | v1 | v2 |
|---|---|---|
| Logs | `logs/dag_id=.../run_id=.../task_id=.../attempt=N.log` files in the `logs/` folder | Databricks cluster logs to Log Analytics; ADF run logs to Azure Monitor |
| Metrics | Airflow UI | ADF + Databricks dashboards |
| Alerting | `email_on_failure: False` (set to True to opt in) | Azure Monitor action groups → email / Teams / PagerDuty |
| Lineage | Manual (READMEs) | ADF activity view + Delta transaction log |

## Operational footprint

| | v1 | v2 |
|---|---|---|
| Prereqs to run | Docker Desktop | Azure subscription, `az` CLI, a service principal, Databricks workspace |
| Time to first successful run | ~10 min | ~45 min (incl. Bicep deploy) |
| Ongoing maintenance | Docker images, Airflow upgrades, Postgres vacuuming | None at the infra level; ADF and Databricks are managed |
| Failure recovery | Restart Docker, replay tasks | ADF rerun-the-pipeline; Databricks rerun-the-job; Delta time travel for data |

## Who is each version for?

- **v1** is for a junior data engineer who needs a working pipeline
  they can run on their laptop without an Azure account. It teaches
  Docker, Airflow, SQL, ETL design, and the discipline of idempotent
  writes.
- **v2** is for the same engineer two years later, working in a team
  that runs on Azure. It teaches distributed compute, decoupled
  storage, native orchestration, schema evolution, and cost
  discipline (job clusters over all-purpose clusters).

Both are useful. Neither is "wrong." The point of putting them in the
same repo is to make the *transition* visible.
