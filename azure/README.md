# Azure Weather ETL — v2 (ADF + Databricks + ADLS Gen2)

> This folder is the **v2** of the project. v1 (local Docker + Airflow +
> PostgreSQL) lives at the repository root and is frozen at tag `v1.0.0`.
> For the full migration story, see [`../docs/evolution.md`](../docs/evolution.md).
> For a side-by-side comparison, see
> [`../docs/architecture/comparison.md`](../docs/architecture/comparison.md).

## What this is

A production-grade Azure-native version of the same ETL pipeline:

- **Source:** Weatherstack API (100 cities, every hour)
- **Orchestration:** Azure Data Factory (hourly schedule trigger, retries, alerts)
- **Compute:** Azure Databricks (PySpark on auto-terminating job clusters)
- **Storage:** ADLS Gen2 with the **Medallion** pattern (Bronze → Silver → Gold in Delta)
- **Secrets:** Azure Key Vault (no API keys in notebooks or `.env` files)
- **Infra as code:** Bicep (one template, one `az deployment` away from a fresh environment)

The 3-stage shape (`extract → transform → load`) and the
`raw → analytics` layer split are preserved from v1. Only the runtime
changed.

## Folder layout

```
azure/
├── README.md                        # ← you are here
├── deploy.md                        # End-to-end deploy guide
├── bicep/                           # Infrastructure-as-Code
│   ├── main.bicep                   # Entry point: resource group, ADLS, Databricks, ADF, KV
│   ├── modules/                     # Reusable Bicep modules
│   └── parameters/
│       └── dev.parameters.json      # Dev-environment parameters
├── databricks/                      # PySpark code & Databricks artefacts
│   ├── notebooks/                   # The 3 pipeline stages
│   │   ├── 01_bronze_ingest.py
│   │   ├── 02_silver_cleanse.py
│   │   └── 03_gold_aggregate.py
│   ├── jobs/                        # (Optional) Databricks Job JSON
│   └── libs/                        # Reusable Python utilities
│       ├── weather_client.py        # Parallel Weatherstack API client
│       ├── schemas.py               # Explicit Spark StructType schemas
│       └── storage.py               # ADLS path helpers
├── adf/                             # Azure Data Factory pipeline definition
│   ├── pipeline/
│   │   └── weather_hourly_pipeline.json
│   ├── linkedService/
│   │   ├── ls_databricks.json
│   │   ├── ls_adls.json
│   │   └── ls_keyvault.json
│   ├── dataset/
│   └── trigger/
│       └── trg_hourly_schedule.json
├── config/                          # Pipeline configuration
│   └── cities.json                  # 100-city list (or symlink to ../shared/cities.json)
├── .env.example                     # Local-dev env template (no real secrets)
└── tests/                           # Optional: pytest + chispa for PySpark unit tests
    └── test_silver_schema.py
```

## Architecture

```
Weatherstack API
       │  HTTP (10 parallel workers per run)
       ▼
┌──────────────────────────────┐
│ Azure Data Factory           │   hourly trigger (cron `0 0 * * * *`)
│  Pipeline: weather_hourly    │   retries: 3, alert on failure
│                              │
│  1. Notebook → Bronze        │
│  2. Notebook → Silver        │
│  3. Notebook → Gold          │
└─────────────┬────────────────┘
              ▼
┌──────────────────────────────┐
│ Azure Databricks             │   job cluster, auto-terminates
│  ┌────────────────────────┐  │
│  │ NB1: bronze_ingest.py  │──┼──► bronze/weather/year=YYYY/month=MM/day=DD/hour=HH/
│  │   ThreadPoolExecutor   │  │    (Parquet, append-only, schemaless)
│  └────────────────────────┘  │
│  ┌────────────────────────┐  │
│  │ NB2: silver_cleanse.py │──┼──► silver/weather/
│  │   typed, deduped       │  │    (Delta, MERGE on city+local_time)
│  └────────────────────────┘  │
│  ┌────────────────────────┐  │
│  │ NB3: gold_aggregate.py │──┼──► gold/weather_hourly
│  │   hourly + daily rollup│  │    gold/weather_daily
│  └────────────────────────┘  │    (Delta, MERGE on city+record_date)
└──────────────────────────────┘
              ▼
┌──────────────────────────────┐
│ ADLS Gen2 (Storage Account)  │
│  bronze/  silver/  gold/     │   Delta on Silver + Gold
│  config/                     │   (cities.json, pipeline parameters)
└──────────────────────────────┘
              ▼
        Power BI / Synapse / ad-hoc
```

## Data flow per hour

1. **ADF schedule trigger** fires at `:00` every hour.
2. **Activity 1 — `nb_bronze_ingest`** (Databricks Notebook):
   - Reads `cities.json` from `abfss://config@<storage>.dfs.core.windows.net/cities.json`
   - Reads the Weatherstack API key from Key Vault via `dbutils.secrets.get`
   - Calls the API in parallel (10 workers) → 100 cities
   - For each successful response, writes one Parquet file to
     `abfss://bronze@<storage>/weather/year=YYYY/month=MM/day=DD/hour=HH/`
3. **Activity 2 — `nb_silver_cleanse`**:
   - Reads Bronze for the current hour
   - Applies the same cleansing rules as v1's `transform.py` (cast, trim, drop nulls, dedupe)
   - `MERGE INTO silver.weather ... ON city, local_time` for idempotency
4. **Activity 3 — `nb_gold_aggregate`**:
   - Reads Silver for the current hour + the day-to-date window
   - Computes hourly + daily aggregates
   - `MERGE INTO gold.weather_hourly` and `gold.weather_daily` on `(city, record_date)` /
     `(city, record_hour)`
5. **ADF** marks the run succeeded/failed; Azure Monitor alerts on failure.

## v1 → v2 mapping

For reviewers: every v1 file has a clear counterpart in v2.

| v1 | v2 |
|---|---|
| `dags/weather_pipeline_dag.py` | `adf/pipeline/weather_hourly_pipeline.json` |
| `src/extract.py` | `databricks/notebooks/01_bronze_ingest.py` + `databricks/libs/weather_client.py` |
| `src/transform.py` | `databricks/notebooks/02_silver_cleanse.py` + `databricks/libs/schemas.py` |
| `src/load.py` (raw + summary) | `databricks/notebooks/03_gold_aggregate.py` (Bronze write is in NB1) |
| `sql/init.sql` | `databricks/libs/schemas.py` (Spark `StructType` for Silver + Gold) |
| `.env` (Weatherstack key) | Azure Key Vault (read via `dbutils.secrets.get`) |
| `docker-compose.yaml` + `Dockerfile` | `bicep/main.bicep` |

The translation is **explicit, not shared**. There is no `utils.py` that
both versions import — by design. See
[`../docs/evolution.md`](../docs/evolution.md#the-mapping-v1-file--v2-equivalent)
for the rationale.

## What v2 deliberately does NOT include

A junior-friendly Azure design is one that stops adding complexity where
it isn't justified. The first cut is:

- ✅ ADF (managed orchestrator)
- ✅ Databricks (job clusters, no idle cost)
- ✅ ADLS Gen2 + Delta (Medallion)
- ✅ Key Vault (secrets)
- ✅ Bicep (one-click infra)
- ❌ Kafka / Event Hubs (hourly cadence is not real-time)
- ❌ Delta Live Tables (plain Delta + `MERGE` is enough for 3 notebooks)
- ❌ Terraform / multi-env CI/CD (one dev workspace is enough)
- ❌ Unity Catalog (workspace-local metastore is enough)
- ❌ Streaming Auto Loader (explicit PySpark writes are easier to reason about)

Each of these is a perfectly fine next step. None belongs in v1 of v2.

## Cost expectations (rough)

| Service | Per-hour pipeline run | Monthly (24 runs × 30 days = 720 runs) |
|---|---|---|
| ADF (1 activity chain) | < $0.01 | < $5 |
| Databricks job cluster (4 workers, ~10 min) | ~$0.40 | ~$300 |
| ADLS Gen2 (Bronze/Silver/Gold, a few GB) | < $0.01 | < $2 |
| Key Vault (1 secret) | < $0.01 | < $0.10 |
| **Total** | **~$0.50** | **~$300** |

These are estimates for the Standard tier, US East. Real cost depends on
cluster sizing, region, and tier. The biggest lever is **cluster size and
runtime** — keep the job cluster as small as possible and let Spark spill
to ADLS rather than over-provisioning memory.

## Getting started

1. Read [`deploy.md`](./deploy.md) for the end-to-end deploy guide.
2. Stand up the infra with `az deployment sub create ...` (Bicep).
3. Upload `config/cities.json` to the `config` container.
4. Import the ADF pipeline JSON into your data factory.
5. Import the three Databricks notebooks into your workspace.
6. Wire the three Notebook activities in the ADF pipeline.
7. Trigger the pipeline manually to verify the first end-to-end run.
8. Enable the schedule trigger.

## What to read next

- [`../docs/evolution.md`](../docs/evolution.md) — the *why* behind the migration.
- [`../docs/architecture/comparison.md`](../docs/architecture/comparison.md) — v1 vs v2 line by line.
- [`deploy.md`](./deploy.md) — the operational guide.

## Status

**Skeleton only.** The folder structure, READMEs, env templates, and
100-city config are in place. The actual Bicep templates, Databricks
notebooks, and ADF pipeline JSON are scaffolded as empty placeholder
files and will be filled in incremental commits on the
`feat/azure-medallion` branch.
