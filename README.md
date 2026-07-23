# Weather ETL Pipeline

> A small ETL project that grew up. **v1** runs locally with Docker, Airflow
> and PostgreSQL. **v2** runs in Azure with Data Factory, Databricks and
> ADLS Gen2. Both live in this repo, side by side, on purpose.

---

## TL;DR

- **What:** Hourly weather data for 10 (v1) or 100 (v2) cities, via the
  Weatherstack API.
- **Why two versions:** v1 shows you can build a clean local pipeline
  (Docker, Airflow, ETL fundamentals). v2 shows you can re-architect the
  same problem for the cloud (managed services, distributed compute,
  Delta Lake).
- **Pick your entry point:**

  | I want to… | Go to |
  |---|---|
  | Run the local pipeline with one command | [v1 quick start](#v1--local-quick-start) |
  | Stand up the Azure version | [`azure/README.md`](./azure/README.md) → [`azure/deploy.md`](./azure/deploy.md) |
  | Understand *why* the migration happened | [`docs/evolution.md`](./docs/evolution.md) |
  | Compare v1 vs v2 stage by stage | [`docs/architecture/comparison.md`](./docs/architecture/comparison.md) |
  | See the repo map | [`ARCHITECTURE.md`](./ARCHITECTURE.md) |
  | Look at the 100-city list | [`shared/cities.json`](./shared/cities.json) |

---

## Versions at a glance

| | v1 — Local | v2 — Azure |
|---|---|---|
| **Stack** | Docker · Airflow · PostgreSQL · Pandas | ADF · Databricks (PySpark) · ADLS Gen2 · Delta |
| **Cities / cadence** | 10 · every 6h | 100 · every hour |
| **Where the code lives** | repo root | [`azure/`](./azure/README.md) |
| **Status** | Frozen at tag `v1.0.0` | Deployable via Bicep |
| **How to run** | `make up` → open Airflow | `az deployment group create` → import JSONs |

---

## v1 — Local (Docker + Airflow + PostgreSQL)

A self-contained local pipeline. No cloud account, no surprises.

### What it does

```
Weatherstack API → extract → transform → load → PostgreSQL
     ↓              ↓         ↓         ↓         ↓
  HTTP/JSON     Raw JSON  Clean Data  Insert   raw + analytics tables
```

- **Extract** — fetches current weather for 10 cities from Weatherstack.
- **Transform** — parses + cleans the JSON into a Pandas DataFrame.
- **Load** — inserts into PostgreSQL via SQLAlchemy; refreshes a daily
  summary with `ON CONFLICT … DO UPDATE`.
- **Orchestration** — Apache Airflow runs the three steps every 6 hours.

Everything runs in Docker. No local Python, Postgres, or Airflow needed.

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) running.
- A free [Weatherstack API key](https://weatherstack.com).

### Quick start

```bash
# 1. Clone
git clone https://github.com/MostafaNouh0011/weather-etl-pipeline.git
cd weather-etl-pipeline

# 2. Configure
cp .env.example .env
# …open .env and set WEATHERSTACK_API_KEY=…

# 3. Build + start
make build
make up

# 4. Wait for init
docker compose logs -f airflow-init
#   …wait for "Admin user created" then Ctrl+C

# 5. Open the UI
#   http://localhost:8080   (admin / admin)
#   Toggle the weather_etl_pipeline DAG → click ▶

# 6. Query the data
make psql
```

```sql
-- recent raw rows
SELECT city, temperature_c, humidity, ingested_at
FROM raw.weather_raw
ORDER BY ingested_at DESC LIMIT 5;

-- daily summary
SELECT city, record_date, avg_temperature_c, total_records
FROM analytics.weather_summary
ORDER BY record_date DESC;
```

### DAG

| Setting | Value |
|---|---|
| Schedule | `0 */6 * * *` (every 6 hours) |
| Retries | 2 attempts, 5 min delay |
| Catchup | Disabled |
| Executor | `LocalExecutor` |

### Database schema

Two schemas inside `weather_db`:

- `raw.weather_raw` (Bronze) — every API response, one row per city per run.
- `analytics.weather_summary` (Gold) — daily aggregates, refreshed every run
  via `ON CONFLICT … DO UPDATE`.

The full DDL is in [`sql/init.sql`](./sql/init.sql).

### Common commands

```bash
make build       # build the custom Airflow image
make up          # start all services in the background
make down        # stop services (data is preserved)
make reset       # stop and DELETE all data
make logs        # stream scheduler logs
make psql        # open psql inside the weather DB
make test        # run the v1 unit tests
make v2-test     # run the v2 (Azure) schema tests
```

### Cities (v1)

Cairo · Alexandria · Dubai · London · New York · Paris · Tokyo · Sydney ·
Berlin · Toronto

Full 100-city list (v2 source of truth) in
[`shared/cities.json`](./shared/cities.json).

### Troubleshooting (v1)

| Symptom | Fix |
|---|---|
| Containers won't start | Check ports `8080` and `5433` are free |
| API errors in extract | Verify `WEATHERSTACK_API_KEY` in `.env` |
| DAG fails | Check the Airflow UI logs or `logs/` |
| Postgres connection issues | `docker compose ps` to verify health |

---

## v2 — Azure (ADF + Databricks + ADLS Gen2)

Same ETL problem, scaled to 100 cities on an hourly cadence, designed as
a Medallion architecture (Bronze → Silver → Gold).

```
Weatherstack API
       │  HTTP (10 parallel workers)
       ▼
Azure Data Factory ── hourly trigger, 3 Notebook activities
       ▼
Azure Databricks (PySpark, auto-terminating job cluster)
  NB1 Bronze  →  NB2 Silver  →  NB3 Gold
       ▼
ADLS Gen2 (Parquet + Delta)
  bronze/  silver/  gold/   config/
```

- **Why ADF:** managed orchestrator, native triggers, retries, alerts.
- **Why Databricks:** distributed compute, job clusters that terminate
  after the run (no idle cost).
- **Why Delta:** schema evolution, time travel, idempotent `MERGE`.
- **Why Key Vault:** the API key never leaves the vault; notebooks read
  it via `dbutils.secrets.get`.

👉 **Full setup, deploy guide, and architecture: [`azure/README.md`](./azure/README.md)**
**Step-by-step deploy: [`azure/deploy.md`](./azure/deploy.md)**

---

## Why both versions exist

They prove two different things:

- **v1** — fundamentals: Docker, Airflow, Postgres, ETL design, idempotent
  writes, secrets in `.env`, SQL `ON CONFLICT` for upserts.
- **v2** — scale: distributed compute, decoupled storage, native
  orchestration, schema evolution, time travel, managed identities,
  cost discipline (job clusters, not all-purpose).

Keeping v1 untouched (no shared library between them) makes the
*contrast* between the two architectures visible. Full rationale in
[`docs/evolution.md`](./docs/evolution.md).

---

## Project layout

```
weather_etl_pipeline/
├── README.md                  ← you are here
├── ARCHITECTURE.md            ← single-source repo map
├── Makefile                   ← convenience commands
├── Dockerfile                 ← Airflow 2.10.3 image (v1)
├── docker-compose.yaml        ← 4-service local stack (v1)
├── requirements.txt           ← v1 Python deps
├── dags/                      ← Airflow DAG
├── src/                       ← extract / transform / load
├── sql/init.sql               ← Postgres schema
├── shared/cities.json         ← 100 cities (v1 uses first 10)
├── tests/                     ← v1 unit tests (stdlib unittest)
├── docs/
│   ├── evolution.md           ← v1 → v2 story
│   ├── architecture/
│   │   └── comparison.md      ← stage-by-stage v1 vs v2
│   └── *.png                  ← v1 sample outputs + DAG diagram
└── azure/                     ← v2 (Bicep + ADF + Databricks)
    ├── README.md              ← v2 architecture + overview
    ├── deploy.md              ← end-to-end deploy guide
    ├── bicep/                 ← infra as code
    ├── adf/                   ← ADF pipeline + linked services
    ├── databricks/
    │   ├── notebooks/         ← 01_bronze · 02_silver · 03_gold
    │   ├── libs/              ← weather_client · schemas · storage
    │   └── tests/             ← 14 unit tests
    └── config/cities.json     ← v2's copy of the city list
```

---

## License

MIT — free to use, modify, and share.
