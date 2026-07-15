# From v1 to v2 — The Migration Story

> Why this project started with Airflow + PostgreSQL, and why it grew into
> Azure Data Factory + Databricks + ADLS Gen2.

This document is the answer to the question every senior engineer will ask
when they open the repo: *"Why two versions?"*

---

## TL;DR

| | v1 — Local | v2 — Azure |
|---|---|---|
| **Problem** | "I need a working weather ETL." | "I need to scale it 10× without rewriting it." |
| **Stack** | Docker, Airflow, Postgres, Pandas | ADF, Databricks, ADLS Gen2, Delta, Key Vault |
| **Cities** | 10 | 100 |
| **Cadence** | Every 6 hours | Every hour |
| **Records/day** | ~40 | ~2,400 |
| **Compute** | Single node | Distributed (Spark) |
| **Storage** | Postgres (row store) | ADLS Gen2 + Delta (columnar, partitioned) |
| **Orchestration** | Airflow in Docker | ADF (managed) |
| **Cost model** | Free (local) | Pay-per-job (job clusters) |
| **Status** | Frozen at `v1.0.0` | Deployable via Bicep |

The two versions are not a clean break — they are a *translation*. The
3-stage shape (`extract → transform → load`) and the
`raw → analytics` layer split survived intact. Only the *runtime* changed.

---

## What v1 Did Well

Before explaining what v1 couldn't do, credit where it's due. v1 was the
right place to start, and every choice had a reason:

- **Docker Compose for everything.** No "works on my machine." A reviewer
  with Docker installed can run the whole pipeline in two commands. That's
  invaluable for a portfolio project.
- **Two Postgres databases.** Airflow's metadata DB stays separate from
  the project's data DB. This is a production pattern introduced in the
  smallest possible scope — a habit worth keeping.
- **Pandas + SQLAlchemy + raw SQL aggregation.** No ORMs, no magic. The
  reader can follow the data from API JSON → DataFrame → `raw` table →
  aggregate query → `analytics` table in a single sitting.
- **A `sql/init.sql` bootstrap.** The schema is documented *in the database
  itself*, not only in the README. Tables appear in a specific order with
  comments. This is the kind of thing a senior notices.
- **`.env`-driven configuration.** No secrets in code, no hardcoded hosts.
- **`ON CONFLICT ... DO UPDATE` for the daily summary.** Idempotent,
  correct, and re-runnable — a property that v2 inherits through Delta
  `MERGE`.

In other words: **v1 demonstrates the fundamentals correctly.** Removing
or skipping it would be a mistake.

---

## Where v1 Started to Hurt

Then the requirements changed:

1. **From 10 cities to 100.** A 10× increase in cities.
2. **From every 6 hours to every hour.** A 6× increase in cadence.
3. **Together: ~60× more records per day.**

Suddenly the things that were "fine" became painful:

### Pain 1 — The API loop is sequential

`extract.py` calls the Weatherstack API for one city at a time:

```python
results = [fetch_weather(city) for city in CITIES]   # 1-by-1
```

With 10 cities, that's a few seconds. With 100 cities, it's a full minute
of wall-clock time per run, blocking the entire pipeline. Parallelising
this on a single machine is possible with `ThreadPoolExecutor`, but you
start bumping into GIL territory, memory pressure, and Airflow worker
limits.

### Pain 2 — Pandas doesn't distribute

`transform.py` loads every API response into a single Pandas DataFrame.
This works up to a point, but as the dataset grows you start considering
Dask, chunking, or moving to a real distributed engine. At that point,
fighting Pandas is harder than switching engines.

### Pain 3 — The Postgres upsert is doing too much

The daily summary is rebuilt by a single SQL statement that scans the
entire `raw.weather_raw` table on every run:

```sql
SELECT city, country, DATE(ingested_at), AVG(temperature_c), ...
FROM raw.weather_raw
GROUP BY city, country, DATE(ingested_at)
ON CONFLICT (city, record_date) DO UPDATE SET ...
```

At ~40 records/day this is instantaneous. At 2,400 records/day it's still
fine — but the pattern doesn't scale to 100,000. The full table is
re-aggregated every run, with no windowing or partition pruning. And
Postgres isn't really designed as an analytical store; it doesn't have
the columnar optimizations that make aggregations cheap.

### Pain 4 — Airflow is great, but Airflow-as-a-service is what you want

Running Airflow in Docker locally is wonderful for development. Running
Airflow in production is a different conversation:

- A managed Postgres for the metadata DB.
- A separate executor (Celery, Kubernetes, CeleryKubernetes).
- Workers, queues, scaling policies.
- Upgrades that touch every component.
- Monitoring beyond what the local stack provides.

For a junior-to-mid project, **the orchestrator should be a managed
service, not a self-hosted one.** That service is Azure Data Factory.

### Pain 5 — No schema evolution, no time travel

If the Weatherstack API adds a new field tomorrow, the Postgres
`weather_raw` table either needs an `ALTER TABLE` or it silently drops
the new data. There's no record of what the data looked like last week.
"Time travel" isn't a Postgres concept; it's a Delta Lake concept, and
it's one of the biggest reasons Delta exists.

### Pain 6 — Secrets live in `.env`

A `.env` file works locally. In production you need:

- Centralised secret rotation
- Access auditing
- No secrets in notebooks or in git history (ever)

That's a managed secret store: Azure Key Vault.

---

## What v2 Fixes

Every v1 pain point maps to a v2 design choice:

| v1 pain | v2 fix |
|---|---|
| Sequential API loop | `ThreadPoolExecutor` (10 workers) inside a Databricks job — easy to scale to `mapPartitions` later if needed |
| Pandas can't distribute | PySpark on a Databricks **job cluster** (auto-terminates after the run) |
| Full-table aggregate upsert in Postgres | Delta `MERGE` on partitioned Silver/Gold tables; only the current hour/day is re-aggregated |
| Self-hosted Airflow | Azure Data Factory — managed, with built-in triggers, retries, and alerts |
| No schema evolution | Delta Lake's schema-enforcement + schema-evolution features |
| No time travel | Delta's transaction log + `DESCRIBE HISTORY` / `RESTORE` |
| `.env` secrets | Azure Key Vault, accessed via `dbutils.secrets.get` |

And one architectural pattern that wasn't in v1 at all, but is the spine
of v2: **the Medallion architecture** (Bronze → Silver → Gold on ADLS
Gen2). It separates concerns in a way that makes the pipeline
self-documenting:

- **Bronze** = "what did the API actually say?" Append-only, raw.
- **Silver** = "what does clean, typed, deduplicated data look like?" Conformed.
- **Gold** = "what does the business actually consume?" Aggregated, business-shaped.

This is the same split v1 had (`raw.*` + `analytics.*`), just moved to
columnar storage with proper partitioning.

---

## What v2 Deliberately Does NOT Include

This is as important as what v2 *does* include. A junior-friendly Azure
design is one that **stops adding complexity where it isn't justified**:

- ❌ **Kafka / Event Hubs.** The cadence is hourly, not real-time. Adding
  a message bus is a stream-processing decision; this is a batch
  decision.
- ❌ **Delta Live Tables (DLT).** DLT is great, but it's a second
  declarative framework on top of PySpark. Plain Delta + `MERGE` is
  enough for three notebooks.
- ❌ **CI/CD with Terraform + multi-env promotion.** A single dev
  workspace is enough. The infra lives in Bicep and is one `az
  deployment` away from being re-created.
- ❌ **Unity Catalog.** Useful in a multi-team enterprise; overkill for a
  one-engineer portfolio project. A workspace-local Hive metastore is
  enough.
- ❌ **Streaming Auto Loader.** Bronze is written by an explicit PySpark
  job, not an Auto Loader stream. You keep the data-flow explicit and
  easy to reason about.

Each of these is a perfectly fine next step *after* v2 lands. None of
them belongs in the first cut.

---

## The Mapping (v1 file → v2 equivalent)

A reviewer asking "where did the old code go?" should be able to trace
every line. Here is the explicit mapping:

| v1 file | v2 equivalent | Notes |
|---|---|---|
| `dags/weather_pipeline_dag.py` | `azure/adf/pipeline/weather_hourly_pipeline.json` | ADF trigger + 3 Notebook activities, replacing the Airflow DAG |
| `src/extract.py` (`fetch_weather`, `extract_all_cities`) | `azure/databricks/notebooks/01_bronze_ingest.py` + `azure/databricks/libs/weather_client.py` | Same HTTP call, but parallelised; results land in Bronze as Parquet |
| `src/transform.py` (`parse_response`, `transform`) | `azure/databricks/notebooks/02_silver_cleanse.py` | Same parsing rules, expressed as a Spark UDF with an explicit schema |
| `src/load.py` (`load_raw`, `load_summary`) | `azure/databricks/notebooks/03_gold_aggregate.py` | `load_raw` becomes an append into Bronze (in NB1); `load_summary` becomes a Delta `MERGE` on Gold (in NB3) |
| `sql/init.sql` | `azure/databricks/libs/schemas.py` (Silver & Gold) + Bronze's implicit Parquet schema | The Postgres DDL is replaced by explicit Spark `StructType` schemas for Silver and Gold; Bronze is schemaless Parquet |
| `.env` (Weatherstack key) | Azure Key Vault secret, read via `dbutils.secrets.get` | No secrets in notebooks, no secrets in `.env` checked into the repo |
| `docker-compose.yaml` + `Dockerfile` | `azure/bicep/main.bicep` | Bicep creates the resource group, ADLS, Databricks workspace, ADF, Key Vault |

If a reviewer diffs `extract.py` against `01_bronze_ingest.py`, they
should see the same logic, expressed in a different engine. No magic.
No shared "ETL framework" hiding the translation.

---

## Lessons Learned (and Re-Applied)

A few things v1 taught that shaped v2's design:

1. **Idempotency wins.** v1's `ON CONFLICT ... DO UPDATE` made reruns
   safe. v2 inherits this through Delta `MERGE`. Every write is
   idempotent by construction.
2. **Logs in one place.** v1's `logs/` folder is convenient locally.
   v2's Databricks cluster logs go to Log Analytics, where they sit
   next to ADF run logs in a single workspace.
3. **Cities as data, not as code.** v1 had `CITIES = [...]` baked into
   `extract.py`. v2 reads `shared/cities.json` (100 cities) from ADLS.
   Adding a city no longer requires a code change or a deploy.
4. **Configuration as data, not as code.** The same idea applies to
   anything that varies per run: window length, retry counts, etc.
   v2 reads from a small `config/` folder in ADLS.
5. **One orchestrator, one source of truth.** Mixing Airflow and ADF
   would have meant two UIs, two failure modes, and two
   permission models. v2 uses ADF end-to-end.

---

## How to Evaluate This Project

A short guide for reviewers:

- **If you have 5 minutes:** skim the top-level `README.md`. The
  comparison table and the "Why both versions exist" section are the
  elevator pitch.
- **If you have 30 minutes:** read `docs/evolution.md` (this file) and
  the v1 quick start. Run v1 if you have Docker.
- **If you have an hour:** read `azure/README.md` and skim
  `azure/bicep/main.bicep` and the three Databricks notebooks. The
  mapping table above is your index.
- **If you want to push back:** the design decisions live in
  `docs/architecture/comparison.md`. That's where to argue.

---

## Open Questions for the Future

Things this design intentionally defers:

- **Backfill.** What happens if Databricks is down for 3 hours? v1
  Airflow's `catchup=False` would skip them. v2's first cut has the
  same behaviour. A "process the last N missed hours" ADF activity is
  a natural follow-up.
- **Schema drift detection.** If Weatherstack adds a field, Silver
  silently drops it (with a warning). A dedicated schema-drift
  notebook + ADF alert is a clean v2.1 addition.
- **Cost dashboards.** Databricks job cluster cost attribution is
  doable but not in v1 of v2.
- **Unit tests for PySpark.** v1 has none. v2 follows suit (out of
  scope for the first cut). A `pytest` + `chispa` setup is a natural
  follow-up.
- **Observability beyond logs.** v2 stops at Log Analytics. A
  Databricks SQL dashboard on top of Gold (e.g. "last 24h of API
  failures by city") is a one-week follow-up.

Each of these is a deliberate *not-now* — and the project is healthier
for naming them explicitly.
