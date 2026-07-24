# Weather ETL Pipeline

A portfolio project that follows an ETL pipeline's lifecycle — from a local
Dockerized MVP to a production-grade Azure-native system.

> **Started** as a local Airflow + PostgreSQL pipeline (10 cities, every 6 hours).
> **Migrated** to Azure (ADF + Databricks + ADLS Gen2 + Delta Lake) to scale to
> 100 cities on an hourly cadence, with cloud-native storage and
> production-grade orchestration.

The two versions live side-by-side in this repo on purpose — see
[`docs/evolution.md`](./docs/evolution.md) for the full migration story, the
*why* behind each design choice, and what was deliberately left out.

---

## Versions

| Version | Stack | Cities | Cadence | Folder | Status |
|---|---|---|---|---|---|
| **v1 — Local** | Docker · Airflow · PostgreSQL · Pandas | 10 | every 6h | root of repo | ✅ Frozen at `v1.0.0` |
| **v2 — Azure** | ADF · Databricks (PySpark) · ADLS Gen2 · Delta · Key Vault | 100 | hourly | [`azure/`](./azure/README.md) | ✅ Deployable via Bicep |

| "I just want to…" | Go to |
|---|---|
| Run the local pipeline with one command | [v1 quick start](#v1--local-quick-start) below |
| See the Azure architecture & deploy steps | [`azure/README.md`](./azure/README.md) |
| Understand *why* the migration happened | [`docs/evolution.md`](./docs/evolution.md) |
| See the v1 → v2 design comparison | [`docs/architecture/comparison.md`](./docs/architecture/comparison.md) |
| Look at the city list | [`shared/cities.json`](./shared/cities.json) |

---

## v1 — Local (Docker + Airflow + PostgreSQL)

A fully self-contained local pipeline. No cloud account, no surprises.

### Architecture

```
Weatherstack API → Extract → Transform → Load → PostgreSQL
     ↓              ↓         ↓         ↓         ↓
  HTTP/JSON     Raw JSON  Clean Data  Insert   Raw & Analytics Tables
```

- **Extract** — fetches current weather for 10 cities from the Weatherstack API.
- **Transform** — parses and cleans the raw JSON into a structured format with Pandas.
- **Load** — inserts into PostgreSQL tables via SQLAlchemy.
- **Orchestration** — Apache Airflow schedules the pipeline every 6 hours.

All components run in Docker — no local Python, Postgres, or Airflow install needed.

### Tech Stack

- **Python 3.11** — ETL logic
- **Weatherstack API** — real-time weather data
- **PostgreSQL 13** — raw and analytics layers
- **Apache Airflow 2.10.3** — orchestration
- **Docker & Docker Compose** — containerization
- **SQLAlchemy** — DB connectivity
- **Pandas** — data processing

### Project Structure (v1)

```
weather_etl_pipeline/
├── dags/
│   └── weather_pipeline_dag.py    # Airflow DAG defining the ETL tasks
├── src/
│   ├── extract.py                 # Fetches data from Weatherstack API
│   ├── transform.py               # Cleans and transforms the data
│   └── load.py                    # Loads data into PostgreSQL
├── sql/
│   └── init.sql                   # Creates database tables on startup
├── logs/                          # Airflow logs (generated automatically)
├── plugins/                       # Airflow plugins (for extensions)
├── .env                           # Environment variables (configure API keys, etc.)
├── docker-compose.yaml            # Defines Docker services
├── Dockerfile                     # Builds the custom Airflow image
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running.
- A free API key from [Weatherstack](https://weatherstack.com) (sign up for a free account).

That's all — no need to install Python, PostgreSQL, or Airflow locally.

### Quick Start

#### 1. Clone the repository

```bash
git clone https://github.com/MostafaNouh0011/weather-etl-pipeline.git
cd weather-etl-pipeline
```

#### 2. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and replace `your_weatherstack_api_key_here` with your real
Weatherstack API key. The other values are pre-configured for the local
Docker setup.

#### 3. Build and start the containers

```bash
docker compose build
docker compose up -d
```

This starts 4 services: Airflow webserver, scheduler, and two PostgreSQL
databases (one for Airflow metadata, one for your data).

#### 4. Wait for initialization

```bash
docker compose logs -f airflow-init
```

Wait until you see "Admin user created" and the version output, then `Ctrl+C`.

#### 5. Access the Airflow UI

Open [http://localhost:8080](http://localhost:8080) in your browser.

- **Username:** `admin`
- **Password:** `admin`

Find the `weather_etl_pipeline` DAG, toggle it **ON**, and click ▶ to trigger
a manual run.

#### 6. Check the data

Connect to the weather database (exposed on port `5433`):

```bash
docker compose exec weather_postgres psql -U weather_user -d weather_db
```

```sql
-- View recent raw data
SELECT city, temperature_c, ingested_at
FROM raw.weather_raw
ORDER BY ingested_at DESC
LIMIT 5;
```
![Raw Output](docs/raw_output.png)

```sql
-- View summary data
SELECT city, country, record_date, avg_temperature_c, max_temperature_c, min_temperature_c, avg_humidity, avg_wind_speed_kmh, dominant_condition, total_records, last_updated
FROM analytics.weather_summary
ORDER BY record_date DESC;
```
![Summary Output](docs/analytics_output.png)

### v1 DAG details

The pipeline runs three tasks in sequence:

- **extract** — calls the API for 10 cities and stores raw responses.
- **transform** — parses and cleans the data into a usable format.
- **load** — saves data to PostgreSQL and updates the daily summary.

| Setting | Value |
|---|---|
| Schedule | `0 */6 * * *` (every 6 hours) |
| Retries | 2 attempts, 5 min delay |
| Catchup | Disabled (only future runs) |
| Executor | `LocalExecutor` |

### v1 Database Schema

The pipeline uses two schemas inside `weather_db`.

#### Raw Layer — `raw.weather_raw` (Bronze)

Stores every API response exactly as received, one row per city per pipeline run.

| Column | Type | Description |
|---|---|---|
| `id` | SERIAL PK | Auto-increment primary key |
| `city` | VARCHAR | City name |
| `country` | VARCHAR | Country name |
| `temperature_c` | FLOAT | Temperature in Celsius |
| `feels_like_c` | FLOAT | Feels-like temperature |
| `humidity` | INT | Humidity percentage |
| `wind_speed_kmh` | FLOAT | Wind speed in km/h |
| `wind_direction` | VARCHAR | Wind direction (N, NE, SW, etc.) |
| `weather_description` | VARCHAR | Human-readable condition |
| `uv_index` | INT | UV index |
| `pressure` | INT | Atmospheric pressure |
| `is_day` | BOOLEAN | Whether it's daytime |
| `ingested_at` | TIMESTAMP | When the row was inserted |

#### Analytics Layer — `analytics.weather_summary` (Gold)

Daily aggregated weather per city. Refreshed on every pipeline run using `ON CONFLICT` upsert.

| Column | Type | Description |
|---|---|---|
| `city` | VARCHAR | City name |
| `record_date` | DATE | Date of aggregation |
| `avg_temperature_c` | FLOAT | Daily average temperature |
| `max_temperature_c` | FLOAT | Daily high |
| `min_temperature_c` | FLOAT | Daily low |
| `avg_humidity` | FLOAT | Daily average humidity |
| `avg_wind_speed_kmh` | FLOAT | Daily average wind speed |
| `dominant_condition` | VARCHAR | Most frequent weather condition |
| `total_records` | INT | Number of raw records aggregated |

### Makefile Commands

```bash
make build    # Build Docker images
make up       # Start all containers in background
make down     # Stop all containers (data preserved)
make reset    # Stop all containers and delete all data
make logs     # Stream scheduler logs
make psql     # Open PostgreSQL shell in weather DB
```

### Cities Tracked (v1)

Cairo · Alexandria · Dubai · London · New York · Paris · Tokyo · Sydney · Berlin · Toronto

The full list lives in [`shared/cities.json`](./shared/cities.json).
v2 expands this to 100 cities on an hourly cadence.

### Environment Variables (v1)

| Variable | Description |
|---|---|
| `WEATHERSTACK_API_KEY` | Your Weatherstack API key |
| `WEATHER_DB_HOST` | PostgreSQL host (use `weather_postgres` inside Docker) |
| `WEATHER_DB_PORT` | PostgreSQL port (default: `5432`) |
| `WEATHER_DB_NAME` | Database name |
| `WEATHER_DB_USER` | Database user |
| `WEATHER_DB_PASSWORD` | Database password |
| `AIRFLOW__CORE__FERNET_KEY` | Encryption key for Airflow secrets |
| `AIRFLOW_ADMIN_USERNAME` | Airflow UI username |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow UI password |

### Troubleshooting (v1)

- **Containers won't start** — ensure Docker is running and ports `8080` and `5433` are free.
- **API errors** — check your Weatherstack API key in `.env`.
- **DAG fails** — view logs in the Airflow UI or check the `logs/` folder.
- **Database connection issues** — verify Postgres is healthy with `docker compose ps`.

For more help, check the [Airflow documentation](https://airflow.apache.org/docs/)
or open an issue on GitHub.

---

## v2 — Azure (ADF + Databricks + ADLS Gen2)

The same ETL problem, scaled to 100 cities on an hourly cadence, designed as
a cloud-native Medallion architecture.

👉 **See [`azure/README.md`](./azure/README.md) for the full Azure setup,
architecture, and deploy guide.**

Quick architectural summary:

```
Weatherstack API
       │  HTTP (parallel, 10 workers)
       ▼
┌──────────────────────────┐
│ Azure Data Factory       │   hourly schedule trigger
│   • 3 Notebook activities│   (Bronze → Silver → Gold)
│   • retries, alerts      │
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│ Azure Databricks         │   job cluster per run, no idle cost
│  NB1 Bronze  → NB2 Silver│   PySpark, parallel API calls
│  → NB3 Gold             │   Delta Lake on ADLS Gen2
└──────────┬───────────────┘
           ▼
┌──────────────────────────┐
│ ADLS Gen2                │
│  bronze/  silver/  gold/ │   Delta on Silver + Gold
└──────────────────────────┘
```

**Why this shape:** ADF for native orchestration, Databricks for distributed
compute, Delta for schema evolution + time travel + `MERGE`, and the
Medallion pattern as the universal data-engineering lingua franca.

---

## Why both versions exist

The two versions are not redundant — they prove two different competencies:

- **v1** shows you can build a working ETL pipeline from scratch with
  fundamentals: Docker, Airflow, Postgres, ETL design, secrets management.
- **v2** shows you can take that same problem and re-design it for the
  cloud: distributed compute, decoupled storage, native orchestration,
  schema evolution, and cost discipline.

The deliberate choice to keep v1 untouched (no shared library between the
two) makes the *contrast* between the architectures visible. See
[`docs/evolution.md`](./docs/evolution.md) for the detailed migration
rationale and lessons learned.

---

## License

MIT License — free to use, modify, and share.
