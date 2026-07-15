# Azure Databricks code

PySpark notebooks, jobs, and reusable Python libraries for the v2
Azure-native pipeline.

## Status

🚧 **Placeholder.** The notebooks and libraries are not yet written. They
will be added incrementally on the `feat/azure-medallion` branch.

## Layout

```
databricks/
├── notebooks/
│   ├── 01_bronze_ingest.py     # NB1: parallel API → Parquet in bronze/
│   ├── 02_silver_cleanse.py    # NB2: typed, deduped → Delta in silver/
│   └── 03_gold_aggregate.py    # NB3: hourly + daily → Delta in gold/
├── jobs/
│   └── weather_hourly_job.json # (Optional) Databricks Job definition
└── libs/
    ├── weather_client.py       # ThreadPoolExecutor-based Weatherstack client
    ├── schemas.py              # Explicit Spark StructType for Silver + Gold
    └── storage.py              # ADLS path helpers + dbutils.secrets wrappers
```

## Why 3 notebooks and not 1

Each notebook corresponds to a logical stage in the Medallion
architecture. ADF orchestrates them as separate activities so:

- Failures are localised to one stage
- Re-running a single stage is safe (every stage is idempotent via Delta `MERGE`)
- Cluster spin-up amortises across the whole pipeline, but each stage
  can be developed and unit-tested in isolation

## Notebooks → v1 mapping

| Notebook | v1 counterpart |
|---|---|
| `01_bronze_ingest.py` | `src/extract.py` (writes to ADLS instead of in-memory list) |
| `02_silver_cleanse.py` | `src/transform.py` (Spark DataFrame + explicit schema) |
| `03_gold_aggregate.py` | `src/load.py` (`MERGE` on Gold tables instead of Postgres upsert) |

The cleansing rules (cast, trim, drop nulls) are preserved 1-to-1.

## Reference

- [`../README.md`](../README.md) — architecture overview
- [`../deploy.md`](../deploy.md) — how to import these notebooks
- [`../../../docs/evolution.md`](../../../docs/evolution.md) — design rationale
