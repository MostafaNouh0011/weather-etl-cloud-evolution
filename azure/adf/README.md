# Azure Data Factory pipeline

ADF pipeline, linked services, datasets, and triggers for the v2
Azure-native pipeline.

## Status

🚧 **Placeholder.** The actual pipeline JSON is not yet written. It will
be added incrementally on the `feat/azure-medallion` branch.

## Layout

```
adf/
├── pipeline/
│   └── weather_hourly_pipeline.json   # The main pipeline
├── linkedService/
│   ├── ls_databricks.json             # Connection to the Databricks workspace
│   ├── ls_adls.json                   # Connection to ADLS Gen2
│   └── ls_keyvault.json               # Connection to Azure Key Vault
├── dataset/
│   └── ds_cities_config.json          # The cities.json blob reference
└── trigger/
    └── trg_hourly_schedule.json       # Hourly schedule trigger
```

## Why ADF and not Airflow

- **Managed.** No infra to run, upgrade, or monitor. Microsoft handles it.
- **Native to Azure.** AAD, Key Vault, Log Analytics, Monitor — all click-through.
- **Visual.** The pipeline JSON renders as a diagram in ADF Studio, which
  is easier to read for a junior than Airflow's Python DAG file.
- **Cost.** Pay per activity run; the hourly schedule costs cents per month.

The trade-off is that ADF's expression language (`@pipeline().parameters.run_date`)
is more verbose than Airflow's templating, and "real" code-as-pipeline is
harder. For a 3-stage ETL this is a non-issue.

## v1 → v2 mapping

| v1 | v2 |
|---|---|
| `dags/weather_pipeline_dag.py` | `pipeline/weather_hourly_pipeline.json` |
| `schedule_interval="0 */6 * * *"` | `trigger/trg_hourly_schedule.json` (hourly: `0 0 * * * *`) |
| `PythonOperator(task_id="extract")` | "Notebook" activity pointing at `01_bronze_ingest` |
| `PythonOperator(task_id="transform")` | "Notebook" activity pointing at `02_silver_cleanse` |
| `PythonOperator(task_id="load")` | "Notebook" activity pointing at `03_gold_aggregate` |
| `>>` (chained dependencies) | "Success" → "Execute" arrows in the JSON `activities` array |

## Reference

- [`../README.md`](../README.md) — architecture overview
- [`../deploy.md`](../deploy.md) — how to import this pipeline into ADF
