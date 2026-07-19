# Azure Data Factory pipeline

ADF pipeline, linked services, datasets, and triggers for the v2
Azure-native pipeline. The structure mirrors v1's
`dags/weather_pipeline_dag.py` — one pipeline, three chained stages,
one hourly schedule — but in ADF JSON instead of Airflow Python.

## Status

✅ **Scaffolded.** Pipeline + 3 linked services + 1 dataset + 1
schedule trigger. The JSONs are ready to be imported into an ADF
factory (UI import or `az datafactory pipeline create` / `az datafactory
trigger create`).

## Layout

```
adf/
├── pipeline/
│   └── weather_hourly_pipeline.json   # Three Notebook activities, in sequence
├── linkedService/
│   ├── ls_databricks.json             # Databricks workspace + SP auth via Key Vault
│   ├── ls_adls.json                   # ADLS Gen2 (managed-identity auth)
│   └── ls_keyvault.json               # Key Vault (managed-identity auth)
├── dataset/
│   └── ds_cities_config.json          # Reference to cities.json in the config container
└── trigger/
    └── trg_hourly_schedule.json       # Top-of-the-hour schedule, UTC
```

## How the pieces fit

```
        ┌──────────── trg_hourly_schedule ────────────┐
        │  cron: every hour, top of hour (UTC)         │
        │  references: weather_hourly_pipeline         │
        └────────────────────┬────────────────────────┘
                             │ fires
                             ▼
        ┌──────────── weather_hourly_pipeline ──────────────┐
        │  variables: run_year / month / day / hour         │
        │   = @formatDateTime(utcNow(), ...)                │
        │  (captured ONCE so all activities see the same    │
        │   hour even if the run crosses an hour boundary)  │
        │                                                   │
        │  nb_bronze_ingest                                 │
        │     ↓                                             │
        │  nb_silver_cleanse (run_year=.., run_hour=..)     │
        │     ↓                                             │
        │  nb_gold_aggregate (run_year=.., run_hour=..)    │
        └───────────────────────────────────────────────────┘
```

## Widget contract per notebook

Each Notebook activity in the pipeline passes a fixed set of
`baseParameters` to the notebook. The notebooks read these via
`dbutils.widgets.get(...)` (Databricks) or fall back to env vars
(developer laptop).

| Widget | Bronze | Silver | Gold | Default |
|---|---|---|---|---|
| `storage_account` | ✓ | ✓ | ✓ | `<STORAGE_ACCOUNT_NAME>` (set by deploy) |
| `scope` | ✓ | — | — | `kv-scope` |
| `api_key_name` | ✓ | — | — | `weatherstack-api-key` |
| `env_fallback` | ✓ | — | — | `WEATHERSTACK_API_KEY` |
| `max_workers` | ✓ | — | — | `10` |
| `run_year` | — | ✓ | ✓ | `@variables('run_year')` (utcNow at start of run) |
| `run_month` | — | ✓ | ✓ | `@variables('run_month')` |
| `run_day` | — | ✓ | ✓ | `@variables('run_day')` |
| `run_hour` | — | ✓ | ✓ | `@variables('run_hour')` |

If the widget is missing in the pipeline, the notebook falls back to
`os.environ.get(...)` (also a string), then to the widget default
`datetime.utcnow()`. The triple fallback means a developer can run
the notebook in a Databricks job that doesn't pass widgets, a CI
runner that doesn't pass env vars, or a fresh laptop with no
configuration — and the notebook still does the right thing.

## Why the placeholders

`weather_hourly_pipeline.json` has `<STORAGE_ACCOUNT_NAME>` as a
literal placeholder. **Before importing, run the substitution from
the Bicep deployment output** (see `deploy.md` step 6):

```bash
STORAGE=$(az deployment group show \
  --resource-group rg-weather-de-dev \
  --name <deployment-name> \
  --query properties.outputs.storageAccountName.value -o tsv)

sed -i "s/<STORAGE_ACCOUNT_NAME>/$STORAGE/g" \
  azure/adf/pipeline/weather_hourly_pipeline.json
```

The same goes for the linked services:

| JSON | Placeholders to substitute |
|---|---|
| `ls_databricks.json` | `<DATABRICKS_WORKSPACE_URL>`, `<DATABRICKS_WORKSPACE_RESOURCE_ID>`, optionally `<JOB_CLUSTER_POOL_NAME_OPTIONAL>` |
| `ls_adls.json` | `<STORAGE_ACCOUNT_NAME>` |
| `ls_keyvault.json` | `<KEY_VAULT_NAME>` |

The pipeline imports *clean* JSON — ADF Studio does not understand
`<…>` placeholders, so the substitution is a deploy step, not an
import step.

## How to import

**Option A — UI (recommended for first import):**

1. In ADF Studio, open **Author → Linked services → + New**.
2. For each `ls_*.json`, click **Import** and upload the file.
3. Fill in the placeholders manually (the Bicep deployment output has
   all four values).
4. Import `ds_cities_config.json` under **Datasets**.
5. Import `weather_hourly_pipeline.json` under **Pipelines**.
6. Import `trg_hourly_schedule.json` under **Triggers**; set
   **runtimeState** to `Started` when you want it to fire.

**Option B — CLI:**

```bash
DATA_FACTORY=<factory-name>
RESOURCE_GROUP=rg-weather-de-dev

# Linked services
az datafactory linked-service create \
  --resource-group $RESOURCE_GROUP \
  --factory-name $DATA_FACTORY \
  --name ls_databricks \
  --properties @azure/adf/linkedService/ls_databricks.json

# (… same pattern for ls_adls, ls_keyvault, ds_cities_config,
#      weather_hourly_pipeline, trg_hourly_schedule …)

# Start the trigger
az datafactory trigger start \
  --resource-group $RESOURCE_GROUP \
  --factory-name $DATA_FACTORY \
  --name trg_hourly_schedule
```

The CLI command's `--properties` argument expects the **inner
`properties` object** of the JSON, not the full file. The deploy.md
"Common issues" table covers the typical CLI pitfalls.

## Auth model (locked in)

The Databricks linked service uses a **service principal** for auth.
The SP is provisioned by the Bicep template
(`modules/serviceprincipal.bicep`); its client secret is stored in
Key Vault at deploy time. The linked service reads the secret via
`ls_keyvault.json` using the standard "AzureKeyVaultSecret" pattern
— the secret value never appears in any ADF UI surface.

The SP gets no Azure role assignments; it only needs the Databricks
workspace's "AAD integration" permission, which is configured
out-of-band (see deploy.md step 4).

## What is NOT here

- **A monitoring / alerting pipeline.** Activity failures surface in
  the shared Log Analytics workspace (set up by the Bicep), but
  alerting rules (email on failure, retry policy) are intentionally
  not in this commit. They're a v2.1 concern that needs an
  Action Group + an Azure Monitor alert, both of which are noisy
  for a portfolio project.
- **A backfill / manual-run pipeline.** Re-running a single hour is
  possible by tweaking the four `run_*` variables in a pipeline
  copy, but the dedicated backfill pipeline (with a per-hour
  foreach loop) is out of scope.
- **A web activity to check the Weatherstack quota before Bronze
  runs.** The free tier has a 1000-call/month cap; one full pipeline
  run uses 100 calls. Worth adding as a guard rail at the v2.1 mark.
- **A "Set Variable" activity at the start of the pipeline** to
  pre-compute the four `run_*` values. The current approach uses
  ADF pipeline **variables** with `defaultValue` set to a
  `@formatDateTime(utcNow(), ...)` expression — the expression
  evaluates once when the pipeline starts, so all three activities
  see the same values. A `Set Variable` activity is more explicit
  but adds noise; the variable approach is fine for v1 of v2.

## Reference

- [`../README.md`](../README.md) — architecture overview
- [`../deploy.md`](../deploy.md) — how to import this pipeline into ADF
- [`../databricks/README.md`](../databricks/README.md) — the notebooks this pipeline calls
