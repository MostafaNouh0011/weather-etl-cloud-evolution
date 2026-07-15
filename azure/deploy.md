# Deploy Guide — Azure Weather ETL (v2)

> End-to-end setup for the Azure-native version. Read
> [`README.md`](./README.md) first for the architecture overview.

This guide assumes you have:

- An **Azure subscription** with contributor access to a resource group.
- The **Azure CLI** installed (`az --version`).
- A **Databricks workspace** accessible from your machine (or willingness to create one via Bicep).
- A **Weatherstack API key** from <https://weatherstack.com>.

It is written to be runnable top-to-bottom in a fresh terminal.

---

## 0. One-time local prerequisites

```bash
# Azure CLI
az --version

# Databricks CLI (for uploading notebooks later)
pip install databricks-cli
databricks --version

# Sign in
az login
az account set --subscription "<your-subscription-id-or-name>"

# Create the resource group
az group create --name rg-weather-de-dev --location eastus
```

## 1. Deploy infrastructure (Bicep)

The Bicep template (`bicep/main.bicep`) provisions:

- ADLS Gen2 storage account with 4 containers (`bronze`, `silver`, `gold`, `config`)
- Azure Key Vault with one secret placeholder for the Weatherstack key
- Azure Databricks workspace
- Azure Data Factory
- A service principal + role assignments for Databricks → ADLS access
- A Log Analytics workspace for cluster logs (optional)

```bash
cd bicep
az deployment group create \
  --resource-group rg-weather-de-dev \
  --template-file main.bicep \
  --parameters @parameters/dev.parameters.json
```

Capture the outputs — you'll need the storage account name, Databricks
workspace URL, and ADF name.

## 2. Store the Weatherstack API key in Key Vault

```bash
# Get the Key Vault name from the Bicep output
KV_NAME=$(az deployment group show \
  --resource-group rg-weather-de-dev \
  --name <deployment-name> \
  --query properties.outputs.keyVaultName.value -o tsv)

az keyvault secret set \
  --vault-name $KV_NAME \
  --name "weatherstack-api-key" \
  --value "your_weatherstack_api_key_here"
```

## 3. Upload cities.json to the `config` container

```bash
# Get the storage account name from the Bicep output
STORAGE=$(az deployment group show \
  --resource-group rg-weather-de-dev \
  --name <deployment-name> \
  --query properties.outputs.storageAccountName.value -o tsv)

az storage blob upload \
  --account-name $STORAGE \
  --container-name config \
  --name cities.json \
  --file ../config/cities.json \
  --auth-mode login
```

## 4. Create a Databricks secret scope backed by Key Vault

In the Databricks workspace UI:

1. Go to **Workspace → Secrets**.
2. Create a new secret scope: name = `kv-scope`, type = **Azure Key Vault-backed**.
3. Point it at the Key Vault created in step 1.

Or via the Databricks CLI:

```bash
databricks secrets create-scope kv-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "/subscriptions/<sub-id>/resourceGroups/rg-weather-de-dev/providers/Microsoft.KeyVault/vaults/$KV_NAME" \
  --profile <your-databricks-profile>
```

The notebooks will read the API key with:

```python
api_key = dbutils.secrets.get(scope="kv-scope", key="weatherstack-api-key")
```

## 5. Import the three Databricks notebooks

```bash
databricks workspace import \
  --language PYTHON \
  --format SOURCE \
  --path /Shared/weather/01_bronze_ingest \
  --file notebooks/01_bronze_ingest.py \
  --profile <your-databricks-profile>

databricks workspace import \
  --language PYTHON \
  --format SOURCE \
  --path /Shared/weather/02_silver_cleanse \
  --file notebooks/02_silver_cleanse.py \
  --profile <your-databricks-profile>

databricks workspace import \
  --language PYTHON \
  --format SOURCE \
  --path /Shared/weather/03_gold_aggregate \
  --file notebooks/03_gold_aggregate.py \
  --profile <your-databricks-profile>
```

## 6. Import the ADF pipeline

Two options:

**Option A — UI:** In ADF Studio, **Author → Pipeline → Import pipeline**
and upload `adf/pipeline/weather_hourly_pipeline.json`. Then re-create the
linked services (storage, Databricks, Key Vault) pointing at the
resources from step 1.

**Option B — CLI:**

```bash
# Requires the ADF data factory resource to be empty (or use a fresh factory).
# The JSON file format is what ADF Studio exports. Import commands vary
# by region / auth; the UI import is the most reliable first cut.
```

## 7. Wire up the linked services

In ADF Studio, create / verify three linked services:

| Linked service | Type | Targets |
|---|---|---|
| `ls_adls` | Azure Blob Storage (ADLS Gen2) | The storage account from step 1 |
| `ls_databricks` | Azure Databricks | The Databricks workspace URL + an access token (or AAD) |
| `ls_keyvault` | Azure Key Vault | The Key Vault from step 1 (optional — Databricks reads directly) |

## 8. Test the pipeline manually

In ADF Studio, click the `weather_hourly_pipeline` → **Debug**.

Expected duration: 5–10 min (cluster spin-up is the bulk).

Verify:

- New Parquet files appear under `bronze/weather/year=.../month=.../day=.../hour=...`
- `silver/weather` has 100 rows for the current hour
- `gold/weather_hourly` and `gold/weather_daily` are updated

## 9. Enable the schedule trigger

In ADF Studio, switch the `weather_hourly_pipeline` trigger from
**Disabled → Started**. Cron: `0 0 * * * *` (top of every hour).

---

## Tear-down

When you're done experimenting:

```bash
az group delete --name rg-weather-de-dev --yes --no-wait
```

This removes every resource in the resource group.

---

## Common issues

| Symptom | Likely cause | Fix |
|---|---|---|
| `403` on ADLS writes from Databricks | Service principal lacks `Storage Blob Data Contributor` | Re-run Bicep or grant the role manually |
| Key Vault access denied from Databricks | Missing AAD access policy on the vault | `az keyvault set-policy --secret-permissions get list --object-id <sp-id>` |
| Notebook times out at 10 min | Cluster too small for the data size | Bump `node_type_id` in the activity JSON |
| Bronze writes but Silver is empty | Cleansing rules dropping everything | Check `weather_description` / temperature nulls in the raw data |
| ADF activity fails with "user not authorized" | Databricks access token expired or wrong scope | Regenerate the token in the Databricks UI |

For anything else, see the [Databricks docs](https://docs.databricks.com/)
or the [ADF docs](https://learn.microsoft.com/azure/data-factory/).
