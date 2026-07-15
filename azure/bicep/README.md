# Azure Bicep templates

Infrastructure-as-Code for the v2 Azure-native pipeline. All resources are
provisioned by a single deployment from the root template.

## Status

🚧 **Placeholder.** The actual Bicep files are not yet written. They will
be added incrementally on the `feat/azure-medallion` branch.

## What goes here

| File | Purpose |
|---|---|
| `main.bicep` | Entry point. Targets the resource group and orchestrates the modules below. |
| `modules/storage.bicep` | ADLS Gen2 storage account with 4 containers (`bronze`, `silver`, `gold`, `config`). |
| `modules/databricks.bicep` | Azure Databricks workspace, with managed identity for ADLS access. |
| `modules/datafactory.bicep` | Azure Data Factory instance, with managed identity. |
| `modules/keyvault.bicep` | Azure Key Vault with RBAC enabled, access policy for the Databricks SP. |
| `modules/loganalytics.bicep` | Log Analytics workspace for Databricks + ADF logs. |
| `parameters/dev.parameters.json` | Dev-environment parameters (region, naming, SKU). |
| `parameters/prod.parameters.json` | Prod-environment parameters (later, for v2.1+). |

## Reference

See [`../deploy.md`](../deploy.md) for the deploy command and the
high-level resource list.
