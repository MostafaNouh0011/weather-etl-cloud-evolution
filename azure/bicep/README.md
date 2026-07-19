# Azure Bicep templates

Infrastructure-as-Code for the v2 Azure-native pipeline. All resources are
provisioned by a single deployment from the root template.

## Status

✅ **Scaffolded.** `main.bicep` + 5 modules + `parameters/dev.parameters.json`
are in place. Resource contracts have been cross-checked manually (no
`az bicep build` was available in the build environment; see *Validation*
below).

## What's here

| File | Purpose |
|---|---|
| `main.bicep` | Entry point. Targets the resource group, orchestrates the modules, and grants the Databricks managed identity `Storage Blob Data Contributor` on the storage account. |
| `modules/storage.bicep` | ADLS Gen2 storage account (hierarchical namespace ON, 4 containers `bronze` / `silver` / `gold` / `config`, TLS 1.2, public blob access off, shared-key access off). |
| `modules/databricks.bicep` | Azure Databricks workspace with a system-assigned managed identity; diagnostic settings fan cluster logs into Log Analytics. |
| `modules/datafactory.bicep` | Azure Data Factory with a system-assigned managed identity; diagnostic settings fan pipeline / trigger / activity logs into Log Analytics. |
| `modules/keyvault.bicep` | Azure Key Vault (RBAC authorization ON, soft-delete ON). Creates the `weatherstack-api-key` secret as an empty placeholder; the real value is set out-of-band by `az keyvault secret set` (deploy.md step 2). Grants the Databricks MI `Key Vault Secrets User` on the vault. |
| `modules/loganalytics.bicep` | Log Analytics workspace (PerGB2018, 30-day retention) used by both Databricks and ADF diagnostic settings. |
| `parameters/dev.parameters.json` | Dev-environment parameters (`location=eastus`, SKUs, retention). |

## Outputs the deploy guide consumes

`main.bicep` emits the four values `deploy.md` references after the
`az deployment group create` call:

- `storageAccountName` → step 3 (`az storage blob upload`)
- `keyVaultName` → step 2 (`az keyvault secret set`)
- `databricksWorkspaceUrl` → step 4 (`databricks secrets create-scope`)
- `dataFactoryName` → step 6 (ADF pipeline import)

Plus the four container names and `primaryBlobEndpoint`, which the
notebooks will read via ABFSS URIs.

## Design choices (locked in)

These are *not* up for review without an explicit reason — they are
recorded here so the next session can find the rationale.

1. **System-assigned managed identities** for both Databricks and ADF.
   No service principals, no secrets to rotate. AAD owns the rotation.
2. **ADLS Gen2 over StorageV1** — required for the Medallion Delta layout
   the notebooks assume. `isHnsEnabled: true` is non-negotiable.
3. **Public blob access off + shared-key access off.** Databricks reaches
   storage only via its MI. The one-time bootstrap of uploading
   `cities.json` in deploy.md step 3 uses `--auth-mode login` (AAD), not
   the storage account key.
4. **Key Vault RBAC mode** (`enableRbacAuthorization: true`). No legacy
   access policies; the Databricks MI gets `Key Vault Secrets User` via
   the standard `Microsoft.Authorization/roleAssignments` resource.
5. **Single Log Analytics workspace** shared between ADF and Databricks.
   The deploy.md "Common issues" table assumes one place to look.
6. **No VNet, no private link, no custom DNS.** Adding them would mean
   a VNet module and a longer deploy. Deferred to v2.1; the
   `publicNetworkAccess: 'Enabled'` defaults make that swap-out obvious.
7. **Storage / KV name safety:** both have 24-char limits. Each module
   uses `take(uniqueString(...), 10)` so even with the max-length
   `projectName` (8) and `environment` (5) inputs, the names fit.

## Validation

This commit was made **without `az bicep build` in the build
environment**. The following manual checks were performed instead, and
should be re-confirmed on a machine with the Azure CLI before the
first real `az deployment group create`:

- All `module` invocations in `main.bicep` pass exactly the parameters
  their target module declares.
- All `module.outputs.*` references in `main.bicep` exist on the
  corresponding module.
- All resource type / API version pairs are valid (e.g.
  `Microsoft.Storage/storageAccounts@2023-05-01`).
- Storage account and Key Vault names are ≤ 24 chars given the
  documented parameter bounds.
- The two role assignments (Storage Blob Data Contributor on the
  storage account, Key Vault Secrets User on the vault) use the
  standard Microsoft role-definition GUIDs and `guid()`-derived names
  so re-deploys don't collide.
- No module references files that were not also written in this
  commit (an early draft of `keyvault.bicep` referenced a missing
  `modules/role-assignments/` module; that was inlined before commit).

**First thing to do on a machine with `az`:**

```bash
az bicep build --file main.bicep
```

Expected outcome: zero errors. If anything fails, the most likely
culprits are (a) a typo in a resource type or API version, (b) a
parameter name mismatch, or (c) a `module.outputs.X` reference to a
property the module never exported.

## Reference

See [`../deploy.md`](../deploy.md) for the deploy command and the
high-level resource list. See [`../README.md`](../README.md) for the
end-to-end architecture diagram.

## Out of scope (v2.1+)

- VNet / private link / custom DNS — the `publicNetworkAccess: 'Enabled'`
  defaults are the seam.
- Unity Catalog (workspace-local Hive metastore is enough for the v1).
- `parameters/prod.parameters.json` — only `dev.parameters.json` exists.
- Auto-shutdown / cost alerts on the Databricks job cluster.
