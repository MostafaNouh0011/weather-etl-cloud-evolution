// ============================================================================
// main.bicep
// ----------------------------------------------------------------------------
// Entry point for the v2 Azure-native Weather ETL infrastructure.
//
//   * Provisions: Log Analytics, ADLS Gen2, Databricks, Key Vault, ADF.
//   * Grants the Databricks managed identity "Storage Blob Data Contributor"
//     on the storage account so job clusters can read/write Bronze, Silver,
//     and Gold without a shared key.
//   * The Key Vault module grants the same identity "Key Vault Secrets User"
//     so notebooks can fetch the Weatherstack API key via dbutils.secrets.get.
//
// Deploy:
//   az deployment group create \
//     --resource-group rg-weather-de-dev \
//     --template-file main.bicep \
//     --parameters @parameters/dev.parameters.json
//
// Outputs (consumed by deploy.md):
//   * storageAccountName        — step 3 (az storage blob upload)
//   * primaryBlobEndpoint        — notebooks read ABFSS URIs from this
//   * keyVaultName               — step 2 (az keyvault secret set)
//   * databricksWorkspaceUrl     — step 4 (databricks secrets create-scope)
//   * dataFactoryName            — step 6 (ADF pipeline import)
// ============================================================================

@description('Azure region for every resource.')
param location string = resourceGroup().location

@description('Short project slug used in resource naming (e.g. "weather").')
@maxLength(8)
param projectName string

@description('Environment slug, e.g. "dev" or "prod".')
@maxLength(5)
param environment string = 'dev'

@description('Storage account SKU. Standard_LRS for dev.')
param storageAccountSku string = 'Standard_LRS'

@description('Databricks SKU: standard / premium / trial.')
param databricksSku string = 'standard'

@description('Key Vault SKU. standard is fine for v1 of v2.')
param keyVaultSku string = 'standard'

@description('Log Analytics retention in days.')
param logAnalyticsRetentionDays int = 30

// ----------------------------------------------------------------------------
// Modules
// ----------------------------------------------------------------------------

module logAnalytics 'modules/loganalytics.bicep' = {
  name: 'logAnalytics'
  params: {
    location: location
    projectName: projectName
    environment: environment
    logAnalyticsRetentionDays: logAnalyticsRetentionDays
  }
}

module storage 'modules/storage.bicep' = {
  name: 'storage'
  params: {
    location: location
    projectName: projectName
    environment: environment
    storageAccountSku: storageAccountSku
  }
}

module databricks 'modules/databricks.bicep' = {
  name: 'databricks'
  params: {
    location: location
    projectName: projectName
    environment: environment
    databricksSku: databricksSku
    logAnalyticsWorkspaceId: logAnalytics.outputs.logAnalyticsWorkspaceId
  }
}

module keyVault 'modules/keyvault.bicep' = {
  name: 'keyVault'
  params: {
    location: location
    projectName: projectName
    environment: environment
    keyVaultSku: keyVaultSku
    databricksManagedIdentityObjectId: databricks.outputs.databricksManagedIdentityObjectId
  }
}

module dataFactory 'modules/datafactory.bicep' = {
  name: 'dataFactory'
  params: {
    location: location
    projectName: projectName
    environment: environment
    logAnalyticsWorkspaceId: logAnalytics.outputs.logAnalyticsWorkspaceId
  }
}

// Service principal for the ADF → Databricks linked service.
// Deployed AFTER keyVault so the SP's client secret can be stored in
// the vault as part of the same deployment.
module servicePrincipal 'modules/serviceprincipal.bicep' = {
  name: 'servicePrincipal'
  params: {
    location: location
    projectName: projectName
    environment: environment
    keyVaultId: keyVault.id
  }
}

// ----------------------------------------------------------------------------
// Cross-module role assignments
// ----------------------------------------------------------------------------

// "Storage Blob Data Contributor" — Databricks MI needs to read/write
// Bronze (Parquet), Silver (Delta), Gold (Delta), and the config container.
resource databricksStorageBlobDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.outputs.storageAccountName, databricks.outputs.databricksManagedIdentityObjectId, 'blob-contributor')
  scope: resourceId('Microsoft.Storage/storageAccounts', storage.outputs.storageAccountName)
  properties: {
    principalId: databricks.outputs.databricksManagedIdentityObjectId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9ee')
  }
}

// ----------------------------------------------------------------------------
// Outputs
// ----------------------------------------------------------------------------

output storageAccountName string = storage.outputs.storageAccountName
output primaryBlobEndpoint string = storage.outputs.primaryBlobEndpoint
output bronzeContainerName string = storage.outputs.bronzeContainerName
output silverContainerName string = storage.outputs.silverContainerName
output goldContainerName string = storage.outputs.goldContainerName
output configContainerName string = storage.outputs.configContainerName
output keyVaultName string = keyVault.outputs.keyVaultName
output keyVaultUri string = keyVault.outputs.keyVaultUri
output databricksWorkspaceName string = databricks.outputs.databricksWorkspaceName
output databricksWorkspaceUrl string = databricks.outputs.databricksWorkspaceUrl
output dataFactoryName string = dataFactory.outputs.dataFactoryName
output logAnalyticsWorkspaceName string = logAnalytics.outputs.logAnalyticsWorkspaceName
// SP outputs — the ADF linked service uses these to authenticate to
// the Databricks workspace. The secret VALUE is exposed only as the
// Key Vault secret URI, never in plain text; the value is available
// only at deploy time via `az deployment ... --query`.
output servicePrincipalAppId string = servicePrincipal.outputs.servicePrincipalAppId
output servicePrincipalObjectId string = servicePrincipal.outputs.servicePrincipalObjectId
output servicePrincipalClientSecretUri string = servicePrincipal.outputs.servicePrincipalClientSecretUri
