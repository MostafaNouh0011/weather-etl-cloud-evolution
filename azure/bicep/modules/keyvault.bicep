// ============================================================================
// modules/keyvault.bicep
// ----------------------------------------------------------------------------
// Azure Key Vault for the Weatherstack API key and any other secrets.
//   * RBAC authorization ON (modern mode — no legacy access policies).
//   * Soft-delete ON with 7-day retention (Azure default, pinned explicitly).
//   * The Weatherstack secret is created as an EMPTY placeholder here; the
//     real value is set out-of-band by `az keyvault secret set` in
//     deploy.md step 2. That mirrors the v1 split between .env.example
//     (committed) and .env (gitignored, real value).
//   * The Databricks managed identity gets the "Key Vault Secrets User"
//     role assignment so notebooks can read the secret via
//     dbutils.secrets.get.
// ============================================================================

@description('Azure region. Inherited from the parent template.')
param location string

@description('Short project slug used in resource naming (e.g. "weather").')
param projectName string

@description('Environment slug, e.g. "dev" or "prod".')
param environment string

@description('Key Vault SKU. "standard" is fine for the v1 of v2.')
param keyVaultSku string = 'standard'

@description('Name of the Weatherstack secret inside the vault. Must match the name used by deploy.md step 2 and the Databricks notebooks.')
param weatherstackSecretName string = 'weatherstack-api-key'

@description('AAD object ID of the Databricks workspace managed identity. When empty, the role assignment is skipped (useful for isolated tests).')
param databricksManagedIdentityObjectId string = ''

// Key Vault names are also capped at 24 chars. 10-char hash keeps us
// under with room to grow the project / environment names.
var keyVaultHash = take(uniqueString(resourceGroup().id, projectName, environment), 10)
var keyVaultName = toLower('kv-${projectName}${environment}${keyVaultHash}')

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  sku: {
    family: 'A'
    name: keyVaultSku
  }
  properties: {
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: true
    publicNetworkAccess: 'Enabled'
  }
  tags: {
    project: projectName
    environment: environment
  }
}

// Empty placeholder — the real value is set by deploy.md step 2.
// The value lives for at most the default 90 days; rotate as needed.
resource weatherstackSecretPlaceholder 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: weatherstackSecretName
  properties: {
    value: 'replace-me-via-az-cli'
    contentType: 'text/plain'
    attributes: {
      enabled: true
    }
  }
}

// "Key Vault Secrets User" role — lets the Databricks identity call
// dbutils.secrets.get without needing list / set / delete. The role
// definition GUID is stable across clouds, so it is safe to inline.
resource databricksSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(databricksManagedIdentityObjectId)) {
  name: guid(keyVault.id, databricksManagedIdentityObjectId, 'kv-secrets-user')
  scope: keyVault
  properties: {
    principalId: databricksManagedIdentityObjectId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
  }
}

output keyVaultName string = keyVault.name
output keyVaultUri string = keyVault.properties.vaultUri
output weatherstackSecretName string = weatherstackSecretPlaceholder.name
