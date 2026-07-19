// ============================================================================
// modules/databricks.bicep
// ----------------------------------------------------------------------------
// Azure Databricks workspace with a system-assigned managed identity. The
// identity is the principal that gets "Storage Blob Data Contributor" on the
// storage account (granted in main.bicep) and "Key Vault Secrets User" on
// Key Vault (granted in keyvault.bicep). Notebooks authenticate to both
// services as this identity — no tokens, no service principals to rotate.
//
// The workspace is left at the public-access default for the v1 of v2.
// Tighter network controls (private link, custom-managed VNet) are a v2.1
// concern and would add a VNet module that the current deploy.md does not
// need.
// ============================================================================

@description('Azure region. Inherited from the parent template.')
param location string

@description('Short project slug used in resource naming (e.g. "weather").')
param projectName string

@description('Environment slug, e.g. "dev" or "prod".')
param environment string

@description('Databricks SKU: "standard", "premium", or "trial". Premium is required for jobs clusters + Delta Live Tables; standard is enough for plain Delta + job clusters, which is what v1 of v2 uses.')
param databricksSku string = 'standard'

@description('Resource ID of the Log Analytics workspace. When provided, Databricks diagnostic settings are wired to it. Pass an empty string to skip.')
param logAnalyticsWorkspaceId string = ''

var managedResourceGroupName = toLower('databricks-rg-${projectName}-${environment}-${uniqueString(resourceGroup().id, projectName, environment)}')
var workspaceName = toLower('dbw-${projectName}-${environment}-${uniqueString(resourceGroup().id, projectName, environment)}')

resource databricksWorkspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: workspaceName
  location: location
  sku: {
    name: databricksSku
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedResourceGroupName)
    publicNetworkAccess: 'Enabled'
  }
  tags: {
    project: projectName
    environment: environment
  }
}

// Diagnostic settings — cluster logs to Log Analytics. This is what makes
// the ADF "Common issues" table in deploy.md actually point somewhere.
resource databricksDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (!empty(logAnalyticsWorkspaceId)) {
  name: 'send-logs-to-log-analytics'
  scope: databricksWorkspace
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        category: 'workspace'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
  }
}

output databricksWorkspaceName string = databricksWorkspace.name
output databricksWorkspaceUrl string = databricksWorkspace.properties.workspaceUrl
output databricksManagedIdentityObjectId string = databricksWorkspace.identity.principalId
output databricksManagedIdentityTenantId string = databricksWorkspace.identity.tenantId
