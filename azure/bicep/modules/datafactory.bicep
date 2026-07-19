// ============================================================================
// modules/datafactory.bicep
// ----------------------------------------------------------------------------
// Azure Data Factory for orchestrating the three Databricks notebooks.
//   * System-assigned managed identity is the only identity ADF needs — it
//     authenticates to the Databricks workspace (via the workspace's
//     managedResourceGroup) and to the storage account.
//   * Diagnostic settings fan pipeline-run + trigger logs into the same
//     Log Analytics workspace the Databricks module uses, so the deploy.md
//     "Common issues" table has a single place to look.
//
// The actual pipeline / trigger / linked-service JSONs are NOT defined
// here — azure/adf/ describes them as separate files imported into ADF
// Studio. That's a deliberate split: ARM/Bicep owns the *factory*, ADF
// Studio owns the *contents*.
// ============================================================================

@description('Azure region. Inherited from the parent template.')
param location string

@description('Short project slug used in resource naming (e.g. "weather").')
param projectName string

@description('Environment slug, e.g. "dev" or "prod".')
param environment string

@description('Resource ID of the Log Analytics workspace. When provided, ADF diagnostic settings are wired to it. Pass an empty string to skip.')
param logAnalyticsWorkspaceId string = ''

var factoryName = toLower('adf-${projectName}-${environment}-${uniqueString(resourceGroup().id, projectName, environment)}')

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: factoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
  tags: {
    project: projectName
    environment: environment
  }
}

resource adfDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (!empty(logAnalyticsWorkspaceId)) {
  name: 'send-logs-to-log-analytics'
  scope: dataFactory
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        category: 'PipelineRuns'
        enabled: true
      }
      {
        category: 'TriggerRuns'
        enabled: true
      }
      {
        category: 'ActivityRuns'
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

output dataFactoryName string = dataFactory.name
output dataFactoryManagedIdentityObjectId string = dataFactory.identity.principalId
