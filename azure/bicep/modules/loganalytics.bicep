// ============================================================================
// modules/loganalytics.bicep
// ----------------------------------------------------------------------------
// Single Log Analytics workspace for ADF diagnostic settings and Databricks
// cluster logs. The Databricks workspace wires its diagnostic setting to this
// workspace, and ADF wires its pipeline-run / trigger logs to it too.
// ============================================================================

@description('Azure region. Inherited from the parent template.')
param location string

@description('Short project slug used in resource naming (e.g. "weather").')
param projectName string

@description('Environment slug, e.g. "dev" or "prod".')
param environment string

@description('Retention in days for the default analytics table.')
param logAnalyticsRetentionDays int = 30

var workspaceName = toLower('log-${projectName}-${environment}-${uniqueString(resourceGroup().id, projectName, environment)}')

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: workspaceName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: logAnalyticsRetentionDays
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
  }
  tags: {
    project: projectName
    environment: environment
  }
}

output logAnalyticsWorkspaceId string = logAnalytics.id
output logAnalyticsWorkspaceName string = logAnalytics.name
output logAnalyticsCustomerId string = logAnalytics.properties.customerId
