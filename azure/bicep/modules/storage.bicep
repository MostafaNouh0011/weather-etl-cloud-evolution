// ============================================================================
// modules/storage.bicep
// ----------------------------------------------------------------------------
// ADLS Gen2 storage account for the v2 Medallion layout.
//   * Hierarchical namespace ON (this is what makes a StorageV2 account Gen2)
//   * Four containers: bronze / silver / gold / config
//   * TLS 1.2 minimum, public blob access OFF, shared-key access OFF
//   * Network defaults: all public networks allowed (dev). A v2.1 parameter
//     can add a networkAcls block when the workspace moves behind a VNet.
// ============================================================================

@description('Azure region. Inherited from the parent template.')
param location string

@description('Short project slug used in resource naming (e.g. "weather").')
param projectName string

@description('Environment slug, e.g. "dev" or "prod".')
param environment string

@description('Storage account SKU. Standard_LRS is the dev default.')
param storageAccountSku string = 'Standard_LRS'

// Storage account names are capped at 24 chars and only allow lowercase
// letters + digits. We use a 10-char hash so the total stays well under
// the limit even if the project / environment names grow.
var storageAccountHash = take(uniqueString(resourceGroup().id, projectName, environment), 10)
var storageAccountName = toLower('${projectName}${environment}${storageAccountHash}')

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: storageAccountSku
  }
  kind: 'StorageV2'
  properties: {
    // Hierarchical namespace is what turns StorageV2 into ADLS Gen2.
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    accessTier: 'Hot'
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
  tags: {
    project: projectName
    environment: environment
    layer: 'storage'
  }
}

// Containers. Names are the contract that the Databricks notebooks rely on —
// do not rename without grepping databricks/notebooks/ and adf/pipeline/.
resource bronzeContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: '${storageAccount.name}/default/bronze'
  properties: {
    publicAccess: 'None'
  }
  dependsOn: [
    storageAccount
  ]
}

resource silverContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: '${storageAccount.name}/default/silver'
  properties: {
    publicAccess: 'None'
  }
  dependsOn: [
    storageAccount
  ]
}

resource goldContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: '${storageAccount.name}/default/gold'
  properties: {
    publicAccess: 'None'
  }
  dependsOn: [
    storageAccount
  ]
}

resource configContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: '${storageAccount.name}/default/config'
  properties: {
    publicAccess: 'None'
  }
  dependsOn: [
    storageAccount
  ]
}

// Outputs consumed by main.bicep and by deploy.md step 2.
output storageAccountName string = storageAccount.name
output primaryBlobEndpoint string = storageAccount.properties.primaryEndpoints.blob
output bronzeContainerName string = bronzeContainer.name
output silverContainerName string = silverContainer.name
output goldContainerName string = goldContainer.name
output configContainerName string = configContainer.name
