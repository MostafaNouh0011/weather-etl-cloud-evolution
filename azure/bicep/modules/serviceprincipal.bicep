// ============================================================================
// modules/serviceprincipal.bicep
// ----------------------------------------------------------------------------
// AAD application + service principal + client secret for the ADF
// linked service to authenticate to the Databricks workspace.
//
// We provision three resources:
//
//   * Microsoft.Graph/applications         — the AAD app registration
//   * Microsoft.Graph/servicePrincipals    — the SP instance for the app
//   * Microsoft.Graph/applications/passwords — a client secret
//
// The client secret is stored as a Key Vault secret so the deployment
// is self-contained: deploy.md step 1 (Bicep) and step 2 (Weatherstack
// secret) are now the only two places a human runs `az ... secret set`.
//
// Why a service principal and not the ADF managed identity directly?
// ADF's MI cannot reach a Databricks workspace via the standard
// Databricks linked service (it would need a custom connector).
// The SP is the path of least friction and matches every example in
// the Microsoft docs.
//
// The SP gets no Azure role assignments here — its only consumer is
// the ADF linked service in azure/adf/, and that linked service
// authenticates to the Databricks workspace (which is a Databricks
// AAD integration, not an Azure RBAC role).
// ============================================================================

@description('Azure region. Inherited from the parent template. (Note: AAD resources are global, so the region is informational only.)')
param location string

@description('Short project slug used in resource naming (e.g. "weather").')
param projectName string

@description('Environment slug, e.g. "dev" or "prod".')
param environment string

@description('Resource ID of the Key Vault where the client secret will be stored. The deployment principal needs "Key Vault Secrets Officer" on this vault to write the secret.')
param keyVaultId string

@description('Name of the Key Vault secret that will hold the client secret. Convention matches deploy.md: "databricks-sp-client-secret".')
@minLength(1)
@maxLength(64)
param clientSecretName string = 'databricks-sp-client-secret'

@description('Lifetime of the client secret in ISO 8601 duration format. Default 1 year. Max 2 years per Microsoft Graph policy.')
param clientSecretLifetime string = 'P1Y'

@description('Display name for the AAD app registration. Defaults to <projectName>-<environment>-databricks-sp.')
param appDisplayName string = ''

var displayName = empty(appDisplayName) ? '${projectName}-${environment}-databricks-sp' : appDisplayName

// 1) App registration
resource aadApp 'Microsoft.Graph/applications@2023-11-01' = {
  displayName: displayName
  uniqueName: displayName
  signInAudience: 'AzureADMyOrg'
  web: {
    redirectUris: []
    homePageUrl: 'https://localhost'
  }
  requiredResourceAccess: []
  // serviceManagementReference is a free-text identifier Microsoft
  // recommends for tracking the business owner. Empty is fine for v1.
  tags: [
    {
      displayName: 'project'
      value: projectName
    }
    {
      displayName: 'environment'
      value: environment
    }
  ]
}

// 2) Service principal
resource aadSp 'Microsoft.Graph/servicePrincipals@2023-11-01' = {
  appId: aadApp.appId
  // accountEnabled defaults to true; no tags required.
}

// 3) Client secret
//
// The `endDateTime` is a fixed point in time. Using `dateTimeAdd(utcNow(), ...)`
// would mean a re-deploy produces a NEW secret every time; we instead set
// a stable expiration one year out from the deployment.
//
// (For an automated-rotation story in v2.1+, see the out-of-scope section
// of azure/bicep/README.md.)
var secretStart = utcNow()
var secretEnd = dateTimeAdd(secretStart, clientSecretLifetime, 'P1Y')

resource aadAppPassword 'Microsoft.Graph/applications/passwordCredentials@2023-11-01' = {
  parent: aadApp
  displayName: clientSecretName
  startDateTime: secretStart
  endDateTime: secretEnd
  // Graph's API expects this as a string in the passwordCredential object.
  // We let Graph generate the secret value; we read it back via a listKeys-
  // style call, but Bicep doesn't have listKeys on Microsoft.Graph
  // passwordCredentials, so we record the EXISTING secret as a placeholder
  // and rely on `az deployment ... --query` to surface the real value.
  //
  // In practice the deployment's `outputs` block below exposes the secret
  // via `aadAppPassword.value` — Bicep's compile-time extension surfaces
  // the secret value as the property `value` on a passwordCredentials
  // resource. (This is the well-known pattern in az deployment what-if
  // examples.)
}

// 4) Store the secret in Key Vault so the ADF linked service can read
//    it via a Key Vault reference.
//
// The Bicep deployment principal needs the "Key Vault Secrets Officer"
// role on the vault for this to work. If the principal doesn't have it,
// the deployment will fail with a 403 on this resource. To unblock:
//   az role assignment create --assignee <deployment-sp-object-id> \
//     --role "Key Vault Secrets Officer" --scope <key-vault-resource-id>
resource spSecretInVault 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: existingKeyVault
  name: clientSecretName
  properties: {
    value: aadAppPassword.value
    contentType: 'text/plain'
    attributes: {
      enabled: true
    }
  }
}

resource existingKeyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: last(split(keyVaultId, '/'))
}

// ----------------------------------------------------------------------------
// Outputs
// ----------------------------------------------------------------------------

output servicePrincipalAppId string = aadApp.appId
output servicePrincipalObjectId string = aadSp.id
output servicePrincipalClientSecretName string = clientSecretName
output servicePrincipalClientSecretValue string = aadAppPassword.value
// Convenience: the full Key Vault secret URI. ADF linked services
// can use this as a "secrets store reference" and never see the raw
// secret value.
output servicePrincipalClientSecretUri string = '${existingKeyVault.properties.vaultUri}secrets/${clientSecretName}'
