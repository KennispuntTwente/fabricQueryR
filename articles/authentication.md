# Get started with Microsoft Fabric authentication

Authentication is how Microsoft Fabric confirms *who you are*.
Authorization is what that person or application is allowed to see and
do. You need both: signing in successfully does not automatically grant
access to a workspace or its data.

This guide starts with the normal setup for a person using R
interactively. That is the best place to begin. Later sections cover app
registrations, service principals, managed identities, custom tokens,
and other options for automated or advanced use.

## Quick start: sign in as yourself

For a normal interactive login, you need:

- a work or school Microsoft account that can sign in to your
  organization’s Fabric portal;
- your organization’s Microsoft Entra **tenant ID**;
- access to at least one Fabric workspace or item; and
- sometimes, a **client ID** supplied by your administrator.

You do **not** need a client secret, certificate, or manually copied
access token for this first setup.

### 1. Find your tenant ID

The tenant ID identifies your organization. It is a GUID that looks like
`12345678-1234-1234-1234-123456789abc`.

You may be able to find the tenant ID in the [Fabric
portal](https://app.fabric.microsoft.com). Open your profile (top-right
corner) and hover over the tooltip icon near ‘Tenant Name’; this will
then show ‘Tenant ID: ’.

Alternatively, follow Microsoft’s [tenant ID
guide](https://learn.microsoft.com/en-us/entra/fundamentals/how-to-find-tenant)
for the Microsoft Entra admin center, Azure portal, PowerShell, or Azure
CLI. The value may also be called the **Directory (tenant) ID**.

If you do not have access to your tenant ID through either portal, ask
your Microsoft 365, Azure, or Fabric administrator for the tenant ID.

### 2. Set the tenant ID in R

For a first test, set it in the current R session:

``` r

Sys.setenv(FABRICQUERYR_TENANT_ID = "<your-tenant-id>")

library(fabricQueryR)
```

If your administrator has given you an app-registration client ID, set
that as well:

``` r

Sys.setenv(FABRICQUERYR_CLIENT_ID = "<your-client-id>")
```

When `FABRICQUERYR_CLIENT_ID` is not set, `fabricQueryR` tries the
public Azure CLI client ID. Some organizations allow this and some do
not. If sign-in is blocked or your organization requires an approved
application, ask the administrator for a dedicated client ID.

### 3. Test the connection

Start with workspace discovery because it is a simple way to check both
sign-in and basic Fabric access:

``` r

workspaces <- fabric_workspaces()

purrr::map(workspaces, function(workspace) {
  workspace[c("displayName", "id")]
})
```

On the first call, a browser may open and ask you to sign in and approve
access. Use the same organizational account that you use in the Fabric
portal. Multifactor authentication and your organization’s Conditional
Access rules still apply.

If the call succeeds, `workspaces` is a list of named `fabric_workspace`
objects available to that account. Item discovery similarly returns a
list of named `fabric_item` objects:

``` r

items <- fabric_items(workspaces[[1]])
purrr::map(items, function(item) {
  item[c("displayName", "type", "id")]
})
```

An empty result does not necessarily mean sign-in failed. It can mean
that the account has not been given access to a Fabric workspace.

### 4. Save the settings for future R sessions

[`Sys.setenv()`](https://rdrr.io/r/base/Sys.setenv.html) only changes
the current R session. To load the IDs automatically, add them to your
user-level `.Renviron` file:

``` r

file.edit("~/.Renviron")
```

Add one or both lines, without R code around them:

``` text
FABRICQUERYR_TENANT_ID=<your-tenant-id>
FABRICQUERYR_CLIENT_ID=<your-client-id>
```

Omit the client-ID line if the default client works for your
organization. Save the file and restart R. Confirm that R can read the
values:

``` r

Sys.getenv("FABRICQUERYR_TENANT_ID")
Sys.getenv("FABRICQUERYR_CLIENT_ID")
```

Use the `.Renviron` file in your user home directory, not one committed
with a project. Tenant and client IDs are not passwords, but keeping
machine-specific configuration out of source control is still good
practice.

## Make sure Fabric access has been granted

`fabricQueryR` cannot grant Fabric permissions. The signed-in account
must already be able to access the relevant workspace, item, and data.

For an initial test, ask a workspace administrator to do one of the
following:

1.  add your account to the Fabric workspace, normally with the
    least-privileged role that supports your task; or
2.  share the specific Fabric item with your account and grant any
    additional data permission it needs.

The **Viewer** workspace role is enough to list the workspace and its
items, but does not itself grant direct OneLake data access. A generic
item **Read** grant also exposes metadata without granting the
underlying OneLake data. For example, a semantic model normally needs
Read and Build permission. A direct Lakehouse read needs an Admin,
Member, or Contributor workspace role; item **Read** plus **ReadAll**;
or, when OneLake security is enabled, item **Read** plus membership in a
OneLake role that grants **Read** on the target data. Detailed workload
guidance appears later in this vignette.

## First-login troubleshooting

These are the most common starting problems:

| What you see | What to check |
|----|----|
| `tenant_id is required` | Set `FABRICQUERYR_TENANT_ID` and check for spelling mistakes in the environment-variable name. |
| The browser signs in to the wrong account | Sign out of the unwanted Microsoft account, or retry without the token cache as shown below. |
| Your organization blocks the application or asks for admin approval | Ask an Entra administrator for an approved app-registration client ID and any required consent. |
| Login succeeds but no workspaces appear | Confirm that the same account can open the expected workspace in the Fabric portal and has been added or invited to it. |
| HTTP 401 | The login/token is invalid for this operation, expired, or belongs to the wrong tenant or resource. |
| HTTP 403 | Fabric knows who you are, but the account lacks a required workspace, item, or data permission. |

If a cached login is using the wrong account, retry once without reading
or writing the cache:

``` r

workspaces <- fabric_workspaces(
  auth_args = list(use_cache = FALSE)
)
```

For R running on a remote machine where no local browser can open, use a
device code:

``` r

workspaces <- fabric_workspaces(
  auth_args = list(auth_type = "device_code")
)
```

R prints a code and a Microsoft sign-in address. Open that address in
any browser, enter the code, and sign in with the intended account.

## How authentication works

`fabricQueryR` uses
[`AzureAuth`](https://azure.r-universe.dev/AzureAuth/doc/token.html) to
sign in to Microsoft Entra. Every exported function that needs
authentication accepts the same four arguments:

- `tenant_id` identifies the organization and defaults to
  `FABRICQUERYR_TENANT_ID`.
- `client_id` identifies the application and defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI public client ID.
- `token` can supply an existing token or token provider. Most
  interactive users should leave it as `NULL`.
- `auth_args` customizes how `AzureAuth` signs in. Most interactive
  users can leave it as an empty list.

With the defaults, `AzureAuth` first looks for a matching cached token.
If none is available, it normally opens a browser or uses device-code
login. Later calls reuse and refresh the cached token, so signing in is
not usually required for every query.

See the `AzureAuth` documentation on [authentication
scenarios](https://azure.r-universe.dev/AzureAuth/doc/scenarios.html)
and
[caching](https://azure.r-universe.dev/AzureAuth/doc/token.html#caching).

### Authentication arguments and precedence

When `token = NULL`, `fabricQueryR` asks `AzureAuth` to obtain the
token. The package chooses the correct token resource for each Fabric
service and supplies the tenant, client ID, and Microsoft
identity-platform version. Other
[`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
options can be supplied in `auth_args`.

When `token` is supplied, it takes responsibility for authentication and
`auth_args` is not used. This avoids accidentally combining two
different login methods.

### Using your own app registration interactively

A dedicated app registration gives an organization more control over
which users may sign in and which delegated API permissions they may
request. Ask an Entra administrator to create or approve one if the
default client is blocked.

For browser-based authorization-code login, the app needs a suitable
redirect URI (normally `http://localhost:1410`) and R needs the `httpuv`
package. For device-code login, the app registration needs **Allow
public client flows** enabled. Your tenant’s user/admin consent,
Conditional Access, and multifactor-authentication policies continue to
apply.

Do not create a client secret for ordinary interactive user login.

## Choosing an advanced authentication method

If the quick start works and you are using R interactively, you can
continue using it. Choose one of the options below only when the way R
runs requires it.

| Method | Best suited to | Credential |
|----|----|----|
| Interactive user login | A person working in a local or remote R session | Browser login or device code |
| Service principal | Scheduled scripts, CI/CD, or applications with no person present | Client secret or certificate |
| Managed identity | R running in a supported Azure-hosted environment | Identity managed by Azure |
| Existing token or provider | An organization with its own token broker or workload-identity system | Supplied by that system |

## Advanced: pass an AzureAuth token

You can acquire a token with `AzureAuth` and pass the resulting R6
object directly. `fabricQueryR` extracts its access token, checks its
expiry, and calls its `refresh()` method when needed:

``` r

fabric_token <- AzureAuth::get_azure_token(
  resource = c(
    "https://api.fabric.microsoft.com/.default",
    "offline_access"
  ),
  tenant = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  app = Sys.getenv("FABRICQUERYR_CLIENT_ID"),
  version = 2
)

workspaces <- fabric_workspaces(token = fabric_token)
```

Tokens are resource-specific. Pass a token issued for the resource used
by the function:

| Operations | Azure AD v2 resource/scope |
|----|----|
| Discovery, Fabric REST, and jobs | `https://api.fabric.microsoft.com/.default` |
| OneLake shortcut Core REST APIs | `https://api.fabric.microsoft.com/.default` |
| Livy as a delegated user | `https://api.fabric.microsoft.com/Lakehouse.Execute.All`, `Lakehouse.Read.All`, `Code.AccessFabric.All`, and `Code.AccessStorage.All` |
| Livy as a service principal | `https://analysis.windows.net/powerbi/api/.default` |
| OneLake files and Delta tables | `https://storage.azure.com/.default` |
| SQL connections | `https://database.windows.net/.default` |
| DAX / Power BI Execute Queries | `https://analysis.windows.net/powerbi/api/.default` |
| KQL | `https://api.kusto.windows.net/.default` |
| GraphQL as a delegated user | `https://analysis.windows.net/powerbi/api/GraphQLApi.Execute.All` |
| GraphQL as a service principal | `https://api.fabric.microsoft.com/.default` |

For a GraphQL service principal acquired by the package, the Fabric API
audience is selected automatically:

``` r

result <- fabric_graphql_query(
  api,
  query,
  auth_args = list(
    password = Sys.getenv("FABRIC_CLIENT_SECRET"),
    auth_type = "client_credentials"
  )
)
```

Set `audience = "https://api.fabric.microsoft.com/.default"` explicitly
when a custom token-provider function obtains the service-principal
token.

## Advanced: authentication for automation

Automated code cannot wait for a person to complete a browser login. A
service principal is an application identity that can sign in on its
own. It requires a dedicated app registration and a secret or
certificate, and the service principal itself must be granted access in
Fabric.

### Service principal with a client secret

`AzureAuth` calls this the `client_credentials` flow. Keep the secret
outside source code:

``` r

workspaces <- fabric_workspaces(
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID"),
  auth_args = list(
    password = Sys.getenv("FABRIC_CLIENT_SECRET"),
    auth_type = "client_credentials"
  )
)
```

When client credentials are selected, the package does not request
`offline_access`, because application tokens do not use delegated
refresh tokens. `AzureAuth` reacquires them with the application
credential.

### Service principal with a certificate

Certificate authentication avoids a long-lived client secret:

``` r

workspaces <- fabric_workspaces(
  auth_args = list(
    certificate = Sys.getenv("FABRIC_CLIENT_CERTIFICATE"),
    auth_type = "client_credentials"
  )
)
```

`certificate` accepts the formats supported by `AzureAuth`, including a
PEM/PFX file and supported Azure Key Vault certificate objects. Protect
the certificate and its private key as credentials.

### Managed identity

Where the individual Fabric API supports managed identities, acquire an
`AzureToken` with
[`AzureAuth::get_managed_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
and pass it through `token`:

``` r

managed_token <- AzureAuth::get_managed_token(
  "https://api.fabric.microsoft.com"
)
workspaces <- fabric_workspaces(token = managed_token)
```

Managed-identity support is API-specific. Check the **Microsoft Entra
supported identities** table on the relevant Fabric REST API page. The
current Fabric [identity-support
documentation](https://learn.microsoft.com/en-us/rest/api/fabric/articles/identity-support)
explains the tenant switch and this per-API requirement.

### Custom token brokers

For workload identity federation, an external secret store, or another
token broker, pass a function. It can accept `audience` and
`force_refresh`:

``` r

provider <- function(audience, force_refresh = FALSE) {
  # Acquire a token for `audience`; bypass your cache when force_refresh is TRUE.
  my_token_broker(audience, refresh = force_refresh)
}

files <- fabric_onelake_list(
  "Analytics",
  "Curated.Lakehouse",
  token = provider
)
```

The callback must return one bearer-token string (or a list containing
an `access_token` or `token` field). This is also the escape hatch for
authentication methods not implemented by `AzureAuth`.

## Detailed Fabric permissions by workload

Getting a token proves an identity; it does not grant that identity
access to Fabric data. The quick-start section described the basic
access requirement. For production use or troubleshooting a particular
function, configure the relevant layers below:

1.  For service principals and managed identities, a Fabric
    administrator normally enables **Service principals can use Fabric
    APIs** under **Admin portal \> Tenant settings \> Developer
    settings**. Scope the setting to a dedicated Entra security group
    where possible. See [Fabric identity
    support](https://learn.microsoft.com/en-us/rest/api/fabric/articles/identity-support).
2.  Add the user, service principal, managed identity, or an appropriate
    group to the workspace, or grant item-level permissions. Fabric
    [workspace
    roles](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces)
    are Admin, Member, Contributor, and Viewer. Use the least-privileged
    role that permits the required operation.
3.  Grant workload-specific data permissions. Workspace visibility and
    data access are not always the same permission.

The main workload considerations are:

- **Fabric discovery and item jobs.** The principal needs access to the
  workspace/item and permission for the requested read, write, or
  execute operation. Service principals and managed identities are
  supported only where the individual API says so.
- **OneLake and Delta.** Admin, Member, and Contributor workspace roles
  can read and write all OneLake data in an item. For narrower direct
  access when OneLake security is not enabled, grant item **Read** (item
  visibility) and **ReadAll** (OneLake data). Item **Read** alone is
  insufficient. When OneLake security is enabled, grant item **Read**
  plus membership in a OneLake role whose **Read** scope includes the
  target table or folder; **ReadAll** grants access only through the
  `DefaultReader` role while that role exists and still includes the
  principal. fabricQueryR is not a Fabric-supported or registered
  authorized third-party engine and doesn’t fetch or enforce OneLake
  RLS/CLS. OneLake blocks direct file reads when the caller’s effective
  access is row- or column-restricted, so the package fails instead of
  returning filtered data. Use an unrestricted caller, a supported
  Fabric engine, or an [authorized
  engine](https://learn.microsoft.com/en-us/fabric/onelake/security/onelake-security-integrations-overview)
  that enforces the returned policies. A Fabric administrator must also
  enable **Users can access data stored in OneLake with apps external to
  Fabric** for the caller in the tenant’s OneLake settings. See [OneLake
  tenant
  settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake),
  the [Fabric permission
  model](https://learn.microsoft.com/en-us/fabric/security/permission-model),
  [OneLake security
  overview](https://learn.microsoft.com/en-us/fabric/onelake/security/get-started-onelake-security),
  [OneLake row-level
  security](https://learn.microsoft.com/en-us/fabric/onelake/security/row-level-security),
  and [OneLake security best
  practices](https://learn.microsoft.com/en-us/fabric/onelake/security/best-practices-secure-data-in-onelake).
- **OneLake shortcuts.** Listing and inspection use the Fabric API
  audience and require `OneLake.Read.All` or `OneLake.ReadWrite.All`;
  creation and deletion require `OneLake.ReadWrite.All`. The Core API
  supports users, service principals, and managed identities. Deleting a
  shortcut leaves the target storage untouched.
- **SQL.** A workspace role or item Read permission permits connection,
  while SQL `GRANT`, `DENY`, and database roles control granular data
  access. See [Microsoft Entra authentication in Fabric Data
  Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/entra-id-authentication)
  and [SQL granular
  permissions](https://learn.microsoft.com/en-us/fabric/data-warehouse/sql-granular-permissions).
- **DAX.** Enable the **Semantic Model Execute Queries REST API** tenant
  setting. A user needs semantic-model Read and Build permissions.
  Service principals additionally need the Power BI service-principal
  tenant setting. For `api = "json"`, service principals cannot query
  models with RLS or SSO enabled. The `api = "arrow"` endpoint
  additionally requires Premium or Fabric capacity and the **Allow XMLA
  endpoints and Analyze in Excel with on-premises semantic models**
  integration setting. It also has a different effective-identity
  contract: `effectiveUsername` is user-only, while a service principal
  may use `roles` when it is a workspace admin. See [JSON Execute
  Queries](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group)
  and the endpoint-specific notes in
  [`?fabric_pbi_dax_query`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md).
- **GraphQL.** Grant **Run Queries and Mutations** (Execute) on the
  GraphQL API and grant access to its underlying data source. A
  Contributor workspace role is convenient for development but
  item-level permission is narrower. See the [GraphQL service-principal
  guide](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-service-principal).
- **KQL.** Grant a Fabric workspace/item role or a Kusto database role
  such as `viewer`; effective access is the union of Fabric and Kusto
  roles. See [Kusto role-based access
  control](https://learn.microsoft.com/en-us/kusto/access-control/role-based-access-control?view=microsoft-fabric).
- **Livy.** The principal needs access to the Lakehouse and the
  execution/read permissions documented for Livy. Service-principal
  authentication is supported for app-only Spark sessions; see the [Livy
  session
  guide](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session).

When authentication succeeds but Fabric returns HTTP 403, inspect
workspace, item, and workload permissions and the **Users can access
data stored in OneLake with apps external to Fabric** tenant setting
before changing the login flow. HTTP 401 more often indicates the wrong
token resource, an expired/nonrefreshable raw token, or a tenant/app
mismatch.

## Sign out, token cache, and secret safety

`AzureAuth` stores cached user tokens in its user-specific AzureR data
directory, never in the package project. Use
[`AzureAuth::list_azure_tokens()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
to inspect the cache and remove only the relevant token with
[`AzureAuth::delete_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).
Reserve
[`AzureAuth::clean_token_directory()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
for intentionally signing out of every cached AzureR session.

Do not commit client secrets, certificates, bearer tokens, `.Renviron`,
or AzureR cache contents. In CI, inject credentials from the platform’s
secret store and prefer certificate or federated credentials over client
secrets where your token provider supports them.
