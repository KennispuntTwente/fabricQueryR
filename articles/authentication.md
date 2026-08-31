# Get started with Microsoft Fabric authentication

When you want to use ‘fabricQueryR’ to access Microsoft Fabric, you
always start with authentication.

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
- your organization’s Microsoft Entra *tenant ID*;
- access to at least one Fabric workspace or item; and
- sometimes, a *client ID* supplied by your administrator.

You do *not* need a client secret, certificate, or manually copied
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
CLI. The value may also be called the *Directory (tenant) ID*.

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

When `FABRICQUERYR_CLIENT_ID` is not set, ‘fabricQueryR’ tries the
public Azure CLI client ID. Some organizations allow this and some do
not. If sign-in is blocked or your organization requires an approved
application, ask the administrator for a dedicated client ID.

### 3. Test the connection

Start with workspace discovery because it is a simple way to check both
sign-in and basic Fabric access:

``` r

workspaces <- fabric_workspaces()

workspaces
workspace <- workspaces[[1L]]
workspace$displayName
workspace$id
```

On the first call, a browser may open and ask you to sign in and approve
access. Use the same organizational account that you use in the Fabric
portal. Multifactor authentication and your organization’s Conditional
Access rules still apply.

If the call succeeds, `workspaces` is a list of read-only
`FabricWorkspace` R6 objects available to that account. The service
fields are available directly with `$`. Workspace methods such as
`$items()` call the corresponding discovery function—in this case,
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)—without
requiring copied IDs. Item discovery returns `FabricItem` objects or
type-specific subclasses:

``` r

items <- workspace$items()
items
item <- items[[1L]]
item$displayName
item$type
item$id
```

Actionable subclasses add methods for their workload. For example,
`workspace$lakehouses()` corresponds to
[`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
and the returned Lakehouse’s `$tables()` method corresponds to
[`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md).
The object reuses the credential acquired during discovery. Call
`$as_list()` or [`as.list()`](https://rdrr.io/r/base/list.html), or
request `output = "list"`, only when a plain record is specifically
needed.

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

‘fabricQueryR’ cannot grant Fabric permissions. The signed-in account
must already be able to access the relevant workspace, item, and data.

For an initial test, ask a workspace administrator to do one of the
following:

1.  add your account to the Fabric workspace, normally with the
    least-privileged role that supports your task; or
2.  share the specific Fabric item with your account and grant any
    additional data permission it needs.

Workspace visibility and data access are not always the same permission.
For example, a semantic model normally needs Read and Build access,
while OneLake data can require an additional data permission. If
discovery works but a read does not, ask the item owner which permission
that workload requires.

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

‘fabricQueryR’ uses
[‘AzureAuth’](https://azure.r-universe.dev/AzureAuth/doc/token.html) to
sign in to Microsoft Entra. Most functions accept the same
authentication arguments:

- `tenant_id` identifies the organization;
- `client_id` identifies the application;
- `token` supplies an existing token or token-provider function; and
- `auth_args` customizes automatic sign-in.

Most interactive users only need the tenant ID. ‘AzureAuth’ opens a
browser or uses device-code login, caches the resulting token, and
refreshes it when needed. ‘fabricQueryR’ chooses the correct token
audience for each Fabric service.

Leave `token = NULL` for automatic sign-in. When you supply `token`, it
becomes responsible for authentication and `auth_args` is not used. See
[`?fabric_workspaces`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md)
and the ‘AzureAuth’ [authentication
scenarios](https://azure.r-universe.dev/AzureAuth/doc/scenarios.html)
for the complete argument details. Supply the bearer token exactly as
issued: leading, trailing, or embedded whitespace and control characters
are rejected rather than trimmed or sent in an HTTP header.

A bearer-token string and an
[`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html)
each represent one OAuth audience. ‘fabricQueryR’ binds either form to
the first service audience that uses it and stops before reusing it for
a different service. For example, an operation that resolves a Fabric
item and then reads OneLake needs both Fabric and Azure Storage tokens.
Supply the function’s `storage_token` or `sql_token` argument when
available, or pass an audience-aware token-provider function that
returns the appropriate token for its `audience` argument. Automatic
sign-in already acquires and caches a separate token for each audience.

## Treat endpoint URLs as credential boundaries

Prefer discovered objects when a method or function accepts them. When
discovery cannot provide an invocation endpoint, copy the complete URL
from the Fabric portal. These routes keep both the service address and
the Fabric item identity visible to the caller.

A custom host, including one fronted by your organization through Azure
API Management or another gateway, is an explicit opt-in. ‘fabricQueryR’
refuses to acquire and forward its normal Fabric credential
automatically; supply `token` as a bearer token or token-provider
function. Use a custom endpoint only when your organization controls the
host, and use a token issued for the gateway’s intended audience. Set
`audience` explicitly when the token provider uses that argument to
acquire the gateway credential.

HTTPS and URL-shape validation reject malformed endpoint input, but do
not prove that your organization owns the hostname, that the gateway
forwards a request safely, or that a supplied token has the correct
audience. Confirm those properties with the gateway owner before sending
credentials or data.

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

## Advanced: authentication for automation

Automated code cannot wait for a person to complete a browser login. A
service principal is an application identity that can sign in on its
own. It requires a dedicated app registration and a secret or
certificate, and the service principal itself must be granted access in
Fabric.

### Service principal with a client secret

‘AzureAuth’ calls this the `client_credentials` flow. Keep the secret
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

Keep secrets outside source code and grant the service principal access
to the Fabric workspace or item. A certificate can replace the client
secret when your organization requires it.

### Managed identities and custom token providers

Some Fabric APIs support managed identities. Other environments may use
workload identity federation or an organization-specific token broker.
Pass an existing token or a token-provider function through `token` for
these cases. Support is API-specific, so check the relevant Fabric
[identity-support
documentation](https://learn.microsoft.com/en-us/rest/api/fabric/articles/identity-support)
and
[`?fabric_workspaces`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md)
before choosing this route.

## Set Fabric permissions by workload

Getting a token proves an identity; it does not grant access to Fabric
data. For a production workflow, check three layers:

1.  the tenant setting allows that identity type to use the API;
2.  the user or application can access the workspace or item; and
3.  the workload-specific data permission allows the requested
    operation.

Use the least-privileged role that supports the task. Fabric’s
[workspace-role
documentation](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces)
explains the standard roles; each workload guide lists its additional
access requirements.

## Signing out (token cache)

‘AzureAuth’ stores cached user tokens in its user-specific ‘AzureR’ data
directory, never in the package project. Use
[`AzureAuth::list_azure_tokens()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
to inspect the cache and remove only the relevant token with
[`AzureAuth::delete_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).
Use
[`AzureAuth::clean_token_directory()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
for signing out of every cached ‘AzureR’ session.
