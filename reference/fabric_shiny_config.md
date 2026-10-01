# Configure per-user Fabric access in a Shiny application

Configure one Entra Web registration outside `server()`. Each visitor
signs in separately through 'shinyOAuth'; Fabric requests use that
visitor's delegated permissions. Requires a version of 'shinyOAuth' with
token targets and module-managed `OAuthConnection$access_token()`.

## Usage

``` r
fabric_shiny_config(
  tenant_id,
  client_id,
  client_secret,
  redirect_uri,
  services = c("fabric", "sql"),
  scopes = list(),
  default_service = services[[1L]],
  endpoint_hosts = list(),
  min_valid_for = 60
)
```

## Arguments

- tenant_id:

  One fixed Entra directory GUID. Guests sign in to this tenant.

- client_id:

  Entra Web application's client ID.

- client_secret:

  Server-side client secret. Never send this to the browser.

- redirect_uri:

  Exact registered Web callback URI; HTTPS in deployment.

- services:

  Character vector selecting `"fabric"` (discovery), `"sql"`, `"dax"`
  (semantic models), `"kql"` (Eventhouse), `"onelake"` (files and Delta
  tables), and/or `"graphql"`. Known endpoints and IDs do not need
  discovery.

- scopes:

  Named list of explicit delegated scopes, overriding each service's
  defaults. Short names are qualified with the service resource.
  Defaults are Fabric `Workspace.Read.All` and `Item.Read.All`, SQL
  `user_impersonation`, DAX `Dataset.Read.All`, KQL and OneLake
  `user_impersonation`, and GraphQL `GraphQLApi.Execute.All`. This
  integration does not support `.default` consent or incremental
  consent.

- default_service:

  Service used for initial code redemption. Defaults to the first entry
  of `services`; later calls select their own token target.

- endpoint_hosts:

  Named list of additional exact hostnames per service. Configure
  trusted gateways in application code, never from user input. Built-in
  Microsoft service hosts remain allowed. HTTPS is required.

- min_valid_for:

  Minimum remaining token lifetime in seconds at acquisition.

## Value

A `fabric_shiny_config` containing the 'shinyOAuth' client and resource
profiles. Printing it does not display credentials.

## Details

Only 'shinyOAuth' owns sign-in, validated OIDC identity, token caches
and refresh. This configuration disables Graph UserInfo while retaining
ID-token validation, PKCE and nonce handling. The client secret
authenticates the app; it does not turn the user's data requests into
service-principal requests. DAX and GraphQL share a Power BI token
target; its declared scopes combine the selected services' permissions
while their endpoint policies stay separate.

Scope selection does not grant Fabric item or SQL permissions. SQL's
delegated scope can allow writes when the user has the corresponding SQL
grants. See
[`vignette("shiny-integration")`](https://kennispunttwente.github.io/fabricQueryR/articles/shiny-integration.md)
for setup and session lifetime guidance.

## See also

[`fabric_shiny_ui()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_ui.md),
[`fabric_shiny_server()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_server.md),
[`fabric_shiny_token_provider()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_token_provider.md)

## Examples

``` r
if (FALSE) { # \dontrun{
config <- fabric_shiny_config(
  tenant_id = Sys.getenv("ENTRA_TENANT_ID"),
  client_id = Sys.getenv("ENTRA_CLIENT_ID"),
  client_secret = Sys.getenv("ENTRA_CLIENT_SECRET"),
  redirect_uri = "http://localhost:8100/",
  services = c("fabric", "sql")
)
} # }
```
