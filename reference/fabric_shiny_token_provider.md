# Use a shinyOAuth connection with Fabric functions

Adapt a module-managed `OAuthConnection` without adding another login
module. Create the provider inside the owning Shiny session, then pass
it as `token` to existing Fabric functions. Each operation reads the
current credential.

## Usage

``` r
fabric_shiny_token_provider(
  connection,
  services = c("fabric", "sql"),
  scopes = list(),
  targets = NULL,
  endpoint_hosts = list(),
  min_valid_for = 60
)
```

## Arguments

- connection:

  A connection returned by 'shinyOAuth' `auth$connection()`, or
  `auth$connection(id)` for a manager.

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

- targets:

  Optional named character vector mapping configured service names to
  the connection's declared target names. Defaults are `fabric`, `sql`,
  `power_bi` (DAX and GraphQL), `kusto` (KQL), and `storage` (OneLake).
  Target declarations on the existing OAuth client must use the matching
  resources and scopes.

- endpoint_hosts:

  Named list of additional exact hostnames per service. Configure
  trusted gateways in application code, never from user input. Built-in
  Microsoft service hosts remain allowed. HTTPS is required.

- min_valid_for:

  Minimum remaining token lifetime in seconds at acquisition.

## Value

A synchronous `function(audience, force_refresh = FALSE)` suitable for
the package's `token` arguments. The provider carries endpoint policy
metadata, which package HTTP and SQL transports enforce before
acquisition.

## Details

Captures this connection, never a lookup of a later login. Logout,
replacement, a foreign session or session closure makes it unusable.
Token refresh retains the reference. Do not put providers, authenticated
items or query results in global app state, serialize them to workers,
or remove their attributes.

Unknown resources and undeclared explicit scopes fail before
acquisition. `force_refresh` is forwarded to 'shinyOAuth'; acquisition
never redirects or falls back to AzureAuth. Typed
`shinyOAuth_access_error` conditions propagate, including
`refresh_pending` and `interaction_required`. SQL may wrap them as the
parent of a `fabric_sql_authentication_error`.

Only bearer tokens are supported. HTTP redirects are rejected for these
providers; pagination and operation URLs are checked independently.
Custom hosts require explicit application configuration. This protects
normal package transports; it is not a sandbox for untrusted R code,
which can export tokens or override an item's authentication arguments.

## See also

[`fabric_shiny_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_config.md),
[`fabric_shiny_server()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_server.md)
