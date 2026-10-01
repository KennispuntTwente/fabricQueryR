# Use the signed-in visitor's Fabric access in a Shiny server

Call once inside `server()`, using the same ID and configuration as
[`fabric_shiny_ui()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_ui.md).
Returns helpers for the current user's authorization and the package's
existing discovery and query interfaces.

## Usage

``` r
fabric_shiny_server(
  id,
  config,
  auto_redirect = FALSE,
  reauth_after_seconds = 8 * 60 * 60,
  refresh_proactively = TRUE
)
```

## Arguments

- id:

  Module identifier, matching `fabric_shiny_server()`.

- config:

  Configuration returned by
  [`fabric_shiny_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_config.md).

- auto_redirect:

  Redirect to sign-in automatically. The default `FALSE` lets the
  application show its sign-in button first.

- reauth_after_seconds:

  Maximum local authorization age in seconds. Defaults to eight hours.
  Must be finite and positive; choose a lifetime appropriate for your
  application.

- refresh_proactively:

  Let 'shinyOAuth' refresh credentials proactively.

## Value

A session-local list of callable helpers:

- `ready(service = NULL)`: reactive recorded scope coverage for all
  selected services, or one service. Does not prove remote access or
  token freshness.

- `status()`: reactive per-service status and acquisition error reason,
  without tokens. `not_acquired` does not establish missing consent.

- [`identity()`](https://rdrr.io/r/base/identity.html): selected
  validated ID-token fields from 'shinyOAuth'.

- `generation()`: the current connection ID, or `NULL` when signed out.
  Unchanged by refresh; use it to guard event-bound results and caches.

- `token_provider()`: a provider bound to the current connection, for
  existing functions' `token` arguments.

- `access_token(service, async = FALSE)`: a fixed bearer token for one
  configured service, or a promise resolving to it when `async = TRUE`.
  Acquire it before invoking a background query. The token retains the
  service's endpoint policy and can be serialized to a worker.

- `workspaces(...)` and `item(workspace, item, ...)`: discovery with the
  current user's credentials, returning the ordinary R6 objects by
  default.

- `request(path, method = "GET", query = list(), body = NULL, idempotent = FALSE)`:
  an advanced Fabric REST call beneath `/v1`, returning an httr2
  response. `path` is relative, without query/fragment; credentials and
  destinations cannot be overridden through this helper.

- `prepare(service = NULL)`: retry acquisition for selected services
  without running a data query. Returns invisibly whether acquisition
  succeeded.

- `login()`, `logout()`, `reauthorize()`: explicit authorization
  actions.

## Details

Each new authorization prefetches the configured resource tokens. One
target's failure leaves other services available. `prepare()` can retry
a transient acquisition failure; interaction/consent failures need
explicit reauthorization or a registration change. `prepare()` and
`reauthorize()` do not run or replay data operations. HTTP calls retain
the package's retry policy: one 401 can trigger forced acquisition and a
retry even when `idempotent = FALSE`. Retrying transient failures
separately requires an idempotent request.

Uses one fixed tenant and session-only retention. OAuth connections stay
in the owning R process. Use `access_token(service, async = TRUE)` with
[shiny::ExtendedTask](https://rdrr.io/pkg/shiny/man/ExtendedTask.html)
with [`mirai::mirai()`](https://mirai.r-lib.org/reference/mirai.html) or
[`promises::future_promise()`](https://rstudio.github.io/promises/reference/future_promise.html);
pass the resolved token and ordinary query inputs to the worker. Fixed
tokens do not refresh in a worker. Acquire a new token for each task
invocation and compare its captured `generation()` before displaying the
result. Logout does not cancel an operation already running in a worker.

Refresh alone does not invalidate generation-dependent queries. Existing
providers and items remain bound to their original authorization and
fail after logout, replacement or session closure. Do not share them
across users or workers.

Keep query results session-local. Store `generation()` beside
event-bound results and compare it reactively before display, so logout
or replacement clears old output. Open DBI connections and Arrow streams
have their own lifetimes: close them explicitly. No logout operation can
undo work already accepted by Fabric. See
[`vignette("shiny-integration")`](https://kennispunttwente.github.io/fabricQueryR/articles/shiny-integration.md)
for a complete app.

## See also

[`fabric_shiny_token_provider()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_token_provider.md),
[`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
