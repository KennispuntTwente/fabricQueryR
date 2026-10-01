# Fabric + Shiny playground

Run this from an R session in the repository root:

```r
shiny::runApp("playground/shiny", port = 8100)
```

The app connects to the persistent `fabricqueryr-dev-dhrkoning` sandbox and
discovers its current items and endpoints. It loads this checkout in both the
Shiny process and its query workers, so you can try changes without reinstalling
fabricQueryR. No copied workspace IDs or connection strings are needed.
Set `FABRIC_SHINY_WORKSPACE` to use the dedicated Shiny sandbox described below.

## Setup

Use the same connection settings as the [R playground](../README.md):
`FABRICQUERYR_TENANT_ID`, `FABRICQUERYR_CLIENT_ID` and
`FABRICQUERYR_CLIENT_SECRET`. With these settings the app launches using the
sandbox application's Fabric permissions. The sidebar identifies this as a
shared sandbox connection.

The app needs Shiny >= 1.8.1, bslib >= 0.7.0, promises >= 1.3.0, devtools and
AzureAuth. Install mirai for background queries; if it is absent, the app uses
future and `promises::future_promise()` instead. Queries use `ExtendedTask`, so
you can interact with the page while Fabric is working.

```r
install.packages(c("shiny", "bslib", "promises", "mirai", "devtools", "AzureAuth"))
```

Each data source also needs its usual fabricQueryR dependencies: for example,
SQL through ODBC needs the odbc package and Microsoft ODBC Driver 18 for SQL
Server. The alternative ADBC selector uses the installed MSSQL ADBC driver.
Delta reads use the package's Python deltalake reader. A missing driver or
unavailable Fabric service appears as an error for that source.

## Start the dedicated Shiny sandbox

The [Manage persistent Fabric sandbox workflow](https://github.com/KennispuntTwente/fabricQueryR/actions/workflows/fabric-sandbox.yaml)
has three manual actions for the app. Select branch `shiny-integration` when
running the workflow, or use these commands from the repository:

```sh
# Read the capacity state without changing anything.
gh workflow run fabric-sandbox.yaml --ref shiny-integration -f action=shiny-status

# Resume the existing paid F2 and prepare the app's persistent data.
gh workflow run fabric-sandbox.yaml --ref shiny-integration -f action=shiny-start
```

`shiny-start` uses the existing `rpackagecap` F2 and creates a separate workspace,
`fabricqueryr-shiny-dhrkoning`. It seeds the Warehouse, Lakehouse, both DAX models,
Eventhouse/KQL, OneLake files and Delta tables, mirrored table, and GraphQL source
used by the app. The optional SQL Database and Warehouse snapshot are not
included. The general `fabricqueryr-dev-dhrkoning` sandbox is managed separately
by the workflow's existing `rebuild` and `teardown` actions.

Wait for the workflow to finish successfully, then launch from R:

```r
Sys.setenv(FABRIC_SHINY_WORKSPACE = "fabricqueryr-shiny-dhrkoning")
shiny::runApp("playground/shiny", port = 8100)
```

Later starts reuse the workspace and its sample data. To reset the sample data,
select `reseed` in the workflow form or add `-f reseed=true` to `shiny-start`.
An interrupted first setup can be retried with another `shiny-start`.

The F2 remains active after a successful start; there is no automatic shutdown.
When finished, pause it while keeping the workspace and its data:

```sh
gh workflow run fabric-sandbox.yaml --ref shiny-integration -f action=shiny-pause
```

Starting F2 resumes capacity billing. Pausing affects every workspace assigned
to that capacity, including any outside this playground. See Microsoft's
[pause and resume documentation](https://learn.microsoft.com/en-us/fabric/enterprise/pause-resume)
for billing behavior. If setup fails or is cancelled, the workflow attempts to
pause a capacity that it resumed itself. After a runner interruption, use
`shiny-status` and, if needed, `shiny-pause` to confirm the final state.

### GitHub configuration

These actions use the existing `fabric-integration` environment and Azure OIDC
login. `FABRIC_CAPACITY_ID__PAID` identifies the Fabric F2 capacity;
`AZURE_SUBSCRIPTION_ID`, `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` configure Azure
access. The Azure resource defaults to resource group `fabric-rg` and capacity
`rpackagecap`; override them with `FABRIC_F2_RESOURCE_GROUP` and `FABRIC_F2_NAME`
if needed. The workflow checks that the Azure resource and Fabric ID refer to
the same F2 before starting it.

The CI identity needs Azure permission to read, resume and suspend this capacity,
plus the existing Fabric permissions to assign a workspace and create/seed its
items. It grants the configured playground owner workspace Admin access. The
app still uses the connection settings described under Setup; starting the
workflow does not configure a delegated Web registration or host the Shiny app.

## Try the data sources

Choose a source and click Run selected source, or use Run available
sources to query every discovered target. The latter uses the current SQL
driver, minimum ID and category controls. Open Run results for a summary,
then select a source to inspect its data. One failed source does not discard
the other results.

| Source | What runs | Expected unfiltered result |
| --- | --- | --- |
| Warehouse / SQL | Parameterized query on `dbo.fabricqueryr_sql_types` | IDs 1-3; amount total 30.5 |
| Lakehouse / SQL | Parameterized query on `dbo.fabricqueryr_basic` | IDs 1-3; amount total 30.5 |
| Semantic model / DAX | `SUMMARIZECOLUMNS` over the Push model's `Facts` table | A: 2 rows, 10.5; B: 1 row, 20 |
| Semantic model / DAX / Arrow | The same DAX on the Import model, with Arrow transport | Same category totals |
| Eventhouse / KQL | Parameterized query on `fabricqueryr_events` | Three rows, or the selected category |
| OneLake / CSV | `Files/fixtures/basic.csv` | alpha, beta and gamma |
| OneLake / Delta | `dbo.fabricqueryr_basic` directly from OneLake | Three rows |
| Mirrored database / Delta | `dbo.fabricqueryr_mirror_types` directly from OneLake | Three rows |
| GraphQL / paginated | `fabricqueryr_basics`, two rows per page | Three rows across two pages |
| Warehouse snapshot / SQL | Query on the seeded Warehouse snapshot | Three rows |
| SQL Database / SQL | Query on the optional SQL Database | Three rows, when provisioned |
| Workspace / discovery | List items using the current query identity | Items that identity can see |

SQL comes first in the source selector. Change Minimum ID to see bound
SQL parameters in action. For DAX, change Category and run again to update
the chart: calculations run in the semantic model, just as they can for a Shiny
dashboard built on a Power BI model. KQL uses the same category control.

The Query tab shows the query text or package call. Connection lists
the discovered fixtures and, in delegated mode, each service's token status.
Check responsiveness increments a counter even while a query is running.
Changing query controls clears results that no longer match those controls.

The app only reads fixtures. It does not create workspaces, resume capacities,
seed data or start Spark jobs. If a fixture is missing, its source is labelled
in the selector; other sources remain usable. Once the sandbox fixtures are
restored, restart the app to discover them. The existing
[tour](../tour.R) contains Spark, jobs and write examples.

## Try delegated access

Delegated mode uses the signed-in visitor's Fabric permissions for every data
query. Fabric then determines which items and rows that visitor can access.
The shared sandbox connection is still used once at startup to discover fixture
addresses; its credentials are not used for delegated queries.

Install the target-enabled sibling shinyOAuth checkout if needed:

```r
devtools::install("../shinyoauth", upgrade = "never")
```

Configure a delegated Entra Web registration as described in the
[Shiny vignette](../../vignettes/shiny-integration.Rmd), including the services
used here: Fabric, SQL, DAX, KQL, OneLake and GraphQL. Then set these values for
your R session and restart the app:

```r
Sys.setenv(
  FABRIC_SHINY_AUTH = "delegated",
  ENTRA_TENANT_ID = "your-tenant-id",
  ENTRA_CLIENT_ID = "your-web-app-client-id",
  ENTRA_CLIENT_SECRET = "your-web-app-client-secret",
  ENTRA_REDIRECT_URI = "http://localhost:8100/"
)
shiny::runApp("playground/shiny", port = 8100)
```

Use Sign in, then query a source. Retry service tokens retries token
preparation; Reauthorize starts a new authorization. Neither button reruns
queries. Sign out clears displayed results, including results arriving from a
previous sign-in. Open separate browser profiles to compare two users' access.

`FABRIC_SHINY_AUTH` defaults to `auto`: it selects delegated mode when any of
the three Entra registration settings is present and requires all three.
Set it to `sandbox` to explicitly use the shared connection. Delegated query
errors never fall back to the sandbox identity.

## Adapt the app

[`app.R`](app.R) connects the sandbox and starts the app.
[`queries.R`](queries.R) contains the source definitions and ordinary package
calls. [`application.R`](application.R) contains the UI, sign-in controls and
background tasks. The worker receives a fixed service token and ordinary query
inputs; the OAuth connection stays in its Shiny session.
