# Fabric + Shiny playground

Run this from an R session in the repository root:

```r
shiny::runApp("playground/shiny", port = 8100)
```

The app connects to the persistent `fabricqueryr-dev-dhrkoning` sandbox and
discovers its current items and endpoints. It loads this checkout in both the
Shiny process and its query workers, so you can try changes without reinstalling
fabricQueryR. No copied workspace IDs or connection strings are needed.

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
