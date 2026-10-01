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
uses the same actions for both sandboxes. Select branch `master`,
`sandbox=shiny`, and `action=start`, or use these commands from the repository:

```sh
# Read the capacity state without changing anything.
gh workflow run fabric-sandbox.yaml --ref master -f sandbox=shiny -f action=status

# Resume F2 for up to one hour and prepare the app's persistent data.
gh workflow run fabric-sandbox.yaml --ref master -f sandbox=shiny -f action=start
```

`start` uses the existing `rpackagecap` F2 and creates a separate workspace,
`fabricqueryr-shiny-dhrkoning`. It seeds the Warehouse, Lakehouse, both DAX models,
Eventhouse/KQL, OneLake files and Delta tables, mirrored table, and GraphQL source
used by the app. The optional SQL Database and Warehouse snapshot are not
included. Choose `sandbox=development` for the full development fixtures in
`fabricqueryr-dev-dhrkoning`, using exactly the same start/status/pause actions
and one-hour shutdown. Both share F2; an active session must finish or be paused
before starting the other sandbox or the integration suite.

Wait for the workflow to finish successfully, then launch from R:

```r
Sys.setenv(FABRIC_SHINY_WORKSPACE = "fabricqueryr-shiny-dhrkoning")
shiny::runApp("playground/shiny", port = 8100)
```

Later starts reuse the workspace and its sample data. To reset the sample data,
select `reseed` in the workflow form or add `-f reseed=true` to `start`.
An interrupted first setup can be retried with another `start`.

### Automatic pause after one hour

Every `start` arms a separate shutdown workflow before it can resume F2.
The deadline is one hour from arming, including setup time, and appears in the
start run's summary with a link to its shutdown run. If shutdown cannot be armed,
the start fails without resuming F2. This also works from a feature branch and
does not depend on a scheduled workflow on the default branch.

The shutdown run continues after the setup run finishes or is cancelled. At its
deadline, it signs in again and pauses F2, retaining the workspace and its data.
Running `start` again while F2 is active keeps the original deadline. Once
F2 has paused, another start opens a new one-hour session. An old shutdown run
checks the session identifier before acting, so it does not pause a newer session.

The first setup may use a substantial part of the hour. The deadline still
applies during provisioning; if it interrupts setup, the next start retries the
incomplete fixtures. To finish earlier, pause manually:

```sh
gh workflow run fabric-sandbox.yaml --ref master -f sandbox=shiny -f action=pause
```

Starting F2 resumes capacity billing. Pausing affects every workspace assigned
to that capacity, including any outside this playground. See Microsoft's
[pause and resume documentation](https://learn.microsoft.com/en-us/fabric/enterprise/pause-resume)
for billing behavior. Failed/cancelled setup still attempts an immediate pause
of its session, including an already active session reused by the start. Cancelling its shutdown run also attempts an immediate
pause. Transient Azure HTTP errors are retried. GitHub runner loss, forced
cancellation or Azure outages can prevent shutdown, so this is an automatic
cost guard rather than a guaranteed billing cap. In those cases, check
`status` and use `pause` or the Azure portal if necessary.

`check-shutdown` exercises the same independent timer with a two-minute
deadline on an already paused F2. It never requests resume and refuses to run
on an active capacity. The internal timer now has its own **Fabric capacity
shutdown (internal)** workflow, dispatched automatically. The sandbox form only
asks for the sandbox, action, and optional reseed.

### Periodic backup check

The separate [Pause expired Fabric F2 workflow](https://github.com/KennispuntTwente/fabricQueryR/actions/workflows/shiny-capacity-watchdog.yaml)
checks the existing capacity hourly, at minute 3. It is installed on `master`,
where GitHub runs scheduled workflows, and can also be run manually. Its queue
is independent of the setup and one-hour shutdown jobs.

If F2 is active after this repository's recorded session deadline, the check pauses
it. It leaves paused capacities and sessions within their deadline alone. Invalid
shutdown metadata on a capacity tagged for this repository also triggers a pause;
an active capacity without that ownership tag reports an error for manual review.
The check rereads the session before pausing, retries transient request failures,
and never resumes capacity or changes workspace data.

This supplements the one-hour shutdown job. GitHub can delay scheduled runs and
automatically disables schedules in public repositories after 60 days without
repository activity, so the interval is not a strict maximum delay. See
[GitHub's schedule behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

### GitHub configuration

These actions use the existing `fabric-integration` environment and Azure OIDC
login. `FABRIC_CAPACITY_ID__PAID` identifies the Fabric F2 capacity;
`AZURE_SUBSCRIPTION_ID`, `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` configure Azure
access. The Azure resource defaults to resource group `fabric-rg` and capacity
`rpackagecap`; override them with `FABRIC_F2_RESOURCE_GROUP` and `FABRIC_F2_NAME`
if needed. The workflow checks that the Azure resource and Fabric ID refer to
the same F2 before starting it.

The CI identity needs Azure permission to read, resume and suspend this capacity,
and merge its shutdown tags (`Microsoft.Resources/tags/write`).
The startup job uses GitHub `actions: write` to dispatch the independent shutdown;
no personal access token is required. It waits until that run has verified the
lease and started its timer before resuming F2. The identity also needs
the existing Fabric permissions to assign a workspace and create/seed its
items. It grants the configured playground owner workspace Admin access. The
app still uses the connection settings described under Setup; starting the
workflow does not configure a delegated Web registration or host the Shiny app.

For one-time Azure administrator setup, the repository includes a narrow
[capacity role](../../infra/fabric/shiny-capacity-role.json) and a setup script:

```powershell
# Preview the role and target first, then apply it to the CI identity.
./tools/fabric-sandbox/configure-shiny-capacity-role.ps1
./tools/fabric-sandbox/configure-shiny-capacity-role.ps1 -Apply
```

The script reads the CI identity from the GitHub environment and assigns the
role on the named F2 only. It permits reading, resuming, suspending and tagging
that resource; it does not grant capacity creation, resizing, deletion or access
management. Running the script does not start F2.

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
