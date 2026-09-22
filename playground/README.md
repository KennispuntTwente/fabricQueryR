# fabricQueryR playground

These scripts connect to the persistent workspace created by the **Manage
persistent Fabric sandbox** GitHub Actions workflow. The workspace is seeded
with the same deterministic resources and data as the live integration suite,
so examples do not need copied item IDs, SQL endpoints, or connection strings.

## Start a session

Wait until the workflow finishes **seeding and discovery**, then start R from
the repository root. A rebuild replaces the workspace: reconnect after it
finishes, because objects from an earlier R session contain the old item IDs.

Open [`tour.R`](tour.R) and run one section at a time for a guided introduction.
It uses the package functions and R6 methods directly, shows expected fixture
values, and includes SQL parameters, precision choices, DBI connections, and
Arrow batch processing. Its write and compute examples are in `if (FALSE)`
blocks; select the calls inside a block when you want to try them.

To load just the reusable demos:

```r
source("playground/sandbox.R")
source("playground/playground.R")

sandbox <- connect_playground_sandbox()
```

The helper loads the current checkout with `devtools::load_all()`. When
`FABRICQUERYR_TENANT_ID`, `FABRICQUERYR_CLIENT_ID`, and
`FABRICQUERYR_CLIENT_SECRET` are set, it uses that application identity.
Otherwise it reuses cached `AzureAuth` tokens or prompts for interactive login,
verifies the configured sandbox identity and workspace ownership marker, and
discovers seeded items. It acquires Fabric, Power BI, SQL, OneLake, and Kusto
tokens and exposes a refreshable provider.

Set `FABRICQUERYR_TENANT_ID` when more than one tenant is present in the local
`AzureAuth` cache. A terminal without a browser can request device-code login:

```r
sandbox <- connect_playground_sandbox(
  client_id = "04b07795-8ddb-461a-bbee-02f9e1bf7b46", # Azure CLI public client
  client_secret = "",
  auth_args = list(auth_type = "device_code")
)
```

Use a public client allowed by your tenant for device-code login; an application
ID configured only for client-secret authentication cannot use that flow.

## Try a feature

Each function returns its results for inspection. Run one at a time:

```r
discovery <- demo_discovery(sandbox)
discovery$inventory

sql <- demo_sql(sandbox)
sql$warehouse$rows

onelake <- demo_onelake(sandbox)
onelake$delta

kql <- demo_kql(sandbox)
kql$selected

graphql <- demo_graphql(sandbox)
graphql$rows

models <- demo_power_bi(sandbox)
models$json
```

The SQL demo defaults to ODBC, which warns once per R session that driver
conversion may lose numeric precision. ADBC's default preserves decimals as
strings and large integers exactly. Explicit `numeric_policy = "driver"`
accepts driver conversion without a warning; `"exact"` rejects unsafe numeric
ODBC results. The optional SQL Database is skipped when deployment omits it
because of the capacity's database limit.

```r
demo_sql(sandbox, targets = "warehouse")
demo_sql(sandbox, backend = "adbc", targets = "warehouse")
demo_arrow_batches(sandbox) # 3 rows, amount_sum = 30.5
```

Reuse a DBI connection for ad hoc SQL, with cleanup even if the query fails:

```r
rows <- local({
  con <- open_playground_sql_connection(sandbox, target = "warehouse")
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbGetQuery(con, "SELECT TOP (3) * FROM dbo.fabricqueryr_sql_types")
})
```

Spark, refreshes, and jobs take longer and can consume Fabric capacity:

```r
spark <- demo_livy(sandbox)
refresh <- demo_power_bi_refresh(sandbox)
job <- run_playground_job(sandbox, target = "pipeline")
history <- demo_job_history(sandbox)
```

The refresh demo uses `FabricQueryRArrowIntegrationModel`, the Import model.
`FabricQueryRIntegrationModel` is a Push model used for JSON DAX reads and
cannot be refreshed this way. Job history only reads metadata; it starts no job.

## Try writes safely

The following demos use isolated names. OneLake files, shortcuts, and Warehouse
tables are removed automatically before the function returns, including after
most errors:

```r
file_round_trip <- demo_onelake_write(sandbox)
shortcut_round_trip <- demo_onelake_shortcut(sandbox)
warehouse_round_trip <- demo_warehouse_write(sandbox)
```

The Lakehouse writer is different because the package deliberately has no
table-deletion helper. This call creates or replaces the dedicated
`fabricqueryr_playground_orders` table and leaves it available for later SQL,
Delta, and Livy experiments:

```r
lakehouse_write <- write_playground_lakehouse_table(sandbox)
lakehouse_write$rows
```

The optional KQL ingestion snippet in `tour.R` similarly retains
`fabricqueryr_playground_events`. Its stable ingestion key is intended for
retrying the same example batch. The KQL export snippet removes its temporary
OneLake directory after reading the Parquet files.

Run the persistent workflow with `action = rebuild` to reset all resources and
seed data from the repository.

## Coverage and further experiments

| Area | Interactive entry point |
| --- | --- |
| Workspace/item discovery and R6 objects | `demo_discovery()`, tour section 1 |
| SQL tables, views, queries, parameters, snapshots, precision, DBI | `demo_sql()`, tour sections 2-3 |
| Arrow streams with cleanup | `demo_arrow_batches()`, tour section 4 |
| OneLake files, Delta tables, mirrored tables | `demo_onelake()`, tour section 5 |
| Typed KQL queries and parameters | `demo_kql()`, tour section 6 |
| GraphQL pagination, collection, introspection | `demo_graphql()`, tour section 6 |
| JSON/Arrow DAX and Import-model refresh | `demo_power_bi()`, `demo_power_bi_refresh()` |
| File, shortcut, Warehouse, and Lakehouse writes | Tour section 8 |
| KQL data-frame ingestion and Parquet export to OneLake | Optional blocks in tour section 8 |
| Job submission, polling, cancellation, schedules, Spark SQL | Tour section 9 |
| Catalog search | Optional block after tour section 7; needs `Catalog.Read.All` |

The tour covers the main workflows, rather than every exported operation.
For further KQL ingestion/export options and GraphQL mutations, see
[`eventhouse-ingestion.Rmd`](../vignettes/eventhouse-ingestion.Rmd),
[`graphql-schema-and-rows.Rmd`](../vignettes/graphql-schema-and-rows.Rmd), and the focused examples in
[`test-integration-fabric-kql-graphql.R`](../tests/testthat/test-integration-fabric-kql-graphql.R).
Job schedule creation/update/deletion and reusable Livy sessions/batches have
longer examples in the [vignettes](../vignettes).

The persistent workflow provisions the items used here. User Data Function
invocation needs separately published functions: configure the
`FABRIC_TEST_FUNCTION_*_URL` variables and follow
[`test-integration-fabric-functions.R`](../tests/testthat/test-integration-fabric-functions.R).
It is not part of the default playground run.

## Local dependencies

Discovery needs `devtools`, `AzureAuth`, and `jsonlite`. Individual examples
also use the dependencies of their package feature. In particular:

- SQL with ODBC needs `DBI`, `odbc`, and Microsoft ODBC Driver 18 for SQL Server
- SQL with ADBC needs `adbi`, `adbcdrivermanager`, and the `mssql` ADBC driver
- Arrow examples and OneLake object reads need `arrow` and `nanoarrow`
- Direct Delta reads also need `reticulate` and its managed `deltalake`/Python
  `nanoarrow` environment; the first read can install it. SQL reads do not use it.

No IDs or tokens need to be pasted into examples. After a failed deployment,
inspect its failing step before trying these demos; an existing workspace alone
does not mean its fixtures are ready.

The provisioning and local dependency details remain in
[`infra/fabric/README.md`](../infra/fabric/README.md).
