# fabricQueryR playground

These scripts connect to the persistent workspace created by the **Manage
persistent Fabric sandbox** GitHub Actions workflow. The workspace is seeded
with the same deterministic resources and data as the live integration suite,
so examples do not need copied item IDs, SQL endpoints, or connection strings.

## Start a session

Run the workflow with `action = rebuild` once, then start R from the repository
root:

```r
source("playground/sandbox.R")
source("playground/playground.R")

sandbox <- connect_playground_sandbox()
```

The connection helper reuses cached `AzureAuth` tokens where possible, prompts
for interactive sign-in when needed, verifies the configured sandbox admin,
and discovers every expected seeded item. It acquires Fabric, Power BI, SQL,
OneLake, and Kusto tokens and exposes a refreshable provider instead of raw
token strings.

Set `FABRICQUERYR_TENANT_ID` when more than one tenant is present in the local
`AzureAuth` cache. A terminal without a browser can request device-code login:

```r
sandbox <- connect_playground_sandbox(
  auth_args = list(auth_type = "device_code")
)
```

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

The SQL demo defaults to ODBC. Pass `backend = "adbc"` to compare the ADBC
path. To keep a connection open for ad hoc DBI calls, disconnect it when done:

```r
con <- open_playground_sql_connection(sandbox, target = "warehouse")
DBI::dbGetQuery(con, "SELECT TOP (3) * FROM dbo.fabricqueryr_sql_types")
DBI::dbDisconnect(con)
```

Spark, refreshes, and jobs take longer and can consume Fabric capacity:

```r
spark <- demo_livy(sandbox)
refresh <- demo_power_bi_refresh(sandbox)
job <- run_playground_job(sandbox, target = "pipeline")
history <- demo_job_history(sandbox)
```

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

Run the persistent workflow with `action = rebuild` to reset all resources and
seed data from the repository.

## Sandbox scope

The existing persistent workflow already provisions every Fabric item used by
these demos, so the playground does not add more capacity resources. User Data
Function invocation remains the one externally published fixture: configure
the `FABRIC_TEST_FUNCTION_*_URL` variables and use the local integration runner
when that optional feature needs live coverage.

## Local dependencies

Discovery needs `devtools`, `AzureAuth`, and `jsonlite`. Individual examples
also use the dependencies of their package feature. In particular:

- SQL with ODBC needs `DBI`, `odbc`, and Microsoft ODBC Driver 18 for SQL Server
- SQL with ADBC needs `adbi`, `adbcdrivermanager`, and the `mssql` ADBC driver
- OneLake object and table examples need `arrow`, `nanoarrow`, and the package's
  configured Delta reader environment

The provisioning and local dependency details remain in
[`infra/fabric/README.md`](../infra/fabric/README.md).
