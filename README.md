# 'fabricQueryR'

<!-- badges: start -->
[![R-CMD-check](https://github.com/kennispunttwente/fabricQueryR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/kennispunttwente/fabricQueryR/actions/workflows/R-CMD-check.yaml)
[![CRAN status](https://www.r-pkg.org/badges/version/fabricQueryR)](https://CRAN.R-project.org/package=fabricQueryR)
<!-- badges: end -->

'fabricQueryR' helps you work with Microsoft Fabric directly from R.

You can use it to find Fabric workspaces and data items, query Fabric data
interfaces (SQL, DAX, KQL, and GraphQL), work with OneLake files and tables,
run Spark code, and start or monitor Fabric jobs.

## Installation

Install the latest CRAN release (currently 0.2.1) for the stable core API:

``` r
install.packages("fabricQueryR")
```

That release exports `fabric_sql_connect()`, `fabric_sql_query()`,
`fabric_pbi_dax_query()`, `fabric_onelake_read_delta_table()`, and
`fabric_livy_query()`. The broader discovery, management, write, reusable
session, and scheduling interfaces shown in this README are part of the
0.2.1.9000 development version. Install it from GitHub to run all examples
below:

``` r
if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes")
}

remotes::install_github("kennispunttwente/fabricQueryR")
```

## Getting started

To connect to Microsoft Fabric, set your Microsoft Entra tenant ID and sign in.
You can also set a client ID if your tenant does not permit the package's
default public Azure CLI client.

``` r
library(fabricQueryR)

Sys.setenv(FABRICQUERYR_TENANT_ID = "your-tenant-id")

# Optional, if your tenant does not permit the public Azure CLI client ID:
# Sys.setenv(FABRICQUERYR_CLIENT_ID = "your-app-client-id")
```

The [authentication vignette](https://kennispunttwente.github.io/fabricQueryR/articles/authentication.html)
covers interactive sign-in, app registrations, service principals, and other
authentication options.

The examples below focus on the main use of each function group. See the
[function reference](https://kennispunttwente.github.io/fabricQueryR/reference/index.html)
and vignettes for configuration options and more involved workflows.

## What you can do

### 1. Discover workspaces and items

Find the Fabric resources you can access. Discovery returns R6 objects with
methods matched to each actionable item type. In the example below,
`$lakehouses()` is the object interface to `fabric_lakehouses()`, and
`$read_table()` calls `fabric_lakehouse_read_table()`.

``` r
# Find a workspace & lakehouse, and then read a table
workspace <- fabric_workspaces()[[1L]]
lakehouse <- workspace$lakehouses()[[1L]]
orders <- lakehouse$read_table("orders", limit = 1000)

# Read service fields directly
lakehouse$id
lakehouse$displayName

# Equivalent plain-record interface: as.list(lakehouse)
lakehouse_record <- lakehouse$as_list()
```

Search the preview OneLake catalog when discovery must span all visible
workspaces:

``` r
sales_items <- fabric_catalog_search(
  search = "sales",
  types = c("Lakehouse", "Warehouse")
)
```

### 2. Query Fabric SQL endpoints

Open a reusable 'DBI' connection with `$sql_connect()`
(`fabric_sql_connect()`), or run a single query with `$sql_query()`
(`fabric_sql_query()`). These interfaces work with a Warehouse, SQL Database,
or Lakehouse SQL analytics endpoint.

``` r
lakehouse <- workspace$lakehouses()[[1L]]

con <- lakehouse$sql_connect()
DBI::dbListTables(con)
DBI::dbDisconnect(con)

customers <- lakehouse$sql_query(
  "SELECT * FROM dbo.Customers WHERE region = 'West'"
)
```

`$sql_connect()` (`fabric_sql_connect()`) supports both ODBC and ADBC. The
default ODBC backend requires
[Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### 3. Query a semantic model with DAX

Run a DAX query with `$dax_query()` (`fabric_pbi_dax_query()`) and return the
result as a tibble. `$semantic_models()` is the workspace method for
`fabric_semantic_models()`.

``` r
semantic_model <- workspace$semantic_models()[[1L]]

customers <- semantic_model$dax_query(
  dax = "EVALUATE TOPN(1000, 'Customers')"
)
```

The function also supports Arrow streaming for modern semantic models on
Premium or Fabric capacity.

### 4. Run Spark code through Livy

Run PySpark, Scala, Spark SQL, or SparkR remotely with `$livy_query()`
(`fabric_livy_query()`) and return the result to the local R session.
`$lakehouses()` is the workspace method for `fabric_lakehouses()`.

``` r
lakehouse <- workspace$lakehouses()[[1L]]

result <- lakehouse$livy_query(
  kind = "pyspark",
  code = "print(1 + 2)"
)
```

Reusable Livy sessions and independent batch submissions are available for
multi-step and application-file workflows. See
[Working with Livy (Spark)](https://kennispunttwente.github.io/fabricQueryR/articles/spark-with-livy.html)
for choosing between one-off queries, reusable sessions, and batch jobs.

### 5. Read and write Lakehouse tables

Move data between R and managed Delta tables with `$read_table()`
(`fabric_lakehouse_read_table()`) and `$write_table()`
(`fabric_lakehouse_write_table()`). The writer accepts data frames as well as
lazy Arrow sources; `$lakehouses()` corresponds to `fabric_lakehouses()`.

``` r
lakehouse <- workspace$lakehouses()[[1L]]
orders <- lakehouse$read_table("orders")

lakehouse$write_table(
  table = "orders_from_r",
  data = orders
)
```

Use `$load_table()` (`fabric_lakehouse_load_table()`) when the source CSV or
Parquet data already exists under `Files/` in the same Lakehouse. See
[Working with Fabric Lakehouses and OneLake](https://kennispunttwente.github.io/fabricQueryR/articles/onelake-and-lakehouse.html)
for the distinction between ordinary files and managed Delta tables, table
loading, larger reads, and shortcuts.

When only metadata existence is needed, use
`fabric_onelake_schema_exists()` or `fabric_onelake_table_exists()` with either
the Delta or Iceberg table protocol.

### 6. Work with OneLake files

Read and write common file formats directly between R and OneLake with
`$onelake_read_file()` (`fabric_onelake_read_file()`) and
`$onelake_write_file()` (`fabric_onelake_write_file()`). Lakehouse file paths
normally start with `Files/`; `$lakehouses()` corresponds to
`fabric_lakehouses()`.

``` r
lakehouse <- workspace$lakehouses()[[1L]]

lakehouse$onelake_write_file(
  path = "Files/exports/orders.parquet",
  data = data.frame(id = 1:3, amount = c(10, 20, 30))
)

orders <- lakehouse$onelake_read_file(
  path = "Files/exports/orders.parquet"
)
```

The same function group also lists, inspects, downloads, uploads, and deletes
OneLake files. The
[Fabric Lakehouses and OneLake vignette](https://kennispunttwente.github.io/fabricQueryR/articles/onelake-and-lakehouse.html)
continues with file discovery, uploads, managed tables, and safe deletion.

### 7. Read and write Warehouse tables

Read a Warehouse table with `$read_table()`
(`fabric_warehouse_read_table()`) or load an R or Arrow object with
`$write_table()` (`fabric_warehouse_write_table()`). Writes use a Lakehouse as
temporary OneLake staging for Fabric's `COPY INTO`. `$warehouses()` and
`$lakehouses()` correspond to `fabric_warehouses()` and `fabric_lakehouses()`.

``` r
warehouse <- workspace$warehouses()[[1L]]
lakehouse <- workspace$lakehouses()[[1L]]
orders <- warehouse$read_table("orders")

warehouse$write_table(
  table = "orders_copy",
  data = orders,
  staging_lakehouse = lakehouse,
  create_if_missing = TRUE
)
```

See
[Working with Fabric Warehouses](https://kennispunttwente.github.io/fabricQueryR/articles/warehouse.html)
for staging, table creation, overwrite behavior, schema matching, and large
Arrow inputs.

### 8. Query and write Eventhouse data

Use `$query()` (`fabric_kql_query()`) to query an Eventhouse database, or
`$write_table()` (`fabric_kql_write_table()`) to write an R or Arrow object to
an existing KQL table. `$kql_databases()` corresponds to
`fabric_kql_databases()`.

``` r
kql_database <- workspace$kql_databases()[[1L]]

events <- kql_database$query(
  "Events | where EventType == 'Warning' | take 100"
)

kql_database$write_table(
  table = "EventsCopy",
  data = events,
  create_if_missing = TRUE
)
```

For tracked ingestion from existing storage files and server-side export to
OneLake, see
[Working with Fabric Eventhouses (real-time data)](https://kennispunttwente.github.io/fabricQueryR/articles/eventhouse-ingestion.html).

### 9. Query a Fabric GraphQL API

Call an API for GraphQL item with `$query()` (`fabric_graphql_query()`). Data
and GraphQL-level errors remain separately available in the result;
`$graphql_apis()` corresponds to `fabric_graphql_apis()`.

``` r
graphql_api <- workspace$graphql_apis()[[1L]]

result <- graphql_api$query(
  query = "{ customers { items { id name region } } }"
)

result$data$customers$items
```

[Working with GraphQL](https://kennispunttwente.github.io/fabricQueryR/articles/graphql-schema-and-rows.html)
covers schema inspection, cursor pagination, and row collection.

### 10. Invoke a User Data Function

Call published Fabric business logic through its public function URL and
inspect the structured result.

``` r
result <- fabric_function_invoke(
  Sys.getenv("FABRIC_FUNCTION_URL"),
  parameters = list(customerName = "Ada", orderId = 42L)
)

result$output
```

Enable Public access in Run only mode and copy the URL from the function's
properties. See the [User Data Functions vignette](https://kennispunttwente.github.io/fabricQueryR/articles/user-data-functions.html)
for permissions, limits, and retry behavior.

### 11. Refresh a semantic model

Start a semantic-model refresh with `$refresh()` (`fabric_pbi_refresh()`) and
wait with `$refresh_wait()` (`fabric_pbi_refresh_wait()`). The returned object
includes the details needed to inspect failures; `$semantic_models()`
corresponds to `fabric_semantic_models()`.

``` r
semantic_model <- workspace$semantic_models()[[1L]]
refresh <- semantic_model$refresh()
completed <- semantic_model$refresh_wait(refresh, timeout = 1800)

completed$state
```

See
[Working with Semantic Models (DAX queries)](https://kennispunttwente.github.io/fabricQueryR/articles/semantic-model-refresh.html)
for DAX queries, enhanced refresh, cancellation, capacity limits, and Direct
Lake behavior.

### 12. Run and monitor Fabric jobs

Start a notebook, data pipeline, or Spark job definition with `$run()`
(`fabric_job_run()`) and wait with `$wait()` (`fabric_job_wait()`).
`$notebooks()` corresponds to `fabric_notebooks()`.

``` r
notebook <- workspace$notebooks()[[1L]]

job <- notebook$run(
  parameters = list(mode = "incremental")
)

result <- notebook$wait(job, timeout = 900)
result$status
```

The [job automation vignette](https://kennispunttwente.github.io/fabricQueryR/articles/job-automation.html)
covers run history, cancellation, and recurring schedules.

### 13. Create OneLake shortcuts

Create a shortcut with `$shortcut_create()`
(`fabric_onelake_shortcut_create()`) when data in another Fabric item should be
available without being copied into the current Lakehouse.

``` r
lakehouses <- workspace$lakehouses()
lakehouse <- lakehouses[[1L]]
source_lakehouse <- lakehouses[[2L]]

lakehouse$shortcut_create(
  path = "Files",
  name = "shared-orders",
  target = source_lakehouse,
  target_path = "Tables/orders"
)
```

### 14. Resume a long-running Fabric operation

Resume an asynchronous Fabric operation from its ID or `Location` URL, wait
for completion, and retrieve its result.

``` r
operation <- fabric_operation_status(
  "00000000-0000-0000-0000-000000000000"
)
operation <- fabric_operation_wait(operation, timeout = 900)
result <- fabric_operation_result(operation)
```

## Background

Microsoft Fabric brings data engineering, data warehousing, real-time
analytics, and Power BI together in one platform, with OneLake as its shared
storage layer.

When our organization started using Fabric, accessing its data from R was not
yet straightforward. This package grew from the helper functions created to
make that work easier and more consistent for other R users.

### About the maintainer

I (Luka Koning) am no longer associated with Kennispunt Twente.
I maintain this open-source R package in my personal capacity.
The repository remains under the Kennispunt Twente GitHub organization for
historical reasons, and because the early development was done while I was
affiliated with them.
