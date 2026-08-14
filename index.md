# fabricQueryR

`fabricQueryR` helps you work with Microsoft Fabric directly from R.

You can use it to find Fabric workspaces and data items, query Fabric
data interfaces (SQL, DAX, KQL, and GraphQL), work with OneLake files
and tables, run Spark code, and start or monitor Fabric jobs.

## Installation

Install the latest release from CRAN:

``` r

install.packages("fabricQueryR")
```

Or install the development version from GitHub:

``` r

if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes")
}

remotes::install_github("kennispunttwente/fabricQueryR")
```

## Getting started

To connect to Microsoft Fabric, set your Microsoft Entra tenant ID and
sign in. The [authentication
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/authentication.html)
covers interactive sign-in, service principals, and other authentication
options.

``` r

library(fabricQueryR)

Sys.setenv(FABRICQUERYR_TENANT_ID = "your-tenant-id")
```

The examples below focus on the main use of each function group. See the
[function
reference](https://kennispunttwente.github.io/fabricQueryR/reference/index.html)
and vignettes for configuration options and more involved workflows.

## What you can do

### 1. Discover workspaces and items

Find the Fabric resources you can access. Typed discovery helpers return
item records that can be passed directly to the package’s query and
storage functions.

``` r

workspace <- fabric_workspaces()[[1L]]
items <- fabric_items(workspace)
lakehouse <- fabric_lakehouses(workspace)[[1L]]
```

Later snippets use names such as `warehouse` and `semantic_model` for
records returned by their matching typed discovery helpers.

### 2. Query Fabric SQL endpoints

Run SQL against a Warehouse, SQL Database, or Lakehouse SQL analytics
endpoint and return the result as a tibble.

``` r

customers <- fabric_sql_query(
  lakehouse,
  "SELECT * FROM dbo.Customers WHERE region = 'West'"
)
```

For longer database workflows,
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
creates a standard DBI connection using either ODBC or ADBC. The default
ODBC backend requires [Microsoft ODBC Driver 18 for SQL
Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### 3. Read and write Lakehouse tables

Move data between R and managed Delta tables in a Lakehouse. The writer
accepts data frames as well as lazy Arrow sources.

``` r

orders <- fabric_lakehouse_read_table(lakehouse, "orders")

fabric_lakehouse_write_table(
  lakehouse,
  table = "orders_from_r",
  data = orders
)
```

Use
[`fabric_lakehouse_load_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
when the source CSV or Parquet data already exists under `Files/` in the
same Lakehouse.

### 4. Work with OneLake files

Read and write common file formats directly between R and OneLake.
Lakehouse file paths normally start with `Files/`.

``` r

fabric_onelake_write_file(
  workspace,
  lakehouse,
  path = "Files/exports/orders.parquet",
  data = orders
)

orders <- fabric_onelake_read_file(
  workspace,
  lakehouse,
  path = "Files/exports/orders.parquet"
)
```

The same function group also lists, inspects, downloads, uploads, and
deletes OneLake files.

### 5. Create OneLake shortcuts

Create a shortcut when data in another Fabric item should be available
without being copied into the current Lakehouse.

``` r

fabric_onelake_shortcut_create(
  lakehouse,
  path = "Files",
  name = "shared-orders",
  target = source_lakehouse,
  target_path = "Tables/orders"
)
```

Deleting a shortcut removes only the link, not the destination data.

### 6. Read and write Warehouse tables

Read a Warehouse table into R or load an R or Arrow object into a
Warehouse. Writes use a Lakehouse as temporary OneLake staging for
Fabric’s `COPY INTO`.

``` r

orders <- fabric_warehouse_read_table(warehouse, "orders")

fabric_warehouse_write_table(
  warehouse,
  table = "orders_copy",
  data = orders,
  staging_lakehouse = lakehouse,
  create_if_missing = TRUE
)
```

See the [Warehouse writing
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/warehouse-write.html)
for overwrite behavior, schema matching, and large Arrow inputs.

### 7. Query and write Eventhouse data

Use KQL to query an Eventhouse database, or write an R or Arrow object
to an existing KQL table.

``` r

events <- fabric_kql_query(
  kql_database,
  "Events | where EventType == 'Warning' | take 100"
)

fabric_kql_write_table(
  kql_database,
  table = "EventsCopy",
  data = events,
  create_if_missing = TRUE
)
```

For tracked ingestion from existing storage files and server-side export
to OneLake, see the [Eventhouse ingestion
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/eventhouse-ingestion.html).

### 8. Query a semantic model with DAX

Run a DAX query against a Fabric or Power BI semantic model and return
the result as a tibble.

``` r

customers <- fabric_pbi_dax_query(
  semantic_model,
  dax = "EVALUATE TOPN(1000, 'Customers')"
)
```

The function also supports Arrow streaming for modern semantic models on
Premium or Fabric capacity.

### 9. Refresh a semantic model

Start a semantic-model refresh and wait for its final state. The
returned object retains the details needed to inspect failures.

``` r

refresh <- fabric_pbi_refresh(semantic_model)
completed <- fabric_pbi_refresh_wait(refresh, timeout = 1800)

completed$state
```

See the [semantic-model refresh
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/semantic-model-refresh.html)
for enhanced refresh, cancellation, capacity limits, and Direct Lake
behavior.

### 10. Query a Fabric GraphQL API

Call an API for GraphQL item configured in Fabric. Data and
GraphQL-level errors remain separately available in the result.

``` r

result <- fabric_graphql_query(
  graphql_api,
  query = "{ customers { items { id name region } } }"
)

result$data$customers$items
```

The [GraphQL
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/graphql-schema-and-rows.html)
covers schema inspection, cursor pagination, and row collection.

### 11. Run Spark code through Livy

Run SparkR, PySpark, Scala, or Spark SQL remotely in Fabric and return
the result to the local R session.

``` r

result <- fabric_livy_query(
  lakehouse,
  kind = "sparkr",
  code = "print(1 + 2)"
)
```

Reusable Livy sessions and independent batch submissions are available
for multi-step and application-file workflows.

### 12. Run and monitor Fabric jobs

Start a notebook, data pipeline, or Spark job definition and wait for it
to finish.

``` r

job <- fabric_job_run(
  notebook,
  parameters = list(mode = "incremental")
)

result <- fabric_job_wait(job, timeout = 900)
result$status
```

The [job automation
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/job-automation.html)
covers run history, cancellation, and recurring schedules.

### 13. Invoke a User Data Function

Call published Fabric business logic through its public function URL and
inspect the structured result.

``` r

result <- fabric_function_invoke(
  Sys.getenv("FABRIC_FUNCTION_URL"),
  parameters = list(customerName = "Ada", orderId = 42L)
)

result$output
```

Enable Public access in Run only mode and copy the URL from the
function’s properties. See the [User Data Functions
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/user-data-functions.html)
for permissions, limits, and retry behavior.

### 14. Resume a long-running Fabric operation

Resume an asynchronous Fabric operation from its ID or `Location` URL,
wait for completion, and retrieve its result.

``` r

operation <- fabric_operation_status(
  "00000000-0000-0000-0000-000000000000"
)
operation <- fabric_operation_wait(operation, timeout = 900)
result <- fabric_operation_result(operation)
```

## Background

Microsoft Fabric brings data engineering, data warehousing, real-time
analytics, and Power BI together in one platform, with OneLake as its
shared storage layer.

When our organization started using Fabric, accessing its data from R
was not yet straightforward. This package grew from the helper functions
created to make that work easier and more consistent for other R users.

### About the maintainer

I (Luka Koning) am no longer associated with Kennispunt Twente. I
maintain this open-source R package in my personal capacity. The
repository remains under the Kennispunt Twente GitHub organization for
historical reasons, and because the early development was done while I
was affiliated with them.
