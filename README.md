# fabricQueryR

<!-- badges: start -->
[![R-CMD-check](https://github.com/kennispunttwente/fabricQueryR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/kennispunttwente/fabricQueryR/actions/workflows/R-CMD-check.yaml)
[![CRAN status](https://www.r-pkg.org/badges/version/fabricQueryR)](https://CRAN.R-project.org/package=fabricQueryR)
<!-- badges: end -->

`fabricQueryR` helps you work with Microsoft Fabric directly from R. 

You can use it to find Fabric workspaces and data items, query various Fabric data interfaces (SQL, DAX, KQL, GraphQL),
work with OneLake files and tables, run Spark code, and start or monitor Fabric jobs.

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

Below is a minimal example of how to find a workspace and some of its items. 

To connect to Microsoft Fabric, you need a Microsoft Entra tenant ID and optionally a client ID.
See the [authentication vignette](https://kennispunttwente.github.io/fabricQueryR/articles/authentication.html)
for more details on how to set these environment variables and sign in to Microsoft Fabric from R.

Once you know these values, you can discover workspaces and items like so:

``` r
library(fabricQueryR)
library(purrr)

# Always set your tenant ID:
Sys.setenv(FABRICQUERYR_TENANT_ID = "your-tenant-id")

# Optional, if your tenant does not permit the public Azure CLI client ID:
# Sys.setenv(FABRICQUERYR_CLIENT_ID = "your-app-client-id")

# Find all workspaces and select one by name
workspaces <- fabric_workspaces()
workspace <- workspaces |>
  keep(~ .x$displayName == "ExampleWorkspace") |>
  first()

# Find all items in the workspace
items <- fabric_items(workspace)

# Find the first item of each type
lakehouse <- fabric_lakehouses(workspace) |> first()
warehouse <- fabric_warehouses(workspace) |> first()
warehouse_snapshot <- fabric_warehouse_snapshots(workspace) |> first()
sql_database <- fabric_sql_databases(workspace) |> first()
semantic_model <- fabric_semantic_models(workspace) |> first()
kql_database <- fabric_kql_databases(workspace) |> first()
graphql_api <- fabric_graphql_apis(workspace) |> first()
notebook <- fabric_notebooks(workspace) |> first()
pipeline <- fabric_data_pipelines(workspace) |> first()
spark_job <- fabric_spark_job_definitions(workspace) |> first()
environments <- fabric_environments(workspace)
user_data_functions <- fabric_user_data_functions(workspace)
```

In the next sections, you can see how to use these items to query data, run Spark code, and more.

Also see the [reference](https://kennispunttwente.github.io/fabricQueryR/reference/index.html)
for full function documentation and more examples.

## What you can do

### 1. Connect to Fabric SQL

Connect to a Warehouse, SQL Database, or a Lakehouse's SQL analytics endpoint. 
The result is a standard DBI connection, 
so it works with familiar R database packages such as DBI and dbplyr.

``` r
# Set up a DBI connection to a Lakehouse SQL endpoint
con <- fabric_sql_connect(lakehouse)
# List the tables
DBI::dbListTables(con)
# Run a SQL query and return the result as a tibble
df_sql <- DBI::dbGetQuery(
  con, 
  "SELECT * FROM dbo.Customers WHERE region = 'West'"
)
# Close the connection when done
DBI::dbDisconnect(con)

# Or, run a single SQL query directly (without a connection object):
df_sql <- fabric_sql_query(
  lakehouse,
  "SELECT * FROM dbo.Customers WHERE region = 'West'"
)
```

The default connection method requires
[Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).
An optional ADBC backend is available for Arrow-based workflows; see
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.html).

### 2. Query a semantic model with DAX

Run a DAX query against tables and measures in a Fabric or
Power BI semantic model. The result is returned as a tibble.

``` r
df_dax <- fabric_pbi_dax_query(
  connstr = semantic_model,
  dax = "EVALUATE TOPN(1000, 'Customers')"
)

# Use the newer typed Arrow API on modern semantic models
arrow_stream <- fabric_pbi_dax_query(
  connstr = semantic_model,
  dax = "EVALUATE TOPN(1000, 'Customers')",
  api = "arrow",
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(arrow_stream)
```

The signed-in account needs access to the workspace or Read and Build
permissions on the semantic model. The Arrow endpoint additionally requires
Premium or Fabric capacity and both the **Dataset Execute Queries REST API**
developer setting and **Allow XMLA endpoints and Analyze in Excel with
on-premises semantic models** integration setting to be enabled by a Power BI
administrator.

### 3. Refresh and diagnose a semantic model

After updating source data, start a semantic-model refresh and wait for its
final state. A discovered semantic model carries the workspace and dataset IDs
needed by every step.

``` r
refresh <- fabric_pbi_refresh(semantic_model)
completed <- fabric_pbi_refresh_wait(refresh, timeout = 1800)

completed$state
completed$attempts
completed$details_url

# Capacity-backed models can use enhanced processing controls
sales_refresh <- fabric_pbi_refresh(
  semantic_model,
  mode = "enhanced",
  type = "Full",
  objects = "Sales",
  commit_mode = "Transactional",
  retry_count = 1L,
  timeout = "02:00:00"
)

history <- fabric_pbi_refresh_history(semantic_model, top = 10L)
```

Standard refresh works on shared capacity, subject to its daily quota.
Enhanced refresh, table/partition selection, and cancellation require a
capacity-backed model. Direct Lake models normally update automatically; an
explicit refresh frames the latest OneLake table versions when controlled
point-in-time visibility is needed. See the
[semantic-model refresh guide](https://kennispunttwente.github.io/fabricQueryR/articles/semantic-model-refresh.html)
for permissions, capacity limits, error diagnosis, and Direct Lake behavior.

### 4. Run Spark code through Livy

Run SparkR, PySpark, Scala, or Spark SQL remotely in Fabric,
and get the result in your local R session.

``` r
# Run a single SparkR statement through Livy
livy_sparkr_result <- fabric_livy_query(
  livy_url = lakehouse,
  kind = "sparkr",
  code = "print(1 + 2)"
)

# Run a reusable session, with deterministic cleanup on errors
run_shared_state <- function(lakehouse) {
  livy <- fabric_livy_session(lakehouse)
  on.exit(livy$close(), add = TRUE)
  livy$wait()
  livy$run("shared_value = 40", kind = "pyspark")
  livy$run("print(shared_value + 2)", kind = "pyspark")
}

# Let Fabric pack isolated REPLs into high-concurrency Spark sessions
run_high_concurrency <- function(lakehouse) {
  livy <- fabric_livy_session(
    lakehouse,
    high_concurrency = TRUE,
    session_tag = "report-workers"
  )
  on.exit(livy$close(), add = TRUE)
  livy$wait()
  livy$run("SELECT current_timestamp()", kind = "sql")
}

# Submit an application file as an independent batch job
batch <- fabric_livy_batch_submit(
  lakehouse,
  file = paste0(
    "abfss://workspace@onelake.dfs.fabric.microsoft.com/",
    "lakehouse.Lakehouse/Files/jobs/daily.py"
  ),
  wait = TRUE,
  cancel_on_timeout = TRUE
)
batch$result()
```

Copy the session or batch connection URL from **Lakehouse settings > Livy
endpoint**, or pass a Lakehouse returned by `fabric_lakehouses()` as above.

### 5. Read a Lakehouse Delta table

Read a Delta table stored in OneLake and return its current rows as a tibble.

``` r
df_onelake <- fabric_onelake_read_delta_table(
  table_path = "Customers",
  workspace_name = workspace,
  lakehouse_name = lakehouse
)

# Optional: return an Arrow C stream instead of a tibble
stream <- fabric_onelake_read_delta_table(
  table_path = "Customers",
  workspace_name = workspace,
  lakehouse_name = lakehouse,
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
```

### 6. Work with OneLake files

List, inspect, download, upload, or delete files in OneLake. 
Paths in a Lakehouse usually start with `Files/`.

``` r
files <- fabric_onelake_list(
  workspace,
  lakehouse,
  path = "Files/fixtures",
  recursive = TRUE
)

fabric_onelake_download(
  workspace,
  lakehouse,
  path = "Files/fixtures/basic.csv",
  dest = "basic.csv"
)
```

### 7. Discover and load Lakehouse tables

Inspect schemas and Delta metadata, load an existing CSV or Parquet path, or
stage a local R data frame as Parquet and wait for Fabric to commit it.

``` r
tables <- fabric_lakehouse_tables(lakehouse)

load <- fabric_lakehouse_load_table(
  lakehouse,
  table = "orders_csv",
  path = "Files/incoming/orders.csv",
  format = "Csv",
  mode = "Overwrite"
)
fabric_operation_wait(load, timeout = 900)

written <- fabric_lakehouse_write_table(
  lakehouse,
  table = "orders_from_r",
  data = data.frame(
    id = 1:3,
    amount = c(10.5, NA, 30),
    loaded_on = Sys.Date()
  )
)
written$operation_status$status
```

The Fabric List Tables and Load Table routes are preview APIs. The higher-level
writer uploads only to a unique `Files/` staging path and never edits managed
`Tables/` files directly. See the [Lakehouse table loading
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/lakehouse-table-loading.html)
for permissions, type mappings, schema behavior, and recovery after failures.

### 8. Query Eventhouse data with KQL

Run a KQL query against a KQL database in an Eventhouse.
A single result table is returned as a tibble, 
with Kusto data types converted to R data types.

``` r
df_kql <- fabric_kql_query(
  kql_database,
  query = paste(
    "declare query_parameters(selected_type:string);",
    "Events | where EventType == selected_type | take 100"
  ),
  parameters = list(selected_type = "Warning")
)
```

Queue existing blob or OneLake files into an existing table and wait for the
per-file outcome:

``` r
source <- paste0(
  "https://onelake.dfs.fabric.microsoft.com/workspace-id/",
  "lakehouse-id/Files/events/2026-08-14.csv;impersonate"
)

ingestion <- fabric_kql_ingest(
  kql_database,
  table = "Events",
  sources = source,
  format = "csv",
  mapping = "EventsCsv",
  ignore_first_record = TRUE,
  ingest_if_not_exists = "events-2026-08-14"
)

ingestion_status <- fabric_kql_ingestion_status(
  ingestion,
  wait = TRUE,
  timeout = 900
)
```

The queued-ingestion REST API is in preview. Submissions use Kusto's ingestion
URI, accept at most 20 existing storage sources per request, and have
at-least-once delivery semantics. The package does not automatically replay a
failed submission or stage local files implicitly. See the [tracked Eventhouse
ingestion guide](https://kennispunttwente.github.io/fabricQueryR/articles/eventhouse-ingestion.html)
for OneLake authentication, mappings, idempotency tags, service limits, and
failure handling.

### 9. Query a Fabric GraphQL API

Call an `API for GraphQL` item that has already been configured in Fabric.
The result keeps the nested GraphQL data and any GraphQL-level errors separate.

``` r
graphql_result <- fabric_graphql_query(
  graphql_api,
  query = paste(
    "query Customers($region: String!) {",
    "  customers(filter: {region: {eq: $region}}) {",
    "    items { id name region }",
    "  }",
    "}"
  ),
  variables = list(region = "West"),
  operation_name = "Customers"
)

graphql_result$data$customers$items
graphql_result$errors
```

Explore APIs whose administrators enabled runtime introspection, then collect
an explicitly selected row path across cursor pages:

``` r
schema <- fabric_graphql_schema(graphql_api)

pages <- fabric_graphql_paginate(
  graphql_api,
  query = paste(
    "query Customers($first: Int!, $after: String) {",
    "  customers(first: $first, after: $after, orderBy: {id: ASC}) {",
    "    items { id name region profile { segment } }",
    "    hasNextPage endCursor",
    "  }",
    "}"
  ),
  variables = list(first = 100L, after = NULL),
  next_cursor = fabric_graphql_cursor("customers")
)

customers <- fabric_graphql_collect(pages, c("customers", "items"))
attr(customers, "complete")
attr(customers, "errors")
```

Nested objects remain list-columns. Fabric disables introspection by default;
a workspace admin can enable it in the API settings, or export the schema from
the portal without enabling runtime introspection.

### 10. Invoke a Fabric User Data Function

Call published Fabric business logic through the function's explicit public
URL. Enable Public access in Run only mode and copy the URL from the function's
properties; discovery intentionally does not guess it.

``` r
function_result <- fabric_function_invoke(
  Sys.getenv("FABRIC_FUNCTION_URL"),
  parameters = list(
    customerName = "Ada",
    order = list(id = 42L, lines = I(c("A", "B")))
  )
)

function_result$status
function_result$output
function_result$errors
```

Valid Fabric execution envelopes remain inspectable for `BadRequest`,
`Failed`, `Timeout`, and `ResponseTooLarge`, including their HTTP status and
structured error details. Invocations are never retried unless you set
`idempotent = TRUE`; do that only when repeating the function cannot duplicate
side effects. See the [User Data Functions
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/user-data-functions.html)
for permissions, trusted-host behavior, limits, and authentication details.

### 11. Run and monitor Fabric jobs

Start a notebook, data pipeline, or Spark job definition, then wait for
completion or inspect/cancel it from R. You can also inspect earlier runs and
manage recurring schedules.

``` r
job <- fabric_job_run(
  notebook,
  parameters = list(mode = "incremental", batch_size = 100L),
  default_lakehouse = lakehouse
)

result <- fabric_job_wait(job, timeout = 900)
result$status
result$exit_value

history <- fabric_job_instances(notebook)

weekly <- fabric_job_schedule_config(
  "Weekly",
  start_time = "2026-10-01T00:00:00Z",
  end_time = "2027-10-01T00:00:00Z",
  time_zone = "W. Europe Standard Time",
  times = "07:30",
  weekdays = c("Monday", "Thursday")
)
schedule <- fabric_job_schedule_create(notebook, weekly)

# Fabric's PATCH contract needs the complete configuration; omitted fields are
# read and preserved automatically when making a partial update from R.
fabric_job_schedule_update(notebook, schedule, enabled = FALSE)

# Destructive removal always requires explicit confirmation.
fabric_job_schedule_delete(notebook, schedule, confirm = TRUE)
```

### 12. Resume a Fabric long-running operation

Some Fabric APIs return an operation ID while provisioning continues. Resume
that work from its ID or `Location` URL, wait for success, and retrieve its
JSON, binary, or empty result through one consistent result object.

``` r
state <- fabric_operation_status(
  "00000000-0000-0000-0000-000000000000"
)

completed <- fabric_operation_wait(state, timeout = 900)
result <- fabric_operation_result(completed)
result$value
```

Package functions that start this protocol return a reusable operation handle.
The handle keeps its in-process authentication context; a saved ID or location
can be resumed later by supplying normal authentication arguments again.

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
The repository remains under the Kennispunt Twente GitHub organization for historical reasons,
and because the early development was done while I was affiliated with them.
