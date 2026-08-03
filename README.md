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
more details on how to set these environment variables and sign in to Microsoft Fabric from R.

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
sql_database <- fabric_sql_databases(workspace) |> first()
semantic_model <- fabric_semantic_models(workspace) |> first()
kql_database <- fabric_kql_databases(workspace) |> first()
graphql_api <- fabric_graphql_apis(workspace) |> first()
notebook <- fabric_notebooks(workspace) |> first()
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
permissions on the semantic model.

### 3. Run Spark code through Livy

Run SparkR, PySpark, Scala, or Spark SQL remotely in Fabric,
and get the result in your local R session.

``` r
# Run a single SparkR statement through Livy
livy_sparkr_result <- fabric_livy_query(
  livy_url = lakehouse,
  kind = "sparkr",
  code = "print(1 + 2)"
)

# Run a Livy session for multiple statements that share state
livy <- fabric_livy_session(lakehouse)
livy$wait()
livy$run("shared_value = 40", kind = "pyspark")
livy_result <- livy$run(
  "print(shared_value + 2)",
  kind = "pyspark"
)
livy$close()
```

Copy the session or batch connection URL from **Lakehouse settings > Livy
endpoint**, or pass a Lakehouse returned by `fabric_lakehouses()` as above.

### 4. Read a Lakehouse Delta table

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

Direct Delta reads require a token for
`https://storage.azure.com/.default` and OneLake data permission; item **Read**
alone only exposes metadata. Grant **ReadAll**, or a OneLake role that can read
the table when OneLake security is enabled. Tables protected by OneLake RLS/CLS
may be unavailable to this external delta-rs reader. A Fabric administrator
must also enable **Users can access data stored in OneLake with apps external
to Fabric** for the caller in
[OneLake tenant settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake).
The pinned runtime supports
classic checkpoints, schema evolution, column mapping, deletion vectors within
its documented safety limit, and version reads; Type Widening, V2 checkpoints,
and Fabric's Variant shredding preview fail explicitly. Warehouse Delta logs are
published asynchronously, so a OneLake read can lag the current Warehouse state.
For external Delta readers, Warehouse table names are limited to ASCII letters,
digits, and underscores, while column names cannot contain spaces, tabs,
carriage returns, square brackets, commas, semicolons, braces, parentheses, or
equals signs. See
[Delta Lake logs in Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-delta-lake-logs).
See `?fabric_onelake_read_delta_table` for the full compatibility contract and
the [Fabric permission model](https://learn.microsoft.com/en-us/fabric/security/permission-model).

### 5. Work with OneLake files

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

### 6. Query Eventhouse data with KQL

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

### 7. Query a Fabric GraphQL API

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

### 8. Run and monitor Fabric jobs

Start a notebook, data pipeline, or Spark job definition, 
then wait for completion or inspect/cancel it from R.

``` r
job <- fabric_job_run(
  notebook,
  parameters = list(mode = "incremental", batch_size = 100L),
  default_lakehouse = lakehouse
)

result <- fabric_job_wait(job, timeout = 900)
result$status
result$exit_value
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
The repository remains under the Kennispunt Twente GitHub organization for historical reasons,
and because the early development was done while I was affiliated with them.
