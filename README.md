# fabricQueryR

<!-- badges: start -->
[![R-CMD-check](https://github.com/kennispunttwente/fabricQueryR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/kennispunttwente/fabricQueryR/actions/workflows/R-CMD-check.yaml)
[![CRAN status](https://www.r-pkg.org/badges/version/fabricQueryR)](https://CRAN.R-project.org/package=fabricQueryR)
<!-- badges: end -->

`fabricQueryR` helps you work with Microsoft Fabric directly from R. You can
use it to find Fabric workspaces and data items, query several Fabric data services,
work with OneLake files and tables, run Spark code, and start or monitor Fabric jobs.
All with familiar R tools and data types.

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

See the [reference](https://kennispunttwente.github.io/fabricQueryR/reference/index.html)
for full function documentation and examples.

See the [authentication vignette](https://kennispunttwente.github.io/fabricQueryR/articles/authentication.html)
for help with signing in to Microsoft Fabric and obtaining access tokens.

Set your Microsoft Entra tenant ID before starting. A client ID is optional
when your tenant permits the use of the default Azure CLI application ID.
On first use, `AzureAuth` may open a browser so you can sign in.

``` r
library(fabricQueryR)

Sys.setenv(FABRICQUERYR_TENANT_ID = "your-tenant-id")
# Sys.setenv(FABRICQUERYR_CLIENT_ID = "your-app-client-id")

workspaces <- fabric_workspaces()
workspace <- workspaces[workspaces$displayName == "ExampleWorkspace", ]

# These helpers find specific kinds of Fabric items and their connection details
lakehouse <- fabric_lakehouses(workspace)[1, ]
semantic_model <- fabric_semantic_models(workspace)[1, ]
kql_database <- fabric_kql_databases(workspace)[1, ]
graphql_api <- fabric_graphql_apis(workspace)[1, ]
notebook <- fabric_notebooks(workspace)[1, ]
```

## What you can do

### 1. Connect to Fabric SQL

Connect to a Warehouse, SQL Database, or a Lakehouse's read-only SQL analytics
endpoint. The result is a standard DBI connection, so it works with familiar R
database tools such as DBI and dbplyr.

``` r
con <- fabric_sql_connect(lakehouse)

DBI::dbListTables(con)

df_sql <- fabric_sql_query(
  lakehouse,
  "SELECT * FROM dbo.Customers WHERE region = ?",
  params = list("West")
)

DBI::dbDisconnect(con)
```

The default connection method requires
[Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).
An optional ADBC backend is available for Arrow-based workflows; see
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.html)
for setup and trade-offs.

### 2. Query a semantic model with DAX

Run a DAX query against the report-ready tables and measures in a Fabric or
Power BI semantic model. The result is returned as a tibble.

``` r
df_dax <- fabric_pbi_dax_query(
  connstr = semantic_model,
  dax = "EVALUATE TOPN(1000, 'Customers')"
)
```

The signed-in account needs access to the workspace or Read and Build
permissions on the semantic model.

### 3. Run Spark code through Livy

Run SparkR, PySpark, Scala, or Spark SQL remotely in Fabric. Use
`fabric_livy_query()` for one statement, `fabric_livy_session()` when several
statements should share state, or `fabric_livy_batch_submit()` for a complete
application file.

``` r
livy_sparkr_result <- fabric_livy_query(
  livy_url = lakehouse,
  kind = "sparkr",
  code = "print(1 + 2)"
)

# Reuse variables and Spark state across statements
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
For a schema-enabled Lakehouse, also provide a schema such as `"dbo"`.

``` r
df_onelake <- fabric_onelake_read_delta_table(
  table_path = "Customers",
  workspace_name = workspace,
  lakehouse_name = lakehouse,
  schema = "dbo"
)
```

The account needs access through the workspace or **Lakehouse > Manage OneLake
data access**. This function downloads the active table files locally, so a
filtered SQL query can be more efficient for very large tables.

### 5. Work with OneLake files

List, inspect, download, upload, or delete ordinary files in OneLake. Paths in a
Lakehouse usually start with `Files/`.

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

Use the Delta table reader above for tables under `Tables/`; changing individual
files there would bypass the Delta transaction log.

### 6. Query Eventhouse data with KQL

Run a KQL query against a KQL database in an Eventhouse. A single result table
is returned as a tibble, with common Kusto data types converted to useful R
types.

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

Using `parameters` keeps changing values separate from the KQL text and avoids
manual quoting.

### 7. Query a Fabric GraphQL API

Call an **API for GraphQL** item that has already been configured in Fabric.
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

Create the API item, connect its data source, and choose which objects to expose
in Fabric before querying it from R.

### 8. Run and monitor Fabric jobs

Start an on-demand Notebook, data pipeline, or Spark job definition, then wait
for completion or inspect/cancel it from R.

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

Use Fabric's scheduler rather than these functions when you need a recurring
timetable.

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