# fabricQueryR

‘fabricQueryR’ is an R package which helps you discover and query data
from Microsoft Fabric in R. It comes with discovery helpers and eight
methods which help you work with Microsoft Fabric from R:

1.  Create a connection to a SQL endpoint (e.g., from a `Lakehouse` or
    `Data Warehouse` item):
    [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md).
    This results in a ‘DBI’ connection object which you can execute SQL
    queries with, and/or use with ‘DBI’-compatible packages like
    ‘dbplyr’.

2.  Execute a DAX query against a Fabric or Power Bi `Semantic Model`
    item:
    [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md).
    With this, you can run DAX queries against a Fabric/Power Bi dataset
    and get the results as a ‘tibble’ dataframe.

3.  Execute Livy API queries: with
    [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md),
    [`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md),
    and
    [`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md),
    you can remotely run Spark/Spark SQL/SparkR/PySpark in Microsoft
    Fabric and get the results in your local R session.

4.  Read a Delta table from a Fabric `Lakehouse` item:
    [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md).
    This function downloads the underlying Parquet files from the Delta
    table stored in OneLake (ADLS Gen2) and returns the data as a
    ‘tibble’ dataframe.

5.  Work with OneLake files using
    [`fabric_onelake_list()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
    [`fabric_onelake_metadata()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
    [`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
    [`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
    and
    [`fabric_onelake_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).

6.  Execute a KQL query against an `Eventhouse`/`KQL Database` item:
    [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md).
    This returns typed Kusto query results as a ‘tibble’ dataframe, or a
    named list of tibbles when KQL returns multiple result tables.

7.  Execute a GraphQL query against an `API for GraphQL` item:
    [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md).
    This preserves GraphQL data, errors, and extensions independently,
    including valid responses containing partial data and errors.

8.  Run and monitor on-demand Fabric item jobs with
    [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
    [`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
    [`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
    and
    [`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md).
    These functions support notebooks, pipelines, and Spark job
    definitions.

## Installation

You can install the development version of ‘fabricQueryR’ like so:

``` r

if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes")
}

remotes::install_github("kennispunttwente/fabricQueryR")
```

Or, install the latest release from CRAN:

``` r

install.packages("fabricQueryR")
```

## Usage

See the
[reference](https://kennispunttwente.github.io/fabricQueryR/reference/index.html)
for the full documentation of all functions. See the [authentication
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/authentication.html)
for interactive login, cached tokens, service principals, certificates,
managed identities, and the permissions each Fabric workload requires.

Below is a code snippet showing how to discover targets and use the
eight methods to work with Fabric data from R:

``` r

# First find your 'tenant' ID & 'client' ID (app registration) in Azure/Entra
# You may be able to use the default Azure CLI app id;
#   this will be automatically used if you do not set 'FABRICQUERYR_CLIENT_ID'
# The AzureAuth package is used to acquire tokens; you may be redirected
#   to a browser window to sign in the first time

library(fabricQueryR)

# Sys.setenv(FABRICQUERYR_TENANT_ID = "...")
# Sys.setenv(FABRICQUERYR_CLIENT_ID = "...")


# Discover workspaces and items ------------------------------------------------

workspaces <- fabric_workspaces()
workspace <- workspaces[workspaces$displayName == "ExampleWorkspace", ]

# Retrieve workload-specific properties and ready-to-use targets
lakehouse <- fabric_lakehouses(workspace)[1, ]
semantic_model <- fabric_semantic_models(workspace)[1, ]
kql_database <- fabric_kql_databases(workspace)[1, ]
graphql_api <- fabric_graphql_apis(workspace)[1, ]
notebook <- fabric_notebooks(workspace)[1, ]

# Other helpers include fabric_warehouses(), fabric_sql_databases(),
# fabric_eventhouses(), fabric_kql_databases(), fabric_notebooks(), and
# fabric_graphql_apis()


# SQL connection to Warehouse, Lakehouse, or SQL Database ----------------------

# A discovered item supplies both the server and required database/catalog.
# You can alternatively provide a complete portal connection string, or a bare
# endpoint together with `database = "..."`; omit it to use Fabric `master`.

# Get connection
con <- fabric_sql_connect(lakehouse)

# List databases
DBI::dbGetQuery(con, "SELECT name FROM sys.databases")

# List tables in the current database
DBI::dbGetQuery(
  con,
  "
  SELECT TABLE_SCHEMA, TABLE_NAME
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_TYPE = 'BASE TABLE'
  "
)

# Read 'Customers' table
df_sql <- fabric_sql_query(
  lakehouse,
  "SELECT * FROM dbo.Customers WHERE region = ?",
  params = list("West")
)

# Close connection
DBI::dbDisconnect(con)

# ADBC is opt-in; first install the external driver with:
#   dbc install mssql
con_adbc <- fabric_sql_connect(lakehouse, backend = "adbc")
DBI::dbDisconnect(con_adbc)

# Keep results in Arrow form for downstream Arrow/nanoarrow processing.
stream <- fabric_sql_query(
  lakehouse,
  "SELECT * FROM dbo.Customers",
  backend = "adbc",
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
customers <- as.data.frame(reader$read_table())


# Table from Lakehouse via OneLake data access ---------------------------------

# Ensure that the account/principal you authenticate with has access via
# being part of the workspace, or via Lakehouse -> Manage OneLake data access

df_onelake <- fabric_onelake_read_delta_table(
  table_path = "Customers",
  workspace_name = workspace$displayName,
  lakehouse_name = lakehouse$displayName
)

# General OneLake files retain their complete paths and ETags. A direct
# https:// or abfss:// OneLake path can be used instead of separate targets
fixture_files <- fabric_onelake_list(
  workspace,
  lakehouse,
  path = "Files/fixtures",
  recursive = TRUE
)
first_bytes <- fabric_onelake_download(
  workspace,
  lakehouse,
  "Files/fixtures/basic.csv",
  range = c(0, 99)
)


# DAX query against Semantic Model ---------------------------------------------

# Ensure that the account you use to authenticate has access to the workspace,
# or that you have been granted 'Build' permissions on the dataset (via share)

df_dax <- fabric_pbi_dax_query(
  connstr = semantic_model,
  dax = "EVALUATE TOPN(1000, 'Sheet1')"
)


# Livy API query to execute Spark code -----------------------------------------

# Run a Livy SparkR query
livy_sparkr_result <- fabric_livy_query(
  livy_url = lakehouse,
  kind = "sparkr",
  code = "print(1+2)"
)

# Reuse Spark state across multiple statements with an R6 session
livy <- fabric_livy_session(lakehouse)
on.exit(livy$close())
livy$wait()
livy$run("shared_value = 40", kind = "pyspark")
livy_result <- livy$run(
  "print(shared_value + 2)",
  kind = "pyspark"
)

# Use high_concurrency = TRUE and session_tag = "..." for an isolated REPL
# in Fabric's shared high-concurrency session pool

# Submit a standalone Spark application from OneLake
batch <- fabric_livy_batch_submit(
  lakehouse,
  file = paste0(
    "abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/",
    "<lakehouse-id>/Files/jobs/example.py"
  )
)
batch$wait()
batch$logs(refresh = FALSE)

# (See `?fabric_livy_session` and `?fabric_livy_batch_submit`)


# KQL query against an Eventhouse database ------------------------------------

# Query parameters are bound separately from the KQL query text
df_kql <- fabric_kql_query(
  kql_database,
  query = paste(
    "declare query_parameters(selected_type:string);",
    "Events | where EventType == selected_type | take 100"
  ),
  parameters = list(selected_type = "Warning")
)


# GraphQL API ---------------------------------------------------------------

# Variables are encoded separately from the GraphQL document. The result keeps
# partial data and GraphQL-level errors in separate fields
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


# On-demand item jobs -------------------------------------------------------

# A discovered notebook record already contains its item type and workspace
job <- fabric_job_run(
  notebook,
  parameters = list(mode = "incremental", batch_size = 100L),
  default_lakehouse = lakehouse
)
result <- fabric_job_wait(job, timeout = 900)
result$status
result$exit_value
```

ODBC remains the default and requires Microsoft ODBC Driver 18 or newer.
The ADBC path uses the CRAN packages `adbi` and `adbcdrivermanager` plus
the separately distributed ADBC Driver Foundry `mssql` driver. That
driver is tested upstream with Fabric Data Warehouse; this package’s
live sandbox additionally tests Lakehouse SQL analytics endpoints and
Fabric SQL Database. `adbcdrivermanager` loads installed drivers but
does not install their external binaries. If the requested driver is
missing,
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
fails before authentication with the exact `dbc install ...` command.
Arrow-stream results implement the Arrow C Stream interface and convert
directly with
[`arrow::as_record_batch_reader()`](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html);
`arrow` remains an optional dependency.

## Background

Microsoft Fabric is a new data platform from Microsoft which combines
various data services, including data warehousing, data lakes, and
business intelligence. It is built on top of Azure Data Services and
integrates with Power BI for analytics and reporting. Microsoft is
actively promoting Fabric as the next-generation data platform for
organizations using Microsoft Azure and Power BI.

As our organization started working with Microsoft Fabric, I found that
loading data into R from Fabric was not yet straightforward, and took
some effort to get working. To help others in the same situation, I
decided to share the functions I created to make this easier.

### About the maintainer

Luka Koning is no longer associated with Kennispunt Twente. He maintains
this open-source R package in his personal capacity.
