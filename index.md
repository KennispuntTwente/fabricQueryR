# fabricQueryR

`fabricQueryR` helps you work with Microsoft Fabric directly from R.

You can use it to find Fabric workspaces and data items, query various
Fabric data interfaces (SQL, DAX, KQL, GraphQL), work with OneLake files
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

Below is a minimal example of how to find a workspace and some of its
items.

To connect to Microsoft Fabric, you need a Microsoft Entra tenant ID and
optionally a client ID. See the [authentication
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/authentication.html)
more details on how to set these environment variables and sign in to
Microsoft Fabric from R.

Once you know these values, you can discover workspaces and items like
so:

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

In the next sections, you can see how to use these items to query data,
run Spark code, and more.

Also see the
[reference](https://kennispunttwente.github.io/fabricQueryR/reference/index.html)
for full function documentation and more examples.

## What you can do

### 1. Connect to Fabric SQL

Connect to a Warehouse, SQL Database, or a Lakehouse’s SQL analytics
endpoint. The result is a standard DBI connection, so it works with
familiar R database packages such as DBI and dbplyr.

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

The default connection method requires [Microsoft ODBC Driver 18 for SQL
Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).
An optional ADBC backend is available for Arrow-based workflows; see
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.html).

### 2. Query a semantic model with DAX

Run a DAX query against tables and measures in a Fabric or Power BI
semantic model. The result is returned as a tibble.

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

Run SparkR, PySpark, Scala, or Spark SQL remotely in Fabric, and get the
result in your local R session.

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

Copy the session or batch connection URL from **Lakehouse settings \>
Livy endpoint**, or pass a Lakehouse returned by
[`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
as above.

### 4. Read a Lakehouse Delta table

Read a Delta table stored in OneLake and return its current rows as a
tibble.

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

The account needs access through the workspace or **Lakehouse \> Manage
OneLake data access**. This function downloads the active table files
locally, so a filtered SQL query can be more efficient for very large
tables. The Arrow result changes the returned object, but does not avoid
the local download or in-memory read.

The reader follows Delta reader protocols 1–3 and supports the profiles
emitted by Fabric Spark Runtime 2.0 and Fabric Warehouse exports:
classic and V2 checkpoints, name/ID column mapping, deletion vectors,
type widening, timestamp-NTZ, shallow-clone paths, and native Variant
values, including Variant shredding. Delta `long` values are returned as
[`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html);
decimals are returned as character so values through precision 38 remain
exact. Legacy `void` fields are returned as logical all-`NA` columns.
For legacy `timestamp` partition values without an offset, supply
`timestamp_partition_timezone` with the timezone of the writing system,
such as `"UTC"` or `"Europe/Amsterdam"`. The Delta log does not record
that timezone, so the reader fails rather than silently assuming one.
Metadata must declare Parquet with no provider-specific options. Unknown
reader features, malformed recursive schemas/commits, catalog-managed
commits, and absolute paths outside OneLake fail before any rows are
returned. A caller-supplied `dest_dir` must be new or empty.

Compatibility reviewed against the Delta protocol and Fabric
documentation in July 2026:

| Delta capability | Reader behavior | Verification |
|----|----|----|
| Reader protocols 1–3 | Supported | Protocol-focused unit tests and live Fabric tables |
| V1 classic/multipart and V2 classic/UUID checkpoints with sidecars | Supported, including same-version alternatives and older-checkpoint recovery | Real Parquet/JSON unit fixtures, remote retry tests, and live Fabric V2 checkpoints |
| Name- and ID-based column mapping | Supported recursively through structs, arrays, and maps | Projection unit tests and live rename/drop fixtures |
| Inline, relative-file, and absolute-OneLake deletion vectors | Supported; file identity includes the DV unique ID | Golden Roaring vectors and live sparse/dense/checkpoint DV tables |
| Absolute OneLake AddFile paths and Fabric shallow clones | Supported when scoped to OneLake | Unit path checks and a live shallow clone |
| Typed partitions, `void`, type widening, and timestamp without time zone | Supported; exact decimals remain character | Unit matrices and live Fabric edge-value tables |
| Top-level Variant, including mixed shredded and unshredded files | Exact `fabric_delta_variant` cells retain type plus Parquet metadata/value bytes; Arrow uses an equivalent typed struct; SQL NULL remains distinct from Variant Null; nested Variant fields fail closed | Mixed physical unit files, Decimal128 boundaries, and live pre/post-shredding writes |
| Non-Parquet formats/options, malformed commits/schemas, catalog-managed commits, and unknown reader features | Rejected before reading data | Negative unit tests |
| Absolute paths outside OneLake | Rejected before reading data | Negative unit tests |

The test suite also compares this R implementation with the independent
Python binding of delta-rs. Deterministic local fixtures cover time
travel, projection, empty and evolving schemas, delete/update rewrites,
encoded and null partitions, scalar boundaries, exact values, and nested
null/empty data. The comparison checks logical R column types and
delta-rs snapshot metadata as well as row values. Because [Fabric
Runtime 2.0 enables deletion vectors by
default](https://learn.microsoft.com/fabric/data-engineering/delta-lake-deletion-vectors)
and Fabric documents that its pinned `deltalake` reader cannot scan
them, the live parity matrix uses explicitly DV-disabled Fabric tables.
Those tables cover projection, empty and evolving schemas, typed
partitions, historical checkpoints, exact scalar boundaries, and nested
values. Runtime-default tables separately exercise deletion vectors,
column mapping, type widening, V2 checkpoint sidecars, shallow clones,
mixed shredded Variant, and Warehouse exports through exact direct
assertions. This split prevents an oracle limitation from either failing
the suite or silently weakening modern Fabric coverage.

Warehouse reads use the latest Delta snapshot that Fabric has published
to OneLake. Warehouse SQL commits start that publication asynchronously,
so a just-committed transaction can briefly be newer than the visible
Delta log. The integration sandbox waits for a post-seed log publication
before asserting Warehouse rows; production callers that require
read-after-write coordination must likewise wait for the published
snapshot.

delta-rs and Python are test dependencies only; users do not need them
to install or run fabricQueryR.

The Fabric integration workflow provisions Runtime 2.0 tables on pushes
to `main`/`master`, in-repository pull requests that touch the reader, a
weekly schedule, and manual runs. Pull requests from forks do not
receive the protected Fabric environment; their protocol/unit suite
still runs in the regular R workflow.

### 5. Work with OneLake files

List, inspect, download, upload, or delete files in OneLake. Paths in a
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

### 6. Query Eventhouse data with KQL

Run a KQL query against a KQL database in an Eventhouse. A single result
table is returned as a tibble, with Kusto data types converted to R data
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

### 7. Query a Fabric GraphQL API

Call an `API for GraphQL` item that has already been configured in
Fabric. The result keeps the nested GraphQL data and any GraphQL-level
errors separate.

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

Start a notebook, data pipeline, or Spark job definition, then wait for
completion or inspect/cancel it from R.

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
