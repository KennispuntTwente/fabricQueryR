# Bring Fabric data into R

Microsoft Fabric stores and serves data through Lakehouses, Warehouses,
Eventhouses, semantic models, files, and APIs. ‘fabricQueryR’ helps you
bring that data into your local R session for analysis, visualization,
and reporting. For most everyday tasks, the result is a tibble (a modern
R data frame).

The best method depends on where the data lives and whether you need a
whole table or a filtered result. Start with a small read and let Fabric
filter the data when possible. Move to Arrow streaming or Spark only
when the data is too large for local memory or the transformation needs
distributed computing. This guide compares the common methods in that
order.

## Choose a reading method

| Your source or goal | Start with | Why |
|----|----|----|
| Warehouse, SQL Database, or a SQL-shaped Lakehouse query | `item$sql_query()` ([`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)) | Filter and summarize on the server with familiar SQL |
| One Lakehouse Delta table | `lakehouse$read_table()` ([`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md)) | Read a table without writing SQL |
| One Warehouse table | `warehouse$read_table()` ([`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md)) | Read a table by name without writing SQL |
| Eventhouse table or KQL result | `kql_database$read_table()` ([`fabric_kql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_read_table.md)) or `$query()` ([`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)) | Use the Eventhouse query engine |
| Power BI/Fabric semantic model | `model$dax_query()` ([`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)) | Use model relationships and measures |
| CSV, Parquet, or Arrow file in OneLake | `lakehouse$onelake_read_file()` ([`fabric_onelake_read_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md)) | Read the file itself |
| API for GraphQL | `api$query()` ([`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)) | Request the fields exposed by the API |
| A transformation that genuinely needs Spark | `lakehouse$livy_query()` ([`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)) | Run distributed code in Fabric |

SQL is a good default for relational data because Fabric does the
filtering and R receives only the rows you need. A direct table reader
is simpler when you want one table and do not need joins or grouping.

## Discover the data source once

Discovery returns read-only R6 objects. They carry the IDs, connection
details, and credential needed by their read methods. Read service
fields such as `$displayName` and `$id` directly:

``` r

library(fabricQueryR)

workspaces <- fabric_workspaces()
matches <- Filter(
  \(x) identical(x$displayName, "Analytics workspace"),
  workspaces
)
stopifnot(length(matches) == 1L)
workspace <- matches[[1L]]
lakehouse <- workspace$lakehouses()[[1L]]
warehouse <- workspace$warehouses()[[1L]]
kql_database <- workspace$kql_databases()[[1L]]
model <- workspace$semantic_models()[[1L]]
```

The workspace methods above correspond to
[`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
[`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
[`fabric_kql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
and
[`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).

Your workspace does not need to contain every item type. Discover only
the source used by your workflow.

## Read with SQL

Use an item’s `$sql_query()` method
([`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md))
for one read-only `SELECT` query. It opens and closes the connection for
you and returns a tibble:

``` r

recent_orders <- warehouse$sql_query(
  sql = paste(
    "SELECT TOP 100 order_id, order_date, amount",
    "FROM dbo.orders",
    "WHERE order_date >= ?",
    "ORDER BY order_date DESC"
  ),
  params = list(as.Date("2026-01-01"))
)

head(recent_orders)
```

The default `numeric_policy = "auto"` uses ODBC’s numeric conversion and
warns once per R session that precision may be lost. Use
`backend = "adbc"` for exact conversion, or `numeric_policy = "exact"`
to reject ODBC results containing INT, BIGINT, DECIMAL, or NUMERIC
columns before fetching. Casting these columns to `varchar` in SQL also
preserves their values. Explicit `numeric_policy = "driver"` accepts
driver conversion without the warning.

The ADBC backend needs the optional ‘adbi’ and ‘adbcdrivermanager’
packages, plus the Microsoft SQL Server ADBC driver. ‘adbi’ is available
from the R-DBI repository while it is archived on CRAN:

``` r

install.packages(
  c("adbi", "adbcdrivermanager"),
  repos = c("https://r-dbi.r-universe.dev", "https://cloud.r-project.org")
)
```

Install the SQL Server driver separately with `dbc install mssql`. See
[`?fabric_sql_connect`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
for driver selection and connection options.

Put changing values in `params` rather than pasting them into the SQL
text. This handles quoting safely. Use `$sql_connect()`
([`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md))
and normal ‘DBI’ functions when several queries should share one
connection:

``` r

orders <- local({
  con <- warehouse$sql_connect()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbListTables(con)
  DBI::dbGetQuery(con, "SELECT TOP 100 * FROM dbo.orders")
})
```

Direct ‘DBI’ reads use the driver’s numeric conversion.

The default SQL backend uses Microsoft ODBC Driver 18 for SQL Server.
See
[`?fabric_sql_connect`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
for the ODBC and ADBC setup choices.

## Read one table without writing a query

Lakehouse and Warehouse table readers accept a table name and can limit
the data before it enters R. The `$read_table()` methods call
[`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md)
and
[`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md),
respectively:

``` r

lakehouse_rows <- lakehouse$read_table(
  table = "orders",
  columns = c("order_id", "order_date", "amount"),
  limit = 100L
)

warehouse_rows <- warehouse$read_table(
  table = "orders",
  schema = "dbo",
  limit = 100L
)
```

Warehouse reads use the same `numeric_policy = "auto"` default as SQL
queries: driver conversion for ODBC, or exact conversion for ADBC. The
ODBC precision warning is shared by queries and table reads and appears
only once per session. Use `numeric_policy = "exact"` to reject unsafe
ODBC results, or `numeric_policy = "driver"` to explicitly accept
conversion without a warning.

Use `lakehouse$tables()`
([`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md))
when you are unsure which Lakehouse tables or schemas are available.

Direct Lakehouse reads use Python’s ‘deltalake’ and ‘nanoarrow’ through
‘reticulate’. **To enable automatic installation, restart R first**,
then explicitly select a managed environment before loading the package:

``` r

Sys.setenv(RETICULATE_PYTHON = "managed")
library(fabricQueryR)
fabric_delta_config(initialize = TRUE)
```

`"managed"` tells ‘reticulate’ to create or reuse an environment that
satisfies the package’s requirements. Loading ‘fabricQueryR’ declares
them with
[`reticulate::py_require()`](https://rstudio.github.io/reticulate/reference/py_require.html),
and initialization uses ‘uv’ to download Python and the dependencies if
needed. ‘reticulate’ also downloads ‘uv’ if needed. No separate
`py_install()` call is necessary. Both entries in `available` should
then be `TRUE`, with installed package versions in `versions`.

Without `RETICULATE_PYTHON = "managed"`, an existing Python selected
through RStudio, environment variables, `use_python()`, or a project
virtualenv can take precedence. Declaring requirements does not install
them into that interpreter or upgrade it; Python 3.9, for example,
cannot satisfy this backend’s minimum of Python 3.10. Once Python
starts, restart R before changing its selection.
[`reticulate::py_config()`](https://rstudio.github.io/reticulate/reference/py_config.html)
reports which interpreter was chosen and why.

To enable managed setup for future sessions in this project, add
`RETICULATE_PYTHON=managed` to the project’s `.Renviron` file and
restart R. Use
[`fabric_delta_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_delta_config.md)
to inspect requirements without starting Python.

For a reusable environment that you manage yourself, create it with
[`reticulate::virtualenv_create()`](https://rstudio.github.io/reticulate/reference/virtualenv-tools.html)
from a compatible installed Python, or with
`uv venv --python 3.11 --seed` at a chosen path. Then install the
dependencies with
`reticulate::py_install(..., envname = ..., method = "virtualenv")` and
select that environment before Python starts. `py_install()` uses ‘pip’
in a virtualenv; it does not upgrade an environment’s Python version.

See
[`?fabric_delta_config`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_delta_config.md)
for complete examples of both setup routes, and
[`?fabric_onelake_read_delta_table`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
for supported table features, OneLake permissions, and exact numeric
conversions.

## Read Eventhouse data

An Eventhouse is optimized for event, log, and time-series data. Its
query language is KQL. Read a whole table by name with `$read_table()`
([`fabric_kql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_read_table.md)):

``` r

events <- kql_database$read_table(
  table = "Events",
  limit = 100L
)
```

Use `$query()`
([`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md))
when Fabric should filter or summarize the events first:

``` r

daily_events <- kql_database$query(
  query = paste(
    "Events",
    "| where observed_at >= ago(7d)",
    "| summarize event_count = count() by bin(observed_at, 1d)",
    "| order by observed_at asc"
  )
)
```

## Query a semantic model with DAX

A semantic model is a dataset ready for reporting, commonly used in
Power BI. Query it with `$dax_query()`
([`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md))
and DAX (Data Analysis Expressions):

``` r

sales_by_region <- model$dax_query(
  dax = paste(
    "EVALUATE",
    "SUMMARIZECOLUMNS(",
    "  'Region'[Region],",
    "  \"Total Sales\", [Total Sales]",
    ")"
  )
)
```

The [Semantic Models
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/semantic-model-refresh.md)
shows more things you can do with semantic models, like refreshing their
data.

## Read a OneLake file

Use the file reader when the file itself is the data contract. Paths in
a Lakehouse usually begin with `Files/`. The `$onelake_read_file()`
method calls
[`fabric_onelake_read_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md):

``` r

orders_file <- lakehouse$onelake_read_file(
  path = "Files/exports/orders.parquet"
)
```

CSV, Parquet, and Arrow IPC files can become R or Arrow objects. Other
file types can be downloaded as raw bytes or to disk. Use a Lakehouse
table reader, not a file reader, for managed data below `Tables/`.

## Read through GraphQL or Spark

GraphQL is useful when a Fabric API item exposes a purpose-built
selection of fields:

``` r

api <- workspace$graphql_apis()[[1L]]

response <- api$query(
  query = "{ products { items { id name category } } }"
)
products <- response$data$products$items
```

Use [Working with
GraphQL](https://kennispunttwente.github.io/fabricQueryR/articles/graphql-schema-and-rows.md)
for schema inspection and pagination. `$graphql_apis()` is the workspace
method for
[`fabric_graphql_apis()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).

Spark is the later choice for distributed transformations, Spark-only
data formats, or logic already written for Spark:

``` r

result <- lakehouse$livy_query(
  kind = "sql",
  code = "SELECT category, count(*) AS n FROM orders GROUP BY category"
)
counts <- result$output$parsed
```

Spark has startup cost. Prefer SQL or a direct reader for a small,
ordinary table read.

## Scale up with Arrow streams

If a selected result is larger than your computer’s working memory,
process it as a stream instead of collecting it all at once.

Several readers accept `result = "arrow_stream"`. This can be used with
the ‘arrow’ R package to read the data in batches. Here `$read_table()`
calls
[`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md):

``` r

row_count <- local({
  stream <- lakehouse$read_table(
    table = "large_orders",
    result = "arrow_stream"
  )
  on.exit(nanoarrow::nanoarrow_pointer_release(stream), add = TRUE)
  reader <- arrow::as_record_batch_reader(stream)
  on.exit(reader$Close(), add = TRUE, after = FALSE)
  count <- 0
  repeat {
    batch <- reader$read_next_batch()
    if (is.null(batch)) break
    # Process or write this batch before reading the next one.
    count <- count + batch$num_rows
  }
  count
})
```

`reader$read_table()` collects every remaining batch into one Arrow
Table; use it only when the full selected result fits in memory. Direct
Delta reads stage the selected data to a temporary file before returning
the stream, so allow enough local disk space even when processing
batches.

Close an Arrow reader after use. For disk-backed OneLake Delta streams,
this also deletes the staged temporary file; when consuming one directly
through ‘nanoarrow’, call `stream[["release"]]()` instead.

See the [‘arrow’ R package](https://arrow.apache.org/docs/r/) for more
information on working with Arrow streams and record batches.
