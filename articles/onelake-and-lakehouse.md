# Working with Fabric Lakehouses and OneLake

A Fabric Lakehouse keeps ordinary files and managed tables together in
OneLake:

- `Files/` contains ordinary files such as CSV, Parquet, JSON, images,
  and scripts.
- `Tables/` contains managed Delta tables that Fabric engines can query.

Use the SQL analytics endpoint for familiar SQL queries against managed
tables. Use the Lakehouse and OneLake helpers when you want to work
directly with a table or file. This guide starts with a small SQL query,
then moves from ordinary files to managed tables and, finally, large or
historical reads.

## Find and connect to a Lakehouse

Start by finding the workspace and Lakehouse by name:

``` r

library(fabricQueryR)

workspaces <- fabric_workspaces()
matches <- Filter(
  \(x) identical(x$displayName, "Analytics workspace"),
  workspaces
)
stopifnot(length(matches) == 1L)
workspace <- matches[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]

lakehouse$displayName
```

Fabric provides a SQL analytics endpoint for querying the managed tables
under `Tables/`. It does not query ordinary files under `Files/`. The
endpoint address is already included in `lakehouse`, so you do not need
to copy it from the Fabric portal.

For a single SQL query, pass `lakehouse` directly to
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md):

``` r

orders <- fabric_sql_query(
  lakehouse,
  "SELECT TOP 10 * FROM dbo.orders"
)
```

The function opens and closes the SQL connection for you. If you want to
run several commands with ‘DBI’, open a reusable connection instead:

``` r

con <- fabric_sql_connect(lakehouse)
DBI::dbListTables(con)
DBI::dbGetQuery(con, "SELECT TOP 10 * FROM dbo.orders")
DBI::dbDisconnect(con)
```

For most work with managed tables, start with SQL. Use
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
for filters, joins, and summaries, or
[`fabric_sql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_tables.md)
to read one table without writing SQL. Use
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
only when you want to keep a connection open for several ‘DBI’ calls.

You can also read a Delta table directly through OneLake with
[`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md).
This is useful for an Arrow stream or an earlier table version. The
`fabric_onelake_*()` functions work with ordinary files under `Files/`,
while
[`fabric_lakehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
adds or replaces managed table data. These functions do not need a SQL
connection. Never change the files underneath `Tables/` directly because
they are part of a managed Delta table.

## List and read ordinary files

Start by listing a small folder:

``` r

files <- fabric_onelake_list(
  workspace,
  lakehouse,
  path = "Files/incoming"
)

files[c("path", "is_directory", "content_length")]
```

Read a supported tabular file directly into R:

``` r

orders <- fabric_onelake_read_file(
  workspace,
  lakehouse,
  path = "Files/incoming/orders.csv"
)

head(orders)
```

Use
[`fabric_onelake_metadata()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
for size and other properties without reading the contents. Use
[`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
for a file type that should stay as bytes or be saved to local disk.

## Write or upload a file

[`fabric_onelake_write_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md)
turns an R or Arrow object into CSV, Parquet, or Arrow IPC content based
on the path or explicit format:

``` r

fabric_onelake_write_file(
  workspace,
  lakehouse,
  path = "Files/exports/orders.parquet",
  data = data.frame(
    order_id = 1:3,
    amount = c(10.5, 20, 30.25)
  )
)
```

If the file already exists on your computer, upload it without first
parsing it in R:

``` r

fabric_onelake_upload(
  workspace,
  lakehouse,
  path = "Files/incoming/logo.png",
  source = "logo.png"
)
```

Both operations can replace a destination. Use a new path while learning
and check
[`?fabric_onelake_write_file`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md)
or
[`?fabric_onelake_upload`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
before replacing shared data.

## Discover and read managed tables

``` r

tables <- fabric_lakehouse_tables(lakehouse)
tables[c("schema", "name", "type", "format")]
```

Select a discovered row or supply the table name:

``` r

table <- tables[1L, ]

rows <- fabric_lakehouse_read_table(
  lakehouse,
  table,
  columns = c("order_id", "amount"),
  limit = 100L
)
```

Column selection and row limits happen before the result is fully
collected in R. For SQL joins, grouping, or complex filters, use
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
against the Lakehouse SQL analytics endpoint instead.

## Write an R object as a managed table

``` r

result <- fabric_lakehouse_write_table(
  lakehouse,
  table = "orders_from_r",
  data = data.frame(
    order_id = 1:3,
    amount = c(10.5, 20, 30.25)
  ),
  mode = "Overwrite"
)

result$rows
result$staging_retained
```

The function stages Parquet files under a unique `Files/` path, asks
Fabric to perform a managed Delta load, waits for completion, and cleans
up after confirmed success. Fabric can infer a missing table’s schema.
Use Spark or a separately schema-controlled process when inference is
not suitable.

## Turn existing files into a table

When CSV or Parquet files already exist in the Lakehouse, ask Fabric to
load them directly:

``` r

operation <- fabric_lakehouse_load_table(
  lakehouse,
  table = "orders_from_csv",
  path = "Files/incoming/orders.csv",
  format = "Csv",
  header = TRUE,
  delimiter = ",",
  mode = "Overwrite"
)

operation <- fabric_operation_wait(operation, timeout = 900)
```

The source can be one file or a folder. Folder loads can filter by
extension and include nested folders. The source must already be inside
the selected Lakehouse’s `Files/` area.

## Read a large or historical Delta table

Use an Arrow stream when the selected data may not fit comfortably in
memory:

``` r

stream <- fabric_lakehouse_read_table(
  lakehouse,
  table = "large_orders",
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
orders <- reader$read_table()
reader$Close()
```

The stream is disk-backed and single-use. Close the Arrow reader when
finished so its staged temporary file is deleted; if you consume the
‘nanoarrow’ stream directly, call `stream[["release"]]()`. Do not rely
on garbage collection for this cleanup. `version` can select an earlier
Delta table version when that history is still available:

``` r

older_rows <- fabric_lakehouse_read_table(
  lakehouse,
  table = "orders",
  version = 42L,
  limit = 100L
)
```

[`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
is the lower-level reader for compatible Delta paths in Lakehouses and
Warehouses. Prefer the Lakehouse wrapper for a normal discovered-table
workflow.
