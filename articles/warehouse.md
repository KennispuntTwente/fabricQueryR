# Working with Fabric Warehouses

A Fabric Warehouse stores data in relational tables, much like a
traditional SQL database. You normally use its SQL endpoint to read and
query those tables. This guide first connects to a Warehouse, then shows
how to add or replace data from R.

## Find and connect to a Warehouse

Start by finding the workspace and Warehouse by name. The returned
`warehouse` is a read-only `FabricWarehouse` R6 object. It keeps its
Fabric fields, IDs, SQL connection details, and credential, and provides
methods for the useful next actions:

``` r

library(fabricQueryR)

workspaces <- fabric_workspaces()
matches <- Filter(\(x) identical(x$displayName, "Analytics"), workspaces)
stopifnot(length(matches) == 1L)
workspace <- matches[[1L]]
warehouse <- workspace$warehouses()[[1L]]
```

`$warehouses()` is the workspace method for
[`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).

The SQL endpoint is the address that database tools use to reach the
Warehouse. Because it is included in `warehouse`, you do not need to
find or copy that address from the Fabric portal.

For a single query, call `$sql_query()`
([`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)):

``` r

orders <- warehouse$sql_query(
  "SELECT TOP 10 * FROM dbo.orders"
)
```

The method opens and closes the SQL connection for you. If you want to
run several commands with ‘DBI’, use `$sql_connect()`
([`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)):

``` r

con <- warehouse$sql_connect()
DBI::dbListTables(con)
DBI::dbGetQuery(con, "SELECT TOP 10 * FROM dbo.orders")
DBI::dbDisconnect(con)
```

For a simple read, `$read_table()`
([`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md))
lets you name a table and optionally select columns or limit the rows,
without writing SQL. It uses SQL internally.
[`fabric_sql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_tables.md)
is the more general version for any supported Fabric SQL item.

Use `$sql_query()`
([`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md))
when you need filters, joins, grouping, or other SQL. Use
`$sql_connect()`
([`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md))
when you want to keep a connection open for several ‘DBI’ calls. To add
or replace many rows from an R data frame or Arrow source, use
`$write_table()`
([`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md)).
See
[`vignette("reading-data")`](https://kennispunttwente.github.io/fabricQueryR/articles/reading-data.md)
for more reading examples.

## Prepare a staging Lakehouse for writes

Writing to a Warehouse also requires a Lakehouse in the same tenant. The
package temporarily stores files there while Fabric loads them, then
removes the files after a confirmed successful write:

``` r

staging_lakehouse <- workspace$lakehouses()[[1L]]
```

`$lakehouses()` is the workspace method for
[`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).

## Add rows to an existing table

Suppose `dbo.orders` already contains order data. This call adds three
new rows to the table; it does not remove or change the rows already
there. The existing table columns must match the R data frame. Use
`$write_table()`
([`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md)):

``` r

written <- warehouse$write_table(
  table = "orders",
  data = data.frame(
    id = 1:3,
    label = c("alpha", "beta", "gamma"),
    amount = c(10.5, NA, 30)
  ),
  staging_lakehouse = staging_lakehouse,
  schema = "dbo",
  mode = "Append"
)

written$rows
written$file_count
written$staging_retained
```

If you want to create a new table, use the same `$write_table()`
([`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md))
method with `create_if_missing = TRUE`:

``` r

created <- warehouse$write_table(
  table = "orders_from_r",
  data = orders,
  staging_lakehouse = staging_lakehouse,
  create_if_missing = TRUE
)
```

## Replace table data

Use overwrite mode with `$write_table()`
([`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md))
when the new data should replace the current rows:

``` r

replaced <- warehouse$write_table(
  table = "orders",
  data = replacement,
  staging_lakehouse = staging_lakehouse,
  mode = "Overwrite",
  overwrite_method = "Truncate"
)
```

The default `"Truncate"` method preserves the existing table definition.
Use `"Drop"` only when Fabric should infer a new definition from the
incoming data. This is another `$write_table()`
([`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md))
call:

``` r

recreated <- warehouse$write_table(
  table = "orders",
  data = replacement,
  staging_lakehouse = staging_lakehouse,
  mode = "Overwrite",
  overwrite_method = "Drop",
  create_if_missing = TRUE
)
```

Dropping a table also removes its table-specific constraints, indexes,
and grants. Prefer `"Truncate"` unless recreating the definition is
intentional. The writer removes staging files after confirmed success
and reports a retained staging path when manual recovery may be needed.

## Scale up with Arrow

The same writer accepts Arrow Datasets, Scanners, ‘dplyr’ queries,
RecordBatchReaders, Tables, and Arrow-compatible streams. These sources
are processed in batches instead of first being collected into an R data
frame. The method remains `$write_table()`
([`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md)):

``` r

dataset <- arrow::open_dataset("local-parquet-directory")

written <- warehouse$write_table(
  table = "orders",
  data = dataset,
  staging_lakehouse = staging_lakehouse
)
```

See
[`?fabric_warehouse_write_table`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md)
when you need to tune staged file sizes or recover a retained staging
directory after a failed write.

## More information

- [COPY INTO in Fabric
  Warehouse](https://learn.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=fabric)
- [Create tables in Fabric
  Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/create-table)
- [Query Parquet
  files](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-parquet-files)
- [Warehouse performance
  guidelines](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance)
- [Warehouse
  transactions](https://learn.microsoft.com/en-us/fabric/data-warehouse/transactions)
