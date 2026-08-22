# Working with Fabric Warehouses

A Fabric Warehouse stores data in relational tables, much like a
traditional SQL database. You normally use its SQL endpoint to read and
query those tables. This guide first connects to a Warehouse, then shows
how to add or replace data from R.

## Find and connect to a Warehouse

Start by finding the workspace and Warehouse by name. The returned
`warehouse` object contains the IDs and SQL connection details that the
other functions need:

``` r

library(fabricQueryR)

workspaces <- fabric_workspaces()
matches <- Filter(\(x) identical(x$displayName, "Analytics"), workspaces)
stopifnot(length(matches) == 1L)
workspace <- matches[[1L]]
warehouse <- fabric_warehouses(workspace)[[1L]]
```

The SQL endpoint is the address that database tools use to reach the
Warehouse. Because it is included in `warehouse`, you do not need to
find or copy that address from the Fabric portal.

For a single query, pass that object directly to
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md):

``` r

orders <- fabric_sql_query(
  warehouse,
  "SELECT TOP 10 * FROM dbo.orders"
)
```

The function opens and closes the SQL connection for you. If you want to
run several commands with ‘DBI’, open a reusable connection instead:

``` r

con <- fabric_sql_connect(warehouse)
DBI::dbListTables(con)
DBI::dbGetQuery(con, "SELECT TOP 10 * FROM dbo.orders")
DBI::dbDisconnect(con)
```

For a simple read,
[`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md)
lets you name a table and optionally select columns or limit the rows,
without writing SQL. It uses SQL internally.
[`fabric_sql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_tables.md)
is the more general version for any supported Fabric SQL item.

Use
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
when you need filters, joins, grouping, or other SQL. Use
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
when you want to keep a connection open for several ‘DBI’ calls. To add
or replace many rows from an R data frame or Arrow source, use
[`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md).
See
[`vignette("reading-data")`](https://kennispunttwente.github.io/fabricQueryR/articles/reading-data.md)
for more reading examples.

## Prepare a staging Lakehouse for writes

Writing to a Warehouse also requires a Lakehouse in the same tenant. The
package temporarily stores files there while Fabric loads them, then
removes the files after a confirmed successful write:

``` r

staging_lakehouse <- fabric_lakehouses(workspace)[[1L]]
```

## Add rows to an existing table

Suppose `dbo.orders` already contains order data. This call adds three
new rows to the table; it does not remove or change the rows already
there. The existing table columns must match the R data frame:

``` r

written <- fabric_warehouse_write_table(
  warehouse,
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

If you want to create a new table, you can use
`create_if_missing = TRUE`:

``` r

created <- fabric_warehouse_write_table(
  warehouse,
  table = "orders_from_r",
  data = orders,
  staging_lakehouse = staging_lakehouse,
  create_if_missing = TRUE
)
```

## Replace table data

Use overwrite mode when the new data should replace the current rows:

``` r

replaced <- fabric_warehouse_write_table(
  warehouse,
  table = "orders",
  data = replacement,
  staging_lakehouse = staging_lakehouse,
  mode = "Overwrite",
  overwrite_method = "Truncate"
)
```

The default `"Truncate"` method preserves the existing table definition.
Use `"Drop"` only when Fabric should infer a new definition from the
incoming data:

``` r

recreated <- fabric_warehouse_write_table(
  warehouse,
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
frame:

``` r

dataset <- arrow::open_dataset("local-parquet-directory")

written <- fabric_warehouse_write_table(
  warehouse,
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
