# Discover and read tables through a Fabric SQL endpoint

These helpers provide a target-independent metadata and table-read layer
for Fabric SQL endpoints. They accept Lakehouse, Warehouse, Warehouse
snapshot, and SQL Database records, or the same direct server inputs as
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md).
Discovery uses SQL catalog metadata views and is limited by the caller's
SQL metadata permissions.

## Usage

``` r
fabric_sql_tables(
  server,
  schema = NULL,
  detail = TRUE,
  database = NULL,
  target_type = c("auto", "lakehouse", "warehouse", "sql_database",
    "sql_analytics_endpoint"),
  backend = c("odbc", "adbc"),
  token = NULL,
  ...
)

fabric_sql_views(
  server,
  schema = NULL,
  detail = TRUE,
  database = NULL,
  target_type = c("auto", "lakehouse", "warehouse", "sql_database",
    "sql_analytics_endpoint"),
  backend = c("odbc", "adbc"),
  token = NULL,
  ...
)

fabric_sql_read_table(
  server,
  table,
  schema = NULL,
  columns = NULL,
  limit = NULL,
  result = c("tibble", "arrow_stream"),
  database = NULL,
  target_type = c("auto", "lakehouse", "warehouse", "sql_database",
    "sql_analytics_endpoint"),
  backend = c("odbc", "adbc"),
  token = NULL,
  ...
)
```

## Arguments

- server:

  Fabric SQL endpoint, portal connection string, or discovered
  SQL-capable item record.

- schema:

  Optional schema filter. `fabric_sql_read_table()` defaults to the
  schema in a discovered table row, otherwise `"dbo"`.

- detail:

  Whether table or view discovery should retrieve column metadata.

- database:

  Optional catalog/database. An explicit value overrides one discovered
  from `server`.

- target_type:

  Kind of Fabric SQL target. Keep `"auto"` unless a custom hostname
  prevents automatic identification.

- backend:

  SQL driver backend, either `"odbc"` or `"adbc"`.

- token:

  Optional SQL access token or audience-aware token-provider function.

- ...:

  Additional authentication, driver, endpoint, timeout, verbosity, and
  retry options passed to
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md).
  Query text, parameters, read-only status, and idempotency are
  controlled by these helpers and cannot be supplied here.

- table:

  Table or view name, or a one-row record containing `name` and
  optionally `schema`.

- columns:

  Optional unique column names to project.

- limit:

  Optional non-negative maximum number of rows to return.

- result:

  Result representation for `fabric_sql_read_table()`; either a tibble
  or a single-use Arrow stream.

## Value

`fabric_sql_tables()` and `fabric_sql_views()` return a tibble with
object `name`, `schema`, `full_name`, `type`, optional view
`definition`, list-column `columns`, and the unmodified discovery row in
`raw`. `fabric_sql_read_table()` returns a tibble or
`nanoarrow_array_stream`.

## References

[System information schema
views](https://learn.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/system-information-schema-views-transact-sql)

[SQL module
definitions](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-sql-modules-transact-sql)

[Fabric SQL analytics
endpoints](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)

## Examples

``` r
if (FALSE) { # \dontrun{
workspace <- fabric_workspaces()[[1L]]
warehouse <- fabric_warehouses(workspace)[[1L]]

tables <- fabric_sql_tables(warehouse, schema = "dbo")
rows <- fabric_sql_read_table(warehouse, tables[1L, ], limit = 1000)
views <- fabric_sql_views(warehouse)
} # }
```
