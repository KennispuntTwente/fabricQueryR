# Parse a Microsoft Fabric SQL target

Converts the different ways Fabric identifies a SQL endpoint into one
consistent set of connection values. Most users can pass a discovered
item directly to
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
and do not need to call this function.

## Usage

``` r
fabric_sql_connection_info(
  server,
  database = NULL,
  target_type = c("auto", "lakehouse", "warehouse", "sql_database",
    "sql_analytics_endpoint"),
  port = NULL
)
```

## Arguments

- server:

  A Fabric SQL server name, a complete connection string copied from the
  Fabric portal, or one Lakehouse, Warehouse, Warehouse snapshot, or SQL
  Database record returned by a discovery function. A discovered record
  is usually simplest because it also supplies the database name.

- database:

  Optional catalog/database. An explicit value overrides a database
  found in `server`. For a bare endpoint, supply the item database shown
  with its connection string in Fabric. If omitted, Warehouse and SQL
  analytics endpoints open Fabric's `master` context, which is useful
  for discovery but does not select the item's tables.

- target_type:

  Label for the endpoint kind. Keep `"auto"` unless the hostname is
  custom or ambiguous. The explicit choices distinguish a Lakehouse SQL
  analytics endpoint, Warehouse, transactional SQL Database, or another
  read-only SQL analytics endpoint; they do not convert one kind of
  endpoint into another.

- port:

  Optional TCP port. An explicit value overrides a port in `server`;
  otherwise the standard SQL port, 1433, is used.

## Value

A `fabric_sql_connection_info` list with `server`, `database`, `port`,
`target_type`, and `source` (whether the input was text or a discovery
record). No connection is opened.
