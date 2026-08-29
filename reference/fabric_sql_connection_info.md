# Get connection details for a Fabric SQL item

Shows the server, database, port, and item type that 'fabricQueryR' will
use for a Fabric SQL connection. Most users can pass a discovered item
directly to
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
and do not need to call this helper

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
  Database object returned by a discovery function. A discovered object
  is usually simplest because it also supplies the database name

- database:

  Optional catalog/database. An explicit value overrides a database
  found in `server`. For a bare endpoint, supply the item database shown
  with its connection string in Fabric. If omitted, Warehouse and SQL
  analytics endpoints open Fabric's `master` context, which is useful
  for discovery but does not select the item's tables

- target_type:

  Kind of Fabric SQL item. Keep `"auto"` unless a custom hostname
  prevents 'fabricQueryR' from identifying it

- port:

  Optional TCP port. An explicit value overrides a port in `server`;
  otherwise the standard SQL port, 1433, is used

## Value

A `fabric_sql_connection_info` list with `server`, `database`, `port`,
`target_type`, and `source` (whether the input was text or a discovery
object). No connection is opened

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover a Warehouse object that already contains its SQL endpoint
workspace <- fabric_workspaces()[[1L]]
# `$warehouses()` calls fabric_warehouses()
warehouse <- workspace$warehouses()[[1L]]

# Inspect connection details without opening a database connection
info <- fabric_sql_connection_info(warehouse)
info[c("server", "database", "port", "target_type")]
} # }
```
