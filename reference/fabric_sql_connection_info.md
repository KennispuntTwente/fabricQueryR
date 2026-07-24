# Parse a Microsoft Fabric SQL target

Normalizes a bare Fabric SQL endpoint, a complete portal connection
string, or one enriched discovery record into connection information
used by
[`fabric_sql_connect()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_sql_connect.md).

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

  A character endpoint/connection string, or one Lakehouse, Warehouse,
  or SQL Database record returned by a discovery function.

- database:

  Optional catalog/database. An explicit value overrides a catalog found
  in `server`.

- target_type:

  Target kind. `"auto"` infers it from discovery metadata or the
  endpoint hostname.

- port:

  Optional TCP port. An explicit value overrides a port in `server`;
  otherwise port 1433 is used.

## Value

A `fabric_sql_connection_info` list with `server`, `database`, `port`,
and `target_type`.
