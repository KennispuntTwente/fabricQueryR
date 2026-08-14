# Write R and Arrow data to Fabric Warehouse

[`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md)
moves an ordinary R object or a lazy Arrow source into an existing
Microsoft Fabric Warehouse table. It stages bounded Parquet parts in
OneLake and submits Fabric’s recommended high-throughput `COPY INTO`
statement over a normal Entra-authenticated SQL connection.

## Discover the destination and staging item

The target table must already exist. Fabric supports OneLake as a
`COPY INTO` source, but its current contract explicitly excludes
Warehouse items as source locations. Supply a Lakehouse for the
temporary `Files/` directory:

``` r

library(fabricQueryR)

workspace <- Filter(
  function(value) value$displayName == "Analytics",
  fabric_workspaces()
)[[1L]]
warehouse <- fabric_warehouses(workspace)[[1L]]
staging_lakehouse <- fabric_lakehouses(workspace)[[1L]]
```

The SQL identity needs `INSERT` and
`ADMINISTER DATABASE BULK OPERATIONS` on the Warehouse. For a OneLake
source, Microsoft also documents Contributor or higher access on the
source and destination workspaces. Cross-workspace staging is supported
in the same tenant by setting `staging_workspace` when the Lakehouse is
supplied by name or ID.

## Append an R data frame

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

Column identifiers are quoted and mapped in the same ordinal order as
the Parquet schema. The names and compatible types therefore need to
match the existing destination columns. Factor columns are serialized as
strings; Arrow retains supported 64-bit integers, dates, timestamps,
decimals, and nulls.

## Stream a larger-than-memory Arrow source

Arrow Datasets, Scanners, dplyr queries, RecordBatchReaders, Tables, and
Arrow-compatible streams use the same function. They are consumed one
record batch at a time rather than collected into an R data frame:

``` r

dataset <- arrow::open_dataset("local-parquet-directory")

written <- fabric_warehouse_write_table(
  warehouse,
  table = "orders",
  data = dataset,
  staging_lakehouse = staging_lakehouse,
  target_file_size = 512 * 1024^2,
  max_rows_per_file = NULL
)
```

`target_file_size` is a soft compressed-size boundary. The 512 MB
default is inside Fabric’s documented 100 MB to 1 GB performance range.
Use `max_rows_per_file` when an exact and deterministic part boundary
matters.

## Replace a table safely

``` r

replaced <- fabric_warehouse_write_table(
  warehouse,
  table = "orders",
  data = replacement,
  staging_lakehouse = staging_lakehouse,
  mode = "Overwrite"
)
```

Overwrite starts an explicit Fabric Warehouse transaction, truncates the
existing table, executes `COPY INTO`, and commits only after the load
succeeds. Fabric Warehouse documents ACID transactions and
all-or-nothing rollback for grouped write operations.

Local temporary files are always removed. The OneLake staging directory
is removed only after confirmed success. If SQL delivery becomes
ambiguous, it is retained even when `keep_staging_on_failure = FALSE`;
the error condition contains `staging_path`, `staging_retained`, and
`ambiguous` fields for recovery and diagnosis.

## Microsoft documentation

- [COPY INTO in Fabric
  Warehouse](https://learn.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=fabric)
- [Warehouse performance
  guidelines](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance)
- [Warehouse
  transactions](https://learn.microsoft.com/en-us/fabric/data-warehouse/transactions)
