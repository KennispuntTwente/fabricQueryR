# Bring R data into Microsoft Fabric

‘fabricQueryR’ can also be used to ingest data into Microsoft Fabric. In
this guide, it is shown you can move R data frames, Arrow objects, or
files into Microsoft Fabric.

## Choose a destination

| What you want in Fabric | Start with | Good fit |
|----|----|----|
| A managed Lakehouse Delta table | [`fabric_lakehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md) | General analytics and data-engineering tables |
| An ordinary file in OneLake | [`fabric_onelake_write_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md) | Exchange files, exports, and non-tabular artifacts |
| A relational Warehouse table | [`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md) | SQL reporting and warehouse workloads |
| An Eventhouse KQL table | [`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md) | Event, log, and time-series data |
| A Lakehouse table from files already in `Files/` | [`fabric_lakehouse_load_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md) | Existing CSV or Parquet staging data |

For a first ingestion, a Lakehouse table is the most direct
general-purpose workflow. The high-level writers accept ordinary data
frames and handle their own temporary Parquet staging.

## Prepare a small R object

``` r

library(fabricQueryR)

orders <- data.frame(
  order_id = 1:3,
  order_date = as.Date(c("2026-08-12", "2026-08-13", "2026-08-14")),
  amount = c(10.50, 20, 30.25)
)

workspace <- fabric_workspaces()[["Analytics workspace"]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
```

Use a simple object and a clearly disposable destination while learning.
This makes column names, inferred types, and replacement behavior easy
to inspect.

## Write a Lakehouse table

``` r

write_result <- fabric_lakehouse_write_table(
  lakehouse,
  table = "orders_from_r",
  data = orders,
  mode = "Overwrite"
)

write_result$rows
write_result$staging_retained
```

The function writes temporary Parquet files, loads them as a managed
Delta table, waits for Fabric to finish, and removes successful staging
files. It can create the destination table; Fabric infers its columns
from the source.

Read back a few rows to verify the result:

``` r

check <- fabric_lakehouse_read_table(
  lakehouse,
  table = "orders_from_r",
  limit = 10L
)
check
```

Use `mode = "Append"` only when the source columns are compatible with
an existing table. `mode = "Overwrite"` replaces the table through
Fabric’s managed load behavior.

## Write an ordinary OneLake file

A file is different from a managed table. Choose this route when another
process expects a specific file or when the content is not tabular:

``` r

fabric_onelake_write_file(
  workspace,
  lakehouse,
  path = "Files/exports/orders.parquet",
  data = orders
)
```

[`fabric_onelake_write_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md)
serializes supported R or Arrow objects. Use
[`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
when a file already exists on local disk:

``` r

fabric_onelake_upload(
  workspace,
  lakehouse,
  path = "Files/incoming/orders.csv",
  source = "orders.csv"
)
```

Do not upload directly below a managed table’s `Tables/` directory.
Delta tables contain a transaction log and must be changed through a
table-aware writer.

## Write a Warehouse table

A Warehouse writer uses a Lakehouse `Files/` directory for temporary
staging, then asks the Warehouse to load it efficiently:

``` r

warehouse <- fabric_warehouses(workspace)[[1L]]

warehouse_result <- fabric_warehouse_write_table(
  warehouse,
  table = "orders_from_r",
  data = orders,
  staging_lakehouse = lakehouse,
  schema = "dbo",
  create_if_missing = TRUE,
  mode = "Append"
)
```

For a missing table, Fabric can infer a basic definition. Pre-create the
table when exact SQL types, lengths, constraints, or grants matter.
[Working with Fabric
WareHouses](https://kennispunttwente.github.io/fabricQueryR/articles/warehouse-write.md)
explains overwrite choices, transactions, and larger Arrow inputs.

## Write an Eventhouse table

Use Eventhouse for event or time-series data that will be queried with
KQL:

``` r

kql_database <- fabric_kql_databases(workspace)[[1L]]

kql_result <- fabric_kql_write_table(
  kql_database,
  table = "OrdersFromR",
  data = orders,
  create_if_missing = TRUE
)

kql_result$status$state
```

The high-level writer stages the R object, submits tracked ingestion,
waits, and cleans up after confirmed success. [Working with Fabric
EventHouses (real-time
data)](https://kennispunttwente.github.io/fabricQueryR/articles/eventhouse-ingestion.md)
covers predefined mappings, existing storage files, idempotency keys,
and failure recovery.

## Load a file that is already in a Lakehouse

If CSV or Parquet data already exists below the same Lakehouse’s
`Files/` area, you can load it without downloading it to R:

``` r

operation <- fabric_lakehouse_load_table(
  lakehouse,
  table = "orders_from_file",
  path = "Files/incoming/orders.csv",
  format = "Csv",
  header = TRUE,
  mode = "Overwrite"
)

completed <- fabric_operation_wait(operation, timeout = 900)
```

This route is useful for file-based pipelines. It does not upload a
local file; use
[`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
first when necessary.

## Choose append and overwrite deliberately

These names describe intent, but the exact behavior depends on the
destination:

| Choice | Use it when | Check first |
|----|----|----|
| Append | New rows belong beside existing rows | Column names and types are compatible; re-running will not duplicate data |
| Overwrite | The supplied data should replace the current contents | Which table definition, grants, or history the workload preserves |
| Create if missing | A basic inferred table is acceptable | Inferred types and nullability meet downstream needs |

Warehouse drop replacement can discard table-specific metadata.
Eventhouse queued ingestion has at-least-once delivery. Read the linked
destination-specific guide before using these paths in an automated
production workflow.

## Scale up with Arrow

The Lakehouse, Warehouse, and Eventhouse writers accept Arrow Tables and
RecordBatches. They can also consume lazy Arrow Datasets, Scanners,
queries, and streams in batches, without collecting the full input as an
R data frame:

``` r

dataset <- arrow::open_dataset("local-parquet-directory")

fabric_lakehouse_write_table(
  lakehouse,
  table = "large_orders",
  data = dataset
)
```

Start with the default part sizes. Tune file or row boundaries only
after measuring a real workload. For transformations that need
distributed compute or exact Spark-managed schemas, use [Working with
Livy
(Spark)](https://kennispunttwente.github.io/fabricQueryR/articles/spark-with-livy.md)
rather than moving all intermediate data through R.
