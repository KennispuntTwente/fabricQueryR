# Bring Fabric data into R

Reading data means selecting data stored or served by Fabric and
returning it to your local R session. For ordinary analysis, the result
is usually a **tibble** (a modern R data frame). It’s also possible to
get an Arrow stream for larger-than-memory data.

The best method depends mainly on where the data lives and whether you
want whole tables or a filtered result. Start with a small, filtered
read. Move to streaming or Spark only when the data is too large or the
transformation needs distributed compute.

## Choose a reading method

| Your source or goal | Start with | Why |
|----|----|----|
| Warehouse, SQL Database, or a SQL-shaped Lakehouse query | [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md) | Filter and summarize on the server with familiar SQL |
| One Lakehouse Delta table | [`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md) | Read a table without writing SQL |
| One Warehouse table | [`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md) | Read a table by name without writing SQL |
| Eventhouse table or KQL result | [`fabric_kql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_read_table.md) or [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md) | Use the Eventhouse query engine |
| Power BI/Fabric semantic model | [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md) | Use model relationships and measures |
| CSV, Parquet, or Arrow file in OneLake | [`fabric_onelake_read_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md) | Read the file itself |
| API for GraphQL | [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md) | Request the fields exposed by the API |
| A transformation that genuinely needs Spark | [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md) | Run distributed code in Fabric |

SQL is a good default for relational data because Fabric does the
filtering and R receives only the rows you need. A direct table reader
is simpler when you want one table and do not need joins or grouping.

## Discover the data source once

The examples use discovered objects. They carry the IDs and connection
details needed by the read functions:

``` r

library(fabricQueryR)

workspace <- fabric_workspaces()[["Analytics workspace"]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
warehouse <- fabric_warehouses(workspace)[[1L]]
kql_database <- fabric_kql_databases(workspace)[[1L]]
model <- fabric_semantic_models(workspace)[[1L]]
```

Your workspace does not need to contain every item type. Discover only
the source used by your workflow.

## Read with SQL

Use
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
for one read-only `SELECT` query. It opens and closes the connection for
you and returns a tibble:

``` r

recent_orders <- fabric_sql_query(
  warehouse,
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

Put changing values in `params` rather than pasting them into the SQL
text. This handles quoting safely. Use
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
and normal `DBI` functions when several queries should share one
connection:

``` r

con <- fabric_sql_connect(warehouse)
on.exit(DBI::dbDisconnect(con), add = TRUE)

DBI::dbListTables(con)
orders <- DBI::dbGetQuery(con, "SELECT TOP 100 * FROM dbo.orders")
```

The default SQL backend uses Microsoft ODBC Driver 18 for SQL Server.
See
[`?fabric_sql_connect`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
for the ODBC and ADBC setup choices.

## Read one table without writing a query

Lakehouse and Warehouse table readers accept a table name and can limit
the data before it enters R:

``` r

lakehouse_rows <- fabric_lakehouse_read_table(
  lakehouse,
  table = "orders",
  columns = c("order_id", "order_date", "amount"),
  limit = 100L
)

warehouse_rows <- fabric_warehouse_read_table(
  warehouse,
  table = "orders",
  schema = "dbo",
  limit = 100L
)
```

Use `fabric_lakehouse_tables(lakehouse)` when you are unsure which
Lakehouse tables or schemas are available.

## Read Eventhouse data

An Eventhouse is optimized for event, log, and time-series data. Its
query language is KQL. Read a whole table by name for a simple start:

``` r

events <- fabric_kql_read_table(
  kql_database,
  table = "Events",
  limit = 100L
)
```

Use a KQL query when Fabric should filter or summarize the events first:

``` r

daily_events <- fabric_kql_query(
  kql_database,
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
PowerBi. Semantic models can be queried with DAX (Data Analysis
Expressions):

``` r

sales_by_region <- fabric_pbi_dax_query(
  model,
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
a Lakehouse usually begin with `Files/`:

``` r

orders_file <- fabric_onelake_read_file(
  workspace,
  lakehouse,
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

api <- fabric_graphql_apis(workspace)[[1L]]

response <- fabric_graphql_query(
  api,
  query = "{ products { items { id name category } } }"
)
products <- response$data$products$items
```

Use [Working with
GraphQL](https://kennispunttwente.github.io/fabricQueryR/articles/graphql-schema-and-rows.md)
for schema inspection and pagination.

Spark is the later choice for distributed transformations, Spark-only
data formats, or logic already written for Spark:

``` r

result <- fabric_livy_query(
  lakehouse,
  kind = "sql",
  code = "SELECT category, count(*) AS n FROM orders GROUP BY category"
)
counts <- result$output$parsed
```

Spark has startup cost. Prefer SQL or a direct reader for a small,
ordinary table read.

## Working with large data (Arrow streams)

If you work with large volumes of data (larger than your device’s
working memory), you may need to stream the data into R rather than
reading it all at once.

Several readers accept `result = "arrow_stream"`. This can be used with
the ‘arrow’ R package to read the data in batches:

``` r

stream <- fabric_lakehouse_read_table(
  lakehouse,
  table = "large_orders",
  result = "arrow_stream"
)

reader <- arrow::as_record_batch_reader(stream)
```

See the [Arrow R package](https://arrow.apache.org/docs/r/) for more
information on working with Arrow streams and record batches.
