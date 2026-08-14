# Write an R or Arrow object to an Eventhouse table

Serializes an R or Arrow object to Parquet, uploads it to the OneLake
staging folder advertised by the Kusto ingestion service, submits
tracked queued ingestion, waits for the terminal per-file result, and
removes staging only after a confirmed success.

## Usage

``` r
fabric_kql_write_table(
  cluster,
  table,
  data,
  database = NULL,
  mapping = NULL,
  staging_folder = NULL,
  staging_root = "fabricqueryr-staging",
  cleanup = TRUE,
  keep_staging_on_failure = TRUE,
  compression = "snappy",
  target_file_size = 512 * 1024^2,
  max_rows_per_file = NULL,
  tags = character(),
  ingest_if_not_exists = character(),
  skip_batching = FALSE,
  creation_time = NULL,
  timeout = 900,
  poll_interval = 2,
  error_on_failure = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  allow_custom_endpoint = FALSE,
  create_if_missing = FALSE,
  column_types = NULL,
  query_cluster = NULL,
  .sleep = Sys.sleep,
  .now = Sys.time
)
```

## Arguments

- cluster:

  Ingestion URI or Eventhouse/KQLDatabase discovery record; see
  [`fabric_kql_ingest()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md).

- table:

  Target KQL table name.

- data:

  Data frame, tibble, Arrow Table/RecordBatch, lazy Arrow
  Dataset/Scanner/query, Arrow RecordBatchReader, or compatible array
  stream.

- database:

  Target KQL database name. Omit for a discovered KQLDatabase.

- mapping:

  Optional predefined Parquet ingestion mapping name.

- staging_folder:

  Optional trusted OneLake folder URI beginning below an item's `Files/`
  area. The ingestion configuration's lake folder is used by default.

- staging_root:

  Relative directory created below the selected lake folder for package
  staging.

- cleanup:

  Remove the unique staging directory after confirmed success.

- keep_staging_on_failure:

  Retain staging after a confirmed terminal Kusto failure. Ambiguous
  failures are always retained.

- compression:

  Parquet compression supported by
  [`arrow::write_parquet()`](https://arrow.apache.org/docs/r/reference/write_parquet.html).

- target_file_size:

  Soft maximum bytes per staged Parquet file. The service's advertised
  total-size and blob-count limits are still enforced.

- max_rows_per_file:

  Optional exact maximum rows per staged file.

- tags, ingest_if_not_exists, skip_batching, creation_time:

  Ingestion properties passed to
  [`fabric_kql_ingest()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md).

- timeout:

  Positive number of seconds allowed for submission and tracked status
  waiting after upload.

- poll_interval:

  Minimum seconds between ingestion status requests.

- error_on_failure:

  Raise a typed error for a confirmed failed or canceled ingestion. Set
  `FALSE` to return the failed result and its staging disposition.

- tenant_id:

  Microsoft Entra tenant ID.

- client_id:

  Microsoft Entra application/client ID.

- token:

  Optional access token or audience-aware token-provider function.

- auth_args:

  Additional options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- allow_custom_endpoint:

  Permit a trusted non-Microsoft Kusto origin.

- create_if_missing:

  Whether to create a missing KQL table from the Arrow schema before
  staging. Existing tables are left unchanged.

- column_types:

  Optional named character vector giving one Kusto scalar type for every
  data column when `create_if_missing = TRUE`. Supported canonical types
  are `bool`, `datetime`, `decimal`, `dynamic`, `guid`, `int`, `long`,
  `real`, `string`, and `timespan`. `NULL` infers them.

- query_cluster:

  Optional Kusto query-service URI or discovery record used for table
  creation. A discovered `cluster` already carries this URI; a standard
  Microsoft ingestion URI is converted to its paired query URI. Supply
  this explicitly for a trusted custom ingestion endpoint.

- .sleep, .now:

  Internal deterministic polling hooks.

## Value

A `fabric_kql_write_result` containing row/byte/file counts, normalized
ingestion status, tracking handle, source IDs, and staging disposition.

## One-call staging workflow

The queued-ingestion REST API accepts storage blobs rather than inline R
values. This function provides the higher-level one-call workflow: it
reads the ingestion service's preview configuration, chooses a trusted
OneLake lake folder, creates a unique `fabricqueryr-staging` path,
uploads bounded Parquet parts, and submits them with `;impersonate`
storage authentication. `staging_folder` can override the advertised
folder with a trusted OneLake `Files/` URI.

The caller therefore needs Kusto Table Ingestor and Database User
access, plus write/delete access to the selected OneLake folder. The
Eventhouse ingestion service must be able to read those files as the
caller.

## R and Arrow inputs

Data frames and tibbles are converted through Arrow. Factors become
strings; complex and `difftime` columns require an explicit conversion.
Arrow Tables, RecordBatches, Datasets, Scanners, `arrow_dplyr_query`
objects, and RecordBatchReaders are accepted, as are Arrow-compatible
`nanoarrow_array_stream` objects returned by package query helpers. Lazy
inputs are read one record batch at a time and written directly to a
temporary Parquet parts, so the complete data set is never collected
into R memory. A supplied reader or stream is single-use and is
consumed.

Parquet identity mapping matches source fields to existing KQL columns
by case-sensitive name. Supply `mapping` when the Parquet schema and
table need an explicit predefined mapping.

Set `create_if_missing = TRUE` to issue Kusto's idempotent
`.create table` command before staging. A missing table is created from
the Arrow schema; an existing table is returned unchanged, so this
option never alters an existing schema. Common Arrow scalar and nested
types are inferred as Kusto types. Supply a named `column_types` vector
to override every column type.

## Failure and cleanup safety

A successful tracked ingestion is cleaned up by default. A submission
error, polling timeout, or other ambiguous result always retains staging
because Kusto may still be reading it. A confirmed terminal ingestion
failure retains staging by default and can remove it with
`keep_staging_on_failure = FALSE`. The retained full OneLake path is
carried by `fabric_kql_write_error` conditions. A transport failure
during OneLake's final atomic rename can also leave the unique
destination present; upload errors report `staging_retained = NA` and
the path to inspect.

## References

[Queued ingestion configuration REST API
(preview)](https://learn.microsoft.com/en-us/kusto/management/data-ingestion/queued-ingest-configuration-http?view=microsoft-fabric)

[Queued ingestion REST API
(preview)](https://learn.microsoft.com/en-us/kusto/management/data-ingestion/queued-ingest-use-http?view=microsoft-fabric)

[Create a Kusto
table](https://learn.microsoft.com/en-us/kusto/management/create-table-command?view=microsoft-fabric)

[Kusto scalar data
types](https://learn.microsoft.com/en-us/kusto/query/scalar-data-types/?view=microsoft-fabric)

[Kusto Parquet
mappings](https://learn.microsoft.com/en-us/kusto/management/parquet-mapping?view=microsoft-fabric)

[OneLake ADLS-compatible
access](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)

[Arrow
RecordBatchReader](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html)

[Arrow Parquet
writer](https://arrow.apache.org/docs/r/reference/ParquetFileWriter.html)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the KQL database that will receive the R data
workspace <- fabric_workspaces()[[1L]]
database <- fabric_kql_databases(workspace)[[1L]]

# Create a new table when needed, stage the data, and wait for ingestion
result <- fabric_kql_write_table(
  database,
  table = "EventsFromR",
  data = data.frame(id = 1:3, value = c("a", "b", "c")),
  create_if_missing = TRUE,
  ingest_if_not_exists = "r-batch-2026-08-14"
)
result$status$state

# A local Arrow Dataset is scanned batch by batch rather than collected
dataset <- arrow::open_dataset(Sys.getenv("ARROW_DATASET_PATH"))
fabric_kql_write_table(database, "EventsFromArrow", dataset)
} # }
```
