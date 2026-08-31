# Submit and monitor tracked Eventhouse ingestion

Queue existing blob or OneLake files for ingestion into an existing KQL
table, then inspect or wait for the tracked per-file result. These
functions use Kusto's queued-ingestion REST API, which is currently in
preview

## Usage

``` r
fabric_kql_ingest(
  cluster,
  table,
  sources,
  database = NULL,
  format,
  source_ids = NULL,
  raw_sizes = NULL,
  mapping = NULL,
  tags = character(),
  ingest_if_not_exists = character(),
  ignore_first_record = FALSE,
  skip_batching = FALSE,
  delete_after_download = FALSE,
  creation_time = NULL,
  validation_policy = NULL,
  zip_pattern = NULL,
  timestamp = NULL,
  timeout = 60,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  .deadline = NULL,
  .now = Sys.time
)

fabric_kql_ingestion_status(
  ingestion,
  cluster = NULL,
  database = NULL,
  table = NULL,
  details = TRUE,
  wait = FALSE,
  timeout = 900,
  poll_interval = 2,
  error_on_failure = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  .sleep = Sys.sleep,
  .now = Sys.time,
  .deadline = NULL
)
```

## Arguments

- cluster:

  Ingestion URI, or one Eventhouse or KQLDatabase object from
  [`fabric_eventhouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  [`fabric_kql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).
  A KQLDatabase object also supplies `database`. Use the **Ingestion
  URI**, not the Query URI, for direct character input

- table:

  One existing target KQL table name

- sources:

  Existing blob or OneLake storage connection strings, a data frame of
  source metadata, or a list of source records. See Sources and storage
  access

- database:

  Target KQL database display name. Omit it when `cluster` is a
  discovered KQLDatabase object

- format:

  Kusto ingestion format. Supported file formats include `csv`, `json`,
  `multijson`, `parquet`, `avro`, `orc`, and the documented delimited
  text formats

- source_ids:

  Optional GUID per character `sources` entry. Missing IDs are
  generated. Do not combine with structured source records

- raw_sizes:

  Optional uncompressed byte size per character `sources` entry. Use
  `NA` for an unknown size. Do not combine with structured source
  records

- mapping:

  Optional name of a predefined ingestion mapping whose kind matches
  `format`

- tags:

  Character vector of extent tags to attach

- ingest_if_not_exists:

  Stable keys used for idempotent ingestion. The service checks existing
  `ingest-by:` tags for these values

- ignore_first_record:

  Whether to skip the first record in every source, commonly used for
  CSV headers

- skip_batching:

  Whether to bypass normal Kusto ingestion batching. This can reduce
  latency but should be reserved for latency-critical workloads

- delete_after_download:

  Whether Kusto may delete a source after it has downloaded it. The
  default preserves source data

- creation_time:

  Optional ISO 8601 extent creation time, `Date`, or `POSIXt`. A `Date`
  is sent as midnight UTC. Align historical values with the target merge
  policy lookback

- validation_policy:

  Optional JSON string or named list describing CSV validation behavior

- zip_pattern:

  Optional regular expression selecting files inside ZIP sources

- timestamp:

  Optional ISO 8601 request timestamp, `Date`, or `POSIXt`

- timeout:

  Positive client-side limit in seconds. For a wait, this bounds the
  complete polling operation; otherwise it bounds the status request

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Status calls reuse
  an in-process handle credential unless authentication is overridden

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- .deadline:

  Internal absolute POSIX date-time used when a higher-level operation
  composes submission and status polling under one deadline

- ingestion:

  A `fabric_kql_ingestion` handle or a non-empty operation ID

- details:

  Whether status should include per-source detail records

- wait:

  Whether to poll until all expected sources are terminal

- poll_interval:

  Minimum seconds between status requests while waiting

- error_on_failure:

  Whether a terminal failed or canceled ingestion raises a typed error.
  Use `FALSE` to inspect the returned status

- .sleep, .now:

  Internal hooks for deterministic deadline and polling tests

## Value

`fabric_kql_ingest()` returns a `fabric_kql_ingestion` handle with the
operation ID and source IDs. `fabric_kql_ingestion_status()` returns a
`fabric_kql_ingestion_status` record with normalized counts, state, UTC
times, and an optional details tibble

## Sources and storage access

`fabric_kql_ingest()` never uploads local data or serializes an R
object. Every `sources` value must already identify a file in blob
storage or OneLake, and `table` must already exist. Use
[`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md)
when the data is a data frame, tibble, or Arrow object; that function
performs staging and can create the target with
`create_if_missing = TRUE`.

`sources` can be a character vector of storage connection strings, a
data frame with `url`, `source_id`, and optional `raw_size` columns, or
a list of records with those fields. The camel-case service names
`sourceId` and `rawSize` are also accepted. Character inputs use the
parallel `source_ids` and `raw_sizes` arguments

Only existing `https://` or `abfss://` storage sources are accepted.
Nonpublic sources must include a Kusto-supported authentication suffix
or credential in the storage connection string. For example, append
`;impersonate` to a OneLake URL when the caller has permission to read
it

Source IDs are generated when omitted and are returned in the ingestion
handle. They identify blobs in status details, but they are not by
themselves an exactly-once guarantee

## Delivery and idempotency

Queued ingestion has at-least-once delivery semantics. Submission is
therefore not automatically replayed after throttling, network failure,
or an ambiguous response. Retain the returned operation ID before
starting unrelated work

For idempotent batch designs, set `ingest_if_not_exists` to one or more
stable keys. The function also attaches the corresponding `ingest-by:`
tags unless they are already present. A later submission with a matching
key is observable in detailed status instead of silently duplicating
committed extents. Idempotency checks can race when the same key is
queued concurrently, so serialize submissions that share a key

## Tracking and failures

`fabric_kql_ingestion_status()` accepts the handle returned by
`fabric_kql_ingest()` or a raw operation ID plus the ingestion target.
With `wait = FALSE`, it returns one snapshot. With `wait = TRUE`, it
polls until every expected source is terminal or `timeout` is reached

The returned status distinguishes `Succeeded`, `PartiallySucceeded`,
`Failed`, `Canceled`, `PartiallyCanceled`, and `InProgress`. Detailed
blob failures retain `error_code`, `failure_status`, and `message`.
Source URLs and raw service data are redacted so SAS tokens and embedded
credentials are not retained in the result. Set
`error_on_failure = FALSE` to inspect a terminal failure instead of
receiving a typed condition carrying the same status in `last_status`.
When a submission handle supplies the expected blob count, completion
requires the documented status counts to match it exactly. Unknown
nonzero status categories and impossible totals raise a protocol error
rather than being misreported as successful completion

## Limits and permissions

The preview REST API accepts at most 20 blobs per request and a maximum
of 6 GB of uncompressed data. `raw_sizes` are validated and summed when
all are known. Supplying sizes also avoids a metadata read by the
ingestion service

The caller needs Kusto Table Ingestor permission on the target table and
Database User access. Reading nonpublic source files additionally
requires storage access through the authentication method in each
storage connection string. `delete_after_download = TRUE` also requires
delete permission and permanently removes successfully downloaded source
blobs

## References

[Queued ingestion REST API
(preview)](https://learn.microsoft.com/en-us/kusto/management/data-ingestion/queued-ingest-use-http?view=microsoft-fabric)

[Queued ingestion status REST API
(preview)](https://learn.microsoft.com/en-us/kusto/management/data-ingestion/queued-ingest-status-http?view=microsoft-fabric)

[Supported ingestion
formats](https://learn.microsoft.com/en-us/kusto/ingestion-supported-formats?view=microsoft-fabric)

[Storage connection
strings](https://learn.microsoft.com/en-us/kusto/api/connection-strings/storage-connection-strings?view=microsoft-fabric)

[Data ingestion
properties](https://learn.microsoft.com/en-us/kusto/ingestion-properties?view=microsoft-fabric)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the KQL database and a Lakehouse containing staged CSV files
workspace <- fabric_workspaces()[[1L]]
database <- fabric_kql_databases(workspace)[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
files <- fabric_onelake_list(
  workspace,
  lakehouse,
  path = "Files/events"
)
csv_file <- files[grepl("[.]csv$", files$path), ][1L, ]

# Build the source URI from discovered IDs and the listed file path
source <- paste0(
  "https://onelake.dfs.fabric.microsoft.com/",
  workspace$id, "/", lakehouse$id, "/", csv_file$path[[1L]],
  ";impersonate"
)

# Choose an existing target and CSV mapping from the KQL database explorer
table <- Sys.getenv("FABRIC_KQL_TABLE")
mapping <- Sys.getenv("FABRIC_KQL_CSV_MAPPING")

# Queue the file once using a stable ingest-if-not-exists key
ingestion <- fabric_kql_ingest(
  database,
  table = table,
  sources = source,
  format = "csv",
  mapping = mapping,
  ignore_first_record = TRUE,
  ingest_if_not_exists = paste0("file:", csv_file$path[[1L]])
)

# Wait for every submitted file to reach a terminal ingestion state
result <- fabric_kql_ingestion_status(
  ingestion,
  wait = TRUE,
  timeout = 900
)
result$state
result$details
} # }
```
