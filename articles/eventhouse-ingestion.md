# Track Eventhouse ingestion from R

Kusto queued ingestion is designed for batch files in blob storage or
OneLake. fabricQueryR can submit those files directly or provide a
one-call workflow that streams an R or Arrow object to Parquet, stages
it in OneLake, waits for tracked ingestion, and cleans up safely. The
underlying REST routes are currently in preview.

## Prepare the target and source

Start with a discovered KQL database. Its record contains both the query
URI used by
[`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
and the ingestion URI used here:

``` r

library(fabricQueryR)

database <- fabric_kql_databases("Telemetry workspace")[[1]]
database$ingestion_service_uri
```

The destination table must already exist. For CSV, JSON, Avro, Parquet,
and ORC workflows, create and validate a named ingestion mapping in
Fabric before the R workflow starts. General Kusto administration is
intentionally outside this API.

The source must be a Kusto storage connection string. A OneLake file can
use the workspace and item GUIDs and caller impersonation:

``` r

source <- paste0(
  "https://onelake.dfs.fabric.microsoft.com/",
  "00000000-0000-0000-0000-000000000001/",
  "00000000-0000-0000-0000-000000000002/",
  "Files/events/2026-08-14.csv;impersonate"
)
```

The signed-in principal must be able to read that OneLake file. Other
supported storage connection strings can carry a SAS token, a
managed-identity suffix, or another authentication method documented by
Kusto. Avoid writing credentialed URLs to logs. Handles and status
results redact recognized secrets, but the submission process
necessarily sends the complete source string to the trusted Kusto
ingestion endpoint.

[`fabric_kql_ingest()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md)
deliberately treats its inputs as existing storage sources. Use
[`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md)
when the source is an R object.

## Write an R or Arrow object in one call

For an ordinary data frame or tibble, supply the target and object. The
function asks the ingestion service for its current Fabric lake folder
and limits, writes Parquet, uploads it with a Storage-audience token,
queues tracked ingestion, waits, and deletes only after confirmed
success:

``` r

written <- fabric_kql_write_table(
  database,
  table = "Events",
  data = data.frame(
    id = 1:3,
    category = c("A", "B", "A"),
    amount = c(10.5, 20, 30.5)
  ),
  ingest_if_not_exists = "r-events-2026-08-14"
)

written$status$state
written$rows
written$staging_retained
```

Parquet identity mapping is case-sensitive. Source field names and types
must match the existing KQL table; otherwise supply a predefined Parquet
`mapping`. Factors are written as strings. Convert complex and
`difftime` columns explicitly.

Arrow Tables and RecordBatches work without an R data-frame conversion.
For data larger than memory, pass a lazy Dataset, Scanner, Arrow dplyr
query, RecordBatchReader, or an Arrow-compatible stream returned by a
package query:

``` r

dataset <- arrow::open_dataset("local-parquet-directory")

written <- fabric_kql_write_table(
  database,
  table = "Events",
  data = dataset,
  mapping = "EventsParquet"
)
```

The dataset is scanned one record batch at a time into bounded temporary
Parquet parts and then uploaded in bounded chunks. It is never collected
into a data frame or Arrow Table. A supplied RecordBatchReader is
single-use and is exhausted by the write. Use `target_file_size` for a
soft byte boundary or `max_rows_per_file` for an exact row boundary. The
serialized batch must remain within the ingestion service’s advertised
size and blob-count limits.

The function uses the OneLake folder returned by
`/v1/rest/ingestion/configuration`. If an administrator provides a
different writable location, pass its full trusted `Files/` URI as
`staging_folder`. Ambiguous submission and polling failures always
retain staging. Confirmed terminal failures retain it by default; set
`keep_staging_on_failure = FALSE` only when automatic removal is
appropriate. An upload transport error can make the final atomic rename
ambiguous; its condition reports the unique path with
`staging_retained = NA` for inspection.

## Queue a tracked batch

Supply the source format and the predefined mapping. Source GUIDs are
generated when omitted and exposed on the returned handle:

``` r

ingestion <- fabric_kql_ingest(
  database,
  table = "Events",
  sources = source,
  format = "csv",
  mapping = "EventsCsv",
  ignore_first_record = TRUE,
  tags = "source:daily-export",
  ingest_if_not_exists = "events-2026-08-14"
)

ingestion$id
ingestion$sources$source_id
ingestion$tags
```

`ingest_if_not_exists` uses stable keys without the `ingest-by:` prefix.
The function attaches the matching extent tag automatically. A later
ingestion with the same key is prevented when that tag already exists.
This is the Kusto idempotency mechanism; a source GUID primarily
identifies a blob in tracking and diagnostics.

Queued ingestion still has at-least-once delivery. In particular,
concurrent requests using the same key can race before either extent
exists. Serialize submissions sharing a key. fabricQueryR also does not
retry the submission POST: after a timeout, throttling response, or lost
connection, the service might already have accepted it, so an automatic
replay could duplicate data.

The preview route accepts at most 20 blobs per request and up to 6 GB of
uncompressed data. Structured records expose known sizes without
parallel vectors:

``` r

sources <- tibble::tibble(
  url = c(source_a, source_b),
  source_id = c(source_a_id, source_b_id),
  raw_size = c(120000000, 180000000)
)

ingestion <- fabric_kql_ingest(
  database,
  table = "Events",
  sources = sources,
  format = "parquet",
  mapping = "EventsParquet"
)
```

Keep `skip_batching = FALSE` for normal throughput. Set it only when
lower latency is more important than Kusto’s normal batching efficiency.
Source files are preserved by default; `delete_after_download = TRUE`
permanently removes a successfully downloaded source and requires
corresponding storage permission.

## Wait and inspect the outcome

One status snapshot is useful for a scheduler that persists operation
IDs:

``` r

snapshot <- fabric_kql_ingestion_status(ingestion)
snapshot$state
snapshot$counts
```

For an interactive or single-process batch, poll with a client-side
deadline:

``` r

result <- fabric_kql_ingestion_status(
  ingestion,
  wait = TRUE,
  timeout = 900,
  poll_interval = 2
)

result$state
result$details
```

The deadline stops only the R waiter. A `fabric_kql_ingestion_timeout`
condition includes the last observed status and explicitly leaves the
service operation running. Status GETs are safe to retry and honor
service throttling hints.

A mixed batch returns `PartiallySucceeded`; completely failed and
canceled batches have their own states. By default terminal problems
raise a typed condition containing `last_status` and the failed-source
details. To handle all states as data:

``` r

result <- fabric_kql_ingestion_status(
  ingestion,
  wait = TRUE,
  error_on_failure = FALSE
)

failed <- subset(
  result$details,
  status %in% c("Failed", "Canceled")
)
failed[c("source_id", "error_code", "failure_status", "message")]
```

Retry a `Transient` source only after considering whether it might
already have committed. A `Permanent` failure such as a missing mapping
or malformed input requires a configuration or data fix. Query the
destination after success:

``` r

loaded <- fabric_kql_query(
  database,
  query = "Events | where ingestion_time() > ago(1h) | take 100"
)
```

The caller needs Table Ingestor permission on the destination and
Database User access. Status details for nonpublic storage also require
source storage read access. For manual
[`fabric_kql_ingest()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md)
workflows, keep the staged file until the tracked result is terminal and
verified.
[`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md)
applies that retention rule automatically and reports the full retained
path whenever manual recovery is required.

## Export a large KQL result to OneLake

[`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
is the right interface when the result belongs in R. When the result is
too large for the client-result channel or should remain in Fabric,
[`fabric_kql_export()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_export.md)
runs Kusto’s service-side export and writes the first result set
directly to storage:

``` r

lakehouse <- fabric_lakehouses("Telemetry workspace")[[1]]

exported <- fabric_kql_export(
  database,
  query = "Events | where observed_at > ago(7d)",
  destination = lakehouse,
  path = "Files/exports/events-weekly",
  format = "parquet",
  name_prefix = "events",
  compression_type = "snappy"
)

exported$state
exported$records
exported$artifacts
```

A discovered item uses a OneLake GUID path with `;impersonate`; the
principal therefore needs write access to the destination. A complete
documented Kusto storage connection string can be supplied instead.
Credential-bearing strings are obfuscated in Kusto command telemetry and
redacted from returned R objects.

The function always uses `async`, polls `.show operations`, and calls
`.show operation ... details` only after the operation reports
`Completed`. The submission itself is never replayed. Kusto documents
that ordinary exports do not retry and that artifacts already written by
a failed export remain and must be treated as incomplete. Timeout and
failure conditions carry the operation ID and safe destination so the
operation can be inspected before a new export is submitted.
