# Export a KQL query directly to external storage

Runs Kusto's server-side `.export to storage` command and waits for its
asynchronous operation to finish. This avoids returning a large query
result through R and the Kusto client-result channel. A discovered
Fabric item plus a `Files/` directory is converted to a OneLake
connection string using caller impersonation; a complete documented
Kusto storage connection string can also be supplied.

## Usage

``` r
fabric_kql_export(
  cluster,
  query,
  destination,
  database = NULL,
  workspace = NULL,
  path = NULL,
  item_type = NULL,
  format = c("parquet", "csv", "tsv", "json"),
  compressed = TRUE,
  include_headers = NULL,
  name_prefix = NULL,
  file_extension = NULL,
  encoding = NULL,
  compression_type = NULL,
  distribution = c("per_shard", "per_node", "single"),
  size_limit = 100 * 1024^2,
  parquet_row_group_size = NULL,
  parquet_datetime_precision = NULL,
  timeout = 900,
  poll_interval = 2,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  allow_custom_endpoint = FALSE,
  .sleep = Sys.sleep,
  .now = Sys.time
)
```

## Arguments

- cluster:

  Query URI, or one Eventhouse or KQLDatabase discovery record. A
  KQLDatabase record also supplies `database`.

- query:

  One non-empty KQL query. The first result set is exported.

- destination:

  A discovered Fabric item, item name or ID, complete OneLake path, or
  complete HTTPS/ABFSS Kusto storage connection string. For an item,
  also supply `path` and optionally `workspace`.

- database:

  KQL database display name. Omit for a discovered KQLDatabase.

- workspace:

  Workspace containing an item supplied as `destination`. Omit when the
  discovered item contains its workspace ID.

- path:

  Destination directory relative to the OneLake item. It must be below
  `Files/`. Omit when `destination` is a complete storage path.

- item_type:

  Optional Fabric item type used to resolve a named item.

- format:

  Storage artifact format.

- compressed:

  Whether the artifacts use compression.

- include_headers:

  For CSV/TSV, one of `"none"`, `"all"`, or `"firstFile"`. `NULL` uses
  Kusto's default.

- name_prefix:

  Optional prefix for generated artifact names.

- file_extension:

  Optional artifact extension beginning with a dot.

- encoding:

  For CSV/TSV/JSON text, `"UTF8NoBOM"` or `"UTF8BOM"`.

- compression_type:

  Optional compression codec. Non-Parquet exports use `"gzip"`; Parquet
  also supports `"snappy"`, `"lz4_raw"`, `"brotli"`, and `"zstd"`.

- distribution:

  Kusto export distribution hint.

- size_limit:

  Maximum uncompressed bytes per artifact, from 100 MiB to 4 GiB.

- parquet_row_group_size:

  Optional positive Parquet row-group row count.

- parquet_datetime_precision:

  Optional `"millisecond"` or `"microsecond"` precision for Parquet
  datetime values.

- timeout:

  Positive total client-side wait limit in seconds.

- poll_interval:

  Positive seconds between operation status requests.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as
  fallback

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- allow_custom_endpoint:

  Logical. Permit a non-Microsoft Kusto HTTPS origin. Keep `FALSE`
  unless the endpoint is trusted; credentials are sent to the supplied
  origin

- .sleep, .now:

  Internal hooks for deterministic polling tests.

## Value

A `fabric_kql_export_result` containing the operation state, redacted
destination, artifact paths, per-artifact record counts, and aggregate
record count.

## Tracking and failure safety

The export submission is sent once and is never automatically replayed.
The function polls `.show operations` until Kusto reports a terminal
state, then calls `.show operation ... details` for the authoritative
artifact paths and record counts. Kusto does not remove files written
before a failed export, so a failure or timeout identifies the
destination and operation ID but never reports partial files as a
successful result.

Storage connection strings are emitted as obfuscated Kusto string
literals and are redacted from returned objects and conditions. If a
submission fails before its operation ID is received, inspect the
destination and Kusto operation history before trying again.

## Output properties

`format` supports Kusto's `parquet`, `csv`, `tsv`, and `json` exporters.
`compressed = TRUE` enables the selected `compression_type`, or Kusto's
default codec when it is omitted. `size_limit` is the uncompressed
target size of each artifact and must be from 100 MiB through 4 GiB.
Text header and encoding options, and Parquet row-group and
datetime-precision options, are accepted only for their applicable
formats.

## Permissions

The caller needs at least Kusto Database Viewer permission. OneLake
caller impersonation additionally needs write access equivalent to
Storage Blob Data Contributor on the destination.

## References

[Kusto export to
storage](https://learn.microsoft.com/en-us/kusto/management/data-export/export-data-to-storage?view=microsoft-fabric)

[Kusto storage connection
strings](https://learn.microsoft.com/en-us/kusto/api/connection-strings/storage-connection-strings?view=microsoft-fabric)

[Kusto management HTTP
request](https://learn.microsoft.com/en-us/kusto/api/rest/request?view=microsoft-fabric)

[Show Kusto
operations](https://learn.microsoft.com/en-us/kusto/management/show-operations?view=microsoft-fabric)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover both the source KQL database and destination Lakehouse
workspace <- fabric_workspaces()[[1L]]
database <- fabric_kql_databases(workspace)[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
table <- Sys.getenv("FABRIC_KQL_TABLE")
table_literal <- jsonlite::toJSON(table, auto_unbox = TRUE)

# Export a bounded query to a new folder in the discovered Lakehouse
exported <- fabric_kql_export(
  database,
  query = paste0("table(", table_literal, ") | take 10000"),
  destination = lakehouse,
  path = "Files/exports/events-weekly",
  format = "parquet",
  name_prefix = "events"
)
exported$artifacts
} # }
```
