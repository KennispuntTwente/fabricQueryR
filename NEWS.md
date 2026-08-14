# fabricQueryR (development version)

## New

* Added workspace and item discovery with `fabric_workspaces()`,
  `fabric_items()`, and typed helpers for common Fabric item types. Discovered
  records can be passed directly to the package's query, OneLake, Livy, and job
  functions, avoiding the need to copy IDs and endpoints manually.

* Added query support for Eventhouse/KQL databases with `fabric_kql_query()`
  and Fabric APIs for GraphQL with `fabric_graphql_query()`. GraphQL helpers
  also support variables, cursor pagination, schema introspection with
  `fabric_graphql_schema()`, and completion-aware tidy collection with
  `fabric_graphql_collect()`. Nested values remain list-columns and exact large
  integers remain character data.

* Added preview tracked Eventhouse ingestion with `fabric_kql_ingest()` and
  `fabric_kql_ingestion_status()`. Existing blob and OneLake batches are
  validated, submitted without unsafe automatic replay, and monitored with
  per-source IDs, partial-failure details, idempotency tags, redacted storage
  credentials, and bounded polling.

* Added `fabric_kql_write_table()` to stream data frames, tibbles, and Arrow
  Tables, Datasets, Scanners, queries, or RecordBatchReaders through temporary
  Parquet into tracked Eventhouse ingestion. The Lakehouse writer now uses the
  same batch-wise Arrow serializer for larger-than-memory inputs. Both writers
  retain staging whenever a remote outcome is ambiguous. Eventhouse staging
  uses a storage-audience token for unattended, audience-aware credentials.

* Added `fabric_kql_export()` for server-side KQL exports to discovered
  OneLake `Files/` directories or documented storage connection strings. It
  submits once, tracks `.show operations`, returns authoritative artifact paths
  and record counts, and treats files from failed or timed-out exports as
  potentially incomplete.

* Lakehouse and Eventhouse writers now rotate lazy Arrow input into bounded
  Parquet parts. Lakehouse uses the documented folder-load contract; Kusto
  batches honor the ingestion service's advertised blob-count and total-size
  limits.

* Added `fabric_warehouse_write_table()` to load data frames, tibbles, and lazy
  Arrow sources into existing Warehouse tables through bounded Parquet staging
  in a Lakehouse and the documented OneLake `COPY INTO` path. Append uses one
  bulk statement; overwrite is transactionally truncated and reloaded.

* Added `fabric_lakehouse_read_table()` as the symmetric, discovery-record-aware
  counterpart to `fabric_lakehouse_write_table()`, with projected, time-travel,
  tibble, and Arrow-stream results delegated to the OneLake Delta reader.

* Added `fabric_function_invoke()` for published Fabric User Data Functions,
  with trusted public-URL validation, flow-aware Power BI authentication,
  non-idempotent retry safety, bounded payloads and responses, redacted result
  envelopes, and structured execution errors.

* Added OneLake file management with `fabric_onelake_list()`,
  `fabric_onelake_metadata()`, `fabric_onelake_download()`,
  `fabric_onelake_upload()`, and `fabric_onelake_delete()`.

* Added `fabric_onelake_read_file()` and `fabric_onelake_write_file()` for
  direct tibble or Arrow interchange with Parquet, CSV, and Arrow IPC files.
  Larger reads return disk-backed Arrow streams and lazy writes remain bounded
  by their current record batch.

* Added `fabric_onelake_shortcuts()`, `fabric_onelake_shortcut_get()`,
  `fabric_onelake_shortcut_create()`, and `fabric_onelake_shortcut_delete()` for
  paginated shortcut discovery and a guarded lifecycle across discovered
  OneLake items or documented connection-backed targets.

* Added `fabric_job_*()` functions to run, monitor, wait for, and cancel
  on-demand Notebook, pipeline, and Spark job definition runs.

* Added `fabric_operation_status()`, `fabric_operation_wait()`, and
  `fabric_operation_result()` for resumable Fabric long-running operations.
  Immediate and asynchronous operations share one result shape, including JSON,
  binary, and empty results.

* Added `fabric_lakehouse_tables()`, `fabric_lakehouse_load_table()`, and
  `fabric_lakehouse_write_table()` for schema-aware Delta table discovery,
  managed CSV/Parquet loading, and failure-aware R data-frame staging through
  OneLake. Lakehouse-scoped load operations use the shared resumable operation
  interface.

* Added `fabric_pbi_refresh()`, `fabric_pbi_refresh_history()`,
  `fabric_pbi_refresh_status()`, `fabric_pbi_refresh_wait()`, and
  `fabric_pbi_refresh_cancel()` for standard and enhanced semantic-model
  refreshes, including table/partition selection, retry and timeout controls,
  normalized execution attempts and errors, and links to refresh details.

* Expanded Livy support with reusable Spark sessions, including
  high-concurrency sessions and standalone batch
  applications.
  `fabric_livy_query()` remains available for one-shot execution.

## Changed

* Authentication is now consistent across the package. Functions accept an
  `AzureAuth::AzureToken`, bearer token, or refreshable token provider through
  `token`, with `auth_args` available for AzureAuth login options. The old
  `access_token` argument for SQL and Livy remains as a deprecated alias.

* SQL functions now also support Fabric SQL Database and accept discovered SQL
  items and complete portal connection strings. They support DBI query
  parameters, can optionally use an ADBC backend, and can return query results
  as either a tibble or an Arrow stream.

* The `database` default for SQL functions is now `NULL`: complete targets
  infer their database, while bare endpoints connect through `master` unless
  one is supplied. `fabric_sql_query()` now accepts only a single read-only
  statement; use a connection from `fabric_sql_connect()` for other SQL work.

* `fabric_pbi_dax_query()` now accepts discovered semantic models or direct
  workspace and dataset IDs, supports optional RLS impersonation, and reports
  incomplete or embedded query errors instead of silently returning partial
  results. It can also use Fabric's Arrow DAX response format to return typed
  tibbles or Arrow streams.

* `fabric_onelake_read_delta_table()` now reads through the optional Python
  `deltalake` runtime instead of downloading table files first. Python is set up
  on the first Delta read and is not required elsewhere. The reader accepts
  discovered Lakehouses and compatible Warehouses, and supports historical
  versions, column selection, row limits, and Arrow streams for large or nested
  results. The old `dest_dir` argument has been removed. Tables using features
  unsupported by the Python reader must instead be queried through SQL or Spark.

* Livy table results now follow the declared Spark schema and preserve large
  whole numbers and decimals exactly.

* Authenticated functions now reject unrecognized service hosts by default,
  reducing the risk of sending access tokens to unintended servers. Where
  custom hosts are supported, they can be enabled with
  `allow_custom_endpoint = TRUE`. Requests also retry temporary failures more
  consistently and provide clearer timeout and service errors.

# fabricQueryR 0.2.1

* Update e-mail address of maintainer in DESCRIPTION file (change to a 
personal e-mail address due to leaving the organization).

* `fabric_onelake_read_delta_table()`: add experimental support for specifying
a Lakehouse schema name to read from a specific schema within a Lakehouse which
has Lakehouse schemas enabled.

# fabricQueryR 0.2.0

* Added `fabric_livy_query()` to run queries against the 'Fabric Livy API',
allowing remote execution of 'Spark' code.

# fabricQueryR 0.1.1

* Initial CRAN submission.
