# fabricQueryR (development version)

* Added paginated workspace and item discovery with `fabric_workspaces()`,
`fabric_items()`, and `fabric_item()`. Typed helpers discover Lakehouses,
Warehouses, SQL Databases, semantic models, Eventhouses/KQL databases,
notebooks, and GraphQL APIs. The returned records can be passed directly to
the corresponding query, OneLake, Livy, and job helpers.

* Authentication is now consistent across the package. Authenticated functions
accept an `AzureAuth::AzureToken`, bearer token, or refreshable provider via
`token`, while `auth_args` configures AzureAuth login flows. The former
`access_token` argument to the SQL and Livy helpers remains available through
`...` as a deprecated alias. Token refresh, transient REST retries, and a new
authentication vignette are also included.

* SQL helpers now support Fabric Warehouse, Lakehouse SQL analytics endpoints,
and SQL Database. They accept discovery records and complete portal
connection strings, bind query parameters through DBI, and retry transient
connection failures. The default `database` changed from `"Lakehouse"` to
`NULL`: complete targets infer their catalog, while bare endpoints use
Fabric's `master` context unless a database is supplied.

* SQL helpers now offer an opt-in ADBC backend through `adbi`,
`adbcdrivermanager`, and the external ADBC Driver Foundry `mssql` driver.
ODBC remains the default. `fabric_sql_query()` can return either its existing
tibble result or a `nanoarrow_array_stream` compatible with
`arrow::as_record_batch_reader()`. Missing external ADBC drivers fail before
authentication with `dbc install` guidance.

* Added authenticated Eventhouse queries with `fabric_kql_query()` and Fabric
API for GraphQL execution with `fabric_graphql_query()`. Both accept direct
endpoints or discovered items and support bound parameters or variables.
GraphQL cursor pagination is available through `fabric_graphql_paginate()`
and `fabric_graphql_cursor()`.

* Added general OneLake file access with `fabric_onelake_list()`,
`fabric_onelake_metadata()`, `fabric_onelake_download()`,
`fabric_onelake_upload()`, and `fabric_onelake_delete()`. These functions
support discovery records and HTTPS/ABFSS paths, ETags, byte ranges,
conditional writes, atomic transfers, and confirmation before deletion.

* Added a common on-demand item-job interface through `fabric_job_run()`,
`fabric_job_status()`, `fabric_job_wait()`, and `fabric_job_cancel()`.
Notebook, pipeline, and Spark job definition runs expose status, result,
failure, timeout, and cancellation information.

* Livy support now includes reusable regular and high-concurrency Spark
sessions through `fabric_livy_session()`, plus standalone batch applications
through `fabric_livy_batch_submit()`. The objects expose status, results, and
cancellation, while sessions also provide explicit cleanup;
`fabric_livy_query()` remains the one-shot interface.

* `fabric_onelake_read_delta_table()` now:
  - Uses the Python `deltalake` package and its delta-rs/DataFusion table
  provider instead of maintaining a separate Delta protocol engine in R.
  `reticulate::py_require()` installs the optional Python runtime on first
  Delta use; package installation, package loading, and non-Delta functions do
  not initialize Python or download the binary wheel. Reticulate-managed
  environments may also include reticulate's normal runtime dependencies.
  - Streams query results through Python and R `nanoarrow` using the Arrow C
  interface. Python `pyarrow`, DuckDB, local transaction-log staging, and local
  Parquet downloads are no longer required.
  - Continues to use the package's Fabric discovery and refreshable
  authentication layer for Lakehouses and Warehouse Delta exports, including
  custom Fabric DFS endpoints. Warehouse documentation and tests now account
  for asynchronous Delta-log publication and verify exact post-mutation rows.
  - Supports `version` for time travel, `columns` and `limit` for narrowing the
  result, `result = "arrow_stream"` for an Arrow C stream (the default remains
  a tibble). Arrow results are now genuinely lazy and single-use. Authentication
  failures while opening either result are retried once with refreshable
  credentials; a stream that has already been returned is never replayed.
  - Accepts discovery records for `workspace_name` and `lakehouse_name`; a
  schema-enabled Lakehouse record supplies its default schema.
  `item_type` disambiguates suffixless Lakehouse and Warehouse display names.
  Workspace names that cannot appear in an ABFSS authority now fail with paired
  GUID/discovery-record guidance instead of being incorrectly percent-encoded.
  - Preserves `long` as `bit64::integer64`, decimals as exact character data,
  and `timestamp_ntz` as a wall-clock class, recursively through nested Arrow
  values. Columns containing the valid Delta integer or long minimum no longer
  confuse those values with R/bit64's reserved NA sentinels: integer columns
  widen to exact doubles and long columns use an exact character-backed class.
  Nullable structs also retain their parent validity, including inside
  lists and maps, so null structs remain distinct from present structs whose
  children are all null. The Arrow bridge normalizes DataFusion view types for
  compatibility with the R `arrow` package.
  - Preserves canonical `arrow.parquet.variant` columns in Arrow-stream results
  and rejects tibble collection with an actionable error instead of silently
  exposing Variant's physical metadata and value buffers as ordinary columns.
  - Deprecates and ignores `dest_dir`, because no local staging occurs.
  `timestamp_partition_timezone` is retained as a compatibility formal but is
  rejected when supplied because delta-rs has no equivalent override.
  - Is checked with deterministic local delta-rs fixtures, independent static
  value expectations, and live Fabric comparisons against feature-neutral
  Spark-materialized reference tables for deletion vectors, column mapping,
  and shallow clones. Per-file deletion-vector masks
  longer than 65,536 rows, type widening, V2 checkpoints, and Fabric Variant
  preview tables are rejected with an actionable unsupported-feature error by
  the current delta-rs runtime instead of being advertised as readable.

* Fabric sandbox seeding now publishes a content-derived fixture revision to
OneLake. Discovery refuses stale or partially seeded persistent workspaces
before R integration tests can report misleading table-level results.

* Added `fabric_delta_config()` to inspect the optional Python runtime and its
declared requirements. Inspection is non-initializing by default. The runtime
is constrained to the tested `deltalake>=1.6.2,<2` range and its required
`DeltaTable`/`QueryBuilder` API is checked before querying.

* `fabric_pbi_dax_query()` now accepts discovered semantic models or direct
workspace/dataset IDs, supports optional RLS impersonation, handles paginated
name lookup safely, and rejects partial or embedded query errors instead of
returning incomplete results. It also supports the newer Arrow
`executeDaxQueries` API, including its advanced request options, typed tibble
results, Arrow C stream results, LZ4-compressed record batches, and HTTP 200
error rowsets. Optional execution metrics are retained as a result attribute.

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
