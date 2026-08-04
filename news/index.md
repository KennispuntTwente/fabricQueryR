# Changelog

## fabricQueryR (development version)

- Added paginated workspace and item discovery with
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md),
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md),
  and
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).
  Typed helpers discover Lakehouses, Warehouses, SQL Databases, semantic
  models, Eventhouses/KQL databases, notebooks, and GraphQL APIs. The
  returned records can be passed directly to the corresponding query,
  OneLake, Livy, and job helpers.

- Authentication is now consistent across the package. Authenticated
  functions accept an
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer token, or refreshable provider via `token`, while `auth_args`
  configures AzureAuth login flows. The former `access_token` argument
  to the SQL and Livy helpers remains available through `...` as a
  deprecated alias. Token refresh, transient REST retries, and a new
  authentication vignette are also included.

- SQL helpers now support Fabric Warehouse, Lakehouse SQL analytics
  endpoints, and SQL Database. They accept discovery records and
  complete portal connection strings, bind query parameters through DBI,
  and retry transient connection failures. The default `database`
  changed from `"Lakehouse"` to `NULL`: complete targets infer their
  catalog, while bare endpoints use Fabric’s `master` context unless a
  database is supplied.

- SQL helpers now offer an opt-in ADBC backend through `adbi`,
  `adbcdrivermanager`, and the external ADBC Driver Foundry `mssql`
  driver. ODBC remains the default.
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  can return either its existing tibble result or a
  `nanoarrow_array_stream` compatible with
  [`arrow::as_record_batch_reader()`](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html).
  Missing external ADBC drivers fail before authentication with
  `dbc install` guidance.

- Added authenticated Eventhouse queries with
  [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
  and Fabric API for GraphQL execution with
  [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md).
  Both accept direct endpoints or discovered items and support bound
  parameters or variables. GraphQL cursor pagination is available
  through
  [`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md)
  and
  [`fabric_graphql_cursor()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_cursor.md).

- Added general OneLake file access with
  [`fabric_onelake_list()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_metadata()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  and
  [`fabric_onelake_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).
  These functions support discovery records and HTTPS/ABFSS paths,
  ETags, byte ranges, conditional writes, atomic transfers, and
  confirmation before deletion.

- Added a common on-demand item-job interface through
  [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
  [`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
  [`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
  and
  [`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md).
  Notebook, pipeline, and Spark job definition runs expose status,
  result, failure, timeout, and cancellation information.

- Livy support now includes reusable regular and high-concurrency Spark
  sessions through
  [`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md),
  plus standalone batch applications through
  [`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md).
  The objects expose status, results, and cancellation, while sessions
  also provide explicit cleanup;
  [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  remains the one-shot interface.

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  now:

  - Uses the Python `deltalake` package and its delta-rs/DataFusion
    table provider instead of maintaining a separate Delta protocol
    engine in R.
    [`reticulate::py_require()`](https://rstudio.github.io/reticulate/reference/py_require.html)
    installs the optional Python runtime on first Delta use; package
    installation, package loading, and non-Delta functions do not
    initialize Python or download the binary wheel. Reticulate-managed
    environments may also include reticulate’s normal runtime
    dependencies.
  - Streams query results through Python and R `nanoarrow` using the
    Arrow C interface. Python `pyarrow`, DuckDB, local transaction-log
    staging, and local Parquet downloads are no longer required. Tibble
    collection now releases full Arrow batches before recursive nested
    validity restoration, retaining only compact validity and offset
    metadata.
  - Continues to use the package’s Fabric discovery and refreshable
    authentication layer for Lakehouses and Warehouse Delta exports,
    including custom Fabric DFS endpoints. Warehouse documentation and
    tests now account for asynchronous Delta-log publication and verify
    exact post-mutation rows.
  - Supports `version` for time travel, `columns` and `limit` for
    narrowing the result, `result = "arrow_stream"` for an Arrow C
    stream (the default remains a tibble). Arrow results are now
    genuinely lazy and single-use. Authentication failures while opening
    either result are retried once with refreshable credentials; a
    stream that has already been returned is never replayed. Lazy
    streams expose their resolved Delta version so a scan that outlives
    its fixed OneLake token can be reopened explicitly at the same
    snapshot.
  - Accepts discovery records for `workspace_name` and `lakehouse_name`;
    a schema-enabled Lakehouse record supplies its default schema.
    `item_type` disambiguates suffixless Lakehouse and Warehouse display
    names. Explicit item types that conflict with a `.Lakehouse` or
    `.Warehouse` suffix now fail before constructing a doubled or
    contradictory OneLake item name. Workspace names that cannot appear
    in an ABFSS authority now fail with paired GUID/discovery-record
    guidance instead of being incorrectly percent-encoded.
  - Preserves `long` as
    [`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html),
    decimals as exact character data, and `timestamp_ntz` as a
    wall-clock class, recursively through nested Arrow values. Columns
    containing the valid Delta integer or long minimum no longer confuse
    those values with R/bit64’s reserved NA sentinels: integer columns
    widen to exact doubles and long columns use an exact
    character-backed class. Nullable structs also retain their parent
    validity, including inside lists and maps, so null structs remain
    distinct from present structs whose children are all null. Nested
    integer fields choose one stable R type across every list or map
    element, and int32 child buffers are widened without reinterpreting
    their physical bytes. Dictionary-encoded integer values use the same
    minimum-value-safe representations as plain arrays. The Arrow bridge
    normalizes DataFusion view types for compatibility with the R
    `arrow` package.
  - Preserves canonical `arrow.parquet.variant` columns supplied by an
    otherwise readable Arrow stream and rejects tibble collection with
    an actionable error instead of silently exposing Variant’s physical
    metadata and value buffers as ordinary columns. Current Fabric
    VariantShreddingPreview tables remain unsupported and fail before a
    stream is returned.
  - Deprecates and ignores `dest_dir`, because no local staging occurs.
    `timestamp_partition_timezone` is retained as a compatibility formal
    but is rejected when supplied because delta-rs has no equivalent
    override.
  - Is checked with deterministic local delta-rs fixtures, independent
    static value expectations, and live Fabric comparisons against
    feature-neutral Spark-materialized reference tables for deletion
    vectors, column mapping, and shallow clones. The live matrix now
    also covers classic checkpoints, schema evolution and time travel,
    void columns, binary/typed partitions, exact minimum integers, every
    type-widening fixture, and readable neutral references for
    unsupported features. Live Arrow results are compared deeply with
    Spark-neutral tables so nested values, binary payloads, validity,
    and exact scalar boundaries are checked without lossy R conversion.
    Spark also publishes an ordered stable-key oracle as JSON in OneLake
    Files; integration tests compare production Delta reads with that
    artifact through the independent file API, so a shared Delta bridge
    defect cannot make both sides of the comparison agree.
    Deletion-vector safety checks enumerate only files that actually
    carry a vector. Affected snapshots use a serialized scan because the
    pinned provider can otherwise misapply large masks at record-batch
    offsets, and positive limits are applied after deletion filtering so
    deleted physical rows do not reduce the logical result. Unreadable
    masks, type widening, V2 checkpoints, and Fabric Variant preview
    tables are rejected with an actionable unsupported-feature error
    instead of being advertised as readable.

- Fabric sandbox seeding now publishes a content-derived fixture
  revision to OneLake. Discovery refuses stale or partially seeded
  persistent workspaces before R integration tests can report misleading
  table-level results. The local runner also requires explicit tenant
  and client IDs before using an environment client secret; it never
  combines a cached interactive identity with that secret.

- Added
  [`fabric_delta_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_delta_config.md)
  to inspect the optional Python runtime and its declared requirements.
  Inspection is non-initializing by default. The runtime is pinned to
  the tested `deltalake==1.6.2` and Python `nanoarrow==0.8.0` versions,
  and its required `DeltaTable`/`QueryBuilder` API is checked before
  querying. This avoids silently accepting future binary/runtime
  combinations whose Arrow bridge and Delta feature behavior have not
  been verified. Compatibility claims for column mapping, deletion
  vectors, and shallow-clone reads are explicitly package-specific and
  do not override Microsoft’s published delta-rs compatibility matrix.

- OneLake authentication guidance now distinguishes item `Read`
  (metadata) from `ReadAll` or a scoped OneLake `Read` role (data-plane
  access), and notes the limitation for row- or column-secured tables
  read by an external engine.

- Delta reader documentation now states that `limit` is unordered and
  cannot be used as stable pagination, and explains when to supply a
  regional OneLake DFS endpoint to avoid cross-region global-endpoint
  resolution.

- [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  now accepts discovered semantic models or direct workspace/dataset
  IDs, supports optional RLS impersonation, handles paginated name
  lookup safely, and rejects partial or embedded query errors instead of
  returning incomplete results. It also supports the newer Arrow
  `executeDaxQueries` API, including its advanced request options, typed
  tibble results, Arrow C stream results, LZ4-compressed record batches,
  and HTTP 200 error rowsets. Optional execution metrics are retained as
  a result attribute.

## fabricQueryR 0.2.1

CRAN release: 2026-04-03

- Update e-mail address of maintainer in DESCRIPTION file (change to a
  personal e-mail address due to leaving the organization).

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md):
  add experimental support for specifying a Lakehouse schema name to
  read from a specific schema within a Lakehouse which has Lakehouse
  schemas enabled.

## fabricQueryR 0.2.0

CRAN release: 2025-09-15

- Added
  [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  to run queries against the ‘Fabric Livy API’, allowing remote
  execution of ‘Spark’ code.

## fabricQueryR 0.1.1

CRAN release: 2025-09-08

- Initial CRAN submission.
