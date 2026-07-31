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
    staging, and local Parquet downloads are no longer required.
  - Continues to use the package’s Fabric discovery and refreshable
    authentication layer for Lakehouses and Warehouse Delta exports,
    including custom Fabric DFS endpoints.
  - Supports `version` for time travel, `columns` and `limit` for
    narrowing the result, `result = "arrow_stream"` for an Arrow C
    stream (the default remains a tibble). Arrow results are now
    genuinely lazy and single-use.
  - Accepts discovery records for `workspace_name` and `lakehouse_name`;
    a schema-enabled Lakehouse record supplies its default schema.
  - Preserves `long` as
    [`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html),
    decimals as exact character data, and `timestamp_ntz` as a
    wall-clock class, recursively through nested Arrow values. The Arrow
    bridge normalizes DataFusion view types for compatibility with the R
    `arrow` package.
  - Deprecates and ignores `dest_dir`, because no local staging occurs.
    `timestamp_partition_timezone` is retained as a compatibility formal
    but is rejected when supplied because delta-rs has no equivalent
    override.
  - Is checked with deterministic local delta-rs fixtures and direct
    live Fabric gates for deletion vectors, column mapping, type
    widening, V2 checkpoints, shallow clones, and Warehouse exports.

- Added
  [`fabric_delta_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_delta_config.md)
  to inspect the optional Python runtime and its declared requirements.
  Inspection is non-initializing by default.

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
