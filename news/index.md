# Changelog

## fabricQueryR (development version)

### New

- Added workspace and item discovery with
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md),
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md),
  and typed helpers for common Fabric item types. Discovered records can
  be passed directly to the package’s query, OneLake, Livy, and job
  functions, avoiding the need to copy IDs and endpoints manually.

- Added query support for Eventhouse/KQL databases with
  [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
  and Fabric APIs for GraphQL with
  [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md).
  GraphQL helpers also support variables, mutations, and cursor
  pagination.

- Added OneLake file management with
  [`fabric_onelake_list()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_metadata()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  and
  [`fabric_onelake_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).

- Added `fabric_job_*()` functions to run, monitor, wait for, and cancel
  on-demand Notebook, pipeline, and Spark job definition runs.

- Expanded Livy support with reusable Spark sessions, including
  high-concurrency sessions, and standalone batch applications.
  [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  remains available for one-shot execution.

### Improved

- Authentication is now consistent across the package. Functions accept
  an
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer token, or refreshable token provider through `token`, with
  `auth_args` available for AzureAuth login options. The old
  `access_token` argument for SQL and Livy remains as a deprecated
  alias.

- SQL functions now support Fabric Warehouse, Lakehouse SQL analytics
  endpoints, and SQL Database. They accept discovered items and portal
  connection strings, support DBI query parameters, and can optionally
  use an ADBC backend.
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  can return either a tibble or an Arrow stream. The `database` default
  is now `NULL`; complete targets infer their database, while bare
  endpoints connect through `master` unless one is given.

- [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  now accepts discovered semantic models or direct workspace and dataset
  IDs, supports optional RLS impersonation, and reports incomplete or
  embedded query errors instead of silently returning partial results.
  It can also use Fabric’s Arrow DAX response format to return typed
  tibbles or Arrow streams, with optional execution metrics.

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  now reads through the optional Python `deltalake` runtime instead of
  staging Delta logs and Parquet files locally. The runtime is installed
  on first Delta use and can be inspected with
  [`fabric_delta_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_delta_config.md);
  other package functions do not require Python. Delta reads now support
  snapshot versions, column selection, row limits, discovery records,
  and lazy Arrow streams. Tibble collection is deliberately limited to
  common scalar columns; exact long integers, decimals, and
  `timestamp_ntz` values are returned as text, while nested and
  extension data stays on the Arrow-stream path. The retired `dest_dir`
  and `timestamp_partition_timezone` arguments have been removed.
  Runtime package selection is now left to reticulate’s declared
  requirements instead of being re-policed on every table read. Tables
  requiring Deletion Vectors, Type Widening, V2 Checkpoints, or Fabric
  Variant preview features remain unsupported. This includes current
  Fabric Warehouse Delta exports; use Fabric SQL or PySpark for those
  tables.

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
