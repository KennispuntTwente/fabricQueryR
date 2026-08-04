# fabricQueryR (development version)

## New

* Added workspace and item discovery with `fabric_workspaces()`,
  `fabric_items()`, and typed helpers for common Fabric item types. Discovered
  records can be passed directly to the package's query, OneLake, Livy, and job
  functions, avoiding the need to copy IDs and endpoints manually.

* Added query support for Eventhouse/KQL databases with `fabric_kql_query()`
  and Fabric APIs for GraphQL with `fabric_graphql_query()`. GraphQL helpers
  also support variables, mutations, and cursor pagination.

* Added OneLake file management with `fabric_onelake_list()`,
  `fabric_onelake_metadata()`, `fabric_onelake_download()`,
  `fabric_onelake_upload()`, and `fabric_onelake_delete()`.

* Added `fabric_job_*()` functions to run, monitor, wait for, and cancel
  on-demand Notebook, pipeline, and Spark job definition runs.

* Expanded Livy support with reusable Spark sessions, including
  high-concurrency sessions, and standalone batch applications.
  `fabric_livy_query()` remains available for one-shot execution.

## Improved

* Authentication is now consistent across the package. Functions accept an
  `AzureAuth::AzureToken`, bearer token, or refreshable token provider through
  `token`, with `auth_args` available for AzureAuth login options. The old
  `access_token` argument for SQL and Livy remains as a deprecated alias.

* SQL functions now support Fabric Warehouse, Lakehouse SQL analytics
  endpoints, and SQL Database. They accept discovered items and portal
  connection strings, support DBI query parameters, and can optionally use an
  ADBC backend. `fabric_sql_query()` can return either a tibble or an Arrow
  stream. The `database` default is now `NULL`; complete targets infer their
  database, while bare endpoints connect through `master` unless one is given.

* `fabric_pbi_dax_query()` now accepts discovered semantic models or direct
  workspace and dataset IDs, supports optional RLS impersonation, and reports
  incomplete or embedded query errors instead of silently returning partial
  results. It can also use Fabric's Arrow DAX response format to return typed
  tibbles or Arrow streams, with optional execution metrics.

* `fabric_onelake_read_delta_table()` now reads through the optional Python
  `deltalake` runtime instead of staging Delta logs and Parquet files locally.
  The runtime is installed on first Delta use and can be inspected with
  `fabric_delta_config()`; other package functions do not require Python.
  Delta reads now support Warehouse tables, snapshot versions, column
  selection, row limits, discovery records, and lazy Arrow streams. Tibble
  results preserve exact long integers and decimals, including within nested
  data. `dest_dir` is deprecated because local staging is no longer used, and
  non-`NULL` `timestamp_partition_timezone` values are no longer supported.
  Tables requiring Type Widening, V2 Checkpoints, or Fabric Variant preview
  features remain unsupported.

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
