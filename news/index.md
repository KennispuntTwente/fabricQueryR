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
  GraphQL helpers also support variables, cursor pagination, and
  mutations where the configured API permits them.

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
  high-concurrency sessions and standalone batch applications.
  [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  remains available for one-shot execution.

### Changed

- Authentication is now consistent across the package. Functions accept
  an
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer token, or refreshable token provider through `token`, with
  `auth_args` available for AzureAuth login options. The old
  `access_token` argument for SQL and Livy remains as a deprecated
  alias.

- SQL functions now also support Fabric SQL Database and accept
  discovered SQL items and complete portal connection strings. They
  support DBI query parameters, can optionally use an ADBC backend, and
  can return query results as either a tibble or an Arrow stream.

- The `database` default for SQL functions is now `NULL`: complete
  targets infer their database, while bare endpoints connect through
  `master` unless one is supplied.
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  now accepts only a single read-only statement; use a connection from
  [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  for other SQL work.

- [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  now accepts discovered semantic models or direct workspace and dataset
  IDs, supports optional RLS impersonation, and reports incomplete or
  embedded query errors instead of silently returning partial results.
  It can also use Fabric’s Arrow DAX response format to return typed
  tibbles or Arrow streams.

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  now reads through the optional Python `deltalake` runtime instead of
  downloading table files first. Python is set up on the first Delta
  read and is not required elsewhere. The reader accepts discovered
  Lakehouses and compatible Warehouses, and supports historical
  versions, column selection, row limits, and Arrow streams for large or
  nested results. The old `dest_dir` argument has been removed. Tables
  using features unsupported by the Python reader must instead be
  queried through SQL or Spark.

- Livy table results now follow the declared Spark schema and preserve
  large whole numbers and decimals exactly.

- Authenticated functions now reject unrecognized service hosts by
  default, reducing the risk of sending access tokens to unintended
  servers. Where custom hosts are supported, they can be enabled with
  `allow_custom_endpoint = TRUE`. Requests also retry temporary failures
  more consistently and provide clearer timeout and service errors.

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
