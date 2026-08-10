# fabricQueryR (development version)

## New

* `fabric_onelake_list()` now exposes the ADLS Gen2 `beginFrom` cursor through
  `begin_from`, allowing a listing to start at a relative path.

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

* Delta-reader documentation now includes the complete source-to-result type
  conversion table for both tibble and Arrow-stream results.

* GraphQL query and pagination HTTP timeouts now default to 110 seconds, leaving
  response-transfer overhead beyond Fabric's 100-second server execution limit.

* `fabric_sql_query()` now rejects DDL and DML instead of sending arbitrary
  statements through `DBI::dbGetQuery()`. Use `DBI::dbExecute()` on a
  `fabric_sql_connect()` connection for side-effecting statements.

* Livy table results now convert columns from their declared Spark schema and
  retain that schema in the `spark_schema` attribute. Longs and decimals remain
  exact character values; typed null/empty, temporal, binary, and nested
  columns now have stable representations.

* OneLake workspace, item, item-type, and Delta schema identifiers must now be
  exactly one URI segment, and conflicting `.Lakehouse`/`.Warehouse` suffixes
  are rejected instead of changing the URL hierarchy.

* KQL results now retain auxiliary tables, completion information, raw frames,
  response headers, and correlation IDs. HTTP-200 partial-failure conditions
  carry any primary data already returned by Kusto.

* Singular `fabric_item()` discovery now retains workspace-specific OneLake DFS
  and Blob endpoint metadata, matching `fabric_items()` in private-link setups.

* `fabric_job_status()` now honors a newly submitted job's initial
  `Retry-After` delay by default; job handles retain the corresponding
  `next_poll_at` timestamp.

* Livy session, statement, and batch waits now enforce one wall-clock deadline
  across status requests, HTTP retries, retry delays, and polling sleeps.
  Timeout conditions consistently retain the last response and state.

* `fabric_pbi_dax_query()` now rejects conflicting connection strings,
  discovered-record IDs, and explicit target IDs instead of silently allowing
  one selector to override another.

* Personal-workspace v2 XMLA strings are no longer resolved through the
  authenticated caller's unscoped `/datasets` collection, which could select a
  same-named model belonging to the wrong owner. Use an explicit `dataset_id`
  with `my_workspace = TRUE`.

* `fabric_kql_query()` now returns Kusto `decimal` columns as character vectors
  so their full 128-bit decimal representation remains exact.

* Explicit `tenant_id` or `client_id` arguments now replace the credential
  stored in a `fabric_job` handle instead of being silently ignored.

* GraphQL pagination now reuses one credential across pages instead of
  reacquiring AzureAuth credentials for every request.

* Empty `conf = list()` and `tags = list()` inputs are now treated as omitted
  Livy settings.

* Fabric REST and KQL endpoint validation now accepts an explicit `:443`,
  consistently with OneLake and Livy, while still rejecting non-default ports.

* KQL timespan columns no longer retain the service's raw text values as
  element names.

* POSIXlt job parameters now work on every declared R version, including R
  4.1 and 4.2, while still rejecting vectors of multiple timestamps. CI now
  includes an R 4.1 lane.

* Discovery can now construct personal-workspace semantic-model XMLA targets
  when `personal_workspace_tenant_id` and `personal_workspace_owner` are
  supplied explicitly. These required values are absent from Fabric's
  workspace response.

* Delta reads now accept deletion-vector table protocols. Deterministic
  runtime tests verify both an enabled-but-unused feature and actual deleted
  rows against the pinned `deltalake` reader.

* Shared HTTP retries now cap server-provided `Retry-After` delays at 120
  seconds by default. Set `options(fabricqueryr.http.max_retry_delay = ...)`
  to choose a different non-negative ceiling.

* HTTP-date `Retry-After` headers are now parsed independently of the session's
  time locale.

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
  tibbles or Arrow streams. Only request properties in Microsoft's published
  `executeDaxQueries` contract are accepted.

* `fabric_onelake_read_delta_table()` now reads through the optional Python
  `deltalake` runtime instead of downloading table files first. Python is set up
  on the first Delta read and is not required by other package functions. The
  reader now accepts discovered items and supports snapshot versions, column
  selection, row limits, and disk-backed Arrow streams that are staged while
  the OneLake token is current. Tibbles support scalar columns; use an Arrow
  stream for nested or extension data. The old `dest_dir` argument has been
  removed. The pinned Python reader reports actionable errors when a table
  requires unsupported Delta features such as Deletion Vectors, Type Widening,
  V2 Checkpoints, or Fabric Variant. This can affect some Warehouse exports;
  use SQL or Spark (Livy) when the package runtime cannot read their protocol.

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
