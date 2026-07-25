# Changelog

## fabricQueryR (development version)

- Delta reader errors now prioritize known unsupported features. Fabric
  job status falls back to the Core scheduler while notebook-specific
  state is unavailable, timeouts now report the last observed state, and
  notebook session tags are validated before submission. Live Spark
  coverage verifies durable Delta effects instead of optional
  diagnostics.

- Authentication is now consistent across all authenticated exported
  functions. New code can pass an
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html)
  object, bearer token, or provider through `token`, and can configure
  any supported AzureAuth flow through `auth_args`. Functions that
  exposed `access_token` in version 0.2.1 consume that former name from
  `...` as a deprecated alias without restoring it to their formal
  arguments; supplying both names is an error. The single polymorphic
  `token` argument replaces it for new code. With `token = NULL`,
  AzureAuth reuses its cache before starting interactive login.
  AzureAuth tokens are expiry-checked and refreshed, and
  client-credential requests omit delegated-only `offline_access`. A new
  authentication vignette covers interactive and device login, service
  principals with secrets or certificates, managed identities, token
  audiences, cache behavior, and Fabric tenant/workspace/workload
  permissions. GraphQL selects the Fabric API audience automatically for
  package-managed client credentials and retains the delegated GraphQL
  scope for user flows.

- [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
  [`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
  [`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
  and
  [`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  add a common on-demand item-job interface. Notebook jobs use Fabric’s
  current release submission API and richer status API; pipelines and
  other item jobs use the Core Job Scheduler. Runs expose IDs, status,
  timestamps, failure reasons, exit values, activity IDs, and polling
  hints. Typed parameter inference, validated workload configuration,
  non-idempotent submission safety, explicit completed/failed/cancelled/
  deduplicated conditions, caller cancellation, and optional
  cancellation on timeout are covered by deterministic tests. The Fabric
  sandbox now deploys a parameterized job notebook for live success,
  failure, timeout, and cancellation coverage.

- General OneLake filesystem access is now available through
  [`fabric_onelake_list()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_metadata()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  and
  [`fabric_onelake_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).
  The helpers support names, paired workspace/item GUIDs, discovery
  records, and HTTPS/ABFSS paths; preserve nested and Unicode paths;
  follow ADLS continuation tokens; expose ETags; support byte ranges and
  conditional overwrite; and require explicit confirmation for deletion.
  Downloads stream atomically to local destinations. Uploads stream
  bounded chunks to temporary sibling files and atomically rename only
  after a complete flush, so failed overwrites do not truncate existing
  data. The Delta reader now uses the same authenticated
  listing/download transport.

- [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)
  adds authenticated Fabric API for GraphQL execution from direct
  endpoints, workspace/API IDs, or discovered GraphQL API items. Results
  preserve `data`, `errors`, and `extensions` independently, with
  configurable return/warn/error behavior for partial GraphQL failures.
  [`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md)
  and
  [`fabric_graphql_cursor()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_cursor.md)
  provide schema-neutral cursor traversal. Discovery now exposes
  executable GraphQL endpoints. The real Fabric sandbox provisions a
  GraphQL item over the seeded Lakehouse table and covers variables,
  nulls, cursor pagination, schema errors, authentication failures, and
  service-principal execution.

- [`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md)
  now creates a stateful `FabricLivySession` R6 object for regular or
  high-concurrency Fabric Spark sessions. Session objects can wait for
  readiness, submit and reuse multiple statements, inspect output, reset
  inactivity timeouts, cancel statements, and close deterministically.
  Structured statement failures retain Livy error values and tracebacks.
  [`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md)
  adds batch status, logs, results, timeout, and cancellation through
  `FabricLivyBatch`. The one-shot
  [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  helper is now implemented on the session object. The integration
  sandbox includes a real batch application covering successful, failed,
  and cancelled jobs.

- [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
  adds first-class, read-only Eventhouse/KQL querying through the Kusto
  v2 REST endpoint. It accepts direct query-service coordinates or
  discovered KQL database items, binds query parameters through Kusto
  request properties, validates HTTP-200 partial failures, supports
  multiple/progressive result tables, and maps Kusto schema types to
  stable R columns. The integration sandbox now provisions and seeds an
  Eventhouse and KQL database for live discovery, typing,
  parameterization, multi-table, and service-error coverage.

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  now preserves the full staged table layout and resolves snapshots from
  both JSON commits and Parquet checkpoints. It projects the selected
  snapshot’s logical schema, fills evolved columns with typed missing
  values, omits removed physical columns, and reads partition values
  from Delta add-file metadata instead of directory-name heuristics. It
  rejects unsupported Delta reader protocols, column mapping, and
  deletion vectors before returning data instead of risking incorrect
  results. The new `version` argument supports versioned reads.
  Directory entries returned by OneLake are excluded from file
  downloads.

- [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  now rejects embedded response, query, and table errors—including HTTP
  200 partial-result responses—rather than returning incomplete data. It
  also supports direct `workspace_id`/`dataset_id` lookup bypass,
  optional RLS impersonation, paginated workspace lookup, and ambiguity
  errors for duplicate names.

- Authentication and REST behavior are now shared across Fabric
  surfaces. Exported functions accept refreshable provider callbacks
  through `token` in addition to static tokens and interactive
  `AzureAuth`; REST calls use bounded request timeouts and retries for
  throttling/transient failures, honor `Retry-After`, refresh after 401,
  and include redacted endpoint/request diagnostics. Shared pagination
  and Fabric long-running-operation polling helpers are covered by
  deterministic tests.

- [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md),
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md),
  and
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md)
  now provide paginated, ambiguity-safe Fabric discovery. Typed helpers
  enrich Lakehouses, Warehouses, SQL Databases, semantic models,
  Eventhouses/KQL databases, notebooks, and GraphQL APIs with the
  workload properties available from Fabric. Discovery records expose
  ready-to-use SQL, OneLake, DAX, Livy, and KQL targets and can be
  passed directly to the corresponding query helpers.

- [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  and
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  now support Fabric Warehouse, Lakehouse SQL analytics endpoints, and
  Fabric SQL Database explicitly. They parse complete portal connection
  strings, disable unsupported MARS behavior, expose read-only intent
  and connection timeout, classify failures, and bind query parameters
  through DBI without SQL interpolation. Complete connection strings and
  discovery records infer their catalog. Bare endpoints no longer guess
  `"Lakehouse"` and can omit the catalog to use Fabric’s documented
  `master` context. Transient opens retry with refreshed tokens and
  bounded backoff; transient query execution retries only when
  explicitly marked idempotent and always uses a fresh connection. The
  Fabric integration sandbox now provisions mandatory Warehouse and SQL
  Database fixtures and validates discovery, connection strings, token
  login, and parameter binding against all three Fabric SQL surfaces.

- The real-service sandbox resolves its manifest consistently from the
  repository root and exercises audience-aware package token providers
  against Fabric API, GraphQL, Kusto, OneLake, SQL, Livy, and Power BI
  endpoints.

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
