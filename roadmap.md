# fabricQueryR roadmap

This roadmap tracks planned work and recently completed priorities. It
is based on the current `fabricQueryR` implementation and Microsoft
Fabric documentation reviewed in August 2026.

## Guiding principles

- Prioritize workflows that are specifically valuable to R users rather
  than wrapping every Fabric REST endpoint.
- Reuse Microsoft-supported APIs and clients instead of implementing
  storage or table protocols partially.
- Keep authentication, retries, pagination, and long-running-operation
  handling behind shared internal interfaces.
- Accept item IDs and endpoints directly in automation while offering
  discovery helpers for interactive use.
- Treat preview Fabric APIs as optional, isolated, and clearly labeled.
- Add public functions only with offline tests, live Fabric coverage,
  documented identity requirements, and explicit service limitations.
- Keep deployment and broad item-definition management with Fabric CLI,
  fabric-cicd, Terraform, and Git integration unless an R-specific
  workflow clearly benefits from package support.

## Maintenance: Notebook status-route migration

Microsoft has announced that the beta notebook API used to retrieve
richer workload-specific job status will be deprecated on **April 1,
2028**. Submission already uses the release contract; only the enriched
status lookup still calls the beta route before falling back to Core Job
Scheduler status.

- By the second quarter of 2027, recheck the Notebook and Core Job
  Scheduler contracts and add the replacement status response to the
  live fixture suite.
- By the fourth quarter of 2027, make the supported stable route primary
  and retain the beta lookup only as a compatibility fallback if still
  required.
- Remove all `beta=true` status requests before April 1, 2028 while
  keeping failure and exit-value reconciliation covered by integration
  tests.

## Priority 0: Complete typed discovery for executable items

**Status (August 2026): completed in the development version.**

### Objective

Make executable and newly supported Fabric items as easy to discover as
Lakehouses, Warehouses, semantic models, and notebooks.

### Direction

- Add
  [`fabric_data_pipelines()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  [`fabric_spark_job_definitions()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  [`fabric_environments()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  and
  [`fabric_user_data_functions()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  as typed wrappers over
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md).
- Add `fabric_reports()` and `fabric_variable_libraries()` only when a
  downstream package workflow needs those records.
- Extend workload-specific detail enrichment only when it adds a
  connection, execution, or query target consumed by fabricQueryR.
- Preserve folder recursion, private-link routing, authentication, and
  detail-error behavior from the existing discovery functions.

### Implementation

- Added
  [`fabric_data_pipelines()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  [`fabric_spark_job_definitions()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  [`fabric_environments()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  and
  [`fabric_user_data_functions()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  over the Core Items API’s documented type filter.
- Added a client-side type check shared by every typed helper while
  preserving all other service fields for forward compatibility.
- Added offline contract tests, live Fabric coverage, job-ready
  examples, and package-site documentation for all four helpers.

### Acceptance criteria

- Every item type accepted by a public execution function has a typed
  discovery helper and an end-to-end example.
- Typed helpers are strict filters over generic discovery and tolerate
  additional fields returned by Fabric.

## Priority 1: Add job history and schedule management

**Status (August 2026): proposed.**

### Objective

Let R users inspect previous runs and automate recurring jobs for
supported Fabric items.

### Direction

- Add `fabric_job_instances()` with pagination and normalized
  `fabric_job_instance` records compatible with
  [`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md).
- Add `fabric_job_schedules()`, `fabric_job_schedule_create()`,
  `fabric_job_schedule_update()`, and `fabric_job_schedule_delete()`.
- Represent cron/minute, daily, weekly, and monthly schedules with
  validated R inputs and a documented escape hatch for future schedule
  types.
- Handle Windows time-zone identifiers, UTC boundaries, disabled and
  auto-disabled schedules, and workload-specific `executionData`
  explicitly.
- Require explicit confirmation for deletion and never infer destructive
  replacement during conflict handling.

### Acceptance criteria

- Live tests cover paginated history and
  create/list/update/disable/delete for at least daily and weekly
  schedules.
- Tests cover daylight-saving boundaries without relying on the runner’s
  local time zone.
- Unknown future schedule and invocation types remain inspectable.

## Priority 2: Generalize Fabric long-running operations

**Status (August 2026): proposed.**

### Objective

Implement the common Fabric asynchronous-operation protocol once before
adding more APIs that return `Location`, `x-ms-operation-id`, and
`Retry-After`.

### Direction

- Add an internal operation handle plus shared status, wait, and result
  helpers.
- Export `fabric_operation_status()`, `fabric_operation_wait()`, and
  `fabric_operation_result()` only if users need to retain or resume
  handles.
- Accept both service-provided locations and operation IDs, honor retry
  delays, and distinguish state responses from result responses.
- Preserve progress, timestamps, request/activity IDs, and structured
  errors.
- Keep Job Scheduler instances and Livy sessions/batches as separate
  lifecycle types because they do not use this protocol.

### Acceptance criteria

- Immediate `200`/`201` and asynchronous `202` completion share one
  stable caller-facing result contract.
- Running, succeeded, failed, timed-out, malformed, and empty-result
  paths are covered offline and through at least one live Fabric
  operation.
- Polling never automatically repeats a non-idempotent initiating
  request.

## Priority 3: Add Lakehouse table discovery and loading

**Status (August 2026): proposed, preview-dependent.** Microsoft
currently marks the Lakehouse List Tables and Load Table APIs as
preview.

### Objective

Move a local R data frame or staged CSV or Parquet file into a managed
Lakehouse Delta table without requiring users to author a Fabric
notebook.

### Direction

- Add `fabric_lakehouse_tables()` with pagination, table type, format,
  location, and schema metadata.
- Add `fabric_lakehouse_load_table()` for an existing OneLake `Files/`
  path, supporting file/folder inputs, CSV options, Parquet, recursion,
  append, and overwrite.
- Add `fabric_lakehouse_write_table()` as a higher-level workflow:
  serialize an R data frame to Parquet, upload it to a unique staging
  path, start and wait for the load operation, and clean up after
  confirmed success.
- Make staging retention configurable after failure so users can
  diagnose or resume a load. Never modify managed `Tables/` files
  directly.
- Use `arrow` for Parquet serialization and document R-to-Fabric
  mappings for 64-bit integers, decimals, dates, timestamps, nested
  columns, and unsupported types.
- Prefer the supported load API or Spark over a custom Delta transaction
  writer.

### Acceptance criteria

- Live tests cover pagination, CSV and Parquet, append and overwrite,
  schemas, and Unicode names.
- A data-frame round trip preserves documented names, nulls, types, and
  row counts when verified through SQL and the Delta reader.
- Failures never expose a partial destination and report any retained
  staging path needed for recovery.

## Priority 4: Manage semantic-model refreshes

**Status (August 2026): proposed.**

### Objective

Complete workflows that update source data by allowing R users to
refresh and diagnose the semantic model serving that data.

### Direction

- Add `fabric_pbi_refresh()`, `fabric_pbi_refresh_history()`,
  `fabric_pbi_refresh_status()`, `fabric_pbi_refresh_wait()`, and
  `fabric_pbi_refresh_cancel()` over the Power BI dataset APIs.
- Accept the same discovered model records, workspace/dataset IDs,
  authentication inputs, and My Workspace behavior as
  [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md).
- Support ordinary and enhanced refresh payloads, including selected
  tables or partitions, commit mode, retry count, and timeout.
- Normalize attempts, engine errors, timestamps, and links to Fabric
  refresh details.
- Keep Power BI refresh scheduling distinct from generic Fabric job
  schedules where their contracts and limits differ.

### Acceptance criteria

- Live tests trigger a deterministic refresh and observe completion,
  history, and execution details.
- Failed, canceled, warning, queued, retry-attempt, and timeout states
  remain distinguishable.
- Documentation explains capacity limits, service-principal
  restrictions, Direct Lake behavior, and when refresh is unnecessary.

## Priority 5: Invoke Fabric User Data Functions

**Status (August 2026): proposed.**

### Objective

Allow R applications to invoke published Fabric business logic with the
same authentication, endpoint validation, retry, and error behavior as
other package interfaces.

### Direction

- Add `fabric_function_invoke()` accepting a trusted public function URL
  and a named R object serialized as JSON.
- Add typed item discovery but require an explicit function URL while
  the item API cannot provide enough information to derive it safely.
- Return function name, invocation ID, status, output, and structured
  errors in an inspectable `fabric_function_result`.
- Retry only when the caller explicitly marks an invocation idempotent
  because a function can have arbitrary side effects.
- Keep definition, publication, and deployment workflows out of initial
  scope.

### Acceptance criteria

- Tests cover scalar and structured inputs/outputs, invalid arguments,
  disabled public access, user errors, timeouts, oversized responses,
  and redaction.
- Permission and trusted-host requirements are documented, and bearer
  tokens never appear in results or conditions.

## Priority 6: Manage OneLake shortcuts

**Status (August 2026): proposed.**

### Objective

Expose shortcut lifecycle operations without confusing shortcut
definitions with the files visible through them.

### Direction

- Add `fabric_onelake_shortcuts()`, `fabric_onelake_shortcut()`,
  `fabric_onelake_shortcut_create()`, and
  `fabric_onelake_shortcut_delete()`.
- Make OneLake-to-OneLake targets ergonomic first. Support external
  targets through validated constructors that reference existing Fabric
  connection IDs rather than accepting external credentials.
- Support pagination, conflict policies, transforms, bulk creation where
  useful, and cache reset.
- Make clear that deleting a shortcut does not delete destination data.

### Acceptance criteria

- Live tests create, inspect, list, and delete a cross-item OneLake
  shortcut and confirm that destination data survives deletion.
- Conflict policies, encoded paths, inaccessible targets, and unknown
  future target types behave deterministically.

## Priority 7: Improve GraphQL schema and tidy-result ergonomics

**Status (August 2026): proposed.**

### Objective

Make Fabric GraphQL APIs easier to explore and convert into
analysis-ready tibbles without assuming a universal schema shape.

### Direction

- Add `fabric_graphql_schema()` using standard introspection, with an
  actionable error when introspection is disabled.
- Add `fabric_graphql_collect()` or `fabric_graphql_rows()` with an
  explicit field path for combining row objects across pages.
- Preserve nested objects as list-columns and retain exact large-integer
  handling instead of flattening or coercing silently.
- Continue to use
  [`fabric_graphql_cursor()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_cursor.md)
  for arbitrary cursor extraction.

### Acceptance criteria

- Tests cover nested paths, empty pages, evolving columns, list-columns,
  partial errors, large integers, and introspection-disabled APIs.
- Collection reports whether pagination completed and never returns an
  apparently complete tibble after reaching a page or service limit.

## Priority 8: Add tracked Eventhouse ingestion

**Status (August 2026): proposed, preview-dependent.** The Kusto
queued-ingestion REST API is currently preview.

### Objective

Let batch-oriented R workflows submit files or staged OneLake data to an
existing Eventhouse table and monitor the outcome.

### Direction

- Add `fabric_kql_ingest()` and `fabric_kql_ingestion_status()` for
  tracked queued ingestion.
- Support existing blob or OneLake sources first. Add data-frame
  convenience only through a staged, documented format with bounded
  memory use.
- Validate format, mapping, tags, size, source IDs, batch limits, and
  target database/table before submission.
- Document at-least-once semantics and expose source IDs for idempotent
  designs.
- Keep general Kusto administration and production use of `.ingest`
  commands out of the primary API.

### Acceptance criteria

- Live tests ingest a tagged fixture, wait on its operation ID, and
  verify rows with
  [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md).
- Duplicate submission, mapping and permission failure, partial batch
  failure, timeout, and throttling are observable and actionable.

## Documentation program

Interface selection is now a user-facing feature. Add task-oriented
vignettes alongside new public functions:

- Choosing among SQL, Delta, DAX, KQL, GraphQL, OneLake files, and
  Spark.
- Loading an R data frame into a Lakehouse and validating the round
  trip.
- Updating source data, refreshing a semantic model, and checking
  history.
- Running SparkR locally through Livy versus using SparkR or `sparklyr`
  inside a Fabric notebook or Spark job definition.
- Production authentication, private links, schedules, and failure
  monitoring.
- Large and nested results with Arrow streams and list-columns.

Each new feature should include a minimal README example, a
task-oriented vignette where several functions form one workflow,
preview/stability labels, identity and scope requirements, service
limits, cleanup behavior, and examples using discovered records instead
of copied IDs where possible.

## Delivery sequence

| Milestone | Scope | Release gate |
|----|----|----|
| M0 | Typed executable-item discovery plus job history and schedules | Every executable item is discoverable; history and schedule lifecycle pass live tests |
| M1 | Generic Fabric LRO support plus Lakehouse table list/load | Shared operation contract passes; preview table APIs are isolated and labeled |
| M2 | R data-frame-to-Lakehouse workflow plus semantic-model refresh | End-to-end load/read/refresh validation preserves documented types and states |
| M3 | User Data Function invocation and OneLake shortcuts | External invocation and shortcut lifecycle are authenticated, safe, and live-tested |
| M4 | GraphQL ergonomics and tracked Eventhouse ingestion | Tidy collection is complete/error-aware; preview ingestion has tracked round-trip coverage |

Each milestone must extend the offline unit suite and live Fabric
fixture contract before its public functions are released.
Preview-dependent features must remain isolated and explicitly labeled
so changes in preview APIs do not destabilize mature package surfaces.

## Documentation references

- [Fabric REST API
  overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/)
- [Fabric REST
  pagination](https://learn.microsoft.com/en-us/rest/api/fabric/articles/pagination)
- [Fabric
  throttling](https://learn.microsoft.com/en-us/rest/api/fabric/articles/throttling)
- [Fabric long-running
  operations](https://learn.microsoft.com/en-us/rest/api/fabric/articles/long-running-operation)
- [Fabric identity
  support](https://learn.microsoft.com/en-us/rest/api/fabric/articles/identity-support)
- [Fabric item management
  support](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/item-management-overview)
- [Fabric Job Scheduler
  API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/)
- [List Fabric item job
  instances](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances)
- [List Fabric item
  schedules](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-schedules)
- [Create a Fabric item
  schedule](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/create-item-schedule)
- [List Lakehouse
  tables](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/list-tables)
- [Load a Lakehouse
  table](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/load-table)
- [Power BI dataset
  APIs](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/)
- [Fabric User Data Functions
  overview](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/user-data-functions-overview)
- [Invoke User Data Functions
  externally](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/tutorial-invoke-from-python-app)
- [OneLake Shortcuts
  API](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/)
- [Fabric GraphQL introspection and schema
  export](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-introspection-schema-export)
- [Kusto queued ingestion REST
  API](https://learn.microsoft.com/en-us/kusto/management/data-ingestion/queued-ingest-use-http?view=microsoft-fabric)
- [Use SparkR in
  Fabric](https://learn.microsoft.com/en-us/fabric/data-science/r-use-sparkr)
- [Use sparklyr in
  Fabric](https://learn.microsoft.com/en-us/fabric/data-science/r-use-sparklyr)
