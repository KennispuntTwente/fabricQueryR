# fabricQueryR roadmap

This roadmap is based on the current `fabricQueryR` implementation and the
Microsoft Fabric documentation reviewed in March 2026. Priorities favor testable
correctness and shared infrastructure before expanding the public API.

## Guiding principles

- Exercise every supported connection path against real Microsoft Fabric, while
  keeping fast offline tests available on every pull request.
- Reuse Microsoft-supported APIs and clients instead of maintaining partial
  implementations of protocols such as Delta Lake.
- Keep authentication, retries, pagination, and long-running operation handling
  behind shared internal interfaces.
- Accept item IDs and endpoints directly in automation, while offering discovery
  helpers for interactive use.
- Treat preview Fabric APIs as optional and clearly mark their lifecycle status in
  the package documentation.
- Add new exported functions only with unit tests, real integration coverage, and
  documented identity/scope requirements.

## Priority 0: Real Fabric integration-test environment

### Objective

Build a reproducible sandbox that deploys real Fabric items, seeds deterministic
test data, runs the R integration suite, and removes the sandbox even when a test
fails. This is the first deliverable because every later roadmap item depends on
being able to verify behavior against the service rather than mocks alone.

### Recommended architecture

Use a layered deployment rather than expecting one tool to own the full lifecycle:

1. **Capacity:** reuse a persistent, dedicated Azure Fabric test capacity. Pass its
   ID to the workflow as configuration. Do not create and destroy capacity for each
   test run: capacity is an Azure resource, is comparatively slow and costly to
   provision, and the Fabric Terraform provider does not support trial capacities.
2. **Workspace lifecycle and access:** use the official
   [Microsoft Fabric Terraform provider](https://registry.terraform.io/providers/microsoft/fabric/latest/docs)
   to create an ephemeral workspace on that capacity and grant the CI service
   principal the required workspace role. Both `fabric_workspace` and
   `fabric_workspace_role_assignment` support service-principal authentication.
   Export the workspace ID and OneLake endpoints as Terraform outputs.
3. **Item deployment:** use
   [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/) to publish
   source-controlled Fabric item definitions into the workspace. It is the item
   deployment layer, not the capacity or workspace lifecycle layer. Pass the
   Terraform workspace ID to `FabricWorkspace`, explicitly provide an Azure
   `TokenCredential`, and use `parameter.yml` dynamic values such as
   `$workspace.$id` and `$items.Lakehouse.<name>.$id` for environment-specific
   references.
4. **Fixture upload:** use the
   [Microsoft Fabric CLI](https://microsoft.github.io/fabric-cli/) or the OneLake
   ADLS Gen2 API to upload small CSV and Parquet fixtures into the lakehouse
   `Files` area. Fabric CLI supports local-to-OneLake copies and federated
   service-principal authentication.
5. **Data seeding:** use the
   [Lakehouse Load Table API](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/load-table)
   for simple CSV/Parquet-to-Delta fixtures. Deploy and run a small Fabric notebook
   for states that require Spark, including partitioned tables, Delta checkpoints,
   schema evolution, column mapping, and deletion vectors. The load API is preview,
   so the notebook is also the fallback if that API changes.
6. **Endpoint discovery:** after provisioning completes, query Fabric item
   properties and wait for generated resources such as the lakehouse SQL analytics
   endpoint to report a successful provisioning state. Generate a temporary test
   manifest containing workspace/item IDs, SQL endpoints, and KQL query URIs. Do
   not commit generated IDs.
7. **Tests:** run the R integration suite against the manifest, capturing Fabric
   request/activity IDs in failures. Tests must create uniquely named mutable data
   or restore known state so reruns are deterministic.
8. **Teardown:** run `terraform destroy` in an unconditional workflow step. Add a
   scheduled janitor that removes expired workspaces with the repository prefix in
   case a runner is canceled before teardown.

The Fabric CLI can also create and remove workspaces, and the Fabric REST API
directly supports workspace create, capacity assignment, and delete. Keep those as
an emergency cleanup path. Terraform is preferred for the normal lifecycle because
it records ownership and models workspace role assignments declaratively.

### Proposed repository layout

```text
.github/workflows/
  integration-fabric.yaml
infra/fabric/
  terraform/
    main.tf
    providers.tf
    variables.tf
    outputs.tf
  workspace/
    TestLakehouse.Lakehouse/
    SeedFixtures.Notebook/
    TestWarehouse.Warehouse/
    TestSemanticModel.SemanticModel/
    TestEventhouse.Eventhouse/
    TestKQLDatabase.KQLDatabase/
    TestGraphQL.GraphQLApi/
    parameter.yml
  fixtures/
    basic.csv
    partitioned/
tools/fabric-sandbox/
  pyproject.toml
  .python-version
  uv.lock
  src/fabricqueryr_sandbox/
    deploy.py
    seed.py
    discover.py
    destroy.py
tests/testthat/integration/
  helper-fabric.R
  test-integration-sql.R
  test-integration-onelake.R
  test-integration-dax.R
  test-integration-livy.R
```

Add KQL and GraphQL integration files when those package features are implemented.
The exact Fabric item directories should be produced from definitions exported by
Fabric source control and kept separate from Terraform state.

### Python and `uv`

Create `tools/fabric-sandbox` as a small Python project managed by
[`uv`](https://docs.astral.sh/uv/guides/projects/):

- Declare `fabric-cicd`, `azure-identity`, and any Fabric CLI/API helper dependency
  in `pyproject.toml`.
- Pin a supported Python version in `.python-version`.
- Generate and commit the cross-platform `uv.lock`; do not edit it manually.
- Use `uv sync --locked` in CI so dependency drift fails the build.
- Expose narrow commands for deploy, seed, discover, and cleanup, and run them with
  `uv run`.
- Keep credentials and environment-specific IDs out of `pyproject.toml`,
  `parameter.yml`, and the lockfile.

### Identity and tenant prerequisites

Prefer a Microsoft Entra application with a service principal and GitHub Actions
[OpenID Connect](https://docs.github.com/en/actions/security-for-github-actions/security-hardening-your-deployments/configuring-openid-connect-in-azure).
The workflow needs `id-token: write` and `contents: read`, and should use a protected
GitHub environment for integration-test access. Store only non-secret identifiers
such as tenant ID, client ID, subscription ID, capacity ID, and principal object ID
in GitHub configuration; use no long-lived client secret.

The tenant/capacity administrator must:

- Enable the tenant setting that allows service principals to use Fabric APIs.
- Enable service-principal creation of workspaces, connections, and deployment
  pipelines if workspace creation is delegated to CI.
- Grant the service principal permission to create workspaces and assign the chosen
  test capacity, normally through an appropriate capacity Contributor/Admin role.
- Ensure the identity receives the workspace role needed by deployment and tests.
- Grant API permissions/scopes required by each tested surface. In particular,
  verify Power BI semantic-model service-principal access separately because
  identity support is not uniform across every Fabric workload.

Use `AzureCliCredential` locally after `az login`. In GitHub Actions, use
`azure/login` with OIDC and then the same explicit `AzureCliCredential`, or pass a
workload-identity `TokenCredential` directly. If a required item still does not
support service principals, mark that test with a capability gate and run it in a
separate delegated-user validation job until Microsoft adds support; do not store a
user password in CI.

### Fixture matrix

Start with small fixtures that deliberately cover behavior rather than volume:

| Surface | Fabric item/state | Minimum assertions |
| --- | --- | --- |
| SQL | Lakehouse SQL endpoint, Warehouse, SQL Database | Token login, endpoint normalization, query results, parameters, nulls, and clear failures |
| OneLake files | Nested CSV/Parquet files with duplicate basenames | Correct paths, listing/pagination, upload/download, and lazy access |
| Delta | Basic, partitioned, checkpointed, schema-evolved, column-mapped, and deletion-vector tables | Correct active rows/schema or an explicit unsupported-feature error |
| DAX | Small semantic model with deterministic measures | Table shape, qualified columns, nulls, API errors, and limit/truncation detection |
| Livy | Lakehouse and seed notebook/session | Session lifecycle, statement success/failure, cleanup, and batch execution |
| Discovery | More than one page where practical and deliberately ambiguous names | Complete pagination, stable ID lookup, and ambiguity errors |
| Reliability | Controlled stubs plus selected live calls | `Retry-After`, transient failures, long-running operations, and request IDs |
| KQL (later) | Eventhouse and KQL database | Query schema/types, multiple tables, and service errors |
| GraphQL (later) | GraphQL API backed by seeded data | Variables, pagination, GraphQL errors, and delegated-auth limitations |

Keep most retry/throttling cases as deterministic HTTP-stub tests; inducing real
throttling in a shared capacity is slow and disruptive.

### Test tiers and CI policy

1. **Unit and contract tests:** run on every push and pull request without Fabric
   credentials. Use `testthat`, mocked HTTP responses, recorded response shapes
   without tokens or tenant data, and static fixture files.
2. **Fabric smoke tests:** run through manual dispatch and on trusted internal pull
   requests after approval. Deploy the smallest lakehouse/semantic-model surface and
   validate one happy path for each current exported function.
3. **Full compatibility suite:** run nightly and before releases. Provision all
   supported items and cover protocol edge cases and future KQL/GraphQL paths.
4. **External pull requests:** never expose the OIDC-enabled environment directly.
   Run only offline tests until a maintainer approves code in a trusted context.

Use a concurrency group so only an appropriate number of sandbox jobs target the
test capacity at once. Give every workspace a unique, recognizable name such as
`fabricqueryr-ci-<run-id>-<attempt>` plus a creation timestamp or expiry tag in its
description.

### Initial deliverables

- [ ] Expand unit coverage for all five exported functions and their HTTP helpers.
- [x] Add Terraform for an ephemeral capacity-bound workspace and CI role
  assignment, with remote or per-run isolated state.
- [x] Add the `uv` project and commit `uv.lock`.
- [x] Add source-controlled provisioning for the Lakehouse, notebook, Warehouse,
  SQL Database, and semantic model using Terraform, `fabric-cicd`, and the
  supported Power BI API according to lifecycle ownership.
- [x] Add deterministic fixture upload, seeding, readiness polling, and manifest
  generation.
- [x] Add integration-test helpers that skip with a precise reason when no
  manifest is available, but fail when a required provisioned workload is absent.
- [x] Add an OIDC-based GitHub Actions smoke workflow.
- [ ] Schedule the full compatibility workflow after the stale-workspace janitor
  is available.
- [x] Add unconditional Terraform teardown to the integration workflow.
- [ ] Add a scheduled stale-workspace janitor for canceled runners.
- [x] Document one-command local deploy, test, and destroy workflows.

### Acceptance criteria

- A new authorized contributor can create the sandbox from a clean checkout using
  documented commands and no manually created workspace items.
- Two consecutive deployments converge without unintended changes.
- The current SQL, DAX, OneLake, and Livy APIs each have at least one real-service
  success test and one deterministic failure-path test.
- Failed tests still trigger workspace deletion, and the janitor handles abandoned
  workspaces.
- Logs and uploaded artifacts contain no tokens, client secrets, or tenant data
  beyond explicitly approved test identifiers.
- Offline tests remain runnable on CRAN and by external contributors without Fabric
  credentials.

## Priority 1: Make Delta table reads protocol-correct

### Problem

`fabric_onelake_read_delta_table()` currently reconstructs a snapshot by replaying
only twenty-digit JSON transaction-log files. That can return incorrect data once a
table uses checkpoints or newer Delta features. Downloading files by `basename()`
also discards partition paths and can overwrite files with equal names.

### Direction

- Replace manual log replay with a maintained Delta reader that supports OneLake,
  preferably through `arrow`/`deltalake` capabilities available to R or a narrowly
  scoped supported backend.
- Preserve complete relative paths during any local staging.
- Read `_last_checkpoint` and checkpoints if a direct backend is unavoidable.
- Inspect protocol versions and table features before reading; reject unsupported
  deletion vectors, column mapping, or other features explicitly rather than
  returning plausible but wrong rows.
- Add `version`/timestamp time-travel options where the selected backend supports
  them.
- Offer lazy Arrow/DuckDB-style results and projection/filter options so reading a
  table does not always collect it fully into memory.
- Keep raw OneLake file access separate from Delta transaction-log semantics.

### Acceptance criteria

- The integration fixture matrix for basic, partitioned, checkpointed,
  schema-evolved, column-mapped, and deletion-vector tables passes, or unsupported
  features fail before data is returned.
- Duplicate Parquet basenames in different partitions are handled correctly.
- Existing simple-table behavior remains compatible.

## Priority 2: Validate complete DAX responses

### Problem

The Power BI Execute Queries API can return HTTP 200 while embedding query or table
errors in the JSON body, including partial-result/truncation conditions. The current
parser reads only the first result/table rows and can silently present incomplete
data as success.

### Direction

- Inspect top-level, result, and table error payloads before constructing a tibble.
- Detect documented row/value/size limit failures and return an actionable error;
  never silently accept truncated data.
- Define behavior for multiple queries, results, and tables, either returning a
  structured result or rejecting unsupported multiplicity explicitly.
- Add direct `workspace_id` and `dataset_id` arguments so automation avoids
  name-based lookups.
- Support caller-provided tokens/credentials and documented impersonated-user
  payloads where permitted.
- Preserve qualified column names predictably and document null/type conversion.
- Make name lookup paginated and fail on ambiguous case-insensitive matches instead
  of selecting the first item.

### Acceptance criteria

- Contract tests cover HTTP errors, nested DAX errors, multiple tables, empty
  results, nulls, and limit/truncation payloads.
- Live tests verify a deterministic semantic model by both IDs and names.

## Priority 3: Unify authentication and resilient HTTP behavior

### Problem

Each connection path obtains tokens and performs requests independently. Shared
helpers currently improve error text but do not provide service-aware retries,
pagination, token refresh, or Fabric long-running operation handling.

### Direction

- Introduce one internal credential abstraction that can obtain tokens for Fabric,
  Power BI, SQL Database, OneLake/Storage, and Kusto audiences.
- Retain interactive `AzureAuth` behavior, but allow noninteractive service
  principals, managed identities, Azure CLI credentials, pre-acquired tokens, and
  refresh callbacks without forcing one global credential choice.
- Centralize request execution with bounded exponential backoff and jitter for
  `429`, `Retry-After`, retriable `408`, and selected transient `5xx` responses.
- Retry only idempotent operations by default; require an idempotency decision for
  POST requests.
- Add reusable continuation-token/`continuationUri` pagination and Fabric
  long-running operation polling with timeout/cancel support.
- Include request IDs, activity IDs, endpoint, status, and a redacted body preview
  in errors. Never log authorization headers or connection tokens.
- Document the audience and minimum permissions for every exported function.

### Acceptance criteria

- Every REST-based function uses the shared request/authentication layer.
- Deterministic tests cover refresh, pagination, `Retry-After`, timeout, LRO failure,
  and redaction.
- Existing interactive examples continue to work.

## Priority 4: Add Fabric workspace and item discovery

### Objective

Make IDs and endpoints easy to discover without coupling every query function to
fragile display-name resolution.

### Direction

- Add functions such as `fabric_workspaces()`, `fabric_items()`, and
  `fabric_item()` over the Fabric Core REST APIs.
- Support item-type filtering and complete pagination.
- Add typed convenience discovery for lakehouses, Warehouses, SQL Databases,
  semantic models, Eventhouses/KQL databases, notebooks, and GraphQL APIs.
- Return IDs plus workload properties such as SQL connection strings, SQL endpoint
  IDs, OneLake paths, and KQL query service URIs when available.
- Require exact or unique name matches and provide useful ambiguity errors.
- Accept discovered objects in downstream functions where doing so simplifies use,
  while continuing to accept plain IDs/endpoints.

### Acceptance criteria

- Paginated and ambiguous discovery is covered offline and in the sandbox.
- Users can go from workspace name to a valid SQL, OneLake, DAX, Livy, or KQL target
  without copying identifiers from the Fabric portal.

## Priority 5: Strengthen SQL connectivity

### Problem

The SQL helper works for basic Fabric endpoints but defaults the database to
`"Lakehouse"`, normalizes only simple server strings, and does not expose safe
parameterized query execution.

### Direction

- Treat Fabric Warehouse, lakehouse SQL analytics endpoints, and Fabric SQL
  Database as explicit supported target types.
- Parse complete Fabric connection strings, including server and database/catalog,
  while still accepting a bare endpoint.
- Change the default database to `NULL` and require/discover a catalog where the
  target needs one.
- Set Fabric-appropriate ODBC options, including disabling Multiple Active Result
  Sets where required by SQL Database guidance.
- Add parameterized query support through DBI rather than interpolating values.
- Expose timeout and read-only options and distinguish authentication, endpoint,
  database, and SQL execution errors.
- Consider a lower-level connection-info object from discovery to avoid duplicating
  endpoint parsing.

### Acceptance criteria

- Real tests cover Warehouse, lakehouse SQL endpoint, and SQL Database when the
  tenant/capacity supports each item.
- Full portal connection strings and bare server names normalize to the same target.
- Parameter binding is tested with strings, dates, nulls, and values containing SQL
  metacharacters.

## Priority 6: Add Eventhouse/KQL querying

### Objective

Add a first-class route to Fabric Real-Time Intelligence, which is a meaningful
connection surface not represented by the current package.

### Direction

- Add `fabric_kql_query()` using the Kusto query REST endpoint and the
  `https://api.kusto.windows.net` token resource.
- Accept cluster/query-service URI and database directly, with optional discovery
  from Eventhouse and KQL database items.
- Parse Kusto response tables and type metadata into stable R objects; define how
  multiple result tables are returned.
- Support query parameters and request properties without string interpolation.
- Reuse shared authentication, retries, diagnostics, and timeout handling.
- Defer ingestion/admin commands until the query API and permission model are
  stable and tested.

### Acceptance criteria

- A seeded Eventhouse/KQL database can be queried in CI with correct R types.
- Multi-table responses, service errors, timeout, and parameterization have tests.

## Priority 7: Expand Livy beyond one-shot statements

### Problem

The package already contains internal session create/wait/statement/close helpers,
but only exports a one-shot query workflow. Fabric also supports batch jobs and
high-concurrency session patterns.

### Direction

- Export a small session API for create, submit statement, inspect status/output,
  and close, using a class with guaranteed cleanup.
- Add batch submission, status, logs/output, cancel, and timeout handling.
- Support documented session scopes and high-concurrency session behavior where
  available.
- Add configurable Spark/session settings and lakehouse attachment without exposing
  raw request assembly for common cases.
- Separate statement output parsing from lifecycle control, preserving structured
  Spark errors and logs.
- Make cleanup robust when user code errors or R is interrupted.

### Acceptance criteria

- Integration tests cover multiple statements in one session, statement failure,
  batch success/failure/cancel, and session cleanup.
- The existing `fabric_livy_query()` remains as a convenience wrapper over the new
  session implementation.

## Priority 8: Add Fabric API for GraphQL

### Objective

Offer GraphQL as a user-configured application API over Fabric data after the
shared authentication and discovery layers are in place.

### Direction

- Add `fabric_graphql_query()` accepting API endpoint/ID, query document,
  variables, and operation name.
- Parse GraphQL `data` and `errors` independently because valid HTTP responses can
  contain partial data and GraphQL errors.
- Provide optional cursor pagination helpers without assuming one schema shape.
- Discover GraphQL API items and endpoints through Fabric APIs.
- Clearly document the delegated `GraphQLApi.Execute.All` scope and current
  identity limitations; do not imply service-principal support unless the live API
  and current documentation confirm it.

### Acceptance criteria

- Tests cover variables, partial data/errors, nulls, pagination, and authentication
  failures against a deterministic sandbox schema.
- Preview or identity limitations are visible in function documentation.

## Priority 9: General OneLake file access

### Objective

Expose OneLake as a general file system independently of the higher-level Delta
reader.

### Direction

- Add list, metadata, download, upload, and delete helpers over the ADLS Gen2/Blob
  compatible APIs.
- Support workspace/item IDs and OneLake paths, with discovery-based convenience.
- Preserve hierarchy, pagination, ETags, ranges, and overwrite semantics.
- Allow streaming or Arrow-based reads for common formats without forcing local
  staging where the selected backend supports remote access.
- Keep destructive operations explicit and off by default in convenience helpers.

### Acceptance criteria

- Nested paths, duplicate basenames, pagination, ranged reads, overwrite conflicts,
  and Unicode file names are covered by tests.
- Delta-table APIs consume this shared transport where appropriate without leaking
  file-system details into snapshot semantics.

## Priority 10: Optional item-job execution

### Objective

Provide a focused common interface for invoking and monitoring Fabric item jobs,
useful for notebooks, pipelines, Spark job definitions, and test-data setup.

### Direction

- Add `fabric_job_run()`, `fabric_job_status()`, `fabric_job_cancel()`, and a wait
  helper over the Fabric Job Scheduler APIs.
- Return structured job IDs, status, timestamps, failure reasons, and activity IDs.
- Reuse shared LRO/retry behavior and support caller-controlled polling intervals and
  timeouts.
- Keep workload-specific execution payloads typed or validated rather than exposing
  an unbounded JSON passthrough as the primary API.
- Add schedule management only after on-demand execution is stable.

### Acceptance criteria

- Sandbox tests run the fixture notebook/pipeline through the same public job API.
- Completed, failed, canceled, timed-out, and deduplicated states are handled
  explicitly.

## Delivery sequence

| Milestone | Scope | Release gate |
| --- | --- | --- |
| M0 | Integration sandbox, expanded unit tests, CI identity, cleanup | All current exports have offline and live smoke coverage |
| M1 | Delta correctness and DAX response validation | No known silent incorrect/truncated result paths |
| M2 | Shared auth/HTTP plus discovery | Existing REST clients migrated; retry/pagination/LRO contracts pass |
| M3 | SQL improvements and KQL querying | Live coverage for supported SQL target types and Eventhouse |
| M4 | Livy sessions/batches and GraphQL | Lifecycle, partial-error, and identity constraints tested |
| M5 | General OneLake access and item jobs | Shared storage/job APIs stable and documented |

Priorities 1 and 2 may begin while the full M0 fixture set is being assembled, but
they should not be released until their corresponding real-service tests are in
place. KQL, GraphQL, and other future fixtures should be added incrementally so the
first sandbox release does not depend on every Fabric workload being available in
the test capacity.

## Documentation references

- [Fabric REST API overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/)
- [Workspace create API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/create-workspace)
- [Workspace capacity assignment](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/assign-to-capacity)
- [Fabric REST pagination](https://learn.microsoft.com/en-us/rest/api/fabric/articles/pagination)
- [Fabric throttling](https://learn.microsoft.com/en-us/rest/api/fabric/articles/throttling)
- [Fabric long-running operations](https://learn.microsoft.com/en-us/rest/api/fabric/articles/long-running-operation)
- [Fabric identity support](https://learn.microsoft.com/en-us/rest/api/fabric/articles/identity-support)
- [Microsoft Fabric Terraform provider](https://registry.terraform.io/providers/microsoft/fabric/latest/docs)
- [`fabric-cicd` documentation](https://microsoft.github.io/fabric-cicd/)
- [Microsoft Fabric CLI](https://microsoft.github.io/fabric-cli/)
- [Manage a lakehouse with REST](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-api)
- [OneLake access APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)
- [Connect to Fabric data warehousing](https://learn.microsoft.com/en-us/fabric/data-warehouse/connectivity)
- [Connect to SQL Database in Fabric](https://learn.microsoft.com/en-us/fabric/database/sql/connect)
- [Power BI Execute Queries](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries)
- [Fabric Livy API overview](https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview)
- [Kusto REST query API](https://learn.microsoft.com/en-us/kusto/api/rest/request)
- [Fabric API for GraphQL](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-overview)
- [Fabric Job Scheduler API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler)
- [`uv` project management](https://docs.astral.sh/uv/guides/projects/)
