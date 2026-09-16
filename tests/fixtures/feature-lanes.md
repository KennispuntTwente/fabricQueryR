# Required Fabric feature lanes

Set `FABRIC_TEST_REQUIRED_FEATURES` to comma-separated feature names when running
a designated feature lane. A missing prerequisite is then a failure, rather than
a skip. These runs supplement the ordinary service-principal integration suite;
a skipped optional feature is not execution evidence.

The GitHub integration workflow accepts `required_features` on manual dispatch,
or reads the repository variable `FABRIC_TEST_REQUIRED_FEATURES` for recurring
runs. Unknown feature names fail. The additional Sunday runs in March and October
repeat whichever features are configured; set `nonutc-schedules` to collect
evidence around European daylight-saving changes. These runs check the current
offset on each side of the transition; they do not prove how Fabric handles a
nonexistent or repeated clock time during the transition itself.

The ordinary GitHub jobs use a service principal. `delegated-livy` and the full
`authorization` matrix need a separate runner with delegated credentials as
described below. Requiring those features in the service-principal workflow will
fail until those credentials and fixtures are supplied.

## GraphQL introspection (`introspection`)

Provide `FABRIC_TEST_GRAPHQL_INTROSPECTION_ENDPOINT` and
`FABRIC_TEST_GRAPHQL_INTROSPECTION_ROOT` for an API exposing a nonempty collection
with `items`, `hasNextPage`, and `endCursor`. A workspace administrator must enable
API Settings > Introspection. Keep the default TestGraphQL fixture disabled, or
set `FABRIC_TEST_GRAPHQL_DISABLED_ENDPOINT` to a separate disabled API.

```r
Sys.setenv(FABRIC_TEST_REQUIRED_FEATURES = "introspection")
source("tools/fabric-sandbox/local-integration.R")
run_fabric_integration_tests(filter = "integration-fabric-kql-graphql")
```

The success test follows nested type references and compares collected row
types with the schema. HC0046 fails this test. The separate disabled test requires
HC0046. See [Microsoft's admin setting](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-introspection-schema-export).

## Livy (`packed-livy`, `delegated-livy`)

The ordinary HC test always executes concurrent statements, attachment, variable
isolation, cancellation, and sibling survival, even when Fabric allocates separate
backing sessions. The `packed-livy` lane repeats that lifecycle and requires a
shared backing session with distinct REPLs. Packing is a service allocation hint;
failure to pack means the evidence requirement is unmet, not a client defect.

Run `integration-fabric-livy` with a delegated user and both features required to
establish packed isolation and activity discovery. The discovery test creates its
own session and batch. It reports a distinct skip under service-principal auth,
and fails under `delegated-livy` if no user identity was supplied. Include this
filter in both the core and runtime2 periodic delegated runs.

On 2026-09-16 the explicit packed HC lifecycle passed in the persistent workspace.
The follow-up review traced empty discovery pages to Fabric returning only the
count for `$count=true`. The client now retrieves the page separately when that
count indicates matching records. After the fix, both delegated discovery tests
found their own submitted work and passed all three assertions. See
[review-followup-validation.md](review-followup-validation.md) for the diagnostic
and execution evidence.

## Restricted access (`authorization`)

Set `FABRIC_TEST_AUTHORIZATION_MATRIX` to a local JSON file following
`authorization.example.json`, and run `integration-fabric-auth-discovery-authorization` with
`FABRIC_TEST_REQUIRED_FEATURES=authorization`. The file names environment
variables containing audience-specific tokens; it must not contain tokens itself.
Acquire tokens immediately before the run. Two valid restricted identities must
each have an allowed and a denied workspace and opposing OneLake folder grants.
The tests prove valid access before testing denial, and require HTTP 403.

Prepare a semantic model with `SecuredRows` containing id=1/group=a and
id=2/group=b. `ReaderA` filters group=a; `ReaderB` filters group=b; assign the
corresponding users. `ByCustomData` filters group=CUSTOMDATA(). Supply a delegated
model administrator token authorized to impersonate both users. The suite checks
the exact expected row sets for JSON impersonation and Arrow impersonation,
roles, and customData separately. JSON Execute Queries does not support service
principals against RLS models. Keep this a distinct delegated authorization lane.

This configuration does not grant permissions or create tenant identities.
Warehouse granular COPY permissions are not asserted: the writer documentation
now follows Microsoft's Contributor requirement on both workspaces.

## Execution options (`job-options`)

`integration-fabric-jobs-dependencies` creates a temporary Spark notebook and
verifies Python imports, archive extraction and a JVM resource from a jar.
`integration-fabric-jobs-overrides` imports a Python module through Spark job
`additionalLibraryUris` (the library language must match the executable). Both
clean up only their own items/files. The tiny
archives are reproducible with `generate-job-dependencies.py`.

The required `job-options` lane additionally needs:

- `FABRIC_TEST_CROSS_WORKSPACE_ID` and `FABRIC_TEST_CROSS_LAKEHOUSE_ID`: a
  readable Lakehouse in another test workspace; the notebook checks both runtime
  context identifiers.
- `FABRIC_TEST_NOTEBOOK_COMPUTE_JSON`: JSON containing `computeConfiguration`
  with an existing custom `instancePool`, driver/executor memory and cores,
  `numExecutors`, and `mountPoints`; plus `file`, `expected_text`, `pool_setting`
  and `pool_expected`. The last pair names the runtime Spark setting identifying
  the chosen pool and its independently known value. The test reads runtime
  settings and mounted file contents. It does not create or resize pools.
- `FABRIC_TEST_SQL_NOTEBOOK_ID` and `FABRIC_TEST_SQL_NOTEBOOK_WAREHOUSE_ID`:
  a DataWarehouse notebook with a `marker` string parameter that inserts that
  value into `dbo.fabricqueryr_sql_notebook_probe(marker varchar(100))` in the
  specified Warehouse. The test asserts absence before running and exactly one
  matching row afterward, then removes that row.

Run filters `integration-fabric-jobs-dependencies|integration-fabric-jobs-overrides`
with `FABRIC_TEST_REQUIRED_FEATURES=job-options`. Missing prerequisites fail this
lane. A normal service-principal run still executes the self-contained dependency
tests and reports the three configured-fixture cases separately as skips.

## Other feature groups

- `functions`: publish the scalar, structured-output, and error fixtures and
  configure the three `FABRIC_TEST_FUNCTION_*_URL` values documented in
  [user-data-functions.md](user-data-functions.md).
- `shortcut-transforms`: set `FABRIC_TEST_SHORTCUT_TRANSFORMS=true` in a tenant
  supporting the documented CSV-to-Delta transform. Service rejection fails a
  required run.
- `shortcut-cache`: use an identity supported by the workspace cache-reset API.
  Enable external shortcut caching for the test workspace first.
  `PrincipalTypeNotSupported` and `ExternalShortcutCacheDisabled` fail a required run.
- `external-shortcuts`: supply the per-provider matrix in
  [external-shortcut.md](external-shortcut.md). Every supported provider has an
  independent case; all must be configured in a required run.
- `kql-cancellation`: supply the service-canceled operation described in
  [kql-recovery.md](kql-recovery.md).
- `nonutc-schedules` and `workload-schedules`: see
  [schedules.md](schedules.md).

`all` requires every feature and is suitable only for a runner with all fixtures
and supported identities. An ordinary offline test run intentionally skips live
tests; it does not establish any of this service evidence.
