# Required Fabric feature lanes

Set `FABRIC_TEST_REQUIRED_FEATURES` to comma-separated feature names when running
a designated feature lane. A missing prerequisite is then a failure, rather than
a skip. These runs supplement the ordinary service-principal integration suite;
a skipped optional feature is not execution evidence.

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
