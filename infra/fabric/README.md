# Fabric integration sandbox

This directory contains the real-service test environment for `fabricQueryR`.
Terraform owns the ephemeral workspace, schema-enabled Lakehouse, Warehouse,
Warehouse snapshot, SQL Database, Eventhouse, KQL database, GraphQL API, and
access assignments. `fabric-cicd`
publishes the source-controlled seed notebook. The Python package uploads fixture
files, runs the notebook, seeds the KQL database, and writes the manifest
consumed by R.

An existing paid Fabric capacity is required. Trial-capacity lifecycle is not
supported by the Microsoft Fabric Terraform provider.

## Prerequisites

- Terraform 1.11 or newer
- `uv`
- Microsoft ODBC Driver 18 and the ADBC Driver Foundry `mssql` driver
- Azure CLI authenticated to the target tenant
- A Fabric capacity ID
- A capacity/region that supports Warehouse, Warehouse snapshot preview, and
  SQL Database items
- A capacity/region that supports Eventhouse and KQL Database items
- A capacity/region that supports API for GraphQL items
- A capacity/region that supports Fabric Spark Runtime 1.3; the preview Delta
  lane additionally requires Fabric Spark Runtime 2.0
- Tenant settings that permit the executing identity to use Fabric APIs and create
  workspaces
- Power BI tenant settings that permit service principals to use Power BI APIs
  and execute semantic-model queries
- Capacity permissions that allow the identity to assign the workspace

For local development, authenticate with `az login --tenant <tenant-id>`. CI uses
Microsoft Entra workload identity federation and `azure/login`, with no client
secret.

## Local lifecycle

Create a local variables file from `terraform/terraform.tfvars.example`, then run:

```bash
terraform -chdir=infra/fabric/terraform init
terraform -chdir=infra/fabric/terraform apply

export FABRIC_WORKSPACE_ID="$(terraform -chdir=infra/fabric/terraform output -raw workspace_id)"
export FABRIC_WORKSPACE_NAME="$(terraform -chdir=infra/fabric/terraform output -raw workspace_name)"
export FABRIC_LAKEHOUSE_ID="$(terraform -chdir=infra/fabric/terraform output -raw lakehouse_id)"
export FABRIC_NON_SCHEMA_LAKEHOUSE_ID="$(terraform -chdir=infra/fabric/terraform output -raw non_schema_lakehouse_id)"
export FABRIC_WAREHOUSE_ID="$(terraform -chdir=infra/fabric/terraform output -raw warehouse_id)"
export FABRIC_WAREHOUSE_SNAPSHOT_ID="$(terraform -chdir=infra/fabric/terraform output -raw warehouse_snapshot_id)"
export FABRIC_SQL_DATABASE_ID="$(terraform -chdir=infra/fabric/terraform output -raw sql_database_id)"
export FABRIC_EVENTHOUSE_ID="$(terraform -chdir=infra/fabric/terraform output -raw eventhouse_id)"
export FABRIC_KQL_DATABASE_ID="$(terraform -chdir=infra/fabric/terraform output -raw kql_database_id)"
export FABRIC_GRAPHQL_API_ID="$(terraform -chdir=infra/fabric/terraform output -raw graphql_api_id)"

uv --directory tools/fabric-sandbox sync --locked
uv --directory tools/fabric-sandbox run pytest
uv --directory tools/fabric-sandbox run fabric-sandbox doctor
uv --directory tools/fabric-sandbox run fabric-sandbox deploy
uv --directory tools/fabric-sandbox run fabric-sandbox seed
uv --directory tools/fabric-sandbox run fabric-sandbox discover

# Install the external ADBC driver once per test machine.
uvx dbc==0.3.0 install "mssql>=1.5,<2"

Rscript -e 'devtools::test(filter = "integration-fabric", stop_on_failure = TRUE)'
```

Both the sandbox command and the R test helper resolve
`.fabric-test-manifest.json` at the repository root by default, even though
`testthat` runs tests from a nested working directory. Set
`FABRIC_TEST_MANIFEST` only to override that shared location.
Before running the seed notebook, the sandbox sets the dedicated workspace to
the selected runtime lane. The default `core` lane uses the GA Fabric Runtime
1.3. The `preview` lane uses Fabric Runtime 2.0 for V2 checkpoints, stable type
widening, Variant, and other forward-looking Delta coverage. Select that lane
by setting both `FABRIC_SPARK_RUNTIME_LANE=preview` and
`FABRIC_SPARK_RUNTIME_VERSION=2.0`; mismatched values fail configuration.
Seeding also fails instead of replacing a named workspace default Environment.
The observed Spark and Delta build versions are stored in the manifest and
included in the fixture revision, so a changed hosted runtime cannot silently
reuse fixtures created by a different build.

The Delta seed matrix includes classic and V2 checkpoints, checkpoint
sidecars, sparse/dense/checkpoint deletion vectors, name- and ID-mapped nested
struct/array/map values across renames and drops, top-level and nested type
widening, `void`, exact scalar values, date/boolean/integer/decimal/timestamp/
timestamp-NTZ/binary/null partitions, a shallow clone, and Variant files written
before and after shredding is enabled. A row-tracking table is updated and
deleted after creation; Spark verifies its hidden row IDs/commit versions, while
the R reader is checked against the remaining complete logical rows. The R
integration suite assigns every
fixture an explicit disposition: representative tables have static exact-value
assertions, supported protocol features are compared with Spark-materialized
neutral references (including deep Arrow equality), unsupported tables assert
specific errors, and their neutral references are fully scanned. Spark also
publishes an independent, schema-aware canonical oracle for every logical row
of the supported protocol-feature tables; the R suite compares complete scalar
and nested values rather than only row counts and keys.

The default sandbox deliberately does not create tenant-specific OneLake
RLS/CLS role assignments or a workspace private link and private DNS path. Its
live access boundary covers an unrestricted storage token and an invalid-token
denial through the global endpoint. Regional/private host construction is unit
tested, but security-policy enforcement and private-network connectivity must
be validated by deployments that configure those tenant and network resources.

Always remove the workspace after testing:

```bash
terraform -chdir=infra/fabric/terraform destroy
```

The generated `.fabric-test-manifest.json`, Terraform state, plans, variables, and
Python virtual environment are ignored by Git. Do not commit access tokens or live
tenant/item identifiers.

## GitHub Actions configuration

Create a protected GitHub environment named `fabric-integration` and define these
environment or repository variables:

| Variable | Purpose |
| --- | --- |
| `AZURE_CLIENT_ID` | Entra application/client ID with a GitHub OIDC federated credential |
| `AZURE_TENANT_ID` | Fabric tenant ID |
| `AZURE_SUBSCRIPTION_ID` | Azure subscription used by `azure/login` |
| `FABRIC_CAPACITY_ID` | Existing paid Fabric capacity assigned to ephemeral workspaces |

The workflow requests `id-token: write`, logs in with `azure/login`, and uses the
resulting Azure CLI session for Terraform and `fabric-cicd`. The Entra application
must be permitted by the Fabric tenant settings, be allowed to create workspaces,
and have sufficient access to assign the configured capacity. The sandbox itself
does not require a client secret for provisioning.

To exercise `fabricQueryR`'s own `AzureAuth` token-acquisition path in addition
to the service tests that use short-lived Azure CLI tokens, define the protected
environment secret `FABRIC_TEST_AUTH_CLIENT_SECRET`. The smoke test
uses it with `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` in a client-credentials
flow, disables the AzureAuth token cache, and verifies that the application can
discover the ephemeral workspace. Required CI fails when the secret is absent;
optional local runs skip only this authentication smoke test. Use a dedicated,
short-lived test secret; the federated workflow login remains responsible for
sandbox provisioning.

The workflow provisions and seeds two isolated workspaces: a Runtime 1.3 `core`
workspace for authentication/discovery, KQL/GraphQL, SQL, Livy, item jobs, and
Power BI, plus a Runtime 2.0 `preview` workspace for OneLake/Delta compatibility
coverage. Each feature job downloads its lane's generated manifest, acquires
its own short-lived tokens, and uses independent R sessions. Terraform state is
retained as a one-day workflow artifact and
consumed by a final teardown job after every matrix leg succeeds, fails, or is
skipped.

A repository-wide concurrency group ensures only one sandbox consumes the test
capacity at a time. The workflow runs weekly and can also be dispatched
manually. CI enables required integration mode, so missing manifests, tokens,
and test dependencies fail rather than silently skipping the live suite.
Because canceling the entire workflow cannot guarantee the teardown job runs, a
daily janitor uses the same concurrency group and removes only workspaces
carrying both the `fabricqueryr-ci-` name prefix and `fabricqueryr-ci;`
description marker. `fabric-sandbox cleanup` is a dry run unless `--confirm` is
supplied.

### Persistent interactive sandbox

The manually dispatched **Manage persistent Fabric sandbox** workflow creates
`fabricqueryr-dev-dhrkoning` from the same Terraform resources, item
definitions, and seed fixtures as the ephemeral integration workflow. Choose
`rebuild` to delete the repository-owned workspace with that exact name and
recreate it from source, or `teardown` to delete it without rebuilding.
The persistent sandbox uses the preview lane so one interactive workspace
contains the complete advanced Delta fixture matrix.

The workflow grants the configured Entra user object ID the `Admin` workspace
role. Fabric role assignments use the object ID and principal type (`User`), so
the guest user principal name is not needed. A
`fabricqueryr-persistent; ...` description marker records the repository,
owner, managing workflow, rebuild time, and workflow run. Reset and teardown
only delete a workspace when its exact name, repository, type, and complete
ownership marker all match. The daily ephemeral-workspace janitor ignores this
marker.

The persistent workflow intentionally has no final Terraform destroy step.
Successful rebuilds leave the workspace available for interactive package
testing, and the Actions job summary reports its name and ID. It shares the
integration concurrency group so rebuild, teardown, ephemeral integration, and
janitor runs cannot modify Fabric sandboxes concurrently.

#### Run the integration suite locally as the workspace admin

From an interactive R session at the repository root:

```r
source("tools/fabric-sandbox/local-integration.R")
run_fabric_integration_tests()
```

To run one of the split feature groups while developing, pass its testthat
filter. For example:

```r
run_fabric_integration_tests(filter = "integration-fabric-sql")
```

User Data Function invocation has a separate opt-in live fixture because the
current User Data Function item-management API supports delegated users but not
the service principal that provisions the disposable CI workspace. Publish
three public functions with these signatures:

```python
@udf.function()
def echoScalar(value: str) -> str:
    return value

@udf.function()
def echoStructured(label: str, values: list[int], metadata: dict) -> dict:
    return {
        "label": label,
        "values": values,
        "total": sum(values),
        "metadata": metadata,
    }

@udf.function()
def raiseValidation(value: int) -> int:
    if value < 0:
        raise fn.UserThrownError(
            "value must be non-negative",
            {"value": value},
        )
    return value
```

Set `FABRIC_TEST_FUNCTION_SCALAR_URL`,
`FABRIC_TEST_FUNCTION_STRUCTURED_URL`, and `FABRIC_TEST_FUNCTION_ERROR_URL` to
their copied Public URLs, then run:

```r
run_fabric_integration_tests(filter = "integration-fabric-functions")
```

The normal local runner supplies the Power BI token. Missing function URLs skip
this opt-in group; the offline suite always covers disabled public access,
service and client timeouts, oversized responses, structured user errors, and
secret redaction.

The local runner:

1. checks the R, ODBC, ADBC, `uv`, and sandbox-tool dependencies;
2. reuses the tenant, client ID, and offline refresh token from a cached Fabric
   `AzureAuth` token;
3. silently obtains user tokens for Fabric, Power BI, SQL, OneLake, and Kusto,
   prompting through AzureAuth only when the cached login cannot do so;
4. verifies the Fabric token's `oid` claim is the configured workspace admin;
5. resolves the single marked `fabricqueryr-dev-dhrkoning` workspace;
6. regenerates `.fabric-test-manifest.json` from its live items; and
7. calls the existing
   `devtools::test(filter = "integration-fabric", stop_on_failure = TRUE)`.
   The AzureAuth acquisition test uses the interactive user context locally;
   CI continues to exercise its client-credentials configuration.

Raw bearer tokens exported by the runner stay in the R process and child
discovery process only. The runner restores the previous environment variables
and token-provider option when it finishes; AzureAuth continues to manage its
normal user token cache. If a matching AzureAuth token is missing, AzureAuth
starts its normal interactive browser login. For a terminal without a usable
browser, request device-code login explicitly:

```r
run_fabric_integration_tests(
  auth_args = list(auth_type = "device_code")
)
```

When no Fabric token has ever been cached and
`FABRICQUERYR_TENANT_ID` is unset, the runner uses the persistent sandbox
tenant domain. `FABRICQUERYR_CLIENT_ID` remains optional and defaults to the
Azure CLI public client application. If multiple Fabric identities are cached,
set `FABRICQUERYR_TENANT_ID` explicitly.

If the cached token belongs to another user, the object-ID check stops before
workspace discovery. Force a fresh interactive login with:

```r
run_fabric_integration_tests(
  auth_args = list(use_cache = FALSE)
)
```

By default, the runner installs the locked ADBC `mssql` driver through `uvx`
when it is missing. Set `install_adbc_driver = FALSE` to require a preinstalled
driver instead. Microsoft ODBC Driver 18 must already be installed because its
installation is operating-system specific.

## Current fixture scope

The sandbox deploys `TestLakehouse`, `TestWarehouse`, `TestWarehouseSnapshot`,
`TestSQLDatabase`,
`TestEventhouse`, `TestKQLDatabase`, `TestGraphQL`, `SeedFixtures`,
`JobFixtures`, `TestPipeline`, and `TestSparkJob`, then creates a small
ephemeral Power BI semantic model through the supported push-dataset API. Its
Delta matrix includes basic, empty, partitioned, typed/null-partition,
schema-evolved, name- and ID-column-mapped, deletion-vector stress,
exact-numeric, nested, type-widened, V2-checkpoint, shallow-clone, and Variant
tables. These exercise active-file replay, partition replacement, metadata-only
rename/drop, exact BIGINT/DECIMAL boundaries, timestamp-NTZ, multiple files and
DV mutations, V2 sidecars, absolute OneLake AddFile paths, and current reader
features. The OneLake suite also reads the Warehouse Delta export, checks exact
Warehouse values, and enforces an explicit assertion disposition for every
discovered Delta fixture. The sandbox creates
matching deterministic typed SQL tables in the Warehouse and SQL Database,
  plus deterministic typed Kusto query and queued-ingestion tables. The
  ingestion table has a predefined CSV mapping and consumes the staged
  `Files/fixtures/basic.csv` fixture in the live ingestion round trip.

Warehouse Delta-log publication is asynchronous after a SQL commit. Seeding
therefore polls OneLake until a log file modified after the fixture rebuild is
visible before allowing integration tests to start.

After the seed table is available, the sandbox refreshes the SQL analytics
endpoint and requires a successful per-table sync status before applying the
supported GraphQL public definition. A missing or `NotRun` fixture is retried
with a clean table rebuild. It then waits briefly for the schema to become
executable. The generated manifest exposes OneLake, all three SQL surfaces,
Livy session and batch
coordinates, DAX, Eventhouse, KQL, GraphQL, pipeline, notebook, and Spark job
coordinates. The job fixtures exercise all three job routes supported by the
package; the notebook and uploaded `livy_batch.py` additionally expose
deterministic success, failure, timeout, and cancellation modes. Required
fixtures are not capability-gated: provisioning, discovery, seeding, or
connectivity failures fail the integration job.

The SQL portion runs the ODBC and ADBC backends against the Lakehouse SQL
analytics endpoint, Warehouse, Warehouse snapshot, and SQL Database. It checks
direct DBI connections and lifecycle, table metadata, discovery records, portal
connection strings, bare server/database pairs, bound parameters, typed and
null values, collected tibbles, and Arrow streams that remain consumable after
the one-shot helper closes its connection. ODBC and ADBC results are normalized
and compared for the writable SQL items. The stream checks cover both nanoarrow
collection and conversion to an `arrow::RecordBatchReader`.
