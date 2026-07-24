# Fabric integration sandbox

This directory contains the real-service test environment for `fabricQueryR`.
Terraform owns the ephemeral workspace, schema-enabled Lakehouse, Warehouse, SQL
Database, Eventhouse, KQL database, GraphQL API, and access assignments. `fabric-cicd`
publishes the source-controlled seed notebook. The Python package uploads fixture
files, runs the notebook, seeds the KQL database, and writes the manifest
consumed by R.

An existing paid Fabric capacity is required. Trial-capacity lifecycle is not
supported by the Microsoft Fabric Terraform provider.

## Prerequisites

- Terraform 1.11 or newer
- `uv`
- Azure CLI authenticated to the target tenant
- A Fabric capacity ID
- A capacity/region that supports Warehouse and SQL Database items
- A capacity/region that supports Eventhouse and KQL Database items
- A capacity/region that supports API for GraphQL items
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
export FABRIC_WAREHOUSE_ID="$(terraform -chdir=infra/fabric/terraform output -raw warehouse_id)"
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

Rscript -e 'devtools::test(filter = "integration-fabric", stop_on_failure = TRUE)'
```

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
and have sufficient access to assign the configured capacity. No client secret is
required or expected.

The workflow uses a repository-wide concurrency group so only one sandbox
consumes the test capacity at a time, and runs Terraform destroy after success
or failure. It runs weekly and can also be dispatched manually. CI enables
required integration mode, so missing manifests, tokens, and test dependencies
fail rather than silently skipping the live suite. Because a canceled runner
cannot guarantee the destroy step, a daily
janitor uses the same concurrency group and removes only workspaces carrying
both the `fabricqueryr-ci-` name prefix and `fabricqueryr-ci;` description
marker. `fabric-sandbox cleanup` is a dry run unless `--confirm` is supplied.

## Current fixture scope

The sandbox deploys `TestLakehouse`, `TestWarehouse`, `TestSQLDatabase`,
`TestEventhouse`, `TestKQLDatabase`, `TestGraphQL`, `SeedFixtures`,
`JobFixtures`, `TestPipeline`, and `TestSparkJob`, then creates a small
ephemeral Power BI semantic model through the supported push-dataset API. It
creates basic, partitioned, schema-evolved, column-mapped, and
deletion-vector Delta tables. These cover checkpoint replay, partition
replacement, schema merging, and explicit rejection of unsupported Delta
protocol features. It also creates a deterministic typed Kusto table.

After the seed table is available, the sandbox applies the supported GraphQL
public definition and waits until the schema is executable. The generated
manifest exposes OneLake, all three SQL surfaces, Livy session and batch
coordinates, DAX, Eventhouse, KQL, GraphQL, pipeline, notebook, and Spark job
coordinates. The job fixtures exercise all three job routes supported by the
package; the notebook and uploaded `livy_batch.py` additionally expose
deterministic success, failure, timeout, and cancellation modes. Required
fixtures are not capability-gated: provisioning, discovery, seeding, or
connectivity failures fail the integration job.
