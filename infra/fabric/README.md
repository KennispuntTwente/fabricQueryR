# Fabric integration sandbox

This directory contains the real-service test environment for `fabricQueryR`.
Terraform owns the ephemeral workspace, schema-enabled Lakehouse, and access
assignments. `fabric-cicd` publishes the source-controlled seed notebook. The Python package
uploads fixture files, runs the notebook, and writes the manifest consumed by R.

An existing paid Fabric capacity is required. Trial-capacity lifecycle is not
supported by the Microsoft Fabric Terraform provider.

## Prerequisites

- Terraform 1.11 or newer
- `uv`
- Azure CLI authenticated to the target tenant
- A Fabric capacity ID
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

uv --directory tools/fabric-sandbox sync --locked
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

The workflow is manual and nightly. It uses a repository-wide concurrency group so
only one sandbox consumes the test capacity at a time, and runs Terraform destroy
after success or failure. A canceled runner cannot guarantee that final step; a
separate stale-workspace janitor remains a follow-up before enabling high-frequency
CI runs.

## Current fixture scope

The sandbox deploys `TestLakehouse` and `SeedFixtures`, then creates a small
ephemeral Power BI semantic model through the supported push-dataset API. It
creates basic and partitioned Delta tables, including a checkpoint-generating
append, and exposes OneLake, SQL, Livy, and DAX test coordinates through the
generated manifest. The integration suite directly exercises every exported
`fabricQueryR` function. Warehouse, SQL Database, Eventhouse/KQL, and GraphQL
fixtures remain deferred until package functions for those services are added.
