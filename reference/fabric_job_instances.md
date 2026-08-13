# Inspect Microsoft Fabric job history

Lists recent and active job instances for a Fabric item. All pages
returned by Fabric are collected, and each result can be passed directly
to
[`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
[`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
or
[`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md).

## Usage

``` r
fabric_job_instances(
  item,
  workspace = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
)
```

## Arguments

- item:

  Item GUID, exact display name, or an item record returned by a
  discovery function. A discovered record is recommended because it
  includes the item type and workspace ID.

- workspace:

  Workspace GUID, exact display name, or a workspace record. Omit it
  when `item` is a discovered record containing `workspaceId`.

- item_type:

  Optional Fabric item type when `item` is a GUID. A discovered item
  supplies this automatically.

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow A `fabric_job` handle reuses
  its stored credential unless `tenant_id`, `client_id`, `token`, or
  non-empty `auth_args` is supplied explicitly

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when no token source is supplied

- api_base:

  Fabric REST API base URL. Most users should keep the default A
  discovered workspace-specific endpoint is used unless this argument is
  supplied explicitly

- allow_custom_endpoint:

  Logical. Set to `TRUE` only when `api_base` is a non-Microsoft HTTPS
  origin that you trust to receive a Fabric token

## Value

A list of `fabric_job_instance` records. Fabric usually retains at most
100 recently completed instances per item, plus active instances.
Unknown future status and invocation values are returned unchanged.

## Details

Reading history requires an item read permission. The returned records
keep an in-process reference to the supplied credential so they can be
refreshed, waited on, or cancelled. That credential is not retained when
a record is serialized.

## References

[List item job
instances](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances)

## Examples

``` r
if (FALSE) { # \dontrun{
notebook <- fabric_notebooks("Analytics workspace")[[1]]
history <- fabric_job_instances(notebook)
history[[1]]$status
fabric_job_status(history[[1]])
} # }
```
