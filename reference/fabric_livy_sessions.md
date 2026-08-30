# Discover and reattach to Microsoft Fabric Livy work

Lists existing Livy sessions or batches and creates a newly
authenticated handle for work that was started by an earlier R process.
Listing never returns authentication credentials. Attaching retrieves
current service state and does not create a new session or batch.

## Usage

``` r
fabric_livy_sessions(
  livy_url,
  high_concurrency = FALSE,
  top = 100L,
  skip = 0L,
  count = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  audience = NULL
)

fabric_livy_batches(
  livy_url,
  top = 100L,
  skip = 0L,
  count = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  audience = NULL
)

fabric_livy_session_attach(
  livy_url,
  session_id,
  high_concurrency = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  verbose = TRUE
)

fabric_livy_batch_attach(
  livy_url,
  batch_id,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  verbose = TRUE
)
```

## Arguments

- livy_url:

  A copied session or batch connection URL, Livy API base URL, or
  enriched Lakehouse object from
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md)

- high_concurrency:

  Whether to list or attach to high-concurrency sessions instead of
  regular sessions

- top:

  Maximum records requested for this page

- skip:

  Number of matching records to skip

- count:

  Whether Fabric should include the total matching record count

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to use
  the normal sign-in flow for a Microsoft Fabric host. A custom
  `livy_url` requires an explicitly supplied token or provider

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- audience:

  Optional sign-in scopes. Delegated sign-in defaults to the required
  Fabric Livy scopes; client credentials require one `.default` audience

- session_id, batch_id:

  Service GUID returned by a list or submit operation

- verbose:

  Logical. Show handle lifecycle messages

## Value

`fabric_livy_sessions()` and `fabric_livy_batches()` return one page as
a tibble with columns `id`, `name`, `state`, `result`, `app_id`, service
timestamps, and `raw`. The tibble has `total_count`, `page_size`, and
`skip` attributes. The attach functions return a
[FabricLivySession](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivySession.md)
or
[FabricLivyBatch](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivyBatch.md)
with a fresh in-process credential.

## Restart recovery

Livy handles intentionally do not serialize their credentials. Store the
service ID, then call the corresponding attach function after restarting
R. Attaching only reconstructs the local handle; it never submits new
Spark work.

## See also

[Microsoft Fabric Livy API
specification](https://github.com/microsoft/fabric-samples/blob/main/docs-samples/data-engineering/Livy-API-swagger/swagger.json)

## Examples

``` r
if (FALSE) { # \dontrun{
workspace <- fabric_workspaces()[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]

sessions <- fabric_livy_sessions(lakehouse)
session <- fabric_livy_session_attach(lakehouse, sessions$id[[1L]])
session$status()

batches <- fabric_livy_batches(lakehouse)
batch <- fabric_livy_batch_attach(lakehouse, batches$id[[1L]])
batch$status()
} # }
```
