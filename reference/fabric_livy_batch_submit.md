# Submit a Microsoft Fabric Livy batch job

Starts a complete Spark application file stored in OneLake or ADLS.
Unlike an interactive Livy session, a batch has its own application
lifecycle and is a good fit for repeatable scripts and unattended
processing.

## Usage

``` r
fabric_livy_batch_submit(
  livy_url,
  file,
  name = NULL,
  class_name = NULL,
  args = NULL,
  jars = NULL,
  files = NULL,
  py_files = NULL,
  archives = NULL,
  conf = NULL,
  environment_id = NULL,
  target_lakehouse_id = NULL,
  tags = NULL,
  driver_memory = NULL,
  driver_cores = NULL,
  executor_memory = NULL,
  executor_cores = NULL,
  num_executors = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  verbose = TRUE,
  wait = FALSE,
  timeout = 1200,
  poll_interval = 5,
  cancel_on_timeout = TRUE,
  allow_custom_endpoint = FALSE
)
```

## Arguments

- livy_url:

  A copied Livy connection URL, Livy API base URL, or enriched Lakehouse
  record. Copy the batch-job URL from **Lakehouse settings \> Livy
  endpoint**, or use an item from
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).

- file:

  ABFS/ABFSS URI of the main Python, R, or Java/Scala application file.
  After uploading a script under a Lakehouse's `Files/` area, its
  **Properties** dialog can copy this path.

- name:

  Optional readable job name shown in Fabric monitoring.

- class_name:

  Main class for a Java/Scala application; leave `NULL` for Python or R
  scripts.

- args:

  Optional character vector of command-line arguments passed to the
  application.

- jars:

  Optional JAR dependency URIs.

- files:

  Optional supporting-file URIs copied to the job.

- py_files:

  Optional Python dependency URIs, such as `.py` or `.zip` files.

- archives:

  Optional archive URIs that Spark should unpack.

- conf:

  Optional named list of Spark settings or application-specific values.

- environment_id:

  Optional GUID of a published Fabric Environment whose libraries and
  Spark settings should be used.

- target_lakehouse_id:

  Optional Lakehouse GUID made available as `spark.targetLakehouse`. Use
  this when the application needs an explicit default Lakehouse context.

- tags:

  Optional named list of string labels for monitoring.

- driver_memory, executor_memory:

  Optional Spark memory values such as `"4g"`. Leave `NULL` to use
  Fabric defaults.

- driver_cores, executor_cores, num_executors:

  Optional Spark resource counts. Larger values consume more capacity;
  leave `NULL` unless the workload has been sized deliberately.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.

- token:

  Optional
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer-token string, or token-provider function. With `NULL`,
  `AzureAuth` reuses a matching cached token or starts its normal
  interactive login flow.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- audience:

  Optional OAuth audience/scope vector. With `NULL`, delegated
  authentication requests Microsoft's four required Livy scopes, while
  an AzureAuth client-credentials flow requests the Power BI `.default`
  audience documented for service principals. Supply this explicitly
  when a custom token provider or identity flow requires a different
  token target.

- verbose:

  Logical. Show submission and lifecycle messages.

- wait:

  Logical. `FALSE` returns immediately so other R work can continue;
  `TRUE` waits for a terminal state before returning the same object.

- timeout:

  Maximum seconds to wait when `wait = TRUE`.

- poll_interval:

  Seconds between status checks when waiting.

- cancel_on_timeout:

  Logical. When waiting at submission time, request cancellation if the
  local timeout expires. Defaults to `TRUE`, so a timed out call does
  not normally leave Spark compute running unattended. The structured
  timeout condition always contains the submitted object in its `batch`
  field, including when cancellation fails or is disabled.

- allow_custom_endpoint:

  Logical. Keep `FALSE` to require a Microsoft Fabric API host. Set
  `TRUE` only for a trusted custom HTTPS service, such as a test
  emulator; the Fabric bearer token is sent to this endpoint.

## Value

A
[FabricLivyBatch](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivyBatch.md)
R6 object. Inspect its `$state`, call `$result()` for structured
metadata and logs, and call `$wait()` later when submitting with
`wait = FALSE`.

## Details

Fabric needs a workspace on supported Fabric capacity and a Lakehouse.
The application file must already be accessible through an ABFS/ABFSS
URI; this function does not upload a local script. Use
[`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
first when needed.

Delegated authentication requests the Livy Lakehouse execution/read and
required `Code.Access*` scopes documented by Microsoft. AzureAuth
client-credentials authentication uses
`https://analysis.windows.net/powerbi/api/.default`.

## See also

[Microsoft Fabric batch
jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-batch)

## Examples

``` r
if (FALSE) { # \dontrun{
lakehouse <- fabric_lakehouses("Analytics workspace")[[1]]

batch <- fabric_livy_batch_submit(
  lakehouse,
  file = paste0(
    "abfss://workspace@onelake.dfs.fabric.microsoft.com/",
    "lakehouse/Files/jobs/daily.py"
  ),
  wait = TRUE,
  cancel_on_timeout = TRUE
)
batch$result()
} # }
```
