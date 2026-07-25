# Submit a Microsoft Fabric Livy batch job

Submits a Spark application stored in OneLake/ADLS and returns an R6
object for status, logs, result inspection, timeout handling, and
cancellation.

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
  verbose = TRUE,
  wait = FALSE,
  timeout = 1200,
  poll_interval = 5
)
```

## Arguments

- livy_url:

  A copied Livy connection URL, Livy API base URL, or enriched Lakehouse
  record.

- file:

  ABFS URI of the application file to execute.

- name:

  Optional job name.

- class_name:

  Optional main class for Java/Scala applications.

- args, jars, files, py_files, archives:

  Optional character vectors passed to Livy.

- conf:

  Optional named list of Spark settings.

- environment_id:

  Optional Fabric Environment ID.

- target_lakehouse_id:

  Optional Lakehouse ID set as `spark.targetLakehouse`.

- tags:

  Optional named list of string tags.

- driver_memory, executor_memory:

  Optional Spark memory strings.

- driver_cores, executor_cores, num_executors:

  Optional Spark resource counts.

- tenant_id:

  Microsoft Entra tenant ID.

- client_id:

  Microsoft Entra application ID.

- token:

  Optional
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer-token string, or token-provider function. With `NULL`,
  `AzureAuth` reuses a matching cached token or starts its normal
  interactive login flow.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- verbose:

  Logical. Emit lifecycle messages.

- wait:

  Logical. Wait for the job to finish before returning.

- timeout, poll_interval:

  Wait controls in seconds.

## Value

A
[FabricLivyBatch](https://lukakoning.github.io/fabricQueryR/reference/FabricLivyBatch.md).

## Details

Requests use the `https://api.fabric.microsoft.com/.default` audience.
Delegated authentication requires the Livy Lakehouse execution/read and
required `Code.Access*` scopes documented by Microsoft.

## See also

[Microsoft Fabric batch
jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-batch)
