# Invoke a published Fabric user data function

Calls the public REST endpoint for one published Microsoft Fabric user
data function and returns the service's synchronous execution result.
Function definition, publication, and deployment are intentionally
outside this helper's scope.

## Usage

``` r
fabric_function_invoke(
  function_url,
  parameters = list(),
  timeout = 110,
  idempotent = FALSE,
  max_response_bytes = .fabric_function_response_limit,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  audience = NULL
)
```

## Arguments

- function_url:

  Complete public URL copied from the published function's properties in
  Fabric. A discovered UserDataFunction item is not sufficient because
  the item API does not return the public function URL.

- parameters:

  Named list, data frame, or named atomic vector serialized as the JSON
  object supplied to the function. Use
  [`list()`](https://rdrr.io/r/base/list.html) for a function with no
  parameters.

- timeout:

  Positive client request timeout in seconds. Fabric currently limits
  execution through a public function endpoint to 100 seconds.

- idempotent:

  Logical. Permit bounded retries after transient failures. Keep `FALSE`
  for functions whose side effects cannot safely be repeated.

- max_response_bytes:

  Positive whole-number client limit for the complete response body. The
  default is 32 MiB, slightly above Fabric's documented 30 MB
  function-output limit.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as
  fallback.

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow.

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- audience:

  OAuth audience/scope passed to the credential. `NULL` selects the
  documented scope from the authentication flow. Set this only for a
  custom token provider or unusual identity flow.

## Value

A `fabric_function_result` list with `function_name`, `invocation_id`,
`status`, `output`, `errors`, `http_status`, and `response`. Function
`output` is returned unchanged because field names such as `token` can
be legitimate domain data. The rest of `response` is redacted and
retains unknown future fields. Inspect `status` and `errors`; receiving
a result does not by itself mean the function succeeded.

## Before you invoke

Publish the user data functions item, switch it to **Run only** mode,
enable **Public access** for the function, and copy its **Public URL**
from the Fabric portal. Pass that complete URL to `function_url`; item
discovery does not currently expose enough information to derive a
public function URL safely.

Parameter names and values must match the published Python signature.
Fabric supports JSON strings, ISO 8601 datetime strings, booleans,
numbers, arrays, and objects as inputs. The top-level `parameters`
object therefore needs unique, non-empty names. A named atomic vector is
converted to a named list; use [`I()`](https://rdrr.io/r/base/AsIs.html)
around a one-element value when it must remain a JSON array.

## Permissions and authentication

Interactive authentication requires the Power BI delegated permission
`UserDataFunction.Execute.All` or `Item.Execute.All`, plus Execute
permission on the user data functions item. Service-to-service callers
can use an application credential with the Power BI `.default` audience
and the required tenant and item access. Most users can leave
`audience = NULL`; fabricQueryR selects the documented delegated scope
or application audience for the authentication flow.

Application authentication for the public invocation endpoint is
distinct from authentication used by connections inside the function.
Microsoft currently does not support using a service principal through
connections managed by user data functions to access Fabric items or
data sources. A service principal can therefore invoke a compatible
function while a function that relies on an unsupported managed
connection can still fail.

The function URL is a credential boundary. Tokens are sent to the
explicitly supplied HTTPS endpoint. URLs containing credentials, query
parameters, fragments, or nonstandard ports are rejected.

## Results, retries, and limits

Fabric reports `Succeeded`, `BadRequest`, `Failed`, `Timeout`, and
`ResponseTooLarge` through one response envelope. Valid envelopes remain
inspectable as `fabric_function_result` objects even when Fabric uses a
non-success HTTP status. Authentication, authorization, throttling, and
malformed service responses continue to raise the package's typed HTTP
or response errors. The documentation describes an error `name`, while
current responses can use `errorCode`; `errors` adds `name` as an alias
when needed and `response` retains the original service shape.

Invocations are not retried by default because functions can have
arbitrary side effects. Set `idempotent = TRUE` only when repeating the
function is safe; this enables the package's bounded retries for
transport failures, throttling, and transient HTTP responses.

Fabric limits public-endpoint execution to 100 seconds, request
parameters to 4 MB, and a function's return value to 30 MB. The default
110-second client timeout allows the service timeout response to arrive.
The 32 MiB client response cap leaves room for Fabric's envelope around
a 30 MB output. Secret-named fields and bearer-token text are redacted
recursively from errors, response metadata, and conditions. Function
`output` is domain data and is returned unchanged, even when it contains
secret-like field names.

## References

[Invoke user data functions from a Python
application](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/tutorial-invoke-from-python-app)

[Fabric user data functions service
limits](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/user-data-functions-service-limits)

[Fabric user data function programming
model](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/python-programming-model)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the user data functions item that owns the published function
workspace <- fabric_workspaces()[[1L]]
function_item <- fabric_user_data_functions(workspace)[[1L]]
function_item$displayName

# Discovery cannot expose a function URL yet. Copy the published function's
# complete Invoke URL from this item's Run-only settings into this variable
function_url <- Sys.getenv("FABRIC_FUNCTION_URL")

# Parameter names must match the published Python function signature
result <- fabric_function_invoke(
  function_url,
  parameters = list(
    customerName = "Ada",
    order = list(id = 42L, lines = I(c("A", "B")))
  )
)

# Inspect the output and any function-level errors returned by Fabric
result$status
result$output
result$errors
} # }
```
