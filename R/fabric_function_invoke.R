.fabric_function_request_limit <- 4 * 1024^2
.fabric_function_response_limit <- 32 * 1024^2

#' Invoke a published Fabric user data function
#'
#' Calls the public REST endpoint for one published Microsoft Fabric user data
#' function and returns the service's synchronous execution result. Function
#' definition, publication, and deployment are intentionally outside this
#' helper's scope.
#'
#' @section Before you invoke:
#' Publish the user data functions item, switch it to **Run only** mode, enable
#' **Public access** for the function, and copy its **Public URL** from the
#' Fabric portal. Pass that complete URL to `function_url`; item discovery does
#' not currently expose enough information to derive a public function URL
#' safely.
#'
#' Parameter names and values must match the published Python signature. Fabric
#' supports JSON strings, ISO 8601 datetime strings, booleans, numbers, arrays,
#' and objects as inputs. The top-level `parameters` object therefore needs
#' unique, non-empty names. A named atomic vector is converted to a named list;
#' use [I()] around a one-element value when it must remain a JSON array.
#'
#' @section Permissions and authentication:
#' Delegated authentication defaults to the narrower Power BI permission
#' `UserDataFunction.Execute.All`. Microsoft also accepts the broader
#' `Item.Execute.All` permission; use it only when the app registration grants
#' that scope, and pass its full scope URL explicitly through `audience`. Either
#' delegated scope still requires Execute permission on the user data functions
#' item. Service-to-service callers can use an application credential with the
#' Power BI `.default` audience and the required tenant and item access.
#'
#' Application authentication for the public invocation endpoint is distinct
#' from authentication used by connections inside the function. Microsoft
#' currently does not support using a service principal through connections
#' managed by user data functions to access Fabric items or data sources. A
#' service principal can therefore invoke a compatible function while a
#' function that relies on an unsupported managed connection can still fail.
#'
#' The function URL is a credential boundary. Tokens are sent to the explicitly
#' supplied HTTPS endpoint. URLs containing credentials,
#' query parameters, fragments, or nonstandard ports are rejected.
#'
#' @section Results, retries, and limits:
#' Fabric reports `Succeeded`, `BadRequest`, `Failed`, `Timeout`, and
#' `ResponseTooLarge` through one response envelope. Valid envelopes remain
#' inspectable as `fabric_function_result` objects even when Fabric uses a
#' non-success HTTP status. Authentication, authorization, throttling, and
#' malformed service responses continue to raise the package's typed HTTP or
#' response errors.
#' The documentation describes an error `name`, while current responses can use
#' `errorCode`; `errors` adds `name` as an alias when needed and `response`
#' retains the original service shape.
#'
#' Invocations are not retried by default because functions can have arbitrary
#' side effects. Set `idempotent = TRUE` only when repeating the function is
#' safe; this enables the package's bounded retries for transport failures,
#' throttling, and transient HTTP responses.
#'
#' Fabric limits public-endpoint execution to 100 seconds, request parameters
#' to 4 MB, and a function's return value to 30 MB. The default 110-second
#' client timeout allows the service timeout response to arrive. The 32 MiB
#' client response cap leaves room for Fabric's envelope around a 30 MB output.
#' Secret-named fields and bearer-token text are redacted recursively from
#' errors, response metadata, and conditions. Function `output` is domain data
#' and is returned unchanged, even when it contains secret-like field names.
#'
#' @param function_url Complete public URL copied from the published function's
#'   properties in Fabric. A discovered UserDataFunction item is not sufficient
#'   because the item API does not return the public function URL.
#' @param parameters Named list, data frame, or named atomic vector serialized
#'   as the JSON object supplied to the function. Use `list()` for a function
#'   with no parameters.
#' @param timeout Positive client request timeout in seconds. Fabric currently
#'   limits execution through a public function endpoint to 100 seconds.
#' @param idempotent Logical. Permit bounded retries after transient failures.
#'   Keep `FALSE` for functions whose side effects cannot safely be repeated.
#' @param max_response_bytes Positive whole-number client limit for the complete
#'   response body. The default is 32 MiB, slightly above Fabric's documented
#'   30 MB function-output limit.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as fallback.
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let 'fabricQueryR' use its normal sign-in flow for a Microsoft Fabric
#'   host. A custom `function_url` requires an explicitly supplied token or
#'   provider so an automatically acquired Fabric credential is not forwarded
#'   to another host.
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()].
#' @param audience OAuth audience/scope passed to the credential. `NULL`
#'   selects `UserDataFunction.Execute.All` for delegated sign-in or the Power
#'   BI `.default` audience for client credentials. To use Microsoft's broader
#'   alternative, pass
#'   `"https://analysis.windows.net/powerbi/api/Item.Execute.All"` explicitly.
#'
#' @return A `fabric_function_result` list with `function_name`,
#'   `invocation_id`, `status`, `output`, `errors`, `http_status`, and
#'   `response`. Function `output` is returned unchanged because field names
#'   such as `token` can be legitimate domain data. The rest of `response` is
#'   redacted and retains unknown future fields. Inspect `status` and `errors`;
#'   receiving a result does not by itself mean the function succeeded.
#' @references
#' [Invoke user data functions from a Python application](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/tutorial-invoke-from-python-app)
#'
#' [Fabric user data functions service limits](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/user-data-functions-service-limits)
#'
#' [Fabric user data function programming model](https://learn.microsoft.com/en-us/fabric/data-engineering/user-data-functions/python-programming-model)
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover the user data functions item that owns the published function
#' workspace <- fabric_workspaces()[[1L]]
#' function_item <- fabric_user_data_functions(workspace)[[1L]]
#' function_item$displayName
#'
#' # Discovery cannot expose a function URL yet. Copy the published function's
#' # complete Invoke URL from this item's Run-only settings into this variable
#' function_url <- Sys.getenv("FABRIC_FUNCTION_URL")
#'
#' # Parameter names must match the published Python function signature
#' result <- fabric_function_invoke(
#'   function_url,
#'   parameters = list(
#'     customerName = "Ada",
#'     order = list(id = 42L, lines = I(c("A", "B")))
#'   )
#' )
#'
#' # Inspect the output and any function-level errors returned by Fabric
#' result$status
#' result$output
#' result$errors
#' }
fabric_function_invoke <- function(
  function_url,
  parameters = list(),
  timeout = 110,
  idempotent = FALSE,
  max_response_bytes = .fabric_function_response_limit,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL
) {
  function_validate_logical(idempotent, "idempotent")
  function_validate_positive_number(timeout, "timeout")
  function_validate_byte_limit(max_response_bytes, "max_response_bytes")

  endpoint <- function_validate_url(function_url)
  fabric_require_explicit_custom_token(endpoint, token, "function_url")
  payload <- function_serialize_parameters(parameters)
  payload_bytes <- charToRaw(enc2utf8(payload))
  if (length(payload_bytes) > .fabric_function_request_limit) {
    .fabric_abort(
      paste0(
        "parameters exceed Fabric's 4 MB public-function request limit"
      ),
      class = "fabric_function_request_too_large",
      request_bytes = length(payload_bytes),
      max_request_bytes = .fabric_function_request_limit
    )
  }

  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  audience <- function_resolve_audience(
    audience,
    token = token,
    auth_args = auth_args
  )

  request <- httr2::request(endpoint) |>
    httr2::req_headers(Accept = "application/json") |>
    httr2::req_body_raw(payload_bytes, type = "application/json") |>
    httr2::req_timeout(timeout) |>
    httr2::req_options(maxfilesize_large = max_response_bytes)
  response <- tryCatch(
    .httr2_perform(
      request,
      credential = credential,
      audience = audience,
      idempotent = idempotent,
      return_error_response = TRUE
    ),
    error = function(error) {
      if (
        grepl(
          "maximum file size|filesize exceeded|file size exceeded",
          conditionMessage(error),
          ignore.case = TRUE
        )
      ) {
        .fabric_abort(
          "Function response exceeded max_response_bytes during transfer",
          class = "fabric_function_response_too_large",
          response_bytes = NA_real_,
          max_response_bytes = max_response_bytes,
          parent = error
        )
      }
      rlang::cnd_signal(error)
    }
  )

  function_parse_response(
    response,
    max_response_bytes = max_response_bytes
  )
}

# Validate a copied public function URL and its credential boundary. Returns a
# normalized URL suitable for an authenticated POST request
function_validate_url <- function(function_url) {
  function_url <- function_required_string(function_url, "function_url")
  function_url <- sub("/+$", "", trimws(function_url))
  parsed <- try(httr2::url_parse(function_url), silent = TRUE)
  valid_url <- !inherits(parsed, "try-error") &&
    identical(parsed$scheme, "https") &&
    nzchar(parsed$hostname %||% "") &&
    !nzchar(parsed$username %||% "") &&
    !nzchar(parsed$password %||% "") &&
    (!nzchar(parsed$port %||% "") || identical(parsed$port, "443")) &&
    !length(parsed$query %||% list()) &&
    !nzchar(parsed$fragment %||% "")
  if (!valid_url) {
    .fabric_abort(
      "function_url must be a valid HTTPS public function URL"
    )
  }

  route <- paste0(
    "^/v1/workspaces/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-",
    "[0-9a-f]{4}-[0-9a-f]{12}/userDataFunctions/",
    "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-",
    "[0-9a-f]{12}/functions/[A-Za-z_][A-Za-z0-9_]*/invoke$"
  )
  if (!grepl(route, parsed$path %||% "", ignore.case = TRUE)) {
    .fabric_abort(
      paste0(
        "function_url must use Fabric's documented public function ",
        "invocation route"
      )
    )
  }

  function_url
}

# Serialize a named top-level R object exactly once so its encoded request size
# can be enforced before authentication or network activity
function_serialize_parameters <- function(parameters) {
  if (is.atomic(parameters) && !is.null(names(parameters))) {
    parameters <- as.list(parameters)
  }
  if (!is.list(parameters)) {
    .fabric_abort(
      "parameters must be a named R object, such as a named list"
    )
  }
  if (
    length(parameters) &&
      (is.null(names(parameters)) ||
        anyNA(names(parameters)) ||
        !all(nzchar(names(parameters))) ||
        anyDuplicated(names(parameters)))
  ) {
    .fabric_abort(
      "parameters must have unique, non-empty names"
    )
  }
  if (!length(parameters)) {
    return("{}")
  }

  encoded <- try(
    jsonlite::toJSON(
      parameters,
      auto_unbox = TRUE,
      null = "null",
      na = "null",
      dataframe = "columns",
      digits = NA
    ),
    silent = TRUE
  )
  if (inherits(encoded, "try-error")) {
    .fabric_abort(
      "parameters must contain only JSON-serializable values",
      class = "fabric_function_parameters_error"
    )
  }
  as.character(encoded)
}

# Select the narrow delegated scope for user sign-in and the Power BI resource
# audience for application credentials, following Fabric's invocation docs
function_resolve_audience <- function(audience, token, auth_args) {
  if (!is.null(audience)) {
    return(function_required_string(audience, "audience"))
  }
  if (is.null(token) && fabric_uses_client_credentials(auth_args)) {
    .fabric_audience$power_bi
  } else {
    .fabric_audience$user_data_function
  }
}

# Decode one bounded Fabric invocation response. Valid execution envelopes are
# domain results regardless of HTTP status; all other HTTP failures retain the
# package's standard typed and redacted error behavior
function_parse_response <- function(
  response,
  max_response_bytes = .fabric_function_response_limit
) {
  function_validate_byte_limit(max_response_bytes, "max_response_bytes")
  status_code <- httr2::resp_status(response)
  content_length <- suppressWarnings(as.numeric(
    httr2::resp_header(response, "content-length") %||% NA_character_
  ))
  if (
    length(content_length) == 1L &&
      !is.na(content_length) &&
      is.finite(content_length) &&
      content_length > max_response_bytes
  ) {
    function_stop_response_too_large(
      response,
      response_bytes = content_length,
      max_response_bytes = max_response_bytes
    )
  }

  body <- httr2::resp_body_raw(response)
  if (length(body) > max_response_bytes) {
    function_stop_response_too_large(
      response,
      response_bytes = length(body),
      max_response_bytes = max_response_bytes
    )
  }
  payload <- try(
    httr2::resp_body_json(
      response,
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    ),
    silent = TRUE
  )
  execution_status_codes <- c(200L, 400L, 403L, 408L, 409L, 422L, 500L)
  valid <- status_code %in%
    execution_status_codes &&
    !inherits(payload, "try-error") &&
    function_is_result_envelope(payload)
  if (!valid) {
    if (status_code >= 400L) {
      .httr2_stop_http(response)
    }
    .fabric_abort(
      "The Fabric function endpoint returned a malformed response envelope",
      class = "fabric_function_response_error",
      response_metadata = .httr2_response_metadata(response)
    )
  }

  output <- payload$output
  payload <- .httr2_redact_object(payload)
  payload$output <- output
  errors <- payload$errors
  if (
    length(errors) &&
      !is.null(names(errors)) &&
      any(names(errors) %in% c("name", "errorCode", "message", "properties"))
  ) {
    errors <- list(errors)
  }
  errors <- lapply(errors, function(error) {
    if (
      is.list(error) &&
        is.null(error$name) &&
        is.character(error$errorCode) &&
        length(error$errorCode) == 1L &&
        !is.na(error$errorCode) &&
        nzchar(error$errorCode)
    ) {
      error$name <- error$errorCode
    }
    error
  })
  structure(
    list(
      function_name = payload$functionName,
      invocation_id = payload$invocationId,
      status = payload$status,
      output = output,
      errors = errors,
      http_status = status_code,
      response = payload
    ),
    class = c("fabric_function_result", "list")
  )
}

# Check the documented five-field execution envelope while allowing unknown
# status values and additional response fields for forward compatibility
function_is_result_envelope <- function(payload) {
  if (!is.list(payload) || is.null(names(payload))) {
    return(FALSE)
  }
  required <- c(
    "functionName",
    "invocationId",
    "status",
    "output",
    "errors"
  )
  if (!all(required %in% names(payload))) {
    return(FALSE)
  }
  strings <- payload[c("functionName", "invocationId", "status")]
  valid_strings <- vapply(
    strings,
    function(value) {
      is.character(value) &&
        length(value) == 1L &&
        !is.na(value) &&
        nzchar(value)
    },
    logical(1)
  )
  all(valid_strings) && is.list(payload$errors)
}

# Raise a credential-free size condition without reading or attaching the
# oversized body
function_stop_response_too_large <- function(
  response,
  response_bytes,
  max_response_bytes
) {
  .fabric_abort(
    sprintf(
      "Fabric function response is %s bytes, exceeding the %s-byte client limit",
      format(response_bytes, scientific = FALSE, trim = TRUE),
      format(max_response_bytes, scientific = FALSE, trim = TRUE)
    ),
    class = "fabric_function_response_too_large",
    response_bytes = response_bytes,
    max_response_bytes = max_response_bytes,
    http_status = httr2::resp_status(response),
    endpoint = .httr2_redact(response$url %||% response$request$url)
  )
}

function_validate_logical <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort(paste0(name, " must be TRUE or FALSE"))
  }
  invisible(TRUE)
}

function_validate_positive_number <- function(value, name) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value <= 0
  ) {
    .fabric_abort(paste0(name, " must be one positive number"))
  }
  invisible(TRUE)
}

function_validate_byte_limit <- function(value, name) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 1 ||
      value != floor(value)
  ) {
    .fabric_abort(paste0(name, " must be one positive whole number"))
  }
  invisible(TRUE)
}

function_required_string <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(trimws(value))
  ) {
    .fabric_abort(paste0(name, " must be one non-empty string"))
  }
  trimws(value)
}
