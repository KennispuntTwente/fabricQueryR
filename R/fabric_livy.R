#' Run Spark code in a temporary Microsoft Fabric Livy session
#'
#' Starts Spark, runs one piece of code, returns its output, and closes the Spark
#' session. This is the simplest Livy helper for a one-off operation. For quick
#' reads from a Lakehouse or Warehouse, SQL is often faster to start
#'
#' @param livy_url A Livy connection URL copied from the Lakehouse settings, or
#'   an enriched Lakehouse object from [fabric_lakehouses()] or [fabric_item()]
#'   A discovered object avoids copying workspace and Lakehouse IDs
#' @param code One string containing the Spark code to run. Objects created in
#'   this temporary session are lost after the function returns, although
#'   writes made to Lakehouse storage persist
#' @param kind Statement language. Use `"pyspark"` for Python with Spark,
#'   `"spark"` for Scala, `"sql"` for Spark SQL, or `"sparkr"` for SparkR. This
#'   must match the syntax in `code`. The `sparklyr` package is an R API, not a
#'   separate Livy language: code that initializes `sparklyr` still uses
#'   `"sparkr"`. SparkR is deprecated upstream in Spark 4.x; see **R on Runtime
#'   2.0** below for the distinction
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let 'fabricQueryR' use its normal sign-in flow. HTTPS validation does
#'   not prove ownership or token audience for a custom host; use one only when
#'   your organization controls it, with a token or provider issued for its
#'   intended audience
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param audience Optional sign-in scopes. For delegated sign-in, `NULL`
#'   requests the four required Livy scopes listed below. An explicit vector
#'   replaces those defaults, so include every required scope plus any optional
#'   `Code.Access*` scope the Spark code needs. Client credentials require one
#'   `.default` audience
#' @param environment_id Optional GUID of a published Fabric Environment whose
#'   libraries and Spark settings should be used. Leave `NULL` to use the
#'   Lakehouse/workspace defaults
#' @param conf Optional named list of Spark configuration overrides, for example
#'   `list("spark.sql.shuffle.partitions" = "100")`. Most users can leave this
#'   `NULL` and configure shared settings in a Fabric Environment
#' @param verbose Logical. Show session startup, execution, and cleanup progress
#' @param poll_interval Seconds between status checks. Lower values update
#'   sooner but make more API calls
#' @param timeout Maximum seconds to wait for session readiness and, separately,
#'   statement completion
#' @param ... Compatibility arguments. The former named `access_token`
#'   argument is accepted here as a deprecated alias for `token`; all other
#'   arguments are rejected
#'
#' @return Invisibly, a `fabric_livy_statement_result` list. The most useful
#'   component is `output$parsed`: a tibble for tabular output, an R object for
#'   JSON, or a character vector for text. The result also keeps status, timing,
#'   submitted code, errors, and the original response. A successful statement
#'   is still returned when session cleanup fails, with a
#'   `fabric_livy_cleanup_warning` identifying the retained session. When both
#'   execution and cleanup fail, a `fabric_livy_execution_cleanup_error` retains
#'   the execution error and safe cleanup diagnostics
#' @section Before you run code:
#' Fabric needs a workspace on supported capacity, a Lakehouse, and the tenant
#' admin setting for the Livy API enabled. In the Fabric portal, open the
#' Lakehouse settings, find **Livy endpoint**, and copy the session-job
#' connection string. For several statements that reuse variables and Spark
#' state, use [fabric_livy_session()]. To run a complete Python, Scala/Java, or
#' R application file, use [fabric_livy_batch_submit()]
#'
#' A delegated caller needs the `Lakehouse.Execute.All`, `Lakehouse.Read.All`,
#' `Code.AccessFabric.All`, and `Code.AccessStorage.All` scopes and must be a
#' Contributor in the workspace. A service principal must also be added to the
#' workspace as a Contributor. Add `Code.AccessAzureKeyvault.All`,
#' `Code.AccessAzureDataLake.All`, `Code.AccessAzureDataExplorer.All`, or
#' `Code.AccessSQL.All` only when the Spark code accesses that Azure service at
#' runtime
#'
#' Spark long and decimal columns are returned as character values when needed
#' to preserve them exactly. Dates and timestamps with a time zone use R
#' temporal classes; timestamps without a time zone remain wall-clock text
#' Fabric's SQL JSON output represents non-finite floating-point values as
#' `null`, so those values are returned as typed missing values. Binary and
#' nested values use list-columns
#'
#' @section R on Runtime 2.0:
#' Microsoft Fabric supports `sparklyr` for R-first workloads and distributes
#' it with Fabric runtimes. In Fabric's documented connection,
#' `sparklyr::spark_connect(method = "synapse")` attaches to the existing Spark
#' session through the current SparkR JVM bridge. Therefore, `sparklyr` lets R
#' code move away from the SparkR DataFrame API, but it does not yet remove the
#' runtime dependency on the `"sparkr"` Livy interpreter. Use `"sparkr"` when
#' submitting R code that initializes `sparklyr`; prefer PySpark or Spark SQL
#' only when the remote Livy workload must be independent of that bridge
#'
#' @seealso
#' [Microsoft Fabric Livy API overview](https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview),
#' [Livy API setup and authorization](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy),
#' [Use sparklyr in Fabric](https://learn.microsoft.com/en-us/fabric/data-science/r-use-sparklyr),
#' and [Fabric Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)
#'
#' @export
#' @example inst/examples/fabric_livy_query.R
fabric_livy_query <- function(
  livy_url,
  code,
  kind = c("spark", "pyspark", "sparkr", "sql"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  environment_id = NULL,
  conf = NULL,
  verbose = TRUE,
  poll_interval = 2,
  timeout = 600,
  ...
) {
  # 1 Validate inputs ------------------------------------------------------------------------------

  # Check the request before starting a Spark session, which can take time

  kind <- match.arg(kind)
  fabric_livy_check_flag(verbose, "verbose")
  fabric_livy_check_number(poll_interval, "poll_interval")
  fabric_livy_check_number(timeout, "timeout")
  fabric_livy_resolve_url(livy_url)
  resolved <- fabric_resolve_token_alias(
    token = token,
    dots = list(...),
    caller = "fabric_livy_query()"
  )
  token <- resolved$token
  if (length(resolved$dots)) {
    .fabric_abort("fabric_livy_query() received unused arguments in ...")
  }

  # 2 Start a temporary session --------------------------------------------------------------------

  # Always register cleanup immediately so errors during startup or execution
  # do not leave an avoidable Spark session running

  session <- fabric_livy_session(
    livy_url = livy_url,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    audience = audience,
    environment_id = environment_id,
    conf = conf,
    verbose = verbose
  )
  cleanup_pending <- TRUE
  cleanup <- function() {
    if (!cleanup_pending) {
      return(NULL)
    }
    cleanup_pending <<- FALSE
    tryCatch(
      {
        cleanup_timeout <- getOption("fabricqueryr.livy.cleanup_timeout", 30)
        fabric_livy_check_number(cleanup_timeout, "Livy cleanup timeout")
        session$close(deadline = Sys.time() + cleanup_timeout)
        NULL
      },
      error = .fabric_livy_cleanup_condition
    )
  }
  on.exit(
    {
      cleanup_error <- cleanup()
      if (!is.null(cleanup_error)) {
        .fabric_warn(
          paste0(
            "The temporary Livy session could not be closed while leaving ",
            "fabric_livy_query(); Fabric may retain compute until idle timeout"
          ),
          class = "fabric_livy_cleanup_warning",
          parent = cleanup_error
        )
      }
    },
    add = TRUE
  )

  # 3 Run the statement ----------------------------------------------------------------------------

  # Run the statement only after its target and options are ready

  execution <- tryCatch(
    {
      session$wait(
        poll_interval = poll_interval,
        timeout = timeout
      )
      list(
        result = session$run(
          code = code,
          kind = kind,
          poll_interval = poll_interval,
          timeout = timeout
        ),
        error = NULL
      )
    },
    error = function(error) list(result = NULL, error = error)
  )
  cleanup_error <- cleanup()
  if (!is.null(execution$error)) {
    if (!is.null(cleanup_error)) {
      .fabric_abort(
        paste0(
          "The Livy statement failed and its temporary session could not be ",
          "closed; Fabric may retain compute until idle timeout"
        ),
        class = "fabric_livy_execution_cleanup_error",
        parent = execution$error,
        cleanup_error = cleanup_error,
        session_url = .httr2_redact(session$url %||% "")
      )
    }
    .fabric_rethrow(execution$error)
  }
  if (!is.null(cleanup_error)) {
    .fabric_warn(
      paste0(
        "The Livy statement succeeded, but its temporary session could not ",
        "be closed; Fabric may retain compute until idle timeout"
      ),
      class = "fabric_livy_cleanup_warning",
      parent = cleanup_error
    )
  }
  invisible(execution$result)
}

# Convert arbitrary cleanup failures into a condition that cannot retain the
# authenticated DELETE request while preserving useful diagnostic text
.fabric_livy_cleanup_condition <- function(error) {
  structure(
    list(
      message = .httr2_redact(conditionMessage(error)),
      call = NULL,
      original_class = class(error)
    ),
    class = c("fabric_livy_cleanup_error", "error", "condition")
  )
}

# Convert cancellation failures into diagnostics that cannot retain an
# authenticated request, response, or callback environment.
.fabric_livy_cancellation_condition <- function(error) {
  if (is.null(error)) {
    return(NULL)
  }
  structure(
    list(
      message = .httr2_redact(conditionMessage(error)),
      call = NULL,
      original_class = class(error)
    ),
    class = c("fabric_livy_cancellation_error", "error", "condition")
  )
}

# Shared Fabric Livy helpers -----------------------------------------------------------------------

.fabric_livy_session_terminal_states <- c(
  "dead",
  "error",
  "failed",
  "killed",
  "shutting_down",
  "cancelled",
  "success",
  "deleting"
)

.fabric_livy_statement_failure_states <- c(
  "error",
  "cancelled"
)

.fabric_livy_batch_success_states <- c("success")

.fabric_livy_batch_failure_states <- c(
  "dead",
  "error",
  "failed",
  "killed",
  "cancelled"
)

# Resolve a Lakehouse record or copied `livy_url`. Returns a validated session
# collection URL used by every public Livy entry point
fabric_livy_resolve_url <- function(livy_url) {
  discovered <- fabric_as_record(livy_url)
  if (!is.null(discovered)) {
    if (
      !identical(
        tolower(fabric_record_value(discovered, "type") %||% ""),
        "lakehouse"
      )
    ) {
      .fabric_abort(
        "livy_url discovery record must be a Lakehouse item"
      )
    }
    livy_url <- fabric_record_value(discovered, "livy_url")
  }
  fabric_livy_validate_endpoint(livy_url)
}

# Validate `livy_url`. Returns a normalized HTTPS URL.
fabric_livy_validate_endpoint <- function(url) {
  fabric_livy_check_string(url, "livy_url")
  value <- sub("/+$", "", trimws(url))
  parsed <- try(httr2::url_parse(value), silent = TRUE)
  if (
    inherits(parsed, "try-error") ||
      !identical(tolower(parsed$scheme %||% ""), "https") ||
      !nzchar(parsed$hostname %||% "")
  ) {
    .fabric_abort("livy_url must be a valid HTTPS endpoint")
  }
  host <- tolower(parsed$hostname)
  fabric_host <- grepl("(^|\\.)api\\.fabric\\.microsoft\\.com$", host)
  if (
    nzchar(parsed$username %||% "") ||
      nzchar(parsed$password %||% "") ||
      length(parsed$query %||% list()) > 0L ||
      nzchar(parsed$fragment %||% "")
  ) {
    .fabric_abort(
      "livy_url must not contain userinfo, a query string, or a fragment"
    )
  }

  if (
    fabric_host &&
      !is.null(parsed$port) &&
      as.character(parsed$port) != "443"
  ) {
    .fabric_abort(
      "Microsoft Fabric livy_url may use only the HTTPS default port"
    )
  }

  value
}

# Check `value` as one non-empty string, optionally allowing `NULL`. Returns
# invisibly for shared Livy argument validation
fabric_livy_check_string <- function(value, name, allow_null = FALSE) {
  if (is.null(value) && isTRUE(allow_null)) {
    return(invisible(value))
  }

  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(trimws(value))
  ) {
    .fabric_abort(cli::format_inline("{name} must be one non-empty string"))
  }
  invisible(value)
}

# Check a Fabric Livy batch application path as an absolute ABFS(S) URI. The
# authority's userinfo is the filesystem/container name, not a bearer
# credential, so passwords and other authority decorations are never valid
# here.
fabric_livy_validate_abfs_uri <- function(value, name) {
  fabric_livy_check_string(value, name)

  parsed <- tryCatch(
    httr2::url_parse(value),
    error = function(...) NULL
  )
  raw_path <- sub(
    "^[A-Za-z][A-Za-z0-9+.-]*://[^/]*",
    "",
    value,
    perl = TRUE
  )
  raw_path <- sub("[?#].*$", "", raw_path, perl = TRUE)
  decoded_path <- tryCatch(
    utils::URLdecode(raw_path),
    warning = function(...) NA_character_,
    error = function(...) NA_character_
  )
  decoded_path_without_ascii_spaces <- gsub(
    " ",
    "",
    decoded_path,
    fixed = TRUE
  )

  unsafe_text <- function(x) {
    is.null(x) ||
      length(x) != 1L ||
      is.na(x) ||
      grepl("[\\p{Z}\\p{C}\\\\]", x, perl = TRUE)
  }
  segments <- if (is.na(decoded_path)) {
    character()
  } else {
    strsplit(decoded_path, "/", fixed = TRUE)[[1L]]
  }

  valid <- !is.null(parsed) &&
    parsed$scheme %in% c("abfs", "abfss") &&
    !unsafe_text(parsed$username) &&
    nzchar(parsed$username) &&
    !unsafe_text(parsed$hostname) &&
    nzchar(parsed$hostname) &&
    is.null(parsed$password) &&
    is.null(parsed$port) &&
    is.null(parsed$query) &&
    is.null(parsed$fragment) &&
    !unsafe_text(raw_path) &&
    !unsafe_text(decoded_path_without_ascii_spaces) &&
    startsWith(raw_path, "/") &&
    nzchar(sub("^/+", "", raw_path)) &&
    !any(segments %in% c(".", "..")) &&
    !grepl("%(?![0-9A-Fa-f]{2})", value, perl = TRUE) &&
    !grepl("(?i)%(?:0[0-9a-f]|1[0-9a-f]|5c|7f)", value, perl = TRUE)

  if (!isTRUE(valid)) {
    .fabric_abort(
      paste0(
        name,
        " must be an absolute ABFS or ABFSS URI with a filesystem, host, ",
        "and non-root path, without credentials, a port, query, fragment, ",
        "unsafe whitespace, backslashes, or dot segments"
      ),
      class = c("fabric_livy_abfs_uri_error", "fabric_livy_error")
    )
  }

  invisible(value)
}

# Check an optional Fabric item identifier as a canonical GUID
fabric_livy_check_guid <- function(value, name, allow_null = TRUE) {
  if (is.null(value) && isTRUE(allow_null)) {
    return(invisible(value))
  }
  fabric_livy_check_string(value, name)
  if (!fabric_is_guid(value)) {
    .fabric_abort(paste0(name, " must be a GUID"))
  }
  invisible(value)
}

# Check numeric `value` against `minimum`. Returns invisibly for Livy timeout,
# polling, and resource settings
fabric_livy_check_number <- function(value, name, minimum = 0) {
  if (
    length(value) != 1L ||
      is.na(value) ||
      !is.numeric(value) ||
      !is.finite(value) ||
      value < minimum
  ) {
    .fabric_abort(paste0(
      name,
      " must be one finite number greater than or equal to ",
      minimum
    ))
  }
  invisible(value)
}

# Check `value` as one whole number at least `minimum`. Returns invisibly for
# Spark core and executor counts
fabric_livy_check_integer <- function(value, name, minimum = 1L) {
  if (
    length(value) != 1L ||
      is.na(value) ||
      !is.numeric(value) ||
      !is.finite(value) ||
      value != trunc(value) ||
      value < minimum ||
      value > .Machine$integer.max
  ) {
    .fabric_abort(paste0(
      name,
      " must be one whole number between ",
      minimum,
      " and ",
      .Machine$integer.max
    ))
  }
  invisible(value)
}

# Check `value` as one non-missing logical. Returns invisibly for Livy switches
# used by public functions and R6 methods
fabric_livy_check_flag <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort(cli::format_inline("{name} must be TRUE or FALSE"))
  }
  invisible(value)
}

# Check optional `value` as one non-empty string. Returns invisibly for optional
# Livy request fields
fabric_livy_check_optional_string <- function(value, name) {
  if (!is.null(value)) {
    fabric_livy_check_string(value, name)
  }
  invisible(value)
}

# Check optional `value` as a character vector. Returns invisibly for Spark file,
# archive, argument, and library lists
fabric_livy_check_string_vector <- function(
  value,
  name,
  allow_empty_strings = FALSE
) {
  if (is.null(value)) {
    return(invisible(value))
  }
  valid <- is.character(value) && !anyNA(value)
  if (valid && !isTRUE(allow_empty_strings)) {
    valid <- all(nzchar(trimws(value)))
  }

  if (!valid) {
    .fabric_abort(cli::format_inline(
      "{name} must be a character vector without missing{if (!allow_empty_strings) ' or empty' else ''} values"
    ))
  }
  invisible(value)
}

# Validate `value` as a uniquely named list. Returns `NULL` or the same list for
# Spark configuration and tag payloads
fabric_livy_normalize_named_list <- function(value, name) {
  if (is.null(value) || !length(value)) {
    return(NULL)
  }
  valid_names <- !is.null(names(value)) &&
    !anyNA(names(value)) &&
    all(nzchar(names(value))) &&
    !anyDuplicated(names(value))
  valid_values <- is.list(value) &&
    all(vapply(
      value,
      function(x) is.character(x) && length(x) == 1L && !is.na(x),
      logical(1)
    ))
  if (!valid_names || !valid_values) {
    .fabric_abort(cli::format_inline(
      "{name} must be a uniquely named list of single, non-missing strings"
    ))
  }
  value
}

# Validate shared session and batch resource fields. Returns invisibly before a
# Livy payload is constructed
fabric_livy_validate_session_fields <- function(
  name = NULL,
  archives = NULL,
  driver_memory = NULL,
  driver_cores = NULL,
  executor_memory = NULL,
  executor_cores = NULL,
  num_executors = NULL
) {
  fabric_livy_check_optional_string(name, "name")
  fabric_livy_check_string_vector(archives, "archives")
  fabric_livy_check_optional_string(driver_memory, "driver_memory")
  fabric_livy_check_optional_string(executor_memory, "executor_memory")
  if (!is.null(driver_cores)) {
    fabric_livy_check_integer(driver_cores, "driver_cores")
  }

  if (!is.null(executor_cores)) {
    fabric_livy_check_integer(executor_cores, "executor_cores")
  }

  if (!is.null(num_executors)) {
    fabric_livy_check_integer(num_executors, "num_executors")
  }
  invisible(NULL)
}

# Combine Spark `conf` with an optional Fabric `environment_id`. Returns a named
# configuration list or `NULL` for session and batch requests
fabric_livy_conf <- function(conf = NULL, environment_id = NULL) {
  conf <- fabric_livy_normalize_named_list(conf, "conf")
  if (!is.null(environment_id)) {
    fabric_livy_check_guid(environment_id, "environment_id")
    conf <- conf %||% list()
    conf[["spark.fabric.environmentDetails"]] <- jsonlite::toJSON(
      list(id = environment_id),
      auto_unbox = TRUE
    )
  }
  conf
}

# Encode a Livy `payload` as JSON while preserving empty arrays. Returns raw JSON
# used by the shared request sender
fabric_livy_json_payload <- function(payload) {
  array_fields <- intersect(
    names(payload),
    c("args", "jars", "files", "pyFiles", "archives")
  )

  for (field in array_fields) {
    payload[[field]] <- I(payload[[field]])
  }
  payload
}

# Build a Livy credential and remember its chosen audience. Returns the internal
# credential stored by Livy session, statement, and batch objects
fabric_livy_credential <- function(
  tenant_id,
  client_id,
  token = NULL,
  auth_args = list(),
  audience = NULL
) {
  fabric_validate_auth_args(auth_args)
  audience <- fabric_livy_audience(audience, token, auth_args)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  credential$livy_audience <- audience
  credential
}

# Choose the explicit or flow-appropriate Livy token audience. Returns one or
# more scopes used when creating the internal credential
fabric_livy_audience <- function(audience, token = NULL, auth_args = list()) {
  if (is.null(audience)) {
    if (is.null(token) && fabric_uses_client_credentials(auth_args)) {
      return(.fabric_audience$power_bi)
    }

    return(.fabric_audience$livy_delegated)
  }

  if (
    !is.character(audience) ||
      !length(audience) ||
      anyNA(audience)
  ) {
    .fabric_abort(
      "audience must be a non-empty character vector without duplicates"
    )
  }
  audience <- trimws(audience)
  if (!all(nzchar(audience)) || anyDuplicated(audience)) {
    .fabric_abort(
      "audience must be a non-empty character vector without duplicates"
    )
  }

  if (
    is.null(token) &&
      fabric_uses_client_credentials(auth_args) &&
      (length(audience) != 1L ||
        !grepl("/[.]default$", audience, ignore.case = TRUE))
  ) {
    .fabric_abort(
      "Client-credentials authentication requires one .default audience"
    )
  }
  audience
}

# Send one Livy request and decode its JSON response. Returns a named list used
# by all Livy object lifecycle methods
fabric_livy_json <- function(
  method,
  url,
  credential,
  payload = NULL,
  query = NULL,
  idempotent = NULL,
  deadline = NULL
) {
  req <- httr2::request(url) |>
    httr2::req_method(method)
  if (!is.null(query)) {
    req <- do.call(httr2::req_url_query, c(list(req), query))
  }
  if (!is.null(payload)) {
    req <- httr2::req_body_json(
      req,
      fabric_livy_json_payload(payload)
    )
  } else if (toupper(method) %in% c("POST", "PUT", "PATCH")) {
    req <- httr2::req_body_raw(req, raw())
  }
  response <- .httr2_perform(
    req,
    credential = credential,
    audience = credential$livy_audience %||% .fabric_audience$fabric,
    idempotent = idempotent,
    deadline = deadline
  )
  tryCatch(
    httr2::resp_body_json(
      response,
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    ),
    error = function(error) {
      decode_error <- structure(
        list(
          message = "The Livy response could not be decoded as JSON",
          call = NULL,
          decode_class = class(error)
        ),
        class = c("fabric_livy_decode_error", "error", "condition")
      )
      .fabric_abort(
        "Livy returned a successful response with an invalid JSON body",
        class = "fabric_livy_protocol_error",
        parent = decode_error,
        response_metadata = .httr2_response_metadata(response),
        call = NULL,
        .trace = FALSE
      )
    }
  )
}

# Calculate seconds remaining until `deadline`. Returns a non-negative number
# used to keep all polling and HTTP work inside the caller's timeout
fabric_livy_remaining <- function(deadline) {
  max(0, as.numeric(difftime(deadline, Sys.time(), units = "secs")))
}

# Sleep for at most `poll_interval` within `deadline`. Returns the remaining
# seconds invisibly and is shared by session, statement, and batch polling
fabric_livy_poll_sleep <- function(
  deadline,
  poll_interval,
  .now = Sys.time,
  .sleep = Sys.sleep
) {
  remaining <- max(0, as.numeric(difftime(deadline, .now(), units = "secs")))
  if (remaining > 0 && poll_interval > 0) {
    .sleep(min(poll_interval, remaining))
  }
  invisible(remaining)
}

# Store a credential behind a weak reference so Livy handles can be serialized
# without persisting bearer tokens, callback environments, or client secrets.
fabric_livy_credential_reference <- function(credential) {
  key <- new.env(parent = emptyenv())
  list(
    reference = rlang::new_weakref(key, credential),
    key = key
  )
}

# Resolve a Livy handle's in-process credential. Direct credentials remain
# accepted for objects constructed by earlier package versions.
fabric_livy_handle_credential <- function(stored) {
  if (inherits(stored, "fabric_credential")) {
    return(stored)
  }
  credential <- if (rlang::is_weakref(stored)) {
    rlang::wref_value(stored)
  } else {
    NULL
  }
  if (is.null(credential)) {
    .fabric_abort(
      paste0(
        "This Livy handle no longer has an in-process credential; create a ",
        "new handle with `token`, `tenant_id`, or other authentication ",
        "arguments"
      ),
      class = "fabric_livy_credential_error"
    )
  }
  credential
}

# Copy only stable public identifiers from an R6 handle for error diagnostics.
fabric_livy_handle_metadata <- function(kind, handle) {
  metadata <- list(
    id = handle$id,
    url = handle$url,
    state = handle$state
  )
  if (identical(kind, "session")) {
    metadata$closed <- handle$closed
    metadata$high_concurrency <- handle$high_concurrency
    metadata$session_id <- handle$session_id
    metadata$repl_id <- handle$repl_id
  } else if (identical(kind, "batch")) {
    metadata$cancel_requested <- handle$cancel_requested
  }
  structure(
    Filter(Negate(is.null), metadata),
    class = c(paste0("fabric_livy_", kind, "_metadata"), "list")
  )
}

# Raise a typed timeout condition with the latest `response`. This function does
# not return and is shared by every Livy polling loop
fabric_livy_abort_timeout <- function(
  kind,
  handle,
  response,
  cancel_accepted = NULL,
  cancel_error = NULL
) {
  field <- switch(
    kind,
    session = "session",
    statement = "statement",
    batch = "batch"
  )
  data <- list(
    message = paste0("Timed out waiting for the Livy ", kind),
    class = "fabric_livy_timeout_error",
    kind = kind,
    handle = handle,
    last_response = .httr2_redact_object(response),
    last_state = fabric_livy_state(response),
    cancel_accepted = cancel_accepted,
    cancel_error = .fabric_livy_cancellation_condition(cancel_error)
  )
  data[[field]] <- fabric_livy_handle_metadata(kind, handle)
  data$call <- NULL
  data$.trace <- FALSE
  do.call(.fabric_abort, data)
}

# Send a Livy request that needs no response body. Returns invisibly after shared
# authentication, retry, and timeout behavior succeeds
fabric_livy_ok <- function(
  method,
  url,
  credential,
  payload = NULL,
  idempotent = NULL,
  accepted_status = integer(),
  deadline = NULL
) {
  req <- httr2::request(url) |>
    httr2::req_method(method)
  if (!is.null(payload)) {
    req <- httr2::req_body_json(
      req,
      fabric_livy_json_payload(payload)
    )
  } else if (toupper(method) %in% c("POST", "PUT", "PATCH")) {
    req <- httr2::req_body_raw(req, raw())
  }
  .httr2_perform(
    req,
    credential = credential,
    audience = credential$livy_audience %||% .fabric_audience$fabric,
    idempotent = idempotent,
    accepted_status = accepted_status,
    deadline = deadline
  )
  invisible(TRUE)
}

# Normalize a copied session or batch URL to the requested collection endpoint
# Returns a trusted URL used by the session and batch entry points
fabric_livy_endpoint <- function(
  url,
  type = c("sessions", "batches", "highConcurrencySessions")
) {
  type <- match.arg(type)
  value <- fabric_livy_validate_endpoint(url)
  collection_pattern <- paste0(
    "(?i)/(sessions|batches|highConcurrencySessions)$"
  )

  if (grepl(collection_pattern, value, perl = TRUE)) {
    sub(collection_pattern, paste0("/", type), value, perl = TRUE)
  } else {
    paste0(value, "/", type)
  }
}

# Read a normalized state from a Livy `response`. Returns lower-case text used
# by every polling and error helper
fabric_livy_state <- function(response) {
  tolower(response$state %||% "")
}

# Extract readable error details from a Livy `response`. Returns service text or
# `fallback` for the typed statement, session, and batch errors
fabric_livy_error_text <- function(response, fallback) {
  output <- response$output %||% list()
  data <- output$data %||% list()
  fabric_state <- response$fabricSessionStateInfo %||%
    response$fabricBatchStateInfo %||%
    list()
  candidates <- c(
    output$evalue,
    output$error,
    fabric_state$errorMessage,
    response$cancellationReason,
    unlist(response$errorInfo %||% list(), recursive = TRUE),
    data[["text/plain"]],
    response$log
  )
  candidates <- as.character(candidates)
  candidates <- candidates[!is.na(candidates) & nzchar(candidates)]
  if (!length(candidates)) {
    fallback
  } else {
    paste(unique(candidates), collapse = "\n")
  }
}

# Raise a typed statement error from `response`. This function does not return
# and preserves the statement output and traceback for callers
fabric_livy_abort_statement <- function(response) {
  state <- response$state %||% "unknown"
  .fabric_abort(
    fabric_livy_error_text(
      response,
      paste0("Livy statement ended with state ", state)
    ),
    class = "fabric_livy_statement_error",
    statement = response,
    output = response$output %||% list(),
    traceback = response$output$traceback %||% character()
  )
}

# Raise a typed session error from `response`. This function does not return and
# attaches the raw session response
fabric_livy_abort_session <- function(response) {
  state <- response$state %||% "unknown"
  .fabric_abort(
    fabric_livy_error_text(
      response,
      paste0("Livy session ended with state ", state)
    ),
    class = "fabric_livy_session_error",
    session = response
  )
}

# Raise a typed batch error from `response`. This function does not return and
# attaches logs and service error information
fabric_livy_abort_batch <- function(response) {
  state <- response$state %||% "unknown"
  .fabric_abort(
    fabric_livy_error_text(
      response,
      paste0("Livy batch ended with state ", state)
    ),
    class = "fabric_livy_batch_error",
    batch = response,
    logs = response$log %||% character(),
    error_info = response$errorInfo %||% list()
  )
}

# Convert a completed statement `response` into a stable result. Returns parsed
# output plus raw data, timing, and statement identity
fabric_livy_output <- function(response, started_local, completed_local, url) {
  fabric_livy_response_object(response, "statement response")
  out <- response$output %||% list()
  fabric_livy_response_object(out, "statement output")
  data <- out$data %||% list()
  fabric_livy_response_object(data, "statement output data")
  parsed <- fabric_livy_parse_output_data(data)
  structure(
    list(
      id = response$id,
      state = response$state,
      started_local = started_local,
      completed_local = completed_local,
      duration_sec = as.numeric(difftime(
        completed_local,
        started_local,
        units = "secs"
      )),
      output = list(
        status = out$status %||% NULL,
        execution_count = out$execution_count %||% NULL,
        data = data,
        parsed = parsed,
        ename = out$ename %||% NULL,
        evalue = out$evalue %||% NULL,
        traceback = out$traceback %||% character()
      ),
      code = response$code %||% NULL,
      source_id = response$sourceId %||% NULL,
      url = url,
      raw = response
    ),
    class = c("fabric_livy_statement_result", "list")
  )
}

# Detect and parse common Livy output shapes in `data`. Returns a tibble, decoded
# JSON object, character output, or `NULL`
fabric_livy_parse_output_data <- function(data) {
  fabric_livy_response_object(data, "statement output data")
  table_mime <- "application/vnd.livy.table.v1+json"
  if (!is.null(data[[table_mime]])) {
    return(fabric_livy_parse_table(data[[table_mime]]))
  }

  if (!is.null(data[["application/json"]])) {
    return(fabric_livy_parse_json(data[["application/json"]]))
  }

  if (!is.null(data[["text/plain"]])) {
    return(as.character(data[["text/plain"]]))
  }
  mime_types <- names(data) %||% character()
  json_mime <- mime_types[grepl("(?:/json|\\+json)$", mime_types)]
  if (length(json_mime)) {
    return(fabric_livy_parse_json(data[[json_mime[[1L]]]]))
  }
  text_mime <- mime_types[grepl("^text/", mime_types)]
  if (length(text_mime)) {
    return(as.character(data[[text_mime[[1L]]]]))
  }
  NULL
}

# Validate one decoded Livy JSON object. Returns invisibly or raises a stable
# protocol error before service-controlled fields are accessed
fabric_livy_response_object <- function(value, name) {
  value_names <- names(value)
  if (
    !is.list(value) ||
      (length(value) &&
        (is.null(value_names) ||
          anyNA(value_names) ||
          !all(nzchar(value_names)) ||
          anyDuplicated(value_names)))
  ) {
    .fabric_abort(
      paste0("Livy returned a malformed ", name, ": expected a JSON object"),
      class = "fabric_livy_protocol_error"
    )
  }
  invisible(value)
}

# Decode one JSON `value` when possible. Returns an R object or `NULL` so output
# parsing can try other supported formats
fabric_livy_parse_json <- function(value) {
  table <- fabric_livy_parse_sql_json(value)
  if (!is.null(table)) {
    return(table)
  }
  obj <- try(
    jsonlite::fromJSON(
      jsonlite::toJSON(value, auto_unbox = TRUE, null = "null"),
      simplifyVector = TRUE,
      bigint_as_char = TRUE
    ),
    silent = TRUE
  )

  if (inherits(obj, "try-error")) {
    return(NULL)
  }

  if (is.data.frame(obj)) tibble::as_tibble(obj) else obj
}

# Decode Spark SQL's schema-and-data JSON `value`. Returns a typed tibble or
# `NULL` when the value is not this output format
fabric_livy_parse_sql_json <- function(value) {
  if (is.character(value) && length(value) == 1L) {
    value <- try(
      jsonlite::fromJSON(
        value,
        simplifyVector = FALSE,
        bigint_as_char = TRUE
      ),
      silent = TRUE
    )
  }

  if (
    inherits(value, "try-error") ||
      !is.list(value) ||
      !is.list(value$schema) ||
      !identical(tolower(value$schema$type %||% ""), "struct") ||
      is.null(value$data)
  ) {
    return(NULL)
  }
  fields <- value$schema$fields
  if (!is.list(fields)) {
    .fabric_abort(
      "Livy returned malformed Spark SQL output: schema fields must be an array",
      class = "fabric_livy_protocol_error"
    )
  }
  headers <- lapply(fields, function(field) {
    list(
      name = if (is.list(field)) field$name else NULL,
      type = if (is.list(field)) field$type else NULL
    )
  })
  fabric_livy_parse_table(list(headers = headers, data = value$data))
}

# Convert a Livy table object into a tibble. Returns typed columns plus the Spark
# schema as an attribute
fabric_livy_parse_table <- function(value) {
  # 1 Read table schema and rows -------------------------------------------------------------------

  # Read table schema and rows once so later checks use a consistent view

  if (is.character(value) && length(value) == 1L) {
    value <- try(
      jsonlite::fromJSON(
        value,
        simplifyVector = FALSE,
        bigint_as_char = TRUE
      ),
      silent = TRUE
    )
  }
  # Raise a table protocol error with beginner-readable `detail`; never returns
  malformed <- function(detail) {
    .fabric_abort(
      paste0("Livy returned malformed table output: ", detail),
      class = "fabric_livy_protocol_error"
    )
  }

  if (inherits(value, "try-error") || !is.list(value)) {
    malformed("the MIME value must be a JSON object")
  }
  headers <- value$headers
  rows <- value$data
  if (!is.list(headers) || !is.list(rows)) {
    malformed("headers and data must be arrays")
  }
  column_names <- vapply(
    headers,
    function(header) {
      name <- if (is.list(header)) header$name else NULL
      if (
        !is.character(name) ||
          length(name) != 1L ||
          is.na(name) ||
          !nzchar(name)
      ) {
        malformed("every header needs one non-empty name")
      }
      name
    },
    character(1)
  )

  if (anyDuplicated(column_names)) {
    malformed("header names must be unique")
  }
  column_count <- length(column_names)
  valid_rows <- vapply(
    rows,
    function(row) is.list(row) && length(row) == column_count,
    logical(1)
  )

  if (!all(valid_rows)) {
    malformed("every row width must match the headers")
  }

  # 2 Convert each Spark column --------------------------------------------------------------------

  # Convert each column with its declared Spark type before building the tibble

  columns <- lapply(seq_len(column_count), function(column) {
    values <- lapply(rows, function(row) row[[column]])
    type <- if (is.list(headers[[column]])) headers[[column]]$type else NULL
    fabric_livy_convert_column(values, type)
  })

  # 3 Return the typed table -----------------------------------------------------------------------

  # Return the typed table in the stable form expected by the caller

  out <- tibble::as_tibble(stats::setNames(columns, column_names))
  attr(out, "spark_schema") <- headers
  out
}

# Normalize one Spark `type` spelling. Returns a base type name used to choose an
# R column converter
fabric_livy_spark_type <- function(type) {
  if (is.list(type)) {
    type <- type$type %||% type$name
  }

  if (!is.character(type) || length(type) != 1L || is.na(type)) {
    return(NULL)
  }
  normalized <- tolower(trimws(type))
  normalized <- sub("_type$", "", normalized)
  sub("[<(].*$", "", normalized)
}

# Convert nullable scalar `values` to character. Returns a safe intermediate
# vector for the more specific Spark type converters
fabric_livy_atomic_text <- function(values) {
  vapply(
    values,
    function(value) if (is.null(value)) NA_character_ else as.character(value),
    character(1)
  )
}

# Raise a typed protocol error naming invalid Spark `kind`. This function does
# not return and keeps conversion failures consistent
fabric_livy_invalid_type <- function(kind) {
  .fabric_abort(
    paste0("Livy returned an invalid value for declared Spark type ", kind),
    class = "fabric_livy_protocol_error"
  )
}

# Convert nullable Spark `values` using declared `type`. Returns one atomic
# vector or list-column for a parsed Livy table
fabric_livy_convert_column <- function(values, type) {
  # 1 Normalize the declared Spark type ------------------------------------------------------------

  # Normalize the declared Spark type so later branches do not repeat the same conversion

  kind <- fabric_livy_spark_type(type)
  if (is.null(kind)) {
    return(fabric_livy_simplify_column(values))
  }

  # 2 Convert scalar values ------------------------------------------------------------------------

  # Convert scalar values to stable R vectors while preserving missing values

  if (kind %in% c("string", "char", "varchar", "decimal", "bigint", "long")) {
    return(fabric_livy_atomic_text(values))
  }

  # Numeric Spark types must convert without silently losing invalid values
  if (kind %in% c("byte", "short", "integer", "int")) {
    text <- fabric_livy_atomic_text(values)
    out <- suppressWarnings(as.integer(text))
    if (any(!is.na(text) & is.na(out))) {
      fabric_livy_invalid_type(kind)
    }

    return(out)
  }

  if (kind %in% c("float", "double")) {
    text <- fabric_livy_atomic_text(values)
    out <- suppressWarnings(as.numeric(text))
    if (any(!is.na(text) & is.na(out) & !is.nan(out))) {
      fabric_livy_invalid_type(kind)
    }

    return(out)
  }

  # Logical and calendar types use strict service text formats
  if (kind %in% c("boolean", "bool")) {
    text <- tolower(fabric_livy_atomic_text(values))
    invalid <- !is.na(text) & !text %in% c("true", "false")
    if (any(invalid)) {
      fabric_livy_invalid_type(kind)
    }

    return(ifelse(is.na(text), NA, text == "true"))
  }

  if (identical(kind, "date")) {
    text <- fabric_livy_atomic_text(values)
    out <- as.Date(text, format = "%Y-%m-%d")
    if (any(!is.na(text) & is.na(out))) {
      fabric_livy_invalid_type(kind)
    }

    return(out)
  }

  # Timestamp values with no timezone remain text to preserve their meaning
  if (identical(kind, "timestamp_ntz")) {
    text <- fabric_livy_atomic_text(values)
    return(sub(
      "^([0-9]{4}-[0-9]{2}-[0-9]{2})T",
      "\\1 ",
      text
    ))
  }

  # Zoned timestamps become UTC instants after trying known Spark formats
  if (identical(kind, "timestamp")) {
    text <- fabric_livy_atomic_text(values)
    parsed <- vapply(
      text,
      function(value) {
        if (is.na(value)) {
          return(NA_real_)
        }
        normalized <- sub(
          "([+-][0-9]{2}):([0-9]{2})$",
          "\\1\\2",
          value,
          perl = TRUE
        )
        formats <- c(
          "%Y-%m-%dT%H:%M:%OSZ",
          "%Y-%m-%dT%H:%M:%OS%z",
          "%Y-%m-%d %H:%M:%OS"
        )

        for (format in formats) {
          candidate <- suppressWarnings(as.POSIXct(
            normalized,
            format = format,
            tz = "UTC"
          ))
          if (!is.na(candidate)) return(as.numeric(candidate))
        }
        NA_real_
      },
      numeric(1)
    )

    if (any(!is.na(text) & is.na(parsed))) {
      fabric_livy_invalid_type(kind)
    }

    return(as.POSIXct(parsed, origin = "1970-01-01", tz = "UTC"))
  }

  # Binary values may already be raw or may arrive as base64 text
  if (identical(kind, "binary")) {
    return(lapply(values, function(value) {
      if (is.null(value)) {
        return(NULL)
      }

      if (is.raw(value)) {
        return(value)
      }
      valid <- is.character(value) &&
        length(value) == 1L &&
        !is.na(value) &&
        grepl(
          "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
          value,
          perl = TRUE
        )
      if (!valid) {
        fabric_livy_invalid_type(kind)
      }
      jsonlite::base64_dec(value)
    }))
  }

  # Arrays, maps, structs, intervals, variants, and future Spark types retain
  # their decoded JSON values as a stable list-column
  values
}

# Simplify uniformly typed scalar `values` when safe. Returns an atomic vector or
# retains a list-column for mixed, nested, or large values
fabric_livy_simplify_column <- function(values) {
  present <- Filter(Negate(is.null), values)
  types <- vapply(present, typeof, character(1))
  supported_types <- c(
    "character",
    "integer",
    "double",
    "logical",
    "complex"
  )
  scalar_atomic <- length(present) &&
    all(vapply(
      present,
      function(value) is.atomic(value) && length(value) == 1L,
      logical(1)
    )) &&
    length(unique(types)) == 1L &&
    types[[1L]] %in% supported_types
  if (!scalar_atomic) {
    return(values)
  }
  template <- present[[1L]]
  missing <- switch(
    typeof(template),
    character = NA_character_,
    integer = NA_integer_,
    double = NA_real_,
    logical = NA,
    complex = NA_complex_,
    NA
  )
  unlist(
    lapply(values, function(value) value %||% missing),
    recursive = FALSE,
    use.names = FALSE
  )
}
