#' Create a Microsoft Fabric Livy session
#'
#' Starts Spark compute that can run several statements while keeping variables
#' and Spark state between calls. Use [fabric_livy_query()] instead for a single,
#' self-contained operation
#'
#' @param livy_url A copied session or batch connection URL, Livy API base URL,
#'   or enriched Lakehouse object from [fabric_lakehouses()] or [fabric_item()]
#'   Copy the session-job URL from **Lakehouse settings > Livy endpoint**, or
#'   use a discovered object to avoid handling IDs manually
#' @param high_concurrency Whether to let Fabric share Spark compute between
#'   several isolated workloads. Keep `FALSE` for a typical sequence of calls in
#'   one R process
#' @param session_tag Optional high-concurrency packing hint. Related requests
#'   with the same tag may share an underlying Livy session while keeping
#'   separate REPL state. Each call still returns a distinct HC session
#' @param name Optional readable session name shown in service metadata
#' @param tags Optional named list of string labels for monitoring
#' @param conf Optional named list of Spark settings. Prefer a published Fabric
#'   Environment for configuration shared by several jobs
#' @param environment_id Optional GUID of a published Fabric Environment whose
#'   libraries and Spark settings should be used
#' @param archives Optional character vector of archive URIs made available to
#'   Spark
#' @param driver_memory,executor_memory Optional Spark memory values such as
#'   `"4g"`. Leave `NULL` to use Fabric defaults
#' @param driver_cores,executor_cores,num_executors Optional Spark resource
#'   counts. Larger values consume more capacity; leave `NULL` unless the
#'   workload has been sized deliberately
#' @param artifact_name Optional Lakehouse/artifact label used for a
#'   high-concurrency job in the Fabric Monitoring hub
#' @param file Optional application file URI for a high-concurrency request
#' @param class_name Optional Java/Scala main class for `file`
#' @param args Optional character vector of application arguments
#' @param jars,files,py_files Optional character vectors of dependency URIs
#'   supplied to Spark
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let 'fabricQueryR' use its normal sign-in flow for a Microsoft Fabric
#'   host. A custom `livy_url` requires an explicitly supplied token or provider.
#'   HTTPS validation does not prove ownership or token audience; use a custom
#'   host only when your organization controls it, with a credential issued for
#'   its intended audience
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param audience Optional sign-in scopes. For delegated sign-in, `NULL`
#'   requests the four required Livy scopes listed below. An explicit vector
#'   replaces those defaults, so include every required scope plus any optional
#'   `Code.Access*` scope the Spark code needs. Client credentials require one
#'   `.default` audience
#' @param verbose Logical. Show session lifecycle messages
#'
#' @return A newly created [FabricLivySession]. It may still be starting; call
#'   `$wait()` before `$submit()`/`$run()`, and `$close()` when finished. These
#'   handle lifecycle methods do not have separate free-function wrappers
#' @section Choosing a session type:
#' Use a standard session for a typical sequence in one R process. High
#' concurrency is for applications that run several independent Spark workloads
#' at the same time; it is not needed for several sequential statements
#'
#' @section Cleanup and permissions:
#' No network request is made when an open object is garbage collected. Call
#' `$close()` explicitly, and use `on.exit(session$close())` inside functions
#' Delegated sign-in requires `Lakehouse.Execute.All`, `Lakehouse.Read.All`,
#' `Code.AccessFabric.All`, and `Code.AccessStorage.All`. Add
#' `Code.AccessAzureKeyvault.All`, `Code.AccessAzureDataLake.All`,
#' `Code.AccessAzureDataExplorer.All`, or `Code.AccessSQL.All` only when Spark
#' accesses that Azure service at runtime. The signed-in identity also needs an
#' appropriate workspace role
#'
#' @section Timeouts:
#' A `fabric_livy_timeout_error` contains the exact session or statement object
#' in its `handle` field, so it can be polled or cancelled in the current R
#' process. The kind-specific `session` or `statement` field contains safe,
#' serializable metadata; a serialized handle intentionally loses its
#' in-process credential
#'
#' @seealso
#' [Microsoft session jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session),
#' [high-concurrency Livy](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy),
#' and the [Apache Livy REST API](https://livy.apache.org/docs/latest/rest-api.html)
#'
#' @examples
#' \dontrun{
#' # Discover the Lakehouse whose Livy endpoint will host the Spark session
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#'
#' run_shared_state <- function(lakehouse) {
#'   # Keep one session alive so successive statements share Spark state
#'   session <- fabric_livy_session(lakehouse)
#'   on.exit(session$close(), add = TRUE)
#'   session$wait()
#'   session$run("shared_value = 40", kind = "pyspark")
#'   session$run("print(shared_value + 2)", kind = "pyspark")
#' }
#' run_shared_state(lakehouse)
#'
#' run_high_concurrency <- function(lakehouse) {
#'   # A session tag lets compatible callers reuse high-concurrency compute
#'   session <- fabric_livy_session(
#'     lakehouse,
#'     high_concurrency = TRUE,
#'     session_tag = "report-workers"
#'   )
#'   on.exit(session$close(), add = TRUE)
#'   session$wait()
#'   session$run("SELECT current_timestamp()", kind = "sql")
#' }
#' run_high_concurrency(lakehouse)
#' }
#'
#' @export
fabric_livy_session <- function(
  livy_url,
  high_concurrency = FALSE,
  session_tag = NULL,
  name = NULL,
  tags = NULL,
  conf = NULL,
  environment_id = NULL,
  archives = NULL,
  driver_memory = NULL,
  driver_cores = NULL,
  executor_memory = NULL,
  executor_cores = NULL,
  num_executors = NULL,
  artifact_name = NULL,
  file = NULL,
  class_name = NULL,
  args = NULL,
  jars = NULL,
  files = NULL,
  py_files = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  verbose = TRUE
) {
  # 1 Validate session options ---------------------------------------------------------------------

  # High-concurrency-only fields are checked together so the error explains the
  # whole unsupported group instead of failing later in Fabric

  fabric_livy_check_flag(high_concurrency, "high_concurrency")
  fabric_livy_validate_session_fields(
    name = name,
    archives = archives,
    driver_memory = driver_memory,
    driver_cores = driver_cores,
    executor_memory = executor_memory,
    executor_cores = executor_cores,
    num_executors = num_executors
  )

  # High-concurrency fields need their own scalar and vector checks
  fabric_livy_check_optional_string(session_tag, "session_tag")
  fabric_livy_check_optional_string(artifact_name, "artifact_name")
  fabric_livy_check_optional_string(file, "file")
  fabric_livy_check_optional_string(class_name, "class_name")
  fabric_livy_check_string_vector(args, "args", allow_empty_strings = TRUE)
  fabric_livy_check_string_vector(jars, "jars")
  fabric_livy_check_string_vector(files, "files")
  fabric_livy_check_string_vector(py_files, "py_files")

  # A session tag has no meaning outside high-concurrency mode
  if (!is.null(session_tag) && !isTRUE(high_concurrency)) {
    .fabric_abort(
      "session_tag is only available for high-concurrency sessions"
    )
  }

  # Check all remaining high-concurrency-only inputs as one group
  hc_values <- list(
    artifact_name,
    file,
    class_name,
    args,
    jars,
    files,
    py_files
  )

  if (!high_concurrency && !all(vapply(hc_values, is.null, logical(1)))) {
    .fabric_abort(paste0(
      "artifact_name, file, class_name, args, jars, files, and py_files ",
      "are only available for high-concurrency sessions"
    ))
  }

  # 2 Build the session request --------------------------------------------------------------------

  # Drop unset options so Fabric can apply its own defaults

  tags <- fabric_livy_normalize_named_list(tags, "tags")
  payload <- Filter(
    Negate(is.null),
    list(
      name = name,
      archives = archives,
      conf = fabric_livy_conf(conf, environment_id),
      tags = tags,
      driverMemory = driver_memory,
      driverCores = driver_cores,
      executorMemory = executor_memory,
      executorCores = executor_cores,
      numExecutors = num_executors,
      sessionTag = session_tag,
      artifactName = artifact_name,
      file = file,
      className = class_name,
      args = args,
      jars = jars,
      files = files,
      pyFiles = py_files
    )
  )

  # 3 Create and return the session ----------------------------------------------------------------

  # Create the session only after the request and authentication are ready

  endpoint <- fabric_livy_resolve_url(livy_url)
  fabric_require_explicit_custom_token(endpoint, token, "livy_url")
  credential <- fabric_livy_credential(
    tenant_id,
    client_id,
    token,
    auth_args,
    audience
  )
  FabricLivySession$new(
    livy_url = endpoint,
    credential = credential,
    payload = payload,
    high_concurrency = high_concurrency,
    verbose = verbose
  )
}

# Livy session and statement R6 objects ------------------------------------------------------------

#' A Microsoft Fabric Livy session
#'
#' A Livy session keeps Spark running while you submit several pieces of code
#' Create one with [fabric_livy_session()], call `$wait()` once it starts, use
#' `$run()` to execute code, and call `$close()` when finished. Most users do
#' not need to call this 'R6' class directly. These lifecycle methods do not
#' have separate free-function wrappers
#'
#' @field id Fabric session or high-concurrency acquisition ID
#' @field url Session lifecycle URL
#' @field state Latest service state
#' @field response Latest raw service response
#' @field closed Whether `$close()` completed
#' @field high_concurrency Whether this is a high-concurrency session
#' @field session_id Underlying Livy session ID for HC sessions
#' @field repl_id Isolated REPL ID for HC sessions
#' @field verbose Whether lifecycle messages are enabled
#' @format An [R6::R6Class] generator
#' @return The `FabricLivySession` 'R6' generator.
#' @examples
#' \dontrun{
#' # fabric_livy_session() creates this class for a discovered Lakehouse
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#' session <- fabric_livy_session(lakehouse)
#' inherits(session, "FabricLivySession")
#'
#' # Wait before running code, and close the Spark session when finished
#' session$wait()
#' session$run("print(1 + 1)", kind = "pyspark")
#' session$close()
#' }
#' @export
FabricLivySession <- R6::R6Class(
  classname = "FabricLivySession",
  public = list(
    id = NULL,
    url = NULL,
    state = NULL,
    response = NULL,
    closed = FALSE,
    high_concurrency = FALSE,
    session_id = NULL,
    repl_id = NULL,
    verbose = TRUE,

    #' @description Internal constructor used by [fabric_livy_session()]
    #' @param livy_url Livy API base or collection URL
    #' @param credential Internal authentication credential
    #' @param payload Session creation request body
    #' @param high_concurrency Whether to acquire an HC session
    #' @param verbose Whether to emit lifecycle messages
    #' @returns A new session object
    initialize = function(
      livy_url,
      credential,
      payload,
      high_concurrency = FALSE,
      verbose = TRUE
    ) {
      rlang::check_installed(
        c("httr2", "jsonlite"),
        reason = "to call the Fabric Livy API"
      )
      fabric_livy_check_flag(high_concurrency, "high_concurrency")
      fabric_livy_check_flag(verbose, "verbose")
      self$high_concurrency <- high_concurrency
      self$verbose <- verbose
      credential_reference <- fabric_livy_credential_reference(credential)
      private$credential_ref <- credential_reference$reference
      private$credential_key <- credential_reference$key
      type <- if (high_concurrency) {
        "highConcurrencySessions"
      } else {
        "sessions"
      }
      private$collection_url <- fabric_livy_endpoint(livy_url, type)
      inform(verbose, "Creating Fabric Livy session")
      response <- fabric_livy_json(
        "POST",
        private$collection_url,
        credential,
        payload = payload,
        idempotent = FALSE
      )
      id <- as.character(response$id %||% "")
      fabric_livy_check_string(id, "Livy session response id")
      self$id <- id
      self$url <- paste0(private$collection_url, "/", id)
      private$update(response)
      invisible(self)
    },

    #' @description Print a concise session summary
    #' @param ... Unused
    #' @returns `self`, invisibly
    print = function(...) {
      label <- if (self$high_concurrency) {
        "Fabric high-concurrency Livy session"
      } else {
        "Fabric Livy session"
      }
      fields <- list(
        id = self$id %||% "<not created>",
        state = self$state %||% "<unknown>"
      )
      if (self$high_concurrency) {
        fields[["session/repl"]] <- paste0(
          self$session_id %||% "<pending>",
          "/",
          self$repl_id %||% "<pending>"
        )
      }
      fields$closed <- self$closed
      .fabric_print(label, fields)
      invisible(self)
    },

    #' @description Return the latest session response
    #' @param refresh Whether to retrieve current state from Fabric
    #' @param deadline Internal wall-clock deadline for the status request
    #' @returns The raw session response list
    status = function(refresh = TRUE, deadline = NULL) {
      private$assert_open()
      fabric_livy_check_flag(refresh, "refresh")
      if (isTRUE(refresh)) {
        private$update(fabric_livy_json(
          "GET",
          self$url,
          fabric_livy_handle_credential(private$credential_ref),
          deadline = deadline
        ))
      }
      self$response
    },

    #' @description Wait until the session can accept statements
    #' @param timeout Maximum wait in seconds
    #' @param poll_interval Polling interval in seconds
    #' @returns `self`, invisibly
    wait = function(timeout = 600, poll_interval = 3) {
      private$assert_open()
      fabric_livy_check_number(timeout, "timeout")
      fabric_livy_check_number(poll_interval, "poll_interval")
      deadline <- Sys.time() + timeout
      progress <- .fabric_poll_progress(
        "Fabric Livy session",
        self$id,
        verbose = self$verbose
      )
      repeat {
        if (fabric_livy_remaining(deadline) <= 0) {
          fabric_livy_abort_timeout("session", self, self$response)
        }
        response <- tryCatch(
          self$status(deadline = deadline),
          fabric_http_deadline_error = function(error) {
            fabric_livy_abort_timeout("session", self, self$response)
          }
        )
        state <- fabric_livy_state(response)
        .fabric_poll_progress_update(progress, state)
        ready <- if (self$high_concurrency) {
          identical(state, "idle") &&
            nzchar(self$session_id %||% "") &&
            nzchar(self$repl_id %||% "")
        } else {
          identical(state, "idle")
        }

        if (ready) {
          .fabric_poll_progress_done(progress)
          return(invisible(self))
        }
        fabric_state <- tolower(
          response$fabricSessionStateInfo$state %||% ""
        )
        result <- tolower(response$result %||% "")
        if (
          state %in%
            .fabric_livy_session_terminal_states ||
            fabric_state %in% c("error", "cancelled", "canceled") ||
            result %in% c("failed", "cancelled", "canceled")
        ) {
          fabric_livy_abort_session(response)
        }
        fabric_livy_poll_sleep(deadline, poll_interval)
      }
    },

    #' @description Submit code without waiting for completion
    #' @param code One string of Spark code
    #' @param kind Statement language
    #' @param source_id Optional caller-defined source identifier
    #' @returns A [FabricLivyStatement]
    submit = function(
      code,
      kind = c("spark", "pyspark", "sparkr", "sql"),
      source_id = NULL
    ) {
      private$assert_open()
      fabric_livy_check_string(code, "code")
      kind <- match.arg(kind)
      if (!is.null(source_id)) {
        fabric_livy_check_string(source_id, "source_id")
      }

      if (!identical(tolower(self$state %||% ""), "idle")) {
        .fabric_abort(
          "The Livy session is not ready; call session$wait() first"
        )
      }
      endpoint <- private$statement_collection()
      response <- fabric_livy_json(
        "POST",
        endpoint,
        fabric_livy_handle_credential(private$credential_ref),
        payload = Filter(
          Negate(is.null),
          list(
            code = code,
            kind = kind,
            sourceId = source_id
          )
        ),
        idempotent = FALSE
      )
      FabricLivyStatement$new(
        session = self,
        response = response,
        url = paste0(endpoint, "/", response$id),
        credential = fabric_livy_handle_credential(private$credential_ref),
        verbose = self$verbose
      )
    },

    #' @description Submit code, wait, and return its parsed result
    #' @param code One string of Spark code
    #' @param kind Statement language
    #' @param source_id Optional caller-defined source identifier
    #' @param timeout Maximum wait in seconds
    #' @param poll_interval Polling interval in seconds
    #' @returns A `fabric_livy_statement_result` list
    run = function(
      code,
      kind = c("spark", "pyspark", "sparkr", "sql"),
      source_id = NULL,
      timeout = 600,
      poll_interval = 2
    ) {
      fabric_livy_check_number(timeout, "timeout")
      fabric_livy_check_number(poll_interval, "poll_interval")
      statement <- self$submit(
        code = code,
        kind = match.arg(kind),
        source_id = source_id
      )
      statement$wait(
        timeout = timeout,
        poll_interval = poll_interval
      )
      statement$result(refresh = FALSE)
    },

    #' @description List every statement in this execution context
    #' @param page_size Maximum statements requested per Livy page
    #' @returns The raw Livy statements response
    statements = function(page_size = 100L) {
      private$assert_open()
      if (
        !is.numeric(page_size) ||
          length(page_size) != 1L ||
          is.na(page_size) ||
          !is.finite(page_size) ||
          page_size < 1 ||
          page_size > .Machine$integer.max ||
          page_size != floor(page_size)
      ) {
        .fabric_abort("page_size must be one positive whole number")
      }
      page_size <- as.integer(page_size)
      credential <- fabric_livy_handle_credential(private$credential_ref)
      offset <- 0L
      total <- NULL
      statements <- list()
      first_response <- NULL
      seen_ids <- character()
      repeat {
        request <- httr2::req_url_query(
          httr2::request(private$statement_collection()),
          from = offset,
          size = page_size
        )
        response <- fabric_livy_json(
          "GET",
          request$url,
          credential
        )
        page <- fabric_livy_statement_page(response, offset, total)
        first_response <- first_response %||% response
        total <- page$total
        page_ids <- vapply(
          page$statements,
          function(statement) {
            id <- statement$id
            if (
              !(is.character(id) || is.numeric(id)) ||
                length(id) != 1L ||
                is.na(id) ||
                !nzchar(as.character(id))
            ) {
              .fabric_abort(
                "Livy returned a statement without one valid id",
                class = "fabric_livy_protocol_error"
              )
            }
            as.character(id)
          },
          character(1)
        )
        if (any(page_ids %in% seen_ids) || anyDuplicated(page_ids)) {
          .fabric_abort(
            "Livy repeated a statement across collection pages",
            class = "fabric_livy_protocol_error"
          )
        }
        seen_ids <- c(seen_ids, page_ids)
        statements <- c(statements, page$statements)
        if (length(statements) == total) {
          break
        }
        if (!length(page$statements)) {
          .fabric_abort(
            "Livy returned an empty statement page before the reported total",
            class = "fabric_livy_protocol_error"
          )
        }
        offset <- length(statements)
      }
      first_response$statements <- statements
      first_response$total_statements <- as.integer(total)
      first_response$from <- 0L
      first_response
    },

    #' @description Reset a regular session's inactivity timeout
    #' @returns `self`, invisibly
    reset_timeout = function() {
      private$assert_open()
      if (self$high_concurrency) {
        .fabric_abort(
          "reset_timeout() is not supported for high-concurrency sessions"
        )
      }
      fabric_livy_ok(
        "POST",
        paste0(self$url, "/reset-timeout"),
        fabric_livy_handle_credential(private$credential_ref),
        idempotent = FALSE
      )
      invisible(self)
    },

    #' @description Release this session or high-concurrency context
    #' @param deadline Internal wall-clock deadline for the cleanup request
    #' @returns `TRUE` when closed or `FALSE` when already closed, invisibly
    close = function(deadline = NULL) {
      if (isTRUE(self$closed)) {
        return(invisible(FALSE))
      }
      inform(self$verbose, "Closing Fabric Livy session")
      fabric_livy_ok(
        "DELETE",
        self$url,
        fabric_livy_handle_credential(private$credential_ref),
        idempotent = TRUE,
        accepted_status = 404L,
        deadline = deadline
      )
      self$closed <- TRUE
      inform(self$verbose, "Fabric Livy session closed", type = "success")
      invisible(TRUE)
    }
  ),
  private = list(
    credential_ref = NULL,
    credential_key = NULL,
    collection_url = NULL,

    # Check that the session is still usable before a public method sends a
    # request. It takes no input and either returns quietly or raises an error
    assert_open = function() {
      if (isTRUE(self$closed)) {
        .fabric_abort("The Livy session is closed")
      }
    },

    # Copy fields from one Livy response onto the session and return that
    # response invisibly. Status-changing public methods use this after a call
    update = function(response) {
      self$response <- response
      self$state <- response$state %||% self$state
      self$session_id <- response$sessionId %||% self$session_id
      self$repl_id <- response$replId %||% self$repl_id
      invisible(response)
    },

    # Build and return the statement collection URL for this session. Submit
    # and statement-list methods use it for normal and shared sessions
    statement_collection = function() {
      if (!self$high_concurrency) {
        return(paste0(self$url, "/statements"))
      }

      if (
        !nzchar(self$session_id %||% "") ||
          !nzchar(self$repl_id %||% "")
      ) {
        .fabric_abort(paste0(
          "The high-concurrency session has no sessionId/replId yet; ",
          "call session$wait()"
        ))
      }
      paste0(
        private$collection_url,
        "/",
        self$session_id,
        "/repls/",
        self$repl_id,
        "/statements"
      )
    }
  ),
  cloneable = FALSE
)

fabric_livy_statement_page <- function(response, offset, previous_total) {
  if (!is.list(response) || is.null(names(response))) {
    .fabric_abort(
      "Livy returned a malformed statement collection",
      class = "fabric_livy_protocol_error"
    )
  }
  statements <- response$statements
  if (
    is.null(statements) ||
      !is.list(statements) ||
      !is.null(names(statements)) ||
      !all(vapply(
        statements,
        function(statement) {
          is.list(statement) && !is.null(names(statement))
        },
        logical(1)
      ))
  ) {
    .fabric_abort(
      "Livy returned a malformed statement collection",
      class = "fabric_livy_protocol_error"
    )
  }
  total <- response$total_statements
  if (
    !is.numeric(total) ||
      length(total) != 1L ||
      is.na(total) ||
      !is.finite(total) ||
      total < 0 ||
      total > .Machine$integer.max ||
      total != floor(total) ||
      offset + length(statements) > total ||
      (!is.null(previous_total) && total != previous_total)
  ) {
    .fabric_abort(
      "Livy returned an invalid or inconsistent statement total",
      class = "fabric_livy_protocol_error"
    )
  }
  response_offset <- response$from
  if (
    !is.null(response_offset) &&
      (!is.numeric(response_offset) ||
        length(response_offset) != 1L ||
        is.na(response_offset) ||
        !is.finite(response_offset) ||
        response_offset != floor(response_offset) ||
        response_offset != offset)
  ) {
    .fabric_abort(
      "Livy returned a statement page from an unexpected offset",
      class = "fabric_livy_protocol_error"
    )
  }
  list(statements = statements, total = as.integer(total))
}

#' A statement submitted to a Fabric Livy session
#'
#' Represents one piece of code submitted to a [FabricLivySession]. Call
#' `$wait()` and then `$result()` to retrieve its output. For the usual
#' submit-and-wait workflow, use the session's `$run()` method instead. These
#' lifecycle methods do not have separate free-function wrappers
#'
#' @field id Numeric Livy statement ID
#' @field url Statement lifecycle URL
#' @field state Latest statement state
#' @field response Latest raw service response
#' @field started_local Local submission timestamp
#' @field completed_local Local completion timestamp
#' @field verbose Whether lifecycle messages are enabled
#' @format An [R6::R6Class] generator
#' @return The `FabricLivyStatement` 'R6' generator.
#' @examples
#' \dontrun{
#' # Statements are returned by a session; users do not construct them directly
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#' session <- fabric_livy_session(lakehouse)
#' session$wait()
#'
#' # Submit code, wait for it, and inspect its result
#' statement <- session$submit("print(40 + 2)", kind = "pyspark")
#' inherits(statement, "FabricLivyStatement")
#' statement$wait()
#' statement$result()
#' session$close()
#' }
#' @export
FabricLivyStatement <- R6::R6Class(
  classname = "FabricLivyStatement",
  public = list(
    id = NULL,
    url = NULL,
    state = NULL,
    response = NULL,
    started_local = NULL,
    completed_local = NULL,
    verbose = TRUE,

    #' @description Internal constructor used by
    #' `FabricLivySession$submit()`
    #' @param session Parent [FabricLivySession]
    #' @param response Initial statement response
    #' @param url Statement lifecycle URL
    #' @param credential Internal authentication credential
    #' @param verbose Whether to emit lifecycle messages
    #' @returns A new statement object
    initialize = function(session, response, url, credential, verbose = TRUE) {
      id <- as.character(response$id %||% "")
      fabric_livy_check_string(id, "Livy statement response id")
      self$id <- response$id
      self$url <- url
      self$state <- response$state
      self$response <- response
      self$started_local <- Sys.time()
      self$verbose <- verbose
      private$session <- session
      credential_reference <- fabric_livy_credential_reference(credential)
      private$credential_ref <- credential_reference$reference
      private$credential_key <- credential_reference$key
      invisible(self)
    },

    #' @description Print a concise statement summary
    #' @param ... Unused
    #' @returns `self`, invisibly
    print = function(...) {
      .fabric_print(
        "Fabric Livy statement",
        list(
          id = self$id,
          state = self$state %||% "<unknown>",
          url = self$url
        )
      )
      invisible(self)
    },

    #' @description Retrieve statement state and available output
    #' @param refresh Whether to retrieve current state from Fabric
    #' @param deadline Internal wall-clock deadline for the status request
    #' @returns The raw statement response list
    status = function(refresh = TRUE, deadline = NULL) {
      fabric_livy_check_flag(refresh, "refresh")
      if (isTRUE(refresh)) {
        self$response <- fabric_livy_json(
          "GET",
          self$url,
          fabric_livy_handle_credential(private$credential_ref),
          deadline = deadline
        )
        self$state <- self$response$state %||% self$state
      }
      self$response
    },

    #' @description Wait for the statement to reach a terminal state
    #' @param timeout Maximum wait in seconds
    #' @param poll_interval Polling interval in seconds
    #' @param error_on_failure Raise a structured error for failed statements
    #' @returns `self`, invisibly
    wait = function(
      timeout = 600,
      poll_interval = 2,
      error_on_failure = TRUE
    ) {
      fabric_livy_check_number(timeout, "timeout")
      fabric_livy_check_number(poll_interval, "poll_interval")
      fabric_livy_check_flag(error_on_failure, "error_on_failure")
      deadline <- Sys.time() + timeout
      progress <- .fabric_poll_progress(
        "Fabric Livy statement",
        self$id,
        verbose = self$verbose
      )
      repeat {
        if (fabric_livy_remaining(deadline) <= 0) {
          fabric_livy_abort_timeout("statement", self, self$response)
        }
        response <- tryCatch(
          self$status(deadline = deadline),
          fabric_http_deadline_error = function(error) {
            fabric_livy_abort_timeout("statement", self, self$response)
          }
        )
        state <- fabric_livy_state(response)
        .fabric_poll_progress_update(progress, state)
        output_error <- identical(
          tolower(response$output$status %||% ""),
          "error"
        )

        if (
          identical(state, "available") ||
            state %in% .fabric_livy_statement_failure_states
        ) {
          self$completed_local <- self$completed_local %||% Sys.time()
          if (
            isTRUE(error_on_failure) &&
              (state %in% .fabric_livy_statement_failure_states || output_error)
          ) {
            fabric_livy_abort_statement(response)
          }

          .fabric_poll_progress_done(progress)
          return(invisible(self))
        }
        fabric_livy_poll_sleep(deadline, poll_interval)
      }
    },

    #' @description Return parsed output and timing metadata
    #' @param refresh Whether to retrieve current state from Fabric
    #' @param error_on_failure Raise a structured error for failed statements
    #' @returns A `fabric_livy_statement_result` list
    result = function(refresh = TRUE, error_on_failure = TRUE) {
      fabric_livy_check_flag(error_on_failure, "error_on_failure")
      response <- self$status(refresh = refresh)
      state <- fabric_livy_state(response)
      if (
        !identical(state, "available") &&
          !state %in% .fabric_livy_statement_failure_states
      ) {
        .fabric_abort(
          "The Livy statement is not complete; call statement$wait()"
        )
      }
      output_error <- identical(
        tolower(response$output$status %||% ""),
        "error"
      )

      if (
        isTRUE(error_on_failure) &&
          (state %in% .fabric_livy_statement_failure_states || output_error)
      ) {
        fabric_livy_abort_statement(response)
      }
      self$completed_local <- self$completed_local %||% Sys.time()
      invisible(fabric_livy_output(
        response,
        started_local = self$started_local,
        completed_local = self$completed_local,
        url = self$url
      ))
    },

    #' @description Request cancellation of this statement
    #' @returns The raw cancellation response, invisibly
    cancel = function() {
      response <- fabric_livy_json(
        "POST",
        paste0(self$url, "/cancel"),
        fabric_livy_handle_credential(private$credential_ref),
        idempotent = FALSE
      )
      invisible(response)
    }
  ),
  private = list(
    session = NULL,
    credential_ref = NULL,
    credential_key = NULL
  ),
  cloneable = FALSE
)
