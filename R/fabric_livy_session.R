#' Create a Microsoft Fabric Livy session
#'
#' Starts Spark compute that can run several statements while keeping variables
#' and Spark state between calls. Use [fabric_livy_query()] instead for a single,
#' self-contained operation.
#'
#' @param livy_url A copied session or batch connection URL, Livy API base URL,
#'   or enriched Lakehouse record from [fabric_lakehouses()] or [fabric_item()].
#'   Copy the session-job URL from **Lakehouse settings > Livy endpoint**, or
#'   use a discovered record to avoid handling IDs manually.
#' @param high_concurrency Whether to let Fabric share Spark compute between
#'   several isolated workloads. Keep `FALSE` for a typical sequence of calls in
#'   one R process. This Fabric capability is currently in preview.
#' @param session_tag Optional high-concurrency packing hint. Related requests
#'   with the same tag may share an underlying Livy session while keeping
#'   separate REPL state. Each call still returns a distinct HC session.
#' @param name Optional readable session name shown in service metadata.
#' @param tags Optional named list of string labels for monitoring.
#' @param conf Optional named list of Spark settings. Prefer a published Fabric
#'   Environment for configuration shared by several jobs.
#' @param environment_id Optional GUID of a published Fabric Environment whose
#'   libraries and Spark settings should be used.
#' @param archives Optional character vector of archive URIs made available to
#'   Spark.
#' @param driver_memory,executor_memory Optional Spark memory values such as
#'   `"4g"`. Leave `NULL` to use Fabric defaults.
#' @param driver_cores,executor_cores,num_executors Optional Spark resource
#'   counts. Larger values consume more capacity; leave `NULL` unless the
#'   workload has been sized deliberately.
#' @param artifact_name Optional Lakehouse/artifact label used for a
#'   high-concurrency job in the Fabric Monitoring hub.
#' @param file Optional application file URI for a high-concurrency request.
#' @param class_name Optional Java/Scala main class for `file`.
#' @param args Optional character vector of application arguments.
#' @param jars,files,py_files Optional character vectors of dependency URIs
#'   supplied to Spark.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let fabricQueryR use its normal sign-in flow.
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()].
#' @param audience Optional sign-in scope. Most users should leave this `NULL`;
#'   set it only for a custom token provider or identity flow.
#' @param verbose Logical. Show session lifecycle messages.
#' @param allow_custom_endpoint Logical. Keep `FALSE` to require a Microsoft
#'   Fabric API host. Set `TRUE` only for a trusted custom HTTPS service, such
#'   as a test emulator; the Fabric bearer token is sent to this endpoint.
#'
#' @return A newly created [FabricLivySession]. It may still be starting; call
#'   `$wait()` before `$submit()`/`$run()`, and `$close()` when finished.
#' @section Choosing a session type:
#' Use a standard session for a typical sequence in one R process. High
#' concurrency is for applications that run several independent Spark workloads
#' at the same time; it is not needed for several sequential statements.
#'
#' @section Cleanup and permissions:
#' No network request is made when an open object is garbage collected. Call
#' `$close()` explicitly, and use `on.exit(session$close())` inside functions.
#' The signed-in identity needs Lakehouse read and execute access, permission for
#' code to access Fabric and storage, and an appropriate workspace role.
#'
#' @seealso
#' [Microsoft session jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session),
#' [high-concurrency Livy](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy)
#'
#' @examples
#' \dontrun{
#' run_shared_state <- function(lakehouse) {
#'   session <- fabric_livy_session(lakehouse)
#'   on.exit(session$close(), add = TRUE)
#'   session$wait()
#'   session$run("shared_value = 40", kind = "pyspark")
#'   session$run("print(shared_value + 2)", kind = "pyspark")
#' }
#'
#' run_high_concurrency <- function(lakehouse) {
#'   session <- fabric_livy_session(
#'     lakehouse,
#'     high_concurrency = TRUE,
#'     session_tag = "report-workers"
#'   )
#'   on.exit(session$close(), add = TRUE)
#'   session$wait()
#'   session$run("SELECT current_timestamp()", kind = "sql")
#' }
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
  verbose = TRUE,
  allow_custom_endpoint = FALSE
) {
  # 1 Validate session options ---------------------------------------------------------------------

  # High-concurrency-only fields are checked together so the error explains the
  # whole unsupported group instead of failing later in Fabric.
  fabric_livy_check_flag(high_concurrency, "high_concurrency")
  fabric_livy_check_flag(allow_custom_endpoint, "allow_custom_endpoint")
  fabric_livy_validate_session_fields(
    name = name,
    archives = archives,
    driver_memory = driver_memory,
    driver_cores = driver_cores,
    executor_memory = executor_memory,
    executor_cores = executor_cores,
    num_executors = num_executors
  )
  fabric_livy_check_optional_string(session_tag, "session_tag")
  fabric_livy_check_optional_string(artifact_name, "artifact_name")
  fabric_livy_check_optional_string(file, "file")
  fabric_livy_check_optional_string(class_name, "class_name")
  fabric_livy_check_string_vector(args, "args", allow_empty_strings = TRUE)
  fabric_livy_check_string_vector(jars, "jars")
  fabric_livy_check_string_vector(files, "files")
  fabric_livy_check_string_vector(py_files, "py_files")
  if (!is.null(session_tag) && !isTRUE(high_concurrency)) {
    rlang::abort(
      "session_tag is only available for high-concurrency sessions"
    )
  }
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
    rlang::abort(paste0(
      "artifact_name, file, class_name, args, jars, files, and py_files ",
      "are only available for high-concurrency sessions"
    ))
  }

  # 2 Build the session request --------------------------------------------------------------------

  # Drop unset options so Fabric can apply its own defaults.
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

  credential <- fabric_livy_credential(
    tenant_id,
    client_id,
    token,
    auth_args,
    audience
  )
  FabricLivySession$new(
    livy_url = fabric_livy_resolve_url(
      livy_url,
      allow_custom_endpoint = allow_custom_endpoint
    ),
    credential = credential,
    payload = payload,
    high_concurrency = high_concurrency,
    verbose = verbose,
    allow_custom_endpoint = allow_custom_endpoint
  )
}

# Livy session and statement R6 objects ------------------------------------------------------------

#' A Microsoft Fabric Livy session
#'
#' A Livy session keeps Spark running while you submit several pieces of code.
#' Create one with [fabric_livy_session()], call `$wait()` once it starts, use
#' `$run()` to execute code, and call `$close()` when finished. Most users do
#' not need to call this R6 class directly.
#'
#' @field id Fabric session or high-concurrency acquisition ID.
#' @field url Session lifecycle URL.
#' @field state Latest service state.
#' @field response Latest raw service response.
#' @field closed Whether `$close()` completed.
#' @field high_concurrency Whether this is a high-concurrency session.
#' @field session_id Underlying Livy session ID for HC sessions.
#' @field repl_id Isolated REPL ID for HC sessions.
#' @field verbose Whether lifecycle messages are enabled.
#' @format An [R6::R6Class] generator.
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

    #' @description Internal constructor used by [fabric_livy_session()].
    #' @param livy_url Livy API base or collection URL.
    #' @param credential Internal authentication credential.
    #' @param payload Session creation request body.
    #' @param high_concurrency Whether to acquire an HC session.
    #' @param verbose Whether to emit lifecycle messages.
    #' @param allow_custom_endpoint Whether a trusted non-Fabric HTTPS endpoint
    #'   may receive the Fabric bearer token.
    #' @returns A new session object.
    initialize = function(
      livy_url,
      credential,
      payload,
      high_concurrency = FALSE,
      verbose = TRUE,
      allow_custom_endpoint = FALSE
    ) {
      rlang::check_installed(
        c("httr2", "jsonlite"),
        reason = "to call the Fabric Livy API"
      )
      fabric_livy_check_flag(high_concurrency, "high_concurrency")
      fabric_livy_check_flag(verbose, "verbose")
      self$high_concurrency <- high_concurrency
      self$verbose <- verbose
      private$credential <- credential
      type <- if (high_concurrency) {
        "highConcurrencySessions"
      } else {
        "sessions"
      }
      private$collection_url <- fabric_livy_endpoint(
        livy_url,
        type,
        allow_custom_endpoint = allow_custom_endpoint
      )
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

    #' @description Print a concise session summary.
    #' @param ... Unused.
    #' @returns `self`, invisibly.
    print = function(...) {
      label <- if (self$high_concurrency) {
        "Fabric high-concurrency Livy session"
      } else {
        "Fabric Livy session"
      }
      cat("<", label, ">\n", sep = "")
      cat("  id: ", self$id %||% "<not created>", "\n", sep = "")
      cat("  state: ", self$state %||% "<unknown>", "\n", sep = "")
      if (self$high_concurrency) {
        cat(
          "  session/repl: ",
          self$session_id %||% "<pending>",
          "/",
          self$repl_id %||% "<pending>",
          "\n",
          sep = ""
        )
      }
      cat("  closed: ", self$closed, "\n", sep = "")
      invisible(self)
    },

    #' @description Return the latest session response.
    #' @param refresh Whether to retrieve current state from Fabric.
    #' @param deadline Internal wall-clock deadline for the status request.
    #' @returns The raw session response list.
    status = function(refresh = TRUE, deadline = NULL) {
      private$assert_open()
      fabric_livy_check_flag(refresh, "refresh")
      if (isTRUE(refresh)) {
        private$update(fabric_livy_json(
          "GET",
          self$url,
          private$credential,
          deadline = deadline
        ))
      }
      self$response
    },

    #' @description Wait until the session can accept statements.
    #' @param timeout Maximum wait in seconds.
    #' @param poll_interval Polling interval in seconds.
    #' @returns `self`, invisibly.
    wait = function(timeout = 600, poll_interval = 3) {
      private$assert_open()
      fabric_livy_check_number(timeout, "timeout")
      fabric_livy_check_number(poll_interval, "poll_interval")
      deadline <- Sys.time() + timeout
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
        ready <- if (self$high_concurrency) {
          identical(state, "idle") &&
            nzchar(self$session_id %||% "") &&
            nzchar(self$repl_id %||% "")
        } else {
          identical(state, "idle")
        }
        if (ready) {
          inform(self$verbose, "Fabric Livy session is ready", type = "success")
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
            result %in% c("uncertain", "failed", "cancelled", "canceled")
        ) {
          fabric_livy_abort_session(response)
        }
        fabric_livy_poll_sleep(deadline, poll_interval)
      }
    },

    #' @description Submit code without waiting for completion.
    #' @param code One string of Spark code.
    #' @param kind Statement language.
    #' @param source_id Optional caller-defined source identifier.
    #' @returns A [FabricLivyStatement].
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
        rlang::abort(
          "The Livy session is not ready; call session$wait() first"
        )
      }
      endpoint <- private$statement_collection()
      response <- fabric_livy_json(
        "POST",
        endpoint,
        private$credential,
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
        credential = private$credential,
        verbose = self$verbose
      )
    },

    #' @description Submit code, wait, and return its parsed result.
    #' @param code One string of Spark code.
    #' @param kind Statement language.
    #' @param source_id Optional caller-defined source identifier.
    #' @param timeout Maximum wait in seconds.
    #' @param poll_interval Polling interval in seconds.
    #' @returns A `fabric_livy_statement_result` list.
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

    #' @description List statements in this execution context.
    #' @returns The raw Livy statements response.
    statements = function() {
      private$assert_open()
      fabric_livy_json(
        "GET",
        private$statement_collection(),
        private$credential
      )
    },

    #' @description Reset a regular session's inactivity timeout.
    #' @returns `self`, invisibly.
    reset_timeout = function() {
      private$assert_open()
      if (self$high_concurrency) {
        rlang::abort(
          "reset_timeout() is not supported for high-concurrency sessions"
        )
      }
      fabric_livy_ok(
        "POST",
        paste0(self$url, "/reset-timeout"),
        private$credential,
        idempotent = FALSE
      )
      invisible(self)
    },

    #' @description Release this session or high-concurrency context.
    #' @returns `TRUE` when closed or `FALSE` when already closed, invisibly.
    close = function() {
      if (isTRUE(self$closed)) {
        return(invisible(FALSE))
      }
      inform(self$verbose, "Closing Fabric Livy session")
      fabric_livy_ok(
        "DELETE",
        self$url,
        private$credential,
        idempotent = TRUE,
        accepted_status = 404L
      )
      self$closed <- TRUE
      inform(self$verbose, "Fabric Livy session closed", type = "success")
      invisible(TRUE)
    }
  ),
  private = list(
    credential = NULL,
    collection_url = NULL,

    # Check that the session is still usable before a public method sends a
    # request. It takes no input and either returns quietly or raises an error.
    assert_open = function() {
      if (isTRUE(self$closed)) {
        rlang::abort("The Livy session is closed")
      }
    },

    # Copy fields from one Livy response onto the session and return that
    # response invisibly. Status-changing public methods use this after a call.
    update = function(response) {
      self$response <- response
      self$state <- response$state %||% self$state
      self$session_id <- response$sessionId %||% self$session_id
      self$repl_id <- response$replId %||% self$repl_id
      invisible(response)
    },

    # Build and return the statement collection URL for this session. Submit
    # and statement-list methods use it for normal and shared sessions.
    statement_collection = function() {
      if (!self$high_concurrency) {
        return(paste0(self$url, "/statements"))
      }
      if (
        !nzchar(self$session_id %||% "") ||
          !nzchar(self$repl_id %||% "")
      ) {
        rlang::abort(paste0(
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

#' A statement submitted to a Fabric Livy session
#'
#' Represents one piece of code submitted to a [FabricLivySession]. Call
#' `$wait()` and then `$result()` to retrieve its output. For the usual
#' submit-and-wait workflow, use the session's `$run()` method instead.
#'
#' @field id Numeric Livy statement ID.
#' @field url Statement lifecycle URL.
#' @field state Latest statement state.
#' @field response Latest raw service response.
#' @field started_local Local submission timestamp.
#' @field completed_local Local completion timestamp.
#' @field verbose Whether lifecycle messages are enabled.
#' @format An [R6::R6Class] generator.
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
    #' `FabricLivySession$submit()`.
    #' @param session Parent [FabricLivySession].
    #' @param response Initial statement response.
    #' @param url Statement lifecycle URL.
    #' @param credential Internal authentication credential.
    #' @param verbose Whether to emit lifecycle messages.
    #' @returns A new statement object.
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
      private$credential <- credential
      invisible(self)
    },

    #' @description Print a concise statement summary.
    #' @param ... Unused.
    #' @returns `self`, invisibly.
    print = function(...) {
      cat("<Fabric Livy statement>\n")
      cat("  id: ", self$id, "\n", sep = "")
      cat("  state: ", self$state %||% "<unknown>", "\n", sep = "")
      cat("  url: ", self$url, "\n", sep = "")
      invisible(self)
    },

    #' @description Retrieve statement state and available output.
    #' @param refresh Whether to retrieve current state from Fabric.
    #' @param deadline Internal wall-clock deadline for the status request.
    #' @returns The raw statement response list.
    status = function(refresh = TRUE, deadline = NULL) {
      fabric_livy_check_flag(refresh, "refresh")
      if (isTRUE(refresh)) {
        self$response <- fabric_livy_json(
          "GET",
          self$url,
          private$credential,
          deadline = deadline
        )
        self$state <- self$response$state %||% self$state
      }
      self$response
    },

    #' @description Wait for the statement to reach a terminal state.
    #' @param timeout Maximum wait in seconds.
    #' @param poll_interval Polling interval in seconds.
    #' @param error_on_failure Raise a structured error for failed statements.
    #' @returns `self`, invisibly.
    wait = function(
      timeout = 600,
      poll_interval = 2,
      error_on_failure = TRUE
    ) {
      fabric_livy_check_number(timeout, "timeout")
      fabric_livy_check_number(poll_interval, "poll_interval")
      fabric_livy_check_flag(error_on_failure, "error_on_failure")
      deadline <- Sys.time() + timeout
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
          return(invisible(self))
        }
        fabric_livy_poll_sleep(deadline, poll_interval)
      }
    },

    #' @description Return parsed output and timing metadata.
    #' @param refresh Whether to retrieve current state from Fabric.
    #' @param error_on_failure Raise a structured error for failed statements.
    #' @returns A `fabric_livy_statement_result` list.
    result = function(refresh = TRUE, error_on_failure = TRUE) {
      fabric_livy_check_flag(error_on_failure, "error_on_failure")
      response <- self$status(refresh = refresh)
      state <- fabric_livy_state(response)
      if (
        !identical(state, "available") &&
          !state %in% .fabric_livy_statement_failure_states
      ) {
        rlang::abort(
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

    #' @description Request cancellation of this statement.
    #' @returns The raw cancellation response, invisibly.
    cancel = function() {
      response <- fabric_livy_json(
        "POST",
        paste0(self$url, "/cancel"),
        private$credential,
        idempotent = FALSE
      )
      invisible(response)
    }
  ),
  private = list(
    session = NULL,
    credential = NULL
  ),
  cloneable = FALSE
)
