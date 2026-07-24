# Livy session and statement R6 objects ------------------------------------

#' A Microsoft Fabric Livy session
#'
#' `FabricLivySession` represents either a regular interactive Livy session or
#' a high-concurrency (HC) session. Create instances with
#' [fabric_livy_session()] rather than calling `$new()` directly.
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
    #' @returns A new session object.
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
      private$credential <- credential
      type <- if (high_concurrency) {
        "highConcurrencySessions"
      } else {
        "sessions"
      }
      private$collection_url <- fabric_livy_endpoint(livy_url, type)
      inform(verbose, "Creating Fabric Livy session ...")
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
    #' @returns The raw session response list.
    status = function(refresh = TRUE) {
      private$assert_open()
      fabric_livy_check_flag(refresh, "refresh")
      if (isTRUE(refresh)) {
        private$update(fabric_livy_json(
          "GET",
          self$url,
          private$credential
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
        response <- self$status()
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
        if (
          state %in%
            .fabric_livy_session_terminal_states ||
            fabric_state %in% c("error", "cancelled", "canceled")
        ) {
          fabric_livy_abort_session(response)
        }
        if (Sys.time() >= deadline) {
          stop("Timed out waiting for the Livy session.", call. = FALSE)
        }
        Sys.sleep(poll_interval)
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
        stop(
          "The Livy session is not ready; call session$wait() first.",
          call. = FALSE
        )
      }
      endpoint <- private$statement_collection()
      response <- fabric_livy_json(
        "POST",
        endpoint,
        private$credential,
        payload = fabric_livy_payload(
          code = code,
          kind = kind,
          sourceId = source_id
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
        stop(
          "reset_timeout() is not supported for high-concurrency sessions.",
          call. = FALSE
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
      inform(self$verbose, "Closing Fabric Livy session ...")
      fabric_livy_ok(
        "DELETE",
        self$url,
        private$credential,
        idempotent = TRUE
      )
      self$closed <- TRUE
      inform(self$verbose, "Fabric Livy session closed", type = "success")
      invisible(TRUE)
    }
  ),
  private = list(
    credential = NULL,
    collection_url = NULL,

    finalize = function() {
      if (!isTRUE(self$closed) && !is.null(self$url)) {
        try(self$close(), silent = TRUE)
      }
    },

    assert_open = function() {
      if (isTRUE(self$closed)) {
        stop("The Livy session is closed.", call. = FALSE)
      }
    },

    update = function(response) {
      self$response <- response
      self$state <- response$state %||% self$state
      self$session_id <- response$sessionId %||% self$session_id
      self$repl_id <- response$replId %||% self$repl_id
      invisible(response)
    },

    statement_collection = function() {
      if (!self$high_concurrency) {
        return(paste0(self$url, "/statements"))
      }
      if (
        !nzchar(self$session_id %||% "") ||
          !nzchar(self$repl_id %||% "")
      ) {
        stop(
          "The high-concurrency session has no sessionId/replId yet; ",
          "call session$wait().",
          call. = FALSE
        )
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
#' Instances are returned by `FabricLivySession$submit()`.
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
    #' @returns The raw statement response list.
    status = function(refresh = TRUE) {
      fabric_livy_check_flag(refresh, "refresh")
      if (isTRUE(refresh)) {
        self$response <- fabric_livy_json(
          "GET",
          self$url,
          private$credential
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
        response <- self$status()
        state <- fabric_livy_state(response)
        output_error <- identical(
          tolower(response$output$status %||% ""),
          "error"
        )
        if (
          identical(state, "available") ||
            state %in% .fabric_livy_statement_failure_states
        ) {
          self$completed_local <- Sys.time()
          if (
            isTRUE(error_on_failure) &&
              (state %in% .fabric_livy_statement_failure_states || output_error)
          ) {
            fabric_livy_abort_statement(response)
          }
          return(invisible(self))
        }
        if (Sys.time() >= deadline) {
          stop("Timed out waiting for the Livy statement.", call. = FALSE)
        }
        Sys.sleep(poll_interval)
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
        stop(
          "The Livy statement is not complete; call statement$wait().",
          call. = FALSE
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
      completed <- self$completed_local %||% Sys.time()
      invisible(fabric_livy_output(
        response,
        started_local = self$started_local,
        completed_local = completed,
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

#' Create a Microsoft Fabric Livy session
#'
#' Creates and returns an R6 object for an interactive Spark session. Set
#' `high_concurrency = TRUE` to acquire an isolated REPL in Fabric's
#' high-concurrency session pool.
#'
#' @param livy_url A copied session or batch connection URL, Livy API base URL,
#'   or enriched Lakehouse record from [fabric_lakehouses()] or [fabric_item()].
#' @param high_concurrency Logical. Acquire a high-concurrency session.
#' @param session_tag Optional packing hint for high-concurrency sessions.
#'   Repeated requests with the same tag remain non-idempotent and return
#'   distinct HC session IDs.
#' @param name Optional session name.
#' @param tags Optional named list of string session tags.
#' @param conf Optional named list of Spark settings.
#' @param environment_id Optional Fabric Environment ID.
#' @param archives Optional character vector of archive URIs.
#' @param driver_memory,executor_memory Optional Spark memory strings.
#' @param driver_cores,executor_cores,num_executors Optional Spark resource
#'   counts.
#' @param artifact_name,file,class_name,args,jars,files,py_files Optional
#'   high-concurrency request fields. `artifact_name` controls the Monitoring
#'   hub label.
#' @param tenant_id Microsoft Entra tenant ID.
#' @param client_id Microsoft Entra application ID.
#' @param access_token Optional Fabric bearer token.
#' @param token_provider Optional callback returning a Fabric bearer token.
#' @param verbose Logical. Emit lifecycle messages.
#'
#' @return A newly created [FabricLivySession].
#' @details A finalizer attempts cleanup if an open object is garbage
#'   collected. Call `$close()` explicitly, and use `on.exit(session$close())`
#'   in functions, for deterministic cleanup. Requests use the
#'   `https://api.fabric.microsoft.com/.default` audience. Delegated
#'   authentication requires `Lakehouse.Execute.All`, `Lakehouse.Read.All`,
#'   `Code.AccessFabric.All`, and `Code.AccessStorage.All`.
#'
#' @seealso
#' [Microsoft session jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session),
#' [high-concurrency Livy](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy)
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
  access_token = NULL,
  token_provider = NULL,
  verbose = TRUE
) {
  fabric_livy_check_flag(high_concurrency, "high_concurrency")
  if (!is.null(session_tag) && !isTRUE(high_concurrency)) {
    stop(
      "session_tag is only available for high-concurrency sessions.",
      call. = FALSE
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
    stop(
      "artifact_name, file, class_name, args, jars, files, and py_files ",
      "are only available for high-concurrency sessions.",
      call. = FALSE
    )
  }
  tags <- fabric_livy_normalize_named_list(tags, "tags")
  payload <- fabric_livy_payload(
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
  credential <- fabric_livy_credential(
    tenant_id,
    client_id,
    access_token,
    token_provider
  )
  FabricLivySession$new(
    livy_url = fabric_livy_resolve_url(livy_url),
    credential = credential,
    payload = payload,
    high_concurrency = high_concurrency,
    verbose = verbose
  )
}
