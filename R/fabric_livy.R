# Manage Livy session + statements ----------------------------------------

#' @title
#' Run a Livy API query (Spark code) in Microsoft Fabric
#'
#' @description
#' High-level helper that creates a Livy session in Microsoft Fabric, waits for
#' it to become idle, submits a statement with Spark code for execution, retrieves
#' the result, and closes the session.
#'
#' @details
#' - In Microsoft Fabric, you can find and copy the Livy session URL by going
#' to a 'Lakehouse' item, then go to 'Settings' -> 'Livy Endpoint' -> 'Session job connection string'.
#' - By default we request a token for `https://api.fabric.microsoft.com/.default`.
#' - \pkg{AzureAuth} is used to acquire the token. Be wary of
#' caching behavior; you may want to call [AzureAuth::clean_token_directory()]
#' to clear cached tokens if you run into issues
#'
#' @seealso
#' [Livy API overview - Microsoft Fabric - 'What is the Livy API for Data Engineering?'](<https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview>);
#' [Livy Docs - REST API](https://livy.apache.org/docs/latest/rest-api.html).
#'
#' @param livy_url Character. Livy session job connection string, e.g.
#' `"https://api.fabric.microsoft.com/v1/workspaces/.../lakehouses/.../livyapi/versions/2023-12-01/sessions"`
#' (see details).
#' @param code Character. Code to run in the Livy session.
#' @param kind Character. One of `"spark"`, `"pyspark"`, `"sparkr"`, or `"sql"`.
#' Indicates the type of Spark code being submitted for evaluation.
#' @param tenant_id Microsoft Azure tenant ID. Defaults to `Sys.getenv("FABRICQUERYR_TENANT_ID")` if missing.
#' @param client_id Microsoft Azure application (client) ID used to authenticate. Defaults to
#'   `Sys.getenv("FABRICQUERYR_CLIENT_ID")`. You may be able to use the Azure CLI app id
#'   `"04b07795-8ddb-461a-bbee-02f9e1bf7b46"`, but may want to make your own
#'   app registration in your tenant for better control.
#' @param access_token Optional character. If supplied, use this bearer token
#'   instead of acquiring a new one via `{AzureAuth}`.
#' @param environment_id Optional character. Fabric Environment (pool) ID to use
#' for the session. If `NULL` (default), the default environment for the user
#'  will be used.
#' @param conf Optional list. Spark configuration settings to apply to the session.
#' @param verbose Logical. Emit progress via `{cli}`. Default `TRUE`.
#' @param poll_interval Integer. Polling interval in seconds when waiting for session/statement readiness.
#' @param timeout Integer. Timeout in seconds when waiting for session/statement readiness.
#'
#' @return A list with statement details and results. The list contains:
#' - `id`: Statement ID.
#' - `state`: Final statement state (should be `"available"`).
#' - `started_local`: Local timestamp when statement started running.
#' - `completed_local`: Local timestamp when statement completed.
#' - `duration_sec`: Duration in seconds (local).
#' - `output`: A list with raw output details:
#'     - `status`: Output status (e.g., `"ok"`).
#'     - `execution_count`: Execution count (if applicable). The number of
#'       statements that have been executed in the session.
#'     - `data`: Raw data list with MIME types as keys (e.g.
#'       `"text/plain"`, `"application/json"`).
#'     - `parsed`: Parsed output, if possible. This may be a data frame (tibble)
#'       if the output was JSON tabular data, or a character vector if it was
#'       plain text. May be `NULL` if parsing was not possible.
#' - `url`: URL of the statement resource in the Livy API.
#'
#' @export
#'
#' @example inst/examples/fabric_livy_query.R
fabric_livy_query <- function(
  livy_url, # <- paste your sessions/batches/base URL
  code,
  kind = c("spark", "pyspark", "sparkr", "sql"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  environment_id = NULL,
  conf = NULL,
  verbose = TRUE,
  poll_interval = 2L,
  timeout = 600L
) {
  kind <- match.arg(kind)
  sess <- fabric_livy_session_create(
    livy_url = livy_url,
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    kind = NULL, # let statements specify kind (0.5+ behavior)
    conf = conf,
    environment_id = environment_id,
    verbose = verbose,
    timeout = timeout
  )
  on.exit(
    try(fabric_livy_session_close(sess, verbose), silent = TRUE),
    add = TRUE
  )
  fabric_livy_session_wait(
    sess,
    poll_interval = poll_interval,
    timeout = timeout,
    verbose = verbose
  )
  fabric_livy_statement(
    sess,
    code = code,
    kind = kind,
    poll_interval = poll_interval,
    timeout = timeout,
    verbose = verbose
  )
}

# Submit ANY code (spark | pyspark | sparkr | sql)
fabric_livy_statement <- function(
  session,
  code,
  kind = NULL, # optional override
  poll_interval = 2L,
  timeout = 600L,
  verbose = TRUE
) {
  rlang::check_installed(
    c("httr2", "jsonlite", "tibble"),
    reason = "to call the Livy REST API"
  )
  stopifnot(is.list(session), nzchar(session$url), nzchar(session$token))
  stopifnot(is.character(code), length(code) == 1L, nzchar(code))

  stmts_url <- paste0(session$url, "/statements")
  payload <- list(code = code)
  if (!is.null(kind)) payload$kind <- kind

  inform(verbose, "Submitting statement ...")
  t_submit <- Sys.time()
  st <- httr2::request(stmts_url) |>
    httr2::req_headers(Authorization = paste("Bearer", session$token)) |>
    httr2::req_body_json(payload) |>
    httr2::req_method("POST") |>
    .httr2_json()

  stmt_url <- paste0(stmts_url, "/", st$id)
  deadline <- Sys.time() + timeout
  state <- st$state %||% "running"

  # Local timing (Fabric often omits server-side started/completed)
  started_local <- if (state %in% c("running", "waiting")) t_submit else NULL
  completed_local <- NULL

  # Pretty CLI progress (single updating line)
  use_cli <- isTRUE(verbose) && rlang::is_installed("cli")
  if (use_cli) {
    guard <- ..local_cli_opts(list(cli.progress_show_after = 0))
    on.exit(try(guard$reset(), silent = TRUE), add = TRUE)

    first_line <- trimws(strsplit(code, "\n", fixed = TRUE)[[1]][1])
    if (nchar(first_line) > 60)
      first_line <- paste0(substr(first_line, 1, 57), "...")

    bar_id <- cli::cli_progress_bar(
      name = paste0("Statement ", st$id, " - ", first_line),
      total = NA,
      clear = FALSE,
      format = "{cli::pb_spin} {cli::pb_name}| time: {cli::pb_elapsed_clock} | status: {cli::pb_status}",
      format_done = "{cli::col_green(cli::symbol$tick)} {cli::pb_name}| time: {cli::pb_elapsed_clock} | status: done"
    )
    show_state <- function(prev, cur) {
      if (is.null(prev)) {
        cli::cli_progress_update(
          id = bar_id,
          status = sprintf("%s", cur)
        )
        return(cur)
      }
      if (identical(prev, cur)) return(prev)
      cli::cli_progress_update(
        id = bar_id,
        status = sprintf("%s \u2192 %s", prev, cur)
      )
      cur
    }
  } else {
    show_state <- function(prev, cur) {
      if (is.null(prev) || !identical(prev, cur))
        inform(TRUE, sprintf("Statement state: %s", cur))
      cur
    }
  }

  prev <- NULL
  prev <- show_state(prev, state)

  while (!identical(state, "available")) {
    if (Sys.time() > deadline)
      stop("Timed out waiting for statement.", call. = FALSE)
    Sys.sleep(poll_interval)

    st <- httr2::request(stmt_url) |>
      httr2::req_headers(Authorization = paste("Bearer", session$token)) |>
      .httr2_json()

    state <- st$state %||% "unknown"
    prev <- show_state(prev, state)

    if (is.null(started_local) && state %in% c("running", "waiting"))
      started_local <- Sys.time()

    if (state %in% c("error", "cancelling", "cancelled")) {
      completed_local <- Sys.time()
      if (use_cli) cli::cli_progress_done(id = bar_id)
      # Try to surface any message if present
      msg <- tryCatch(
        {
          o <- st$output
          if (is.list(o) && !is.null(o$error)) as.character(o$error) else if (
            is.list(o) && !is.null(o$data$`text/plain`)
          )
            as.character(o$data$`text/plain`) else NULL
        },
        error = function(e) NULL
      )
      if (nzchar(msg %||% "")) {
        stop(
          sprintf("Statement ended with state: %s\n%s", state, msg),
          call. = FALSE
        )
      } else {
        stop(sprintf("Statement ended with state: %s", state), call. = FALSE)
      }
    } else {
      if (use_cli) cli::cli_progress_update(id = bar_id, status = state)
    }
  }

  # Final timestamp when we reach 'available'
  completed_local <- completed_local %||% Sys.time()
  started_local <- started_local %||% t_submit

  if (use_cli) {
    cli::cli_progress_update(id = bar_id, status = "available")
    cli::cli_progress_done(id = bar_id)
  }

  out <- st$output %||% list()
  data <- out$data %||% list()
  parsed <- NULL
  if (!is.null(data[["application/json"]])) {
    obj <- try(
      jsonlite::fromJSON(
        jsonlite::toJSON(data[["application/json"]], auto_unbox = TRUE),
        simplifyVector = TRUE
      ),
      silent = TRUE
    )
    if (!inherits(obj, "try-error"))
      parsed <- if (is.data.frame(obj)) tibble::as_tibble(obj) else obj
  } else if (!is.null(data[["text/plain"]])) {
    parsed <- as.character(data[["text/plain"]])
  }

  duration_sec <- as.numeric(difftime(
    completed_local,
    started_local,
    units = "secs"
  ))

  invisible(list(
    id = st$id,
    state = st$state,
    started_local = started_local,
    completed_local = completed_local,
    duration_sec = duration_sec,
    output = list(
      status = out$status %||% NULL,
      execution_count = out$execution_count %||% NULL,
      data = data,
      parsed = parsed
    ),
    url = stmt_url
  ))
}

# Create a Livy session (kind optional; per-statement is allowed)
fabric_livy_session_create <- function(
  livy_url, # <- the URL you copy from Fabric
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  kind = NULL, # "spark","pyspark","sparkr","sql" (optional)
  name = NULL,
  conf = NULL,
  environment_id = NULL, # Fabric Environment (pool) id
  jars = NULL,
  pyFiles = NULL,
  files = NULL,
  driverMemory = NULL,
  driverCores = NULL,
  executorMemory = NULL,
  executorCores = NULL,
  numExecutors = NULL,
  archives = NULL,
  queue = NULL,
  proxyUser = NULL,
  heartbeatTimeoutInSecond = NULL,
  ttl = NULL,
  verbose = TRUE,
  timeout = 600L
) {
  rlang::check_installed(c("httr2"), reason = "to call the Livy REST API")
  if (is.null(access_token)) {
    rlang::check_installed("AzureAuth")
    inform(verbose, "Authenticating for Fabric Livy API ...")
    access_token <- fabric_get_livy_token(tenant_id, client_id)
  }

  sessions_url <- fabric_livy_endpoint(livy_url, "sessions")

  payload <- Filter(
    Negate(is.null),
    list(
      kind = kind,
      name = name,
      conf = conf,
      jars = jars,
      pyFiles = pyFiles,
      files = files,
      driverMemory = driverMemory,
      driverCores = driverCores,
      executorMemory = executorMemory,
      executorCores = executorCores,
      numExecutors = numExecutors,
      archives = archives,
      queue = queue,
      proxyUser = proxyUser,
      heartbeatTimeoutInSecond = heartbeatTimeoutInSecond,
      ttl = ttl
    )
  )
  if (!is.null(environment_id) && nzchar(environment_id)) {
    payload$conf <- payload$conf %||% list()
    payload$conf[["spark.fabric.environmentDetails"]] <- sprintf(
      "{\"id\":\"%s\"}",
      environment_id
    )
  }

  inform(verbose, "Creating Livy session ...")
  resp <- httr2::request(sessions_url) |>
    httr2::req_headers(Authorization = paste("Bearer", access_token)) |>
    httr2::req_body_json(payload) |>
    httr2::req_method("POST") |>
    .httr2_json()

  session_id <- as.character(resp$id %||% NA)
  if (!nzchar(session_id)) stop("Failed to create Livy session.", call. = FALSE)
  invisible(list(
    id = session_id,
    url = paste0(sessions_url, "/", session_id),
    token = access_token
  ))
}

fabric_livy_session_wait <- function(
  session,
  poll_interval = 3L,
  timeout = 600L,
  verbose = TRUE
) {
  rlang::check_installed("httr2")
  stopifnot(is.list(session), nzchar(session$url), nzchar(session$token))
  deadline <- Sys.time() + timeout

  use_cli <- isTRUE(verbose) && rlang::is_installed("cli")
  if (use_cli) {
    guard <- ..local_cli_opts(list(cli.progress_show_after = 0))
    on.exit(try(guard$reset(), silent = TRUE), add = TRUE)
    bar_id <- cli::cli_progress_bar(
      name = "Livy session",
      total = NA,
      clear = FALSE,
      format = "{cli::pb_spin} {cli::pb_name}| time: {cli::pb_elapsed_clock} | status: {cli::pb_status}",
      format_done = "{cli::col_green(cli::symbol$tick)} {cli::pb_name}| time: {cli::pb_elapsed_clock} | status: ready"
    )
    update_status <- function(prev, cur) {
      if (is.null(prev)) {
        cli::cli_progress_update(
          id = bar_id,
          status = sprintf("%s", cur)
        )
        return(cur)
      }
      if (identical(prev, cur)) return(prev)
      cli::cli_progress_update(
        id = bar_id,
        status = sprintf("%s \u2192 %s", prev, cur)
      )
      cur
    }
  } else {
    inform(TRUE, "Waiting for Livy session to become idle ...")
    update_status <- function(prev, cur) {
      if (is.null(prev) || !identical(prev, cur))
        inform(TRUE, sprintf("Session state: %s", cur))
      cur
    }
  }

  prev <- NULL
  repeat {
    if (Sys.time() > deadline)
      stop("Timed out waiting for session to become idle.", call. = FALSE)

    s <- httr2::request(session$url) |>
      httr2::req_headers(Authorization = paste("Bearer", session$token)) |>
      .httr2_json()

    st <- s$state %||% "unknown"
    prev <- update_status(prev, st)

    if (st == "idle") break
    if (st %in% c("error", "dead", "killed", "shutting_down")) {
      if (use_cli) cli::cli_progress_done(id = bar_id)
      stop(paste("Session failed:", st), call. = FALSE)
    }
    Sys.sleep(poll_interval)
  }

  if (use_cli) {
    cli::cli_progress_update(id = bar_id, status = "idle")
    cli::cli_progress_done(id = bar_id)
  }
  invisible(session)
}

fabric_livy_session_close <- function(session, verbose = TRUE) {
  rlang::check_installed("httr2")
  inform(verbose, "Closing Livy session ...")

  closed <- FALSE
  try(
    {
      httr2::request(session$url) |>
        httr2::req_headers(Authorization = paste("Bearer", session$token)) |>
        httr2::req_method("DELETE") |>
        .httr2_ok()
      closed <- TRUE
    },
    silent = TRUE
  )

  if (verbose) {
    if (closed) {
      inform(verbose, "Livy session closed", type = "success")
    } else {
      inform(verbose, "Livy session not closed", type = "warning")
    }
  }

  return(invisible(closed))
}

# Get token ---------------------------------------------------------------

# Token for Fabric + refresh capability
fabric_get_livy_token <- function(tenant_id, client_id) {
  tok <- AzureAuth::get_azure_token(
    resource = c("https://api.fabric.microsoft.com/.default", "offline_access"),
    tenant = tenant_id,
    app = client_id,
    version = 2
  )
  tok$credentials$access_token
}


# Normalize endpoint from URL ---------------------------------------------

# Normalize /sessions or /batches URLs to a standard form endpoint
fabric_livy_endpoint <- function(url, type = c("sessions", "batches")) {
  stopifnot(is.character(url), length(url) == 1L, nzchar(url))
  type <- match.arg(type)
  u <- trimws(url)
  # strip trailing slash
  u <- sub("/+$", "", u)
  # If already ends with sessions/batches (case-insensitive), replace as needed
  if (grepl("(?i)/(sessions|batches)$", u, perl = TRUE)) {
    u <- sub("(?i)/(sessions|batches)$", paste0("/", type), u, perl = TRUE)
  } else {
    # otherwise assume user gave the livy base; append the type
    # (works for .../livyapi/versions/2023-12-01 and similar)
    u <- paste0(u, "/", type)
  }
  u
}


# Miscellaneous -----------------------------------------------------------

# CLI progress bar options
..local_cli_opts <- function(opts) {
  old <- options(opts)
  structure(
    list(
      reset = function() options(old)
    ),
    class = "cli_opt_guard"
  )
}
