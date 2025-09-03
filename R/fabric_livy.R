# =========================
# Fabric Livy – URL-first API
# =========================

# ---- deps & small utils ----

.fabric_inform <- function(
  verbose,
  msg,
  type = c("info", "warning", "danger", "success")
) {
  if (!isTRUE(verbose)) return(invisible())
  type <- match.arg(type)
  switch(
    type,
    info = cli::cli_alert_info(msg),
    warning = cli::cli_alert_warning(msg),
    danger = cli::cli_alert_danger(msg),
    success = cli::cli_alert_success(msg)
  )
  invisible()
}

.httr2_body_preview <- function(resp, max_chars = 8000L) {
  ctype <- httr2::resp_content_type(resp)
  txt <- NULL
  if (grepl("json", ctype, ignore.case = TRUE)) {
    # try to pretty JSON; fall back to raw text
    txt <- try(
      {
        obj <- httr2::resp_body_json(resp, simplifyVector = FALSE)
        jsonlite::toJSON(obj, auto_unbox = TRUE, pretty = TRUE)
      },
      silent = TRUE
    )
    if (inherits(txt, "try-error")) txt <- httr2::resp_body_string(resp)
  } else {
    txt <- httr2::resp_body_string(resp)
  }
  if (is.na(txt) || is.null(txt)) txt <- ""
  if (nchar(txt) > max_chars) {
    paste0(substr(txt, 1L, max_chars), "\n... <truncated> ...")
  } else {
    txt
  }
}

# Compose a helpful error with status, request-id, and body
.httr2_stop_http <- function(resp, prefix = "HTTP request failed") {
  status <- httr2::resp_status(resp)
  reason <- httr2::resp_status_desc(resp)
  rid <- httr2::resp_header(resp, "x-ms-request-id") %||%
    httr2::resp_header(resp, "request-id")
  act <- httr2::resp_header(resp, "x-ms-activity-id")
  body <- .httr2_body_preview(resp)

  hdr <- paste0(prefix, ": HTTP ", status, " ", reason, ".")
  mid <- paste(
    if (!is.null(rid)) paste0("x-ms-request-id: ", rid) else NULL,
    if (!is.null(act)) paste0("x-ms-activity-id: ", act) else NULL,
    sep = "\n"
  )
  msg <- paste0(
    hdr,
    if (isTRUE(nzchar(mid))) paste0("\n", mid) else "",
    "\n--- Response body ---\n",
    body
  )
  stop(msg, call. = FALSE)
}

# Perform a request and parse JSON; do NOT throw until we format a great error
.httr2_json <- function(req) {
  req <- httr2::req_error(req, is_error = function(resp) FALSE) # don’t auto-stop
  resp <- httr2::req_perform(req)
  if (httr2::resp_status(resp) >= 400L) .httr2_stop_http(resp)
  httr2::resp_body_json(resp, simplifyVector = TRUE)
}

# Perform a request where we don't need a body back (DELETE, etc.)
.httr2_ok <- function(req) {
  req <- httr2::req_error(req, is_error = function(resp) FALSE)
  resp <- httr2::req_perform(req)
  if (httr2::resp_status(resp) >= 400L) .httr2_stop_http(resp)
  invisible(TRUE)
}

# Token for Fabric + refresh capability
fabric_get_fabric_token <- function(tenant_id, client_id) {
  tok <- AzureAuth::get_azure_token(
    resource = c("https://api.fabric.microsoft.com/.default", "offline_access"),
    tenant = tenant_id,
    app = client_id,
    version = 2
  )
  tok$credentials$access_token
}

# Normalize whatever the user pasted to /sessions or /batches
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

..local_cli_opts <- function(opts) {
  old <- options(opts)
  structure(
    list(
      reset = function() options(old)
    ),
    class = "cli_opt_guard"
  )
}


# =========================
# Sessions + statements
# =========================

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
    .fabric_inform(verbose, "Authenticating for Fabric Livy API ...")
    access_token <- fabric_get_fabric_token(tenant_id, client_id)
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

  .fabric_inform(verbose, "Creating Livy session ...")
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
      format = "{cli::pb_spin} {cli::pb_name} | status: {cli::pb_status} | time: {cli::pb_elapsed_clock}",
      format_done = "{cli::col_green(cli::symbol$tick)} {cli::pb_name} | status: ready (idle) | time: {cli::pb_elapsed_clock}"
    )
    update_status <- function(prev, cur) {
      if (is.null(prev)) {
        cli::cli_progress_update(
          id = bar_id,
          status = sprintf("state: %s", cur)
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
    .fabric_inform(TRUE, "Waiting for Livy session to become idle ...")
    update_status <- function(prev, cur) {
      if (is.null(prev) || !identical(prev, cur))
        .fabric_inform(TRUE, sprintf("Session state: %s", cur))
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
  .fabric_inform(verbose, "Closing Livy session ...")

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
      .fabric_inform(verbose, "Livy session closed", type = "success")
    } else {
      .fabric_inform(verbose, "Livy session not closed", type = "warning")
    }
  }

  return(invisible(closed))
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
  rlang::check_installed(c("httr2", "jsonlite", "tibble"))
  stopifnot(is.list(session), nzchar(session$url), nzchar(session$token))
  stopifnot(is.character(code), length(code) == 1L, nzchar(code))

  stmts_url <- paste0(session$url, "/statements")
  payload <- list(code = code)
  if (!is.null(kind)) payload$kind <- kind

  .fabric_inform(verbose, "Submitting statement ...")
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
      name = paste0("Statement ", st$id, " — ", first_line),
      total = NA,
      clear = FALSE,
      format = "{cli::pb_spin} {cli::pb_name} | status: {cli::pb_status} | time: {cli::pb_elapsed_clock}",
      format_done = "{cli::col_green(cli::symbol$tick)} {cli::pb_name} | status: done (available) | time: {cli::pb_elapsed_clock}"
    )
    show_state <- function(prev, cur) {
      if (is.null(prev)) {
        cli::cli_progress_update(
          id = bar_id,
          status = sprintf("state: %s", cur)
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
        .fabric_inform(TRUE, sprintf("Statement state: %s", cur))
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


# Convenience: create -> wait -> submit -> close
fabric_livy_run <- function(
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
  timeout = 600L,
  auto_close = TRUE
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
    if (isTRUE(auto_close))
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

# =========================
# Batches (non-interactive)
# =========================

fabric_livy_batch_submit <- function(
  livy_url,
  file,
  args = NULL,
  className = NULL,
  jars = NULL,
  pyFiles = NULL,
  files = NULL,
  driverMemory = NULL,
  driverCores = NULL,
  executorMemory = NULL,
  executorCores = NULL,
  numExecutors = NULL,
  archives = NULL,
  name = NULL,
  queue = NULL,
  proxyUser = NULL,
  conf = NULL,
  ttl = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  environment_id = NULL,
  verbose = TRUE,
  timeout = 600L
) {
  rlang::check_installed("httr2")
  if (is.null(access_token)) {
    rlang::check_installed("AzureAuth")
    .fabric_inform(verbose, "Authenticating for Fabric Livy API ...")
    access_token <- fabric_get_fabric_token(tenant_id, client_id)
  }

  batches_url <- fabric_livy_endpoint(livy_url, "batches")
  payload <- Filter(
    Negate(is.null),
    list(
      file = file,
      args = args,
      className = className,
      jars = jars,
      pyFiles = pyFiles,
      files = files,
      driverMemory = driverMemory,
      driverCores = driverCores,
      executorMemory = executorMemory,
      executorCores = executorCores,
      numExecutors = numExecutors,
      archives = archives,
      name = name,
      queue = queue,
      proxyUser = proxyUser,
      conf = conf,
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

  .fabric_inform(verbose, "Submitting Livy batch ...")
  resp <- httr2::request(batches_url) |>
    httr2::req_headers(Authorization = paste("Bearer", access_token)) |>
    httr2::req_body_json(payload) |>
    httr2::req_method("POST") |>
    .httr2_json()

  invisible(list(
    id = resp$id,
    url = paste0(batches_url, "/", resp$id),
    token = access_token
  ))
}

fabric_livy_batch_state <- function(batch, verbose = TRUE) {
  rlang::check_installed("httr2")
  res <- httr2::request(paste0(batch$url, "/state")) |>
    httr2::req_headers(Authorization = paste("Bearer", batch$token)) |>
    .httr2_json()
  .fabric_inform(verbose, sprintf("Batch state: %s", res$state))
  invisible(res)
}

fabric_livy_batch_log <- function(batch, from = 0L, size = 100L) {
  rlang::check_installed("httr2")
  httr2::request(paste0(batch$url, "/log")) |>
    httr2::req_url_query(from = as.integer(from), size = as.integer(size)) |>
    httr2::req_headers(Authorization = paste("Bearer", batch$token)) |>
    .httr2_json()
}

fabric_livy_batch_kill <- function(batch) {
  rlang::check_installed("httr2")
  httr2::request(batch$url) |>
    httr2::req_headers(Authorization = paste("Bearer", batch$token)) |>
    httr2::req_method("DELETE") |>
    .httr2_ok()
  invisible(TRUE)
}
