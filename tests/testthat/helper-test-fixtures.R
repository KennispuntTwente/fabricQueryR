# Shared fixtures must load before shuffled test expressions

# test-delta-rs-oracle.R
fabric_test_delta_runtime_python <- function(root) {
  relative <- if (.Platform$OS.type == "windows") {
    file.path(".venv", "Scripts", "python.exe")
  } else {
    file.path(".venv", "bin", "python")
  }
  python <- file.path(root, relative)
  # On Unix, the venv executable is commonly a symlink to the base Python
  # Resolving that final component makes reticulate bypass the venv packages
  file.path(
    normalizePath(dirname(python), winslash = "/", mustWork = TRUE),
    basename(python)
  )
}

fabric_test_select_delta_runtime <- function() {
  oracle <- fabric_test_require_delta_oracle()
  python <- fabric_test_delta_runtime_python(oracle$root)
  expect_true(startsWith(python, paste0(oracle$root, "/.venv/")))
  if (reticulate::py_available(initialize = FALSE)) {
    selected <- normalizePath(
      reticulate::py_config()$python,
      winslash = "/",
      mustWork = TRUE
    )
    fabric_test_skip_or_fail(
      !identical(tolower(selected), tolower(python)),
      "reticulate was initialized with a different Python interpreter"
    )
  } else {
    Sys.setenv(RETICULATE_PYTHON = python)
  }
  oracle
}


# test-fabric_discovery.R
discovery_response <- function(
  body,
  url = "https://api.fabric.microsoft.com/v1"
) {
  httr2::response(
    status_code = 200L,
    url = url,
    headers = list("content-type" = "application/json"),
    body = charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
  )
}


# test-fabric_function_invoke.R
function_test_url <- paste0(
  "https://api.fabric.microsoft.com/v1/workspaces/",
  "cfafbeb1-8037-4d0c-896e-a46fb27ff229/userDataFunctions/",
  "5b218778-e7a5-4d73-8187-f10824047715/functions/echoInput/invoke"
)

function_test_response <- function(
  body,
  status = 200L,
  url = function_test_url,
  headers = list()
) {
  if (!is.raw(body)) {
    body <- charToRaw(jsonlite::toJSON(
      body,
      auto_unbox = TRUE,
      null = "null"
    ))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = c(list("content-type" = "application/json"), headers),
    body = body
  )
}

function_success_body <- function(output = "ok", ...) {
  c(
    list(
      functionName = "echoInput",
      invocationId = "c63f7f60-1ce4-4b30-9694-dcdcec871bba",
      status = "Succeeded",
      output = output,
      errors = list()
    ),
    list(...)
  )
}

function_fake_azure_token <- function() {
  class <- R6::R6Class(
    "FunctionFakeAzureToken",
    inherit = AzureAuth::AzureToken,
    public = list(
      initialize = function() {
        self$credentials <- list(access_token = "azure-token")
      },
      validate = function() TRUE,
      refresh = function() invisible(self)
    )
  )
  class$new()
}


# test-fabric_graphql_query.R
graphql_test_response <- function(
  body,
  status = 200L,
  url = "https://api.fabric.microsoft.com/graphql"
) {
  if (!is.raw(body)) {
    body <- charToRaw(jsonlite::toJSON(
      body,
      auto_unbox = TRUE,
      null = "null"
    ))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = list("content-type" = "application/json"),
    body = body
  )
}

graphql_fake_azure_token <- function() {
  class <- R6::R6Class(
    "GraphQLFakeAzureToken",
    inherit = AzureAuth::AzureToken,
    public = list(
      initialize = function() {
        self$credentials <- list(access_token = "azure-token")
      },
      validate = function() TRUE,
      refresh = function() invisible(self)
    )
  )
  class$new()
}


# test-fabric_job_scheduler.R
scheduler_test_item <- function(type = "Notebook") {
  list(
    id = "11111111-1111-1111-1111-111111111111",
    workspaceId = "22222222-2222-2222-2222-222222222222",
    type = type,
    displayName = "Scheduled fixture"
  )
}

scheduler_test_configuration <- function(type = "Daily") {
  arguments <- list(
    type = type,
    start_time = "2026-10-01T00:00:00Z",
    end_time = "2027-10-01T00:00:00Z",
    time_zone = "W. Europe Standard Time"
  )
  if (identical(type, "Cron")) {
    arguments$interval <- 15L
  } else {
    arguments$times <- "09:30"
  }
  if (identical(type, "Weekly")) {
    arguments$weekdays <- c("Monday", "thursday")
  }
  if (identical(type, "Monthly")) {
    arguments$recurrence <- 2L
    arguments$day_of_month <- 15L
  }
  do.call(fabric_job_schedule_config, arguments)
}

scheduler_test_response <- function(
  id = "33333333-3333-3333-3333-333333333333",
  configuration = scheduler_test_configuration(),
  enabled = TRUE,
  execution_data = NULL
) {
  body <- list(
    id = id,
    enabled = enabled,
    createdDateTime = "2026-08-13T12:30:00Z",
    configuration = unclass(configuration),
    owner = list(
      id = "44444444-4444-4444-4444-444444444444",
      type = "User"
    )
  )
  if (!is.null(execution_data)) {
    body$executionData <- execution_data
  }
  body
}


# test-fabric_jobs.R
job_test_item <- function(type = "Notebook") {
  list(
    id = "11111111-1111-1111-1111-111111111111",
    workspaceId = "22222222-2222-2222-2222-222222222222",
    type = type,
    displayName = "Job fixture"
  )
}

job_test_handle <- function(
  status_result = NULL,
  item_type = "Notebook",
  retry_after = NULL
) {
  structure(
    list(
      id = "33333333-3333-3333-3333-333333333333",
      workspace_id = "22222222-2222-2222-2222-222222222222",
      item_id = "11111111-1111-1111-1111-111111111111",
      item_type = item_type,
      job_type = if (item_type == "Notebook") "RunNotebook" else "Pipeline",
      location = "/jobs/instances/33333333-3333-3333-3333-333333333333",
      retry_after = retry_after,
      submitted_at = as.POSIXct("2026-01-01", tz = "UTC"),
      api_base = "https://api.fabric.test/v1",
      allow_custom_endpoint = TRUE,
      route = if (item_type == "Notebook") "notebook" else "core",
      credential = fabric_credential(token = "test-token"),
      status_result = status_result
    ),
    class = "fabric_job"
  )
}


# test-fabric_kql_export.R
kusto_export_test_id <- "11111111-2222-4333-8444-555555555555"

kusto_export_test_table <- function(columns, rows = list()) {
  list(
    TableName = "Table_0",
    Columns = lapply(names(columns), function(name) {
      list(ColumnName = name, ColumnType = unname(columns[[name]]))
    }),
    Rows = rows
  )
}

kusto_export_test_response <- function(table, request_id = NULL) {
  headers <- list("content-type" = "application/json")
  if (!is.null(request_id)) {
    headers[["x-ms-request-id"]] <- request_id
  }
  httr2::response(
    status_code = 200L,
    url = "https://cluster.z1.kusto.fabric.microsoft.com/v1/rest/mgmt",
    headers = headers,
    body = charToRaw(jsonlite::toJSON(
      list(Tables = list(table)),
      auto_unbox = TRUE,
      null = "null",
      digits = 22
    ))
  )
}

kusto_export_test_operation <- function() {
  kusto_export_test_table(
    c(OperationId = "Guid"),
    list(list(kusto_export_test_id))
  )
}

kusto_export_test_status <- function(state, status = "") {
  kusto_export_test_table(
    c(
      OperationId = "Guid",
      Operation = "String",
      StartedOn = "DateTime",
      LastUpdatedOn = "DateTime",
      Duration = "TimeSpan",
      State = "String",
      Status = "String"
    ),
    list(list(
      kusto_export_test_id,
      "DataExportToFile",
      "2026-08-14T10:00:00Z",
      "2026-08-14T10:00:01Z",
      "00:00:01",
      state,
      status
    ))
  )
}

kusto_export_test_target <- function() {
  list(
    id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    workspaceId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    type = "Lakehouse",
    displayName = "Exports"
  )
}


# test-fabric_kql_ingestion.R
kusto_ingestion_test_response <- function(
  body,
  status = 200L,
  url = "https://ingest-cluster.kusto.fabric.microsoft.com",
  headers = list()
) {
  if (!is.raw(body)) {
    body <- charToRaw(jsonlite::toJSON(
      body,
      auto_unbox = TRUE,
      null = "null",
      digits = 22
    ))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = c(list("content-type" = "application/json"), headers),
    body = body
  )
}

kusto_ingestion_test_status <- function(
  succeeded = 0L,
  failed = 0L,
  in_progress = 0L,
  canceled = 0L,
  details = list()
) {
  list(
    startTime = "2026-08-14T10:00:00Z",
    lastUpdated = "2026-08-14T10:01:00Z",
    status = list(
      Succeeded = succeeded,
      Failed = failed,
      InProgress = in_progress,
      Canceled = canceled
    ),
    details = details
  )
}


# test-fabric_kql_query.R
kusto_test_response <- function(
  body,
  status = 200L,
  url = "https://cluster.test"
) {
  if (!is.raw(body)) {
    body <- charToRaw(jsonlite::toJSON(
      body,
      auto_unbox = TRUE,
      null = "null",
      digits = 22
    ))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = list("content-type" = "application/json"),
    body = body
  )
}

kusto_test_completion <- function(has_errors = FALSE, errors = NULL) {
  out <- list(
    FrameType = "DataSetCompletion",
    HasErrors = has_errors,
    Cancelled = FALSE
  )
  if (!is.null(errors)) {
    out$OneApiErrors <- errors
  }
  out
}


# test-fabric_kql_write_table.R
kql_write_test_folder <- paste0(
  "https://onelake.dfs.fabric.microsoft.com/",
  "11111111-1111-4111-8111-111111111111/",
  "22222222-2222-4222-8222-222222222222/Files/ingestion"
)

kql_write_test_configuration <- function(
  max_data_size = 6442450944,
  max_blobs = 20,
  lake_folder = kql_write_test_folder
) {
  list(
    lake_folders = lake_folder,
    max_data_size = max_data_size,
    max_blobs = max_blobs,
    preferred_upload_method = "Lake",
    preferred_ingestion_method = "REST"
  )
}

kql_write_test_ingestion <- function() {
  structure(
    list(
      id = "ingest_op_r_data",
      operation_id = "ingest_op_r_data"
    ),
    class = "fabric_kql_ingestion"
  )
}

kql_write_test_status <- function(state = "Succeeded") {
  structure(
    list(
      operation_id = "ingest_op_r_data",
      state = state,
      complete = TRUE,
      succeeded = if (identical(state, "Succeeded")) 1 else 0,
      failed = if (identical(state, "Failed")) 1 else 0,
      in_progress = 0,
      canceled = 0,
      details = tibble::tibble(
        source_id = "11111111-1111-4111-8111-111111111111",
        url = "[REDACTED]",
        status = state,
        start_time = as.POSIXct("2026-08-14", tz = "UTC"),
        last_updated = as.POSIXct("2026-08-14", tz = "UTC"),
        error_code = if (identical(state, "Failed")) "BadRequest" else NA,
        failure_status = if (identical(state, "Failed")) "Permanent" else NA,
        message = if (identical(state, "Failed")) "schema mismatch" else NA
      )
    ),
    class = "fabric_kql_ingestion_status"
  )
}


# test-fabric_lakehouse_tables.R
lakehouse_table_test_workspace <- "11111111-2222-3333-4444-555555555555"
lakehouse_table_test_id <- "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
lakehouse_table_test_operation <- "99999999-8888-7777-6666-555555555555"

lakehouse_table_test_item <- function(default_schema = "dbo") {
  structure(
    list(
      id = lakehouse_table_test_id,
      workspaceId = lakehouse_table_test_workspace,
      displayName = "Curated",
      type = "Lakehouse",
      properties = list(defaultSchema = default_schema)
    ),
    class = "fabric_item"
  )
}


# test-fabric_livy.R
livy_test_credential <- function() {
  fabric_credential(token = "token")
}


# test-fabric_onelake_files.R
onelake_test_response <- function(
  status = 200L,
  body = raw(),
  headers = list(),
  url = "https://onelake.dfs.fabric.microsoft.com"
) {
  if (is.list(body)) {
    body <- charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
    headers[["content-type"]] <- "application/json"
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = headers,
    body = body
  )
}


# test-fabric_onelake_shortcuts.R
shortcut_test_workspace_id <- "11111111-1111-4111-8111-111111111111"
shortcut_test_item_id <- "22222222-2222-4222-8222-222222222222"
shortcut_test_target_workspace_id <- "33333333-3333-4333-8333-333333333333"
shortcut_test_target_item_id <- "44444444-4444-4444-8444-444444444444"
shortcut_test_connection_id <- "55555555-5555-4555-8555-555555555555"

shortcut_test_item <- function() {
  structure(
    list(
      id = shortcut_test_item_id,
      workspaceId = shortcut_test_workspace_id,
      displayName = "Destination",
      type = "Lakehouse"
    ),
    class = c("fabric_item", "list")
  )
}

shortcut_test_target <- function() {
  structure(
    list(
      id = shortcut_test_target_item_id,
      workspaceId = shortcut_test_target_workspace_id,
      displayName = "Source",
      type = "Warehouse"
    ),
    class = c("fabric_item", "list")
  )
}

shortcut_test_response <- function(
  body = NULL,
  status = 200L,
  url = "https://api.fabric.microsoft.com/v1/shortcuts"
) {
  raw_body <- if (is.null(body)) {
    raw()
  } else {
    charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE, null = "null"))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = list(`content-type` = "application/json"),
    body = raw_body
  )
}

shortcut_test_onelake_record <- function(
  path = "Files/shared",
  name = "orders",
  transform = FALSE
) {
  record <- list(
    path = path,
    name = name,
    target = list(
      type = "OneLake",
      oneLake = list(
        workspaceId = shortcut_test_target_workspace_id,
        itemId = shortcut_test_target_item_id,
        path = "Tables/dbo/orders"
      )
    )
  )
  if (transform) {
    record$isShortcutTransform <- TRUE
    record$transform <- list(type = "csvToDelta")
  }
  record
}


# test-fabric_operations.R
operation_test_id <- "11111111-2222-3333-4444-555555555555"

operation_test_response <- function(
  status = 200L,
  body = NULL,
  headers = list(),
  content_type = "application/json",
  url = "https://api.fabric.microsoft.com/v1/operations/test"
) {
  if (!is.null(content_type)) {
    headers[["content-type"]] <- content_type
  }
  raw_body <- if (is.null(body)) {
    raw()
  } else if (is.raw(body)) {
    body
  } else {
    charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE, null = "null"))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = headers,
    body = raw_body
  )
}

operation_test_request <- function() {
  httr2::request("https://api.fabric.microsoft.com/v1/workspaces/items") |>
    httr2::req_method("POST") |>
    httr2::req_body_json(list(displayName = "test"))
}

operation_test_clock <- function() {
  clock <- new.env(parent = emptyenv())
  clock$time <- as.POSIXct("2026-08-13 12:00:00", tz = "UTC")
  clock$delays <- numeric()
  list(
    now = function() clock$time,
    sleep = function(seconds) {
      clock$delays <- c(clock$delays, seconds)
      clock$time <- clock$time + seconds
    },
    delays = function() clock$delays
  )
}


# test-fabric_pbi_dax_query.R
# pbi_parse_connstr() -----------------------------------------------------

pbi_test_workspace_id <- "11111111-1111-4111-8111-111111111111"
pbi_test_dataset_id <- "22222222-2222-4222-8222-222222222222"


# test-fabric_pbi_refresh.R
pbi_refresh_workspace_id <- "11111111-1111-4111-8111-111111111111"
pbi_refresh_dataset_id <- "22222222-2222-4222-8222-222222222222"
pbi_refresh_id <- "33333333-3333-4333-8333-333333333333"

pbi_refresh_test_model <- function() {
  structure(
    list(
      id = pbi_refresh_dataset_id,
      workspaceId = pbi_refresh_workspace_id,
      type = "SemanticModel",
      displayName = "Refresh fixture"
    ),
    class = c("fabric_item", "list")
  )
}

pbi_refresh_test_handle <- function(
  mode = "enhanced",
  retry_after = NULL,
  my_workspace = FALSE
) {
  .pbi_refresh_handle(
    refresh_id = pbi_refresh_id,
    target = list(
      workspace_id = if (my_workspace) NULL else pbi_refresh_workspace_id,
      dataset_id = pbi_refresh_dataset_id,
      my_workspace = my_workspace
    ),
    credential = fabric_credential(token = "test-token"),
    api_base = "https://powerbi.test/v1.0/myorg",
    allow_custom_endpoint = TRUE,
    retry_after = retry_after,
    mode = mode
  )
}


# test-fabric_warehouse_tables.R
warehouse_write_test_workspace_id <- "11111111-1111-4111-8111-111111111111"
warehouse_write_test_warehouse_id <- "22222222-2222-4222-8222-222222222222"
warehouse_write_test_lakehouse_id <- "33333333-3333-4333-8333-333333333333"

warehouse_write_test_warehouse <- function() {
  structure(
    list(
      id = warehouse_write_test_warehouse_id,
      workspaceId = warehouse_write_test_workspace_id,
      displayName = "TestWarehouse",
      type = "Warehouse",
      sql_server = "test.datawarehouse.fabric.microsoft.com",
      sql_database = "TestWarehouse"
    ),
    class = c("fabric_item", "list")
  )
}

warehouse_write_test_lakehouse <- function() {
  structure(
    list(
      id = warehouse_write_test_lakehouse_id,
      workspaceId = warehouse_write_test_workspace_id,
      displayName = "TestLakehouse",
      type = "Lakehouse",
      workspaceOneLakeDfsEndpoint = paste0(
        "https://",
        warehouse_write_test_workspace_id,
        ".z12.dfs.fabric.microsoft.com"
      )
    ),
    class = c("fabric_item", "list")
  )
}


# test-http-auth.R
json_response <- function(
  status = 200L,
  body = list(ok = TRUE),
  headers = list(),
  url = "https://example.com"
) {
  headers[["content-type"]] <- "application/json"
  httr2::response(
    status_code = status,
    url = url,
    headers = headers,
    body = charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
  )
}

fake_azure_token <- function(access_token = "azure-token", valid = TRUE) {
  class <- R6::R6Class(
    "FabricQueryRFakeAzureToken",
    inherit = AzureAuth::AzureToken,
    public = list(
      refreshes = 0L,
      valid = NULL,
      initialize = function() {
        self$credentials <- list(access_token = access_token)
        self$valid <- valid
      },
      validate = function() self$valid,
      refresh = function() {
        self$refreshes <- self$refreshes + 1L
        self$valid <- TRUE
        self$credentials$access_token <- paste0(
          access_token,
          "-refreshed"
        )
        invisible(self)
      }
    )
  )
  class$new()
}


# test-integration-fabric-onelake-tables.R
# Fabric integration coverage: schema-aware table metadata and managed
# CSV/Parquet loads through the Lakehouse preview APIs

fabric_test_use_table_delta_runtime <- function() {
  if (
    reticulate::py_available(initialize = FALSE) ||
      nzchar(Sys.getenv("RETICULATE_PYTHON"))
  ) {
    return(invisible(NULL))
  }
  root <- fabric_test_delta_oracle_root()
  relative <- if (.Platform$OS.type == "windows") {
    file.path(".venv", "Scripts", "python.exe")
  } else {
    file.path(".venv", "bin", "python")
  }
  python <- file.path(root, relative)
  fabric_test_skip_or_fail(
    !file.exists(python),
    "The locked delta-rs Python environment has not been installed"
  )
  Sys.setenv(
    RETICULATE_PYTHON = normalizePath(
      python,
      winslash = "/",
      mustWork = TRUE
    )
  )
  invisible(NULL)
}

fabric_test_lakehouse_table_target <- function(manifest, lakehouse) {
  structure(
    utils::modifyList(
      lakehouse,
      list(
        workspaceId = manifest$workspace_id,
        properties = list(defaultSchema = lakehouse$schema)
      )
    ),
    class = "fabric_item"
  )
}


# test-integration-fabric-onelake.R
# Fabric integration coverage: OneLake files and the production delta-rs path

fabric_test_use_delta_runtime <- function() {
  if (
    reticulate::py_available(initialize = FALSE) ||
      nzchar(Sys.getenv("RETICULATE_PYTHON"))
  ) {
    return(invisible(NULL))
  }
  root <- fabric_test_delta_oracle_root()
  relative <- if (.Platform$OS.type == "windows") {
    file.path(".venv", "Scripts", "python.exe")
  } else {
    file.path(".venv", "bin", "python")
  }
  python <- file.path(root, relative)
  fabric_test_skip_or_fail(
    !file.exists(python),
    "The locked delta-rs Python environment has not been installed"
  )
  Sys.setenv(
    RETICULATE_PYTHON = normalizePath(
      python,
      winslash = "/",
      mustWork = TRUE
    )
  )
  invisible(NULL)
}

fabric_test_read_delta <- function(
  manifest,
  item,
  table,
  schema = item$schema %||% "dbo",
  ...
) {
  testthat::expect_no_warning(
    fabric_onelake_read_delta_table(
      table_path = table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = item$id,
      schema = schema,
      token = fabric_test_token_provider(),
      verbose = FALSE,
      ...
    )
  )
}

fabric_test_order_delta_rows <- function(value, feature) {
  key <- intersect(c("id", "row_id", "event_id"), names(value))
  expect_true(
    length(key) == 1L,
    label = paste(feature, "has one stable id, row_id, or event_id column")
  )
  key_values <- value[[key]]
  integer_text <- is.character(key_values) &&
    all(
      is.na(key_values) | grepl("^[+-]?[0-9]+$", key_values)
    )
  if (integer_text) {
    key_values <- bit64::as.integer64(key_values)
  }
  value[order(key_values, na.last = TRUE), , drop = FALSE]
}

fabric_test_canonicalize_delta_maps <- function(value) {
  if (is.data.frame(value)) {
    if (identical(names(value), c("key", "value")) && nrow(value) > 1L) {
      labels <- vapply(
        seq_len(nrow(value)),
        function(index) {
          key <- value$key
          key <- if (is.data.frame(key)) {
            key[index, , drop = FALSE]
          } else {
            key[[index]]
          }
          if (is.data.frame(key)) {
            rownames(key) <- NULL
          }
          paste(capture.output(dput(key)), collapse = "\n")
        },
        character(1)
      )
      value <- value[order(labels), , drop = FALSE]
    }
    for (name in names(value)) {
      value[[name]] <- fabric_test_canonicalize_delta_maps(value[[name]])
    }
    rownames(value) <- NULL
    return(value)
  }
  if (is.list(value)) {
    return(lapply(value, fabric_test_canonicalize_delta_maps))
  }
  value
}

fabric_test_arrow_scalar_text <- function(value) {
  if (inherits(value, "POSIXt")) {
    return(format(value, "%Y-%m-%d %H:%M:%OS6", tz = "UTC"))
  }
  as.character(value)
}

fabric_test_expect_arrow_scalar_values <- function(
  actual,
  expected,
  feature
) {
  key <- intersect(c("id", "row_id"), names(expected))
  if (length(key) == 1L && nrow(expected)) {
    key_values <- actual$GetColumnByName(key[[1L]])$as_vector()
    indices <- order(key_values, na.last = TRUE) - 1L
    actual <- actual$Take(arrow::Array$create(as.integer(indices)))
    expected <- expected[
      order(expected[[key]], na.last = TRUE),
      ,
      drop = FALSE
    ]
  }
  actual_frame <- as.data.frame(actual)
  scalar <- names(expected)[vapply(
    expected,
    function(column) is.atomic(column) && !is.raw(column),
    logical(1)
  )]
  expect_gt(length(scalar), 0L, label = paste(feature, "scalar columns"))
  for (name in scalar) {
    actual_column <- actual$GetColumnByName(name)
    timestamp_text <- is.character(expected[[name]]) &&
      grepl("^timestamp\\[", actual_column$type$ToString())
    actual_value <- if (timestamp_text) {
      actual_column$cast(arrow::utf8())$as_vector()
    } else {
      actual_frame[[name]]
    }
    expected_value <- if (timestamp_text) {
      sub("T", " ", expected[[name]], fixed = TRUE)
    } else {
      expected[[name]]
    }
    expect_identical(
      fabric_test_arrow_scalar_text(actual_value),
      fabric_test_arrow_scalar_text(expected_value),
      label = paste(feature, name)
    )
  }
  invisible(actual_frame)
}

fabric_test_read_arrow_table <- function(
  manifest,
  item,
  table,
  ...
) {
  stream <- fabric_test_read_delta(
    manifest,
    item,
    table,
    result = "arrow_stream",
    ...
  )
  expect_s3_class(stream, "nanoarrow_array_stream")
  arrow::as_record_batch_reader(stream)$read_table()
}

fabric_test_order_arrow_rows <- function(value, feature) {
  key <- intersect(c("id", "row_id"), value$ColumnNames())
  if (!value$num_rows) {
    return(value)
  }
  expect_true(
    length(key) == 1L,
    label = paste(feature, "Arrow table has one stable key")
  )
  key_values <- value$GetColumnByName(key[[1L]])$as_vector()
  indices <- order(key_values, na.last = TRUE) - 1L
  value$Take(arrow::Array$create(as.integer(indices)))
}

fabric_test_arrow_column_equals <- function(actual, expected) {
  if (!identical(actual$type$ToString(), expected$type$ToString())) {
    return(FALSE)
  }
  if (isTRUE(actual$Equals(expected))) {
    return(TRUE)
  }
  identical(
    fabric_test_canonicalize_delta_maps(actual$as_vector()),
    fabric_test_canonicalize_delta_maps(expected$as_vector())
  )
}

fabric_test_spark_logical_oracle <- function(manifest, lakehouse) {
  payload <- fabric_onelake_download(
    manifest$workspace_id,
    lakehouse$id,
    "Files/fixtures/delta-reader-spark-oracle.json",
    token = fabric_test_token_provider()
  )
  jsonlite::fromJSON(rawToChar(payload), simplifyVector = FALSE)
}

fabric_test_spark_type_name <- function(data_type) {
  if (is.character(data_type)) {
    return(data_type)
  }
  data_type$type
}

fabric_test_spark_canonical_primitive <- function(value, data_type) {
  type_name <- fabric_test_spark_type_name(data_type)
  if (identical(type_name, "binary")) {
    if (is.null(value)) {
      return(NULL)
    }
    return(jsonlite::base64_enc(value))
  }
  if (
    is.null(value) ||
      !length(value) ||
      (length(value) == 1L && isTRUE(is.na(value)))
  ) {
    return(NULL)
  }
  if (identical(type_name, "boolean")) {
    return(as.logical(value))
  }
  if (type_name %in% c("float", "double")) {
    return(as.numeric(value))
  }
  if (identical(type_name, "date")) {
    return(as.character(value))
  }
  if (type_name %in% c("timestamp", "timestamp_ntz")) {
    text <- if (inherits(value, "POSIXt")) {
      format(value, "%Y-%m-%d %H:%M:%OS6", tz = "UTC")
    } else {
      sub("T", " ", as.character(value), fixed = TRUE)
    }
    text <- sub("(\\.[0-9]*[1-9])0+$", "\\1", text)
    return(sub("\\.0+$", "", text))
  }
  if (
    type_name %in%
      c("byte", "short", "integer", "long") ||
      startsWith(type_name, "decimal(")
  ) {
    return(as.character(value))
  }
  as.character(value)
}

fabric_test_spark_canonical_cell <- function(column, data_type, index) {
  type_name <- fabric_test_spark_type_name(data_type)
  if (identical(type_name, "struct")) {
    if (is.null(column) || isTRUE(is.na(column)[[index]])) {
      return(NULL)
    }
    fields <- data_type$fields
    values <- lapply(fields, function(field) {
      fabric_test_spark_canonical_cell(
        column[[field$name]],
        field$type,
        index
      )
    })
    return(stats::setNames(
      values,
      vapply(fields, `[[`, character(1), "name")
    ))
  }
  if (type_name %in% c("array", "map")) {
    return(fabric_test_spark_canonical_value(column[[index]], data_type))
  }
  if (identical(type_name, "binary") && is.list(column)) {
    return(fabric_test_spark_canonical_primitive(
      column[[index]],
      data_type
    ))
  }
  fabric_test_spark_canonical_primitive(column[index], data_type)
}

fabric_test_spark_canonical_value <- function(value, data_type) {
  type_name <- fabric_test_spark_type_name(data_type)
  if (identical(type_name, "array")) {
    if (is.null(value)) {
      return(NULL)
    }
    size <- vctrs::vec_size(value)
    return(lapply(seq_len(size), function(index) {
      fabric_test_spark_canonical_cell(
        value,
        data_type$elementType,
        index
      )
    }))
  }
  if (identical(type_name, "map")) {
    if (is.null(value)) {
      return(NULL)
    }
    entries <- lapply(seq_len(vctrs::vec_size(value)), function(index) {
      list(
        key = fabric_test_spark_canonical_cell(
          value$key,
          data_type$keyType,
          index
        ),
        value = fabric_test_spark_canonical_cell(
          value$value,
          data_type$valueType,
          index
        )
      )
    })
    if (length(entries) > 1L) {
      labels <- vapply(
        entries,
        function(entry) {
          jsonlite::toJSON(
            list(value = entry$key),
            auto_unbox = TRUE,
            null = "null",
            digits = NA
          )
        },
        character(1)
      )
      entries <- entries[order(labels)]
    }
    return(entries)
  }
  if (identical(type_name, "struct")) {
    return(fabric_test_spark_canonical_cell(value, data_type, 1L))
  }
  fabric_test_spark_canonical_primitive(value, data_type)
}

fabric_test_spark_canonical_rows <- function(value, schema) {
  fields <- schema$fields
  lapply(seq_len(nrow(value)), function(index) {
    values <- lapply(fields, function(field) {
      fabric_test_spark_canonical_cell(
        value[[field$name]],
        field$type,
        index
      )
    })
    stats::setNames(
      values,
      vapply(fields, `[[`, character(1), "name")
    )
  })
}

fabric_test_expect_arrow_matches_reference <- function(
  manifest,
  lakehouse,
  source,
  reference,
  feature,
  columns = NULL
) {
  actual <- fabric_test_read_arrow_table(
    manifest,
    lakehouse,
    source,
    columns = columns
  )
  expected <- fabric_test_read_arrow_table(
    manifest,
    lakehouse,
    reference,
    columns = columns
  )
  expect_identical(
    actual$ColumnNames(),
    expected$ColumnNames(),
    label = paste(feature, "Arrow columns")
  )
  expect_identical(
    actual$num_rows,
    expected$num_rows,
    label = paste(feature, "Arrow rows")
  )
  actual <- fabric_test_order_arrow_rows(actual, feature)
  expected <- fabric_test_order_arrow_rows(
    expected,
    paste(feature, "feature-neutral reference")
  )
  for (name in actual$ColumnNames()) {
    expect_true(
      fabric_test_arrow_column_equals(
        actual$GetColumnByName(name),
        expected$GetColumnByName(name)
      ),
      label = paste(feature, name, "logical Arrow equality")
    )
  }
  invisible(actual)
}

fabric_test_delta_differences <- function(actual, expected, feature) {
  waldo::compare(
    actual,
    expected,
    x_arg = feature,
    y_arg = paste(feature, "feature-neutral reference"),
    max_diffs = 10L
  )
}

fabric_test_expect_no_delta_differences <- function(differences) {
  if (length(differences)) {
    testthat::fail(paste(differences, collapse = "\n"))
  }
  invisible(NULL)
}


# test-fabric_lakehouse_tables.R additional fixtures
lakehouse_table_test_response <- function(
  body = NULL,
  status = 200L,
  headers = list(),
  url = "https://onelake.table.fabric.microsoft.com/test"
) {
  headers[["content-type"]] <- "application/json"
  raw_body <- if (is.null(body)) {
    raw()
  } else {
    charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE, null = "null"))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = headers,
    body = raw_body
  )
}

lakehouse_table_test_clock <- function() {
  clock <- new.env(parent = emptyenv())
  clock$time <- as.POSIXct("2026-08-13 12:00:00", tz = "UTC")
  clock$delays <- numeric()
  list(
    now = function() clock$time,
    sleep = function(seconds) {
      clock$delays <- c(clock$delays, seconds)
      clock$time <- clock$time + seconds
    },
    delays = function() clock$delays
  )
}


# test-integration-fabric-onelake.R additional fixtures
# Both sides use the production bridge. This comparison isolates protocol
# feature handling by comparing a feature-bearing table with a Spark-materialized
# neutral table; independent static assertions cover bridge conversion.
fabric_test_expect_delta_matches_reference <- function(
  manifest,
  lakehouse,
  source,
  reference,
  feature
) {
  actual <- fabric_test_read_delta(manifest, lakehouse, source)
  expected <- fabric_test_read_delta(manifest, lakehouse, reference)

  expect_s3_class(actual, "tbl_df")
  expect_s3_class(expected, "tbl_df")
  expect_named(actual, names(expected), label = feature)
  expect_equal(nrow(actual), nrow(expected), label = feature)
  expect_gt(nrow(actual), 0L, label = feature)

  actual <- fabric_test_order_delta_rows(actual, feature)
  expected <- fabric_test_order_delta_rows(
    expected,
    paste(feature, "feature-neutral reference")
  )
  actual <- fabric_test_canonicalize_delta_maps(actual)
  expected <- fabric_test_canonicalize_delta_maps(expected)
  rownames(actual) <- NULL
  rownames(expected) <- NULL
  differences <- fabric_test_delta_differences(actual, expected, feature)
  if (length(differences)) {
    fail(
      paste(
        c(
          paste(
            "Delta result differs from its feature-neutral reference:",
            feature
          ),
          differences
        ),
        collapse = "\n"
      )
    )
  }
  invisible(actual)
}


# test-integration-fabric-sql.R additional fixtures
fabric_test_sql_item <- function(name, backend) {
  manifest <- fabric_test_manifest()
  api_token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  sql_token <- fabric_test_token("FABRIC_TEST_SQL_TOKEN")

  provisioned <- fabric_test_manifest_item(manifest, name)
  table_name <- provisioned$tables$types
  if (is.null(table_name)) {
    rlang::abort(sprintf(
      "Fabric integration manifest has no typed SQL table for '%s'",
      name
    ))
  }
  context <- paste(name, backend)
  target <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = provisioned$type,
    token = api_token
  )
  info <- fabric_sql_connection_info(target)
  expect_equal(info$database, provisioned$database_name, info = context)
  expect_equal(info$source, "discovery", info = context)
  expect_equal(
    info$target_type,
    if (identical(name, "TestWarehouse")) "warehouse" else "sql_database",
    info = context
  )

  con <- fabric_sql_connect(
    target,
    backend = backend,
    token = sql_token,
    read_only = TRUE,
    verbose = FALSE
  )
  connected <- TRUE
  on.exit(
    if (connected) {
      if (identical(backend, "adbc")) {
        DBI::dbDisconnect(con, force = TRUE)
      } else {
        DBI::dbDisconnect(con)
      }
    },
    add = TRUE
  )
  expect_true(DBI::dbIsValid(con), info = context)
  expect_true(
    DBI::dbExistsTable(
      con,
      DBI::Id(schema = "dbo", table = table_name)
    ),
    info = context
  )
  catalog <- DBI::dbGetQuery(con, "SELECT DB_NAME() AS database_name")
  expect_equal(
    catalog$database_name,
    provisioned$database_name,
    info = context
  )
  rows <- DBI::dbGetQuery(
    con,
    sprintf(
      paste(
        "SELECT id, name, amount, active, event_date,",
        "loaded_at, nullable_value",
        "FROM dbo.%s",
        "WHERE id > 0",
        "ORDER BY id"
      ),
      table_name
    )
  )
  expect_equal(rows$id, c(1L, 2L, 3L), info = context)
  expect_equal(
    rows$name,
    c("alpha", "beta", "gamma"),
    info = context
  )
  expect_equal(as.numeric(rows$amount), c(10.5, 20, NA), info = context)
  expect_equal(as.logical(rows$active), c(TRUE, FALSE, NA), info = context)
  expect_s3_class(rows$event_date, "Date")
  expect_equal(
    rows$event_date,
    as.Date(c("2026-01-01", "2026-01-02", NA)),
    info = context
  )
  expect_s3_class(rows$loaded_at, "POSIXct")
  expect_equal(
    as.numeric(rows$loaded_at),
    rep(as.numeric(as.POSIXct("2026-01-01", tz = "UTC")), 3),
    info = context
  )
  expect_equal(
    rows$nullable_value,
    c(NA, "present", NA),
    info = context
  )
  disconnected <- if (identical(backend, "adbc")) {
    DBI::dbDisconnect(con, force = TRUE)
  } else {
    DBI::dbDisconnect(con)
  }
  connected <- FALSE
  expect_true(isTRUE(disconnected), info = context)
  if (!identical(backend, "adbc")) {
    expect_false(DBI::dbIsValid(con), info = context)
  }

  bound_rows <- fabric_sql_query(
    target,
    sprintf(
      paste(
        "SELECT id, name",
        "FROM dbo.%s",
        "WHERE id >= ?",
        "ORDER BY id"
      ),
      table_name
    ),
    params = list(2L),
    backend = backend,
    token = sql_token,
    verbose = FALSE
  )
  expect_s3_class(bound_rows, "tbl_df")
  expect_equal(bound_rows$id, c(2L, 3L), info = context)
  expect_equal(bound_rows$name, c("beta", "gamma"), info = context)

  from_manifest <- fabric_sql_query(
    provisioned$connection_string,
    "SELECT CAST(? AS nvarchar(100)) AS bound_value",
    params = list("safe ' value; --"),
    backend = backend,
    database = if (identical(name, "TestWarehouse")) {
      provisioned$database_name
    } else {
      NULL
    },
    token = sql_token,
    verbose = FALSE
  )
  expect_equal(
    from_manifest$bound_value,
    "safe ' value; --",
    info = context
  )
  bare_server <- if (identical(name, "TestSQLDatabase")) {
    provisioned$server_fqdn
  } else {
    fabric_sql_connection_info(provisioned$connection_string)$server
  }
  from_server_and_database <- fabric_sql_query(
    bare_server,
    "SELECT DB_NAME() AS database_name",
    database = provisioned$database_name,
    backend = backend,
    token = sql_token,
    verbose = FALSE
  )
  expect_equal(
    from_server_and_database$database_name,
    provisioned$database_name,
    info = context
  )

  stream <- fabric_sql_query(
    target,
    sprintf(
      paste(
        "SELECT id, name, amount",
        "FROM dbo.%s",
        "WHERE id > 0",
        "ORDER BY id"
      ),
      table_name
    ),
    backend = backend,
    result = "arrow_stream",
    token = sql_token,
    verbose = FALSE
  )
  expect_s3_class(
    stream,
    "nanoarrow_array_stream"
  )
  streamed <- as.data.frame(nanoarrow::collect_array_stream(stream))
  expect_equal(streamed$id, c(1L, 2L, 3L), info = context)
  expect_equal(
    streamed$name,
    c("alpha", "beta", "gamma"),
    info = context
  )
  expect_equal(as.numeric(streamed$amount), c(10.5, 20, NA), info = context)

  list(
    id = as.integer(rows$id),
    name = as.character(rows$name),
    amount = as.numeric(rows$amount),
    active = as.logical(rows$active),
    event_date = as.character(rows$event_date),
    nullable_value = as.character(rows$nullable_value)
  )
}
