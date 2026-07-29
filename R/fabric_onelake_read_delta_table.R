.fabric_delta_max_exact_version <- 2^53
.fabric_delta_max_exact_version_text <- "00009007199254740992"
.fabric_delta_engines <- c("delta-rs", "R")
.fabric_delta_result_types <- c("tibble", "arrow_stream")

#' Read a Delta table from Microsoft Fabric OneLake
#'
#' @description
#' Reads a Delta table from a Fabric Lakehouse or Warehouse export. By default,
#' the result is returned as a tibble, so it works like a regular R data frame.
#'
#' @details
#' **Choosing an engine**
#'
#' - `"delta-rs"` is the default and is the best choice for most tables. It
#'   reads OneLake directly with the native
#'   [`delta-rs`](https://github.com/delta-io/delta-rs) library.
#' - `"R"` selects the package's original R and DuckDB reader. Use it for V2
#'   checkpoints, type widening, shredded Variant data, or when you want to
#'   keep a local copy of the table files in `dest_dir`.
#'
#' If the selected engine cannot read a table feature, the function returns a
#' clear error. It never silently switches engines.
#'
#' **Choosing a result**
#'
#' `result = "tibble"` is simplest for normal R analysis. Use
#' `result = "arrow_stream"` to return a `nanoarrow_array_stream`, which can be
#' passed to [arrow::as_record_batch_reader()] or another Arrow C stream
#' consumer without first collecting the table as a tibble.
#'
#' **Technical notes**
#'
#' Both engines use the same OneLake target resolution, authentication, input
#' validation, and return formats. The native engine pushes `columns` and
#' `limit` into DataFusion and exchanges data with R through Arrow IPC. The R
#' engine stages the Delta log and active Parquet files, then reads them with
#' DuckDB.
#'
#' Delta `long` values are returned as `bit64::integer64`. Delta decimals are
#' returned as character vectors so all digits remain exact. Other conversions
#' follow the selected engine's Arrow or DuckDB conversion.
#'
#' Tokens use the `https://storage.azure.com/.default` audience. Give the
#' signed-in user or application Read access through a workspace role or
#' **Lakehouse > Manage OneLake data access**.
#'
#' @param table_path Table name, for example `"PatientInfo"`. A nested string
#'   is accepted for backward compatibility, but only its final segment is
#'   used; select a schema with `schema`.
#' @param workspace_name Fabric workspace display name or GUID, or a record
#'   from [fabric_workspaces()].
#' @param lakehouse_name Lakehouse or Warehouse item name, GUID, or discovery
#'   record. The argument name is retained for backward compatibility.
#' @param schema Lakehouse schema name, for example `"dbo"`, or `NULL`.
#'   A discovered schema-enabled Lakehouse supplies its default schema
#'   automatically.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function.
#' @param auth_args Named list passed to [AzureAuth::get_azure_token()] when no
#'   token source is supplied.
#' @param version Optional non-negative Delta transaction version. `NULL` reads
#'   the latest snapshot. Versions through `2^53` are represented exactly.
#' @param dest_dir Local work directory or `NULL`. The default creates a
#'   temporary directory and removes it on exit. With `engine = "R"`, a supplied
#'   directory retains the staged transaction log and active files. With
#'   `engine = "delta-rs"`, it retains the Arrow IPC result. A supplied
#'   directory must be new or empty.
#' @param verbose Logical. Show progress.
#' @param dfs_base OneLake DFS endpoint.
#' @param columns Optional character vector of logical Delta column names to
#'   return, in the requested order.
#' @param limit Optional non-negative whole number limiting returned rows.
#' @param engine Reader implementation. `"delta-rs"` is the default native
#'   engine; `"R"` selects the original staged R and DuckDB engine.
#' @param result Return format. `"tibble"` returns a tibble.
#'   `"arrow_stream"` returns a `nanoarrow_array_stream` compatible with
#'   [arrow::as_record_batch_reader()] and other Arrow C stream consumers.
#'
#' @return With `result = "tibble"`, a tibble containing the selected Delta
#'   snapshot. With `result = "arrow_stream"`, a
#'   `nanoarrow_array_stream`. Empty tables keep their column schema in either
#'   format.
#' @references
#' [Delta Lake protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
#'
#' [delta-rs](https://github.com/delta-io/delta-rs)
#'
#' [Connect to OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)
#' @export
#'
#' @examples
#' \dontrun{
#' df <- fabric_onelake_read_delta_table(
#'   table_path = "PatientInfo",
#'   workspace_name = "PatientsWorkspace",
#'   lakehouse_name = "Lakehouse.Lakehouse"
#' )
#'
#' # Use the original staged implementation explicitly.
#' staged <- fabric_onelake_read_delta_table(
#'   table_path = "PatientInfo",
#'   workspace_name = "PatientsWorkspace",
#'   lakehouse_name = "Lakehouse.Lakehouse",
#'   engine = "R"
#' )
#'
#' # Keep the result in an Arrow-compatible stream.
#' stream <- fabric_onelake_read_delta_table(
#'   table_path = "PatientInfo",
#'   workspace_name = "PatientsWorkspace",
#'   lakehouse_name = "Lakehouse.Lakehouse",
#'   result = "arrow_stream"
#' )
#' reader <- arrow::as_record_batch_reader(stream)
#' }
fabric_onelake_read_delta_table <- function(
  table_path,
  workspace_name,
  lakehouse_name,
  schema = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  version = NULL,
  dest_dir = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  columns = NULL,
  limit = NULL,
  engine = c("delta-rs", "R"),
  result = c("tibble", "arrow_stream")
) {
  engine <- rlang::arg_match(engine, .fabric_delta_engines)
  result <- rlang::arg_match(result, .fabric_delta_result_types)
  context <- fabric_delta_read_context(
    table_path = table_path,
    workspace_name = workspace_name,
    lakehouse_name = lakehouse_name,
    schema = schema,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    version = version,
    dest_dir = dest_dir,
    verbose = verbose,
    dfs_base = dfs_base,
    columns = columns,
    limit = limit,
    engine = engine
  )
  if (isTRUE(context$cleanup)) {
    on.exit(
      unlink(context$dest_dir, recursive = TRUE, force = TRUE),
      add = TRUE
    )
  }

  inform(verbose, "Table root: {.path {context$table_dir}}")
  inform(verbose, "Reading the Delta snapshot with the {.val {engine}} engine")
  value <- switch(
    engine,
    "delta-rs" = fabric_onelake_read_delta_table_delta_rs(context),
    "R" = fabric_onelake_read_delta_table_r(context)
  )
  row_count <- fabric_delta_result_rows(value)
  value <- fabric_delta_format_result(value, result)
  inform(verbose, "Loaded {row_count} row{?s}", type = "success")
  value
}

#' Resolve and validate input shared by both Delta reader engines
#' @keywords internal
#' @noRd
fabric_delta_read_context <- function(
  table_path,
  workspace_name,
  lakehouse_name,
  schema,
  tenant_id,
  client_id,
  token,
  auth_args,
  version,
  dest_dir,
  verbose,
  dfs_base,
  columns,
  limit,
  engine
) {
  workspace_target <- workspace_name
  workspace_record <- fabric_as_record(workspace_name)
  if (!is.null(workspace_record)) {
    workspace_name <- fabric_record_value(workspace_record, "id", "workspaceId")
  }

  lakehouse_target <- lakehouse_name
  item_type <- if (
    is.character(lakehouse_name) &&
      length(lakehouse_name) == 1L &&
      !is.na(lakehouse_name) &&
      grepl("\\.warehouse$", lakehouse_name, ignore.case = TRUE)
  ) {
    "Warehouse"
  } else {
    "Lakehouse"
  }
  lakehouse_record <- fabric_as_record(lakehouse_name)
  if (!is.null(lakehouse_record)) {
    record_type <- tolower(
      fabric_record_value(lakehouse_record, "type") %||% ""
    )
    if (!record_type %in% c("lakehouse", "warehouse")) {
      rlang::abort(
        "lakehouse_name discovery record must be a Lakehouse or Warehouse item"
      )
    }
    item_type <- if (identical(record_type, "warehouse")) {
      "Warehouse"
    } else {
      "Lakehouse"
    }
    lakehouse_name <- fabric_record_value(lakehouse_record, "id")
    schema <- schema %||%
      fabric_record_value(lakehouse_record, "default_schema", "defaultSchema")
  }

  fabric_delta_validate_read_arguments(
    table_path,
    workspace_name,
    lakehouse_name,
    schema,
    version,
    columns,
    limit
  )
  if (!is.null(version)) {
    version <- as.numeric(version)
  }
  if (!is.null(limit)) {
    limit <- as.numeric(limit)
  }

  if (is.null(token)) {
    inform(verbose, "Authenticating with {.pkg AzureAuth} (MSAL v2)")
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )

  parts <- strsplit(table_path, "/", fixed = TRUE)[[1L]]
  table_name <- parts[[length(parts)]]
  table_dir <- if (is.null(schema)) {
    paste("Tables", table_name, sep = "/")
  } else {
    paste("Tables", schema, table_name, sep = "/")
  }
  target <- onelake_resolve_target(
    workspace_target,
    lakehouse_target,
    path = table_dir,
    item_type = item_type,
    dfs_base = dfs_base
  )
  work <- fabric_delta_work_dir(dest_dir)

  list(
    target = target,
    table_dir = table_dir,
    credential = credential,
    version = version,
    dest_dir = work$path,
    cleanup = work$cleanup,
    verbose = verbose,
    columns = columns,
    limit = limit,
    engine = engine
  )
}

#' Validate public Delta reader arguments
#' @keywords internal
#' @noRd
fabric_delta_validate_read_arguments <- function(
  table_path,
  workspace_name,
  lakehouse_name,
  schema,
  version,
  columns,
  limit
) {
  scalar_string <- function(value) {
    is.character(value) &&
      length(value) == 1L &&
      !is.na(value) &&
      nzchar(value)
  }
  if (!scalar_string(table_path)) {
    rlang::abort("table_path must be one non-empty string")
  }
  if (!scalar_string(workspace_name)) {
    rlang::abort("workspace_name must be one non-empty string or record")
  }
  if (!scalar_string(lakehouse_name)) {
    rlang::abort("lakehouse_name must be one non-empty string or record")
  }
  if (!is.null(schema) && !scalar_string(schema)) {
    rlang::abort("schema must be one non-empty string")
  }
  if (
    !is.null(version) &&
      (length(version) != 1L ||
        is.na(version) ||
        !is.numeric(version) ||
        version < 0 ||
        version != floor(version) ||
        version > .fabric_delta_max_exact_version)
  ) {
    rlang::abort(paste0(
      "version must be one exactly representable non-negative integer ",
      "no greater than 2^53"
    ))
  }
  if (
    !is.null(columns) &&
      (!is.character(columns) ||
        !length(columns) ||
        anyNA(columns) ||
        !all(nzchar(columns)) ||
        anyDuplicated(columns))
  ) {
    rlang::abort(
      "columns must be NULL or one or more unique, non-empty strings"
    )
  }
  if (
    !is.null(limit) &&
      (!is.numeric(limit) ||
        length(limit) != 1L ||
        is.na(limit) ||
        !is.finite(limit) ||
        limit < 0 ||
        limit != floor(limit) ||
        limit > .fabric_delta_max_exact_version)
  ) {
    rlang::abort(
      "limit must be NULL or one exactly representable non-negative integer"
    )
  }
  invisible(NULL)
}

#' Prepare an isolated work directory shared by Delta reader engines
#' @keywords internal
#' @noRd
fabric_delta_work_dir <- function(dest_dir) {
  cleanup <- is.null(dest_dir)
  path <- dest_dir %||% tempfile("onelake_tbl_")
  if (
    !is.character(path) ||
      length(path) != 1L ||
      is.na(path) ||
      !nzchar(path)
  ) {
    rlang::abort("dest_dir must be NULL or one non-empty path")
  }
  if (dir.exists(path)) {
    entries <- list.files(
      path,
      all.files = TRUE,
      no.. = TRUE,
      full.names = TRUE
    )
    if (length(entries)) {
      rlang::abort(c(
        "dest_dir must be a new or empty directory",
        "x" = cli::format_inline(
          "{.path {path}} contains files from an earlier operation"
        )
      ))
    }
  } else if (!dir.create(path, recursive = TRUE, showWarnings = FALSE)) {
    rlang::abort(cli::format_inline(
      "Could not create Delta work directory {.path {path}}"
    ))
  }
  list(path = path, cleanup = cleanup)
}

#' Count rows before formatting a Delta reader result
#' @keywords internal
#' @noRd
fabric_delta_result_rows <- function(value) {
  if (inherits(value, "ArrowTabular")) {
    return(as.numeric(value$num_rows))
  }
  NROW(value)
}

#' Format a Delta reader result as a tibble or Arrow C stream
#' @keywords internal
#' @noRd
fabric_delta_format_result <- function(value, result) {
  if (identical(result, "arrow_stream")) {
    rlang::check_installed(
      c("arrow", "nanoarrow"),
      reason = "to return result = \"arrow_stream\""
    )
    table <- if (inherits(value, "ArrowTabular")) {
      value
    } else {
      arrow::Table$create(value)
    }
    return(nanoarrow::as_nanoarrow_array_stream(table))
  }

  if (inherits(value, "ArrowTabular")) {
    value <- as.data.frame(value)
  } else {
    value <- as.data.frame(value, optional = TRUE)
  }
  for (name in names(value)) {
    column <- value[[name]]
    if (
      inherits(column, "POSIXct") &&
        (is.null(attr(column, "tzone")) || !nzchar(attr(column, "tzone")))
    ) {
      attr(column, "tzone") <- "UTC"
      value[[name]] <- column
    }
  }
  tibble::as_tibble(value)
}

#' Convert a resolved OneLake target to a delta-rs ABFSS URI
#' @keywords internal
#' @noRd
fabric_delta_abfss_uri <- function(target) {
  endpoint <- httr2::url_parse(target$dfs_base)
  host <- endpoint$hostname %||% ""
  if (!nzchar(host)) {
    rlang::abort("The OneLake DFS endpoint has no hostname")
  }
  workspace <- utils::URLencode(target$workspace, reserved = TRUE)
  values <- c(target$item, target$path)
  values <- values[nzchar(values)]
  paste0(
    "abfss://",
    workspace,
    "@",
    host,
    "/",
    onelake_encode_path(values)
  )
}
