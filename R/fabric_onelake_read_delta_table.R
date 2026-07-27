.fabric_delta_max_exact_version <- 2^53
.fabric_delta_max_exact_version_text <- "00009007199254740992"

#' @title
#' Read a Delta table from a Microsoft Fabric Lakehouse
#'
#' @description
#' Downloads a Lakehouse Delta table from OneLake and returns it as a tibble.
#' Delta tables consist of Parquet data files plus a transaction log that says
#' which files make up the current table. This function reads that log so that
#' deleted or superseded files are not accidentally included.
#'
#' @details
#' - In Microsoft Fabric, OneLake exposes each workspace as an ADLS Gen2
#'  filesystem. Within a Lakehouse item, Delta tables are stored under
#'  `Tables/<table>` (non-schema lakehouse) or `Tables/<schema>/<table>`
#'  (schema-enabled lakehouse). The function first stages the transaction log,
#'  then downloads only the Parquet files active in the requested version.
#' - Checkpoint Parquet and data Parquet files are read with DuckDB. The staged
#'  reader supports Delta reader protocols 1 through 3, name-based column
#'  mapping, deletion vectors stored inline or in table-relative sidecar files,
#'  timestamps without time zones, and supported type widening. This covers
#'  the reader 3/writer 7 format currently emitted by Fabric Warehouse Delta
#'  export. ID-based column mapping, absolute deletion-vector paths, v2
#'  checkpoints, and unrecognised reader features are rejected with a
#'  `fabric_delta_unsupported_error` before any data is returned.
#' - The returned columns follow the logical schema in the selected Delta
#'  snapshot. Schema additions are filled with typed missing values, removed
#'  physical columns are omitted, and partition values come from Delta add-file
#'  actions rather than being inferred from directory names.
#' - Schema-enabled lakehouses (the default for new lakehouses) organise
#'  tables into named schemas. If the Fabric Lakehouse explorer shows the table
#'  under a schema such as `dbo`, supply that name in `schema`.
#' - Give the signed-in user or application Read access through a workspace role
#'  or **Lakehouse > Manage OneLake data access**.
#' - Tokens use the `https://storage.azure.com/.default` audience.
#' - \pkg{AzureAuth} is used to acquire the token. Be wary of
#'  caching behavior; you may want to call [AzureAuth::clean_token_directory()]
#'  if the wrong account or tenant is being reused.
#' - The active files are downloaded locally and the final table is collected
#'  into R memory. `columns` and `limit` can reduce the data read by DuckDB and
#'  materialised in R, but they do not reduce the active Parquet files
#'  downloaded from OneLake. For very large tables, a SQL query that filters
#'  rows in Fabric may transfer much less data.
#'
#' @param table_path Table name, for example `"PatientInfo"`. For backward
#'   compatibility a nested string is accepted, but only its final segment is
#'   used; select a schema with `schema`, not by adding it to `table_path`.
#' @param workspace_name Fabric workspace display name or GUID, or a row from
#'   [fabric_workspaces()]. GUIDs are safest for scheduled code and names are
#'   convenient interactively.
#' @param lakehouse_name Lakehouse item name or GUID, or a row from
#'   [fabric_lakehouses()]. A character name may include the `.Lakehouse`
#'   suffix; a discovered row avoids suffix and renaming ambiguity.
#' @param schema Lakehouse schema name, for example `"dbo"`, or `NULL`.
#'   When supplied, the table is resolved under `Tables/<schema>/<table>`
#'   instead of `Tables/<table>`. When `lakehouse_name` is a discovered
#'   schema-enabled Lakehouse and `schema` is `NULL`, its `defaultSchema` is
#'   used automatically. Use `NULL` with a name or GUID for a non-schema
#'   Lakehouse. Schema support in this reader is experimental.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function. With `NULL`, `AzureAuth` reuses a matching cached
#'   token or starts its normal interactive login flow.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied.
#' @param version Optional non-negative Delta transaction version. `NULL` reads
#'   the latest snapshot; supplying a version provides time travel when that
#'   version and its active files are still available in OneLake. Versions
#'   through `2^53` are represented exactly; larger versions are rejected.
#' @param dest_dir Local staging directory for the Delta log and active data
#'   files, or `NULL`. The default creates a temporary directory and removes it
#'   on exit. Supply a directory to retain the downloaded files for inspection
#'   or reuse, and ensure it has enough free space.
#' @param verbose Logical. Show download and read progress.
#' @param dfs_base OneLake DFS endpoint. Keep the default unless using a
#'   regional or workspace-private endpoint.
#' @param columns Optional character vector of logical Delta column names to
#'   return, in the requested order. `NULL` returns every column.
#' @param limit Optional non-negative whole number limiting returned rows.
#'   `NULL` returns every row. This limits DuckDB collection but not OneLake
#'   file downloads.
#'
#' @return A tibble containing the rows and logical schema of the selected Delta
#'   snapshot. An empty table returns a zero-row tibble. Delta/R type conversion
#'   follows DuckDB; schema evolution is applied and partition values are
#'   included as columns.
#' @references
#' [Connect to OneLake with ADLS APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)
#'
#' [Lakehouse schemas](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas)
#'
#' [Delta Lake tables in OneLake](https://learn.microsoft.com/en-us/fabric/fundamentals/delta-lake-interoperability)
#' @export
#'
#' @examples
#' # Example is not executed since it requires configured credentials for Fabric
#' \dontrun{
#' df <- fabric_onelake_read_delta_table(
#'   table_path     = "PatientInfo",
#'   workspace_name = "PatientsWorkspace",
#'   lakehouse_name = "Lakehouse.Lakehouse",
#'   tenant_id      = Sys.getenv("FABRICQUERYR_TENANT_ID"),
#'   client_id      = Sys.getenv("FABRICQUERYR_CLIENT_ID")
#' )
#' dplyr::glimpse(df)
#'
#' # Schema-enabled lakehouse: read from Tables/dbo/PatientInfo
#' df2 <- fabric_onelake_read_delta_table(
#'   table_path     = "PatientInfo",
#'   workspace_name = "PatientsWorkspace",
#'   lakehouse_name = "Lakehouse.Lakehouse",
#'   schema         = "dbo",
#'   columns        = c("PatientId", "Status"),
#'   limit          = 1000
#' )
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
  limit = NULL
) {
  workspace_target <- workspace_name
  workspace_record <- fabric_as_record(workspace_name)
  if (!is.null(workspace_record)) {
    workspace_name <- fabric_record_value(workspace_record, "id", "workspaceId")
  }
  lakehouse_target <- lakehouse_name
  lakehouse_record <- fabric_as_record(lakehouse_name)
  if (!is.null(lakehouse_record)) {
    if (
      !identical(
        tolower(fabric_record_value(lakehouse_record, "type") %||% ""),
        "lakehouse"
      )
    ) {
      rlang::abort(
        "lakehouse_name discovery record must be a Lakehouse item"
      )
    }
    lakehouse_name <- fabric_record_value(lakehouse_record, "id")
    schema <- schema %||%
      fabric_record_value(
        lakehouse_record,
        "default_schema",
        "defaultSchema"
      )
  }
  # ---- validate args ----
  if (
    !is.character(table_path) ||
      length(table_path) != 1L ||
      is.na(table_path) ||
      !nzchar(table_path)
  ) {
    rlang::abort("table_path must be one non-empty string")
  }
  if (
    !is.character(workspace_name) ||
      length(workspace_name) != 1L ||
      is.na(workspace_name) ||
      !nzchar(workspace_name)
  ) {
    rlang::abort("workspace_name must be one non-empty string or record")
  }
  if (
    !is.character(lakehouse_name) ||
      length(lakehouse_name) != 1L ||
      is.na(lakehouse_name) ||
      !nzchar(lakehouse_name)
  ) {
    rlang::abort("lakehouse_name must be one non-empty string or record")
  }
  if (!is.null(version)) {
    if (
      length(version) != 1L ||
        is.na(version) ||
        !is.numeric(version) ||
        version < 0 ||
        version != floor(version) ||
        version > .fabric_delta_max_exact_version
    ) {
      rlang::abort(paste0(
        "version must be one exactly representable non-negative integer ",
        "no greater than 2^53"
      ))
    }
    version <- as.numeric(version)
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
  if (!is.null(limit)) {
    limit <- as.numeric(limit)
  }

  # ---- deps ----
  rlang::check_installed(
    c(
      "DBI",
      "duckdb",
      "fs"
    ),
    reason = "to read OneLake Delta tables"
  )

  # ---- auth (MSAL v2 + refresh) ----
  if (is.null(token)) {
    inform(verbose, "Authenticating with {.pkg AzureAuth} (MSAL v2)")
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  # ---- normalize lakehouse item + table dir ----
  parts <- strsplit(table_path, "/", fixed = TRUE)[[1]]
  table_name <- parts[length(parts)]

  if (!is.null(schema)) {
    if (
      !is.character(schema) ||
        length(schema) != 1L ||
        is.na(schema) ||
        !nzchar(schema)
    ) {
      rlang::abort("schema must be one non-empty string")
    }
    table_dir <- paste("Tables", schema, table_name, sep = "/")
  } else {
    table_dir <- paste("Tables", table_name, sep = "/")
  }
  target <- onelake_resolve_target(
    workspace_target,
    lakehouse_target,
    path = table_dir,
    item_type = "Lakehouse",
    dfs_base = dfs_base
  )

  inform(verbose, "Table root: {.path {table_dir}}")

  auto_cleanup <- is.null(dest_dir)
  dest_dir <- dest_dir %||% fs::file_temp("onelake_tbl_")
  fs::dir_create(dest_dir, recurse = TRUE)
  if (auto_cleanup) {
    on.exit(try(fs::dir_delete(dest_dir), silent = TRUE), add = TRUE)
  }

  # ---- locate and stage the transaction log ----
  log_target <- target
  log_target$path <- paste0(table_dir, "/_delta_log")
  last_checkpoint <- if (is.null(version)) {
    fabric_delta_last_checkpoint_version(log_target, credential)
  } else {
    NULL
  }
  files <- onelake_list_target(
    log_target,
    credential,
    recursive = FALSE,
    page_size = 5000L,
    begin_from = if (!is.null(last_checkpoint)) {
      sprintf("%020.0f", last_checkpoint)
    }
  )
  files <- fabric_delta_file_rows(files)
  if (NROW(files) > 0) {
    file_paths <- if ("path" %in% names(files)) files$path else files$name
  } else {
    file_paths <- character()
  }
  selection <- if (length(file_paths)) {
    fabric_delta_select_log_paths(file_paths, version = version)
  } else {
    NULL
  }
  checkpoint_missing <- !is.null(last_checkpoint) &&
    (is.null(selection) ||
      is.null(selection$checkpoint_version) ||
      selection$checkpoint_version < last_checkpoint)
  if (checkpoint_missing) {
    files <- onelake_list_target(
      log_target,
      credential,
      recursive = FALSE,
      page_size = 5000L
    )
    files <- fabric_delta_file_rows(files)
    file_paths <- if (NROW(files) > 0 && "path" %in% names(files)) {
      files$path
    } else if (NROW(files) > 0) {
      files$name
    } else {
      character()
    }
    selection <- if (length(file_paths)) {
      fabric_delta_select_log_paths(file_paths, version = version)
    } else {
      NULL
    }
  }
  if (NROW(files) == 0) {
    rlang::abort(cli::format_inline(
      "No {.path _delta_log} files found under {.path {table_dir}}"
    ))
  }
  if (is.null(selection) || !length(selection$paths)) {
    rlang::abort("No supported Delta commits or checkpoints were found")
  }

  log_staged <- fabric_delta_stage_paths(
    selection$paths,
    table_dir,
    dest_dir
  )
  if (!all(grepl("^_delta_log/", log_staged$relative))) {
    rlang::abort(cli::format_inline(
      "OneLake returned a file outside the requested {.path _delta_log}"
    ))
  }

  inform(
    verbose,
    "Downloading {nrow(log_staged)} Delta log file{?s} to {.path {dest_dir}}"
  )
  fabric_delta_download_staged(
    log_staged,
    target,
    credential
  )

  snapshot <- fabric_delta_resolve_snapshot(dest_dir, version = version)
  if (length(snapshot$active)) {
    data_sources <- paste0(
      table_dir,
      "/",
      utils::URLdecode(snapshot$active)
    )
    data_staged <- fabric_delta_stage_paths(
      unique(data_sources),
      table_dir,
      dest_dir
    )
    inform(
      verbose,
      "Downloading {nrow(data_staged)} active data file{?s}"
    )
    fabric_delta_download_staged(
      data_staged,
      target,
      credential
    )
  }
  deletion_vector_paths <- fabric_delta_deletion_vector_paths(snapshot)
  if (length(deletion_vector_paths)) {
    deletion_sources <- paste0(table_dir, "/", deletion_vector_paths)
    deletion_staged <- fabric_delta_stage_paths(
      unique(deletion_sources),
      table_dir,
      dest_dir
    )
    inform(
      verbose,
      "Downloading {nrow(deletion_staged)} deletion-vector sidecar file{?s}"
    )
    fabric_delta_download_staged(
      deletion_staged,
      target,
      credential
    )
  }

  # ---- read the requested Delta snapshot ----
  inform(verbose, "Reading the Delta snapshot with {.pkg duckdb}")
  df <- fabric_delta_read_staged(
    dest_dir,
    version = version,
    columns = columns,
    limit = limit
  )

  inform(verbose, "Loaded {nrow(df)} row{?s}", type = "success")
  tibble::as_tibble(df)
}

#' Read the recent checkpoint pointer without listing the full Delta log
#' @keywords internal
#' @noRd
fabric_delta_last_checkpoint_version <- function(log_target, credential) {
  checkpoint_target <- log_target
  checkpoint_target$path <- paste0(log_target$path, "/_last_checkpoint")
  response <- .httr2_perform(
    onelake_request(onelake_path_url(checkpoint_target)),
    credential = credential,
    audience = .fabric_audience$storage,
    accepted_status = 404L
  )
  if (identical(httr2::resp_status(response), 404L)) {
    return(NULL)
  }
  checkpoint <- tryCatch(
    jsonlite::fromJSON(
      rawToChar(httr2::resp_body_raw(response)),
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    ),
    error = function(error) NULL
  )
  checkpoint_version <- checkpoint$version %||% NULL
  if (
    length(checkpoint_version) != 1L ||
      !is.numeric(checkpoint_version) ||
      is.na(checkpoint_version) ||
      !is.finite(checkpoint_version) ||
      checkpoint_version < 0 ||
      checkpoint_version != floor(checkpoint_version)
  ) {
    return(NULL)
  }
  if (checkpoint_version > .fabric_delta_max_exact_version) {
    fabric_delta_abort_version_range()
  }
  as.numeric(checkpoint_version)
}

#' Parse zero-padded Delta versions without silently losing precision
#' @keywords internal
#' @noRd
fabric_delta_versions_from_text <- function(versions) {
  if (
    !all(grepl("^[0-9]{20}$", versions)) ||
      any(versions > .fabric_delta_max_exact_version_text)
  ) {
    fabric_delta_abort_version_range()
  }
  as.numeric(versions)
}

#' Reject Delta versions outside R's exact numeric range
#' @keywords internal
#' @noRd
fabric_delta_abort_version_range <- function() {
  rlang::abort(
    paste0(
      "Delta versions greater than 2^53 are not supported because R cannot ",
      "represent them exactly"
    ),
    class = "fabric_delta_unsupported_error"
  )
}

#' Select the checkpoint and commits required for one Delta snapshot
#' @keywords internal
#' @noRd
fabric_delta_select_log_paths <- function(paths, version = NULL) {
  filenames <- basename(paths)
  json_match <- regexec("^([0-9]{20})\\.json$", filenames)
  json_parts <- regmatches(filenames, json_match)
  json_keep <- lengths(json_parts) > 0L
  json_versions <- fabric_delta_versions_from_text(vapply(
    json_parts[json_keep],
    `[[`,
    character(1),
    2L
  ))
  json_paths <- paths[json_keep]
  checkpoints <- fabric_delta_checkpoint_sets(paths)
  checkpoint_versions <- vapply(
    checkpoints,
    `[[`,
    numeric(1),
    "version"
  )
  available <- c(json_versions, checkpoint_versions)
  if (!length(available)) {
    return(NULL)
  }

  latest <- max(available)
  target <- version %||% latest
  if (target > latest) {
    rlang::abort(cli::format_inline(
      paste0(
        "Delta version {target} does not exist; ",
        "the latest available version is {latest}"
      )
    ))
  }
  eligible <- checkpoint_versions[checkpoint_versions <= target]
  checkpoint_version <- if (length(eligible)) max(eligible) else NULL
  checkpoint_paths <- if (!is.null(checkpoint_version)) {
    checkpoints[[match(checkpoint_version, checkpoint_versions)]]$paths
  } else {
    character()
  }
  first_json <- if (is.null(checkpoint_version)) 0 else checkpoint_version + 1
  json_needed <- json_paths[
    json_versions >= first_json & json_versions <= target
  ]
  list(
    paths = unique(c(checkpoint_paths, json_needed)),
    target = target,
    checkpoint_version = checkpoint_version
  )
}

#' Download files into their validated Delta staging locations
#' @keywords internal
#' @noRd
fabric_delta_download_staged <- function(staged, target, credential) {
  purrr::walk(
    unique(fs::path_dir(staged$destination)),
    fs::dir_create,
    recurse = TRUE
  )
  purrr::walk2(
    staged$source,
    staged$destination,
    function(source, destination) {
      file_target <- target
      file_target$path <- source
      onelake_download_target(
        file_target,
        credential,
        dest = destination,
        overwrite = TRUE
      )
    }
  )
  invisible(staged)
}

#' Keep downloadable files from a OneLake storage listing
#' @param files Data frame returned by the shared OneLake transport.
#' @return The rows that represent files rather than directories.
#' @keywords internal
#' @noRd
fabric_delta_file_rows <- function(files) {
  if (!is.data.frame(files) || !any(c("name", "path") %in% names(files))) {
    rlang::abort("OneLake returned an invalid storage listing")
  }
  if (!"name" %in% names(files)) {
    files$name <- files$path
  }
  if ("isdir" %in% names(files)) {
    files <- files[is.na(files$isdir) | !files$isdir, , drop = FALSE]
  } else if ("is_directory" %in% names(files)) {
    files <- files[
      is.na(files$is_directory) | !files$is_directory,
      ,
      drop = FALSE
    ]
  }
  files
}

#' Normalize a Lakehouse item name to include the `.Lakehouse` suffix
#' @keywords internal
#' @noRd
fabric_normalize_lakehouse_item <- function(lakehouse_name) {
  if (
    stringr::str_detect(
      lakehouse_name,
      stringr::regex(
        "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        ignore_case = TRUE
      )
    )
  ) {
    return(lakehouse_name)
  }
  if (
    stringr::str_ends(
      lakehouse_name,
      stringr::regex("\\.lakehouse$", ignore_case = TRUE)
    )
  ) {
    lakehouse_name
  } else {
    paste0(lakehouse_name, ".Lakehouse")
  }
}

#' Map OneLake table files to safe local staging paths
#' @param sources Character vector returned by `list_storage_files()`.
#' @param table_dir OneLake table root.
#' @param dest_dir Local staging root.
#' @return A data frame containing source, relative, and destination paths.
#' @keywords internal
#' @noRd
fabric_delta_stage_paths <- function(sources, table_dir, dest_dir) {
  sources <- gsub("\\\\", "/", sources)
  table_dir <- sub("/+$", "", gsub("\\\\", "/", table_dir))
  prefix <- paste0(table_dir, "/")
  if (!length(sources) || !all(startsWith(sources, prefix))) {
    rlang::abort("OneLake returned a file outside the requested Delta table")
  }

  relative <- substring(sources, nchar(prefix) + 1L)
  parts <- strsplit(relative, "/", fixed = TRUE)
  unsafe <- !nzchar(relative) |
    vapply(
      parts,
      function(x) !all(nzchar(x)) || any(x %in% c(".", "..")),
      logical(1)
    )
  if (any(unsafe)) {
    rlang::abort("OneLake returned an unsafe relative Delta table path")
  }

  data.frame(
    source = sources,
    relative = relative,
    destination = fs::path(dest_dir, relative),
    stringsAsFactors = FALSE
  )
}

#' Read a locally staged Delta snapshot
#' @param table_dir Local Delta table root.
#' @param version Optional Delta table version.
#' @param columns Optional logical columns to return.
#' @param limit Optional maximum number of rows to return.
#' @return A data frame.
#' @keywords internal
#' @noRd
fabric_delta_read_staged <- function(
  table_dir,
  version = NULL,
  columns = NULL,
  limit = NULL
) {
  snapshot <- fabric_delta_resolve_snapshot(table_dir, version = version)
  schema <- fabric_delta_schema(snapshot$metadata)
  fabric_delta_validate_type_widening(
    schema,
    unlist(
      snapshot$protocol$readerFeatures %||% list(),
      use.names = FALSE
    )
  )

  relative <- utils::URLdecode(snapshot$active)
  parts <- strsplit(gsub("\\\\", "/", relative), "/", fixed = TRUE)
  if (
    any(grepl("^[/\\\\]", relative)) ||
      any(vapply(parts, function(x) any(x %in% c("", ".", "..")), logical(1)))
  ) {
    rlang::abort("Delta log contains an unsafe data-file path")
  }
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  projection <- fabric_delta_schema_projection(con, schema)
  selected_indexes <- if (is.null(columns)) {
    seq_along(projection$names)
  } else {
    unknown <- setdiff(columns, projection$names)
    if (length(unknown)) {
      rlang::abort(paste0(
        "Requested Delta column",
        if (length(unknown) == 1L) " is" else "s are",
        " not present in the selected snapshot: ",
        paste(unknown, collapse = ", ")
      ))
    }
    match(columns, projection$names)
  }
  limit_sql <- if (is.null(limit)) {
    ""
  } else {
    paste0(" LIMIT ", sprintf("%.0f", limit))
  }
  if (!length(snapshot$active)) {
    return(DBI::dbGetQuery(
      con,
      paste0(
        "SELECT ",
        paste(projection$empty[selected_indexes], collapse = ", "),
        " WHERE FALSE",
        limit_sql
      )
    ))
  }

  paths <- fs::path(table_dir, relative)
  missing <- !fs::file_exists(paths)
  if (any(missing)) {
    rlang::abort(c(
      "Delta snapshot references data files that were not staged",
      "x" = cli::format_inline(
        "{.path {paths[which(missing)[1L]]}} is missing"
      )
    ))
  }

  normalized_paths <- gsub(
    "\\\\",
    "/",
    normalizePath(paths, mustWork = TRUE)
  )
  literals <- as.character(DBI::dbQuoteString(
    con,
    normalized_paths
  ))
  parquet_without_source <- paste0(
    "read_parquet([",
    paste(literals, collapse = ", "),
    "], union_by_name = true, hive_partitioning = false)"
  )
  physical <- DBI::dbGetQuery(
    con,
    paste0("DESCRIBE SELECT * FROM ", parquet_without_source)
  )$column_name
  source_column <- "fabric_delta_source_path_internal"
  while (source_column %in% physical) {
    source_column <- paste0(source_column, "_")
  }
  row_column <- "fabric_delta_row_index_internal"
  while (row_column %in% c(physical, source_column)) {
    row_column <- paste0(row_column, "_")
  }
  quoted_source <- as.character(DBI::dbQuoteIdentifier(con, source_column))
  quoted_row <- as.character(DBI::dbQuoteIdentifier(con, row_column))
  parquet <- paste(
    vapply(
      seq_along(literals),
      function(index) {
        paste0(
          "SELECT *, ",
          literals[[index]],
          " AS ",
          quoted_source,
          ", row_number() OVER () - 1 AS ",
          quoted_row,
          " FROM read_parquet(",
          literals[[index]],
          ", hive_partitioning = false)"
        )
      },
      character(1)
    ),
    collapse = " UNION ALL BY NAME "
  )
  parquet <- paste0("(", parquet, ")")
  mapping <- fabric_delta_partition_mapping(
    snapshot,
    normalized_paths,
    schema
  )
  DBI::dbWriteTable(con, "fabric_delta_partitions", mapping, temporary = TRUE)
  deletions <- fabric_delta_deletion_mapping(
    snapshot,
    normalized_paths,
    table_dir
  )
  DBI::dbWriteTable(con, "fabric_delta_deletions", deletions, temporary = TRUE)
  source <- paste0(
    parquet,
    " AS delta_source LEFT JOIN fabric_delta_partitions AS delta_partitions ",
    "ON delta_source.",
    as.character(DBI::dbQuoteIdentifier(con, source_column)),
    " = delta_partitions.fabric_delta_source_path ",
    "LEFT JOIN fabric_delta_deletions AS delta_deletions ON delta_source.",
    as.character(DBI::dbQuoteIdentifier(con, source_column)),
    " = delta_deletions.fabric_delta_source_path AND delta_source.",
    as.character(DBI::dbQuoteIdentifier(con, row_column)),
    " = delta_deletions.fabric_delta_row_index"
  )
  selected <- fabric_delta_read_projection(
    con,
    schema,
    physical,
    source_column
  )[selected_indexes]
  DBI::dbGetQuery(
    con,
    paste0(
      "SELECT ",
      paste(selected, collapse = ", "),
      " FROM ",
      source,
      " WHERE delta_deletions.fabric_delta_row_index IS NULL",
      limit_sql
    )
  )
}

#' Parse and validate the current logical Delta schema
#' @keywords internal
#' @noRd
fabric_delta_schema <- function(metadata) {
  schema_string <- metadata$schemaString %||% NULL
  if (
    is.null(schema_string) ||
      !is.character(schema_string) ||
      length(schema_string) != 1L ||
      is.na(schema_string) ||
      !nzchar(schema_string)
  ) {
    rlang::abort(
      "Delta snapshot does not contain a valid metadata schemaString"
    )
  }
  schema <- tryCatch(
    jsonlite::fromJSON(schema_string, simplifyVector = FALSE),
    error = function(error) {
      rlang::abort(
        "Could not parse the Delta metadata schemaString",
        parent = error
      )
    }
  )
  if (!identical(schema$type, "struct") || !is.list(schema$fields)) {
    rlang::abort("Delta metadata schemaString must describe a struct")
  }
  field_names <- vapply(
    schema$fields,
    function(field) as.character(field$name %||% ""),
    character(1)
  )
  if (!all(nzchar(field_names)) || anyDuplicated(tolower(field_names))) {
    rlang::abort(
      "Delta metadata schema contains missing or duplicate field names"
    )
  }
  schema$partitionColumns <- unlist(
    metadata$partitionColumns %||% list(),
    use.names = FALSE
  )
  configuration <- metadata$configuration %||% list()
  schema$columnMappingMode <- tolower(as.character(
    configuration[["delta.columnMapping.mode"]] %||% "none"
  ))
  unknown_partitions <- setdiff(schema$partitionColumns, field_names)
  if (length(unknown_partitions)) {
    rlang::abort(paste0(
      "Delta metadata references unknown partition column(s): ",
      paste(unknown_partitions, collapse = ", ")
    ))
  }
  schema
}

#' Validate every recorded Delta type-widening transition
#' @keywords internal
#' @noRd
fabric_delta_validate_type_widening <- function(schema, reader_features) {
  feature <- intersect(
    as.character(reader_features),
    c("typeWidening", "typeWidening-preview")
  )
  if (!length(feature)) {
    return(invisible(schema))
  }
  preview <- identical(feature[[1L]], "typeWidening-preview")
  visit_field <- function(field) {
    changes <- field$metadata[["delta.typeChanges"]] %||% list()
    for (change in changes) {
      from <- tolower(as.character(change$fromType %||% ""))
      to <- tolower(as.character(change$toType %||% ""))
      if (!fabric_delta_supported_type_change(from, to, preview = preview)) {
        fabric_delta_abort_unsupported(
          paste0(
            "Delta type-widening transition ",
            from,
            " -> ",
            to,
            " is not supported"
          )
        )
      }
    }
    visit_type(field$type)
  }
  visit_type <- function(type) {
    if (!is.list(type)) {
      return(invisible())
    }
    kind <- tolower(as.character(type$type %||% ""))
    if (identical(kind, "struct")) {
      lapply(type$fields %||% list(), visit_field)
    } else if (identical(kind, "array")) {
      visit_type(type$elementType)
    } else if (identical(kind, "map")) {
      visit_type(type$keyType)
      visit_type(type$valueType)
    }
    invisible()
  }
  lapply(schema$fields, visit_field)
  invisible(schema)
}

#' Check one documented Delta widening transition
#' @keywords internal
#' @noRd
fabric_delta_supported_type_change <- function(from, to, preview = FALSE) {
  if (!nzchar(from) || !nzchar(to)) {
    return(FALSE)
  }
  decimal <- "^decimal\\(([0-9]+),([0-9]+)\\)$"
  from_decimal <- regexec(decimal, from)
  to_decimal <- regexec(decimal, to)
  from_parts <- regmatches(from, from_decimal)[[1L]]
  to_parts <- regmatches(to, to_decimal)[[1L]]
  if (length(from_parts) && length(to_parts)) {
    from_precision <- as.numeric(from_parts[[2L]])
    from_scale <- as.numeric(from_parts[[3L]])
    to_precision <- as.numeric(to_parts[[2L]])
    to_scale <- as.numeric(to_parts[[3L]])
    precision_change <- to_precision - from_precision
    scale_change <- to_scale - from_scale
    return(
      precision_change >= 0 &&
        scale_change >= 0 &&
        precision_change >= scale_change
    )
  }
  if (preview) {
    allowed <- list(
      byte = c("short", "integer"),
      short = "integer",
      float = "double"
    )
  } else {
    allowed <- list(
      byte = c("short", "integer", "long", "double"),
      short = c("integer", "long", "double"),
      integer = c("long", "double"),
      float = "double",
      date = "timestamp_ntz"
    )
    if (
      grepl(decimal, to) &&
        from %in% c("byte", "short", "integer", "long")
    ) {
      return(TRUE)
    }
  }
  to %in% (allowed[[from]] %||% character())
}

#' Translate a Delta schema type into a DuckDB type
#' @keywords internal
#' @noRd
fabric_delta_duckdb_type <- function(con, type) {
  if (is.character(type) && length(type) == 1L) {
    normalized <- tolower(type)
    primitive <- c(
      string = "VARCHAR",
      long = "BIGINT",
      integer = "INTEGER",
      short = "SMALLINT",
      byte = "TINYINT",
      float = "FLOAT",
      double = "DOUBLE",
      boolean = "BOOLEAN",
      binary = "BLOB",
      date = "DATE",
      timestamp = "TIMESTAMPTZ",
      timestamp_ntz = "TIMESTAMP"
    )
    if (normalized %in% names(primitive)) {
      return(unname(primitive[[normalized]]))
    }
    if (grepl("^decimal\\([0-9]+,[0-9]+\\)$", normalized)) {
      return(toupper(normalized))
    }
    rlang::abort(paste0("Unsupported Delta schema type: ", type))
  }
  if (!is.list(type)) {
    rlang::abort("Delta schema contains an invalid field type")
  }
  kind <- tolower(as.character(type$type %||% ""))
  if (identical(kind, "struct")) {
    fields <- type$fields %||% list()
    if (!length(fields)) {
      rlang::abort("Empty Delta struct fields are not supported")
    }
    definitions <- vapply(
      fields,
      function(field) {
        paste(
          as.character(DBI::dbQuoteIdentifier(con, field$name)),
          fabric_delta_duckdb_type(con, field$type)
        )
      },
      character(1)
    )
    return(paste0("STRUCT(", paste(definitions, collapse = ", "), ")"))
  }
  if (identical(kind, "array")) {
    return(paste0(
      fabric_delta_duckdb_type(con, type$elementType),
      "[]"
    ))
  }
  if (identical(kind, "map")) {
    return(paste0(
      "MAP(",
      fabric_delta_duckdb_type(con, type$keyType),
      ", ",
      fabric_delta_duckdb_type(con, type$valueType),
      ")"
    ))
  }
  rlang::abort(paste0("Unsupported Delta complex schema type: ", kind))
}

#' Build logical and empty-table schema projections
#' @keywords internal
#' @noRd
fabric_delta_schema_projection <- function(con, schema) {
  names <- vapply(schema$fields, `[[`, character(1), "name")
  types <- vapply(
    schema$fields,
    function(field) fabric_delta_duckdb_type(con, field$type),
    character(1)
  )
  aliases <- as.character(DBI::dbQuoteIdentifier(con, names))
  list(
    names = names,
    types = types,
    empty = paste0("CAST(NULL AS ", types, ") AS ", aliases)
  )
}

#' Resolve the Parquet name for one Delta schema field
#' @keywords internal
#' @noRd
fabric_delta_field_physical_name <- function(field, mapping_mode = "none") {
  if (identical(mapping_mode, "none")) {
    return(as.character(field$name))
  }
  physical <- field$metadata[["delta.columnMapping.physicalName"]] %||% NULL
  if (
    is.null(physical) ||
      !is.character(physical) ||
      length(physical) != 1L ||
      is.na(physical) ||
      !nzchar(physical)
  ) {
    fabric_delta_abort_unsupported(
      paste0(
        "Delta column mapping metadata for field ",
        as.character(field$name),
        " is missing a physical name"
      )
    )
  }
  physical
}

#' Rebuild nested name-mapped values with their logical field names
#' @keywords internal
#' @noRd
fabric_delta_field_expression <- function(
  con,
  field,
  expression,
  mapping_mode = "none"
) {
  if (identical(mapping_mode, "none")) {
    return(expression)
  }
  fabric_delta_type_expression(
    con,
    field$type,
    expression,
    mapping_mode
  )
}

#' Recursively project a physical Delta value through a logical data type
#' @keywords internal
#' @noRd
fabric_delta_type_expression <- function(
  con,
  type,
  expression,
  mapping_mode = "none"
) {
  if (is.character(type) && length(type) == 1L) {
    return(expression)
  }
  kind <- tolower(as.character(type$type %||% ""))
  if (identical(kind, "struct")) {
    fields <- type$fields %||% list()
    packed <- vapply(
      fields,
      function(field) {
        physical <- fabric_delta_field_physical_name(field, mapping_mode)
        logical <- as.character(DBI::dbQuoteIdentifier(con, field$name))
        nested <- paste0(
          expression,
          ".",
          as.character(DBI::dbQuoteIdentifier(con, physical))
        )
        paste0(
          logical,
          " := ",
          fabric_delta_type_expression(
            con,
            field$type,
            nested,
            mapping_mode
          )
        )
      },
      character(1)
    )
    return(paste0("struct_pack(", paste(packed, collapse = ", "), ")"))
  }
  if (identical(kind, "array")) {
    return(paste0(
      "list_transform(",
      expression,
      ", fabric_delta_element -> ",
      fabric_delta_type_expression(
        con,
        type$elementType,
        "fabric_delta_element",
        mapping_mode
      ),
      ")"
    ))
  }
  if (identical(kind, "map")) {
    return(paste0(
      "map(list_transform(map_keys(",
      expression,
      "), fabric_delta_key -> ",
      fabric_delta_type_expression(
        con,
        type$keyType,
        "fabric_delta_key",
        mapping_mode
      ),
      "), list_transform(map_values(",
      expression,
      "), fabric_delta_value -> ",
      fabric_delta_type_expression(
        con,
        type$valueType,
        "fabric_delta_value",
        mapping_mode
      ),
      "))"
    ))
  }
  expression
}

#' Normalize a Delta partition-values map
#' @keywords internal
#' @noRd
fabric_delta_partition_values <- function(value) {
  if (is.null(value) || !length(value)) {
    return(list())
  }
  if (is.data.frame(value) && all(c("key", "value") %in% names(value))) {
    return(stats::setNames(as.list(value$value), value$key))
  }
  if (
    is.list(value) &&
      all(c("key", "value") %in% names(value)) &&
      length(value$key) == length(value$value)
  ) {
    return(stats::setNames(as.list(value$value), unlist(value$key)))
  }
  if (!is.null(names(value))) {
    return(as.list(value))
  }
  rlang::abort("Delta log contains an invalid partitionValues map")
}

#' Construct a temporary per-file partition mapping for DuckDB
#' @keywords internal
#' @noRd
fabric_delta_partition_mapping <- function(snapshot, paths, schema) {
  partitions <- schema$partitionColumns
  mapping <- data.frame(
    fabric_delta_source_path = paths,
    stringsAsFactors = FALSE
  )
  if (!length(partitions)) {
    return(mapping)
  }
  files <- snapshot$files %||% list()
  for (index in seq_along(partitions)) {
    partition <- partitions[[index]]
    mapping_mode <- schema$columnMappingMode %||% "none"
    physical_partition <- if (identical(mapping_mode, "none")) {
      partition
    } else {
      field_names <- vapply(schema$fields, `[[`, character(1), "name")
      fabric_delta_field_physical_name(
        schema$fields[[match(partition, field_names)]],
        mapping_mode
      )
    }
    values <- lapply(snapshot$active, function(path) {
      record <- files[[path]] %||% list(partitionValues = list())
      map <- record$partitionValues %||% list()
      if (!physical_partition %in% names(map)) {
        rlang::abort(cli::format_inline(
          "Delta file {.path {path}} has no value for partition column {.field {partition}}"
        ))
      }
      map[[physical_partition]]
    })
    mapping[[paste0("fabric_delta_partition_", index)]] <- vapply(
      values,
      function(value) {
        if (is.null(value) || length(value) == 0L || is.na(value[[1L]])) {
          NA_character_
        } else {
          text <- as.character(value[[1L]])
          if (nzchar(text)) text else NA_character_
        }
      },
      character(1)
    )
  }
  mapping
}

#' Normalize one checkpoint deletion-vector descriptor
#' @keywords internal
#' @noRd
fabric_delta_deletion_vector_value <- function(value) {
  if (is.null(value) || !length(value)) {
    return(NULL)
  }
  if (!is.list(value)) {
    if (length(value) == 1L && is.na(value)) {
      return(NULL)
    }
    rlang::abort("Delta checkpoint contains an invalid deletion vector")
  }
  if (is.data.frame(value)) {
    value <- lapply(value, function(column) column[[1L]])
  }
  storage_type <- value$storageType %||% NULL
  if (
    is.null(storage_type) ||
      !length(storage_type) ||
      is.na(storage_type[[1L]])
  ) {
    return(NULL)
  }
  lapply(value, function(field) {
    if (!length(field) || is.na(field[[1L]])) NULL else field[[1L]]
  })
}

#' Resolve the table-relative path of a persisted deletion vector
#' @keywords internal
#' @noRd
fabric_delta_deletion_vector_path <- function(descriptor) {
  storage_type <- as.character(descriptor$storageType %||% "")
  encoded <- as.character(descriptor$pathOrInlineDv %||% "")
  if (!identical(storage_type, "u")) {
    return(NULL)
  }
  if (nchar(encoded, type = "bytes") < 20L) {
    rlang::abort("Delta deletion vector contains an invalid relative path")
  }
  prefix_length <- nchar(encoded, type = "bytes") - 20L
  prefix <- if (prefix_length) {
    substr(encoded, 1L, prefix_length)
  } else {
    ""
  }
  uuid_bytes <- fabric_delta_z85_decode(
    substr(encoded, prefix_length + 1L, prefix_length + 20L)
  )
  if (length(uuid_bytes) != 16L) {
    rlang::abort("Delta deletion vector contains an invalid UUID")
  }
  hex <- paste0(sprintf("%02x", as.integer(uuid_bytes)), collapse = "")
  uuid <- paste(
    substr(hex, 1L, 8L),
    substr(hex, 9L, 12L),
    substr(hex, 13L, 16L),
    substr(hex, 17L, 20L),
    substr(hex, 21L, 32L),
    sep = "-"
  )
  relative <- if (nzchar(prefix)) {
    paste0(prefix, "/deletion_vector_", uuid, ".bin")
  } else {
    paste0("deletion_vector_", uuid, ".bin")
  }
  parts <- strsplit(gsub("\\\\", "/", relative), "/", fixed = TRUE)[[1L]]
  if (
    grepl("^[/\\\\]", relative) ||
      any(!nzchar(parts) | parts %in% c(".", ".."))
  ) {
    rlang::abort("Delta deletion vector contains an unsafe relative path")
  }
  relative
}

#' List persisted deletion-vector sidecars required by a snapshot
#' @keywords internal
#' @noRd
fabric_delta_deletion_vector_paths <- function(snapshot) {
  paths <- unlist(
    lapply(snapshot$active %||% character(), function(path) {
      descriptor <- snapshot$files[[path]]$deletionVector %||% NULL
      if (is.null(descriptor)) {
        return(NULL)
      }
      fabric_delta_deletion_vector_path(descriptor)
    }),
    use.names = FALSE
  )
  unique(paths[nzchar(paths)])
}

#' Build the per-file row-index deletion relation used by DuckDB
#' @keywords internal
#' @noRd
fabric_delta_deletion_mapping <- function(snapshot, paths, table_dir) {
  rows <- Map(
    function(path, normalized_path) {
      descriptor <- snapshot$files[[path]]$deletionVector %||% NULL
      if (is.null(descriptor)) {
        return(NULL)
      }
      indexes <- fabric_delta_read_deletion_vector(descriptor, table_dir)
      if (!length(indexes)) {
        return(NULL)
      }
      data.frame(
        fabric_delta_source_path = rep(normalized_path, length(indexes)),
        fabric_delta_row_index = indexes,
        stringsAsFactors = FALSE
      )
    },
    snapshot$active,
    paths
  )
  rows <- Filter(Negate(is.null), rows)
  if (!length(rows)) {
    return(data.frame(
      fabric_delta_source_path = character(),
      fabric_delta_row_index = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

#' Read and validate one Delta deletion vector
#' @keywords internal
#' @noRd
fabric_delta_read_deletion_vector <- function(descriptor, table_dir) {
  storage_type <- as.character(descriptor$storageType %||% "")
  size <- as.numeric(descriptor$sizeInBytes %||% NA_real_)
  cardinality <- as.numeric(descriptor$cardinality %||% NA_real_)
  if (
    !storage_type %in% c("u", "i") ||
      !is.finite(size) ||
      size < 4 ||
      size != floor(size) ||
      !is.finite(cardinality) ||
      cardinality < 0 ||
      cardinality != floor(cardinality)
  ) {
    rlang::abort("Delta deletion vector contains an invalid descriptor")
  }

  if (identical(storage_type, "i")) {
    if (!is.null(descriptor$offset)) {
      rlang::abort("Inline Delta deletion vectors must not contain an offset")
    }
    bitmap_data <- fabric_delta_z85_decode(
      as.character(descriptor$pathOrInlineDv %||% "")
    )
    if (length(bitmap_data) != size) {
      rlang::abort(
        "Inline Delta deletion vector size does not match its descriptor"
      )
    }
  } else {
    relative <- fabric_delta_deletion_vector_path(descriptor)
    path <- fs::path(table_dir, relative)
    if (!fs::file_exists(path)) {
      rlang::abort(c(
        "Delta snapshot references a deletion-vector file that was not staged",
        "x" = cli::format_inline("{.path {path}} is missing")
      ))
    }
    bytes <- readBin(path, "raw", n = fs::file_size(path))
    offset <- as.numeric(descriptor$offset %||% 0)
    start <- offset + 1L
    if (
      !is.finite(offset) ||
        offset < 0 ||
        offset != floor(offset) ||
        start + 3L > length(bytes)
    ) {
      rlang::abort("Delta deletion-vector sidecar offset is invalid")
    }
    if (
      offset > 0 &&
        (!length(bytes) || as.integer(bytes[[1L]]) != 1L)
    ) {
      rlang::abort("Delta deletion-vector sidecar has an unsupported version")
    }
    stored_size <- fabric_delta_raw_uint32(bytes, start, endian = "big")
    if (!identical(as.numeric(stored_size), as.numeric(size))) {
      rlang::abort(
        "Delta deletion-vector sidecar size does not match its descriptor"
      )
    }
    data_start <- start + 4L
    data_end <- data_start + size - 1L
    crc_start <- data_end + 1L
    if (crc_start + 3L > length(bytes)) {
      rlang::abort("Delta deletion-vector sidecar is truncated")
    }
    bitmap_data <- bytes[data_start:data_end]
    stored_crc <- fabric_delta_raw_uint32(bytes, crc_start, endian = "big")
    actual_crc <- fabric_delta_crc32(bitmap_data)
    if (!identical(as.numeric(stored_crc), as.numeric(actual_crc))) {
      rlang::abort("Delta deletion-vector sidecar checksum is invalid")
    }
  }

  magic <- fabric_delta_raw_uint32(bitmap_data, 1L, endian = "little")
  if (!identical(as.numeric(magic), 1681511377)) {
    rlang::abort("Delta deletion vector has an unsupported bitmap format")
  }
  parsed <- fabric_delta_roaring64(bitmap_data[-seq_len(4L)])
  if (!identical(as.numeric(length(parsed)), cardinality)) {
    rlang::abort(
      "Delta deletion vector cardinality does not match its descriptor"
    )
  }
  parsed
}

#' Decode a ZeroMQ Z85 string
#' @keywords internal
#' @noRd
fabric_delta_z85_decode <- function(value) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      nchar(value, type = "bytes") %% 5L != 0L
  ) {
    rlang::abort("Delta deletion vector contains invalid Z85 data")
  }
  if (!nzchar(value)) {
    return(raw())
  }
  alphabet <- strsplit(
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#",
    "",
    fixed = TRUE
  )[[1L]]
  lookup <- stats::setNames(seq_along(alphabet) - 1L, alphabet)
  chars <- strsplit(value, "", fixed = TRUE)[[1L]]
  digits <- unname(lookup[chars])
  if (anyNA(digits)) {
    rlang::abort("Delta deletion vector contains invalid Z85 data")
  }
  groups <- matrix(digits, nrow = 5L)
  decoded <- apply(groups, 2L, function(group) {
    number <- sum(group * 85^(4:0))
    if (number > 4294967295) {
      rlang::abort("Delta deletion vector contains invalid Z85 data")
    }
    as.raw(c(
      floor(number / 16777216) %% 256,
      floor(number / 65536) %% 256,
      floor(number / 256) %% 256,
      number %% 256
    ))
  })
  as.raw(as.vector(decoded))
}

#' Read an unsigned integer from a raw vector without precision loss
#' @keywords internal
#' @noRd
fabric_delta_raw_uint32 <- function(bytes, start, endian = c("little", "big")) {
  endian <- match.arg(endian)
  indexes <- start + 0:3
  if (start < 1L || max(indexes) > length(bytes)) {
    rlang::abort("Delta deletion vector is truncated")
  }
  value <- as.numeric(as.integer(bytes[indexes]))
  powers <- if (identical(endian, "little")) 256^(0:3) else 256^(3:0)
  sum(value * powers)
}

fabric_delta_raw_uint16 <- function(bytes, start) {
  indexes <- start + 0:1
  if (start < 1L || max(indexes) > length(bytes)) {
    rlang::abort("Delta deletion vector is truncated")
  }
  value <- as.numeric(as.integer(bytes[indexes]))
  sum(value * c(1, 256))
}

#' Compute the CRC-32/ISO-HDLC checksum used by DV sidecars
#' @keywords internal
#' @noRd
fabric_delta_crc32 <- function(bytes) {
  crc <- -1L
  polynomial <- -306674912L
  for (byte in as.integer(bytes)) {
    crc <- bitwXor(crc, byte)
    for (bit in seq_len(8L)) {
      crc <- if (bitwAnd(crc, 1L)) {
        bitwXor(bitwShiftR(crc, 1L), polynomial)
      } else {
        bitwShiftR(crc, 1L)
      }
    }
  }
  crc <- bitwXor(crc, -1L)
  if (crc < 0) as.numeric(crc) + 4294967296 else as.numeric(crc)
}

#' Decode the portable 64-bit Roaring bitmap used by Delta
#' @keywords internal
#' @noRd
fabric_delta_roaring64 <- function(bytes) {
  if (length(bytes) < 8L) {
    rlang::abort("Delta deletion vector contains a truncated Roaring bitmap")
  }
  low <- fabric_delta_raw_uint32(bytes, 1L, "little")
  high <- fabric_delta_raw_uint32(bytes, 5L, "little")
  bucket_count <- low + high * 4294967296
  if (bucket_count > .Machine$integer.max) {
    rlang::abort("Delta deletion vector contains too many Roaring buckets")
  }
  cursor <- 9L
  values <- vector("list", as.integer(bucket_count))
  for (index in seq_len(as.integer(bucket_count))) {
    bucket <- fabric_delta_raw_uint32(bytes, cursor, "little")
    if (bucket >= 2147483648) {
      rlang::abort("Delta deletion vector contains an invalid signed row index")
    }
    parsed <- fabric_delta_roaring32(bytes, cursor + 4L)
    values[[index]] <- bucket * 4294967296 + parsed$values
    cursor <- parsed$cursor
  }
  if (cursor != length(bytes) + 1L) {
    rlang::abort("Delta deletion vector contains trailing bitmap data")
  }
  unlist(values, use.names = FALSE)
}

#' Decode one standard portable 32-bit Roaring bitmap
#' @keywords internal
#' @noRd
fabric_delta_roaring32 <- function(bytes, start) {
  cookie <- fabric_delta_raw_uint32(bytes, start, "little")
  if (identical(cookie, 12346)) {
    size <- fabric_delta_raw_uint32(bytes, start + 4L, "little")
    cursor <- start + 8L
    runs <- rep(FALSE, size)
    has_offsets <- TRUE
  } else if (cookie %% 65536 == 12347) {
    size <- floor(cookie / 65536) + 1L
    cursor <- start + 4L
    run_bytes <- ceiling(size / 8)
    if (cursor + run_bytes - 1L > length(bytes)) {
      rlang::abort("Delta deletion vector contains a truncated Roaring header")
    }
    flags <- as.integer(bytes[cursor:(cursor + run_bytes - 1L)])
    runs <- vapply(
      seq_len(size),
      function(index) {
        bitwAnd(
          flags[[floor((index - 1L) / 8L) + 1L]],
          2^((index - 1L) %% 8L)
        ) !=
          0L
      },
      logical(1)
    )
    cursor <- cursor + run_bytes
    has_offsets <- size >= 4L
  } else {
    rlang::abort("Delta deletion vector contains an invalid Roaring cookie")
  }
  if (size > 65536 || cursor + 4 * size - 1L > length(bytes)) {
    rlang::abort("Delta deletion vector contains an invalid Roaring header")
  }
  keys <- cards <- numeric(size)
  for (index in seq_len(size)) {
    keys[[index]] <- fabric_delta_raw_uint16(bytes, cursor)
    cards[[index]] <- fabric_delta_raw_uint16(bytes, cursor + 2L) + 1
    cursor <- cursor + 4L
  }
  if (has_offsets) {
    cursor <- cursor + 4L * size
    if (cursor - 1L > length(bytes)) {
      rlang::abort(
        "Delta deletion vector contains a truncated Roaring offset table"
      )
    }
  }

  values <- vector("list", size)
  for (index in seq_len(size)) {
    card <- cards[[index]]
    if (runs[[index]]) {
      count <- fabric_delta_raw_uint16(bytes, cursor)
      cursor <- cursor + 2L
      container <- numeric()
      for (run in seq_len(count)) {
        first <- fabric_delta_raw_uint16(bytes, cursor)
        length_minus_one <- fabric_delta_raw_uint16(bytes, cursor + 2L)
        container <- c(container, seq(first, first + length_minus_one))
        cursor <- cursor + 4L
      }
    } else if (card <= 4096) {
      container <- vapply(
        seq_len(card),
        function(item) {
          fabric_delta_raw_uint16(bytes, cursor + 2L * (item - 1L))
        },
        numeric(1)
      )
      cursor <- cursor + 2L * card
    } else {
      if (cursor + 8191L > length(bytes)) {
        rlang::abort(
          "Delta deletion vector contains a truncated Roaring bitset"
        )
      }
      bitset <- as.integer(bytes[cursor:(cursor + 8191L)])
      present <- which(bitset != 0L)
      container <- unlist(
        lapply(present, function(byte_index) {
          bits <- which(vapply(
            0:7,
            function(bit) bitwAnd(bitset[[byte_index]], 2^bit) != 0L,
            logical(1)
          )) -
            1L
          (byte_index - 1L) * 8 + bits
        }),
        use.names = FALSE
      )
      cursor <- cursor + 8192L
    }
    if (length(container) != card) {
      rlang::abort(
        "Delta deletion vector contains an invalid Roaring cardinality"
      )
    }
    values[[index]] <- keys[[index]] * 65536 + container
  }
  list(values = unlist(values, use.names = FALSE), cursor = cursor)
}

#' Project physical Parquet data through the current Delta schema
#' @keywords internal
#' @noRd
fabric_delta_read_projection <- function(
  con,
  schema,
  physical,
  source_column
) {
  projection <- fabric_delta_schema_projection(con, schema)
  vapply(
    seq_along(projection$names),
    function(index) {
      name <- projection$names[[index]]
      type <- projection$types[[index]]
      alias <- as.character(DBI::dbQuoteIdentifier(con, name))
      if (name %in% schema$partitionColumns) {
        partition_index <- match(name, schema$partitionColumns)
        expression <- paste0(
          "delta_partitions.",
          as.character(DBI::dbQuoteIdentifier(
            con,
            paste0("fabric_delta_partition_", partition_index)
          ))
        )
      } else {
        field <- schema$fields[[index]]
        physical_name <- fabric_delta_field_physical_name(
          field,
          schema$columnMappingMode
        )
        if (physical_name %in% physical) {
          expression <- fabric_delta_field_expression(
            con,
            field,
            paste0(
              "delta_source.",
              as.character(DBI::dbQuoteIdentifier(con, physical_name))
            ),
            schema$columnMappingMode
          )
        } else {
          expression <- "NULL"
        }
      }
      paste0("CAST(", expression, " AS ", type, ") AS ", alias)
    },
    character(1)
  )
}

#' Resolve a Delta snapshot from checkpoints and JSON commits
#' @param table_dir Local Delta table root.
#' @param version Optional requested version.
#' @return Snapshot metadata and active data-file paths.
#' @keywords internal
#' @noRd
fabric_delta_resolve_snapshot <- function(table_dir, version = NULL) {
  log_dir <- fs::path(table_dir, "_delta_log")
  if (!fs::dir_exists(log_dir)) {
    rlang::abort(cli::format_inline(
      "No {.path _delta_log} directory found in the staged table"
    ))
  }

  logs <- fs::dir_ls(log_dir, type = "file")
  names <- basename(logs)
  json_match <- regexec("^([0-9]{20})\\.json$", names)
  json_parts <- regmatches(names, json_match)
  json_keep <- lengths(json_parts) > 0L
  json_versions <- fabric_delta_versions_from_text(vapply(
    json_parts[json_keep],
    `[[`,
    character(1),
    2L
  ))
  json_paths <- logs[json_keep]

  checkpoint_sets <- fabric_delta_checkpoint_sets(logs)
  checkpoint_versions <- vapply(
    checkpoint_sets,
    `[[`,
    numeric(1),
    "version"
  )

  available <- c(json_versions, checkpoint_versions)
  if (!length(available)) {
    rlang::abort("No Delta commits or checkpoints were found")
  }
  latest <- max(available)
  target <- version %||% latest
  if (target > latest) {
    rlang::abort(cli::format_inline(
      "Delta version {target} does not exist; the latest staged version is {latest}"
    ))
  }

  eligible <- unique(checkpoint_versions[checkpoint_versions <= target])
  checkpoint_version <- if (length(eligible)) max(eligible) else NULL
  state <- list(
    active = character(),
    files = list(),
    protocol = NULL,
    metadata = NULL,
    has_deletion_vectors = FALSE
  )

  if (!is.null(checkpoint_version)) {
    checkpoint_index <- match(checkpoint_version, checkpoint_versions)
    checkpoint <- fabric_delta_read_checkpoint(
      checkpoint_sets[[checkpoint_index]]$paths
    )
    state <- fabric_delta_apply_checkpoint(state, checkpoint)
  }

  first_json <- if (is.null(checkpoint_version)) 0 else checkpoint_version + 1
  needed <- if (first_json <= target) seq(first_json, target) else numeric()
  present <- sort(json_versions[
    json_versions >= first_json & json_versions <= target
  ])
  if (!identical(as.numeric(present), as.numeric(needed))) {
    rlang::abort(cli::format_inline(
      "Delta log is incomplete for version {target}; a required commit is missing"
    ))
  }
  if (length(present)) {
    ordered_paths <- json_paths[match(present, json_versions)]
    for (path in ordered_paths) {
      state <- fabric_delta_apply_json_log(state, path)
    }
  }

  fabric_delta_validate_reader(state)
  c(state, list(version = target, checkpoint_version = checkpoint_version))
}

#' Find complete classic or multipart Delta checkpoints
#' @param paths Paths in a Delta log directory.
#' @return A list of complete checkpoint records with `version` and `paths`.
#' @keywords internal
#' @noRd
fabric_delta_checkpoint_sets <- function(paths) {
  filenames <- basename(paths)
  matches <- regexec(
    "^([0-9]{20})\\.checkpoint(?:\\.([0-9]{10})\\.([0-9]{10}))?\\.parquet$",
    filenames
  )
  parts <- regmatches(filenames, matches)
  keep <- lengths(parts) > 0L
  if (!any(keep)) {
    return(list())
  }

  records <- Map(
    function(match, path) {
      multipart <- length(match) >= 4L &&
        nzchar(match[[3L]]) &&
        nzchar(match[[4L]])
      list(
        version_text = match[[2L]],
        version = fabric_delta_versions_from_text(match[[2L]]),
        part = if (multipart) as.integer(match[[3L]]) else NA_integer_,
        total = if (multipart) as.integer(match[[4L]]) else NA_integer_,
        path = path
      )
    },
    parts[keep],
    as.list(paths[keep])
  )

  by_version <- split(
    records,
    vapply(records, `[[`, character(1), "version_text")
  )
  complete <- lapply(by_version, function(version_records) {
    classic <- Filter(
      function(record) is.na(record$part),
      version_records
    )
    if (length(classic)) {
      return(list(
        version = classic[[1L]]$version,
        paths = classic[[1L]]$path
      ))
    }

    totals <- sort(unique(vapply(
      version_records,
      `[[`,
      integer(1),
      "total"
    )))
    for (total in totals) {
      candidates <- Filter(
        function(record) identical(record$total, total),
        version_records
      )
      part_numbers <- vapply(candidates, `[[`, integer(1), "part")
      if (
        total > 1L &&
          !anyDuplicated(part_numbers) &&
          identical(sort(part_numbers), seq_len(total))
      ) {
        ordered <- candidates[order(part_numbers)]
        return(list(
          version = ordered[[1L]]$version,
          paths = vapply(ordered, `[[`, character(1), "path")
        ))
      }
    }
    NULL
  })
  complete <- Filter(Negate(is.null), complete)
  complete[order(vapply(complete, `[[`, numeric(1), "version"))]
}

#' Read Delta checkpoint rows with DuckDB's built-in Parquet reader
#' @keywords internal
#' @noRd
fabric_delta_read_checkpoint <- function(paths) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  literals <- as.character(DBI::dbQuoteString(
    con,
    gsub("\\\\", "/", normalizePath(paths, mustWork = TRUE))
  ))
  DBI::dbGetQuery(
    con,
    paste0(
      "SELECT add, remove, protocol, metaData FROM read_parquet([",
      paste(literals, collapse = ", "),
      "], union_by_name = true)"
    )
  )
}

#' Apply actions stored in a Delta checkpoint
#' @keywords internal
#' @noRd
fabric_delta_apply_checkpoint <- function(state, checkpoint) {
  adds <- checkpoint$add$path
  removes <- checkpoint$remove$path
  for (path in removes[!is.na(removes)]) {
    state <- fabric_delta_remove_file(state, path)
  }
  add_rows <- which(!is.na(adds))
  for (i in add_rows) {
    partition_values <- fabric_delta_checkpoint_value(
      checkpoint$add$partitionValues %||% NULL,
      i,
      length(adds)
    )
    deletion_vector <- fabric_delta_checkpoint_value(
      checkpoint$add$deletionVector %||% NULL,
      i,
      length(adds)
    )
    state <- fabric_delta_add_file(
      state,
      list(
        path = adds[[i]],
        partitionValues = fabric_delta_partition_values(partition_values),
        deletionVector = fabric_delta_deletion_vector_value(deletion_vector)
      )
    )
  }

  protocol_rows <- which(!is.na(checkpoint$protocol$minReaderVersion))
  if (length(protocol_rows)) {
    i <- utils::tail(protocol_rows, 1L)
    state$protocol <- list(
      minReaderVersion = checkpoint$protocol$minReaderVersion[[i]],
      minWriterVersion = (checkpoint$protocol$minWriterVersion %||%
        rep(NA_integer_, i))[[i]],
      readerFeatures = checkpoint$protocol$readerFeatures[[i]]
    )
  }
  metadata_rows <- which(!is.na(checkpoint$metaData$id))
  if (length(metadata_rows)) {
    i <- utils::tail(metadata_rows, 1L)
    config <- fabric_delta_checkpoint_value(
      checkpoint$metaData$configuration,
      i,
      length(checkpoint$metaData$id)
    )
    configuration <- if (is.null(config) || !NROW(config)) {
      list()
    } else {
      fabric_delta_partition_values(config)
    }
    state$metadata <- list(
      schemaString = fabric_delta_checkpoint_value(
        checkpoint$metaData$schemaString %||% NULL,
        i,
        length(checkpoint$metaData$id)
      ),
      partitionColumns = fabric_delta_checkpoint_value(
        checkpoint$metaData$partitionColumns %||% NULL,
        i,
        length(checkpoint$metaData$id)
      ) %||%
        list(),
      configuration = configuration
    )
  }
  state
}

#' Extract one nested value from a DuckDB checkpoint column
#' @keywords internal
#' @noRd
fabric_delta_checkpoint_value <- function(column, index, row_count) {
  if (is.null(column)) {
    return(NULL)
  }
  if (is.data.frame(column)) {
    return(lapply(column, function(value) value[[index]]))
  }
  if (is.list(column) && length(column) == row_count) {
    return(column[[index]])
  }
  if (length(column) == row_count) {
    return(column[[index]])
  }
  column
}

#' Apply an active Delta add-file action
#' @keywords internal
#' @noRd
fabric_delta_add_file <- function(state, add) {
  path <- add$path
  state$active <- c(setdiff(state$active, path), path)
  state$files[[path]] <- list(
    path = path,
    partitionValues = fabric_delta_partition_values(
      add$partitionValues %||% list()
    ),
    deletionVector = add$deletionVector %||% NULL
  )
  state$has_deletion_vectors <- state$has_deletion_vectors ||
    !is.null(add$deletionVector)
  state
}

#' Apply a Delta remove-file action
#' @keywords internal
#' @noRd
fabric_delta_remove_file <- function(state, path) {
  state$active <- setdiff(state$active, path)
  state$files[[path]] <- NULL
  state
}

#' Apply actions from one Delta JSON commit
#' @keywords internal
#' @noRd
fabric_delta_apply_json_log <- function(state, path) {
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  actions <- lapply(
    lines[nzchar(lines)],
    function(line) {
      tryCatch(
        jsonlite::fromJSON(line, simplifyVector = FALSE),
        error = function(e) {
          rlang::abort(
            cli::format_inline(
              "Could not parse Delta commit {.path {basename(path)}}"
            ),
            parent = e
          )
        }
      )
    }
  )
  for (action in actions) {
    if (!is.null(action$remove$path)) {
      state <- fabric_delta_remove_file(state, action$remove$path)
    }
  }
  for (action in actions) {
    if (!is.null(action$add$path)) {
      state <- fabric_delta_add_file(state, action$add)
    }
    if (!is.null(action$protocol)) {
      state$protocol <- action$protocol
    }
    if (!is.null(action$metaData)) {
      state$metadata <- action$metaData
    }
  }
  state
}

#' Reject Delta reader features not implemented by the staged reader
#' @keywords internal
#' @noRd
fabric_delta_validate_reader <- function(state) {
  if (is.null(state$protocol$minReaderVersion)) {
    rlang::abort("Delta snapshot does not contain a reader protocol action")
  }
  reader_version <- as.numeric(state$protocol$minReaderVersion)
  writer_version <- as.numeric(state$protocol$minWriterVersion %||% NA_real_)
  features <- unlist(
    state$protocol$readerFeatures %||% list(),
    use.names = FALSE
  )
  features <- as.character(features[!is.na(features) & nzchar(features)])
  supported_features <- c(
    "columnMapping",
    "deletionVectors",
    "timestampNtz",
    "typeWidening",
    "typeWidening-preview",
    "vacuumProtocolCheck"
  )
  unsupported <- setdiff(features, supported_features)

  configuration <- state$metadata$configuration %||% list()
  mapping <- tolower(as.character(
    configuration[["delta.columnMapping.mode"]] %||% "none"
  ))
  if (!mapping %in% c("none", "name", "id")) {
    fabric_delta_abort_unsupported(
      cli::format_inline(
        "Delta column mapping mode {.val {mapping}} is invalid"
      )
    )
  }
  if (identical(mapping, "id")) {
    fabric_delta_abort_unsupported(
      "Delta column mapping mode \"id\" is not supported"
    )
  }
  if (reader_version < 1 || reader_version > 3) {
    fabric_delta_abort_unsupported(
      paste0("Delta reader protocol version ", reader_version)
    )
  }
  if (reader_version < 3 && length(features)) {
    fabric_delta_abort_unsupported(
      paste0(
        "Delta reader protocol version ",
        reader_version,
        " with reader features: ",
        paste(features, collapse = ", ")
      )
    )
  }
  if (
    reader_version == 3 &&
      (!is.finite(writer_version) || writer_version != 7)
  ) {
    fabric_delta_abort_unsupported(
      paste0(
        "Delta reader protocol version 3 with writer protocol version ",
        writer_version
      )
    )
  }
  if (length(unsupported)) {
    fabric_delta_abort_unsupported(
      paste0("Delta reader feature(s): ", paste(unsupported, collapse = ", "))
    )
  }
  if (
    !identical(mapping, "none") &&
      !(reader_version == 2 ||
        (reader_version == 3 && "columnMapping" %in% features))
  ) {
    fabric_delta_abort_unsupported(
      "Delta column mapping without matching protocol support"
    )
  }
  active_deletion_vectors <- any(vapply(
    state$active %||% character(),
    function(path) {
      !is.null(state$files[[path]]$deletionVector %||% NULL)
    },
    logical(1)
  ))
  if (
    active_deletion_vectors &&
      !(reader_version == 3 &&
        "deletionVectors" %in% features)
  ) {
    fabric_delta_abort_unsupported(
      "Delta deletion vectors without matching protocol support"
    )
  }
  for (path in state$active %||% character()) {
    descriptor <- state$files[[path]]$deletionVector %||% NULL
    if (
      !is.null(descriptor) &&
        identical(as.character(descriptor$storageType %||% ""), "p")
    ) {
      fabric_delta_abort_unsupported(
        "Delta deletion vectors stored at absolute paths are not supported"
      )
    }
  }
  invisible(state)
}

fabric_delta_abort_unsupported <- function(feature) {
  rlang::abort(
    c(
      paste0(feature, " by the staged reader."),
      "i" = paste(
        "This reader supports Delta reader protocols 1 through 3 with",
        "name-based column mapping, deletion vectors, timestampNtz,",
        "type widening, and vacuumProtocolCheck."
      ),
      "i" = paste(
        "Query the table through its Fabric SQL analytics endpoint or",
        "Fabric Spark instead."
      )
    ),
    class = "fabric_delta_unsupported_error"
  )
}
