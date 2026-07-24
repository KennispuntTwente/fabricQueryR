#' @title
#' Read a Microsoft Fabric/OneLake Delta table (ADLS Gen2)
#'
#' @description
#' Authenticates to OneLake through the package's shared ADLS Gen2 transport,
#' stages the Delta transaction log, resolves the requested snapshot, and then
#' downloads only the data files active in that snapshot.
#'
#' @details
#' - In Microsoft Fabric, OneLake exposes each workspace as an ADLS Gen2
#'  filesystem. Within a Lakehouse item, Delta tables are stored under
#'  `Tables/<table>` (non-schema lakehouse) or `Tables/<schema>/<table>`
#'  (schema-enabled lakehouse). The complete table is staged because Delta
#'  checkpoints and table features are resolved before data files are
#'  downloaded, so historical and tombstoned Parquet files are not staged.
#' - Checkpoint Parquet and data Parquet files are read with DuckDB. Tables that
#'  require reader protocol versions or reader features this package does not
#'  implement are rejected before any data is returned.
#' - Schema-enabled lakehouses (the default for new lakehouses) organise
#'  tables into named schemas. Supply the `schema` argument (e.g. `"dbo"`)
#'  to read a table stored under a specific schema.
#' - Ensure the account/principal you authenticate with has access via
#'  **Lakehouse -> Manage OneLake data access** (or is a member of the workspace).
#' - Tokens use the `https://storage.azure.com/.default` audience.
#' - \pkg{AzureAuth} is used to acquire the token. Be wary of
#'  caching behavior; you may want to call [AzureAuth::clean_token_directory()]
#'  to clear cached tokens if you run into issues
#'
#' @param table_path Character. Table name or nested path (e.g.
#'   `"Patienten"` or `"Patienten/patienten_hash"`). Only the last path
#'   segment is used as the table directory under `Tables/`.
#' @param workspace_name Character. Fabric workspace display name or GUID
#'   (this is the ADLS filesystem/container name).
#' @param lakehouse_name Character. Lakehouse item name, with or without the
#'   `.Lakehouse` suffix (e.g. `"Lakehouse"` or `"Lakehouse.Lakehouse"`).
#' @param schema Character or `NULL`. Lakehouse schema name (e.g. `"dbo"`).
#'   When supplied, the table is resolved under `Tables/<schema>/<table>`
#'   instead of `Tables/<table>`. Schema support requires a schema-enabled
#'   Lakehouse (enabled by default for new lakehouses). Defaults to `NULL`
#'   (no schema, for non-schema lakehouses). (Note: schema support through this
#'   argument is experimental.)
#' @param tenant_id Character. Entra ID (Azure AD) tenant GUID. Defaults to
#'   `Sys.getenv("FABRICQUERYR_TENANT_ID")` if missing.
#' @param client_id Character. App registration (client) ID. Defaults to
#'   `Sys.getenv("FABRICQUERYR_CLIENT_ID")`, falling back to the Azure CLI app id
#'   `"04b07795-8ddb-461a-bbee-02f9e1bf7b46"` if not set.
#' @param access_token Optional character. If supplied, use this bearer token
#'   instead of acquiring a new one via `{AzureAuth}`.
#' @param token_provider Optional function returning a OneLake Storage bearer
#'   token. It may accept `audience` and `force_refresh` arguments. Supply only
#'   one of `access_token` and `token_provider`.
#' @param version Optional non-negative integer Delta table version to read.
#'   Defaults to the latest version.
#' @param dest_dir Character or `NULL`. Local staging directory for the Delta
#'   log and active snapshot files. If `NULL` (default), a temp dir is used and
#'   cleaned up on exit.
#' @param verbose Logical. Print progress messages via `{cli}`. Default `TRUE`.
#' @param dfs_base Character. OneLake DFS endpoint. Default
#'   `"https://onelake.dfs.fabric.microsoft.com"`.
#'
#' @return A tibble with the table's current rows (0 rows if the table is empty).
#' @export
#'
#' @examples
#' # Example is not executed since it requires configured credentials for Fabric
#' \dontrun{
#' df <- fabric_onelake_read_delta_table(
#'   table_path     = "Patients/PatientInfo",
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
#'   schema         = "dbo"
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
  access_token = NULL,
  token_provider = NULL,
  version = NULL,
  dest_dir = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
) {
  workspace_record <- fabric_as_record(workspace_name)
  if (!is.null(workspace_record)) {
    workspace_name <- fabric_record_value(workspace_record, "id", "workspaceId")
  }
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
        version != floor(version)
    ) {
      rlang::abort("version must be a single non-negative integer")
    }
    version <- as.integer(version)
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
  if (is.null(access_token) && is.null(token_provider)) {
    inform(verbose, "Authenticating with {.pkg AzureAuth} (MSAL v2)")
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider
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
    workspace_name,
    lakehouse_name,
    path = table_dir,
    item_type = "Lakehouse",
    dfs_base = dfs_base
  )

  inform(verbose, "Table root: {.path {table_dir}}")

  # ---- list and stage the transaction log ----
  log_target <- target
  log_target$path <- paste0(table_dir, "/_delta_log")
  files <- onelake_list_target(
    log_target,
    credential,
    recursive = TRUE,
    page_size = 5000L
  )
  files <- fabric_delta_file_rows(files)
  if (NROW(files) == 0) {
    rlang::abort(cli::format_inline(
      "No {.path _delta_log} files found under {.path {table_dir}}"
    ))
  }

  auto_cleanup <- is.null(dest_dir)
  dest_dir <- dest_dir %||% fs::path_temp("onelake_tbl_")
  fs::dir_create(dest_dir, recurse = TRUE)
  if (auto_cleanup) {
    on.exit(try(fs::dir_delete(dest_dir), silent = TRUE), add = TRUE)
  }

  file_paths <- if ("path" %in% names(files)) files$path else files$name
  log_staged <- fabric_delta_stage_paths(file_paths, table_dir, dest_dir)
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

  # ---- read the requested Delta snapshot ----
  inform(verbose, "Reading the Delta snapshot with {.pkg duckdb}")
  df <- fabric_delta_read_staged(dest_dir, version = version)

  inform(verbose, "Loaded {nrow(df)} row{?s}", type = "success")
  tibble::as_tibble(df)
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
#' @return A data frame.
#' @keywords internal
#' @noRd
fabric_delta_read_staged <- function(table_dir, version = NULL) {
  snapshot <- fabric_delta_resolve_snapshot(table_dir, version = version)
  if (!length(snapshot$active)) {
    return(data.frame())
  }

  relative <- utils::URLdecode(snapshot$active)
  parts <- strsplit(gsub("\\\\", "/", relative), "/", fixed = TRUE)
  if (
    any(grepl("^[/\\\\]", relative)) ||
      any(vapply(parts, function(x) any(x %in% c("", ".", "..")), logical(1)))
  ) {
    rlang::abort("Delta log contains an unsafe data-file path")
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

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  literals <- as.character(DBI::dbQuoteString(
    con,
    gsub("\\\\", "/", normalizePath(paths, mustWork = TRUE))
  ))
  DBI::dbGetQuery(
    con,
    paste0(
      "SELECT * FROM read_parquet([",
      paste(literals, collapse = ", "),
      "], union_by_name = true, hive_partitioning = true)"
    )
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
  json_versions <- as.numeric(vapply(
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
        version = as.numeric(match[[2L]]),
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
  state$active <- union(state$active, adds[!is.na(adds)])
  state$active <- setdiff(state$active, removes[!is.na(removes)])

  deletion_storage <- checkpoint$add$deletionVector$storageType
  state$has_deletion_vectors <- state$has_deletion_vectors ||
    !all(is.na(deletion_storage))

  protocol_rows <- which(!is.na(checkpoint$protocol$minReaderVersion))
  if (length(protocol_rows)) {
    i <- utils::tail(protocol_rows, 1L)
    state$protocol <- list(
      minReaderVersion = checkpoint$protocol$minReaderVersion[[i]],
      readerFeatures = checkpoint$protocol$readerFeatures[[i]]
    )
  }
  metadata_rows <- which(!is.na(checkpoint$metaData$id))
  if (length(metadata_rows)) {
    i <- utils::tail(metadata_rows, 1L)
    config <- checkpoint$metaData$configuration[[i]]
    configuration <- if (is.null(config) || !NROW(config)) {
      list()
    } else {
      stats::setNames(as.list(config$value), config$key)
    }
    state$metadata <- list(configuration = configuration)
  }
  state
}

#' Apply actions from one Delta JSON commit
#' @keywords internal
#' @noRd
fabric_delta_apply_json_log <- function(state, path) {
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  for (line in lines[nzchar(lines)]) {
    action <- tryCatch(
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
    if (!is.null(action$add$path)) {
      state$active <- union(state$active, action$add$path)
      state$has_deletion_vectors <- state$has_deletion_vectors ||
        !is.null(action$add$deletionVector)
    }
    if (!is.null(action$remove$path)) {
      state$active <- setdiff(state$active, action$remove$path)
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
  features <- unlist(
    state$protocol$readerFeatures %||% list(),
    use.names = FALSE
  )
  if (reader_version > 1 || length(features)) {
    detail <- if (length(features)) {
      paste0(". Reader features: ", paste(features, collapse = ", "))
    } else {
      ""
    }
    rlang::abort(
      paste0(
        "Unsupported Delta reader protocol version ",
        reader_version,
        ". This reader safely supports protocol version 1 only",
        detail
      )
    )
  }

  configuration <- state$metadata$configuration %||% list()
  mapping <- configuration[["delta.columnMapping.mode"]] %||% "none"
  if (!identical(tolower(as.character(mapping)), "none")) {
    rlang::abort(cli::format_inline(
      "Delta column mapping mode {.val {mapping}} is not supported by this reader"
    ))
  }
  if (isTRUE(state$has_deletion_vectors)) {
    rlang::abort(
      "Delta deletion vectors are not supported by this reader"
    )
  }
  invisible(state)
}
