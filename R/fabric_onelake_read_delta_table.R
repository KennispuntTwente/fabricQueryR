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
#'  reader supports Delta reader protocol version 1 without table features. It
#'  does not currently support column mapping, deletion vectors, or higher
#'  reader protocols. These occur in some current Fabric tables, including
#'  Warehouse Delta exports. Such tables are rejected with a
#'  `fabric_delta_unsupported_error` before any data is returned; use the
#'  Lakehouse SQL analytics endpoint or Fabric Spark for those tables.
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
#'  into R memory. For very large tables, a SQL query that filters rows in
#'  Fabric may transfer much less data.
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
#'   version and its active files are still available in OneLake.
#' @param dest_dir Local staging directory for the Delta log and active data
#'   files, or `NULL`. The default creates a temporary directory and removes it
#'   on exit. Supply a directory to retain the downloaded files for inspection
#'   or reuse, and ensure it has enough free space.
#' @param verbose Logical. Show download and read progress.
#' @param dfs_base OneLake DFS endpoint. Keep the default unless using a
#'   regional or workspace-private endpoint.
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
  token = NULL,
  auth_args = list(),
  version = NULL,
  dest_dir = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
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
  dest_dir <- dest_dir %||% fs::file_temp("onelake_tbl_")
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
  schema <- fabric_delta_schema(snapshot$metadata)

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
  if (!length(snapshot$active)) {
    return(DBI::dbGetQuery(
      con,
      paste0(
        "SELECT ",
        paste(projection$empty, collapse = ", "),
        " WHERE FALSE"
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

  literals <- as.character(DBI::dbQuoteString(
    con,
    gsub("\\\\", "/", normalizePath(paths, mustWork = TRUE))
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
  source_column <- "filename"
  if ("filename" %in% physical) {
    source_column <- "fabric_delta_source_path_internal"
    while (source_column %in% physical) {
      source_column <- paste0(source_column, "_")
    }
    quoted_source <- as.character(DBI::dbQuoteIdentifier(con, source_column))
    parquet <- paste(
      vapply(
        seq_along(literals),
        function(index) {
          paste0(
            "SELECT *, ",
            literals[[index]],
            " AS ",
            quoted_source,
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
  } else {
    parquet <- paste0(
      "read_parquet([",
      paste(literals, collapse = ", "),
      "], union_by_name = true, hive_partitioning = false, filename = true)"
    )
  }
  mapping <- fabric_delta_partition_mapping(
    snapshot,
    gsub("\\\\", "/", normalizePath(paths, mustWork = TRUE)),
    schema
  )
  DBI::dbWriteTable(con, "fabric_delta_partitions", mapping, temporary = TRUE)
  source <- paste0(
    parquet,
    " AS delta_source LEFT JOIN fabric_delta_partitions AS delta_partitions ",
    "ON delta_source.",
    as.character(DBI::dbQuoteIdentifier(con, source_column)),
    " = delta_partitions.fabric_delta_source_path"
  )
  selected <- fabric_delta_read_projection(
    con,
    schema,
    physical,
    source_column
  )
  DBI::dbGetQuery(
    con,
    paste0(
      "SELECT ",
      paste(selected, collapse = ", "),
      " FROM ",
      source
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
  unknown_partitions <- setdiff(schema$partitionColumns, field_names)
  if (length(unknown_partitions)) {
    rlang::abort(paste0(
      "Delta metadata references unknown partition column(s): ",
      paste(unknown_partitions, collapse = ", ")
    ))
  }
  schema
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
    values <- lapply(snapshot$active, function(path) {
      record <- files[[path]] %||% list(partitionValues = list())
      map <- record$partitionValues %||% list()
      if (!partition %in% names(map)) {
        rlang::abort(cli::format_inline(
          "Delta file {.path {path}} has no value for partition column {.field {partition}}"
        ))
      }
      map[[partition]]
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
      } else if (name %in% physical) {
        expression <- paste0(
          "delta_source.",
          as.character(DBI::dbQuoteIdentifier(con, name))
        )
      } else {
        expression <- "NULL"
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
  add_rows <- which(!is.na(adds))
  for (i in add_rows) {
    partition_values <- fabric_delta_checkpoint_value(
      checkpoint$add$partitionValues %||% NULL,
      i,
      length(adds)
    )
    state <- fabric_delta_add_file(
      state,
      list(
        path = adds[[i]],
        partitionValues = fabric_delta_partition_values(partition_values)
      )
    )
  }
  for (path in removes[!is.na(removes)]) {
    state <- fabric_delta_remove_file(state, path)
  }

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
    )
  )
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
      state <- fabric_delta_add_file(state, action$add)
      state$has_deletion_vectors <- state$has_deletion_vectors ||
        !is.null(action$add$deletionVector)
    }
    if (!is.null(action$remove$path)) {
      state <- fabric_delta_remove_file(state, action$remove$path)
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

  configuration <- state$metadata$configuration %||% list()
  mapping <- configuration[["delta.columnMapping.mode"]] %||% "none"
  if (!identical(tolower(as.character(mapping)), "none")) {
    fabric_delta_abort_unsupported(
      cli::format_inline(
        "Delta column mapping mode {.val {mapping}} is not supported"
      )
    )
  }
  if (isTRUE(state$has_deletion_vectors)) {
    fabric_delta_abort_unsupported(
      "Delta deletion vectors are not supported"
    )
  }

  if (reader_version > 1 || length(features)) {
    detail <- if (length(features)) {
      paste0(". Reader features: ", paste(features, collapse = ", "))
    } else {
      ""
    }
    fabric_delta_abort_unsupported(
      paste0("Delta reader protocol version ", reader_version, detail)
    )
  }
  invisible(state)
}

fabric_delta_abort_unsupported <- function(feature) {
  rlang::abort(
    c(
      paste0(feature, " by the staged reader."),
      "i" = paste(
        "This reader supports Delta reader protocol version 1 without",
        "table features."
      ),
      "i" = paste(
        "Query the table through its Fabric SQL analytics endpoint or",
        "Fabric Spark instead."
      )
    ),
    class = "fabric_delta_unsupported_error"
  )
}
