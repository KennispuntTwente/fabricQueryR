.fabric_delta_max_exact_version <- 2^53
.fabric_delta_max_exact_version_text <- "00009007199254740992"
.fabric_delta_result_types <- c("tibble", "arrow_stream")
.fabric_delta_supported_reader_features <- c(
  "columnMapping",
  "deletionVectors",
  "timestampNtz",
  "typeWidening",
  "typeWidening-preview",
  "vacuumProtocolCheck",
  "v2Checkpoint",
  "variantType",
  "variantShredding",
  "variantShredding-preview"
)

#' @title
#' Read a Delta table from Microsoft Fabric OneLake
#'
#' @description
#' Downloads a Lakehouse or Warehouse-exported Delta table from OneLake. By
#' default it returns a tibble. It can instead return an Arrow-compatible stream
#' for use with the `arrow` R package and other Arrow tools.
#'
#' Delta tables consist of Parquet data files plus a transaction log that says
#' which files make up the current table. This function reads that log so
#' deleted or superseded files are not accidentally included.
#'
#' @details
#' - In Microsoft Fabric, OneLake exposes each workspace as an ADLS Gen2
#'  filesystem. Within a Lakehouse item, Delta tables are stored under
#'  `Tables/<table>` (non-schema lakehouse) or `Tables/<schema>/<table>`
#'  (schema-enabled lakehouse). The function first stages the transaction log,
#'  then downloads only the Parquet files active in the requested version.
#' - Checkpoint and data Parquet files are read with DuckDB. The staged reader
#'  supports Delta reader protocols 1 through 3; classic, multipart, and V2
#'  checkpoints; name- and ID-based column mapping; inline, relative, and
#'  absolute deletion vectors; timestamps without time zones; supported type
#'  widening; and native Variant values, including Variant shredding. Absolute
#'  AddFile and deletion-vector URIs must point to Microsoft Fabric OneLake.
#'  This includes Fabric shallow clones and the reader 3/writer 7 Warehouse
#'  export profile.
#'  Support is feature-specific, not a blanket claim for every table writable
#'  by Delta Lake 4.2. In particular, experimental Runtime 2.0 features not
#'  listed here are rejected when they add an unknown reader feature.
#' - Warehouse commits are published to Delta logs by a Fabric background
#'  process. For Warehouse items, this function reads the latest *published*
#'  OneLake snapshot, which can briefly lag a just-committed SQL transaction.
#'  Warehouse Delta exports are read-only; writes belong to the Warehouse
#'  engine.
#' - Metadata must declare the Parquet provider with no provider-specific
#'  options. Recursive schema shape, case-insensitive sibling-name uniqueness,
#'  partition columns, and singleton protocol/metadata actions are validated
#'  before data is read. Unrecognised reader features, catalog-managed commits,
#'  non-OneLake absolute URIs, and unsupported schema types fail with a
#'  `fabric_delta_unsupported_error` before data is returned.
#' - The returned columns follow the logical schema in the selected Delta
#'  snapshot. Schema additions are filled with typed missing values, removed
#'  physical columns are omitted, and partition values come from Delta add-file
#'  actions rather than being inferred from directory names. Legacy `void`
#'  fields are retained as logical all-missing columns. Timestamp partition
#'  values without an explicit offset require `timestamp_partition_timezone`
#'  because the Delta log does not record the writer timezone.
#' - Delta `long` values are returned as `bit64::integer64`. Delta decimals are
#'  returned as character vectors, including decimals nested in complex types,
#'  so all 38 digits remain exact. Delta `timestamp_ntz` values use the
#'  character-backed `fabric_delta_timestamp_ntz` class and retain the exact
#'  wall-clock value; use `as.POSIXct(x, tz = "...")` to localise them.
#'  Struct columns use `fabric_delta_struct_column`; `is.na(x)` reports parent
#'  nullness and distinguishes a null struct from a present struct whose
#'  children are all null. Top-level Variant columns are returned as
#'  exact `fabric_delta_variant` cells containing their type, display value, and
#'  Parquet metadata/value bytes. SQL NULL is returned as a missing list element
#'  and remains distinct from a Variant Null cell. Nested Variant fields fail
#'  closed because their independent Parquet validity cannot yet be retained.
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
#' @param workspace_name Fabric workspace display name or GUID, or a record from
#'   [fabric_workspaces()]. GUIDs are safest for scheduled code and names are
#'   convenient interactively.
#' @param lakehouse_name Lakehouse or Warehouse item name, GUID, or discovery
#'   record. A character name may include its `.Lakehouse` or `.Warehouse`
#'   suffix; a discovered record avoids suffix and renaming ambiguity. The
#'   argument name is retained for backward compatibility.
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
#' @param timestamp_partition_timezone Timezone used to interpret legacy Delta
#'   `timestamp` partition values that do not contain a UTC offset. Supply the
#'   timezone of the system that wrote the table, for example `"UTC"` or
#'   `"Europe/Amsterdam"`. The Delta log does not record this timezone, so the
#'   default `NULL` rejects offset-less timestamp partition values rather than
#'   silently returning a shifted instant. ISO8601 partition values containing
#'   `Z` or an explicit offset do not require this argument.
#' @param dest_dir Local staging directory for the Delta log and active data
#'   files, or `NULL`. The default creates a temporary directory and removes it
#'   on exit. Supply a new or empty directory to retain the downloaded files
#'   for inspection, and ensure it has enough free space. Non-empty directories
#'   are rejected so stale files cannot affect snapshot resolution.
#' @param verbose Logical. Show download and read progress.
#' @param dfs_base OneLake DFS endpoint. Keep the default unless using a
#'   regional or workspace-private endpoint.
#' @param columns Optional character vector of logical Delta column names to
#'   return, in the requested order. `NULL` returns every column.
#' @param limit Optional non-negative whole number limiting returned rows.
#'   `NULL` returns every row. This limits DuckDB collection but not OneLake
#'   file downloads.
#' @param result Return format. `"tibble"` returns a tibble.
#'   `"arrow_stream"` returns a `nanoarrow_array_stream` compatible with
#'   [arrow::as_record_batch_reader()] and other Arrow C stream consumers.
#'   The table is still staged and read into local memory before the stream is
#'   created.
#'
#' @return With `result = "tibble"`, a tibble containing the selected Delta
#'   snapshot. With `result = "arrow_stream"`, a single-use
#'   `nanoarrow_array_stream`. Empty tables preserve their column schema in
#'   either format. Delta `long` columns use `bit64::integer64`; decimal columns
#'   use exact character values; `timestamp_ntz` columns use
#'   `fabric_delta_timestamp_ntz`. Struct columns use
#'   `fabric_delta_struct_column`, for which `is.na()` reports parent nullness.
#'   Delta Variant columns are list columns whose
#'   non-missing elements have class `fabric_delta_variant`; each element
#'   retains the exact Parquet Variant metadata and value bytes, its DuckDB
#'   logical type, and a display value. In Arrow results, Variant columns are
#'   structs with `type`, `display`, `metadata`, and `value` fields. SQL NULL
#'   has four missing fields; Variant Null has type `VARIANT_NULL` and non-null
#'   metadata/value bytes.
#' @references
#' [Delta Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
#'
#' [Connect to OneLake with ADLS APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)
#'
#' [Lakehouse schemas](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas)
#'
#' [Delta Lake tables in OneLake](https://learn.microsoft.com/en-us/fabric/fundamentals/delta-lake-interoperability)
#'
#' [Delta Lake logs in Fabric Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-delta-lake-logs)
#'
#' [Schema evolution for Delta tables](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-schema-evolution)
#'
#' [Variant data type for Delta tables](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-variant)
#'
#' [Fabric Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)
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
#'
#' # Return an Arrow-compatible stream instead of a tibble.
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
  timestamp_partition_timezone = NULL,
  dest_dir = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  columns = NULL,
  limit = NULL,
  result = c("tibble", "arrow_stream")
) {
  result <- rlang::arg_match(result, .fabric_delta_result_types)
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
  if (
    !is.null(timestamp_partition_timezone) &&
      (!is.character(timestamp_partition_timezone) ||
        length(timestamp_partition_timezone) != 1L ||
        is.na(timestamp_partition_timezone) ||
        !nzchar(timestamp_partition_timezone))
  ) {
    rlang::abort(
      "timestamp_partition_timezone must be NULL or one non-empty timezone string"
    )
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
  if (identical(result, "arrow_stream")) {
    rlang::check_installed(
      c("arrow", "nanoarrow"),
      reason = "to return result = \"arrow_stream\""
    )
  }

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
    item_type = item_type,
    dfs_base = dfs_base
  )

  inform(verbose, "Table root: {.path {table_dir}}")

  auto_cleanup <- is.null(dest_dir)
  dest_dir <- dest_dir %||% fs::file_temp("onelake_tbl_")
  if (
    fs::dir_exists(dest_dir) &&
      length(fs::dir_ls(dest_dir, all = TRUE, fail = FALSE))
  ) {
    rlang::abort(c(
      "dest_dir must be a new or empty directory",
      "x" = cli::format_inline(
        "{.path {dest_dir}} contains files from an earlier operation"
      )
    ))
  }
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

  fabric_delta_stage_log_selection(
    selection,
    table_dir,
    dest_dir,
    target,
    credential,
    verbose
  )
  snapshot <- tryCatch(
    fabric_delta_resolve_snapshot(dest_dir, version = version),
    error = function(error) error
  )
  if (inherits(snapshot, "fabric_delta_checkpoint_error")) {
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
    checkpoint_sets <- fabric_delta_checkpoint_sets(file_paths)
    checkpoint_versions <- vapply(
      checkpoint_sets,
      `[[`,
      numeric(1),
      "version"
    )
    fallback_versions <- sort(
      unique(checkpoint_versions[
        checkpoint_versions < selection$checkpoint_version &
          checkpoint_versions <= selection$target
      ]),
      decreasing = TRUE
    )
    fallback_versions <- c(as.list(fallback_versions), list(NA_real_))
    for (fallback_version in fallback_versions) {
      fallback <- fabric_delta_select_log_paths(
        file_paths,
        version = selection$target,
        checkpoint_version_override = fallback_version
      )
      fabric_delta_stage_log_selection(
        fallback,
        table_dir,
        dest_dir,
        target,
        credential,
        verbose
      )
      snapshot <- tryCatch(
        fabric_delta_resolve_snapshot(dest_dir, version = version),
        error = function(error) error
      )
      if (!inherits(snapshot, "fabric_delta_checkpoint_error")) {
        break
      }
    }
  }
  if (inherits(snapshot, "error")) {
    rlang::cnd_signal(snapshot)
  }
  if (length(snapshot$active)) {
    data_staged <- fabric_delta_stage_files(
      unique(snapshot$active),
      target,
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
    deletion_staged <- fabric_delta_stage_files(
      unique(deletion_vector_paths),
      target,
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
  read_args <- list(
    table_dir = dest_dir,
    version = version,
    columns = columns,
    limit = limit
  )
  if (!is.null(timestamp_partition_timezone)) {
    read_args$timestamp_partition_timezone <- timestamp_partition_timezone
  }
  df <- do.call(fabric_delta_read_staged, read_args)

  inform(verbose, "Loaded {nrow(df)} row{?s}", type = "success")
  fabric_delta_format_result(df, result)
}

#' Format a staged Delta result as a tibble or Arrow C stream
#' @keywords internal
#' @noRd
fabric_delta_format_result <- function(value, result) {
  if (identical(result, "arrow_stream")) {
    value <- fabric_delta_arrow_compatible(value)
    table <- do.call(arrow::Table$create, value)
    return(nanoarrow::as_nanoarrow_array_stream(table))
  }
  tibble::as_tibble(value)
}

#' Convert exact Delta nested columns to validity-preserving Arrow arrays
#' @keywords internal
#' @noRd
fabric_delta_arrow_compatible <- function(value) {
  columns <- as.list(value)
  for (name in names(value)) {
    column <- value[[name]]
    if (inherits(column, "fabric_delta_timestamp_ntz")) {
      columns[[name]] <- arrow::Array$create(
        unclass(column),
        type = arrow::utf8()
      )$cast(arrow::timestamp("us"))
      next
    }
    if (inherits(column, "fabric_delta_struct_column")) {
      validity <- attr(
        column,
        "fabric_delta_struct_validity",
        exact = TRUE
      )
      fields <- fabric_delta_arrow_compatible(column)
      struct <- do.call(arrow::StructArray$create, fields)
      columns[[name]] <- arrow::call_function(
        "if_else",
        arrow::Array$create(validity, type = arrow::boolean()),
        struct,
        arrow::Scalar$create(NULL)
      )
      next
    }
    if (!inherits(column, "fabric_delta_variant_column")) {
      next
    }
    fields <- list(
      type = arrow::Array$create(
        vapply(
          column,
          function(cell) if (is.null(cell)) NA_character_ else cell$type,
          character(1)
        ),
        type = arrow::utf8()
      ),
      display = arrow::Array$create(
        vapply(
          column,
          function(cell) if (is.null(cell)) NA_character_ else cell$display,
          character(1)
        ),
        type = arrow::utf8()
      ),
      metadata = arrow::Array$create(
        lapply(
          column,
          function(cell) if (is.null(cell)) NULL else cell$metadata
        ),
        type = arrow::binary()
      ),
      value = arrow::Array$create(
        lapply(
          column,
          function(cell) if (is.null(cell)) NULL else cell$value
        ),
        type = arrow::binary()
      )
    )
    columns[[name]] <- do.call(arrow::StructArray$create, fields)
  }
  columns
}

#' Format a Delta timestamp without time zone as wall-clock text
#' @param x A Delta `timestamp_ntz` vector.
#' @param format Optional output format.
#' @param ... Additional arguments passed to [base::format.POSIXct()] when
#'   `format` is supplied.
#' @return Character wall-clock timestamps.
#' @export
format.fabric_delta_timestamp_ntz <- function(x, format = NULL, ...) {
  value <- unclass(x)
  if (is.null(format)) {
    return(value)
  }
  format(
    as.POSIXct.fabric_delta_timestamp_ntz(x, tz = "UTC"),
    format = format,
    tz = "UTC",
    ...
  )
}

#' Localize a Delta timestamp without time zone
#' @param x A Delta `timestamp_ntz` vector.
#' @param tz IANA timezone in which to interpret the wall-clock values.
#' @param ... Unused.
#' @return A `POSIXct` vector localized in `tz`.
#' @export
as.POSIXct.fabric_delta_timestamp_ntz <- function(x, tz = "UTC", ...) {
  as.POSIXct(
    unclass(x),
    format = "%Y-%m-%d %H:%M:%OS",
    tz = tz
  )
}

#' @export
#' @noRd
`[.fabric_delta_timestamp_ntz` <- function(x, ...) {
  structure(NextMethod("["), class = class(x))
}

#' Test parent nullness for a Delta struct column
#' @param x A nested Delta struct column.
#' @return A logical vector that is `TRUE` for null parent structs.
#' @export
is.na.fabric_delta_struct_column <- function(x) {
  !attr(x, "fabric_delta_struct_validity", exact = TRUE)
}

#' Subset a Delta struct column while retaining its parent validity
#' @keywords internal
#' @noRd
#' @export
`[.fabric_delta_struct_column` <- function(x, i, j, drop = FALSE) {
  validity <- attr(x, "fabric_delta_struct_validity", exact = TRUE)
  row_index <- seq_len(nrow(x))
  column_only <- nargs() == 2L
  selected_rows <- if (column_only || missing(i)) {
    row_index
  } else {
    row_index[i]
  }
  result <- NextMethod("[")
  if (is.data.frame(result)) {
    class(result) <- unique(c(
      "fabric_delta_struct_column",
      class(result)
    ))
    attr(result, "fabric_delta_struct_validity") <- validity[selected_rows]
  }
  result
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
fabric_delta_select_log_paths <- function(
  paths,
  version = NULL,
  checkpoint_version_override = NULL
) {
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
  checkpoint_version <- if (is.null(checkpoint_version_override)) {
    if (length(eligible)) max(eligible) else NULL
  } else if (is.na(checkpoint_version_override)) {
    NULL
  } else {
    if (!checkpoint_version_override %in% eligible) {
      rlang::abort(
        "The requested fallback Delta checkpoint is not available"
      )
    }
    as.numeric(checkpoint_version_override)
  }
  checkpoint_paths <- if (!is.null(checkpoint_version)) {
    checkpoint <- checkpoints[[match(checkpoint_version, checkpoint_versions)]]
    candidates <- checkpoint$alternatives %||% list(checkpoint)
    unique(unlist(lapply(candidates, `[[`, "paths"), use.names = FALSE))
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

#' Stage one Delta checkpoint candidate and its JSON tail
#' @keywords internal
#' @noRd
fabric_delta_stage_log_selection <- function(
  selection,
  table_dir,
  dest_dir,
  target,
  credential,
  verbose
) {
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
  missing_logs <- !fs::file_exists(log_staged$destination)
  if (any(missing_logs)) {
    inform(
      verbose,
      paste0(
        "Downloading {sum(missing_logs)} Delta log file{?s} to ",
        "{.path {dest_dir}}"
      )
    )
    fabric_delta_download_staged(
      log_staged[missing_logs, , drop = FALSE],
      target,
      credential
    )
  }
  checkpoint_sidecars <- fabric_delta_checkpoint_sidecar_paths(
    log_staged$destination,
    target = target,
    table_dir = table_dir
  )
  if (length(checkpoint_sidecars)) {
    sidecar_staged <- fabric_delta_stage_paths(
      paste0(
        table_dir,
        "/_delta_log/_sidecars/",
        checkpoint_sidecars
      ),
      table_dir,
      dest_dir
    )
    missing_sidecars <- !fs::file_exists(sidecar_staged$destination)
    if (any(missing_sidecars)) {
      inform(
        verbose,
        "Downloading {sum(missing_sidecars)} checkpoint sidecar file{?s}"
      )
      for (index in which(missing_sidecars)) {
        try(
          fabric_delta_download_staged(
            sidecar_staged[index, , drop = FALSE],
            target,
            credential
          ),
          silent = TRUE
        )
      }
    }
  }
  invisible(log_staged)
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
  purrr::walk(
    seq_len(nrow(staged)),
    function(index) {
      file_target <- if ("target" %in% names(staged)) {
        staged$target[[index]]
      } else {
        current <- target
        current$path <- staged$source[[index]]
        current
      }
      onelake_download_target(
        file_target,
        credential,
        dest = staged$destination[[index]],
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

#' Map relative or absolute OneLake Delta files to local staging paths
#' @keywords internal
#' @noRd
fabric_delta_stage_files <- function(paths, target, table_dir, dest_dir) {
  records <- lapply(paths, function(path) {
    path <- as.character(path)
    decoded <- utils::URLdecode(path)
    if (grepl("^(?:https|abfss?)://", decoded, ignore.case = TRUE)) {
      file_target <- onelake_parse_uri(decoded)
      relative <- fabric_delta_external_relative(file_target)
    } else {
      parts <- strsplit(gsub("\\\\", "/", decoded), "/", fixed = TRUE)[[1L]]
      if (
        grepl("^[/\\\\]", decoded) ||
          any(!nzchar(parts) | parts %in% c(".", ".."))
      ) {
        rlang::abort("Delta log contains an unsafe data-file path")
      }
      file_target <- target
      file_target$path <- paste0(table_dir, "/", decoded)
      file_target$.encoded_path <- paste0(
        onelake_encode_path(table_dir),
        "/",
        fabric_delta_encode_uri_path(path)
      )
      relative <- decoded
    }
    list(
      source = file_target$path,
      relative = relative,
      destination = fs::path(dest_dir, relative),
      target = file_target
    )
  })
  data.frame(
    source = vapply(records, `[[`, character(1), "source"),
    relative = vapply(records, `[[`, character(1), "relative"),
    destination = fs::path(vapply(
      records,
      `[[`,
      character(1),
      "destination"
    )),
    target = I(lapply(records, `[[`, "target")),
    stringsAsFactors = FALSE
  )
}

#' Encode a Delta URI path without decoding its existing percent escapes
#' @keywords internal
#' @noRd
fabric_delta_encode_uri_path <- function(path) {
  path <- gsub("\\\\", "/", as.character(path))
  segments <- strsplit(path, "/", fixed = TRUE)[[1L]]
  if (!length(segments) || !all(nzchar(segments))) {
    rlang::abort("Delta log contains an unsafe data-file path")
  }
  encoded <- vapply(
    segments,
    function(segment) {
      invalid <- grepl(
        "%(?![[:xdigit:]]{2})",
        segment,
        perl = TRUE
      )
      if (invalid) {
        rlang::abort("Delta log contains an invalid percent-encoded path")
      }
      matches <- gregexpr(
        "%[[:xdigit:]]{2}",
        segment,
        perl = TRUE
      )[[1L]]
      if (matches[[1L]] == -1L) {
        return(utils::URLencode(segment, reserved = TRUE))
      }
      lengths <- attr(matches, "match.length")
      pieces <- character()
      cursor <- 1L
      for (index in seq_along(matches)) {
        start <- matches[[index]]
        if (start > cursor) {
          pieces <- c(
            pieces,
            utils::URLencode(
              substr(segment, cursor, start - 1L),
              reserved = TRUE
            )
          )
        }
        pieces <- c(
          pieces,
          toupper(substr(
            segment,
            start,
            start + lengths[[index]] - 1L
          ))
        )
        cursor <- start + lengths[[index]]
      }
      if (cursor <= nchar(segment)) {
        pieces <- c(
          pieces,
          utils::URLencode(
            substr(segment, cursor, nchar(segment)),
            reserved = TRUE
          )
        )
      }
      paste0(pieces, collapse = "")
    },
    character(1),
    USE.NAMES = FALSE
  )
  paste(encoded, collapse = "/")
}

#' Derive a collision-resistant local path for an absolute OneLake URI
#' @keywords internal
#' @noRd
fabric_delta_external_relative <- function(target) {
  canonical <- paste(
    sub("/+$", "", target$dfs_base),
    target$workspace,
    target$item,
    sub("^/+", "", target$path),
    sep = "/"
  )
  extension <- fs::path_ext(target$path)
  suffix <- if (nzchar(extension)) paste0(".", extension) else ""
  fs::path(
    "_delta_log",
    ".fabricqueryr-external",
    paste0(
      digest::digest(canonical, algo = "sha256", serialize = FALSE),
      suffix
    )
  )
}

#' Resolve a transaction-log data path to its staged local file
#' @keywords internal
#' @noRd
fabric_delta_local_file <- function(table_dir, path) {
  decoded <- utils::URLdecode(as.character(path))
  absolute <- grepl(
    "^(?:https|abfss?)://",
    decoded,
    ignore.case = TRUE
  )
  relative <- if (absolute) {
    fabric_delta_external_relative(onelake_parse_uri(decoded))
  } else {
    parts <- strsplit(gsub("\\\\", "/", decoded), "/", fixed = TRUE)[[1L]]
    if (
      grepl("^[/\\\\]", decoded) ||
        any(!nzchar(parts) | parts %in% c(".", ".."))
    ) {
      rlang::abort("Delta log contains an unsafe data-file path")
    }
    decoded
  }
  fs::path(table_dir, relative)
}

#' Read a locally staged Delta snapshot
#' @param table_dir Local Delta table root.
#' @param version Optional Delta table version.
#' @param columns Optional logical columns to return.
#' @param limit Optional maximum number of rows to return.
#' @param timestamp_partition_timezone Writer timezone for offset-less
#'   `timestamp` partition values.
#' @return A data frame.
#' @keywords internal
#' @noRd
fabric_delta_read_staged <- function(
  table_dir,
  version = NULL,
  columns = NULL,
  limit = NULL,
  timestamp_partition_timezone = NULL
) {
  snapshot <- fabric_delta_resolve_snapshot(table_dir, version = version)
  schema <- fabric_delta_schema(snapshot$metadata)
  requirements <- fabric_delta_schema_requirements(schema)
  if (requirements$variant) {
    rlang::check_installed(
      "arrow",
      reason = paste(
        "to preserve SQL NULL separately from Delta Variant null values"
      )
    )
  }
  fabric_delta_validate_type_widening(
    schema,
    unlist(
      snapshot$protocol$readerFeatures %||% list(),
      use.names = FALSE
    )
  )

  relative <- snapshot$active
  con <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = ":memory:",
    bigint = "integer64"
  )
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, "SET preserve_insertion_order = true")
  if (fabric_delta_schema_has_timestamp(schema)) {
    fabric_delta_load_icu(con)
  }
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
    empty <- DBI::dbGetQuery(
      con,
      paste0(
        "SELECT ",
        paste(projection$empty[selected_indexes], collapse = ", "),
        " WHERE FALSE",
        limit_sql
      )
    )
    empty <- fabric_delta_mark_variant_columns(
      empty,
      schema$fields[selected_indexes]
    )
    for (index in selected_indexes) {
      field <- schema$fields[[index]]
      empty[[field$name]] <- fabric_delta_restore_timestamp_ntz(
        empty[[field$name]],
        field$type
      )
    }
    selected_schema <- schema
    selected_schema$fields <- schema$fields[selected_indexes]
    selected_schema$partitionColumns <- intersect(
      schema$partitionColumns,
      projection$names[selected_indexes]
    )
    attr(empty, "fabric_delta_schema") <- selected_schema
    return(empty)
  }
  variant_fields <- Filter(
    function(field) {
      is.character(field$type) &&
        length(field$type) == 1L &&
        identical(tolower(field$type), "variant")
    },
    schema$fields[selected_indexes]
  )
  has_active_deletion_vectors <- any(vapply(
    snapshot$active,
    function(path) {
      !is.null(snapshot$files[[path]]$deletionVector)
    },
    logical(1)
  ))
  needs_file_rows <- length(variant_fields) || has_active_deletion_vectors

  paths <- vapply(
    relative,
    function(path) fabric_delta_local_file(table_dir, path),
    character(1)
  )
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
  id_mappings <- if (identical(schema$columnMappingMode, "id")) {
    fabric_delta_validate_id_mapping(con, normalized_paths, schema)
  } else {
    NULL
  }
  literals <- as.character(DBI::dbQuoteString(
    con,
    normalized_paths
  ))
  physical <- if (is.null(id_mappings)) {
    parquet_without_source <- paste0(
      "read_parquet([",
      paste(literals, collapse = ", "),
      "], union_by_name = true, hive_partitioning = false)"
    )
    DBI::dbGetQuery(
      con,
      paste0("DESCRIBE SELECT * FROM ", parquet_without_source)
    )$column_name
  } else {
    vapply(
      schema$fields[
        !vapply(
          schema$fields,
          function(field) field$name %in% schema$partitionColumns,
          logical(1)
        )
      ],
      fabric_delta_field_physical_name,
      character(1),
      mapping_mode = "id"
    )
  }
  source_column <- "fabric_delta_source_path_internal"
  while (source_column %in% c(physical, projection$names)) {
    source_column <- paste0(source_column, "_")
  }
  row_column <- "fabric_delta_row_index_internal"
  while (row_column %in% c(physical, projection$names, source_column)) {
    row_column <- paste0(row_column, "_")
  }
  quoted_source <- as.character(DBI::dbQuoteIdentifier(con, source_column))
  quoted_row <- as.character(DBI::dbQuoteIdentifier(con, row_column))
  parquet <- paste(
    vapply(
      seq_along(literals),
      function(index) {
        parquet_source <- paste0(
          "read_parquet(",
          literals[[index]],
          ", hive_partitioning = false)"
        )
        if (needs_file_rows) {
          file_schema <- DBI::dbGetQuery(
            con,
            paste0("DESCRIBE SELECT * FROM ", parquet_source)
          )
        }
        row_source <- "file_row_number"
        collision <- needs_file_rows &&
          row_source %in% file_schema$column_name
        if (collision) {
          row_source <- "fabric_delta_physical_row_internal"
          while (
            row_source %in%
              c(
                file_schema$column_name,
                physical,
                projection$names,
                source_column,
                row_column
              )
          ) {
            row_source <- paste0(row_source, "_")
          }
        }
        quoted_row_source <- as.character(DBI::dbQuoteIdentifier(
          con,
          row_source
        ))
        numbered_source <- if (collision) {
          paste0(
            "(SELECT *, row_number() OVER () - 1 AS ",
            quoted_row_source,
            " FROM ",
            parquet_source,
            ")"
          )
        } else if (needs_file_rows) {
          paste0(
            "read_parquet(",
            literals[[index]],
            ", hive_partitioning = false, file_row_number = true)"
          )
        } else {
          parquet_source
        }
        file_columns <- if (is.null(id_mappings) && needs_file_rows) {
          paste0("* EXCLUDE (", quoted_row_source, ")")
        } else if (is.null(id_mappings)) {
          "*"
        } else {
          fabric_delta_id_file_projection(
            con,
            schema,
            id_mappings[[index]]
          )
        }
        paste0(
          "SELECT ",
          file_columns,
          if (nzchar(file_columns)) ", " else "",
          literals[[index]],
          " AS ",
          quoted_source,
          ", ",
          if (needs_file_rows) quoted_row_source else "0::BIGINT",
          " AS ",
          quoted_row,
          " FROM ",
          numbered_source
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
  mapping <- fabric_delta_normalize_timestamp_partitions(
    mapping,
    schema,
    timestamp_partition_timezone
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
  variant_masks <- if (length(variant_fields)) {
    fabric_delta_variant_null_masks(paths, variant_fields, schema)
  } else {
    NULL
  }
  struct_mask_fields <- Filter(
    function(index) {
      fabric_delta_type_has_struct(schema$fields[[index]]$type)
    },
    selected_indexes
  )
  struct_masks <- lapply(struct_mask_fields, function(index) {
    field <- schema$fields[[index]]
    physical_name <- fabric_delta_field_physical_name(
      field,
      schema$columnMappingMode
    )
    source_expression <- if (
      field$name %in% schema$partitionColumns || !physical_name %in% physical
    ) {
      "NULL"
    } else {
      paste0(
        "delta_source.",
        as.character(DBI::dbQuoteIdentifier(con, physical_name))
      )
    }
    mask_name <- paste0("fabric_delta_struct_mask_internal_", index)
    while (
      mask_name %in%
        c(
          projection$names,
          source_column,
          row_column
        )
    ) {
      mask_name <- paste0(mask_name, "_")
    }
    list(
      index = index,
      name = mask_name,
      expression = paste0(
        fabric_delta_struct_mask_expression(
          con,
          field$type,
          source_expression,
          schema$columnMappingMode
        ),
        " AS ",
        as.character(DBI::dbQuoteIdentifier(con, mask_name))
      )
    )
  })
  internal <- if (length(variant_fields)) {
    c(
      paste0("delta_source.", quoted_source, " AS ", quoted_source),
      paste0("delta_source.", quoted_row, " AS ", quoted_row)
    )
  } else {
    character()
  }
  internal <- c(
    internal,
    vapply(struct_masks, `[[`, character(1), "expression")
  )
  result <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT ",
      paste(c(selected, internal), collapse = ", "),
      " FROM ",
      source,
      " WHERE delta_deletions.fabric_delta_row_index IS NULL",
      limit_sql
    )
  )
  if (length(variant_fields)) {
    result <- fabric_delta_restore_variants(
      result,
      variant_fields,
      variant_masks,
      source_column,
      row_column
    )
  }
  for (mask in struct_masks) {
    field <- schema$fields[[mask$index]]
    result[[field$name]] <- fabric_delta_apply_struct_mask(
      result[[field$name]],
      field$type,
      result[[mask$name]]
    )
    result[[mask$name]] <- NULL
  }
  for (index in selected_indexes) {
    field <- schema$fields[[index]]
    result[[field$name]] <- fabric_delta_restore_timestamp_ntz(
      result[[field$name]],
      field$type
    )
  }
  selected_schema <- schema
  selected_schema$fields <- schema$fields[selected_indexes]
  selected_schema$partitionColumns <- intersect(
    schema$partitionColumns,
    projection$names[selected_indexes]
  )
  attr(result, "fabric_delta_schema") <- selected_schema
  result
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
  fabric_delta_validate_schema_type(schema, path = "<root>")
  field_names <- vapply(schema$fields, `[[`, character(1), "name")
  partition_columns <- metadata$partitionColumns %||% list()
  if (
    !is.list(partition_columns) &&
      !is.character(partition_columns)
  ) {
    rlang::abort("Delta metadata partitionColumns must be an array of names")
  }
  schema$partitionColumns <- unlist(
    partition_columns,
    use.names = FALSE
  )
  if (
    length(schema$partitionColumns) &&
      (!is.character(schema$partitionColumns) ||
        anyNA(schema$partitionColumns) ||
        !all(nzchar(schema$partitionColumns)) ||
        anyDuplicated(tolower(schema$partitionColumns)))
  ) {
    rlang::abort(
      "Delta metadata partitionColumns contains invalid or duplicate names"
    )
  }
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

#' Validate the recursive shape and sibling names of a Delta schema type
#' @keywords internal
#' @noRd
fabric_delta_validate_schema_type <- function(type, path) {
  if (
    is.character(type) && length(type) == 1L && !is.na(type) && nzchar(type)
  ) {
    return(invisible(type))
  }
  if (!is.list(type)) {
    rlang::abort(paste0("Delta schema type at ", path, " is invalid"))
  }
  kind <- tolower(as.character(type$type %||% ""))
  if (length(kind) != 1L || is.na(kind) || !nzchar(kind)) {
    rlang::abort(paste0("Delta schema type at ", path, " is invalid"))
  }
  if (identical(kind, "struct")) {
    fields <- type$fields %||% NULL
    if (!is.list(fields)) {
      rlang::abort(paste0("Delta struct at ", path, " has invalid fields"))
    }
    names <- vapply(
      fields,
      function(field) {
        if (!is.list(field)) {
          return("")
        }
        name <- field$name %||% ""
        if (
          !is.character(name) ||
            length(name) != 1L ||
            is.na(name)
        ) {
          return("")
        }
        name
      },
      character(1)
    )
    if (!all(nzchar(names)) || anyDuplicated(tolower(names))) {
      prefix <- if (identical(path, "<root>")) {
        "Delta metadata schema"
      } else {
        paste0("Delta struct at ", path)
      }
      rlang::abort(
        paste0(prefix, " contains missing or duplicate field names")
      )
    }
    for (index in seq_along(fields)) {
      field <- fields[[index]]
      if (is.null(field$type)) {
        rlang::abort(paste0(
          "Delta schema field ",
          paste0(path, ".", names[[index]]),
          " has no type"
        ))
      }
      nullable <- field$nullable %||% TRUE
      if (!is.logical(nullable) || length(nullable) != 1L || is.na(nullable)) {
        rlang::abort(paste0(
          "Delta schema field ",
          paste0(path, ".", names[[index]]),
          " has invalid nullability"
        ))
      }
      metadata <- field$metadata %||% list()
      if (!is.list(metadata)) {
        rlang::abort(paste0(
          "Delta schema field ",
          paste0(path, ".", names[[index]]),
          " has invalid metadata"
        ))
      }
      fabric_delta_validate_schema_type(
        field$type,
        paste0(path, ".", names[[index]])
      )
    }
    return(invisible(type))
  }
  if (identical(kind, "array")) {
    if (is.null(type$elementType)) {
      rlang::abort(paste0("Delta array at ", path, " has no elementType"))
    }
    contains_null <- type$containsNull %||% TRUE
    if (
      !is.logical(contains_null) ||
        length(contains_null) != 1L ||
        is.na(contains_null)
    ) {
      rlang::abort(paste0("Delta array at ", path, " has invalid nullability"))
    }
    fabric_delta_validate_schema_type(
      type$elementType,
      paste0(path, ".element")
    )
    return(invisible(type))
  }
  if (identical(kind, "map")) {
    if (is.null(type$keyType) || is.null(type$valueType)) {
      rlang::abort(paste0("Delta map at ", path, " has no key/value type"))
    }
    value_contains_null <- type$valueContainsNull %||% TRUE
    if (
      !is.logical(value_contains_null) ||
        length(value_contains_null) != 1L ||
        is.na(value_contains_null)
    ) {
      rlang::abort(paste0("Delta map at ", path, " has invalid nullability"))
    }
    fabric_delta_validate_schema_type(type$keyType, paste0(path, ".key"))
    fabric_delta_validate_schema_type(type$valueType, paste0(path, ".value"))
    return(invisible(type))
  }
  rlang::abort(paste0(
    "Delta schema at ",
    path,
    " contains unsupported complex type ",
    kind
  ))
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
  resolve_path <- function(type, path) {
    current <- type
    if (nzchar(path)) {
      components <- strsplit(path, ".", fixed = TRUE)[[1L]]
      if (!all(nzchar(components))) {
        fabric_delta_abort_unsupported(
          paste0("Delta type-widening fieldPath ", path, " is invalid")
        )
      }
      for (component in components) {
        if (!is.list(current)) {
          fabric_delta_abort_unsupported(
            paste0(
              "Delta type-widening fieldPath ",
              path,
              " does not resolve through the current schema"
            )
          )
        }
        kind <- tolower(as.character(current$type %||% ""))
        current <- if (
          identical(kind, "array") &&
            identical(component, "element")
        ) {
          current$elementType
        } else if (
          identical(kind, "map") &&
            identical(component, "key")
        ) {
          current$keyType
        } else if (
          identical(kind, "map") &&
            identical(component, "value")
        ) {
          current$valueType
        } else {
          fabric_delta_abort_unsupported(
            paste0(
              "Delta type-widening fieldPath ",
              path,
              " does not resolve through the current schema"
            )
          )
        }
      }
    }
    if (!is.character(current) || length(current) != 1L || is.na(current)) {
      fabric_delta_abort_unsupported(
        paste0(
          "Delta type-widening fieldPath ",
          if (nzchar(path)) path else "<field>",
          " does not identify a primitive field"
        )
      )
    }
    tolower(current)
  }
  visit_field <- function(field) {
    changes <- field$metadata[["delta.typeChanges"]] %||% list()
    if (length(changes)) {
      paths <- vapply(
        changes,
        function(change) {
          path <- change$fieldPath %||% ""
          if (
            !is.character(path) ||
              length(path) != 1L ||
              is.na(path)
          ) {
            fabric_delta_abort_unsupported(
              "Delta type-widening fieldPath is invalid"
            )
          }
          path
        },
        character(1)
      )
      for (path in unique(paths)) {
        path_changes <- changes[paths == path]
        previous_to <- NULL
        for (change in path_changes) {
          from <- tolower(as.character(change$fromType %||% ""))
          to <- tolower(as.character(change$toType %||% ""))
          if (
            length(from) != 1L ||
              is.na(from) ||
              length(to) != 1L ||
              is.na(to) ||
              !fabric_delta_supported_type_change(
                from,
                to,
                preview = preview
              )
          ) {
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
          if (!is.null(previous_to) && !identical(from, previous_to)) {
            fabric_delta_abort_unsupported(
              paste0(
                "Delta type-widening history for fieldPath ",
                if (nzchar(path)) path else "<field>",
                " is not contiguous"
              )
            )
          }
          previous_to <- to
        }
        current <- resolve_path(field$type, path)
        if (!identical(previous_to, current)) {
          fabric_delta_abort_unsupported(
            paste0(
              "Delta type-widening history for fieldPath ",
              if (nzchar(path)) path else "<field>",
              " ends at ",
              previous_to,
              " but the current schema type is ",
              current
            )
          )
        }
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
    valid_decimals <- from_precision <= 38 &&
      to_precision <= 38 &&
      from_scale <= from_precision &&
      to_scale <= to_precision
    if (!valid_decimals) {
      return(FALSE)
    }
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
      to_precision <- as.numeric(to_parts[[2L]])
      to_scale <- as.numeric(to_parts[[3L]])
      required_integer_digits <- if (identical(from, "long")) 20 else 10
      return(
        to_precision <= 38 &&
          to_scale <= to_precision &&
          to_precision >= required_integer_digits + to_scale
      )
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
      timestamp_ntz = "TIMESTAMP",
      void = "BOOLEAN",
      variant = "VARIANT"
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

#' Translate a mapped Delta type using its physical Parquet field names
#' @keywords internal
#' @noRd
fabric_delta_duckdb_physical_type <- function(
  con,
  type,
  mapping_mode = "none"
) {
  if (is.character(type) && length(type) == 1L) {
    return(fabric_delta_duckdb_type(con, type))
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
          as.character(DBI::dbQuoteIdentifier(
            con,
            fabric_delta_field_physical_name(field, mapping_mode)
          )),
          fabric_delta_duckdb_physical_type(
            con,
            field$type,
            mapping_mode
          )
        )
      },
      character(1)
    )
    return(paste0("STRUCT(", paste(definitions, collapse = ", "), ")"))
  }
  if (identical(kind, "array")) {
    return(paste0(
      fabric_delta_duckdb_physical_type(
        con,
        type$elementType,
        mapping_mode
      ),
      "[]"
    ))
  }
  if (identical(kind, "map")) {
    return(paste0(
      "MAP(",
      fabric_delta_duckdb_physical_type(con, type$keyType, mapping_mode),
      ", ",
      fabric_delta_duckdb_physical_type(con, type$valueType, mapping_mode),
      ")"
    ))
  }
  rlang::abort(paste0("Unsupported Delta complex schema type: ", kind))
}

#' Translate a Delta type into an exact R-facing DuckDB result type
#'
#' DuckDB's R client preserves BIGINT as `bit64::integer64` when the connection
#' requests it, but currently materializes DECIMAL values as binary doubles.
#' Project DECIMAL values to text at every nesting level so no precision is
#' discarded while crossing the DBI boundary.
#' @keywords internal
#' @noRd
fabric_delta_duckdb_result_type <- function(con, type) {
  if (is.character(type) && length(type) == 1L) {
    normalized <- tolower(type)
    if (
      grepl("^decimal\\([0-9]+,[0-9]+\\)$", normalized) ||
        identical(normalized, "timestamp_ntz")
    ) {
      return("VARCHAR")
    }
    return(fabric_delta_duckdb_type(con, type))
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
          fabric_delta_duckdb_result_type(con, field$type)
        )
      },
      character(1)
    )
    return(paste0("STRUCT(", paste(definitions, collapse = ", "), ")"))
  }
  if (identical(kind, "array")) {
    return(paste0(
      fabric_delta_duckdb_result_type(con, type$elementType),
      "[]"
    ))
  }
  if (identical(kind, "map")) {
    return(paste0(
      "MAP(",
      fabric_delta_duckdb_result_type(con, type$keyType),
      ", ",
      fabric_delta_duckdb_result_type(con, type$valueType),
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
    function(field) fabric_delta_duckdb_result_type(con, field$type),
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

#' Collect ID-mapped fields from a Delta schema
#' @keywords internal
#' @noRd
fabric_delta_id_fields <- function(schema) {
  collect_type <- function(type) {
    if (!is.list(type)) {
      return(list())
    }
    kind <- tolower(as.character(type$type %||% ""))
    if (identical(kind, "struct")) {
      return(collect(type$fields %||% list(), top_level = FALSE))
    }
    if (identical(kind, "array")) {
      return(collect_type(type$elementType))
    }
    if (identical(kind, "map")) {
      return(c(
        collect_type(type$keyType),
        collect_type(type$valueType)
      ))
    }
    list()
  }
  collect <- function(fields, top_level = FALSE) {
    unlist(
      lapply(fields, function(field) {
        is_partition <- top_level &&
          as.character(field$name) %in% schema$partitionColumns
        own <- if (is_partition) {
          list()
        } else {
          id <- field$metadata[["delta.columnMapping.id"]] %||% NULL
          if (
            is.null(id) ||
              length(id) != 1L ||
              is.na(id) ||
              !is.numeric(id) ||
              id < 0 ||
              id != floor(id)
          ) {
            fabric_delta_abort_unsupported(
              paste0(
                "Delta ID column mapping field ",
                as.character(field$name),
                " has no valid column ID"
              )
            )
          }
          list(list(
            id = as.numeric(id),
            physical = fabric_delta_field_physical_name(field, "id")
          ))
        }
        nested <- collect_type(field$type)
        c(own, nested)
      }),
      recursive = FALSE
    )
  }
  collect(schema$fields, top_level = TRUE)
}

#' Validate ID-mapped Delta fields against Parquet field IDs
#' @keywords internal
#' @noRd
fabric_delta_validate_id_mapping <- function(con, paths, schema) {
  expected <- fabric_delta_id_fields(schema)
  expected_ids <- vapply(expected, `[[`, numeric(1), "id")
  if (anyDuplicated(expected_ids)) {
    fabric_delta_abort_unsupported(
      "Delta ID column mapping metadata contains duplicate column IDs"
    )
  }
  mappings <- lapply(paths, function(path) {
    literal <- as.character(DBI::dbQuoteString(con, path))
    parquet <- DBI::dbGetQuery(
      con,
      paste0(
        "SELECT name, field_id FROM parquet_schema(",
        literal,
        ")"
      )
    )
    present_ids <- !is.na(parquet$field_id)
    parquet_ids <- as.numeric(parquet$field_id[present_ids])
    parquet_names <- as.character(parquet$name[present_ids])
    if (length(expected) && !length(parquet_ids)) {
      fabric_delta_abort_unsupported(
        cli::format_inline(
          "Parquet file {.path {path}} has no field IDs for Delta ID column mapping"
        )
      )
    }
    if (anyDuplicated(parquet_ids)) {
      fabric_delta_abort_unsupported(
        cli::format_inline(
          "Parquet file {.path {path}} contains duplicate field IDs"
        )
      )
    }
    stats::setNames(parquet_names, as.character(parquet_ids))
  })
  mappings
}

#' Project one ID-mapped Parquet file into canonical physical names
#' @keywords internal
#' @noRd
fabric_delta_id_file_projection <- function(con, schema, mapping) {
  fields <- Filter(
    function(field) !field$name %in% schema$partitionColumns,
    schema$fields
  )
  paste(
    vapply(
      fields,
      function(field) {
        id <- as.character(field$metadata[["delta.columnMapping.id"]])
        actual <- unname(mapping[[id]] %||% "")
        expected <- fabric_delta_field_physical_name(field, "id")
        expression <- if (nzchar(actual)) {
          as.character(DBI::dbQuoteIdentifier(con, actual))
        } else {
          "NULL"
        }
        expression <- fabric_delta_id_type_expression(
          con,
          field$type,
          expression,
          mapping
        )
        paste0(
          expression,
          " AS ",
          as.character(DBI::dbQuoteIdentifier(con, expected))
        )
      },
      character(1)
    ),
    collapse = ", "
  )
}

#' Rebuild nested ID-mapped values using canonical physical names
#' @keywords internal
#' @noRd
fabric_delta_id_type_expression <- function(con, type, expression, mapping) {
  if (
    is.character(type) &&
      length(type) == 1L &&
      identical(tolower(type), "void")
  ) {
    return("NULL")
  }
  if (
    identical(expression, "NULL") ||
      (is.character(type) && length(type) == 1L)
  ) {
    return(expression)
  }
  kind <- tolower(as.character(type$type %||% ""))
  if (identical(kind, "struct")) {
    packed <- vapply(
      type$fields %||% list(),
      function(field) {
        id <- as.character(field$metadata[["delta.columnMapping.id"]])
        actual <- unname(mapping[[id]] %||% "")
        nested <- if (nzchar(actual)) {
          paste0(
            expression,
            ".",
            as.character(DBI::dbQuoteIdentifier(con, actual))
          )
        } else {
          "NULL"
        }
        expected <- fabric_delta_field_physical_name(field, "id")
        paste0(
          as.character(DBI::dbQuoteIdentifier(con, expected)),
          " := ",
          fabric_delta_id_type_expression(
            con,
            field$type,
            nested,
            mapping
          )
        )
      },
      character(1)
    )
    return(paste0(
      "CASE WHEN ",
      expression,
      " IS NULL THEN NULL ELSE struct_pack(",
      paste(packed, collapse = ", "),
      ") END"
    ))
  }
  if (identical(kind, "array")) {
    return(paste0(
      "list_transform(",
      expression,
      ", fabric_delta_id_element -> ",
      fabric_delta_id_type_expression(
        con,
        type$elementType,
        "fabric_delta_id_element",
        mapping
      ),
      ")"
    ))
  }
  if (identical(kind, "map")) {
    return(paste0(
      "map(list_transform(map_keys(",
      expression,
      "), fabric_delta_id_key -> ",
      fabric_delta_id_type_expression(
        con,
        type$keyType,
        "fabric_delta_id_key",
        mapping
      ),
      "), list_transform(map_values(",
      expression,
      "), fabric_delta_id_value -> ",
      fabric_delta_id_type_expression(
        con,
        type$valueType,
        "fabric_delta_id_value",
        mapping
      ),
      "))"
    ))
  }
  expression
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
  if (
    is.character(field$type) &&
      length(field$type) == 1L &&
      identical(tolower(field$type), "void")
  ) {
    return("NULL")
  }
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
    if (identical(tolower(type), "void")) {
      return("NULL")
    }
    return(expression)
  }
  kind <- tolower(as.character(type$type %||% ""))
  expression <- paste0(
    "CAST(",
    expression,
    " AS ",
    fabric_delta_duckdb_physical_type(con, type, mapping_mode),
    ")"
  )
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
    return(paste0(
      "CASE WHEN ",
      expression,
      " IS NULL THEN NULL ELSE struct_pack(",
      paste(packed, collapse = ", "),
      ") END"
    ))
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

#' Detect whether a Delta type contains a struct at any nesting level
#' @keywords internal
#' @noRd
fabric_delta_type_has_struct <- function(type) {
  if (is.character(type) && length(type) == 1L) {
    return(FALSE)
  }
  if (!is.list(type)) {
    return(FALSE)
  }
  kind <- tolower(as.character(type$type %||% ""))
  if (identical(kind, "struct")) {
    return(TRUE)
  }
  if (identical(kind, "array")) {
    return(fabric_delta_type_has_struct(type$elementType))
  }
  if (identical(kind, "map")) {
    return(
      fabric_delta_type_has_struct(type$keyType) ||
        fabric_delta_type_has_struct(type$valueType)
    )
  }
  FALSE
}

#' Choose a collision-free validity member for a struct-mask value
#' @keywords internal
#' @noRd
fabric_delta_struct_validity_name <- function(type) {
  names <- vapply(type$fields %||% list(), `[[`, character(1), "name")
  result <- "fabric_delta_struct_valid_internal"
  while (result %in% names) {
    result <- paste0(result, "_")
  }
  result
}

#' Build a recursive SQL mirror of Delta struct validity
#' @keywords internal
#' @noRd
fabric_delta_struct_mask_expression <- function(
  con,
  type,
  expression,
  mapping_mode = "none"
) {
  if (is.character(type) && length(type) == 1L) {
    return("TRUE")
  }
  kind <- tolower(as.character(type$type %||% ""))
  expression <- paste0(
    "CAST(",
    expression,
    " AS ",
    fabric_delta_duckdb_physical_type(con, type, mapping_mode),
    ")"
  )
  if (identical(kind, "struct")) {
    fields <- type$fields %||% list()
    validity <- as.character(DBI::dbQuoteIdentifier(
      con,
      fabric_delta_struct_validity_name(type)
    ))
    nested <- unlist(
      lapply(fields, function(field) {
        if (!fabric_delta_type_has_struct(field$type)) {
          return(NULL)
        }
        physical <- fabric_delta_field_physical_name(field, mapping_mode)
        logical <- as.character(DBI::dbQuoteIdentifier(con, field$name))
        paste0(
          logical,
          " := ",
          fabric_delta_struct_mask_expression(
            con,
            field$type,
            paste0(
              expression,
              ".",
              as.character(DBI::dbQuoteIdentifier(con, physical))
            ),
            mapping_mode
          )
        )
      }),
      use.names = FALSE
    )
    return(paste0(
      "struct_pack(",
      paste(
        c(paste0(validity, " := ", expression, " IS NOT NULL"), nested),
        collapse = ", "
      ),
      ")"
    ))
  }
  if (identical(kind, "array")) {
    return(paste0(
      "list_transform(",
      expression,
      ", fabric_delta_mask_element -> ",
      fabric_delta_struct_mask_expression(
        con,
        type$elementType,
        "fabric_delta_mask_element",
        mapping_mode
      ),
      ")"
    ))
  }
  if (identical(kind, "map")) {
    return(paste0(
      "map(map_keys(",
      expression,
      "), list_transform(map_values(",
      expression,
      "), fabric_delta_mask_value -> ",
      fabric_delta_struct_mask_expression(
        con,
        type$valueType,
        "fabric_delta_mask_value",
        mapping_mode
      ),
      "))"
    ))
  }
  "TRUE"
}

#' Attach recursive struct-validity mirrors to materialized R columns
#' @keywords internal
#' @noRd
fabric_delta_apply_struct_mask <- function(value, type, mask) {
  if (is.character(type) && length(type) == 1L) {
    return(value)
  }
  kind <- tolower(as.character(type$type %||% ""))
  if (identical(kind, "struct")) {
    if (!is.data.frame(value) || !is.data.frame(mask)) {
      rlang::abort("DuckDB returned an invalid Delta struct validity mirror")
    }
    validity_name <- fabric_delta_struct_validity_name(type)
    validity <- mask[[validity_name]]
    if (!is.logical(validity) || length(validity) != nrow(value)) {
      rlang::abort("DuckDB returned an invalid Delta struct validity bitmap")
    }
    for (field in type$fields %||% list()) {
      if (!fabric_delta_type_has_struct(field$type)) {
        next
      }
      value[[field$name]] <- fabric_delta_apply_struct_mask(
        value[[field$name]],
        field$type,
        mask[[field$name]]
      )
    }
    class(value) <- unique(c("fabric_delta_struct_column", class(value)))
    attr(value, "fabric_delta_struct_validity") <- validity
    return(value)
  }
  if (identical(kind, "array")) {
    for (index in seq_along(value)) {
      if (is.null(value[[index]]) || is.null(mask[[index]])) {
        next
      }
      value[index] <- list(fabric_delta_apply_struct_mask(
        value[[index]],
        type$elementType,
        mask[[index]]
      ))
    }
    return(value)
  }
  if (identical(kind, "map")) {
    for (index in seq_along(value)) {
      if (is.null(value[[index]]) || is.null(mask[[index]])) {
        next
      }
      value[[index]]$value <- fabric_delta_apply_struct_mask(
        value[[index]]$value,
        type$valueType,
        mask[[index]]$value
      )
    }
    return(value)
  }
  value
}

#' Mark timezone-free Delta timestamps as wall-clock vectors
#' @keywords internal
#' @noRd
fabric_delta_restore_timestamp_ntz <- function(value, type) {
  if (is.character(type) && length(type) == 1L) {
    if (!identical(tolower(type), "timestamp_ntz")) {
      return(value)
    }
    text <- if (inherits(value, "POSIXct")) {
      format(value, "%Y-%m-%d %H:%M:%OS6", tz = "UTC")
    } else {
      as.character(value)
    }
    text[is.na(value)] <- NA_character_
    present <- !is.na(text)
    text[present] <- sub("T", " ", text[present], fixed = TRUE)
    fractional <- present & grepl("\\.[0-9]+$", text)
    if (any(fractional)) {
      whole <- sub("\\.([0-9]+)$", "", text[fractional])
      digits <- sub("^.*\\.([0-9]+)$", "\\1", text[fractional])
      digits <- substr(paste0(digits, "000000"), 1L, 6L)
      text[fractional] <- paste0(whole, ".", digits)
    }
    text[present & !fractional] <- paste0(
      text[present & !fractional],
      ".000000"
    )
    return(structure(
      text,
      class = c("fabric_delta_timestamp_ntz", "character")
    ))
  }
  kind <- tolower(as.character(type$type %||% ""))
  if (identical(kind, "struct")) {
    for (field in type$fields %||% list()) {
      value[[field$name]] <- fabric_delta_restore_timestamp_ntz(
        value[[field$name]],
        field$type
      )
    }
    return(value)
  }
  if (identical(kind, "array")) {
    for (index in seq_along(value)) {
      if (is.null(value[[index]])) {
        next
      }
      value[index] <- list(fabric_delta_restore_timestamp_ntz(
        value[[index]],
        type$elementType
      ))
    }
    return(value)
  }
  if (identical(kind, "map")) {
    for (index in seq_along(value)) {
      if (is.null(value[[index]])) {
        next
      }
      value[[index]]$key <- fabric_delta_restore_timestamp_ntz(
        value[[index]]$key,
        type$keyType
      )
      value[[index]]$value <- fabric_delta_restore_timestamp_ntz(
        value[[index]]$value,
        type$valueType
      )
    }
    return(value)
  }
  value
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
    result <- as.list(value)
    attr(result, "fabric_delta_partition_tokens") <-
      attr(value, "fabric_delta_partition_tokens", exact = TRUE)
    return(result)
  }
  rlang::abort("Delta log contains an invalid partitionValues map")
}

#' Extract lossless JSON string tokens from a partitionValues object
#' @keywords internal
#' @noRd
fabric_delta_json_partition_tokens <- function(line) {
  bytes <- as.integer(charToRaw(enc2utf8(line)))
  length_bytes <- length(bytes)
  whitespace <- c(9L, 10L, 13L, 32L)
  skip_whitespace <- function(index) {
    while (index <= length_bytes && bytes[[index]] %in% whitespace) {
      index <- index + 1L
    }
    index
  }
  scan_string <- function(index) {
    if (index > length_bytes || bytes[[index]] != 34L) {
      return(NA_integer_)
    }
    cursor <- index + 1L
    while (cursor <= length_bytes) {
      if (bytes[[cursor]] == 92L) {
        cursor <- cursor + 2L
      } else if (bytes[[cursor]] == 34L) {
        return(cursor)
      } else {
        cursor <- cursor + 1L
      }
    }
    NA_integer_
  }
  token_text <- function(start, end) {
    rawToChar(as.raw(bytes[start:end]))
  }
  decode_string <- function(start, end) {
    tryCatch(
      jsonlite::fromJSON(token_text(start, end), simplifyVector = FALSE),
      error = function(error) NULL
    )
  }

  cursor <- 1L
  object_start <- NA_integer_
  while (cursor <= length_bytes) {
    if (bytes[[cursor]] != 34L) {
      cursor <- cursor + 1L
      next
    }
    string_end <- scan_string(cursor)
    if (is.na(string_end)) {
      return(NULL)
    }
    property <- decode_string(cursor, string_end)
    after <- skip_whitespace(string_end + 1L)
    if (
      identical(property, "partitionValues") &&
        after <= length_bytes &&
        bytes[[after]] == 58L
    ) {
      after <- skip_whitespace(after + 1L)
      if (after <= length_bytes && bytes[[after]] == 123L) {
        object_start <- after
        break
      }
    }
    cursor <- string_end + 1L
  }
  if (is.na(object_start)) {
    return(NULL)
  }

  tokens <- list()
  cursor <- skip_whitespace(object_start + 1L)
  if (cursor <= length_bytes && bytes[[cursor]] == 125L) {
    return(tokens)
  }
  repeat {
    key_end <- scan_string(cursor)
    if (is.na(key_end)) {
      return(NULL)
    }
    key <- decode_string(cursor, key_end)
    if (
      !is.character(key) ||
        length(key) != 1L ||
        is.na(key)
    ) {
      return(NULL)
    }
    cursor <- skip_whitespace(key_end + 1L)
    if (cursor > length_bytes || bytes[[cursor]] != 58L) {
      return(NULL)
    }
    cursor <- skip_whitespace(cursor + 1L)
    if (cursor <= length_bytes && bytes[[cursor]] == 34L) {
      value_end <- scan_string(cursor)
      if (is.na(value_end)) {
        return(NULL)
      }
      tokens[[key]] <- token_text(cursor, value_end)
      cursor <- value_end + 1L
    } else if (
      cursor + 3L <= length_bytes &&
        identical(bytes[cursor:(cursor + 3L)], c(110L, 117L, 108L, 108L))
    ) {
      tokens[key] <- list(NULL)
      cursor <- cursor + 4L
    } else {
      return(NULL)
    }
    cursor <- skip_whitespace(cursor)
    if (cursor > length_bytes) {
      return(NULL)
    }
    if (bytes[[cursor]] == 125L) {
      break
    }
    if (bytes[[cursor]] != 44L) {
      return(NULL)
    }
    cursor <- skip_whitespace(cursor + 1L)
  }
  tokens
}

#' Attach lossless partition string tokens to one parsed Delta action
#' @keywords internal
#' @noRd
fabric_delta_preserve_partition_tokens <- function(action, line) {
  if (is.null(action$add$partitionValues)) {
    return(action)
  }
  tokens <- fabric_delta_json_partition_tokens(line)
  if (!is.null(tokens)) {
    attr(action$add$partitionValues, "fabric_delta_partition_tokens") <- tokens
  }
  action
}

#' Decode one JSON string token into Unicode scalar values
#' @keywords internal
#' @noRd
fabric_delta_json_string_codepoints <- function(token) {
  if (
    !is.character(token) ||
      length(token) != 1L ||
      is.na(token) ||
      nchar(token, type = "bytes") < 2L
  ) {
    rlang::abort("Delta log contains an invalid binary partition value")
  }
  bytes <- as.integer(charToRaw(enc2utf8(token)))
  if (bytes[[1L]] != 34L || bytes[[length(bytes)]] != 34L) {
    rlang::abort("Delta log contains an invalid binary partition value")
  }
  values <- numeric()
  cursor <- 2L
  last <- length(bytes) - 1L
  flush_utf8 <- function(raw_values) {
    if (!length(raw_values)) {
      return(numeric())
    }
    utf8ToInt(rawToChar(as.raw(raw_values)))
  }
  while (cursor <= last) {
    if (bytes[[cursor]] != 92L) {
      start <- cursor
      while (cursor <= last && bytes[[cursor]] != 92L) {
        cursor <- cursor + 1L
      }
      values <- c(values, flush_utf8(bytes[start:(cursor - 1L)]))
      next
    }
    cursor <- cursor + 1L
    if (cursor > last) {
      rlang::abort("Delta log contains an invalid binary partition value")
    }
    escaped <- bytes[[cursor]]
    simple <- c(
      `34` = 34L,
      `47` = 47L,
      `92` = 92L,
      `98` = 8L,
      `102` = 12L,
      `110` = 10L,
      `114` = 13L,
      `116` = 9L
    )
    if (as.character(escaped) %in% names(simple)) {
      values <- c(values, unname(simple[[as.character(escaped)]]))
      cursor <- cursor + 1L
      next
    }
    if (escaped != 117L || cursor + 4L > last) {
      rlang::abort("Delta log contains an invalid binary partition value")
    }
    hex <- rawToChar(as.raw(bytes[(cursor + 1L):(cursor + 4L)]))
    if (!grepl("^[0-9A-Fa-f]{4}$", hex)) {
      rlang::abort("Delta log contains an invalid binary partition value")
    }
    values <- c(values, strtoi(hex, base = 16L))
    cursor <- cursor + 5L
  }
  values
}

#' Decode UTF-8 bytes without passing embedded NUL through an R string
#' @keywords internal
#' @noRd
fabric_delta_utf8_codepoints <- function(bytes) {
  bytes <- as.integer(bytes)
  values <- numeric()
  cursor <- 1L
  while (cursor <= length(bytes)) {
    first <- bytes[[cursor]]
    width <- if (first < 128L) {
      1L
    } else if (first >= 194L && first <= 223L) {
      2L
    } else if (first >= 224L && first <= 239L) {
      3L
    } else if (first >= 240L && first <= 244L) {
      4L
    } else {
      rlang::abort("Delta log contains invalid UTF-8 in a partition value")
    }
    if (cursor + width - 1L > length(bytes)) {
      rlang::abort("Delta log contains invalid UTF-8 in a partition value")
    }
    continuation <- if (width == 1L) {
      integer()
    } else {
      bytes[(cursor + 1L):(cursor + width - 1L)]
    }
    if (
      length(continuation) && any(continuation < 128L | continuation > 191L)
    ) {
      rlang::abort("Delta log contains invalid UTF-8 in a partition value")
    }
    value <- switch(
      as.character(width),
      `1` = first,
      `2` = (first - 192L) * 64 + continuation[[1L]] - 128L,
      `3` = (first - 224L) *
        4096 +
        (continuation[[1L]] - 128L) * 64 +
        continuation[[2L]] -
        128L,
      `4` = (first - 240L) *
        262144 +
        (continuation[[1L]] - 128L) * 4096 +
        (continuation[[2L]] - 128L) * 64 +
        continuation[[3L]] -
        128L
    )
    if (
      (width == 2L && value < 128L) ||
        (width == 3L && value < 2048L) ||
        (width == 4L && value < 65536L) ||
        value > 1114111L ||
        value %in% 55296:57343
    ) {
      rlang::abort("Delta log contains invalid UTF-8 in a partition value")
    }
    values <- c(values, value)
    cursor <- cursor + width
  }
  values
}

#' Decode an even-length hexadecimal string to raw bytes
#' @keywords internal
#' @noRd
fabric_delta_hex_raw <- function(value) {
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    rlang::abort("Delta checkpoint contains invalid partition value bytes")
  }
  if (!nzchar(value)) {
    return(raw())
  }
  if (
    nchar(value, type = "bytes") %% 2L != 0L ||
      !grepl("^[0-9A-Fa-f]+$", value)
  ) {
    rlang::abort("Delta checkpoint contains invalid partition value bytes")
  }
  starts <- seq.int(1L, nchar(value, type = "bytes"), by = 2L)
  as.raw(strtoi(substring(value, starts, starts + 1L), base = 16L))
}

#' Decode one protocol-serialized binary partition value
#' @keywords internal
#' @noRd
fabric_delta_binary_partition <- function(value, token = NULL) {
  if (is.null(value) || !length(value) || is.na(value[[1L]])) {
    return(NULL)
  }
  codepoints <- if (inherits(token, "fabric_delta_partition_utf8")) {
    fabric_delta_utf8_codepoints(token$bytes)
  } else if (!is.null(token)) {
    fabric_delta_json_string_codepoints(token)
  } else {
    utf8ToInt(enc2utf8(as.character(value[[1L]])))
  }
  if (!length(codepoints)) {
    return(NULL)
  }
  if (any(codepoints < 0L | codepoints > 255L)) {
    rlang::abort(
      "Delta log binary partition values must encode bytes from 0 through 255"
    )
  }
  as.raw(codepoints)
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
    field_names <- vapply(schema$fields, `[[`, character(1), "name")
    field <- schema$fields[[match(partition, field_names)]]
    is_binary <- is.character(field$type) &&
      length(field$type) == 1L &&
      identical(tolower(field$type), "binary")
    mapping_mode <- schema$columnMappingMode %||% "none"
    physical_partition <- if (identical(mapping_mode, "none")) {
      partition
    } else {
      fabric_delta_field_physical_name(
        field,
        mapping_mode
      )
    }
    records <- lapply(snapshot$active, function(path) {
      record <- files[[path]] %||% list(partitionValues = list())
      map <- record$partitionValues %||% list()
      if (!physical_partition %in% names(map)) {
        rlang::abort(cli::format_inline(
          "Delta file {.path {path}} has no value for partition column {.field {partition}}"
        ))
      }
      tokens <- attr(map, "fabric_delta_partition_tokens", exact = TRUE)
      list(
        path = path,
        value = map[[physical_partition]],
        token = tokens[[physical_partition]] %||% NULL
      )
    })
    column <- paste0("fabric_delta_partition_", index)
    if (is_binary) {
      mapping[[column]] <- I(lapply(records, function(record) {
        fabric_delta_binary_partition(record$value, record$token)
      }))
    } else {
      mapping[[column]] <- vapply(
        records,
        function(record) {
          value <- record$value
          token <- record$token
          if (
            inherits(token, "fabric_delta_partition_utf8") &&
              any(as.integer(token$bytes) == 0L)
          ) {
            rlang::abort(cli::format_error(c(
              "Delta file {.path {record$path}} has an embedded NUL in non-binary partition column {.field {partition}}",
              "i" = "R cannot represent embedded NUL in a character partition value."
            )))
          }
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

#' Derive the protocol-defined identity of a deletion vector
#' @keywords internal
#' @noRd
fabric_delta_deletion_vector_id <- function(descriptor) {
  if (is.null(descriptor) || !length(descriptor)) {
    return(NA_character_)
  }
  storage_type <- as.character(descriptor$storageType %||% "")
  encoded <- as.character(descriptor$pathOrInlineDv %||% "")
  if (
    length(storage_type) != 1L ||
      is.na(storage_type) ||
      !nzchar(storage_type) ||
      length(encoded) != 1L ||
      is.na(encoded) ||
      !nzchar(encoded)
  ) {
    rlang::abort("Delta deletion vector contains an invalid identity")
  }
  offset <- descriptor$offset %||% NULL
  if (is.null(offset)) {
    return(paste0(storage_type, encoded))
  }
  offset <- as.numeric(offset)
  if (
    length(offset) != 1L ||
      !is.finite(offset) ||
      offset < 0 ||
      offset != floor(offset)
  ) {
    rlang::abort("Delta deletion vector contains an invalid identity")
  }
  paste0(storage_type, encoded, "@", format(offset, scientific = FALSE))
}

#' Resolve the table-relative path of a persisted deletion vector
#' @keywords internal
#' @noRd
fabric_delta_deletion_vector_path <- function(descriptor) {
  storage_type <- as.character(descriptor$storageType %||% "")
  encoded <- as.character(descriptor$pathOrInlineDv %||% "")
  if (identical(storage_type, "p")) {
    decoded <- utils::URLdecode(encoded)
    onelake_parse_uri(decoded)
    return(decoded)
  }
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
    !storage_type %in% c("u", "i", "p") ||
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
    stored_path <- fabric_delta_deletion_vector_path(descriptor)
    path <- fabric_delta_local_file(table_dir, stored_path)
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

#' Detect whether a Delta schema contains an instant timestamp
#' @keywords internal
#' @noRd
fabric_delta_schema_has_timestamp <- function(schema) {
  visit <- function(type) {
    if (is.character(type) && length(type) == 1L) {
      return(identical(tolower(type), "timestamp"))
    }
    if (!is.list(type)) {
      return(FALSE)
    }
    kind <- tolower(as.character(type$type %||% ""))
    if (identical(kind, "struct")) {
      return(any(vapply(
        type$fields %||% list(),
        function(field) visit(field$type),
        logical(1)
      )))
    }
    if (identical(kind, "array")) {
      return(visit(type$elementType))
    }
    if (identical(kind, "map")) {
      return(visit(type$keyType) || visit(type$valueType))
    }
    FALSE
  }
  any(vapply(
    schema$fields %||% list(),
    function(field) {
      visit(field$type)
    },
    logical(1)
  ))
}

#' Load DuckDB's timezone support with a deterministic install fallback
#' @keywords internal
#' @noRd
fabric_delta_load_icu <- function(con) {
  loaded <- tryCatch(
    {
      DBI::dbExecute(con, "LOAD icu")
      TRUE
    },
    error = function(error) FALSE
  )
  if (!loaded) {
    tryCatch(
      {
        DBI::dbExecute(con, "INSTALL icu")
        DBI::dbExecute(con, "LOAD icu")
      },
      error = function(error) {
        rlang::abort(
          "DuckDB's ICU extension is required to read Delta timestamp values",
          parent = error
        )
      }
    )
  }
  invisible(con)
}

#' Normalize legacy timestamp partition values to explicit UTC instants
#' @keywords internal
#' @noRd
fabric_delta_normalize_timestamp_partitions <- function(
  mapping,
  schema,
  timezone
) {
  timestamp_indexes <- which(vapply(
    schema$partitionColumns,
    function(name) {
      field <- schema$fields[[match(
        name,
        vapply(
          schema$fields,
          `[[`,
          character(1),
          "name"
        )
      )]]
      is.character(field$type) &&
        length(field$type) == 1L &&
        identical(tolower(field$type), "timestamp")
    },
    logical(1)
  ))
  for (index in timestamp_indexes) {
    column <- paste0("fabric_delta_partition_", index)
    values <- mapping[[column]]
    naive <- !is.na(values) &
      !grepl(
        "(Z|[+-][0-9]{2}:?[0-9]{2})$",
        values,
        ignore.case = TRUE
      )
    if (!any(naive)) {
      next
    }
    if (is.null(timezone)) {
      rlang::abort(paste0(
        "Delta timestamp partition values without UTC offsets require ",
        "timestamp_partition_timezone to identify the writer timezone"
      ))
    }
    if (
      !is.character(timezone) ||
        length(timezone) != 1L ||
        is.na(timezone) ||
        !nzchar(timezone) ||
        !timezone %in% OlsonNames()
    ) {
      rlang::abort(paste0(
        "timestamp_partition_timezone is not a recognized IANA timezone: ",
        paste(timezone, collapse = ", ")
      ))
    }
    local <- sub("T", " ", values[naive], fixed = TRUE)
    parsed <- as.POSIXct(
      local,
      format = "%Y-%m-%d %H:%M:%OS",
      tz = timezone
    )
    if (anyNA(parsed)) {
      rlang::abort(
        "Delta log contains an invalid offset-less timestamp partition value"
      )
    }
    mapping[[column]][naive] <- format(
      parsed,
      "%Y-%m-%dT%H:%M:%OS6Z",
      tz = "UTC"
    )
  }
  mapping
}

#' Read outer Parquet validity for top-level Delta Variant columns
#' @keywords internal
#' @noRd
fabric_delta_variant_null_masks <- function(paths, fields, schema) {
  paths <- gsub(
    "\\\\",
    "/",
    normalizePath(paths, mustWork = TRUE)
  )
  masks <- stats::setNames(
    vector("list", length(fields)),
    vapply(
      fields,
      `[[`,
      character(1),
      "name"
    )
  )
  for (path in paths) {
    data <- arrow::read_parquet(path, as_data_frame = TRUE)
    for (field in fields) {
      physical_name <- fabric_delta_field_physical_name(
        field,
        schema$columnMappingMode
      )
      sql_null <- if (!physical_name %in% names(data)) {
        rep(TRUE, nrow(data))
      } else {
        value <- data[[physical_name]]
        if (!is.data.frame(value) || !"metadata" %in% names(value)) {
          rlang::abort(paste0(
            "Arrow did not expose the physical Parquet Variant structure for ",
            field$name
          ))
        }
        vapply(
          value$metadata,
          function(metadata) is.null(metadata) || !length(metadata),
          logical(1)
        )
      }
      masks[[field$name]][[path]] <- sql_null
    }
  }
  masks
}

#' Restore exact Delta Variant cells after DuckDB row selection
#' @keywords internal
#' @noRd
fabric_delta_restore_variants <- function(
  result,
  fields,
  masks,
  source_column,
  row_column
) {
  sources <- result[[source_column]]
  rows <- as.numeric(result[[row_column]]) + 1
  for (field in fields) {
    column <- result[[field$name]]
    if (
      !is.data.frame(column) ||
        !all(c("type", "display", "physical") %in% names(column)) ||
        !is.data.frame(column$physical) ||
        !all(c("metadata", "value") %in% names(column$physical))
    ) {
      rlang::abort(paste0(
        "DuckDB did not return an exact Parquet Variant representation for ",
        field$name
      ))
    }
    values <- vector("list", nrow(result))
    for (index in seq_len(nrow(result))) {
      source_mask <- masks[[field$name]][[sources[[index]]]]
      if (
        is.null(source_mask) ||
          rows[[index]] < 1 ||
          rows[[index]] > length(source_mask)
      ) {
        rlang::abort(
          "Could not reconcile a Delta Variant value with its file row"
        )
      }
      if (isTRUE(source_mask[[rows[[index]]]])) {
        values[index] <- list(NULL)
      } else {
        values[[index]] <- fabric_delta_variant(
          type = column$type[[index]],
          display = column$display[[index]],
          metadata = column$physical$metadata[[index]],
          value = column$physical$value[[index]]
        )
      }
    }
    class(values) <- c("fabric_delta_variant_column", "list")
    result[[field$name]] <- values
  }
  result[[source_column]] <- NULL
  result[[row_column]] <- NULL
  result
}

#' Mark empty or schema-evolved top-level Variant list columns
#' @keywords internal
#' @noRd
fabric_delta_mark_variant_columns <- function(result, fields) {
  for (field in fields) {
    if (
      is.character(field$type) &&
        length(field$type) == 1L &&
        identical(tolower(field$type), "variant")
    ) {
      result[[field$name]] <- structure(
        as.list(result[[field$name]]),
        class = c("fabric_delta_variant_column", "list")
      )
    }
  }
  result
}

#' Construct one exact Delta Variant value
#' @keywords internal
#' @noRd
fabric_delta_variant <- function(type, display, metadata, value) {
  structure(
    list(
      type = as.character(type),
      display = as.character(display),
      metadata = metadata,
      value = value
    ),
    class = "fabric_delta_variant"
  )
}

#' Format an exact Delta Variant value
#' @param x A `fabric_delta_variant` object.
#' @param ... Unused.
#' @return A readable character representation.
#' @export
format.fabric_delta_variant <- function(x, ...) {
  if (length(x$display) != 1L || is.na(x$display)) {
    "null"
  } else {
    x$display
  }
}

#' @export
print.fabric_delta_variant <- function(x, ...) {
  cat("<fabric_delta_variant:", x$type, ">", format(x), "\n")
  invisible(x)
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
      field <- schema$fields[[index]]
      is_void <- is.character(field$type) &&
        length(field$type) == 1L &&
        identical(tolower(field$type), "void")
      if (is_void) {
        expression <- "NULL"
      } else if (name %in% schema$partitionColumns) {
        partition_index <- match(name, schema$partitionColumns)
        expression <- paste0(
          "delta_partitions.",
          as.character(DBI::dbQuoteIdentifier(
            con,
            paste0("fabric_delta_partition_", partition_index)
          ))
        )
        if (
          is.character(field$type) &&
            length(field$type) == 1L &&
            identical(tolower(field$type), "timestamp")
        ) {
          expression <- paste0(
            "CASE WHEN ",
            expression,
            " IS NULL THEN NULL WHEN regexp_matches(",
            expression,
            ", '([Zz]|[+-][0-9]{2}:?[0-9]{2})$') THEN CAST(",
            expression,
            " AS TIMESTAMPTZ) ELSE CAST(",
            expression,
            " AS TIMESTAMPTZ) END"
          )
        }
      } else {
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
      if (
        is.character(field$type) &&
          length(field$type) == 1L &&
          identical(tolower(field$type), "variant")
      ) {
        return(paste0(
          "struct_pack(",
          "type := variant_typeof(",
          expression,
          "), display := CAST(",
          expression,
          " AS VARCHAR), physical := variant_to_parquet_variant(",
          expression,
          ")) AS ",
          alias
        ))
      }
      paste0("CAST(", expression, " AS ", type, ") AS ", alias)
    },
    character(1)
  )
}

#' Apply one complete Delta checkpoint candidate
#' @keywords internal
#' @noRd
fabric_delta_apply_checkpoint_candidate <- function(
  state,
  candidate,
  checkpoint_version,
  table_dir
) {
  checkpoint_paths <- candidate$paths
  checkpoint <- fabric_delta_read_checkpoint(checkpoint_paths)
  state <- fabric_delta_apply_checkpoint(state, checkpoint)
  checkpoint_actions <- checkpoint$actions %||% list()
  is_v2 <- isTRUE(attr(checkpoint, "fabric_delta_v2")) ||
    any(vapply(
      checkpoint_actions,
      function(action) !is.null(action$checkpointMetadata),
      logical(1)
    )) ||
    isTRUE(candidate$v2_named)
  if (is_v2) {
    checkpoint_features <- unlist(
      state$protocol$readerFeatures %||% list(),
      use.names = FALSE
    )
    if (!"v2Checkpoint" %in% checkpoint_features) {
      rlang::abort(
        "Delta V2 checkpoint requires the v2Checkpoint reader feature"
      )
    }
    metadata_versions <- if (length(checkpoint_actions)) {
      unlist(
        lapply(checkpoint_actions, function(action) {
          action$checkpointMetadata$version %||% NULL
        }),
        use.names = FALSE
      )
    } else {
      attr(checkpoint, "fabric_delta_checkpoint_versions") %||% numeric()
    }
    if (
      length(metadata_versions) != 1L ||
        is.na(metadata_versions) ||
        as.numeric(metadata_versions) != checkpoint_version
    ) {
      rlang::abort(
        "Delta V2 checkpoint must contain exactly one matching checkpointMetadata action"
      )
    }
  }
  sidecar_names <- if (is_v2) {
    fabric_delta_checkpoint_sidecar_paths(checkpoint_paths)
  } else {
    character()
  }
  embedded_file_actions <- if (length(checkpoint_actions)) {
    any(vapply(
      checkpoint_actions,
      function(action) {
        !is.null(action$add$path) || !is.null(action$remove$path)
      },
      logical(1)
    ))
  } else {
    isTRUE(attr(checkpoint, "fabric_delta_has_file_actions"))
  }
  if (length(sidecar_names) && embedded_file_actions) {
    rlang::abort(
      "Delta V2 checkpoint mixes embedded and sidecar file actions"
    )
  }
  if (length(sidecar_names)) {
    sidecar_paths <- fs::path(
      table_dir,
      "_delta_log",
      "_sidecars",
      sidecar_names
    )
    missing_sidecars <- !fs::file_exists(sidecar_paths)
    if (any(missing_sidecars)) {
      rlang::abort(c(
        "Delta V2 checkpoint references a sidecar that was not staged",
        "x" = cli::format_inline(
          "{.path {sidecar_paths[which(missing_sidecars)[1L]]}} is missing"
        )
      ))
    }
    for (sidecar_path in sidecar_paths) {
      state <- fabric_delta_apply_checkpoint(
        state,
        fabric_delta_read_checkpoint(sidecar_path)
      )
    }
  }
  state
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

  eligible_indexes <- which(checkpoint_versions <= target)
  if (length(eligible_indexes)) {
    eligible_indexes <- eligible_indexes[
      order(checkpoint_versions[eligible_indexes], decreasing = TRUE)
    ]
  }
  attempts <- unlist(
    lapply(eligible_indexes, function(index) {
      checkpoint_set <- checkpoint_sets[[index]]
      lapply(
        checkpoint_set$alternatives %||% list(checkpoint_set),
        function(candidate) {
          list(
            candidate = candidate,
            version = checkpoint_set$version
          )
        }
      )
    }),
    recursive = FALSE
  )
  attempts[[length(attempts) + 1L]] <- list(
    candidate = NULL,
    version = NULL
  )
  errors <- list()
  no_checkpoint_error <- NULL

  for (attempt in attempts) {
    checkpoint_version <- attempt$version
    first_json <- if (is.null(checkpoint_version)) {
      0
    } else {
      checkpoint_version + 1
    }
    needed <- if (first_json <= target) {
      seq(first_json, target)
    } else {
      numeric()
    }
    present <- sort(json_versions[
      json_versions >= first_json & json_versions <= target
    ])
    if (!identical(as.numeric(present), as.numeric(needed))) {
      incomplete <- rlang::error_cnd(
        message = cli::format_inline(
          "Delta log is incomplete for version {target}; a required commit is missing"
        )
      )
      if (is.null(checkpoint_version)) {
        no_checkpoint_error <- incomplete
      } else {
        errors[[length(errors) + 1L]] <- incomplete
      }
      next
    }

    result <- tryCatch(
      {
        state <- list(
          active = character(),
          files = list(),
          protocol = NULL,
          metadata = NULL,
          has_deletion_vectors = FALSE
        )
        if (!is.null(attempt$candidate)) {
          state <- fabric_delta_apply_checkpoint_candidate(
            state,
            attempt$candidate,
            checkpoint_version,
            table_dir
          )
        }
        if (length(present)) {
          ordered_paths <- json_paths[match(present, json_versions)]
          for (path in ordered_paths) {
            state <- fabric_delta_apply_json_log(state, path)
          }
        }
        fabric_delta_validate_reader(state)
        c(
          state,
          list(
            version = target,
            checkpoint_version = checkpoint_version
          )
        )
      },
      error = function(error) error
    )
    if (!inherits(result, "error")) {
      return(result)
    }
    if (is.null(checkpoint_version)) {
      no_checkpoint_error <- result
    } else {
      errors[[length(errors) + 1L]] <- result
    }
  }

  if (length(eligible_indexes)) {
    parent <- errors[[length(errors)]] %||% no_checkpoint_error
    rlang::abort(
      paste0(
        "No usable Delta checkpoint was found at or before version ",
        max(checkpoint_versions[eligible_indexes])
      ),
      class = "fabric_delta_checkpoint_error",
      parent = parent
    )
  }
  if (!is.null(no_checkpoint_error)) {
    rlang::cnd_signal(no_checkpoint_error)
  }
  rlang::abort("Could not resolve the Delta snapshot")
}

#' Find complete classic, multipart, or UUID-named Delta checkpoints
#' @param paths Paths in a Delta log directory.
#' @return A list of complete checkpoint records with `version` and `paths`.
#' @keywords internal
#' @noRd
fabric_delta_checkpoint_sets <- function(paths) {
  filenames <- basename(paths)
  classic_matches <- regexec(
    "^([0-9]{20})\\.checkpoint(?:\\.([0-9]{10})\\.([0-9]{10}))?\\.parquet$",
    filenames
  )
  classic_parts <- regmatches(filenames, classic_matches)
  classic_keep <- lengths(classic_parts) > 0L
  uuid_matches <- regexec(
    paste0(
      "^([0-9]{20})\\.checkpoint\\.",
      "([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-",
      "[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\\.(json|parquet)$"
    ),
    filenames
  )
  uuid_parts <- regmatches(filenames, uuid_matches)
  uuid_keep <- lengths(uuid_parts) > 0L
  if (!any(classic_keep) && !any(uuid_keep)) {
    return(list())
  }

  classic_records <- Map(
    function(match, path) {
      multipart <- length(match) >= 4L &&
        nzchar(match[[3L]]) &&
        nzchar(match[[4L]])
      list(
        version_text = match[[2L]],
        version = fabric_delta_versions_from_text(match[[2L]]),
        part = if (multipart) as.integer(match[[3L]]) else NA_integer_,
        total = if (multipart) as.integer(match[[4L]]) else NA_integer_,
        path = path,
        kind = if (multipart) "multipart" else "classic",
        format = "parquet"
      )
    },
    classic_parts[classic_keep],
    as.list(paths[classic_keep])
  )
  uuid_records <- Map(
    function(match, path) {
      list(
        version_text = match[[2L]],
        version = fabric_delta_versions_from_text(match[[2L]]),
        part = NA_integer_,
        total = NA_integer_,
        path = path,
        kind = "uuid",
        format = tolower(match[[4L]])
      )
    },
    uuid_parts[uuid_keep],
    as.list(paths[uuid_keep])
  )
  records <- c(classic_records, uuid_records)

  by_version <- split(
    records,
    vapply(records, `[[`, character(1), "version_text")
  )
  complete <- lapply(by_version, function(version_records) {
    candidates <- list()
    uuid <- Filter(
      function(record) identical(record$kind, "uuid"),
      version_records
    )
    if (length(uuid)) {
      uuid <- uuid[order(vapply(uuid, `[[`, character(1), "path"))]
      candidates <- c(
        candidates,
        lapply(uuid, function(record) {
          list(
            version = record$version,
            paths = record$path,
            format = record$format,
            v2_named = TRUE
          )
        })
      )
    }
    classic <- Filter(
      function(record) identical(record$kind, "classic"),
      version_records
    )
    if (length(classic)) {
      candidates <- c(
        candidates,
        list(list(
          version = classic[[1L]]$version,
          paths = classic[[1L]]$path,
          format = "parquet",
          v2_named = FALSE
        ))
      )
    }

    totals <- sort(unique(vapply(
      version_records,
      `[[`,
      integer(1),
      "total"
    )))
    for (total in totals) {
      part_candidates <- Filter(
        function(record) identical(record$total, total),
        version_records
      )
      part_numbers <- vapply(part_candidates, `[[`, integer(1), "part")
      if (
        total > 1L &&
          !anyDuplicated(part_numbers) &&
          identical(sort(part_numbers), seq_len(total))
      ) {
        ordered <- part_candidates[order(part_numbers)]
        candidates <- c(
          candidates,
          list(list(
            version = ordered[[1L]]$version,
            paths = vapply(ordered, `[[`, character(1), "path"),
            format = "parquet",
            v2_named = FALSE
          ))
        )
      }
    }
    if (!length(candidates)) {
      return(NULL)
    }
    c(candidates[[1L]], list(alternatives = candidates))
  })
  complete <- Filter(Negate(is.null), complete)
  complete[order(vapply(complete, `[[`, numeric(1), "version"))]
}

#' Parse actions from a JSON checkpoint
#' @keywords internal
#' @noRd
fabric_delta_read_checkpoint_json <- function(path) {
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  actions <- lapply(lines[nzchar(lines)], function(line) {
    action <- tryCatch(
      jsonlite::fromJSON(line, simplifyVector = FALSE),
      error = function(error) {
        rlang::abort(
          cli::format_inline(
            "Could not parse Delta checkpoint {.path {basename(path)}}"
          ),
          parent = error
        )
      }
    )
    fabric_delta_preserve_partition_tokens(action, line)
  })
  result <- list(actions = actions)
  attr(result, "fabric_delta_v2") <- any(vapply(
    actions,
    function(action) !is.null(action$checkpointMetadata),
    logical(1)
  ))
  result
}

#' Read Delta checkpoint rows with DuckDB's built-in Parquet reader
#' @keywords internal
#' @noRd
fabric_delta_read_checkpoint <- function(paths) {
  if (
    length(paths) == 1L &&
      identical(tolower(tools::file_ext(paths)), "json")
  ) {
    return(fabric_delta_read_checkpoint_json(paths))
  }
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  literals <- as.character(DBI::dbQuoteString(
    con,
    gsub("\\\\", "/", normalizePath(paths, mustWork = TRUE))
  ))
  source <- paste0(
    "read_parquet([",
    paste(literals, collapse = ", "),
    "], union_by_name = true)"
  )
  available <- DBI::dbGetQuery(
    con,
    paste0("DESCRIBE SELECT * FROM ", source)
  )$column_name
  columns <- intersect(
    c("add", "remove", "protocol", "metaData", "checkpointMetadata"),
    available
  )
  is_sidecar <- all(basename(dirname(paths)) == "_sidecars")
  if (
    is_sidecar &&
      any(c("protocol", "metaData", "checkpointMetadata") %in% available)
  ) {
    rlang::abort("Delta V2 checkpoint sidecar contains non-file actions")
  }
  if (!length(columns)) {
    rlang::abort("Delta checkpoint contains no snapshot actions")
  }
  partition_hex_column <- "fabric_delta_partition_values_hex_internal"
  select_expressions <- as.character(DBI::dbQuoteIdentifier(con, columns))
  if ("add" %in% columns) {
    add_index <- match("add", columns)
    select_expressions[[add_index]] <- paste0(
      "struct_update(add, partitionValues := NULL) AS ",
      as.character(DBI::dbQuoteIdentifier(con, "add"))
    )
    select_expressions <- c(
      select_expressions,
      paste0(
        "map(map_keys(add.partitionValues), ",
        "list_transform(map_values(add.partitionValues), ",
        "fabric_delta_partition_value -> ",
        "hex(encode(CAST(fabric_delta_partition_value AS VARCHAR))))) AS ",
        as.character(DBI::dbQuoteIdentifier(con, partition_hex_column))
      )
    )
  }
  result <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT ",
      paste(select_expressions, collapse = ", "),
      " FROM ",
      source
    )
  )
  if ("add" %in% columns) {
    encoded <- result[[partition_hex_column]]
    partition_values <- lapply(seq_len(nrow(result)), function(index) {
      value <- fabric_delta_checkpoint_value(encoded, index, nrow(result))
      if (is.null(value) || !NROW(value)) {
        return(list())
      }
      value <- fabric_delta_partition_values(value)
      tokens <- lapply(value, function(hex) {
        if (is.null(hex) || !length(hex) || is.na(hex[[1L]])) {
          return(NULL)
        }
        structure(
          list(bytes = fabric_delta_hex_raw(as.character(hex[[1L]]))),
          class = "fabric_delta_partition_utf8"
        )
      })
      decoded <- lapply(tokens, function(token) {
        if (is.null(token)) {
          return(NULL)
        }
        if (!length(token$bytes) || any(as.integer(token$bytes) == 0L)) {
          ""
        } else {
          rawToChar(token$bytes)
        }
      })
      attr(decoded, "fabric_delta_partition_tokens") <- tokens
      decoded
    })
    result$add$partitionValues <- I(partition_values)
    result[[partition_hex_column]] <- NULL
  }
  attr(result, "fabric_delta_v2") <- "checkpointMetadata" %in% available
  attr(result, "fabric_delta_has_file_actions") <- any(vapply(
    c("add", "remove"),
    function(action) {
      values <- result[[action]]$path %||% character()
      !all(is.na(values))
    },
    logical(1)
  ))
  if ("checkpointMetadata" %in% available) {
    versions <- DBI::dbGetQuery(
      con,
      paste0(
        "SELECT checkpointMetadata.version AS version FROM ",
        source,
        " WHERE checkpointMetadata IS NOT NULL"
      )
    )$version
    attr(result, "fabric_delta_checkpoint_versions") <- versions
  }
  result
}

#' Discover and validate V2 checkpoint sidecar paths
#' @keywords internal
#' @noRd
fabric_delta_checkpoint_sidecar_paths <- function(
  paths,
  target = NULL,
  table_dir = NULL
) {
  paths <- paths[grepl("\\.checkpoint\\.", basename(paths))]
  if (!length(paths)) {
    return(character())
  }
  sidecars <- unlist(
    lapply(paths, function(path) {
      if (identical(tolower(tools::file_ext(path)), "json")) {
        actions <- fabric_delta_read_checkpoint_json(path)$actions
        return(vapply(
          Filter(function(action) !is.null(action$sidecar$path), actions),
          function(action) as.character(action$sidecar$path),
          character(1)
        ))
      }
      con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
      on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
      literal <- as.character(DBI::dbQuoteString(
        con,
        gsub("\\\\", "/", normalizePath(path, mustWork = TRUE))
      ))
      available <- DBI::dbGetQuery(
        con,
        paste0("DESCRIBE SELECT * FROM read_parquet(", literal, ")")
      )$column_name
      if (!"sidecar" %in% available) {
        return(character())
      }
      rows <- DBI::dbGetQuery(
        con,
        paste0(
          "SELECT sidecar FROM read_parquet(",
          literal,
          ") WHERE sidecar IS NOT NULL"
        )
      )
      if (!nrow(rows)) {
        return(character())
      }
      values <- lapply(seq_len(nrow(rows)), function(index) {
        fabric_delta_checkpoint_value(rows$sidecar, index, nrow(rows))
      })
      vapply(
        values,
        function(value) {
          as.character(value$path %||% "")
        },
        character(1)
      )
    }),
    use.names = FALSE
  )
  sidecars <- utils::URLdecode(sidecars[nzchar(sidecars)])
  names <- vapply(
    sidecars,
    function(sidecar) {
      normalized <- gsub("\\\\", "/", sidecar)
      absolute <- grepl(
        "^(?:https|abfss?)://",
        normalized,
        ignore.case = TRUE
      )
      if (absolute) {
        parsed <- onelake_parse_uri(normalized)
        parts <- strsplit(sub("^/+", "", parsed$path), "/", fixed = TRUE)[[1L]]
        if (!is.null(target) && !is.null(table_dir)) {
          expected <- paste0(table_dir, "/_delta_log/_sidecars/")
          same_target <- identical(parsed$workspace, target$workspace) &&
            identical(parsed$item, target$item) &&
            startsWith(sub("^/+", "", parsed$path), expected)
          if (!same_target) {
            rlang::abort(
              "Delta V2 checkpoint references a sidecar outside its table"
            )
          }
        }
      } else {
        if (grepl("^/", normalized)) {
          rlang::abort(
            "Delta V2 checkpoint contains an unsafe sidecar-file path"
          )
        }
        parts <- strsplit(normalized, "/", fixed = TRUE)[[1L]]
      }
      if (
        any(!nzchar(parts) | parts %in% c(".", "..")) ||
          length(parts) < 1L
      ) {
        rlang::abort(
          "Delta V2 checkpoint contains an unsafe sidecar-file path"
        )
      }
      name <- utils::tail(parts, 1L)
      parent <- parts[seq_len(length(parts) - 1L)]
      qualified <- length(parent) >= 1L &&
        identical(utils::tail(parent, 1L), "_sidecars") &&
        (length(parent) == 1L ||
          identical(utils::tail(parent, 2L), c("_delta_log", "_sidecars")))
      if (length(parent) && !qualified) {
        rlang::abort(
          "Delta V2 checkpoint sidecar path is not under _delta_log/_sidecars"
        )
      }
      name
    },
    character(1)
  )
  unique(names)
}

#' Apply actions stored in a Delta checkpoint
#' @keywords internal
#' @noRd
fabric_delta_apply_checkpoint <- function(state, checkpoint) {
  if (!is.null(checkpoint$actions)) {
    fabric_delta_validate_checkpoint_actions(checkpoint$actions)
    return(fabric_delta_apply_actions(state, checkpoint$actions))
  }
  adds <- checkpoint$add$path %||% character()
  removes <- checkpoint$remove$path %||% character()
  remove_rows <- which(!is.na(removes))
  for (i in remove_rows) {
    deletion_vector <- fabric_delta_checkpoint_value(
      checkpoint$remove$deletionVector %||% NULL,
      i,
      length(removes)
    )
    state <- fabric_delta_remove_file(
      state,
      list(
        path = removes[[i]],
        deletionVector = fabric_delta_deletion_vector_value(deletion_vector)
      )
    )
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

  protocol_rows <- which(
    !is.na(
      checkpoint$protocol$minReaderVersion %||% numeric()
    )
  )
  if (length(protocol_rows)) {
    if (length(protocol_rows) > 1L) {
      rlang::abort("Delta checkpoint contains multiple protocol actions")
    }
    i <- utils::tail(protocol_rows, 1L)
    protocol_row_count <- length(checkpoint$protocol$minReaderVersion)
    state$protocol <- list(
      minReaderVersion = checkpoint$protocol$minReaderVersion[[i]],
      minWriterVersion = (checkpoint$protocol$minWriterVersion %||%
        rep(NA_integer_, i))[[i]],
      readerFeatures = fabric_delta_checkpoint_value(
        checkpoint$protocol$readerFeatures %||% NULL,
        i,
        protocol_row_count
      ),
      writerFeatures = fabric_delta_checkpoint_value(
        checkpoint$protocol$writerFeatures %||% NULL,
        i,
        protocol_row_count
      )
    )
  }
  metadata_rows <- which(!is.na(checkpoint$metaData$id %||% character()))
  if (length(metadata_rows)) {
    if (length(metadata_rows) > 1L) {
      rlang::abort("Delta checkpoint contains multiple metadata actions")
    }
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
    format <- fabric_delta_checkpoint_value(
      checkpoint$metaData$format %||% NULL,
      i,
      length(checkpoint$metaData$id)
    )
    state$metadata <- list(
      id = checkpoint$metaData$id[[i]],
      format = format,
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
  deletion_vector <- add$deletionVector %||% NULL
  state$active <- c(setdiff(state$active, path), path)
  state$files[[path]] <- list(
    path = path,
    partitionValues = fabric_delta_partition_values(
      add$partitionValues %||% list()
    ),
    deletionVector = deletion_vector,
    deletionVectorId = fabric_delta_deletion_vector_id(deletion_vector)
  )
  state$has_deletion_vectors <- state$has_deletion_vectors ||
    !is.null(deletion_vector)
  state
}

#' Validate singleton state actions in a JSON checkpoint
#' @keywords internal
#' @noRd
fabric_delta_validate_checkpoint_actions <- function(actions) {
  count <- function(name) {
    sum(vapply(
      actions,
      function(action) !is.null(action[[name]]),
      logical(1)
    ))
  }
  if (count("protocol") > 1L) {
    rlang::abort("Delta checkpoint contains multiple protocol actions")
  }
  if (count("metaData") > 1L) {
    rlang::abort("Delta checkpoint contains multiple metadata actions")
  }
  invisible(actions)
}

#' Apply a Delta remove-file action
#' @keywords internal
#' @noRd
fabric_delta_remove_file <- function(state, remove) {
  path <- remove$path
  current <- state$files[[path]] %||% NULL
  if (is.null(current)) {
    return(state)
  }
  removed_id <- fabric_delta_deletion_vector_id(
    remove$deletionVector %||% NULL
  )
  current_id <- current$deletionVectorId %||%
    fabric_delta_deletion_vector_id(current$deletionVector %||% NULL)
  if (!identical(removed_id, current_id)) {
    return(state)
  }
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
      fabric_delta_preserve_partition_tokens(action, line)
    }
  )
  fabric_delta_validate_commit_actions(actions, path)
  fabric_delta_apply_actions(state, actions)
}

#' Validate mutually reconciling actions within one Delta commit
#' @keywords internal
#' @noRd
fabric_delta_validate_commit_actions <- function(actions, path) {
  count <- function(name) {
    sum(vapply(
      actions,
      function(action) !is.null(action[[name]]),
      logical(1)
    ))
  }
  label <- basename(path)
  if (count("metaData") > 1L) {
    rlang::abort(cli::format_inline(
      "Delta commit {.path {label}} contains multiple metadata actions"
    ))
  }
  if (count("protocol") > 1L) {
    rlang::abort(cli::format_inline(
      "Delta commit {.path {label}} contains multiple protocol actions"
    ))
  }
  transactions <- Filter(
    function(action) !is.null(action$txn$appId),
    actions
  )
  if (length(transactions)) {
    app_ids <- vapply(
      transactions,
      function(action) as.character(action$txn$appId),
      character(1)
    )
    if (anyDuplicated(app_ids)) {
      rlang::abort(cli::format_inline(
        "Delta commit {.path {label}} contains duplicate transaction actions"
      ))
    }
  }
  file_keys <- unlist(
    lapply(actions, function(action) {
      values <- list()
      if (!is.null(action$add$path)) {
        values <- c(
          values,
          list(paste0(
            action$add$path,
            "\r",
            fabric_delta_deletion_vector_id(action$add$deletionVector %||% NULL)
          ))
        )
      }
      if (!is.null(action$remove$path)) {
        values <- c(
          values,
          list(paste0(
            action$remove$path,
            "\r",
            fabric_delta_deletion_vector_id(
              action$remove$deletionVector %||% NULL
            )
          ))
        )
      }
      values
    }),
    use.names = FALSE
  )
  if (anyDuplicated(file_keys)) {
    rlang::abort(cli::format_inline(
      "Delta commit {.path {label}} contains conflicting file actions"
    ))
  }
  for (action_name in c("add", "remove")) {
    paths <- unlist(
      lapply(actions, function(action) {
        action[[action_name]]$path %||% NULL
      }),
      use.names = FALSE
    )
    if (anyDuplicated(paths)) {
      rlang::abort(cli::format_inline(
        paste0(
          "Delta commit {.path {label}} contains multiple ",
          action_name,
          " actions for one file path"
        )
      ))
    }
  }
  invisible(actions)
}

#' Apply a collection of Delta actions with remove-before-add reconciliation
#' @keywords internal
#' @noRd
fabric_delta_apply_actions <- function(state, actions) {
  for (action in actions) {
    if (!is.null(action$remove$path)) {
      state <- fabric_delta_remove_file(state, action$remove)
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

#' Validate one Delta protocol version field
#' @keywords internal
#' @noRd
fabric_delta_protocol_version <- function(value, name, supported) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value != floor(value)
  ) {
    fabric_delta_abort_unsupported(paste0(
      "Delta protocol ",
      name,
      " must be one whole number"
    ))
  }
  value <- as.integer(value)
  if (!value %in% supported) {
    fabric_delta_abort_unsupported(paste0(
      "Delta protocol ",
      name,
      " ",
      value
    ))
  }
  value
}

#' Validate one Delta table-feature array
#' @keywords internal
#' @noRd
fabric_delta_protocol_features <- function(value, name) {
  if (is.null(value)) {
    return(character())
  }
  is_character_tree <- function(item) {
    if (is.character(item)) {
      return(TRUE)
    }
    is.list(item) && all(vapply(item, is_character_tree, logical(1)))
  }
  if (!is_character_tree(value)) {
    fabric_delta_abort_unsupported(paste0(
      "Delta protocol ",
      name,
      " must contain unique non-empty strings"
    ))
  }
  features <- unlist(value, use.names = FALSE)
  if (!length(features)) {
    return(character())
  }
  if (
    !is.character(features) ||
      anyNA(features) ||
      !all(nzchar(features)) ||
      anyDuplicated(features)
  ) {
    fabric_delta_abort_unsupported(paste0(
      "Delta protocol ",
      name,
      " must contain unique non-empty strings"
    ))
  }
  features
}

#' Reject Delta reader features not implemented by the staged reader
#' @keywords internal
#' @noRd
fabric_delta_validate_reader <- function(state) {
  protocol <- state$protocol %||% list()
  if (is.null(protocol$minReaderVersion)) {
    rlang::abort("Delta snapshot does not contain a reader protocol action")
  }
  reader_version <- fabric_delta_protocol_version(
    protocol$minReaderVersion,
    "minReaderVersion",
    1:3
  )
  writer_version <- fabric_delta_protocol_version(
    protocol$minWriterVersion,
    "minWriterVersion",
    1:7
  )
  reader_features_present <- !is.null(protocol$readerFeatures)
  writer_features_present <- !is.null(protocol$writerFeatures)
  features <- fabric_delta_protocol_features(
    protocol$readerFeatures,
    "readerFeatures"
  )
  writer_features <- fabric_delta_protocol_features(
    protocol$writerFeatures,
    "writerFeatures"
  )
  unsupported <- setdiff(
    features,
    .fabric_delta_supported_reader_features
  )

  if (is.null(state$metadata)) {
    rlang::abort("Delta snapshot does not contain a metadata action")
  }
  fabric_delta_validate_metadata_format(state$metadata)
  schema <- fabric_delta_schema(state$metadata)
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
  if (reader_version < 3 && reader_features_present) {
    fabric_delta_abort_unsupported(
      paste0(
        "Delta reader protocol version ",
        reader_version,
        " with a readerFeatures field"
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
  if (
    reader_version == 3 &&
      !reader_features_present
  ) {
    fabric_delta_abort_unsupported(
      "Delta reader protocol version 3 without readerFeatures"
    )
  }
  if (length(unsupported)) {
    fabric_delta_abort_unsupported(
      paste0("Delta reader feature(s): ", paste(unsupported, collapse = ", "))
    )
  }
  variant_shredding <- any(
    c("variantShredding", "variantShredding-preview") %in% features
  )
  if (variant_shredding && !"variantType" %in% features) {
    fabric_delta_abort_unsupported(
      "Delta variantShredding without its required variantType feature"
    )
  }
  requirements <- fabric_delta_schema_requirements(schema)
  if (
    requirements$timestamp_ntz &&
      !(reader_version == 3 && "timestampNtz" %in% features)
  ) {
    fabric_delta_abort_unsupported(
      "Delta timestamp_ntz schema without matching timestampNtz support"
    )
  }
  if (
    requirements$variant &&
      !(reader_version == 3 && "variantType" %in% features)
  ) {
    fabric_delta_abort_unsupported(
      "Delta variant schema without matching variantType support"
    )
  }
  if (writer_version < 7 && writer_features_present) {
    fabric_delta_abort_unsupported(
      paste0(
        "Delta writer protocol version ",
        writer_version,
        " with a writerFeatures field"
      )
    )
  }
  if (writer_version == 7 && !writer_features_present) {
    fabric_delta_abort_unsupported(
      "Delta writer protocol version 7 without writerFeatures"
    )
  }
  missing_writer_features <- setdiff(features, writer_features)
  if (length(missing_writer_features)) {
    fabric_delta_abort_unsupported(paste0(
      "Delta reader feature(s) absent from writerFeatures: ",
      paste(missing_writer_features, collapse = ", ")
    ))
  }
  if (requirements$variant && fabric_delta_has_nested_variant(schema)) {
    fabric_delta_abort_unsupported(
      "Delta Variant fields nested inside another complex field"
    )
  }
  if (
    requirements$type_widening &&
      !any(c("typeWidening", "typeWidening-preview") %in% features)
  ) {
    fabric_delta_abort_unsupported(
      "Delta type-change metadata without matching type-widening support"
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
  invisible(state)
}

#' Validate the Delta metadata data-file format
#' @keywords internal
#' @noRd
fabric_delta_validate_metadata_format <- function(metadata) {
  format <- metadata$format %||% NULL
  if (!is.list(format)) {
    rlang::abort("Delta metadata does not contain a valid data format")
  }
  provider <- format$provider %||% NULL
  if (
    !is.character(provider) ||
      length(provider) != 1L ||
      is.na(provider) ||
      !nzchar(provider)
  ) {
    rlang::abort("Delta metadata does not contain a valid format provider")
  }
  if (!identical(tolower(provider), "parquet")) {
    fabric_delta_abort_unsupported(
      paste0("Delta data format ", provider)
    )
  }
  options <- format$options %||% list()
  options <- fabric_delta_partition_values(options)
  if (length(options)) {
    fabric_delta_abort_unsupported(
      "Delta Parquet format options"
    )
  }
  invisible(metadata)
}

#' Collect table-feature requirements encoded in a Delta schema
#' @keywords internal
#' @noRd
fabric_delta_schema_requirements <- function(schema) {
  requirements <- list(
    timestamp_ntz = FALSE,
    variant = FALSE,
    type_widening = FALSE
  )
  visit_field <- function(field) {
    if (length(field$metadata[["delta.typeChanges"]] %||% list())) {
      requirements$type_widening <<- TRUE
    }
    visit_type(field$type)
  }
  visit_type <- function(type) {
    if (is.character(type) && length(type) == 1L) {
      normalized <- tolower(type)
      if (identical(normalized, "timestamp_ntz")) {
        requirements$timestamp_ntz <<- TRUE
      } else if (identical(normalized, "variant")) {
        requirements$variant <<- TRUE
      }
      return(invisible())
    }
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
  lapply(schema$fields %||% list(), visit_field)
  requirements
}

#' Detect Variant fields that cannot retain their Parquet validity independently
#' @keywords internal
#' @noRd
fabric_delta_has_nested_variant <- function(schema) {
  visit <- function(type, nested) {
    if (is.character(type) && length(type) == 1L) {
      return(nested && identical(tolower(type), "variant"))
    }
    if (!is.list(type)) {
      return(FALSE)
    }
    kind <- tolower(as.character(type$type %||% ""))
    if (identical(kind, "struct")) {
      return(any(vapply(
        type$fields %||% list(),
        function(field) visit(field$type, TRUE),
        logical(1)
      )))
    }
    if (identical(kind, "array")) {
      return(visit(type$elementType, TRUE))
    }
    if (identical(kind, "map")) {
      return(visit(type$keyType, TRUE) || visit(type$valueType, TRUE))
    }
    FALSE
  }
  any(vapply(
    schema$fields %||% list(),
    function(field) visit(field$type, FALSE),
    logical(1)
  ))
}

fabric_delta_abort_unsupported <- function(feature) {
  rlang::abort(
    c(
      paste0(feature, " by the staged reader."),
      "i" = paste(
        "This reader supports Delta reader protocols 1 through 3 with",
        "name- and ID-based column mapping, deletion vectors, timestampNtz,",
        "type widening, V2 checkpoints, Variant, and vacuumProtocolCheck."
      ),
      "i" = paste(
        "Query the table through its Fabric SQL analytics endpoint or",
        "Fabric Spark instead."
      )
    ),
    class = "fabric_delta_unsupported_error"
  )
}
