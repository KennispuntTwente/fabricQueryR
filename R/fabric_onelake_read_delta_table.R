#' @title
#' Read a Microsoft Fabric/OneLake Delta table (ADLS Gen2)
#'
#' @description
#' Authenticates to OneLake (ADLS Gen2), resolves the table's
#' `_delta_log` to determine the *current* active Parquet parts,
#' downloads only those parts to a local staging directory, and
#' returns the result as a tibble.
#'
#' @details
#' - In Microsoft Fabric, OneLake exposes each workspace as an ADLS Gen2
#'  filesystem. Within a Lakehouse item, Delta tables are stored under
#'  `Tables/<table>` with a `_delta_log/` directory that tracks commit state.
#'  This helper replays the JSON commits to avoid double-counting
#'  compacted/removed files.
#' - Ensure the account/principal you authenticate with has access via
#'  **Lakehouse -> Manage OneLake data access** (or is a member of the workspace).
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
#' @param tenant_id Character. Entra ID (Azure AD) tenant GUID. Defaults to
#'   `Sys.getenv("FABRICQUERYR_TENANT_ID")` if missing.
#' @param client_id Character. App registration (client) ID. Defaults to
#'   `Sys.getenv("FABRICQUERYR_CLIENT_ID")`, falling back to the Azure CLI app id
#'   `"04b07795-8ddb-461a-bbee-02f9e1bf7b46"` if not set.
#' @param dest_dir Character or `NULL`. Local staging directory for Parquet
#'   parts. If `NULL` (default), a temp dir is used and cleaned up on exit.
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
#' }
fabric_onelake_read_delta_table <- function(
  table_path,
  workspace_name,
  lakehouse_name,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  dest_dir = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
) {
  # ---- validate args ----
  stopifnot(
    is.character(table_path),
    length(table_path) == 1L,
    nzchar(table_path),
    is.character(workspace_name),
    length(workspace_name) == 1L,
    nzchar(workspace_name),
    is.character(lakehouse_name),
    length(lakehouse_name) == 1L,
    nzchar(lakehouse_name)
  )
  if (!nzchar(tenant_id))
    stop(
      "tenant_id is required (or set FABRICQUERYR_TENANT_ID env var).",
      call. = FALSE
    )
  if (!nzchar(client_id))
    stop(
      "client_id is required (or set FABRICQUERYR_CLIENT_ID env var).",
      call. = FALSE
    )

  # ---- deps ----
  rlang::check_installed(
    c(
      "AzureStor",
      "arrow",
      "jsonlite",
      "readr",
      "fs"
    ),
    reason = "to read OneLake Delta tables"
  )

  inform <- function(msg, type = c("info", "warning", "danger", "success")) {
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

  # ---- auth (MSAL v2 + refresh) ----
  inform("Authenticating with {.pkg AzureAuth} (MSAL v2)...")
  token <- fabric_get_storage_token(
    tenant_id = tenant_id,
    client_id = client_id
  )

  # ---- DFS endpoint + filesystem (workspace) ----
  ep <- AzureStor::adls_endpoint(dfs_base, token = token)
  fs_cont <- AzureStor::storage_container(ep, workspace_name)

  # ---- normalize lakehouse item + table dir ----
  lakehouse_item <- fabric_normalize_lakehouse_item(lakehouse_name)
  parts <- strsplit(table_path, "/", fixed = TRUE)[[1]]
  table_name <- parts[length(parts)]
  table_dir <- fs::path(lakehouse_item, "Tables", table_name)
  log_dir <- fs::path(table_dir, "_delta_log")
  inform("Table root: {.path {table_dir}}")

  # ---- list files once ----
  files <- AzureStor::list_storage_files(
    fs_cont,
    dir = table_dir,
    recursive = TRUE
  )
  if (NROW(files) == 0) {
    cli::cli_abort(
      "Nothing found under {.path {table_dir}}. Check names/permissions."
    )
  }

  # ---- resolve active parquet parts via _delta_log ----
  inform("Resolving {.path _delta_log} to determine active Parquet parts...")
  active_rel <- fabric_delta_active_parts(fs_cont, files, log_dir)

  if (!length(active_rel)) {
    cli::cli_abort(
      "Delta log resolution produced no active data files in {.path {table_dir}}."
    )
  }

  data_blobs <- fs::path(table_dir, active_rel)
  if (isTRUE(verbose)) {
    cli::cli_inform("Active parquet parts: {length(data_blobs)}")
    if (length(data_blobs) > 10) {
      # preview first 10 if chatty
      invisible(utils::head(data_blobs, 10))
    }
  }

  # ---- download active parquet parts ----
  auto_cleanup <- is.null(dest_dir)
  dest_dir <- dest_dir %||% fs::path_temp("onelake_tbl_")
  fs::dir_create(dest_dir, recurse = TRUE)
  if (auto_cleanup) {
    on.exit(try(fs::dir_delete(dest_dir), silent = TRUE), add = TRUE)
  }

  inform("Downloading {length(data_blobs)} part{?s} to {.path {dest_dir}} ...")
  AzureStor::storage_multidownload(
    fs_cont,
    src = data_blobs,
    dest = fs::path(dest_dir, basename(data_blobs)),
    overwrite = TRUE
  )

  # ---- read with Arrow ----
  inform("Reading dataset with {.pkg arrow} ...")
  ds <- arrow::open_dataset(dest_dir, format = "parquet")
  df <- dplyr::collect(ds)

  inform("Loaded {nrow(df)} row{?s}.", type = "success")
  tibble::as_tibble(df)
}

#' Get a OneLake (ADLS Gen2) access token with AzureAuth
#'
#' @param tenant_id Azure AD tenant GUID.
#' @param client_id Azure AD application (client) ID.
#' @return A bearer access token suitable for ADLS Gen2 (`Authorization: Bearer ...`).
#' @keywords internal
#' @noRd
fabric_get_storage_token <- function(tenant_id, client_id) {
  tok <- AzureAuth::get_azure_token(
    tenant = tenant_id,
    app = client_id,
    version = 2,
    resource = c("https://storage.azure.com/.default", "offline_access")
  )
  tok$credentials$access_token
}

#' Normalize a Lakehouse item name to include the `.Lakehouse` suffix
#' @keywords internal
#' @noRd
fabric_normalize_lakehouse_item <- function(lakehouse_name) {
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

#' Resolve active parquet parts from Delta _delta_log JSONs
#' @param fs_cont AzureStor container handle.
#' @param files Data frame from `list_storage_files()`.
#' @param log_dir Character path to `_delta_log` dir.
#' @return Character vector of relative parquet paths (relative to `table_dir`).
#' @keywords internal
#' @noRd
fabric_delta_active_parts <- function(fs_cont, files, log_dir) {
  pattern <- stringr::fixed(paste0(log_dir, "/"))
  json_logs <- files$name[
    stringr::str_detect(files$name, pattern) &
      stringr::str_detect(files$name, "[0-9]{20}\\.json$")
  ]

  if (!length(json_logs)) {
    cli::cli_abort("No _delta_log JSON files found under {.path {log_dir}}.")
  }

  seqs <- as.numeric(stringr::str_remove(basename(json_logs), "\\.json$"))
  json_logs <- json_logs[order(seqs)]

  active <- character(0)

  purrr::walk(json_logs, function(p) {
    tmp <- tempfile(fileext = ".json")
    AzureStor::storage_download(fs_cont, src = p, dest = tmp, overwrite = TRUE)

    lines <- readr::read_lines(tmp, progress = FALSE)
    if (!length(lines)) return(invisible())

    recs <- purrr::map(
      lines,
      ~ tryCatch(
        jsonlite::fromJSON(.x, simplifyVector = TRUE),
        error = function(e) NULL
      )
    )

    adds <- purrr::map_chr(
      purrr::keep(recs, ~ !is.null(.x$add$path)),
      ~ .x$add$path,
      .default = character()
    )
    if (length(adds)) {
      adds <- adds[grepl("\\.parquet$", adds, ignore.case = TRUE)]
      if (length(adds)) active <<- union(active, adds)
    }

    removes <- purrr::map_chr(
      purrr::keep(recs, ~ !is.null(.x$remove$path)),
      ~ .x$remove$path,
      .default = character()
    )
    if (length(removes)) active <<- setdiff(active, removes)
  })

  active
}
