.fabric_onelake_table_base <- "https://onelake.table.fabric.microsoft.com/delta"

#' Discover and load Microsoft Fabric Lakehouse tables
#'
#' @description
#' Use Fabric's table APIs to inspect Delta tables, load staged CSV or Parquet
#' files, or write an R/Arrow object through a failure-aware staging workflow.
#'
#' - `fabric_lakehouse_tables()` combines Fabric's paginated List Tables API
#'   with the read-only OneLake Delta table API. The first supplies managed or
#'   external type, format, and location; the second supplies schemas and,
#'   with `detail = TRUE`, column metadata.
#' - `fabric_lakehouse_load_table()` starts the preview Fabric Load Table API
#'   for a file or folder that already exists below the Lakehouse `Files/`
#'   area. It returns a handle accepted by [fabric_operation_status()].
#' - `fabric_lakehouse_write_table()` streams an R or Arrow object to Parquet,
#'   uploads it to a unique `Files/` staging path, waits for the Delta load, and
#'   removes the staged file after confirmed success by default.
#'
#' @param lakehouse Lakehouse GUID, exact display name, or one Lakehouse object
#'   returned by [fabric_lakehouses()]. A discovered object is recommended
#'   because it includes the workspace and default schema.
#' @param workspace Workspace GUID, exact display name, or discovered workspace.
#'   Omit it when `lakehouse` is a discovered object containing `workspaceId`.
#' @param schema Optional Lakehouse schema. When omitted from
#'   `fabric_lakehouse_tables()`, every schema is listed. For loading, a
#'   discovered schema-enabled Lakehouse supplies its documented default
#'   schema; otherwise provide the destination schema explicitly.
#' @param detail Whether table discovery should retrieve per-table column
#'   metadata. Detail retrieval enriches the listing snapshot and never removes
#'   a listed row if a table disappears concurrently. Set to `FALSE` to make
#'   only schema and table-list requests.
#' @param page_size Optional maximum records requested per table API page, from
#'   1 to the Fabric List Tables maximum of 100. All continuation values are
#'   followed regardless of this value.
#' @param table Destination Delta table name. Fabric's Load Table API permits
#'   1 to 256 ASCII letters, numbers, and underscores and requires at least one
#'   letter or underscore.
#' @param path Existing item-relative OneLake source path equal to `"Files"` or
#'   beginning with `Files/`, for example `"Files/incoming/orders.parquet"`.
#' @param path_type Whether `path` names one `"File"` or a `"Folder"`.
#' @param format Source format, `"Parquet"` or `"Csv"`. For a file, `NULL`
#'   infers the format from its extension. A folder should specify the format.
#' @param mode Load mode, `"Overwrite"` or `"Append"`. Overwrite and append
#'   behavior is performed by Fabric's managed Delta load, never by changing
#'   files below `Tables/` directly. Fabric documents overwrite as dropping and
#'   recreating an existing Delta table; the API does not expose a truncate
#'   alternative.
#' @param recursive Whether a folder load should include descendant folders.
#' @param header Whether the first CSV row contains column names.
#' @param delimiter CSV delimiter of 0 to 8 characters. Spaces and tabs are
#'   allowed; Fabric excludes parentheses, brackets, braces, and quotes.
#' @param file_extension Optional extension used to filter a folder load,
#'   without a leading dot.
#' @param data A data frame, tibble, Arrow Table/RecordBatch, lazy Arrow
#'   Dataset/Scanner/query, or Arrow RecordBatchReader to serialize as Parquet.
#'   Lazy inputs are consumed batch by batch without collecting the complete
#'   object in R memory. Arrow-compatible `nanoarrow_array_stream` inputs are
#'   also accepted. Readers and streams are single-use. The optional 'arrow'
#'   package is required.
#' @param staging_root Item-relative directory below `Files/` used for unique
#'   staging files.
#' @param cleanup Whether to delete the staged Parquet files after Fabric
#'   confirms a successful load.
#' @param keep_staging_on_failure Whether to retain a completely uploaded
#'   staging directory when the load fails. The raised condition includes
#'   `staging_path` and `staging_retained` fields.
#' @param compression Parquet compression passed to [arrow::write_parquet()].
#' @param target_file_size Soft maximum bytes per staged Parquet file. A file
#'   rotates after its current Arrow row group reaches this size.
#' @param max_rows_per_file Optional exact maximum rows per staged file. This is
#'   useful when row counts are a more predictable boundary than compressed
#'   bytes.
#' @param poll_interval Minimum seconds between load-operation status requests.
#'   `NULL` follows Fabric's `Retry-After` hint with the shared fallback.
#' @param timeout Maximum total seconds to wait for an R/Arrow load.
#' @param tenant_id Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or audience-aware token-provider function.
#'   Table discovery needs both Fabric- and Storage-audience tokens; staging
#'   needs Storage and loading needs Fabric.
#' @param storage_token Optional separate Azure Storage token or token-provider
#'   function for `fabric_lakehouse_tables()` and
#'   `fabric_lakehouse_write_table()`. Supply it when `token` is a fixed bearer
#'   token or `AzureToken`; automatic and callback credentials obtain both
#'   audiences themselves.
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied.
#' @param api_base Fabric REST API base URL. Most users should keep the default.
#' @param table_api_base OneLake Delta table API base URL. Most users should
#'   keep the default.
#' @param dfs_base OneLake DFS service address used for the staging upload.
#'   A workspace-specific endpoint from a discovered object is preferred when
#'   this argument is not supplied.
#'
#' @section Preview status and permissions:
#' Microsoft marks Fabric's List Tables and Load Table routes as preview or
#' beta and does not recommend them for production use. Loading requires write
#' access to the Lakehouse and the `Lakehouse.ReadWrite.All` delegated scope.
#' Discovery requires `Lakehouse.Read.All` or `Lakehouse.ReadWrite.All` for the
#' Fabric list plus table read permission for OneLake metadata.
#'
#' Fabric currently rejects List Tables for some schema-enabled Lakehouses. In
#' that documented-endpoint/service mismatch, discovery still returns OneLake
#' schema, format, location, and column metadata; `type` can be missing because
#' OneLake currently returns a null table type for those records.
#'
#' Service principals and managed identities are supported by the Load Table
#' API. Tenant and item permissions still determine whether those identities
#' can use OneLake and the Lakehouse.
#'
#' @section Choose an existing-file load or an R-object write:
#' `fabric_lakehouse_load_table()` never uploads a local file or serializes an R
#' object. Its `path` must already exist inside the selected Lakehouse's
#' OneLake `Files/` area. Use [fabric_onelake_upload()] first when intentionally
#' managing that source yourself, or use [fabric_lakehouse_write_table()] for a
#' single call that accepts a data frame, tibble, or Arrow object, stages it,
#' waits for the load, and cleans up.
#'
#' Both load functions can create a missing destination Delta table. Fabric
#' infers its schema from the source. No `create_if_missing` flag is needed.
#'
#' @section Data types and names:
#' Arrow determines the Parquet schema before Fabric infers the destination
#' Delta schema. Ordinary R logical, integer, double, character, `Date`,
#' `POSIXct`, and `bit64::integer64` columns map to their corresponding Parquet
#' logical types. Factors are written as strings. List columns are passed to
#' Arrow as nested data and can fail if their values do not have one consistent
#' Arrow type. R complex and `difftime` columns are rejected.
#'
#' R has no native fixed-precision decimal vector. Supply Arrow data with a
#' decimal field when decimal precision and scale must be explicit. Fabric's
#' Load to Tables flow does not accept a caller-defined destination schema, so
#' use Spark or another schema-controlled writer when inference is unsuitable.
#'
#' To preserve names exactly, `fabric_lakehouse_write_table()` requires unique
#' column names containing only Unicode letters, numbers, and underscores, up
#' to Fabric's documented 128-character limit.
#'
#' @section Failure and cleanup behavior:
#' The high-level writer uploads complete Parquet parts atomically to a unique
#' folder and starts the managed folder load only after every upload succeeds. A
#' successful load is a committed Delta operation. On failure, the destination
#' is left to Fabric's transactional load behavior and 'fabricQueryR' never
#' edits `Tables/` files.
#'
#' Retained staging paths are included in `fabric_lakehouse_write_error`
#' conditions so the source can be inspected or passed to
#' `fabric_lakehouse_load_table()` again. Cleanup failures after a successful
#' load produce a warning and return `staging_retained = TRUE`; they do not make
#' a committed table load appear to have failed. Once Fabric accepts a load,
#' staging is retained if status polling loses access or fails ambiguously;
#' only a confirmed terminal operation failure permits failure cleanup.
#'
#' @return `fabric_lakehouse_tables()` returns a tibble with table `name`,
#'   `schema`, `full_name`, `type`, `format`, `location`, timestamps, list-column
#'   `columns`, `schema_metadata`, the unmodified OneLake `raw` record, and the
#'   matching unmodified Fabric `fabric_raw` record. Unknown future metadata
#'   remains available in those raw list columns.
#'
#'   `fabric_lakehouse_load_table()` returns a reusable `fabric_operation`.
#'   Pass it to [fabric_operation_status()], [fabric_operation_wait()], or
#'   [fabric_operation_result()].
#'
#'   `fabric_lakehouse_write_table()` returns a
#'   `fabric_lakehouse_write_result` containing the destination, row count,
#'   terminal operation state, staging path, and whether staging was retained.
#'
#' @references
#' [OneLake table APIs for Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)
#'
#' [Getting started with OneLake Delta table APIs](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-get-started)
#'
#' [Arrow RecordBatchReader](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html)
#'
#' [List Lakehouse tables](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/list-tables)
#'
#' [Load a Lakehouse table](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/load-table)
#'
#' [Load a schema Lakehouse table (beta)](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/load-schema-table%28beta%29)
#'
#' [Load to Delta Lake tables](https://learn.microsoft.com/en-us/fabric/data-engineering/load-to-tables)
#'
#' @examples
#' \dontrun{
#' # Discover a Lakehouse instead of copying its workspace and item IDs
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#'
#' # List its existing Delta tables
#' tables <- fabric_lakehouse_tables(lakehouse)
#'
#' # Discover a CSV already stored in this Lakehouse's Files area
#' files <- fabric_onelake_list(
#'   workspace,
#'   lakehouse,
#'   path = "Files/incoming"
#' )
#' csv_file <- files[grepl("[.]csv$", files$path), ][1L, ]
#'
#' # Load that discovered CSV into a managed Delta table
#' operation <- fabric_lakehouse_load_table(
#'   lakehouse,
#'   table = "orders_from_csv",
#'   path = csv_file$path[[1L]],
#'   format = "Csv",
#'   header = TRUE,
#'   delimiter = ","
#' )
#' fabric_operation_wait(operation, timeout = 900)
#'
#' # Or stage an R data frame and write it as a managed Delta table
#' result <- fabric_lakehouse_write_table(
#'   lakehouse,
#'   table = "orders_from_r",
#'   data = data.frame(id = 1:3, amount = c(10.5, NA, 30))
#' )
#' result$operation_status$status
#' }
#' @name fabric_lakehouse_tables
NULL

#' Read a Microsoft Fabric Lakehouse table
#'
#' Provides the symmetric read counterpart to [fabric_lakehouse_write_table()].
#' It resolves a discovered Lakehouse object and table record, then delegates to the
#' authenticated OneLake Delta reader. Use `result = "arrow_stream"` to keep a
#' larger result out of R memory.
#'
#' @param lakehouse Lakehouse GUID, exact display name, or one Lakehouse object
#'   returned by [fabric_lakehouses()]. A discovered object is recommended
#'   because it carries its workspace ID and default schema.
#' @param table Table name or one row returned by [fabric_lakehouse_tables()].
#' @param workspace Workspace GUID, exact display name, or discovered workspace.
#'   Omit it when `lakehouse` is a discovered object containing `workspaceId`.
#' @param schema Optional schema. A table record supplies its schema when this
#'   argument is omitted.
#' @param columns Optional unique column names to project before collection.
#' @param limit Optional non-negative maximum number of rows to return.
#' @param version Optional non-negative Delta table version for time travel.
#' @param result Return a `"tibble"` or a disk-backed, single-use
#'   `"arrow_stream"`. Release a stream with `stream[["release"]]()` when using
#'   'nanoarrow' directly, or close the 'arrow' reader that takes ownership of it
#' @param verbose Whether to report authentication and read progress.
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or audience-aware token-provider function.
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()].
#' @param dfs_base OneLake DFS service address. A private or regional endpoint
#'   on a discovered object is preferred when this argument is omitted.
#'
#' @return A tibble, or a disk-backed `nanoarrow_array_stream` when
#'   `result = "arrow_stream"`. Explicit release deletes its temporary file.
#' @references
#' [OneLake table APIs for Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)
#'
#' [Connect to OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)
#' @export
#' @examples
#' \dontrun{
#' # Discover both the Lakehouse and the table to read
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#' tables <- fabric_lakehouse_tables(lakehouse)
#' table <- tables[1L, ]
#'
#' # Read the discovered table into a tibble
#' rows <- fabric_lakehouse_read_table(lakehouse, table)
#'
#' # Stream selected columns when the full table may not fit in R memory
#' stream <- fabric_lakehouse_read_table(
#'   lakehouse,
#'   table,
#'   result = "arrow_stream"
#' )
#' reader <- arrow::as_record_batch_reader(stream)
#' rows <- reader$read_table()
#' reader$Close()
#' }
fabric_lakehouse_read_table <- function(
  lakehouse,
  table,
  workspace = NULL,
  schema = NULL,
  columns = NULL,
  limit = NULL,
  version = NULL,
  result = c("tibble", "arrow_stream"),
  verbose = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
) {
  dfs_base_supplied <- !missing(dfs_base)
  schema_supplied <- !is.null(schema)
  default_schema <- NULL
  lakehouse_record <- fabric_as_record(lakehouse)
  if (!is.null(lakehouse_record)) {
    type <- tolower(fabric_record_value(lakehouse_record, "type") %||% "")
    if (!identical(type, "lakehouse")) {
      .fabric_abort(
        "lakehouse discovery record must be a Lakehouse item",
        class = c("fabric_lakehouse_read_error", "fabric_delta_error")
      )
    }
    workspace <- workspace %||%
      fabric_record_value(
        lakehouse_record,
        "workspaceId",
        "workspace_id"
      )
    default_schema <- fabric_record_value(
      lakehouse_record,
      "default_schema",
      "defaultSchema"
    )
  }
  if (is.null(workspace)) {
    .fabric_abort(
      paste0(
        "workspace is required unless lakehouse is a discovered object ",
        "containing workspaceId"
      ),
      class = c("fabric_lakehouse_read_error", "fabric_delta_error")
    )
  }

  table_record <- fabric_as_record(table)
  table_schema <- NULL
  storage_target <- NULL
  if (!is.null(table_record)) {
    table <- fabric_record_value(table_record, "name", "table")
    table_schema <- fabric_record_value(table_record, "schema")
    storage_target <- .fabric_onelake_table_storage_target(table_record)
    if (!schema_supplied && !is.null(storage_target)) {
      table <- storage_target$table
      schema <- storage_target$schema
    }
  }
  if (schema_supplied || is.null(storage_target)) {
    schema <- schema %||% table_schema %||% default_schema
  }
  lakehouse_target <- if (!is.null(storage_target) && is.null(schema)) {
    fabric_record_value(lakehouse_record, "id")
  } else {
    lakehouse
  }

  fabric_onelake_read_delta_table(
    table_path = table,
    workspace_name = workspace,
    lakehouse_name = lakehouse_target,
    schema = schema,
    item_type = "Lakehouse",
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    version = version,
    verbose = verbose,
    dfs_base = if (dfs_base_supplied) dfs_base else NULL,
    columns = columns,
    limit = limit,
    result = result
  )
}

#' @rdname fabric_lakehouse_tables
#' @export
fabric_lakehouse_tables <- function(
  lakehouse,
  workspace = NULL,
  schema = NULL,
  detail = TRUE,
  page_size = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  table_api_base = .fabric_onelake_table_base,
  storage_token = NULL
) {
  # 1 Validate options and resolve the Lakehouse --------------------------------------------------

  .fabric_operation_logical(detail, "detail")
  .fabric_onelake_table_page_size(page_size)
  if (!is.null(schema)) {
    .fabric_lakehouse_nonempty(schema, "schema")
  }

  api_base_supplied <- !missing(api_base)
  base <- fabric_api_base(api_base)
  table_base <- .fabric_onelake_table_api_base(
    table_api_base,
    error_class = c(
      "fabric_lakehouse_endpoint_error",
      "fabric_lakehouse_error"
    )
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  storage_credential <- fabric_service_credential(
    credential,
    storage_token,
    "storage_token",
    "fabric_lakehouse_tables()"
  )
  target <- .fabric_lakehouse_target(
    lakehouse,
    workspace,
    credential,
    base,
    use_workspace_endpoint = !api_base_supplied
  )

  # 2 Read Fabric's paginated inventory for authoritative type and location ----------------------

  fabric_records <- .fabric_lakehouse_fabric_inventory(
    target,
    credential,
    page_size
  )

  # 3 List schemas, then every OneLake metadata page ---------------------------------------------

  .fabric_onelake_table_inventory(
    workspace_id = target$workspace_id,
    item_id = target$lakehouse_id,
    schema = schema,
    detail = detail,
    page_size = page_size,
    credential = storage_credential,
    table_base = table_base,
    fabric_records = fabric_records,
    error_class = c(
      "fabric_lakehouse_protocol_error",
      "fabric_lakehouse_error"
    )
  )
}

.fabric_lakehouse_fabric_inventory <- function(
  target,
  credential,
  page_size
) {
  tryCatch(
    .fabric_lakehouse_fabric_table_pages(target, credential, page_size),
    fabric_http_error = function(error) {
      if (
        identical(
          error$error_code,
          "UnsupportedOperationForSchemasEnabledLakehouse"
        )
      ) {
        return(list())
      }
      .fabric_abort(
        "Could not list tables in the Lakehouse",
        parent = error
      )
    }
  )
}

# Follow Fabric List Tables continuation URIs/tokens. This source is the only
# documented table inventory that promises Managed/External type metadata
.fabric_lakehouse_fabric_table_pages <- function(
  target,
  credential,
  page_size
) {
  request <- httr2::request(paste0(
    target$api_base,
    "/workspaces/",
    target$workspace_id,
    "/lakehouses/",
    target$lakehouse_id,
    "/tables"
  ))
  if (!is.null(page_size)) {
    request <- httr2::req_url_query(request, maxResults = as.integer(page_size))
  }
  .httr2_collection(
    request$url,
    credential = credential,
    audience = .fabric_audience$fabric,
    value_key = "data"
  )
}

#' @rdname fabric_lakehouse_tables
#' @export
fabric_lakehouse_load_table <- function(
  lakehouse,
  table,
  path,
  workspace = NULL,
  schema = NULL,
  path_type = c("File", "Folder"),
  format = NULL,
  mode = c("Overwrite", "Append"),
  recursive = FALSE,
  header = TRUE,
  delimiter = ",",
  file_extension = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  # 1 Resolve the destination and authentication --------------------------------------------------

  api_base_supplied <- !missing(api_base)
  base <- fabric_api_base(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  target <- .fabric_lakehouse_target(
    lakehouse,
    workspace,
    credential,
    base,
    use_workspace_endpoint = !api_base_supplied
  )
  schema <- schema %||% target$default_schema

  # 2 Validate and submit one non-idempotent load -------------------------------------------------

  settings <- .fabric_lakehouse_load_settings(
    table = table,
    path = path,
    schema = schema,
    path_type = path_type,
    format = format,
    mode = mode,
    recursive = recursive,
    header = header,
    delimiter = delimiter,
    file_extension = file_extension
  )
  .fabric_lakehouse_load_submit(
    target,
    settings,
    credential
  )
}

#' @rdname fabric_lakehouse_tables
#' @export
fabric_lakehouse_write_table <- function(
  lakehouse,
  table,
  data,
  workspace = NULL,
  schema = NULL,
  mode = c("Overwrite", "Append"),
  staging_root = "Files/fabricqueryr-staging",
  cleanup = TRUE,
  keep_staging_on_failure = TRUE,
  compression = "snappy",
  target_file_size = 512 * 1024^2,
  max_rows_per_file = NULL,
  poll_interval = NULL,
  timeout = 900,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  storage_token = NULL
) {
  # 1 Validate local inputs before authentication or network I/O ---------------------------------

  .fabric_operation_logical(cleanup, "cleanup")
  .fabric_operation_logical(keep_staging_on_failure, "keep_staging_on_failure")
  .fabric_operation_poll_interval(poll_interval)
  .fabric_operation_timeout(timeout)
  prepared <- .fabric_lakehouse_prepare_data(data)
  .fabric_lakehouse_column_names(prepared$names)
  .fabric_lakehouse_nonempty(compression, "compression")
  staging_root <- .fabric_lakehouse_files_path(staging_root, "staging_root")

  # Validate table and mode using the same contract as direct file loading
  settings <- .fabric_lakehouse_load_settings(
    table = table,
    path = paste0(staging_root, "/placeholder"),
    schema = schema,
    path_type = "Folder",
    format = "Parquet",
    mode = mode,
    recursive = FALSE,
    header = TRUE,
    delimiter = ",",
    file_extension = "parquet"
  )

  # 2 Serialize to bounded temporary Parquet parts ------------------------------------------------

  parquet_directory <- tempfile("fabricqueryr-table-")
  dir.create(parquet_directory)
  on.exit(
    unlink(parquet_directory, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  serialized <- .fabric_parquet_write_dataset(
    prepared,
    directory = parquet_directory,
    compression = compression,
    target_file_size = target_file_size,
    max_rows_per_file = max_rows_per_file,
    caller = "fabric_lakehouse_write_table()",
    error_class = c(
      "fabric_lakehouse_arrow_error",
      "fabric_lakehouse_error"
    )
  )

  # 3 Resolve Fabric and OneLake targets with one audience-aware credential ----------------------

  api_base_supplied <- !missing(api_base)
  dfs_base_supplied <- !missing(dfs_base)
  base <- fabric_api_base(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  storage_credential <- fabric_service_credential(
    credential,
    storage_token,
    "storage_token",
    "fabric_lakehouse_write_table()"
  )
  target <- .fabric_lakehouse_target(
    lakehouse,
    workspace,
    credential,
    base,
    use_workspace_endpoint = !api_base_supplied
  )
  settings$schema <- settings$schema %||% target$default_schema
  if (!is.null(settings$schema)) {
    .fabric_lakehouse_schema_name(settings$schema)
  }
  staging_path <- paste(
    staging_root,
    .fabric_lakehouse_staging_id(),
    sep = "/"
  )
  settings$path <- staging_path
  storage_targets <- lapply(basename(serialized$paths), function(name) {
    onelake_resolve_target(
      target$workspace_record %||% target$workspace_id,
      target$lakehouse_record %||% target$lakehouse_id,
      paste(staging_path, name, sep = "/"),
      dfs_base = if (dfs_base_supplied) dfs_base else NULL
    )
  })
  storage_target <- storage_targets[[1L]]

  # 4 Upload every complete staged part -----------------------------------------------------------

  tryCatch(
    for (index in seq_along(storage_targets)) {
      onelake_upload_target(
        storage_targets[[index]],
        storage_credential,
        source = serialized$paths[[index]],
        overwrite = FALSE,
        if_match = NULL,
        chunk_size = getOption(
          "fabricqueryr.onelake.chunk_size",
          8 * 1024^2
        ),
        content_type = "application/vnd.apache.parquet",
        create_parents = TRUE
      )
    },
    error = function(error) {
      .fabric_lakehouse_write_abort(
        error,
        storage_target,
        storage_credential,
        staging_path,
        keep_staging_on_failure
      )
    }
  )

  # 5 Start and wait for the managed Delta load --------------------------------------------------

  operation <- tryCatch(
    .fabric_lakehouse_load_submit(
      target,
      settings,
      credential
    ),
    error = function(error) {
      .fabric_lakehouse_write_abort(
        error,
        storage_target,
        storage_credential,
        staging_path,
        keep_staging_on_failure
      )
    }
  )
  state <- tryCatch(
    fabric_operation_wait(
      operation,
      poll_interval = poll_interval,
      timeout = timeout
    ),
    error = function(error) {
      .fabric_lakehouse_write_abort(
        error,
        storage_target,
        storage_credential,
        staging_path,
        keep_staging_on_failure,
        operation_accepted = TRUE
      )
    }
  )

  # 6 Clean only after confirmed success ----------------------------------------------------------

  staging_retained <- TRUE
  if (isTRUE(cleanup)) {
    removed <- .fabric_lakehouse_remove_staging(
      storage_target,
      storage_credential
    )
    staging_retained <- !removed
    if (!removed) {
      .fabric_warn(
        c(
          "Staging cleanup failed after the Lakehouse table load succeeded",
          "i" = "Staged files remain at {.path {staging_path}}"
        ),
        .format = TRUE
      )
    }
  }
  structure(
    list(
      workspace_id = target$workspace_id,
      lakehouse_id = target$lakehouse_id,
      schema = settings$schema,
      table = settings$table,
      mode = settings$mode,
      rows = serialized$rows,
      bytes = serialized$total_bytes,
      file_count = serialized$file_count,
      files = vapply(storage_targets, `[[`, character(1), "path"),
      staging_path = staging_path,
      staging_retained = staging_retained,
      operation_status = state,
      operation = state$operation
    ),
    class = "fabric_lakehouse_write_result"
  )
}

# Resolve one Lakehouse record/name/ID into the IDs and endpoints used by all
# table APIs. Returns the original records as well for OneLake private routing
.fabric_lakehouse_target <- function(
  lakehouse,
  workspace,
  credential,
  api_base,
  use_workspace_endpoint
) {
  lakehouse_record <- fabric_as_record(lakehouse)
  workspace_record <- fabric_as_record(workspace)
  record_type <- fabric_record_value(lakehouse_record %||% list(), "type")
  if (!is.null(record_type) && !identical(tolower(record_type), "lakehouse")) {
    .fabric_abort(
      paste0("`lakehouse` has type '", record_type, "', not 'Lakehouse'"),
      class = c("fabric_lakehouse_validation_error", "fabric_lakehouse_error")
    )
  }
  target <- .fabric_job_target(
    item = lakehouse,
    workspace = workspace,
    item_type = "Lakehouse",
    credential = credential,
    api_base = api_base,
    use_workspace_endpoint = use_workspace_endpoint
  )
  list(
    workspace_id = target$workspace_id,
    lakehouse_id = target$item_id,
    api_base = target$api_base,
    default_schema = fabric_record_value(
      lakehouse_record %||% list(),
      "defaultSchema",
      "default_schema"
    ),
    lakehouse_record = lakehouse_record,
    workspace_record = workspace_record
  )
}

# Validate and normalize the dedicated OneLake Delta metadata endpoint
.fabric_onelake_table_api_base <- function(
  value,
  error_class
) {
  .fabric_lakehouse_nonempty(value, "table_api_base")
  endpoint <- sub("/+$", "", trimws(value))
  parsed <- try(httr2::url_parse(endpoint), silent = TRUE)
  path <- if (inherits(parsed, "try-error")) {
    ""
  } else {
    sub("/+$", "", parsed$path %||% "")
  }
  host <- if (inherits(parsed, "try-error")) {
    ""
  } else {
    tolower(parsed$hostname %||% "")
  }
  clean <- !inherits(parsed, "try-error") &&
    identical(tolower(parsed$scheme %||% ""), "https") &&
    nzchar(host) &&
    !nzchar(parsed$username %||% "") &&
    !nzchar(parsed$password %||% "") &&
    (parsed$port %||% "") %in% c("", "443") &&
    path %in% c("", "/delta") &&
    length(parsed$query %||% list()) == 0L &&
    !nzchar(parsed$fragment %||% "")
  if (!clean) {
    .fabric_abort(
      "table_api_base must be an HTTPS origin with an optional /delta path",
      class = error_class
    )
  }
  if (identical(tolower(path), "/delta")) {
    endpoint
  } else {
    paste0(endpoint, "/delta")
  }
}

# List schemas and tables through OneLake's Unity Catalog-compatible metadata
# endpoint. Lakehouse discovery can additionally merge its Fabric REST table
# inventory; other OneLake item types leave fabric_records empty.
.fabric_onelake_table_inventory <- function(
  workspace_id,
  item_id,
  schema,
  detail,
  page_size,
  credential,
  table_base,
  fabric_records = list(),
  error_class
) {
  catalog_url <- paste0(
    table_base,
    "/",
    onelake_encode_path(workspace_id, item_id),
    "/api/2.1/unity-catalog"
  )
  schema_records <- if (is.null(schema)) {
    .fabric_onelake_table_pages(
      paste0(catalog_url, "/schemas"),
      field = "schemas",
      query = list(catalog_name = item_id),
      credential = credential,
      page_size = page_size,
      error_class = error_class
    )
  } else {
    list(list(name = schema))
  }
  schema_names <- vapply(
    schema_records,
    function(record) {
      value <- record$name
      if (
        !is.character(value) ||
          length(value) != 1L ||
          is.na(value) ||
          !nzchar(value)
      ) {
        .fabric_abort(
          "OneLake returned schema metadata without one non-empty name",
          class = error_class
        )
      }
      value
    },
    character(1)
  )

  rows <- list()
  for (schema_index in seq_along(schema_names)) {
    schema_name <- schema_names[[schema_index]]
    records <- .fabric_onelake_table_pages(
      paste0(catalog_url, "/tables"),
      field = "tables",
      query = list(
        catalog_name = item_id,
        schema_name = schema_name
      ),
      credential = credential,
      page_size = page_size,
      error_class = error_class
    )

    for (record in records) {
      table_name <- record$name
      if (
        !is.character(table_name) ||
          length(table_name) != 1L ||
          is.na(table_name) ||
          !nzchar(table_name)
      ) {
        .fabric_abort(
          "OneLake returned table metadata without one non-empty name",
          class = error_class
        )
      }
      table_schema <- record$schema_name %||% schema_name
      full_name <- paste(item_id, table_schema, table_name, sep = ".")
      if (isTRUE(detail)) {
        detail_url <- paste0(
          catalog_url,
          "/tables/",
          utils::URLencode(full_name, reserved = TRUE)
        )
        detail_record <- tryCatch(
          .httr2_json(
            httr2::req_url_query(
              httr2::request(detail_url),
              catalog_name = item_id,
              schema_name = table_schema
            ),
            simplifyVector = FALSE,
            credential = credential,
            audience = .fabric_audience$storage
          ),
          fabric_http_error = function(error) {
            if (
              identical(error$status, 404L) &&
                identical(error$error_code, "TableNotFound")
            ) {
              return(NULL)
            }
            rlang::cnd_signal(error)
          }
        )
        if (!is.null(detail_record)) {
          record <- utils::modifyList(record, detail_record)
        }
      }
      rows[[length(rows) + 1L]] <- .fabric_onelake_table_row(
        record,
        schema_name = table_schema,
        schema_record = schema_records[[schema_index]],
        fabric_record = .fabric_lakehouse_match_fabric_table(
          record,
          table_schema,
          fabric_records
        )
      )
    }
  }

  .fabric_onelake_table_tibble(rows)
}

# Follow Unity Catalog-compatible page tokens for a schema or table collection
.fabric_onelake_table_pages <- function(
  url,
  field,
  query,
  credential,
  page_size,
  error_class,
  max_pages = getOption("fabricqueryr.onelake.max_pages", 10000L),
  pagination_timeout = getOption(
    "fabricqueryr.onelake.pagination_timeout",
    300
  ),
  .now = Sys.time
) {
  records <- list()
  page_token <- NULL
  page <- 0L
  seen_urls <- character()
  limits <- .fabric_onelake_pagination_limits(
    max_pages,
    pagination_timeout,
    .now
  )
  repeat {
    request_query <- c(
      list(httr2::request(url)),
      query,
      list(
        max_results = page_size,
        page_token = page_token
      )
    )
    request_query <- request_query[!vapply(request_query, is.null, logical(1))]
    request <- do.call(httr2::req_url_query, request_query)
    page <- page + 1L
    seen_urls <- .httr2_pagination_guard(
      request$url,
      seen_urls,
      page,
      max_pages = limits$max_pages,
      deadline = limits$deadline,
      .now = .now,
      error_class = error_class
    )
    body <- .httr2_json(
      request,
      simplifyVector = FALSE,
      credential = credential,
      audience = .fabric_audience$storage,
      deadline = limits$deadline,
      .now = .now
    )
    values <- body[[field]] %||% list()
    if (!is.list(values)) {
      .fabric_abort(
        paste0("OneLake returned an invalid `", field, "` collection"),
        class = error_class
      )
    }
    records <- c(records, values)
    page_token <- body$next_page_token %||% body$nextPageToken
    if (is.null(page_token)) {
      break
    }
    if (
      !is.character(page_token) || length(page_token) != 1L || is.na(page_token)
    ) {
      .fabric_abort(
        "OneLake returned an invalid next_page_token",
        class = error_class
      )
    }
    if (!nzchar(page_token)) break
  }
  records
}

# Build the normalized row while preserving columns and all unknown raw fields
.fabric_onelake_table_row <- function(
  record,
  schema_name,
  schema_record,
  fabric_record
) {
  resolved_schema <- record$schema_name %||% schema_name
  list(
    name = as.character(record$name),
    schema = as.character(resolved_schema),
    full_name = as.character(
      record$full_name %||%
        paste(
          resolved_schema,
          record$name,
          sep = "."
        )
    ),
    type = as.character(
      fabric_record$type %||%
        record$table_type %||%
        record$type %||%
        NA_character_
    ),
    format = as.character(
      fabric_record$format %||%
        record$data_source_format %||%
        record$format %||%
        NA_character_
    ),
    location = as.character(
      fabric_record$location %||%
        record$storage_location %||%
        record$location %||%
        NA_character_
    ),
    comment = as.character(record$comment %||% NA_character_),
    table_id = as.character(record$table_id %||% NA_character_),
    created_at = .fabric_onelake_epoch_ms(record$created_at),
    updated_at = .fabric_onelake_epoch_ms(record$updated_at),
    columns = record$columns %||% list(),
    schema_metadata = schema_record,
    raw = record,
    fabric_raw = fabric_record
  )
}

# Match inventories by their path below Tables, falling back to an unambiguous
# table name for service responses that omit or transform a storage location
.fabric_lakehouse_match_fabric_table <- function(
  record,
  schema_name,
  fabric_records
) {
  if (!length(fabric_records)) {
    return(list())
  }
  record_key <- .fabric_lakehouse_table_location_key(
    record$storage_location %||% record$location
  )
  if (!is.null(record_key)) {
    keys <- vapply(
      fabric_records,
      function(candidate) {
        .fabric_lakehouse_table_location_key(candidate$location) %||% ""
      },
      character(1)
    )
    matches <- which(keys == record_key)
    if (length(matches) == 1L) return(fabric_records[[matches]])
  }
  expected_names <- c(
    record$name,
    paste(schema_name, record$name, sep = ".")
  )
  matches <- which(vapply(
    fabric_records,
    function(candidate) {
      is.character(candidate$name) &&
        length(candidate$name) == 1L &&
        !is.na(candidate$name) &&
        candidate$name %in% expected_names
    },
    logical(1)
  ))
  if (length(matches) == 1L) fabric_records[[matches]] else list()
}

.fabric_lakehouse_table_location_key <- function(value) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    return(NULL)
  }
  normalized <- sub(
    "/+$",
    "",
    gsub("\\\\", "/", utils::URLdecode(value))
  )
  match <- regexec("/Tables/(.+)$", normalized, ignore.case = TRUE)
  parts <- regmatches(normalized, match)[[1L]]
  if (length(parts) < 2L || !nzchar(parts[[2L]])) NULL else parts[[2L]]
}

# Bind normalized table rows into a stable tibble, including empty list columns
.fabric_onelake_table_tibble <- function(rows) {
  empty <- tibble::tibble(
    name = character(),
    schema = character(),
    full_name = character(),
    type = character(),
    format = character(),
    location = character(),
    comment = character(),
    table_id = character(),
    created_at = as.POSIXct(character(), tz = "UTC"),
    updated_at = as.POSIXct(character(), tz = "UTC"),
    columns = list(),
    schema_metadata = list(),
    raw = list(),
    fabric_raw = list()
  )
  if (!length(rows)) {
    return(empty)
  }
  tibble::tibble(
    name = vapply(rows, `[[`, character(1), "name"),
    schema = vapply(rows, `[[`, character(1), "schema"),
    full_name = vapply(rows, `[[`, character(1), "full_name"),
    type = vapply(rows, `[[`, character(1), "type"),
    format = vapply(rows, `[[`, character(1), "format"),
    location = vapply(rows, `[[`, character(1), "location"),
    comment = vapply(rows, `[[`, character(1), "comment"),
    table_id = vapply(rows, `[[`, character(1), "table_id"),
    created_at = as.POSIXct(
      vapply(rows, function(row) as.numeric(row$created_at), numeric(1)),
      origin = "1970-01-01",
      tz = "UTC"
    ),
    updated_at = as.POSIXct(
      vapply(rows, function(row) as.numeric(row$updated_at), numeric(1)),
      origin = "1970-01-01",
      tz = "UTC"
    ),
    columns = lapply(rows, `[[`, "columns"),
    schema_metadata = lapply(rows, `[[`, "schema_metadata"),
    raw = lapply(rows, `[[`, "raw"),
    fabric_raw = lapply(rows, `[[`, "fabric_raw")
  )
}

# Convert Unity Catalog epoch milliseconds to UTC POSIXct, retaining missingness
.fabric_onelake_epoch_ms <- function(value) {
  number <- suppressWarnings(as.numeric(value %||% NA_real_))
  if (length(number) != 1L || is.na(number) || !is.finite(number)) {
    return(as.POSIXct(NA_real_, origin = "1970-01-01", tz = "UTC"))
  }
  as.POSIXct(number / 1000, origin = "1970-01-01", tz = "UTC")
}

# Validate all Load Table fields and build normalized settings for submission
.fabric_lakehouse_load_settings <- function(
  table,
  path,
  schema,
  path_type,
  format,
  mode,
  recursive,
  header,
  delimiter,
  file_extension
) {
  .fabric_lakehouse_table_name(table)
  if (!is.null(schema)) {
    .fabric_lakehouse_schema_name(schema)
  }
  path <- .fabric_lakehouse_files_path(path, "path")
  path_type <- .fabric_lakehouse_choice(
    path_type,
    c("File", "Folder"),
    "path_type"
  )
  mode <- .fabric_lakehouse_choice(mode, c("Overwrite", "Append"), "mode")
  .fabric_operation_logical(recursive, "recursive")
  .fabric_operation_logical(header, "header")
  if (identical(path_type, "File") && isTRUE(recursive)) {
    .fabric_abort("recursive = TRUE requires path_type = \"Folder\"")
  }
  if (!is.null(file_extension)) {
    .fabric_lakehouse_nonempty(file_extension, "file_extension")
    file_extension <- sub("^[.]", "", file_extension)
    if (!grepl("^[A-Za-z0-9_-]{1,16}$", file_extension)) {
      .fabric_abort(
        "file_extension must contain 1 to 16 letters, numbers, underscores, or hyphens"
      )
    }
    if (identical(path_type, "File")) {
      .fabric_abort("file_extension is only used with path_type = \"Folder\"")
    }
  }
  if (is.null(format)) {
    extension <- if (identical(path_type, "File")) {
      tools::file_ext(path)
    } else {
      file_extension
    }
    format <- switch(
      tolower(extension %||% ""),
      csv = "Csv",
      parquet = "Parquet",
      .fabric_abort(
        "format must be supplied when it cannot be inferred as CSV or Parquet"
      )
    )
  } else {
    format <- .fabric_lakehouse_choice(format, c("Parquet", "Csv"), "format")
  }
  if (identical(format, "Csv")) {
    invalid_delimiter <- !is.character(delimiter) ||
      length(delimiter) != 1L ||
      is.na(delimiter) ||
      nchar(delimiter) > 8L ||
      grepl("[()\\[\\]{}'\"]", delimiter, perl = TRUE)
    if (invalid_delimiter) {
      .fabric_abort(
        "delimiter must be 0 to 8 characters without brackets, braces, parentheses, or quotes"
      )
    }
  }
  list(
    table = table,
    schema = schema,
    path = path,
    path_type = path_type,
    format = format,
    mode = mode,
    recursive = recursive,
    header = header,
    delimiter = delimiter,
    file_extension = file_extension
  )
}

# Submit the preview load request once and attach useful destination metadata
.fabric_lakehouse_load_submit <- function(
  target,
  settings,
  credential
) {
  base_url <- paste0(
    target$api_base,
    "/workspaces/",
    target$workspace_id,
    "/lakehouses/",
    target$lakehouse_id
  )
  url <- if (is.null(settings$schema)) {
    paste0(base_url, "/tables/", settings$table, "/load")
  } else {
    paste0(
      base_url,
      "/schemas/",
      settings$schema,
      "/tables/",
      settings$table,
      "/load"
    )
  }
  request <- httr2::request(url) |>
    httr2::req_method("POST")
  if (!is.null(settings$schema)) {
    request <- httr2::req_url_query(request, beta = "true")
  }
  payload <- Filter(
    Negate(is.null),
    list(
      relativePath = settings$path,
      pathType = settings$path_type,
      mode = settings$mode,
      recursive = settings$recursive,
      fileExtension = settings$file_extension,
      formatOptions = if (identical(settings$format, "Csv")) {
        list(
          format = "Csv",
          header = settings$header,
          delimiter = settings$delimiter
        )
      } else {
        list(format = "Parquet")
      }
    )
  )
  request <- httr2::req_body_json(request, payload)
  operation <- .fabric_operation_submit(
    request,
    credential,
    api_base = target$api_base,
    idempotent = FALSE,
    result_expected = FALSE
  )
  operation$workspace_id <- target$workspace_id
  operation$lakehouse_id <- target$lakehouse_id
  operation$schema <- settings$schema
  operation$table <- settings$table
  operation$source_path <- settings$path
  operation$mode <- settings$mode
  operation$format <- settings$format
  operation
}

# Validate and adapt an R or Arrow object to a lazy record-batch reader
.fabric_lakehouse_prepare_data <- function(data) {
  tryCatch(
    .fabric_parquet_prepare_data(
      data,
      caller = "fabric_lakehouse_write_table()"
    ),
    fabric_arrow_error = function(error) {
      .fabric_abort(
        conditionMessage(error),
        class = c(
          "fabric_lakehouse_arrow_error",
          "fabric_lakehouse_error"
        ),
        parent = error
      )
    }
  )
}

# Require names that Fabric documents as preserving exactly during table load
.fabric_lakehouse_column_names <- function(value) {
  if (!length(value)) {
    .fabric_abort("data must contain at least one column")
  }
  invalid <- is.na(value) |
    !nzchar(value) |
    nchar(value) > 128L |
    !grepl("^[\\p{L}\\p{N}_]+$", value, perl = TRUE)
  if (any(invalid)) {
    .fabric_abort(paste0(
      "Column names must contain only Unicode letters, numbers, and underscores ",
      "and be at most 128 characters; invalid: ",
      paste(value[invalid], collapse = ", ")
    ))
  }
  if (anyDuplicated(value)) {
    .fabric_abort("Column names must be unique")
  }
  invisible(value)
}

# Raise one actionable load error and retain or remove the source as requested
.fabric_lakehouse_write_abort <- function(
  error,
  storage_target,
  credential,
  staging_path,
  keep_staging,
  operation_accepted = FALSE
) {
  retained <- TRUE
  confirmed_failure <- inherits(error, "fabric_operation_failed") ||
    (!isTRUE(operation_accepted) &&
      inherits(error, "fabric_http_error") &&
      !is.null(error$status) &&
      error$status >= 400L &&
      error$status < 500L &&
      !error$status %in% c(408L, 429L))
  if (!isTRUE(keep_staging) && isTRUE(confirmed_failure)) {
    retained <- !.fabric_lakehouse_remove_staging(storage_target, credential)
  }
  .fabric_abort(
    paste0(
      "Fabric could not load the staged Parquet files. ",
      if (retained) {
        paste0("Staging was retained at '", staging_path, "'.")
      } else {
        "The staging directory was removed."
      }
    ),
    class = c("fabric_lakehouse_write_error", "fabric_lakehouse_error"),
    parent = error,
    staging_path = staging_path,
    staging_retained = retained,
    operation_accepted = operation_accepted
  )
}

# Best-effort removal used after load success or in an already-failing path
.fabric_lakehouse_remove_staging <- function(target, credential) {
  isTRUE(tryCatch(
    {
      directory <- target
      directory$path <- dirname(target$path)
      onelake_delete_target(
        directory,
        credential,
        recursive = TRUE,
        is_directory = TRUE
      )
      TRUE
    },
    error = function(error) FALSE
  ))
}

# Create a filesystem-safe, process-unique directory component without an
# additional package dependency
.fabric_lakehouse_staging_id <- function() {
  gsub("[^A-Za-z0-9_-]", "", basename(tempfile("load-")))
}

# Validate an item-relative path accepted by the Load Table API
.fabric_lakehouse_files_path <- function(value, name) {
  .fabric_lakehouse_nonempty(value, name)
  normalized <- onelake_normalize_path(value)
  if (
    !grepl(
      "^Files(/[\\p{L}\\w]([ \\p{L}\\w.-]*[\\p{L}\\w.-])?)*$",
      normalized,
      perl = TRUE
    )
  ) {
    .fabric_abort(paste0(
      "`",
      name,
      "` must be Files or begin with Files/ and match Fabric relativePath syntax"
    ))
  }
  normalized
}

# Validate documented Lakehouse destination identifiers
.fabric_lakehouse_table_name <- function(value) {
  .fabric_lakehouse_nonempty(value, "table")
  if (
    nchar(value) > 256L ||
      !grepl("^(?=.*[A-Za-z_])[A-Za-z0-9_]+$", value, perl = TRUE)
  ) {
    .fabric_abort(
      "table must contain 1 to 256 ASCII letters, numbers, or underscores and include a letter or underscore"
    )
  }
  invisible(value)
}

.fabric_lakehouse_schema_name <- function(value) {
  .fabric_lakehouse_nonempty(value, "schema")
  if (nchar(value) > 128L || !grepl("^[A-Za-z0-9_]+$", value)) {
    .fabric_abort(
      "schema must contain 1 to 128 ASCII letters, numbers, or underscores"
    )
  }
  invisible(value)
}

# Match public choices case-insensitively while returning service casing
.fabric_lakehouse_choice <- function(value, choices, name) {
  if (length(value) > 1L) {
    value <- value[[1L]]
  }
  .fabric_lakehouse_nonempty(value, name)
  index <- match(tolower(value), tolower(choices))
  if (is.na(index)) {
    .fabric_abort(paste0(
      "`",
      name,
      "` must be one of ",
      paste(choices, collapse = ", ")
    ))
  }
  choices[[index]]
}

.fabric_lakehouse_nonempty <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_abort(paste0("`", name, "` must be one non-empty string"))
  }
  invisible(value)
}

.fabric_onelake_table_page_size <- function(value) {
  if (is.null(value)) {
    return(invisible(NULL))
  }
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 1 ||
      value != floor(value) ||
      value > 100
  ) {
    .fabric_abort("page_size must be NULL or one whole number from 1 to 100")
  }
  invisible(as.integer(value))
}
