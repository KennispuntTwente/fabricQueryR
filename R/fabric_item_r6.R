#' Internal R6 base for discovered Fabric records
#'
#' @noRd
FabricRecord <- R6::R6Class(
  classname = "FabricRecord",
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description Internal constructor used by discovery factories.
    #' @param record One named Fabric API record.
    #' @param legacy_class Classes restored by `$as_list()`.
    #' @param credential Optional internal authentication credential.
    initialize = function(record, legacy_class, credential = NULL) {
      if (!is.list(record) || is.null(names(record))) {
        .fabric_abort("`record` must be one named Fabric record list")
      }
      if (
        !is.character(legacy_class) ||
          !length(legacy_class) ||
          anyNA(legacy_class) ||
          !all(nzchar(legacy_class))
      ) {
        .fabric_abort("`legacy_class` must contain non-empty class names")
      }
      if (!is.null(credential) && !inherits(credential, "fabric_credential")) {
        .fabric_abort("`credential` must be an internal Fabric credential")
      }

      class(record) <- NULL
      private$record <- record
      private$legacy_class <- legacy_class
      reference <- .fabric_r6_credential_reference(credential)
      private$credential_ref <- reference$reference
      private$credential_key <- reference$key

      reserved <- ls(self, all.names = TRUE)
      for (name in setdiff(names(record), reserved)) {
        local({
          key <- name
          makeActiveBinding(
            key,
            function(value) {
              if (!missing(value)) {
                .fabric_abort(
                  paste0(
                    "Fabric record field `",
                    key,
                    "` is read-only"
                  ),
                  class = "fabric_r6_read_only_error"
                )
              }
              private$record[[key]]
            },
            self
          )
        })
      }
      lockEnvironment(self, bindings = FALSE)
      invisible(self)
    },

    #' @description Print a concise record summary.
    #' @param ... Unused.
    #' @returns `self`, invisibly.
    print = function(...) {
      .fabric_print(
        .fabric_r6_record_label(self),
        list(
          name = private$value("displayName", "display_name") %||%
            "<unnamed>",
          type = private$value("type", "workspaceType") %||% "<unknown>",
          id = private$value("id") %||% "<unknown>",
          workspace = private$value(
            "workspaceDisplayName",
            "workspaceId"
          )
        )
      )
      invisible(self)
    },

    #' @description Return one raw record field.
    #' @param name One field name.
    #' @param default Value returned when the field is absent.
    #' @returns The stored field or `default`.
    get = function(name, default = NULL) {
      if (
        !is.character(name) ||
          length(name) != 1L ||
          is.na(name) ||
          !nzchar(name)
      ) {
        .fabric_abort("`name` must be one non-empty field name")
      }
      value <- private$record[[name]]
      if (is.null(value)) default else value
    },

    #' @description Return all raw field names.
    #' @returns A character vector.
    field_names = function() names(private$record),

    #' @description Convert the object to a plain discovery record.
    #' @returns A `fabric_item` or `fabric_workspace` list.
    as_list = function() {
      structure(private$record, class = private$legacy_class)
    }
  ),
  private = list(
    record = NULL,
    legacy_class = NULL,
    credential_ref = NULL,
    credential_key = NULL,

    value = function(...) {
      fabric_record_value(private$record, ...)
    },

    workspace_id = function(kind = "Fabric item") {
      workspace_id <- private$value("workspaceId", "workspace_id")
      if (is.null(workspace_id)) {
        .fabric_abort(
          paste0("This ", kind, " does not contain a workspace ID"),
          class = "fabric_r6_context_error"
        )
      }
      workspace_id
    },

    credential = function() {
      .fabric_r6_credential_value(private$credential_ref)
    },

    invoke = function(
      fun,
      args = list(),
      dots = list(),
      authenticated = TRUE,
      output = NULL
    ) {
      .fabric_r6_invoke(
        fun = fun,
        args = args,
        dots = dots,
        credential = if (isTRUE(authenticated)) {
          private$credential()
        } else {
          NULL
        },
        authenticated = authenticated,
        output = output
      )
    }
  )
)

#' R6 objects for discovered Microsoft Fabric resources
#'
#' These generators back the default `output = "r6"` discovery interface.
#' Users normally receive objects from [fabric_workspaces()], [fabric_items()],
#' [fabric_item()], the typed discovery helpers, or [fabric_catalog_search()]
#' rather than constructing them directly.
#'
#' Every object includes the complete Fabric API record. Read non-conflicting
#' fields directly with `$`; use `$get()` for collision-safe field access and
#' `$as_list()` or [as.list()] for a plain record. `$get()` is an object-only
#' field helper. Record fields are read-only.
#'
#' Methods delegate to the corresponding `fabric_*()` function. Their `...`
#' arguments are forwarded unchanged, and the credential used for discovery is
#' reused while the object is in the current R process. An explicitly
#' supplied `token`, `tenant_id`, `client_id`, or `auth_args` takes precedence.
#'
#' SQL-capable resources inherit common `sql_*()` methods. Lakehouses,
#' Warehouses, mirrored databases, Eventhouses, KQL databases, GraphQL APIs,
#' semantic models, and runnable job items add workload-specific methods.
#' Other discovered types are returned as `FabricItem` objects with `$details()`
#' ([fabric_item()]) and record access. They do not expose methods that cannot
#' operate from discovery metadata.
#'
#' @format An [R6::R6Class] generator.
#' @return The corresponding R6 generator.
#' @examples
#' \dontrun{
#' workspace <- fabric_workspaces()[[1L]]
#' workspace$displayName
#'
#' # Equivalent function: fabric_lakehouses(workspace)
#' lakehouse <- workspace$lakehouses()[[1L]]
#' lakehouse$id
#' # Equivalent function: fabric_lakehouse_tables(lakehouse)
#' lakehouse$tables()
#' # Equivalent function: fabric_lakehouse_read_table(lakehouse, ...)
#' orders <- lakehouse$read_table("orders", limit = 100L)
#'
#' # Workload-specific subclasses expose their own lifecycle methods
#' # Equivalent function: fabric_semantic_models(workspace)
#' model <- workspace$semantic_models()[[1L]]
#' # Equivalent function: fabric_pbi_refresh(model)
#' refresh <- model$refresh()
#' # Equivalent function: fabric_pbi_refresh_wait(refresh, ...)
#' model$refresh_wait(refresh, timeout = 1800)
#'
#' # Equivalent generic: as.list(lakehouse)
#' lakehouse_record <- lakehouse$as_list()
#' }
#' @name FabricItem
NULL

#' @rdname FabricItem
#' @export
FabricWorkspace <- R6::R6Class(
  classname = "FabricWorkspace",
  inherit = FabricRecord,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description Internal constructor used by discovery factories.
    #' @param record One named Fabric workspace record.
    #' @param legacy_class Classes assigned by `$as_list()`.
    #' @param credential Optional internal authentication credential.
    initialize = function(
      record,
      legacy_class = c("fabric_workspace", "list"),
      credential = NULL
    ) {
      super$initialize(record, legacy_class, credential)
    },

    #' @description Discover items in this workspace.
    #' @param ... Arguments forwarded to [fabric_items()].
    #' @returns A list of `FabricItem` objects or type-specific subclasses.
    items = function(...) {
      private$invoke(
        fabric_items,
        args = list(workspace = self),
        dots = list(...),
        output = "r6"
      )
    },

    #' @description Discover and enrich one item in this workspace.
    #' @param item Item GUID, display name, or discovered item.
    #' @param ... Arguments forwarded to [fabric_item()].
    #' @returns A `FabricItem` object or one of its subclasses.
    item = function(item, ...) {
      private$invoke(
        fabric_item,
        args = list(workspace = self, item = item),
        dots = list(...),
        output = "r6"
      )
    },

    #' @description Discover Lakehouses in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricLakehouse` objects.
    lakehouses = function(detail = TRUE, ...) {
      private$invoke(
        fabric_lakehouses,
        args = list(workspace = self, detail = detail),
        dots = list(...),
        output = "r6"
      )
    },

    #' @description Discover Warehouses in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricWarehouse` objects.
    warehouses = function(detail = TRUE, ...) {
      private$typed_items(fabric_warehouses, detail, list(...))
    },

    #' @description Discover Warehouse snapshots in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricWarehouseSnapshot` objects.
    warehouse_snapshots = function(detail = TRUE, ...) {
      private$typed_items(fabric_warehouse_snapshots, detail, list(...))
    },

    #' @description Discover mirrored databases in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricMirroredDatabase` objects.
    mirrored_databases = function(detail = TRUE, ...) {
      private$typed_items(fabric_mirrored_databases, detail, list(...))
    },

    #' @description Discover SQL databases in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricSqlDatabase` objects.
    sql_databases = function(detail = TRUE, ...) {
      private$typed_items(fabric_sql_databases, detail, list(...))
    },

    #' @description Discover semantic models in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricSemanticModel` objects.
    semantic_models = function(detail = FALSE, ...) {
      private$typed_items(fabric_semantic_models, detail, list(...))
    },

    #' @description Discover Eventhouses in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricEventhouse` objects.
    eventhouses = function(detail = TRUE, ...) {
      private$typed_items(fabric_eventhouses, detail, list(...))
    },

    #' @description Discover KQL databases in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricKqlDatabase` objects.
    kql_databases = function(detail = TRUE, ...) {
      private$typed_items(fabric_kql_databases, detail, list(...))
    },

    #' @description Discover notebooks in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricJobItem` objects.
    notebooks = function(detail = TRUE, ...) {
      private$typed_items(fabric_notebooks, detail, list(...))
    },

    #' @description Discover data pipelines in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricJobItem` objects.
    data_pipelines = function(detail = TRUE, ...) {
      private$typed_items(fabric_data_pipelines, detail, list(...))
    },

    #' @description Discover Spark job definitions in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricJobItem` objects.
    spark_job_definitions = function(detail = TRUE, ...) {
      private$typed_items(fabric_spark_job_definitions, detail, list(...))
    },

    #' @description Discover environments in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricItem` objects.
    environments = function(detail = TRUE, ...) {
      private$typed_items(fabric_environments, detail, list(...))
    },

    #' @description Discover User Data Functions in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricItem` objects.
    user_data_functions = function(detail = FALSE, ...) {
      private$typed_items(fabric_user_data_functions, detail, list(...))
    },

    #' @description Discover GraphQL APIs in this workspace.
    #' @param detail Whether to retrieve workload details.
    #' @param ... Additional discovery arguments.
    #' @returns A list of `FabricGraphQLApi` objects.
    graphql_apis = function(detail = FALSE, ...) {
      private$typed_items(fabric_graphql_apis, detail, list(...))
    },

    #' @description Reset this workspace's OneLake shortcut cache.
    #' @param ... Arguments forwarded to [fabric_onelake_shortcut_cache_reset()].
    #' @returns A `fabric_operation` handle.
    shortcut_cache_reset = function(...) {
      private$invoke(
        fabric_onelake_shortcut_cache_reset,
        args = list(workspace = self),
        dots = list(...)
      )
    }
  ),
  private = list(
    typed_items = function(fun, detail, dots) {
      private$invoke(
        fun,
        args = list(workspace = self, detail = detail),
        dots = dots,
        output = "r6"
      )
    }
  )
)

#' @rdname FabricItem
#' @export
FabricItem <- R6::R6Class(
  classname = "FabricItem",
  inherit = FabricRecord,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description Internal constructor used by discovery factories.
    #' @param record One named Fabric item record.
    #' @param legacy_class Classes assigned by `$as_list()`.
    #' @param credential Optional internal authentication credential.
    initialize = function(
      record,
      legacy_class = c("fabric_item", "list"),
      credential = NULL
    ) {
      super$initialize(record, legacy_class, credential)
    },

    #' @description Retrieve a fresh item record and supported workload details.
    #'   User Data Function workload details require `detail = TRUE` and a
    #'   delegated user identity.
    #' @param ... Arguments forwarded to [fabric_item()].
    #' @returns A new `FabricItem` object or one of its subclasses.
    details = function(...) {
      workspace_id <- private$workspace_id()
      private$invoke(
        fabric_item,
        args = list(workspace = workspace_id, item = self),
        dots = list(...),
        output = "r6"
      )
    }
  )
)

#' SQL-capable Fabric item base
#'
#' @noRd
FabricSqlItem <- R6::R6Class(
  classname = "FabricSqlItem",
  inherit = FabricItem,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description Return normalized SQL connection information.
    #' @param ... Arguments forwarded to [fabric_sql_connection_info()].
    #' @returns A `fabric_sql_connection_info` record.
    sql_connection_info = function(...) {
      private$invoke(
        fabric_sql_connection_info,
        args = list(server = self),
        dots = list(...),
        authenticated = FALSE
      )
    },

    #' @description Open a DBI connection to the SQL analytics endpoint.
    #' @param ... Arguments forwarded to [fabric_sql_connect()].
    #' @returns A DBI connection.
    sql_connect = function(...) {
      private$invoke(
        fabric_sql_connect,
        args = list(server = self),
        dots = list(...)
      )
    },

    #' @description Run a SQL query against the SQL analytics endpoint.
    #' @param sql One SQL statement.
    #' @param ... Arguments forwarded to [fabric_sql_query()].
    #' @returns A tibble or Arrow stream.
    sql_query = function(sql, ...) {
      private$invoke(
        fabric_sql_query,
        args = list(server = self, sql = sql),
        dots = list(...)
      )
    },

    #' @description List SQL tables.
    #' @param ... Arguments forwarded to [fabric_sql_tables()].
    #' @returns A table inventory tibble.
    sql_tables = function(...) {
      private$invoke(
        fabric_sql_tables,
        args = list(server = self),
        dots = list(...)
      )
    },

    #' @description List SQL views.
    #' @param ... Arguments forwarded to [fabric_sql_views()].
    #' @returns A view inventory tibble.
    sql_views = function(...) {
      private$invoke(
        fabric_sql_views,
        args = list(server = self),
        dots = list(...)
      )
    },

    #' @description Read one table through the SQL analytics endpoint.
    #' @param table Table name or discovered table row.
    #' @param ... Arguments forwarded to [fabric_sql_read_table()].
    #' @returns A tibble or Arrow stream.
    sql_read_table = function(table, ...) {
      private$invoke(
        fabric_sql_read_table,
        args = list(server = self, table = table),
        dots = list(...)
      )
    }
  )
)

#' @rdname FabricItem
#' @export
FabricLakehouse <- R6::R6Class(
  classname = "FabricLakehouse",
  inherit = FabricSqlItem,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description List Lakehouse schemas.
    #' @param ... Arguments forwarded to [fabric_lakehouse_schemas()].
    #' @returns A schema inventory tibble.
    schemas = function(...) {
      private$invoke(
        fabric_lakehouse_schemas,
        args = list(lakehouse = self),
        dots = list(...)
      )
    },

    #' @description Retrieve one Lakehouse table's metadata.
    #' @param table Table name or discovered table row.
    #' @param ... Arguments forwarded to [fabric_lakehouse_table()].
    #' @returns A one-row table metadata tibble.
    table = function(table, ...) {
      private$invoke(
        fabric_lakehouse_table,
        args = list(lakehouse = self, table = table),
        dots = list(...)
      )
    },

    #' @description List Lakehouse tables.
    #' @param ... Arguments forwarded to [fabric_lakehouse_tables()].
    #' @returns A table inventory tibble.
    tables = function(...) {
      private$invoke(
        fabric_lakehouse_tables,
        args = list(lakehouse = self),
        dots = list(...)
      )
    },

    #' @description Read one managed Delta table.
    #' @param table Table name or discovered table row.
    #' @param ... Arguments forwarded to [fabric_lakehouse_read_table()].
    #' @returns A tibble or Arrow stream.
    read_table = function(table, ...) {
      private$invoke(
        fabric_lakehouse_read_table,
        args = list(lakehouse = self, table = table),
        dots = list(...)
      )
    },

    #' @description Read a Delta table directly from OneLake.
    #' @param table_path Table path below the item.
    #' @param ... Arguments forwarded to [fabric_onelake_read_delta_table()].
    #' @returns A tibble or Arrow stream.
    read_delta_table = function(table_path, ...) {
      workspace_id <- private$workspace_id("Lakehouse")
      private$invoke(
        fabric_onelake_read_delta_table,
        args = list(
          table_path = table_path,
          workspace_name = workspace_id,
          lakehouse_name = self
        ),
        dots = list(...)
      )
    },

    #' @description Load an existing file into a managed table.
    #' @param table Destination table name.
    #' @param path Source path under the Lakehouse.
    #' @param ... Arguments forwarded to [fabric_lakehouse_load_table()].
    #' @returns A `fabric_operation` or completed operation result.
    load_table = function(table, path, ...) {
      private$invoke(
        fabric_lakehouse_load_table,
        args = list(lakehouse = self, table = table, path = path),
        dots = list(...)
      )
    },

    #' @description Write R or Arrow data to a managed table.
    #' @param table Destination table name.
    #' @param data Data frame or Arrow-compatible source.
    #' @param ... Arguments forwarded to [fabric_lakehouse_write_table()].
    #' @returns A completed write result.
    write_table = function(table, data, ...) {
      private$invoke(
        fabric_lakehouse_write_table,
        args = list(lakehouse = self, table = table, data = data),
        dots = list(...)
      )
    },

    #' @description Run one Spark statement through Livy.
    #' @param code Spark, PySpark, SparkR, or Spark SQL code.
    #' @param ... Arguments forwarded to [fabric_livy_query()].
    #' @returns A Livy statement result.
    livy_query = function(code, ...) {
      private$invoke(
        fabric_livy_query,
        args = list(livy_url = self, code = code),
        dots = list(...)
      )
    },

    #' @description Create a reusable Livy session.
    #' @param ... Arguments forwarded to [fabric_livy_session()].
    #' @returns A [FabricLivySession].
    livy_session = function(...) {
      private$invoke(
        fabric_livy_session,
        args = list(livy_url = self),
        dots = list(...)
      )
    },

    #' @description Submit a standalone Livy batch.
    #' @param file ABFSS application-file URI.
    #' @param ... Arguments forwarded to [fabric_livy_batch_submit()].
    #' @returns A [FabricLivyBatch] or its result.
    livy_batch_submit = function(file, ...) {
      private$invoke(
        fabric_livy_batch_submit,
        args = list(livy_url = self, file = file),
        dots = list(...)
      )
    },

    #' @description List OneLake files and directories.
    #' @param path Path within the Lakehouse.
    #' @param ... Arguments forwarded to [fabric_onelake_list()].
    #' @returns A file inventory tibble.
    onelake_list = function(path = "", ...) {
      private$onelake_call(fabric_onelake_list, path, list(...))
    },

    #' @description Retrieve OneLake path metadata.
    #' @param path Path within the Lakehouse.
    #' @param ... Arguments forwarded to [fabric_onelake_metadata()].
    #' @returns A one-row metadata tibble.
    onelake_metadata = function(path = "", ...) {
      private$onelake_call(fabric_onelake_metadata, path, list(...))
    },

    #' @description Read a supported file from OneLake.
    #' @param path Path within the Lakehouse.
    #' @param ... Arguments forwarded to [fabric_onelake_read_file()].
    #' @returns A tibble or Arrow stream.
    onelake_read_file = function(path, ...) {
      private$onelake_call(fabric_onelake_read_file, path, list(...))
    },

    #' @description Write data to a supported OneLake file.
    #' @param path Destination path within the Lakehouse.
    #' @param data Data to write.
    #' @param ... Arguments forwarded to [fabric_onelake_write_file()].
    #' @returns A file-write result.
    onelake_write_file = function(path, data, ...) {
      private$onelake_call(
        fabric_onelake_write_file,
        path,
        c(list(data = data), list(...))
      )
    },

    #' @description Download one OneLake file.
    #' @param path Source path within the Lakehouse.
    #' @param ... Arguments forwarded to [fabric_onelake_download()].
    #' @returns The local destination path.
    onelake_download = function(path, ...) {
      private$onelake_call(fabric_onelake_download, path, list(...))
    },

    #' @description Upload a local file or raw vector to OneLake.
    #' @param path Destination path within the Lakehouse.
    #' @param source Local path or raw vector.
    #' @param ... Arguments forwarded to [fabric_onelake_upload()].
    #' @returns A file-write result.
    onelake_upload = function(path, source, ...) {
      private$onelake_call(
        fabric_onelake_upload,
        path,
        c(list(source = source), list(...))
      )
    },

    #' @description Delete a OneLake path.
    #' @param path Path within the Lakehouse.
    #' @param ... Arguments forwarded to [fabric_onelake_delete()].
    #' @returns `TRUE`, invisibly, after deletion.
    onelake_delete = function(path, ...) {
      private$onelake_call(fabric_onelake_delete, path, list(...))
    },

    #' @description Check whether a schema exists.
    #' @param schema Schema name.
    #' @param ... Arguments forwarded to [fabric_onelake_schema_exists()].
    #' @returns One logical value.
    schema_exists = function(schema, ...) {
      private$invoke(
        fabric_onelake_schema_exists,
        args = list(item = self, schema = schema),
        dots = list(...)
      )
    },

    #' @description Check whether a table exists.
    #' @param table Table name or discovered table row.
    #' @param ... Arguments forwarded to [fabric_onelake_table_exists()].
    #' @returns One logical value.
    table_exists = function(table, ...) {
      private$invoke(
        fabric_onelake_table_exists,
        args = list(item = self, table = table),
        dots = list(...)
      )
    },

    #' @description List OneLake shortcuts.
    #' @param ... Arguments forwarded to [fabric_onelake_shortcuts()].
    #' @returns A shortcut inventory tibble.
    shortcuts = function(...) {
      private$invoke(
        fabric_onelake_shortcuts,
        args = list(item = self),
        dots = list(...)
      )
    },

    #' @description Retrieve one OneLake shortcut.
    #' @param path Shortcut parent path.
    #' @param name Shortcut name.
    #' @param ... Arguments forwarded to [fabric_onelake_shortcut_get()].
    #' @returns A one-row shortcut tibble.
    shortcut = function(path, name, ...) {
      private$invoke(
        fabric_onelake_shortcut_get,
        args = list(item = self, path = path, name = name),
        dots = list(...)
      )
    },

    #' @description Create a OneLake shortcut.
    #' @param path Shortcut parent path.
    #' @param name Shortcut name.
    #' @param target Shortcut target.
    #' @param ... Arguments forwarded to [fabric_onelake_shortcut_create()].
    #' @returns A one-row shortcut tibble.
    shortcut_create = function(path, name, target, ...) {
      private$invoke(
        fabric_onelake_shortcut_create,
        args = list(item = self, path = path, name = name, target = target),
        dots = list(...)
      )
    },

    #' @description Create multiple OneLake shortcuts.
    #' @param shortcuts Shortcut request lists.
    #' @param ... Arguments forwarded to
    #'   [fabric_onelake_shortcuts_bulk_create()].
    #' @returns A `fabric_operation` handle.
    shortcuts_bulk_create = function(shortcuts, ...) {
      private$invoke(
        fabric_onelake_shortcuts_bulk_create,
        args = list(item = self, shortcuts = shortcuts),
        dots = list(...)
      )
    },

    #' @description Delete one OneLake shortcut.
    #' @param path Shortcut parent path.
    #' @param name Shortcut name.
    #' @param ... Arguments forwarded to [fabric_onelake_shortcut_delete()].
    #' @returns `TRUE`, invisibly, after deletion.
    shortcut_delete = function(path, name, ...) {
      private$invoke(
        fabric_onelake_shortcut_delete,
        args = list(item = self, path = path, name = name),
        dots = list(...)
      )
    }
  ),
  private = list(
    onelake_call = function(fun, path, dots) {
      workspace_id <- private$workspace_id("Lakehouse")
      private$invoke(
        fun,
        args = list(workspace = workspace_id, item = self, path = path),
        dots = dots
      )
    }
  )
)

#' @rdname FabricItem
#' @export
FabricWarehouse <- R6::R6Class(
  classname = "FabricWarehouse",
  inherit = FabricSqlItem,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description List Warehouse schemas.
    #' @param ... Arguments forwarded to [fabric_warehouse_schemas()].
    #' @returns A schema inventory tibble.
    schemas = function(...) {
      private$invoke(
        fabric_warehouse_schemas,
        args = list(warehouse = self),
        dots = list(...)
      )
    },

    #' @description Retrieve one Warehouse table's metadata.
    #' @param table Table name or discovered table row.
    #' @param ... Arguments forwarded to [fabric_warehouse_table()].
    #' @returns A one-row table metadata tibble.
    table = function(table, ...) {
      private$invoke(
        fabric_warehouse_table,
        args = list(warehouse = self, table = table),
        dots = list(...)
      )
    },

    #' @description List Warehouse tables.
    #' @param ... Arguments forwarded to [fabric_warehouse_tables()].
    #' @returns A table inventory tibble.
    tables = function(...) {
      private$invoke(
        fabric_warehouse_tables,
        args = list(warehouse = self),
        dots = list(...)
      )
    },

    #' @description Read one Warehouse table.
    #' @param table Table name or discovered table row.
    #' @param ... Arguments forwarded to [fabric_warehouse_read_table()].
    #' @returns A tibble or Arrow stream.
    read_table = function(table, ...) {
      private$invoke(
        fabric_warehouse_read_table,
        args = list(warehouse = self, table = table),
        dots = list(...)
      )
    },

    #' @description Write R or Arrow data to a Warehouse table.
    #' @param table Destination table name.
    #' @param data Data frame or Arrow-compatible source.
    #' @param staging_lakehouse Lakehouse used for staged Parquet data.
    #' @param ... Arguments forwarded to [fabric_warehouse_write_table()].
    #' @returns A completed write result.
    write_table = function(table, data, staging_lakehouse, ...) {
      private$invoke(
        fabric_warehouse_write_table,
        args = list(
          warehouse = self,
          table = table,
          data = data,
          staging_lakehouse = staging_lakehouse
        ),
        dots = list(...)
      )
    },

    #' @description Read a Delta table directly from OneLake.
    #' @param table_path Table path below the item.
    #' @param ... Arguments forwarded to [fabric_onelake_read_delta_table()].
    #' @returns A tibble or Arrow stream.
    read_delta_table = function(table_path, ...) {
      workspace_id <- private$workspace_id("Warehouse")
      private$invoke(
        fabric_onelake_read_delta_table,
        args = list(
          table_path = table_path,
          workspace_name = workspace_id,
          lakehouse_name = self
        ),
        dots = list(...)
      )
    }
  )
)

#' @rdname FabricItem
#' @export
FabricWarehouseSnapshot <- R6::R6Class(
  classname = "FabricWarehouseSnapshot",
  inherit = FabricSqlItem,
  cloneable = FALSE,
  lock_objects = FALSE
)

#' @rdname FabricItem
#' @export
FabricSqlDatabase <- R6::R6Class(
  classname = "FabricSqlDatabase",
  inherit = FabricSqlItem,
  cloneable = FALSE,
  lock_objects = FALSE
)

#' @rdname FabricItem
#' @export
FabricMirroredDatabase <- R6::R6Class(
  classname = "FabricMirroredDatabase",
  inherit = FabricSqlItem,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description List mirrored database schemas.
    #' @param ... Arguments forwarded to
    #'   [fabric_mirrored_database_schemas()].
    #' @returns A schema inventory tibble.
    schemas = function(...) {
      private$invoke(
        fabric_mirrored_database_schemas,
        args = list(mirrored_database = self),
        dots = list(...)
      )
    },

    #' @description Retrieve one mirrored table's metadata.
    #' @param table Table name or discovered table row.
    #' @param ... Arguments forwarded to [fabric_mirrored_database_table()].
    #' @returns A one-row table metadata tibble.
    table = function(table, ...) {
      private$invoke(
        fabric_mirrored_database_table,
        args = list(mirrored_database = self, table = table),
        dots = list(...)
      )
    },

    #' @description List mirrored database tables.
    #' @param ... Arguments forwarded to [fabric_mirrored_database_tables()].
    #' @returns A table inventory tibble.
    tables = function(...) {
      private$invoke(
        fabric_mirrored_database_tables,
        args = list(mirrored_database = self),
        dots = list(...)
      )
    },

    #' @description Read one mirrored Delta table.
    #' @param table Table name or discovered table row.
    #' @param ... Arguments forwarded to
    #'   [fabric_mirrored_database_read_table()].
    #' @returns A tibble or Arrow stream.
    read_table = function(table, ...) {
      private$invoke(
        fabric_mirrored_database_read_table,
        args = list(mirrored_database = self, table = table),
        dots = list(...)
      )
    },

    #' @description Read a Delta table directly from OneLake.
    #' @param table_path Table path below the item.
    #' @param ... Arguments forwarded to [fabric_onelake_read_delta_table()].
    #' @returns A tibble or Arrow stream.
    read_delta_table = function(table_path, ...) {
      workspace_id <- private$workspace_id("mirrored database")
      private$invoke(
        fabric_onelake_read_delta_table,
        args = list(
          table_path = table_path,
          workspace_name = workspace_id,
          lakehouse_name = self
        ),
        dots = list(...)
      )
    }
  )
)

#' KQL-capable Fabric item base
#'
#' @noRd
FabricKqlItem <- R6::R6Class(
  classname = "FabricKqlItem",
  inherit = FabricItem,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description Run a KQL query.
    #' @param query One KQL query.
    #' @param ... Arguments forwarded to [fabric_kql_query()].
    #' @returns A `fabric_kql_result`.
    query = function(query, ...) {
      private$invoke(
        fabric_kql_query,
        args = list(cluster = self, query = query),
        dots = list(...)
      )
    },

    #' @description List KQL tables.
    #' @param ... Arguments forwarded to [fabric_kql_tables()].
    #' @returns A table inventory tibble.
    tables = function(...) {
      private$invoke(
        fabric_kql_tables,
        args = list(cluster = self),
        dots = list(...)
      )
    },

    #' @description Read one KQL table.
    #' @param table Table name or discovered table row.
    #' @param ... Arguments forwarded to [fabric_kql_read_table()].
    #' @returns A `fabric_kql_result`.
    read_table = function(table, ...) {
      private$invoke(
        fabric_kql_read_table,
        args = list(cluster = self, table = table),
        dots = list(...)
      )
    },

    #' @description Ingest existing sources into a KQL table.
    #' @param table Destination table name.
    #' @param sources Source URLs or source records.
    #' @param format Source data format.
    #' @param ... Arguments forwarded to [fabric_kql_ingest()].
    #' @returns A `fabric_kql_ingestion_status`.
    ingest = function(table, sources, format, ...) {
      private$invoke(
        fabric_kql_ingest,
        args = list(
          cluster = self,
          table = table,
          sources = sources,
          format = format
        ),
        dots = list(...)
      )
    },

    #' @description Write R or Arrow data to a KQL table.
    #' @param table Destination table name.
    #' @param data Data frame or Arrow-compatible source.
    #' @param ... Arguments forwarded to [fabric_kql_write_table()].
    #' @returns A `fabric_kql_ingestion_status`.
    write_table = function(table, data, ...) {
      private$invoke(
        fabric_kql_write_table,
        args = list(cluster = self, table = table, data = data),
        dots = list(...)
      )
    },

    #' @description Export a KQL query to Fabric storage.
    #' @param query One KQL query.
    #' @param destination Destination Fabric item or OneLake target.
    #' @param ... Arguments forwarded to [fabric_kql_export()].
    #' @returns A `fabric_kql_export` handle or completed result.
    export = function(query, destination, ...) {
      private$invoke(
        fabric_kql_export,
        args = list(
          cluster = self,
          query = query,
          destination = destination
        ),
        dots = list(...)
      )
    },

    #' @description Retrieve one KQL ingestion status snapshot.
    #' @param ingestion Ingestion handle, status record, or operation ID.
    #' @param ... Arguments forwarded to [fabric_kql_ingestion_status()].
    #' @returns A `fabric_kql_ingestion_status` record.
    ingestion_status = function(ingestion, ...) {
      private$ingestion_call(ingestion, FALSE, list(...))
    },

    #' @description Wait for a KQL ingestion to finish.
    #' @param ingestion Ingestion handle, status record, or operation ID.
    #' @param ... Arguments forwarded to [fabric_kql_ingestion_status()].
    #' @returns A terminal `fabric_kql_ingestion_status` record.
    ingestion_wait = function(ingestion, ...) {
      private$ingestion_call(ingestion, TRUE, list(...))
    }
  ),
  private = list(
    ingestion_call = function(ingestion, wait, dots) {
      args <- list(ingestion = ingestion, wait = wait)
      if (
        !inherits(ingestion, "fabric_kql_ingestion") &&
          !inherits(ingestion, "fabric_kql_ingestion_status")
      ) {
        args$cluster <- self
      }
      private$invoke(
        fabric_kql_ingestion_status,
        args = args,
        dots = dots
      )
    }
  )
)

#' @rdname FabricItem
#' @export
FabricEventhouse <- R6::R6Class(
  classname = "FabricEventhouse",
  inherit = FabricKqlItem,
  cloneable = FALSE,
  lock_objects = FALSE
)

#' @rdname FabricItem
#' @export
FabricKqlDatabase <- R6::R6Class(
  classname = "FabricKqlDatabase",
  inherit = FabricKqlItem,
  cloneable = FALSE,
  lock_objects = FALSE
)

#' @rdname FabricItem
#' @export
FabricGraphQLApi <- R6::R6Class(
  classname = "FabricGraphQLApi",
  inherit = FabricItem,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description Run a GraphQL query.
    #' @param query One GraphQL query.
    #' @param ... Arguments forwarded to [fabric_graphql_query()].
    #' @returns A `fabric_graphql_result`.
    query = function(query, ...) {
      private$invoke(
        fabric_graphql_query,
        args = list(api = self, query = query),
        dots = list(...)
      )
    },

    #' @description Retrieve the GraphQL schema.
    #' @param ... Arguments forwarded to [fabric_graphql_schema()].
    #' @returns A `fabric_graphql_schema`.
    schema = function(...) {
      private$invoke(
        fabric_graphql_schema,
        args = list(api = self),
        dots = list(...)
      )
    },

    #' @description Retrieve all pages of a GraphQL cursor query.
    #' @param query One GraphQL query.
    #' @param next_cursor Function extracting the next cursor.
    #' @param ... Arguments forwarded to [fabric_graphql_paginate()].
    #' @returns A `fabric_graphql_pages` list.
    paginate = function(query, next_cursor, ...) {
      private$invoke(
        fabric_graphql_paginate,
        args = list(api = self, query = query, next_cursor = next_cursor),
        dots = list(...)
      )
    }
  )
)

#' @rdname FabricItem
#' @export
FabricSemanticModel <- R6::R6Class(
  classname = "FabricSemanticModel",
  inherit = FabricItem,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description Run a DAX query against this semantic model.
    #' @param dax One DAX query.
    #' @param ... Arguments forwarded to [fabric_pbi_dax_query()].
    #' @returns A tibble or Arrow stream.
    dax_query = function(dax, ...) {
      private$invoke(
        fabric_pbi_dax_query,
        args = list(connstr = self, dax = dax),
        dots = list(...)
      )
    },

    #' @description Start a refresh of this semantic model.
    #' @param ... Arguments forwarded to [fabric_pbi_refresh()].
    #' @returns A `fabric_pbi_refresh` handle.
    refresh = function(...) {
      private$invoke(
        fabric_pbi_refresh,
        args = list(connstr = self),
        dots = list(...)
      )
    },

    #' @description Retrieve this semantic model's refresh history.
    #' @param ... Arguments forwarded to [fabric_pbi_refresh_history()].
    #' @returns A `fabric_pbi_refresh_history` list.
    refresh_history = function(...) {
      private$invoke(
        fabric_pbi_refresh_history,
        args = list(connstr = self),
        dots = list(...)
      )
    },

    #' @description Retrieve one refresh status snapshot.
    #' @param refresh Refresh handle, detail record, or request ID.
    #' @param ... Arguments forwarded to [fabric_pbi_refresh_status()].
    #' @returns A `fabric_pbi_refresh_detail` record.
    refresh_status = function(refresh, ...) {
      args <- list(refresh = refresh)
      if (
        !inherits(refresh, "fabric_pbi_refresh") &&
          !inherits(refresh, "fabric_pbi_refresh_detail")
      ) {
        args$connstr <- self
      }
      private$invoke(
        fabric_pbi_refresh_status,
        args = args,
        dots = list(...)
      )
    },

    #' @description Wait for a submitted refresh to finish.
    #' @param refresh Refresh handle or detail record.
    #' @param ... Arguments forwarded to [fabric_pbi_refresh_wait()].
    #' @returns A terminal `fabric_pbi_refresh_detail` record.
    refresh_wait = function(refresh, ...) {
      private$invoke(
        fabric_pbi_refresh_wait,
        args = list(refresh = refresh),
        dots = list(...)
      )
    },

    #' @description Cancel a submitted refresh.
    #' @param refresh Refresh handle, detail record, or request ID.
    #' @param ... Arguments forwarded to [fabric_pbi_refresh_cancel()].
    #' @returns `TRUE`, invisibly, when cancellation is accepted.
    refresh_cancel = function(refresh, ...) {
      args <- list(refresh = refresh)
      if (
        !inherits(refresh, "fabric_pbi_refresh") &&
          !inherits(refresh, "fabric_pbi_refresh_detail")
      ) {
        args$connstr <- self
      }
      private$invoke(
        fabric_pbi_refresh_cancel,
        args = args,
        dots = list(...)
      )
    }
  )
)

#' @rdname FabricItem
#' @export
FabricJobItem <- R6::R6Class(
  classname = "FabricJobItem",
  inherit = FabricItem,
  cloneable = FALSE,
  lock_objects = FALSE,
  public = list(
    #' @description Start an on-demand item job.
    #' @param ... Arguments forwarded to [fabric_job_run()].
    #' @returns A `fabric_job` handle.
    run = function(...) {
      private$invoke(
        fabric_job_run,
        args = list(item = self),
        dots = list(...)
      )
    },

    #' @description Retrieve one job status snapshot.
    #' @param job Job handle, instance record, or job instance ID.
    #' @param ... Arguments forwarded to [fabric_job_status()].
    #' @returns A `fabric_job_instance` record.
    status = function(job = NULL, ...) {
      private$invoke(
        fabric_job_status,
        args = list(job = job, item = self),
        dots = list(...)
      )
    },

    #' @description Wait for a submitted job to finish.
    #' @param job Job handle or instance record.
    #' @param ... Arguments forwarded to [fabric_job_wait()].
    #' @returns A terminal `fabric_job_instance` record.
    wait = function(job, ...) {
      private$invoke(
        fabric_job_wait,
        args = list(job = job),
        dots = list(...)
      )
    },

    #' @description Cancel a submitted job.
    #' @param job Job handle, instance record, or job instance ID.
    #' @param ... Arguments forwarded to [fabric_job_cancel()].
    #' @returns `TRUE`, invisibly, when cancellation is accepted.
    cancel = function(job = NULL, ...) {
      private$invoke(
        fabric_job_cancel,
        args = list(job = job, item = self),
        dots = list(...)
      )
    },

    #' @description Retrieve this item's job history.
    #' @param ... Arguments forwarded to [fabric_job_instances()].
    #' @returns A `fabric_job_instance_list`.
    instances = function(...) {
      private$invoke(
        fabric_job_instances,
        args = list(item = self),
        dots = list(...)
      )
    },

    #' @description List this item's schedules.
    #' @param ... Arguments forwarded to [fabric_job_schedules()].
    #' @returns A `fabric_job_schedule_list`.
    schedules = function(...) {
      private$invoke(
        fabric_job_schedules,
        args = list(item = self),
        dots = list(...)
      )
    },

    #' @description Create an item schedule.
    #' @param configuration A [fabric_job_schedule_config()] record.
    #' @param ... Arguments forwarded to [fabric_job_schedule_create()].
    #' @returns A `fabric_job_schedule` record.
    schedule_create = function(configuration, ...) {
      private$invoke(
        fabric_job_schedule_create,
        args = list(item = self, configuration = configuration),
        dots = list(...)
      )
    },

    #' @description Update an item schedule.
    #' @param schedule_id Schedule GUID or schedule record.
    #' @param configuration Optional replacement schedule configuration.
    #' @param ... Arguments forwarded to [fabric_job_schedule_update()].
    #' @returns A `fabric_job_schedule` record.
    schedule_update = function(schedule_id, configuration = NULL, ...) {
      private$invoke(
        fabric_job_schedule_update,
        args = list(
          item = self,
          schedule_id = schedule_id,
          configuration = configuration
        ),
        dots = list(...)
      )
    },

    #' @description Delete an item schedule.
    #' @param schedule_id Schedule GUID or schedule record.
    #' @param ... Arguments forwarded to [fabric_job_schedule_delete()].
    #' @returns `TRUE`, invisibly, after deletion.
    schedule_delete = function(schedule_id, ...) {
      private$invoke(
        fabric_job_schedule_delete,
        args = list(item = self, schedule_id = schedule_id),
        dots = list(...)
      )
    }
  )
)

#' @export
as.list.FabricRecord <- function(x, ...) {
  x$as_list()
}

#' @export
names.FabricRecord <- function(x) {
  x$field_names()
}

#' @export
length.FabricRecord <- function(x) {
  length(x$field_names())
}

# Convert one legacy record into the appropriate R6 class. This remains
# internal so discovery functions control credentials and legacy classes.
fabric_r6_record <- function(record, legacy_class, credential = NULL) {
  type <- fabric_record_value(record, "type")
  type <- if (is.character(type) && length(type) == 1L && !is.na(type)) {
    tolower(type)
  } else {
    ""
  }
  generator <- if ("fabric_workspace" %in% legacy_class) {
    FabricWorkspace
  } else {
    switch(
      type,
      lakehouse = FabricLakehouse,
      warehouse = FabricWarehouse,
      warehousesnapshot = FabricWarehouseSnapshot,
      mirroreddatabase = FabricMirroredDatabase,
      sqldatabase = FabricSqlDatabase,
      semanticmodel = FabricSemanticModel,
      eventhouse = FabricEventhouse,
      kqldatabase = FabricKqlDatabase,
      graphqlapi = FabricGraphQLApi,
      notebook = FabricJobItem,
      datapipeline = FabricJobItem,
      sparkjobdefinition = FabricJobItem,
      FabricItem
    )
  }
  generator$new(
    record = record,
    legacy_class = legacy_class,
    credential = credential
  )
}

.fabric_r6_output <- function(output) {
  match.arg(output, c("r6", "list"))
}

.fabric_r6_record_label <- function(record) {
  label <- class(record)[[1L]]
  gsub("([a-z])([A-Z])", "\\1 \\2", label)
}

.fabric_r6_credential_reference <- function(credential) {
  if (is.null(credential)) {
    return(list(reference = NULL, key = NULL))
  }
  key <- new.env(parent = emptyenv())
  list(
    reference = rlang::new_weakref(key, credential),
    key = key
  )
}

.fabric_r6_credential_value <- function(reference) {
  if (rlang::is_weakref(reference)) {
    rlang::wref_value(reference)
  } else {
    NULL
  }
}

.fabric_r6_invoke <- function(
  fun,
  args,
  dots,
  credential,
  authenticated,
  output
) {
  dot_names <- names(dots)
  if (
    length(dots) &&
      (is.null(dot_names) || anyNA(dot_names) || !all(nzchar(dot_names)))
  ) {
    .fabric_abort("All arguments supplied through `...` must be named")
  }
  duplicate <- intersect(names(args), dot_names)
  if (length(duplicate)) {
    .fabric_abort(
      paste0(
        "Method context already supplies `",
        duplicate[[1L]],
        "`"
      ),
      class = "fabric_r6_argument_error"
    )
  }
  if (!is.null(output)) {
    if ("output" %in% dot_names) {
      .fabric_abort(
        "R6 discovery methods always return R6 objects",
        class = "fabric_r6_argument_error"
      )
    }
    dots$output <- output
  }
  auth_names <- c("token", "tenant_id", "client_id", "auth_args")
  if (
    isTRUE(authenticated) &&
      !is.null(credential) &&
      !any(auth_names %in% names(dots))
  ) {
    dots$token <- credential
  }
  do.call(fun, c(args, dots))
}
