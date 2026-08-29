#' Discover and read tables through a Fabric SQL endpoint
#'
#' These helpers provide a target-independent metadata and table-read layer for
#' Fabric SQL endpoints. They accept Lakehouse, Warehouse, Warehouse snapshot,
#' and SQL Database objects, or the same direct server inputs as
#' [fabric_sql_query()]. Discovery uses SQL catalog metadata views and is
#' limited by the caller's SQL metadata permissions.
#'
#' @param server Fabric SQL endpoint, portal connection string, or discovered
#'   SQL-capable item object.
#' @param schema Optional schema filter. `fabric_sql_read_table()` defaults to
#'   the schema in a discovered table row, otherwise `"dbo"`.
#' @param detail Whether table or view discovery should retrieve column
#'   metadata.
#' @param table Table or view name, or a one-row record containing `name` and
#'   optionally `schema`.
#' @param columns Optional unique column names to project.
#' @param limit Optional non-negative maximum number of rows to return.
#' @param result Result representation for `fabric_sql_read_table()`; either a
#'   tibble or a single-use Arrow stream.
#' @param database Optional catalog/database. An explicit value overrides one
#'   discovered from `server`.
#' @param target_type Kind of Fabric SQL target. Keep `"auto"` unless a custom
#'   hostname prevents automatic identification.
#' @param backend SQL driver backend, either `"odbc"` or `"adbc"`.
#' @param token Optional SQL access token or audience-aware token-provider
#'   function.
#' @param ... Additional authentication, driver, endpoint, timeout, verbosity,
#'   and retry options passed to [fabric_sql_query()]. Query text, parameters,
#'   read-only status, and idempotency are controlled by these helpers and
#'   cannot be supplied here.
#'
#' @return `fabric_sql_tables()` and `fabric_sql_views()` return a tibble with
#'   object `name`, `schema`, `full_name`, `type`, optional view `definition`,
#'   list-column `columns`, and the unmodified discovery row in `raw`.
#'   `fabric_sql_read_table()` returns a tibble or `nanoarrow_array_stream`.
#'
#' @references
#' [System information schema views](https://learn.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/system-information-schema-views-transact-sql)
#'
#' [SQL module definitions](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-sql-modules-transact-sql)
#'
#' [Fabric SQL analytics endpoints](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)
#' @name fabric_sql_tables
NULL

#' @rdname fabric_sql_tables
#' @export
#'
#' @examples
#' \dontrun{
#' workspace <- fabric_workspaces()[[1L]]
#' warehouse <- fabric_warehouses(workspace)[[1L]]
#'
#' tables <- fabric_sql_tables(warehouse, schema = "dbo")
#' rows <- fabric_sql_read_table(warehouse, tables[1L, ], limit = 1000)
#' views <- fabric_sql_views(warehouse)
#' }
fabric_sql_tables <- function(
  server,
  schema = NULL,
  detail = TRUE,
  database = NULL,
  target_type = c(
    "auto",
    "lakehouse",
    "warehouse",
    "sql_database",
    "sql_analytics_endpoint"
  ),
  backend = c("odbc", "adbc"),
  token = NULL,
  ...
) {
  .fabric_sql_object_inventory(
    object_type = "BASE TABLE",
    server = server,
    schema = schema,
    detail = detail,
    database = database,
    target_type = match.arg(target_type),
    backend = match.arg(backend),
    token = token,
    dots = list(...)
  )
}

#' @rdname fabric_sql_tables
#' @export
fabric_sql_views <- function(
  server,
  schema = NULL,
  detail = TRUE,
  database = NULL,
  target_type = c(
    "auto",
    "lakehouse",
    "warehouse",
    "sql_database",
    "sql_analytics_endpoint"
  ),
  backend = c("odbc", "adbc"),
  token = NULL,
  ...
) {
  .fabric_sql_object_inventory(
    object_type = "VIEW",
    server = server,
    schema = schema,
    detail = detail,
    database = database,
    target_type = match.arg(target_type),
    backend = match.arg(backend),
    token = token,
    dots = list(...)
  )
}

#' @rdname fabric_sql_tables
#' @export
fabric_sql_read_table <- function(
  server,
  table,
  schema = NULL,
  columns = NULL,
  limit = NULL,
  result = c("tibble", "arrow_stream"),
  database = NULL,
  target_type = c(
    "auto",
    "lakehouse",
    "warehouse",
    "sql_database",
    "sql_analytics_endpoint"
  ),
  backend = c("odbc", "adbc"),
  token = NULL,
  ...
) {
  table_target <- .fabric_sql_table_target(table, schema)
  columns <- .fabric_sql_projection(columns)
  .fabric_sql_limit(limit)
  selection <- if (is.null(columns)) {
    "*"
  } else {
    paste(
      vapply(columns, .fabric_sql_quote_identifier, character(1)),
      collapse = ", "
    )
  }
  top <- if (is.null(limit)) {
    ""
  } else {
    paste0(" TOP (", format(limit, scientific = FALSE, trim = TRUE), ")")
  }
  sql <- paste0(
    "SELECT",
    top,
    " ",
    selection,
    " FROM ",
    .fabric_sql_quote_identifier(table_target$schema),
    ".",
    .fabric_sql_quote_identifier(table_target$table)
  )
  .fabric_sql_helper_query(
    server = server,
    sql = sql,
    params = NULL,
    result = match.arg(result),
    database = database,
    target_type = match.arg(target_type),
    backend = match.arg(backend),
    token = token,
    dots = list(...)
  )
}

.fabric_sql_object_inventory <- function(
  object_type,
  server,
  schema,
  detail,
  database,
  target_type,
  backend,
  token,
  dots
) {
  if (!is.null(schema)) {
    .fabric_sql_name(schema, "schema")
  }
  .fabric_operation_logical(detail, "detail")
  params <- if (is.null(schema)) NULL else list(schema)
  objects <- .fabric_sql_helper_query(
    server = server,
    sql = .fabric_sql_objects_sql(object_type, schema),
    params = params,
    result = "tibble",
    database = database,
    target_type = target_type,
    backend = backend,
    token = token,
    dots = dots
  )
  objects <- .fabric_sql_metadata_columns(
    objects,
    c("schema_name", "object_name", "object_type")
  )
  if (!nrow(objects)) {
    return(.fabric_sql_object_tibble())
  }

  column_rows <- NULL
  if (isTRUE(detail)) {
    column_rows <- .fabric_sql_helper_query(
      server = server,
      sql = .fabric_sql_columns_sql(object_type, schema),
      params = params,
      result = "tibble",
      database = database,
      target_type = target_type,
      backend = backend,
      token = token,
      dots = dots
    )
    column_rows <- .fabric_sql_metadata_columns(
      column_rows,
      c(
        "schema_name",
        "object_name",
        "column_name",
        "ordinal_position",
        "column_default",
        "is_nullable",
        "data_type",
        "character_maximum_length",
        "numeric_precision",
        "numeric_scale",
        "datetime_precision",
        "collation_name"
      )
    )
  }

  rows <- lapply(seq_len(nrow(objects)), function(index) {
    object <- objects[index, , drop = FALSE]
    object_columns <- list()
    if (!is.null(column_rows) && nrow(column_rows)) {
      matches <- which(
        column_rows$schema_name == object$schema_name[[1L]] &
          column_rows$object_name == object$object_name[[1L]]
      )
      object_columns <- lapply(matches, function(column_index) {
        .fabric_sql_column_record(
          column_rows[column_index, , drop = FALSE]
        )
      })
    }
    definition <- if ("view_definition" %in% names(object)) {
      .fabric_sql_optional_character(object$view_definition[[1L]])
    } else {
      NA_character_
    }
    list(
      name = as.character(object$object_name[[1L]]),
      schema = as.character(object$schema_name[[1L]]),
      full_name = paste(
        object$schema_name[[1L]],
        object$object_name[[1L]],
        sep = "."
      ),
      type = as.character(object$object_type[[1L]]),
      definition = definition,
      columns = object_columns,
      raw = as.list(object)
    )
  })
  .fabric_sql_object_tibble(rows)
}

.fabric_sql_objects_sql <- function(object_type, schema) {
  definition <- if (identical(object_type, "VIEW")) {
    paste0(
      ", m.definition AS view_definition, ",
      "v.CHECK_OPTION AS check_option, ",
      "v.IS_UPDATABLE AS is_updatable"
    )
  } else {
    ""
  }
  from <- if (identical(object_type, "VIEW")) {
    paste0(
      " FROM INFORMATION_SCHEMA.TABLES AS t ",
      "LEFT JOIN INFORMATION_SCHEMA.VIEWS AS v ",
      "ON v.TABLE_CATALOG = t.TABLE_CATALOG ",
      "AND v.TABLE_SCHEMA = t.TABLE_SCHEMA ",
      "AND v.TABLE_NAME = t.TABLE_NAME ",
      "LEFT JOIN sys.schemas AS s ",
      "ON s.name = t.TABLE_SCHEMA ",
      "LEFT JOIN sys.views AS sv ",
      "ON sv.schema_id = s.schema_id ",
      "AND sv.name = t.TABLE_NAME ",
      "LEFT JOIN sys.sql_modules AS m ",
      "ON m.object_id = sv.object_id"
    )
  } else {
    " FROM INFORMATION_SCHEMA.TABLES AS t"
  }
  paste0(
    "SELECT t.TABLE_SCHEMA AS schema_name, ",
    "t.TABLE_NAME AS object_name, ",
    "t.TABLE_TYPE AS object_type",
    definition,
    from,
    " WHERE t.TABLE_TYPE = '",
    object_type,
    "'",
    if (is.null(schema)) "" else " AND t.TABLE_SCHEMA = ?",
    " ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME"
  )
}

.fabric_sql_columns_sql <- function(object_type, schema) {
  paste0(
    "SELECT c.TABLE_SCHEMA AS schema_name, ",
    "c.TABLE_NAME AS object_name, ",
    "c.COLUMN_NAME AS column_name, ",
    "c.ORDINAL_POSITION AS ordinal_position, ",
    "c.COLUMN_DEFAULT AS column_default, ",
    "c.IS_NULLABLE AS is_nullable, ",
    "c.DATA_TYPE AS data_type, ",
    "c.CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, ",
    "c.NUMERIC_PRECISION AS numeric_precision, ",
    "c.NUMERIC_SCALE AS numeric_scale, ",
    "c.DATETIME_PRECISION AS datetime_precision, ",
    "c.COLLATION_NAME AS collation_name ",
    "FROM INFORMATION_SCHEMA.COLUMNS AS c ",
    "INNER JOIN INFORMATION_SCHEMA.TABLES AS t ",
    "ON t.TABLE_CATALOG = c.TABLE_CATALOG ",
    "AND t.TABLE_SCHEMA = c.TABLE_SCHEMA ",
    "AND t.TABLE_NAME = c.TABLE_NAME ",
    "WHERE t.TABLE_TYPE = '",
    object_type,
    "'",
    if (is.null(schema)) "" else " AND t.TABLE_SCHEMA = ?",
    " ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION"
  )
}

.fabric_sql_metadata_columns <- function(value, required) {
  indices <- match(tolower(required), tolower(names(value)))
  if (anyNA(indices)) {
    .fabric_abort(
      paste0(
        "Fabric SQL metadata result is missing: ",
        paste(required[is.na(indices)], collapse = ", ")
      ),
      class = "fabric_sql_metadata_error"
    )
  }
  names(value)[indices] <- required
  value
}

.fabric_sql_column_record <- function(row) {
  list(
    name = as.character(row$column_name[[1L]]),
    ordinal_position = as.integer(row$ordinal_position[[1L]]),
    default = .fabric_sql_optional_character(row$column_default[[1L]]),
    nullable = identical(toupper(as.character(row$is_nullable[[1L]])), "YES"),
    data_type = as.character(row$data_type[[1L]]),
    character_maximum_length = .fabric_sql_optional_number(
      row$character_maximum_length[[1L]]
    ),
    numeric_precision = .fabric_sql_optional_number(
      row$numeric_precision[[1L]]
    ),
    numeric_scale = .fabric_sql_optional_number(row$numeric_scale[[1L]]),
    datetime_precision = .fabric_sql_optional_number(
      row$datetime_precision[[1L]]
    ),
    collation = .fabric_sql_optional_character(row$collation_name[[1L]]),
    raw = as.list(row)
  )
}

.fabric_sql_object_tibble <- function(rows = list()) {
  if (!length(rows)) {
    return(tibble::tibble(
      name = character(),
      schema = character(),
      full_name = character(),
      type = character(),
      definition = character(),
      columns = list(),
      raw = list()
    ))
  }
  tibble::tibble(
    name = vapply(rows, `[[`, character(1), "name"),
    schema = vapply(rows, `[[`, character(1), "schema"),
    full_name = vapply(rows, `[[`, character(1), "full_name"),
    type = vapply(rows, `[[`, character(1), "type"),
    definition = vapply(rows, `[[`, character(1), "definition"),
    columns = lapply(rows, `[[`, "columns"),
    raw = lapply(rows, `[[`, "raw")
  )
}

.fabric_sql_helper_query <- function(
  server,
  sql,
  params,
  result,
  database,
  target_type,
  backend,
  token,
  dots
) {
  reserved <- intersect(
    names(dots),
    c("server", "sql", "params", "result", "read_only", "idempotent")
  )
  if (length(reserved)) {
    .fabric_abort(
      paste0(
        "These arguments are controlled by the SQL table helper: ",
        paste(reserved, collapse = ", ")
      ),
      class = "fabric_sql_metadata_error"
    )
  }
  do.call(
    fabric_sql_query,
    c(
      list(
        server = server,
        sql = sql,
        params = params,
        result = result,
        database = database,
        target_type = target_type,
        backend = backend,
        token = token,
        read_only = TRUE,
        idempotent = TRUE
      ),
      dots
    )
  )
}

.fabric_sql_table_target <- function(table, schema) {
  record <- fabric_as_record(table)
  if (!is.null(record)) {
    table <- fabric_record_value(record, "name", "table", "displayName")
    schema <- schema %||% fabric_record_value(record, "schema", "schema_name")
  }
  .fabric_sql_name(table, "table")
  schema <- schema %||% "dbo"
  .fabric_sql_name(schema, "schema")
  list(table = table, schema = schema)
}

.fabric_sql_projection <- function(columns) {
  if (is.null(columns)) {
    return(NULL)
  }
  if (!is.character(columns) || !length(columns) || anyNA(columns)) {
    .fabric_abort(
      "columns must be NULL or a non-empty character vector",
      class = "fabric_sql_table_error"
    )
  }
  for (column in columns) {
    .fabric_sql_name(column, "column")
  }
  if (anyDuplicated(tolower(columns))) {
    .fabric_abort(
      "columns must be unique ignoring case",
      class = "fabric_sql_table_error"
    )
  }
  columns
}

.fabric_sql_limit <- function(limit) {
  if (is.null(limit)) {
    return(invisible(TRUE))
  }
  if (
    !is.numeric(limit) ||
      length(limit) != 1L ||
      is.na(limit) ||
      !is.finite(limit) ||
      limit < 0 ||
      limit != floor(limit)
  ) {
    .fabric_abort(
      "limit must be NULL or one non-negative whole number",
      class = "fabric_sql_table_error"
    )
  }
  invisible(TRUE)
}

.fabric_sql_name <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value) ||
      grepl("[\r\n]", value)
  ) {
    .fabric_abort(
      paste0(name, " must be one non-empty SQL identifier"),
      class = "fabric_sql_table_error"
    )
  }
  invisible(TRUE)
}

.fabric_sql_quote_identifier <- function(value) {
  paste0("[", gsub("]", "]]", value, fixed = TRUE), "]")
}

.fabric_sql_optional_character <- function(value) {
  if (length(value) != 1L || is.na(value)) {
    NA_character_
  } else {
    as.character(value)
  }
}

.fabric_sql_optional_number <- function(value) {
  number <- suppressWarnings(as.numeric(value))
  if (length(number) != 1L || is.na(number)) NA_real_ else number
}
