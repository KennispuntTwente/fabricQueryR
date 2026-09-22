test_that("SQL table discovery normalizes objects and detailed columns", {
  calls <- list()
  local_mocked_bindings(
    fabric_sql_query = function(...) {
      call <- list(...)
      calls[[length(calls) + 1L]] <<- call
      tibble::tibble(
        SCHEMA_NAME = c("sales", "sales", "sales"),
        OBJECT_NAME = c("customers", "orders", "orders"),
        OBJECT_TYPE = "BASE TABLE",
        future_metadata = c("kept-customers", "kept-orders", "kept-orders"),
        COLUMN_NAME = c("id", "id", "amount"),
        ORDINAL_POSITION = c(1L, 1L, 2L),
        COLUMN_DEFAULT = c(NA_character_, NA_character_, "((0))"),
        IS_NULLABLE = c("NO", "NO", "YES"),
        DATA_TYPE = c("bigint", "int", "decimal"),
        CHARACTER_MAXIMUM_LENGTH = c(NA, NA, NA),
        NUMERIC_PRECISION = c(19, 10, 18),
        NUMERIC_SCALE = c(0, 0, 2),
        DATETIME_PRECISION = c(NA, NA, NA),
        COLLATION_NAME = c(NA_character_, NA_character_, NA_character_)
      )
    }
  )
  snapshot <- structure(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      type = "WarehouseSnapshot",
      displayName = "SalesSnapshot",
      sql_connection_string = paste0(
        "Server=snapshot.datawarehouse.fabric.microsoft.com;",
        "Database=SalesSnapshot"
      )
    ),
    class = "fabric_item"
  )

  tables <- fabric_sql_tables(
    snapshot,
    schema = "sales",
    backend = "adbc",
    token = "sql-token",
    verbose = FALSE
  )

  expect_s3_class(tables, "tbl_df")
  expect_equal(tables$name, c("customers", "orders"))
  expect_equal(tables$schema, rep("sales", 2L))
  expect_equal(tables$full_name, c("sales.customers", "sales.orders"))
  expect_equal(tables$type, rep("BASE TABLE", 2L))
  expect_true(all(is.na(tables$definition)))
  expect_equal(tables$columns[[1L]][[1L]]$data_type, "bigint")
  expect_equal(tables$columns[[2L]][[2L]]$name, "amount")
  expect_true(tables$columns[[2L]][[2L]]$nullable)
  expect_equal(tables$columns[[2L]][[2L]]$numeric_scale, 2)
  expect_equal(tables$raw[[2L]]$future_metadata, "kept-orders")
  expect_named(
    tables$raw[[2L]],
    c("schema_name", "object_name", "object_type", "future_metadata")
  )
  expect_named(
    tables$columns[[2L]][[2L]]$raw,
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
  expect_length(calls, 1L)
  expect_true(calls[[1L]]$read_only)
  expect_true(calls[[1L]]$idempotent)
  expect_identical(calls[[1L]]$params, list("sales"))
  expect_identical(calls[[1L]]$backend, "adbc")
  expect_match(calls[[1L]]$sql, "TABLE_TYPE = 'BASE TABLE'", fixed = TRUE)
  expect_match(
    calls[[1L]]$sql,
    "LEFT JOIN INFORMATION_SCHEMA.COLUMNS",
    fixed = TRUE
  )
})

test_that("SQL view discovery includes definitions without detail", {
  calls <- list()
  long_definition <- paste0(
    "CREATE VIEW reporting.monthly_sales AS SELECT '",
    paste(rep("x", 5000L), collapse = ""),
    "' AS payload"
  )
  local_mocked_bindings(
    fabric_sql_query = function(...) {
      calls[[length(calls) + 1L]] <<- list(...)
      tibble::tibble(
        schema_name = "reporting",
        object_name = "monthly_sales",
        object_type = "VIEW",
        view_definition = long_definition,
        check_option = "NONE",
        is_updatable = "NO"
      )
    }
  )

  views <- fabric_sql_views(
    "warehouse.datawarehouse.fabric.microsoft.com",
    database = "Analytics",
    detail = FALSE,
    token = "sql-token",
    verbose = FALSE
  )

  expect_equal(views$name, "monthly_sales")
  expect_equal(views$schema, "reporting")
  expect_equal(views$type, "VIEW")
  expect_identical(views$definition, long_definition)
  expect_gt(nchar(views$definition, type = "chars"), 4000L)
  expect_length(views$columns[[1L]], 0L)
  expect_equal(views$raw[[1L]]$check_option, "NONE")
  expect_length(calls, 1L)
  expect_match(calls[[1L]]$sql, "INFORMATION_SCHEMA.VIEWS", fixed = TRUE)
  expect_match(calls[[1L]]$sql, "sys.views", fixed = TRUE)
  expect_match(calls[[1L]]$sql, "sys.sql_modules", fixed = TRUE)
  expect_match(calls[[1L]]$sql, "m.definition", fixed = TRUE)
  expect_false(grepl("v.VIEW_DEFINITION", calls[[1L]]$sql, fixed = TRUE))
  expect_false(grepl(
    "INFORMATION_SCHEMA.COLUMNS",
    calls[[1L]]$sql,
    fixed = TRUE
  ))
  expect_null(calls[[1L]]$params)
})

test_that("detailed SQL views retain definitions and separate schemas", {
  long_definition <- paste0(
    "CREATE VIEW sales.orders AS SELECT '",
    paste(rep("x", 5000L), collapse = ""),
    "' AS payload, 1 AS id"
  )
  local_mocked_bindings(
    fabric_sql_query = function(...) {
      tibble::tibble(
        schema_name = c("archive", "sales", "sales", "sales"),
        object_name = c("orders", "orders", "orders", "restricted"),
        object_type = "VIEW",
        view_definition = c(NA, long_definition, long_definition, NA),
        check_option = "NONE",
        is_updatable = "NO",
        column_name = c("archived_id", "payload", "id", NA),
        ordinal_position = c(1, 1, 2, NA),
        column_default = NA_character_,
        is_nullable = c("NO", "NO", "NO", NA),
        data_type = c("int", "varchar", "int", NA),
        character_maximum_length = c(NA, 5000, NA, NA),
        numeric_precision = c(10, NA, 10, NA),
        numeric_scale = c(0, NA, 0, NA),
        datetime_precision = NA_real_,
        collation_name = NA_character_
      )
    }
  )

  views <- fabric_sql_views(
    "warehouse.datawarehouse.fabric.microsoft.com",
    database = "Analytics",
    token = "sql-token",
    verbose = FALSE
  )

  expect_equal(
    views$full_name,
    c("archive.orders", "sales.orders", "sales.restricted")
  )
  expect_identical(
    views$definition,
    c(NA_character_, long_definition, NA_character_)
  )
  expect_equal(lengths(views$columns), c(1L, 2L, 0L))
  expect_equal(views$columns[[1L]][[1L]]$name, "archived_id")
  expect_equal(
    vapply(views$columns[[2L]], `[[`, character(1), "name"),
    c("payload", "id")
  )
  expect_identical(views$raw[[2L]]$view_definition, long_definition)
  expect_equal(views$raw[[3L]]$object_name, "restricted")
})

test_that("detailed SQL discovery uses and closes one connection per attempt", {
  events <- character()
  failure <- NULL
  local_mocked_bindings(
    fabric_sql_require_backend = function(...) invisible(TRUE),
    fabric_sql_connect = function(...) {
      events <<- c(events, "connect")
      structure(list(), class = "OdbcConnection")
    },
    .fabric_sql_db_get_query = function(...) {
      events <<- c(events, "query")
      if (!is.null(failure)) {
        message <- failure
        failure <<- NULL
        rlang::abort(message)
      }
      tibble::tibble(
        schema_name = "dbo",
        object_name = "orders",
        object_type = "BASE TABLE",
        column_name = "id",
        ordinal_position = 1,
        column_default = NA_character_,
        is_nullable = "NO",
        data_type = "int",
        character_maximum_length = NA_real_,
        numeric_precision = 10,
        numeric_scale = 0,
        datetime_precision = NA_real_,
        collation_name = NA_character_
      )
    },
    .fabric_sql_db_disconnect = function(...) {
      events <<- c(events, "disconnect")
      invisible(TRUE)
    },
    .fabric_sql_sleep = function(...) invisible(NULL)
  )
  discover <- function() {
    fabric_sql_tables(
      "warehouse.datawarehouse.fabric.microsoft.com",
      database = "Analytics",
      schema = "dbo",
      token = "sql-token",
      verbose = FALSE,
      retry_delay = 0,
      max_tries = 2L
    )
  }

  tables <- discover()
  expect_equal(tables$columns[[1L]][[1L]]$name, "id")
  expect_identical(events, c("connect", "query", "disconnect"))

  events <- character()
  failure <- "SQLSTATE 08S01: Communication link failure"
  expect_equal(discover(), tables)
  expect_identical(events, rep(c("connect", "query", "disconnect"), 2L))

  events <- character()
  failure <- "Permission denied"
  expect_snapshot(error = TRUE, discover())
  expect_identical(events, c("connect", "query", "disconnect"))
})

test_that("SQL discovery preserves a stable empty result", {
  calls <- 0L
  local_mocked_bindings(
    fabric_sql_query = function(...) {
      calls <<- calls + 1L
      tibble::tibble(
        schema_name = character(),
        object_name = character(),
        object_type = character()
      )
    }
  )

  tables <- fabric_sql_tables(
    "warehouse.datawarehouse.fabric.microsoft.com",
    database = "Empty",
    token = "sql-token",
    verbose = FALSE
  )

  expect_named(
    tables,
    c("name", "schema", "full_name", "type", "definition", "columns", "raw")
  )
  expect_equal(nrow(tables), 0L)
  expect_equal(calls, 1L)
})

test_that("generic SQL table reads safely quote table records", {
  queried <- NULL
  stream <- structure(list(), class = "nanoarrow_array_stream")
  local_mocked_bindings(
    fabric_sql_query = function(...) {
      queried <<- list(...)
      stream
    }
  )
  table <- tibble::tibble(name = "orders]archive", schema = "sales data")

  result <- fabric_sql_read_table(
    "warehouse.datawarehouse.fabric.microsoft.com",
    table,
    columns = c("id", "display]name"),
    limit = 25,
    result = "arrow_stream",
    backend = "adbc",
    database = "Analytics",
    token = "sql-token",
    verbose = FALSE,
    timeout = 12
  )

  expect_identical(result, stream)
  expect_identical(
    queried$sql,
    paste0(
      "SELECT TOP (25) [id], [display]]name] ",
      "FROM [sales data].[orders]]archive]"
    )
  )
  expect_identical(queried$result, "arrow_stream")
  expect_identical(queried$backend, "adbc")
  expect_identical(queried$database, "Analytics")
  expect_identical(queried$timeout, 12)
  expect_true(queried$read_only)
  expect_true(queried$idempotent)
})

test_that("SQL table helpers validate before executing queries", {
  calls <- 0L
  local_mocked_bindings(
    fabric_sql_query = function(...) {
      calls <<- calls + 1L
      tibble::tibble()
    }
  )

  expect_snapshot(error = TRUE, {
    fabric_sql_read_table("server", "", token = "token")
  })
  expect_snapshot(error = TRUE, {
    fabric_sql_read_table(
      "server",
      "orders",
      columns = c("id", "id"),
      token = "token"
    )
  })
  expect_snapshot(error = TRUE, {
    fabric_sql_read_table("server", "orders", limit = 1.5, token = "token")
  })
  expect_snapshot(error = TRUE, {
    fabric_sql_tables("server", sql = "SELECT 1", token = "token")
  })
  expect_equal(calls, 0L)
})
test_that("SQL identifiers retain case-sensitive distinctions", {
  expect_identical(.fabric_sql_projection(c("id", "ID")), c("id", "ID"))
  expect_identical(.fabric_warehouse_column_names(c("id", "ID")), c("id", "ID"))
})
