test_that("SQL table discovery normalizes objects and detailed columns", {
  calls <- list()
  local_mocked_bindings(
    fabric_sql_query = function(...) {
      call <- list(...)
      calls[[length(calls) + 1L]] <<- call
      if (grepl("INFORMATION_SCHEMA.COLUMNS", call$sql, fixed = TRUE)) {
        return(tibble::tibble(
          SCHEMA_NAME = c("sales", "sales", "sales"),
          OBJECT_NAME = c("customers", "orders", "orders"),
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
        ))
      }
      tibble::tibble(
        SCHEMA_NAME = c("sales", "sales"),
        OBJECT_NAME = c("customers", "orders"),
        OBJECT_TYPE = c("BASE TABLE", "BASE TABLE"),
        future_metadata = c("kept-customers", "kept-orders")
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
  expect_length(calls, 2L)
  expect_true(all(vapply(calls, function(call) call$read_only, logical(1))))
  expect_true(all(vapply(calls, function(call) call$idempotent, logical(1))))
  expect_true(all(vapply(
    calls,
    function(call) identical(call$params, list("sales")),
    logical(1)
  )))
  expect_true(all(vapply(
    calls,
    function(call) identical(call$backend, "adbc"),
    logical(1)
  )))
  expect_true(all(grepl(
    "TABLE_TYPE = 'BASE TABLE'",
    vapply(
      calls,
      `[[`,
      character(1),
      "sql"
    ),
    fixed = TRUE
  )))
})

test_that("SQL view discovery includes definitions without detail", {
  calls <- list()
  local_mocked_bindings(
    fabric_sql_query = function(...) {
      calls[[length(calls) + 1L]] <<- list(...)
      tibble::tibble(
        schema_name = "reporting",
        object_name = "monthly_sales",
        object_type = "VIEW",
        view_definition = "SELECT month, amount FROM dbo.sales",
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
  expect_equal(views$definition, "SELECT month, amount FROM dbo.sales")
  expect_length(views$columns[[1L]], 0L)
  expect_equal(views$raw[[1L]]$check_option, "NONE")
  expect_length(calls, 1L)
  expect_match(calls[[1L]]$sql, "INFORMATION_SCHEMA.VIEWS", fixed = TRUE)
  expect_null(calls[[1L]]$params)
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
      columns = c("id", "ID"),
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
