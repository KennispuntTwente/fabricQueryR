test_that("Warehouse schema discovery retains an empty stable shape", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    lakehouse_table_test_response(
      list(schemas = list(), next_page_token = NULL),
      url = req$url
    )
  })

  schemas <- fabric_warehouse_schemas(
    warehouse_write_test_warehouse(),
    token = "storage-token"
  )

  expect_s3_class(schemas, "tbl_df")
  expect_named(
    schemas,
    c(
      "name",
      "catalog",
      "full_name",
      "comment",
      "owner",
      "schema_id",
      "created_at",
      "updated_at",
      "raw"
    )
  )
  expect_equal(nrow(schemas), 0L)
  expect_length(calls, 1L)
  expect_match(calls, "/schemas", fixed = TRUE)
})

test_that("Warehouse singular discovery requests one detailed table", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    lakehouse_table_test_response(
      list(
        name = "orders",
        schema_name = "sales",
        table_type = "MANAGED",
        data_source_format = "DELTA",
        table_id = "table-orders",
        columns = list(list(name = "id", type_name = "long")),
        future_detail = "kept"
      ),
      url = req$url
    )
  })

  table <- fabric_warehouse_table(
    warehouse_write_test_warehouse(),
    "orders",
    schema = "sales",
    token = "storage-token"
  )

  expect_equal(table$name, "orders")
  expect_equal(table$schema, "sales")
  expect_equal(table$full_name, "sales.orders")
  expect_equal(table$type, "MANAGED")
  expect_equal(table$format, "DELTA")
  expect_equal(table$columns[[1L]][[1L]]$type_name, "long")
  expect_equal(table$raw[[1L]]$future_detail, "kept")
  expect_length(table$fabric_raw[[1L]], 0L)
  expect_length(calls, 1L)
  expect_match(utils::URLdecode(calls), "sales.orders", fixed = TRUE)
  expect_match(calls, "schema_name=sales", fixed = TRUE)
})

test_that("Warehouse table discovery follows schema and table pages", {
  calls <- character()
  audiences <- character()
  provider <- function(audience, force_refresh = FALSE) {
    audiences <<- c(audiences, audience)
    "test-token"
  }
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    url <- utils::URLdecode(req$url)
    body <- if (grepl("/schemas[?]", url)) {
      if (grepl("page_token=schema-2", url, fixed = TRUE)) {
        list(
          schemas = list(list(name = "sales", comment = "Sales schema")),
          next_page_token = NULL
        )
      } else {
        list(
          schemas = list(list(name = "dbo", future_schema = "kept")),
          next_page_token = "schema-2"
        )
      }
    } else if (grepl("/tables[?]", url)) {
      schema <- if (grepl("schema_name=sales", url, fixed = TRUE)) {
        "sales"
      } else {
        "dbo"
      }
      name <- if (identical(schema, "sales")) "éxport_数据" else "orders"
      list(
        tables = list(list(
          name = name,
          schema_name = schema,
          table_type = "MANAGED",
          data_source_format = "DELTA",
          storage_location = paste0(
            "https://onelake.dfs.fabric.microsoft.com/workspace/warehouse/",
            "Tables/",
            schema,
            "/",
            name
          ),
          future_table = list(value = "kept")
        )),
        next_page_token = NULL
      )
    } else {
      name <- if (grepl("éxport_数据", url, fixed = TRUE)) {
        "éxport_数据"
      } else {
        "orders"
      }
      schema <- if (identical(name, "éxport_数据")) "sales" else "dbo"
      list(
        name = name,
        schema_name = schema,
        table_id = paste0("id-", name),
        created_at = 1786622400123,
        updated_at = 1786622460456,
        columns = list(list(
          name = "id",
          type_name = "long",
          nullable = FALSE,
          position = 0L
        )),
        future_detail = "kept"
      )
    }
    lakehouse_table_test_response(body, url = req$url)
  })

  tables <- fabric_warehouse_tables(
    warehouse_write_test_warehouse(),
    page_size = 1L,
    token = provider
  )

  expect_s3_class(tables, "tbl_df")
  expect_equal(tables$name, c("orders", "éxport_数据"))
  expect_equal(tables$schema, c("dbo", "sales"))
  expect_equal(tables$full_name, c("dbo.orders", "sales.éxport_数据"))
  expect_equal(tables$type, rep("MANAGED", 2L))
  expect_equal(tables$format, rep("DELTA", 2L))
  expect_s3_class(tables$created_at, "POSIXct")
  expect_equal(tables$columns[[1L]][[1L]]$type_name, "long")
  expect_equal(tables$schema_metadata[[1L]]$future_schema, "kept")
  expect_equal(tables$raw[[1L]]$future_table$value, "kept")
  expect_equal(tables$raw[[1L]]$future_detail, "kept")
  expect_length(tables$fabric_raw[[1L]], 0L)
  expect_length(calls, 6L)
  expect_equal(sum(grepl("max_results=1", calls, fixed = TRUE)), 4L)
  expect_equal(
    sum(grepl(warehouse_write_test_workspace_id, calls, fixed = TRUE)),
    6L
  )
  expect_equal(
    sum(grepl(warehouse_write_test_warehouse_id, calls, fixed = TRUE)),
    6L
  )
  expect_equal(audiences, rep(.fabric_audience$storage, 6L))
})

test_that("Warehouse discovery can target one schema without detail", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    lakehouse_table_test_response(
      list(
        tables = list(list(
          name = "orders",
          schema_name = "sales",
          table_type = NULL,
          data_source_format = "DELTA",
          storage_location = paste0(
            "https://onelake.dfs.fabric.microsoft.com/workspace/warehouse/",
            "Tables/sales/orders"
          )
        )),
        next_page_token = NULL
      ),
      url = req$url
    )
  })

  tables <- fabric_warehouse_tables(
    warehouse_write_test_warehouse(),
    schema = "sales",
    detail = FALSE,
    token = "storage-token"
  )

  expect_equal(tables$name, "orders")
  expect_equal(tables$schema, "sales")
  expect_equal(tables$format, "DELTA")
  expect_length(tables$columns[[1L]], 0L)
  expect_length(calls, 1L)
  expect_match(calls, "schema_name=sales", fixed = TRUE)
})

test_that("Warehouse table reader resolves and safely quotes its query", {
  resolved <- NULL
  queried <- NULL
  stream <- structure(list(), class = "nanoarrow_array_stream")
  local_mocked_bindings(
    fabric_sql_require_backend = function(...) invisible(TRUE),
    .fabric_warehouse_resolve_item = function(...) {
      resolved <<- list(...)
      list(record = warehouse_write_test_warehouse())
    },
    fabric_sql_query = function(...) {
      queried <<- list(...)
      if (identical(queried$result, "arrow_stream")) {
        stream
      } else {
        tibble::tibble(id = 1L)
      }
    }
  )
  table <- list(
    name = "orders]archive",
    schema = "sales"
  )

  result <- fabric_warehouse_read_table(
    warehouse_write_test_warehouse(),
    table,
    columns = c("id", "display]name"),
    limit = 25,
    result = "arrow_stream",
    backend = "adbc",
    token = "sql-token",
    verbose = FALSE,
    timeout = 12,
    max_tries = 2,
    retry_delay = 0
  )

  expect_identical(result, stream)
  expect_identical(resolved$expected_type, "Warehouse")
  expect_true(resolved$require_sql)
  expect_identical(resolved$argument, "warehouse")
  expect_identical(
    queried$sql,
    paste0(
      "SELECT TOP (25) [id], [display]]name] ",
      "FROM [sales].[orders]]archive]"
    )
  )
  expect_identical(queried$result, "arrow_stream")
  expect_identical(queried$target_type, "warehouse")
  expect_identical(queried$backend, "adbc")
  expect_s3_class(queried$token, "fabric_credential")
  expect_true(queried$read_only)
  expect_true(queried$idempotent)
  expect_false(queried$verbose)
  expect_identical(queried$timeout, 12)
  expect_identical(queried$max_tries, 2)

  plain <- fabric_warehouse_read_table(
    warehouse_write_test_warehouse(),
    "orders",
    token = "sql-token",
    verbose = FALSE
  )
  expect_identical(plain$id, 1L)
  expect_identical(queried$sql, "SELECT * FROM [dbo].[orders]")
  expect_identical(queried$result, "tibble")
})

test_that("Warehouse schema and table names reject unsupported separators", {
  expect_error(
    .fabric_warehouse_identifier("sales/archive", "table"),
    "without / or \\",
    fixed = TRUE
  )
  expect_error(
    .fabric_warehouse_identifier("sales\\archive", "schema"),
    "without / or \\",
    fixed = TRUE
  )
  expect_no_error(.fabric_warehouse_identifier("order/archive", "column"))
})

test_that("Warehouse table reader validates before target resolution", {
  calls <- 0L
  local_mocked_bindings(
    fabric_sql_require_backend = function(...) invisible(TRUE),
    .fabric_warehouse_resolve_item = function(...) {
      calls <<- calls + 1L
      list(record = warehouse_write_test_warehouse())
    },
    fabric_sql_query = function(...) {
      calls <<- calls + 1L
      tibble::tibble()
    }
  )
  read <- function(...) {
    fabric_warehouse_read_table(
      warehouse_write_test_warehouse(),
      "orders",
      token = "token",
      verbose = FALSE,
      ...
    )
  }

  expect_error(read(schema = ""), "schema")
  expect_error(read(columns = character()), "columns must be NULL")
  expect_error(read(columns = c("id", "ID")), "unique ignoring case")
  for (limit in list(-1, 1.5, Inf, NA_real_, c(1, 2))) {
    expect_error(read(limit = limit), "limit must be NULL")
  }
  expect_error(read(result = "data.frame"))
  expect_error(read(backend = "spark"))
  expect_equal(calls, 0L)
})

test_that("Warehouse writer stages Parquet and issues a mapped COPY", {
  skip_if_not_installed("arrow")
  uploads <- list()
  statements <- character()
  connect_args <- NULL
  disconnects <- 0L
  cleanup_calls <- 0L
  connection <- structure(list(), class = "warehouse-test-connection")
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "fixed-load",
    onelake_upload_target = function(target, credential, source, ...) {
      uploads[[length(uploads) + 1L]] <<- list(
        target = target,
        data = as.data.frame(arrow::read_parquet(source)),
        credential = credential
      )
      tibble::tibble(path = target$path)
    },
    .fabric_warehouse_connect = function(...) {
      connect_args <<- list(...)
      connection
    },
    .fabric_warehouse_query = function(...) {
      data.frame(column_name = c("id", "display name"))
    },
    .fabric_warehouse_execute = function(connection, sql) {
      statements <<- c(statements, sql)
      3L
    },
    .fabric_warehouse_disconnect = function(...) {
      disconnects <<- disconnects + 1L
      TRUE
    },
    .fabric_warehouse_remove_staging = function(target, credential) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )
  value <- data.frame(
    id = 1:3,
    `display name` = c("alpha", "beta", "gamma"),
    check.names = FALSE
  )

  result <- fabric_warehouse_write_table(
    warehouse_write_test_warehouse(),
    table = "sales]orders",
    data = value,
    staging_lakehouse = warehouse_write_test_lakehouse(),
    token = "test-token",
    verbose = FALSE
  )

  expect_s3_class(result, "fabric_warehouse_write_result")
  expect_equal(result$rows, 3)
  expect_equal(result$file_count, 1L)
  expect_equal(result$rows_affected, 3)
  expect_false(result$staging_retained)
  expect_equal(result$staging_lakehouse_id, warehouse_write_test_lakehouse_id)
  expect_equal(cleanup_calls, 1L)
  expect_equal(disconnects, 1L)
  expect_equal(length(statements), 1L)
  expect_match(
    statements,
    "COPY INTO [dbo].[sales]]orders] ([id], [display name])",
    fixed = TRUE
  )
  expect_false(grepl("[id] 1", statements, fixed = TRUE))
  expect_match(
    statements,
    paste0(
      "https://onelake.dfs.fabric.microsoft.com/",
      warehouse_write_test_workspace_id,
      "/",
      warehouse_write_test_lakehouse_id,
      "/Files/fabricqueryr-staging/fixed-load/*.parquet"
    ),
    fixed = TRUE
  )
  expect_equal(uploads[[1L]]$data, value)
  expect_equal(
    uploads[[1L]]$target$dfs_base,
    paste0(
      "https://",
      warehouse_write_test_workspace_id,
      ".z12.dfs.fabric.microsoft.com"
    )
  )
  expect_s3_class(uploads[[1L]]$credential, "fabric_credential")
  expect_s3_class(connect_args$token, "fabric_credential")
  expect_false(connect_args$read_only)
})

test_that("Warehouse writer uploads bounded Parquet parts", {
  skip_if_not_installed("arrow")
  uploads <- list()
  statement <- NULL
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "bounded-load",
    onelake_upload_target = function(target, source, ...) {
      uploads[[length(uploads) + 1L]] <<- list(
        path = target$path,
        data = as.data.frame(arrow::read_parquet(source))
      )
      tibble::tibble(path = target$path)
    },
    .fabric_warehouse_connect = function(...) list(),
    .fabric_warehouse_query = function(...) {
      data.frame(column_name = "id")
    },
    .fabric_warehouse_execute = function(connection, sql) {
      statement <<- sql
      5L
    },
    .fabric_warehouse_disconnect = function(...) TRUE,
    .fabric_warehouse_remove_staging = function(...) TRUE
  )

  result <- fabric_warehouse_write_table(
    warehouse_write_test_warehouse(),
    "orders",
    data.frame(id = 1:5),
    staging_lakehouse = warehouse_write_test_lakehouse(),
    max_rows_per_file = 2,
    token = "test-token",
    verbose = FALSE
  )

  expect_equal(result$file_count, 3L)
  expect_equal(length(uploads), 3L)
  expect_equal(
    vapply(uploads, function(value) nrow(value$data), integer(1)),
    c(2L, 2L, 1L)
  )
  expect_equal(unlist(lapply(uploads, function(value) value$data$id)), 1:5)
  expect_equal(result$files, vapply(uploads, `[[`, character(1), "path"))
  expect_match(statement, "bounded-load/*.parquet", fixed = TRUE)
})

test_that("Warehouse overwrite is one explicit transaction", {
  skip_if_not_installed("arrow")
  events <- character()
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "overwrite-load",
    onelake_upload_target = function(...) tibble::tibble(),
    .fabric_warehouse_connect = function(...) list(),
    .fabric_warehouse_query = function(...) {
      data.frame(column_name = "id")
    },
    .fabric_warehouse_begin = function(...) {
      events <<- c(events, "begin")
      TRUE
    },
    .fabric_warehouse_execute = function(connection, sql) {
      events <<- c(events, sql)
      2L
    },
    .fabric_warehouse_commit = function(...) {
      events <<- c(events, "commit")
      TRUE
    },
    .fabric_warehouse_rollback = function(...) {
      events <<- c(events, "rollback")
      TRUE
    },
    .fabric_warehouse_disconnect = function(...) TRUE,
    .fabric_warehouse_remove_staging = function(...) TRUE
  )

  result <- fabric_warehouse_write_table(
    warehouse_write_test_warehouse(),
    "orders",
    data.frame(id = 1:2),
    staging_lakehouse = warehouse_write_test_lakehouse(),
    mode = "overwrite",
    token = "test-token",
    verbose = FALSE
  )

  expect_equal(events[[1L]], "begin")
  expect_equal(events[[2L]], "TRUNCATE TABLE [dbo].[orders]")
  expect_match(events[[3L]], "^COPY INTO", perl = TRUE)
  expect_equal(events[[4L]], "commit")
  expect_false("rollback" %in% events)
  expect_equal(result$mode, "Overwrite")
  expect_equal(result$overwrite_method, "Truncate")
  expect_false(result$table_created)
  expect_false(result$table_recreated)
})

test_that("Warehouse writer creates a missing table with transactional CTAS", {
  skip_if_not_installed("arrow")
  events <- character()
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "create-load",
    onelake_upload_target = function(...) tibble::tibble(),
    .fabric_warehouse_connect = function(...) list(),
    .fabric_warehouse_table_exists = function(...) FALSE,
    .fabric_warehouse_begin = function(...) {
      events <<- c(events, "begin")
      TRUE
    },
    .fabric_warehouse_execute = function(connection, sql) {
      events <<- c(events, sql)
      2L
    },
    .fabric_warehouse_commit = function(...) {
      events <<- c(events, "commit")
      TRUE
    },
    .fabric_warehouse_disconnect = function(...) TRUE,
    .fabric_warehouse_remove_staging = function(...) TRUE
  )

  result <- fabric_warehouse_write_table(
    warehouse_write_test_warehouse(),
    "new_orders",
    data.frame(id = 1:2, label = c("a", "b")),
    staging_lakehouse = warehouse_write_test_lakehouse(),
    create_if_missing = TRUE,
    token = "test-token",
    verbose = FALSE
  )

  expect_equal(events[[1L]], "begin")
  expect_match(
    events[[2L]],
    paste0(
      "CREATE TABLE [dbo].[new_orders] AS SELECT [id], [label] ",
      "FROM OPENROWSET(BULK"
    ),
    fixed = TRUE
  )
  expect_match(events[[2L]], "create-load/*.parquet", fixed = TRUE)
  expect_equal(events[[3L]], "commit")
  expect_true(result$table_created)
  expect_false(result$table_recreated)
  expect_equal(result$rows_affected, 2)
})

test_that("drop overwrite recreates the table with CTAS", {
  skip_if_not_installed("arrow")
  events <- character()
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "drop-load",
    onelake_upload_target = function(...) tibble::tibble(),
    .fabric_warehouse_connect = function(...) list(),
    .fabric_warehouse_begin = function(...) {
      events <<- c(events, "begin")
      TRUE
    },
    .fabric_warehouse_execute = function(connection, sql) {
      events <<- c(events, sql)
      2L
    },
    .fabric_warehouse_commit = function(...) {
      events <<- c(events, "commit")
      TRUE
    },
    .fabric_warehouse_disconnect = function(...) TRUE,
    .fabric_warehouse_remove_staging = function(...) TRUE
  )

  result <- fabric_warehouse_write_table(
    warehouse_write_test_warehouse(),
    "orders",
    data.frame(id = 1:2),
    staging_lakehouse = warehouse_write_test_lakehouse(),
    mode = "Overwrite",
    overwrite_method = "Drop",
    token = "test-token",
    verbose = FALSE
  )

  expect_equal(events[[1L]], "begin")
  expect_equal(events[[2L]], "DROP TABLE [dbo].[orders]")
  expect_match(
    events[[3L]],
    "^CREATE TABLE \\[dbo\\]\\.\\[orders\\] AS",
    perl = TRUE
  )
  expect_equal(events[[4L]], "commit")
  expect_equal(result$overwrite_method, "Drop")
  expect_false(result$table_created)
  expect_true(result$table_recreated)
})

test_that("Warehouse table discovery uses escaped catalog literals", {
  sql <- NULL
  local_mocked_bindings(
    .fabric_warehouse_query = function(connection, statement) {
      sql <<- statement
      data.frame(table_exists = TRUE)
    }
  )

  expect_true(.fabric_warehouse_table_exists(list(), "sales'ops", "orders"))
  expect_match(sql, "[s].[name] = N'sales''ops'", fixed = TRUE)
  expect_match(sql, "[t].[name] = N'orders'", fixed = TRUE)
})

test_that("Warehouse writer requires exact destination column names", {
  sql <- NULL
  local_mocked_bindings(
    .fabric_warehouse_query = function(connection, statement) {
      sql <<- statement
      data.frame(column_name = c("Id", "label", "created_at"))
    }
  )

  expect_no_error(
    .fabric_warehouse_validate_destination_columns(
      list(),
      "sales'ops",
      "orders",
      c("Id", "label")
    )
  )
  error <- tryCatch(
    .fabric_warehouse_validate_destination_columns(
      list(),
      "sales'ops",
      "orders",
      c("id", "amount")
    ),
    error = identity
  )

  expect_s3_class(error, "fabric_warehouse_column_error")
  expect_match(conditionMessage(error), "Missing: id, amount", fixed = TRUE)
  expect_match(
    conditionMessage(error),
    "catalog matches include: Id",
    fixed = TRUE
  )
  expect_match(sql, "[s].[name] = N'sales''ops'", fixed = TRUE)
  expect_match(sql, "ORDER BY [c].[column_id]", fixed = TRUE)
})

test_that("Warehouse writer stops before COPY when destination names differ", {
  skip_if_not_installed("arrow")
  sql_calls <- 0L
  cleanup_calls <- 0L
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "column-mismatch",
    onelake_upload_target = function(...) tibble::tibble(),
    .fabric_warehouse_connect = function(...) list(),
    .fabric_warehouse_query = function(...) {
      data.frame(column_name = c("Id", "label"))
    },
    .fabric_warehouse_execute = function(...) {
      sql_calls <<- sql_calls + 1L
      0L
    },
    .fabric_warehouse_disconnect = function(...) TRUE,
    .fabric_warehouse_remove_staging = function(...) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )

  error <- tryCatch(
    fabric_warehouse_write_table(
      warehouse_write_test_warehouse(),
      "orders",
      data.frame(id = 1L, label = "one"),
      staging_lakehouse = warehouse_write_test_lakehouse(),
      keep_staging_on_failure = FALSE,
      token = "test-token",
      verbose = FALSE
    ),
    error = identity
  )

  expect_s3_class(error, "fabric_warehouse_write_error")
  expect_s3_class(error$parent, "fabric_warehouse_column_error")
  expect_false(error$ambiguous)
  expect_false(error$staging_retained)
  expect_identical(sql_calls, 0L)
  expect_identical(cleanup_calls, 1L)
})

test_that("Warehouse writer rolls back and retains ambiguous SQL staging", {
  skip_if_not_installed("arrow")
  events <- character()
  cleanup_calls <- 0L
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "failed-copy",
    onelake_upload_target = function(...) tibble::tibble(),
    .fabric_warehouse_connect = function(...) list(),
    .fabric_warehouse_query = function(...) {
      data.frame(column_name = "id")
    },
    .fabric_warehouse_begin = function(...) {
      events <<- c(events, "begin")
      TRUE
    },
    .fabric_warehouse_execute = function(connection, sql) {
      if (startsWith(sql, "COPY INTO")) {
        rlang::abort("connection lost")
      }
      events <<- c(events, "truncate")
      0L
    },
    .fabric_warehouse_rollback = function(...) {
      events <<- c(events, "rollback")
      TRUE
    },
    .fabric_warehouse_disconnect = function(...) {
      events <<- c(events, "disconnect")
      TRUE
    },
    .fabric_warehouse_remove_staging = function(...) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )

  error <- expect_error(
    fabric_warehouse_write_table(
      warehouse_write_test_warehouse(),
      "orders",
      data.frame(id = 1L),
      staging_lakehouse = warehouse_write_test_lakehouse(),
      mode = "Overwrite",
      keep_staging_on_failure = FALSE,
      token = "test-token",
      verbose = FALSE
    ),
    class = "fabric_warehouse_write_error"
  )

  expect_true(error$ambiguous)
  expect_true(error$staging_retained)
  expect_equal(cleanup_calls, 0L)
  expect_equal(events, c("begin", "truncate", "rollback", "disconnect"))
})

test_that("Warehouse writer can remove staging after a pre-SQL failure", {
  skip_if_not_installed("arrow")
  cleanup_calls <- 0L
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "connect-failed",
    onelake_upload_target = function(...) tibble::tibble(),
    .fabric_warehouse_connect = function(...) {
      rlang::abort("driver unavailable")
    },
    .fabric_warehouse_remove_staging = function(...) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )

  error <- expect_error(
    fabric_warehouse_write_table(
      warehouse_write_test_warehouse(),
      "orders",
      data.frame(id = 1L),
      staging_lakehouse = warehouse_write_test_lakehouse(),
      keep_staging_on_failure = FALSE,
      token = "test-token",
      verbose = FALSE
    ),
    class = "fabric_warehouse_write_error"
  )

  expect_false(error$ambiguous)
  expect_false(error$staging_retained)
  expect_equal(cleanup_calls, 1L)
})

test_that("Warehouse writer streams a lazy Arrow Dataset", {
  skip_if_not_installed("arrow")
  directory <- tempfile("fabricqueryr-warehouse-dataset-")
  dir.create(directory)
  on.exit(unlink(directory, recursive = TRUE, force = TRUE), add = TRUE)
  arrow::write_parquet(
    data.frame(id = 1:2, label = c("a", "b")),
    file.path(directory, "one.parquet")
  )
  arrow::write_parquet(
    data.frame(id = 3:4, label = c("c", "d")),
    file.path(directory, "two.parquet")
  )
  uploaded <- list()
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "arrow-load",
    onelake_upload_target = function(source, ...) {
      uploaded[[length(uploaded) + 1L]] <<-
        as.data.frame(arrow::read_parquet(source))
      tibble::tibble()
    },
    .fabric_warehouse_connect = function(...) list(),
    .fabric_warehouse_query = function(...) {
      data.frame(column_name = c("id", "label"))
    },
    .fabric_warehouse_execute = function(...) 4L,
    .fabric_warehouse_disconnect = function(...) TRUE,
    .fabric_warehouse_remove_staging = function(...) TRUE
  )

  result <- fabric_warehouse_write_table(
    warehouse_write_test_warehouse(),
    "orders",
    arrow::open_dataset(directory),
    staging_lakehouse = warehouse_write_test_lakehouse(),
    max_rows_per_file = 2,
    token = "test-token",
    verbose = FALSE
  )

  expect_equal(result$rows, 4)
  expect_equal(result$file_count, 2L)
  expect_equal(unlist(lapply(uploaded, `[[`, "id")), 1:4)
})

test_that("Warehouse writer validates destinations before network I/O", {
  skip_if_not_installed("arrow")
  calls <- 0L
  invoke <- function(
    data = data.frame(id = 1L),
    staging_lakehouse = warehouse_write_test_lakehouse(),
    ...
  ) {
    fabric_warehouse_write_table(
      warehouse_write_test_warehouse(),
      "orders",
      data,
      staging_lakehouse = staging_lakehouse,
      token = "test-token",
      verbose = FALSE,
      ...
    )
  }
  local_mocked_bindings(
    onelake_upload_target = function(...) {
      calls <<- calls + 1L
      tibble::tibble()
    }
  )

  expect_error(invoke(schema = ""), "schema")
  expect_error(
    invoke(staging_root = "Tables/staging"),
    "must begin with Files/"
  )
  expect_error(invoke(mode = "merge"), "must be one of")
  expect_error(invoke(overwrite_method = "replace"), "must be one of")
  expect_error(invoke(create_if_missing = NA), "TRUE or FALSE")
  expect_error(
    invoke(data = data.frame(A = 1L, a = 2L, check.names = FALSE)),
    "unique ignoring case"
  )
  bad_stage <- warehouse_write_test_lakehouse()
  bad_stage$type <- "Warehouse"
  expect_error(
    invoke(staging_lakehouse = bad_stage),
    class = "fabric_warehouse_target_error"
  )
  expect_error(
    invoke(workspace = "44444444-4444-4444-8444-444444444444"),
    "belongs to a different workspace"
  )
  expect_equal(calls, 0L)
})
