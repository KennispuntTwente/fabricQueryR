warehouse_write_test_workspace_id <- "11111111-1111-4111-8111-111111111111"
warehouse_write_test_warehouse_id <- "22222222-2222-4222-8222-222222222222"
warehouse_write_test_lakehouse_id <- "33333333-3333-4333-8333-333333333333"

warehouse_write_test_warehouse <- function() {
  structure(
    list(
      id = warehouse_write_test_warehouse_id,
      workspaceId = warehouse_write_test_workspace_id,
      displayName = "TestWarehouse",
      type = "Warehouse",
      sql_server = "test.datawarehouse.fabric.microsoft.com",
      sql_database = "TestWarehouse"
    ),
    class = c("fabric_item", "list")
  )
}

warehouse_write_test_lakehouse <- function() {
  structure(
    list(
      id = warehouse_write_test_lakehouse_id,
      workspaceId = warehouse_write_test_workspace_id,
      displayName = "TestLakehouse",
      type = "Lakehouse",
      workspaceOneLakeDfsEndpoint = paste0(
        "https://",
        warehouse_write_test_workspace_id,
        ".z12.dfs.fabric.microsoft.com"
      )
    ),
    class = c("fabric_item", "list")
  )
}

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
    "COPY INTO [dbo].[sales]]orders] ([id] 1, [display name] 2)",
    fixed = TRUE
  )
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
})

test_that("Warehouse writer rolls back and retains ambiguous SQL staging", {
  skip_if_not_installed("arrow")
  events <- character()
  cleanup_calls <- 0L
  local_mocked_bindings(
    .fabric_warehouse_staging_id = function() "failed-copy",
    onelake_upload_target = function(...) tibble::tibble(),
    .fabric_warehouse_connect = function(...) list(),
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
