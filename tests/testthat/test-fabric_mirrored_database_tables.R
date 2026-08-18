mirrored_database_test_workspace <- "11111111-1111-4111-8111-111111111111"
mirrored_database_test_id <- "22222222-2222-4222-8222-222222222222"

mirrored_database_test_item <- function(default_schema = "sales") {
  structure(
    list(
      id = mirrored_database_test_id,
      workspaceId = mirrored_database_test_workspace,
      displayName = "OperationalReplica",
      type = "MirroredDatabase",
      default_schema = default_schema,
      one_lake_tables_path = paste0(
        "https://onelake.dfs.fabric.microsoft.com/",
        mirrored_database_test_workspace,
        "/",
        mirrored_database_test_id,
        "/Tables"
      ),
      sql_server = "mirror.datawarehouse.fabric.microsoft.com",
      sql_database = "OperationalReplica"
    ),
    class = c("fabric_item", "list")
  )
}

test_that("mirrored database table discovery follows OneLake metadata", {
  calls <- character()
  audiences <- character()
  provider <- function(audience, force_refresh = FALSE) {
    audiences <<- c(audiences, audience)
    "storage-token"
  }
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    decoded <- utils::URLdecode(req$url)
    body <- if (grepl("/schemas?", decoded, fixed = TRUE)) {
      list(
        schemas = list(list(
          name = "sales",
          catalog_name = mirrored_database_test_id,
          future_schema = "kept"
        )),
        next_page_token = NULL
      )
    } else if (grepl("/tables?", decoded, fixed = TRUE)) {
      list(
        tables = list(list(
          name = "orders",
          schema_name = "sales",
          table_type = "MANAGED",
          data_source_format = "DELTA",
          storage_location = paste0(
            "https://onelake.dfs.fabric.microsoft.com/workspace/mirror/",
            "Tables/sales/orders"
          )
        )),
        next_page_token = NULL
      )
    } else {
      list(
        name = "orders",
        schema_name = "sales",
        table_type = "MANAGED",
        data_source_format = "DELTA",
        columns = list(list(name = "id", type_name = "long")),
        future_detail = "kept"
      )
    }
    lakehouse_table_test_response(body, url = req$url)
  })

  tables <- fabric_mirrored_database_tables(
    mirrored_database_test_item(),
    detail = TRUE,
    page_size = 1L,
    token = provider
  )

  expect_equal(tables$name, "orders")
  expect_equal(tables$schema, "sales")
  expect_equal(tables$full_name, "sales.orders")
  expect_equal(tables$type, "MANAGED")
  expect_equal(tables$format, "DELTA")
  expect_equal(tables$columns[[1L]][[1L]]$name, "id")
  expect_equal(tables$schema_metadata[[1L]]$future_schema, "kept")
  expect_equal(tables$raw[[1L]]$future_detail, "kept")
  expect_length(tables$fabric_raw[[1L]], 0L)
  expect_length(calls, 3L)
  expect_true(all(grepl("max_results=1", calls[1:2], fixed = TRUE)))
  expect_equal(audiences, rep(.fabric_audience$storage, 3L))
})

test_that("mirrored database schema and singular helpers retain stable shapes", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    body <- if (grepl("/schemas", req$url, fixed = TRUE)) {
      list(
        schemas = list(list(
          name = "sales",
          catalog_name = mirrored_database_test_id,
          comment = "Replicated sales"
        )),
        next_page_token = NULL
      )
    } else {
      list(
        name = "orders",
        schema_name = "sales",
        data_source_format = "DELTA",
        columns = list(list(name = "id", type_name = "long"))
      )
    }
    lakehouse_table_test_response(body, url = req$url)
  })

  schemas <- fabric_mirrored_database_schemas(
    mirrored_database_test_item(),
    token = "storage-token"
  )
  table <- fabric_mirrored_database_table(
    mirrored_database_test_item(),
    "orders",
    token = "storage-token"
  )

  expect_equal(schemas$name, "sales")
  expect_equal(schemas$comment, "Replicated sales")
  expect_equal(table$name, "orders")
  expect_equal(table$schema, "sales")
  expect_equal(table$columns[[1L]][[1L]]$name, "id")
  expect_length(calls, 2L)
  expect_match(utils::URLdecode(calls[[2L]]), "sales.orders", fixed = TRUE)
})

test_that("mirrored database reader resolves records and forwards Delta reads", {
  captured <- NULL
  local_mocked_bindings(
    fabric_onelake_read_delta_table = function(...) {
      captured <<- list(...)
      tibble::tibble(id = 1L)
    }
  )
  table <- tibble::tibble(name = "orders", schema = "replicated")

  result <- fabric_mirrored_database_read_table(
    mirrored_database_test_item(),
    table,
    columns = "id",
    limit = 1L,
    version = 4,
    result = "tibble",
    verbose = FALSE,
    token = "storage-token"
  )

  expect_equal(result$id, 1L)
  expect_identical(captured$table_path, "orders")
  expect_identical(captured$schema, "replicated")
  expect_identical(captured$item_type, "MirroredDatabase")
  expect_identical(captured$workspace_name, mirrored_database_test_workspace)
  expect_identical(captured$lakehouse_name$type, "MirroredDatabase")
  expect_identical(captured$columns, "id")
  expect_identical(captured$limit, 1L)
  expect_identical(captured$version, 4)
  expect_null(captured$dfs_base)
  expect_s3_class(captured$token, "fabric_credential")
})
