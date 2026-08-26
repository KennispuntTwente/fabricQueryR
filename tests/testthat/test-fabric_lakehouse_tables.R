test_that("Lakehouse schema discovery follows OneLake metadata pages", {
  calls <- character()
  audiences <- character()
  provider <- function(audience, force_refresh = FALSE) {
    audiences <<- c(audiences, audience)
    "storage-token"
  }
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    second <- grepl("page_token=next-schema", req$url, fixed = TRUE)
    body <- if (second) {
      list(
        schemas = list(list(
          name = "sales",
          catalog_name = lakehouse_table_test_id,
          full_name = paste(lakehouse_table_test_id, "sales", sep = "."),
          owner = "data-team",
          schema_id = "schema-sales"
        )),
        next_page_token = NULL
      )
    } else {
      list(
        schemas = list(list(
          name = "dbo",
          catalog_name = lakehouse_table_test_id,
          comment = "Default schema",
          created_at = 1786622400123,
          future_schema = "kept"
        )),
        next_page_token = "next-schema"
      )
    }
    lakehouse_table_test_response(body, url = req$url)
  })

  schemas <- fabric_lakehouse_schemas(
    lakehouse_table_test_item(),
    page_size = 1L,
    token = provider
  )

  expect_s3_class(schemas, "tbl_df")
  expect_equal(schemas$name, c("dbo", "sales"))
  expect_equal(schemas$catalog, rep(lakehouse_table_test_id, 2L))
  expect_equal(
    schemas$full_name,
    paste(lakehouse_table_test_id, c("dbo", "sales"), sep = ".")
  )
  expect_equal(schemas$comment[[1L]], "Default schema")
  expect_equal(schemas$owner[[2L]], "data-team")
  expect_s3_class(schemas$created_at, "POSIXct")
  expect_equal(schemas$raw[[1L]]$future_schema, "kept")
  expect_length(calls, 2L)
  expect_true(all(grepl("max_results=1", calls, fixed = TRUE)))
  expect_equal(audiences, rep(.fabric_audience$storage, 2L))
})

test_that("Lakehouse singular discovery merges Fabric and OneLake metadata", {
  calls <- character()
  audiences <- character()
  provider <- function(audience, force_refresh = FALSE) {
    audiences <<- c(audiences, audience)
    "audience-token"
  }
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    if (grepl("api.fabric.microsoft.com", req$url, fixed = TRUE)) {
      return(lakehouse_table_test_response(
        list(
          data = list(list(
            name = "sales.\u00e9xport_\u6570\u636e",
            type = "External",
            format = "Delta",
            location = paste0(
              "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/",
              "Tables/sales/\u00e9xport_\u6570\u636e"
            ),
            future_fabric = "kept"
          )),
          continuationToken = NULL
        ),
        url = req$url
      ))
    }
    lakehouse_table_test_response(
      list(
        name = "\u00e9xport_\u6570\u636e",
        schema_name = "sales",
        table_id = "table-export",
        columns = list(list(name = "id", type_name = "long")),
        storage_location = paste0(
          "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/",
          "Tables/sales/\u00e9xport_\u6570\u636e"
        ),
        future_detail = "kept"
      ),
      url = req$url
    )
  })

  table <- fabric_lakehouse_table(
    lakehouse_table_test_item(),
    list(name = "\u00e9xport_\u6570\u636e", schema = "sales"),
    token = provider
  )

  expect_equal(table$name, "\u00e9xport_\u6570\u636e")
  expect_equal(table$schema, "sales")
  expect_equal(table$full_name, "sales.\u00e9xport_\u6570\u636e")
  expect_equal(table$type, "External")
  expect_equal(table$format, "Delta")
  expect_equal(table$columns[[1L]][[1L]]$name, "id")
  expect_equal(table$schema_metadata[[1L]]$name, "sales")
  expect_equal(table$raw[[1L]]$future_detail, "kept")
  expect_equal(table$fabric_raw[[1L]]$future_fabric, "kept")
  expect_length(calls, 2L)
  expect_match(
    calls[[2L]],
    "%C3%A9xport_%E6%95%B0%E6%8D%AE",
    ignore.case = TRUE
  )
  expect_equal(audiences, c(.fabric_audience$fabric, .fabric_audience$storage))
})

test_that("OneLake table targets accept named-list aliases and schema overrides", {
  aliases <- .fabric_onelake_table_target(
    list(table = "orders", schema_name = "curated"),
    schema = NULL,
    default_schema = "dbo"
  )
  override <- .fabric_onelake_table_target(
    list(name = "orders", schema = "curated"),
    schema = "reporting",
    default_schema = "dbo"
  )

  expect_identical(aliases, list(table = "orders", schema = "curated"))
  expect_identical(override, list(table = "orders", schema = "reporting"))
})

test_that("Lakehouse reader resolves discovered item and table records", {
  captured <- NULL
  item <- lakehouse_table_test_item(default_schema = "sales")
  table <- tibble::tibble(name = "orders", schema = "curated")
  local_mocked_bindings(
    fabric_onelake_read_delta_table = function(...) {
      captured <<- list(...)
      tibble::tibble(id = 1L)
    }
  )

  result <- fabric_lakehouse_read_table(
    item,
    table,
    columns = "id",
    limit = 1,
    version = 4,
    result = "tibble",
    verbose = FALSE,
    token = "storage-token"
  )

  expect_equal(result$id, 1L)
  expect_identical(captured$table_path, "orders")
  expect_identical(captured$workspace_name, item$workspaceId)
  expect_identical(captured$lakehouse_name, item)
  expect_identical(captured$schema, "curated")
  expect_identical(captured$item_type, "Lakehouse")
  expect_identical(captured$columns, "id")
  expect_identical(captured$limit, 1)
  expect_identical(captured$version, 4)
  expect_identical(captured$result, "tibble")
  expect_false(captured$verbose)
  expect_identical(captured$token, "storage-token")
  expect_null(captured$dfs_base)

  fabric_lakehouse_read_table(
    item,
    table,
    schema = "explicit",
    token = "storage-token"
  )
  expect_identical(captured$schema, "explicit")
})

test_that("Lakehouse reader accepts names and explicit schema", {
  captured <- NULL
  local_mocked_bindings(
    fabric_onelake_read_delta_table = function(...) {
      captured <<- list(...)
      structure(list(), class = "nanoarrow_array_stream")
    }
  )

  result <- fabric_lakehouse_read_table(
    "Curated",
    "orders",
    workspace = "Analytics",
    schema = "dbo",
    result = "arrow_stream",
    token = "storage-token"
  )

  expect_s3_class(result, "nanoarrow_array_stream")
  expect_identical(captured$workspace_name, "Analytics")
  expect_identical(captured$lakehouse_name, "Curated")
  expect_identical(captured$schema, "dbo")
  expect_identical(captured$result, "arrow_stream")
})

test_that("Lakehouse reader honors a schema-disabled table location", {
  captured <- NULL
  item <- lakehouse_table_test_item(default_schema = "dbo")
  item$workspaceId <- "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
  item$id <- "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  table <- tibble::tibble(
    name = "orders",
    schema = "dbo",
    location = paste0(
      "https://onelake.dfs.fabric.microsoft.com/",
      item$workspaceId,
      "/",
      item$id,
      "/",
      "Tables/orders"
    )
  )
  local_mocked_bindings(
    fabric_onelake_read_delta_table = function(...) {
      captured <<- list(...)
      tibble::tibble(id = 1L)
    }
  )

  fabric_lakehouse_read_table(item, table, token = "storage-token")

  expect_identical(captured$table_path, "orders")
  expect_null(captured$schema)
})

test_that("Lakehouse reader rejects ambiguous and non-Lakehouse targets", {
  expect_error(
    fabric_lakehouse_read_table("Curated", "orders"),
    "workspace is required",
    class = "fabric_lakehouse_read_error"
  )
  expect_error(
    fabric_lakehouse_read_table(
      list(
        id = "warehouse-id",
        workspaceId = "workspace-id",
        type = "Warehouse"
      ),
      "orders"
    ),
    "must be a Lakehouse",
    class = "fabric_lakehouse_read_error"
  )
})


test_that("Lakehouse table discovery follows schema and table pages", {
  calls <- character()
  audiences <- character()
  provider <- function(audience, force_refresh = FALSE) {
    audiences <<- c(audiences, audience)
    "test-token"
  }
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    url <- utils::URLdecode(req$url)
    body <- if (grepl("api.fabric.microsoft.com", url, fixed = TRUE)) {
      token <- if (grepl("continuationToken=fabric-2", url, fixed = TRUE)) {
        "fabric-2"
      } else if (grepl("continuationToken=fabric-3", url, fixed = TRUE)) {
        "fabric-3"
      } else {
        ""
      }
      name <- switch(
        token,
        `fabric-2` = "customers",
        `fabric-3` = "\u00e9xport_\u6570\u636e",
        "orders"
      )
      suffix <- if (identical(name, "\u00e9xport_\u6570\u636e")) {
        paste("sales", name, sep = "/")
      } else {
        name
      }
      list(
        data = list(list(
          name = name,
          type = if (identical(name, "\u00e9xport_\u6570\u636e")) {
            "External"
          } else {
            "Managed"
          },
          format = "Delta",
          location = paste0(
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/",
            "Tables/",
            suffix
          ),
          future_fabric = paste0("kept-", name)
        )),
        continuationToken = switch(
          token,
          `fabric-2` = "fabric-3",
          `fabric-3` = NULL,
          "fabric-2"
        )
      )
    } else if (grepl("/schemas[?]", url, fixed = FALSE)) {
      if (grepl("page_token=schema-2", url, fixed = TRUE)) {
        list(
          schemas = list(list(name = "sales", comment = "Sales data")),
          next_page_token = NULL
        )
      } else {
        list(
          schemas = list(list(name = "dbo", future_schema = "kept")),
          next_page_token = "schema-2"
        )
      }
    } else if (grepl("/tables[?]", url, fixed = FALSE)) {
      schema <- if (grepl("schema_name=sales", url, fixed = TRUE)) {
        "sales"
      } else {
        "dbo"
      }
      if (
        identical(schema, "dbo") &&
          !grepl("page_token=table-2", url, fixed = TRUE)
      ) {
        list(
          tables = list(list(
            name = "orders",
            schema_name = "dbo",
            table_type = NULL,
            data_source_format = "DELTA",
            storage_location = paste0(
              "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/",
              "Tables/orders"
            ),
            future_table = list(value = "kept")
          )),
          next_page_token = "table-2"
        )
      } else {
        name <- if (identical(schema, "sales")) {
          "\u00e9xport_\u6570\u636e"
        } else {
          "customers"
        }
        list(
          tables = list(list(
            name = name,
            schema_name = schema,
            table_type = NULL,
            data_source_format = "DELTA",
            storage_location = paste0(
              "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/",
              "Tables/",
              if (identical(schema, "sales")) paste0("sales/", name) else name
            )
          )),
          next_page_token = NULL
        )
      }
    } else {
      name <- if (grepl("orders", url, fixed = TRUE)) {
        "orders"
      } else if (grepl("customers", url, fixed = TRUE)) {
        "customers"
      } else {
        "\u00e9xport_\u6570\u636e"
      }
      schema <- if (identical(name, "\u00e9xport_\u6570\u636e")) {
        "sales"
      } else {
        "dbo"
      }
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
        ))
      )
    }
    lakehouse_table_test_response(body, url = req$url)
  })

  tables <- fabric_lakehouse_tables(
    lakehouse_table_test_item(),
    page_size = 1L,
    token = provider
  )

  expect_s3_class(tables, "tbl_df")
  expect_setequal(
    tables$name,
    c("orders", "customers", "\u00e9xport_\u6570\u636e")
  )
  expect_setequal(tables$schema, c("dbo", "sales"))
  expect_setequal(
    tables$full_name,
    c(
      "dbo.orders",
      "dbo.customers",
      "sales.\u00e9xport_\u6570\u636e"
    )
  )
  expect_equal(tables$type[tables$name == "orders"], "Managed")
  expect_equal(
    tables$type[tables$name == "\u00e9xport_\u6570\u636e"],
    "External"
  )
  expect_equal(tables$format, rep("Delta", 3L))
  expect_s3_class(tables$created_at, "POSIXct")
  expect_equal(tables$columns[[1L]][[1L]]$type_name, "long")
  expect_equal(
    tables$schema_metadata[[which(tables$schema == "dbo")[[1L]]]]$future_schema,
    "kept"
  )
  expect_equal(
    tables$raw[[which(tables$name == "orders")]]$future_table$value,
    "kept"
  )
  expect_equal(
    tables$fabric_raw[[which(tables$name == "orders")]]$future_fabric,
    "kept-orders"
  )
  expect_equal(
    tables$location[tables$name == "orders"],
    paste0(
      "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/",
      "Tables/orders"
    )
  )
  expect_equal(
    sum(grepl("api.fabric.microsoft.com", calls, fixed = TRUE)),
    3L
  )
  expect_equal(sum(grepl("/schemas", calls, fixed = TRUE)), 2L)
  expect_equal(
    sum(
      grepl("onelake.table.fabric.microsoft.com", calls, fixed = TRUE) &
        grepl("/tables?", calls, fixed = TRUE)
    ),
    3L
  )
  expect_equal(length(calls), 11L)
  expect_equal(sum(grepl("maxResults=1", calls, fixed = TRUE)), 3L)
  expect_equal(sum(grepl("max_results=1", calls, fixed = TRUE)), 5L)
  expect_equal(audiences[seq_len(3L)], rep(.fabric_audience$fabric, 3L))
  expect_true(all(audiences[-seq_len(3L)] == .fabric_audience$storage))
})

test_that("Lakehouse discovery can target one schema without detail requests", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    if (grepl("api.fabric.microsoft.com", req$url, fixed = TRUE)) {
      return(lakehouse_table_test_response(
        list(
          data = list(list(
            name = "orders",
            type = "Managed",
            format = "Delta",
            location = paste0(
              "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/",
              "Tables/sales/orders"
            )
          )),
          continuationToken = NULL
        ),
        url = req$url
      ))
    }
    lakehouse_table_test_response(
      list(
        tables = list(list(
          name = "orders",
          schema_name = "sales",
          data_source_format = "DELTA",
          storage_location = paste0(
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/",
            "Tables/sales/orders"
          )
        )),
        next_page_token = NULL
      ),
      url = req$url
    )
  })

  tables <- fabric_lakehouse_tables(
    lakehouse_table_test_item(),
    schema = "sales",
    detail = FALSE,
    token = "fabric-token",
    storage_token = "storage-token"
  )

  expect_equal(tables$full_name, "sales.orders")
  expect_equal(tables$type, "Managed")
  expect_length(calls, 2L)
  expect_false(any(grepl("/schemas", calls, fixed = TRUE)))
  expect_length(tables$columns[[1L]], 0L)
})

test_that("Lakehouse detail enrichment retains its listing snapshot", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    if (grepl("api.fabric.microsoft.com", req$url, fixed = TRUE)) {
      return(lakehouse_table_test_response(
        list(
          data = list(
            list(name = "removed", type = "Managed", format = "Delta"),
            list(name = "current", type = "Managed", format = "Delta"),
            list(name = "after", type = "External", format = "Delta")
          ),
          continuationToken = NULL
        ),
        url = req$url
      ))
    }
    if (grepl("/tables?", req$url, fixed = TRUE)) {
      return(lakehouse_table_test_response(
        list(
          tables = list(
            list(name = "removed", schema_name = "dbo"),
            list(name = "current", schema_name = "dbo"),
            list(name = "after", schema_name = "dbo")
          ),
          next_page_token = NULL
        ),
        url = req$url
      ))
    }
    if (grepl("removed", req$url, fixed = TRUE)) {
      return(lakehouse_table_test_response(
        list(error = list(code = "TableNotFound", message = "Missing")),
        status = 404L,
        url = req$url
      ))
    }
    if (grepl("current", req$url, fixed = TRUE)) {
      return(lakehouse_table_test_response(
        list(
          name = "current",
          schema_name = "dbo",
          columns = list(list(name = "id", type_name = "long"))
        ),
        url = req$url
      ))
    }
    lakehouse_table_test_response(
      list(
        name = "after",
        schema_name = "dbo",
        columns = list(list(name = "value", type_name = "string"))
      ),
      url = req$url
    )
  })

  tables <- fabric_lakehouse_tables(
    lakehouse_table_test_item(),
    schema = "dbo",
    token = "fabric-token",
    storage_token = "storage-token"
  )

  expect_equal(tables$name, c("removed", "current", "after"))
  expect_length(tables$columns[[1L]], 0L)
  expect_equal(tables$columns[[2L]][[1L]]$name, "id")
  expect_equal(tables$columns[[3L]][[1L]]$name, "value")
  expect_identical(tables$raw[[1L]]$name, "removed")
  expect_identical(tables$fabric_raw[[1L]]$type, "Managed")
  expect_length(calls, 5L)
})

test_that("schema discovery falls back when Fabric rejects List Tables", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    if (grepl("api.fabric.microsoft.com", req$url, fixed = TRUE)) {
      return(lakehouse_table_test_response(
        list(
          requestId = lakehouse_table_test_operation,
          errorCode = "UnsupportedOperationForSchemasEnabledLakehouse",
          message = "The operation is not supported for Lakehouse with schemas enabled.",
          isRetriable = FALSE
        ),
        status = 400L,
        url = req$url
      ))
    }
    lakehouse_table_test_response(
      list(
        tables = list(list(
          name = "orders",
          schema_name = "sales",
          table_type = NULL,
          data_source_format = "DELTA",
          storage_location = paste0(
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/",
            "Tables/sales/orders"
          )
        )),
        next_page_token = NULL
      ),
      url = req$url
    )
  })

  tables <- fabric_lakehouse_tables(
    lakehouse_table_test_item(),
    schema = "sales",
    detail = FALSE,
    token = "fabric-token",
    storage_token = "storage-token"
  )

  expect_equal(tables$name, "orders")
  expect_true(is.na(tables$type))
  expect_equal(tables$format, "DELTA")
  expect_match(tables$location, "/Tables/sales/orders$")
  expect_length(tables$fabric_raw[[1L]], 0L)
  expect_length(calls, 2L)
})

test_that("Lakehouse load builds the documented CSV schema request", {
  captured <- NULL
  location <- paste0(
    "https://api.fabric.microsoft.com/v1/workspaces/",
    lakehouse_table_test_workspace,
    "/lakehouses/",
    lakehouse_table_test_id,
    "/operations/",
    lakehouse_table_test_operation
  )
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    lakehouse_table_test_response(
      status = 202L,
      headers = list(
        Location = location,
        `x-ms-operation-id` = lakehouse_table_test_operation,
        `Retry-After` = "5"
      ),
      url = req$url
    )
  })

  operation <- fabric_lakehouse_load_table(
    lakehouse_table_test_item(),
    table = "orders_2026",
    path = "Files/incoming/caf\u00e9-\u6570\u636e.csv",
    format = "csv",
    mode = "append",
    header = FALSE,
    delimiter = ";",
    token = "test-token"
  )

  expect_s3_class(operation, "fabric_operation")
  expect_null(operation$result_url)
  expect_false(operation$result_expected)
  expect_equal(operation$schema, "dbo")
  expect_equal(
    operation$source_path,
    "Files/incoming/caf\u00e9-\u6570\u636e.csv"
  )
  expect_equal(operation$retry_after, 5)
  expect_equal(captured$method, "POST")
  expect_match(
    captured$url,
    "/schemas/dbo/tables/orders_2026/load[?]beta=true$"
  )
  expect_equal(
    captured$body$data,
    list(
      relativePath = "Files/incoming/caf\u00e9-\u6570\u636e.csv",
      pathType = "File",
      mode = "Append",
      recursive = FALSE,
      formatOptions = list(format = "Csv", header = FALSE, delimiter = ";")
    )
  )
})

test_that("Lakehouse load supports folder Parquet options", {
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    lakehouse_table_test_response(
      status = 202L,
      headers = list(
        Location = paste0("/v1/operations/", lakehouse_table_test_operation),
        `x-ms-operation-id` = lakehouse_table_test_operation
      ),
      url = req$url
    )
  })

  operation <- fabric_lakehouse_load_table(
    lakehouse_table_test_item(default_schema = NULL),
    table = "orders",
    path = "Files/incoming/orders",
    path_type = "folder",
    format = "parquet",
    mode = "overwrite",
    recursive = TRUE,
    file_extension = ".parquet",
    token = "test-token"
  )

  expect_match(captured$url, "/tables/orders/load$")
  expect_equal(captured$body$data$fileExtension, "parquet")
  expect_equal(captured$body$data$formatOptions, list(format = "Parquet"))
  expect_true(captured$body$data$recursive)
  expect_equal(operation$format, "Parquet")
  expect_false(operation$result_expected)
  expect_null(operation$result_url)
})

test_that("Lakehouse-scoped operation states are normalized and result-ready", {
  clock <- lakehouse_table_test_clock()
  calls <- 0L
  location <- paste0(
    "https://api.fabric.microsoft.com/v1/workspaces/",
    lakehouse_table_test_workspace,
    "/lakehouses/",
    lakehouse_table_test_id,
    "/operations/",
    lakehouse_table_test_operation
  )
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    if (calls == 1L) {
      return(lakehouse_table_test_response(
        status = 202L,
        headers = list(
          Location = location,
          `x-ms-operation-id` = lakehouse_table_test_operation,
          `Retry-After` = "0"
        ),
        url = req$url
      ))
    }
    status <- if (calls == 2L) 2L else 3L
    lakehouse_table_test_response(
      list(
        Status = status,
        CreatedTimeUtc = "",
        LastUpdatedTimeUtc = "",
        PercentComplete = if (status == 2L) 50L else 100L,
        Error = NULL
      ),
      headers = list(`Retry-After` = "1"),
      url = req$url
    )
  })

  operation <- fabric_lakehouse_load_table(
    lakehouse_table_test_item(default_schema = NULL),
    table = "orders",
    path = "Files/orders.parquet",
    token = "test-token"
  )
  operation$next_poll_at <- NULL
  completed <- fabric_operation_wait(
    operation,
    timeout = 10,
    .sleep = clock$sleep,
    .now = clock$now
  )
  result <- fabric_operation_result(
    completed,
    wait = FALSE,
    .sleep = clock$sleep,
    .now = clock$now
  )

  expect_equal(completed$status, "Succeeded")
  expect_equal(completed$percent_complete, 100L)
  expect_null(completed$created_time)
  expect_equal(result$value$Status, 3L)
  expect_equal(result$content_type, "application/json")
  expect_false(result$empty)
  expect_false(result$operation$result_expected)
  expect_equal(calls, 4L)
  expect_false(any(grepl(
    "/result",
    c(
      completed$operation$status_url,
      completed$operation$result_url %||% ""
    ),
    fixed = TRUE
  )))
})

test_that("Lakehouse table inputs fail before requests are sent", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    lakehouse_table_test_response(url = req$url)
  })
  invoke <- function(table = "orders", path = "Files/orders.csv", ...) {
    fabric_lakehouse_load_table(
      lakehouse_table_test_item(),
      table = table,
      path = path,
      token = "test-token",
      ...
    )
  }

  expect_error(invoke(table = "123"), "table must contain")
  expect_error(invoke(table = "orders-bad"), "table must contain")
  expect_error(invoke(path = "Tables/orders.csv"), "must be Files or begin")
  expect_error(invoke(path = "files/orders.csv"), "relativePath syntax")
  expect_error(invoke(path = "Files/orders?.csv"), "relativePath syntax")
  expect_error(invoke(path = "Files/.hidden"), "relativePath syntax")
  expect_error(invoke(schema = "bad-name"), "schema must contain")
  expect_error(invoke(recursive = TRUE), "requires path_type")
  expect_error(
    invoke(path_type = "folder", file_extension = "this-extension-is-too-long"),
    "file_extension must"
  )
  expect_equal(
    .fabric_onelake_table_api_base(
      "https://custom.example/delta",
      error_class = "fabric_lakehouse_endpoint_error"
    ),
    "https://custom.example/delta"
  )
  expect_error(
    fabric_lakehouse_tables(
      lakehouse_table_test_item(),
      page_size = 101L,
      token = "test-token"
    ),
    "1 to 100"
  )
  expect_equal(calls, 0L)
})

test_that("Lakehouse discovery does not reuse a fixed Fabric token", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    lakehouse_table_test_response(url = req$url)
  })

  error <- expect_error(
    fabric_lakehouse_tables(
      lakehouse_table_test_item(),
      token = "fabric-token"
    ),
    class = "fabric_multi_audience_auth_error"
  )

  expect_identical(error$argument, "storage_token")
  expect_identical(calls, 0L)
})

test_that("OneLake table metadata enforces a maximum page count", {
  calls <- 0L
  local_mocked_bindings(
    .httr2_json = function(...) {
      calls <<- calls + 1L
      list(
        tables = list(),
        next_page_token = paste0("page-", calls)
      )
    }
  )

  error <- expect_error(
    .fabric_onelake_table_pages(
      "https://onelake.table.fabric.microsoft.com/delta/tables",
      field = "tables",
      query = list(catalog_name = lakehouse_table_test_id),
      credential = fabric_credential(token = "storage-token"),
      page_size = 1L,
      error_class = c(
        "fabric_lakehouse_protocol_error",
        "fabric_lakehouse_error"
      ),
      max_pages = 2L,
      pagination_timeout = 60
    ),
    class = "fabric_lakehouse_protocol_error"
  )

  expect_identical(error$max_pages, 2L)
  expect_identical(calls, 2L)
})

test_that("Lakehouse paths follow the Fabric relativePath grammar", {
  valid <- c(
    "Files",
    "Files/a",
    "Files/incoming/orders 2026-08.csv",
    "Files/\u6570\u636e/\u00e9chantillon.parquet",
    "Files/a-b_c."
  )
  for (path in valid) {
    expect_identical(.fabric_lakehouse_files_path(path, "path"), path)
  }

  invalid <- c(
    "files/a.csv",
    "Files/a?b.csv",
    "Files/-leading.csv",
    "Files/ leading.csv"
  )
  for (path in invalid) {
    expect_error(
      .fabric_lakehouse_files_path(path, "path"),
      "relativePath syntax",
      fixed = TRUE
    )
  }
})

test_that("Lakehouse CSV delimiters follow the published schema", {
  settings <- function(delimiter) {
    .fabric_lakehouse_load_settings(
      table = "orders",
      schema = NULL,
      path = "Files/orders.csv",
      path_type = "File",
      format = "Csv",
      mode = "Append",
      recursive = FALSE,
      header = TRUE,
      delimiter = delimiter,
      file_extension = NULL
    )
  }

  for (delimiter in c("", " ", "\t", "12345678")) {
    expect_identical(settings(delimiter)$delimiter, delimiter)
  }
  for (delimiter in c("123456789", "(", ")", "[", "]", "{", "}", "'", '"')) {
    expect_error(
      settings(delimiter),
      "delimiter must be 0 to 8 characters",
      fixed = TRUE
    )
  }
})

test_that("Lakehouse writer serializes, loads, and cleans up after success", {
  skip_if_not_installed("arrow")
  uploaded <- NULL
  cleanup_calls <- 0L
  fake_operation <- structure(
    list(id = lakehouse_table_test_operation),
    class = "fabric_operation"
  )
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "load-fixed",
    onelake_upload_target = function(target, credential, source, ...) {
      uploaded <<- list(
        target = target,
        data = as.data.frame(arrow::read_parquet(source)),
        credential = credential
      )
      tibble::tibble(path = target$path)
    },
    .fabric_lakehouse_load_submit = function(
      target,
      settings,
      credential,
      ...
    ) {
      expect_equal(settings$path, "Files/staging/load-fixed")
      expect_equal(settings$path_type, "Folder")
      expect_equal(settings$file_extension, "parquet")
      expect_equal(settings$format, "Parquet")
      fake_operation
    },
    fabric_operation_wait = function(operation, ...) {
      structure(
        list(
          status = "Succeeded",
          operation = operation
        ),
        class = "fabric_operation_state"
      )
    },
    .fabric_lakehouse_remove_staging = function(target, credential) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )
  unicode_name <- "caf\u00e9_\u6570\u636e"
  value <- data.frame(
    id = bit64::as.integer64(c("9007199254740993", NA)),
    amount = c(10.5, NA),
    observed_on = as.Date(c("2026-01-01", NA)),
    kind = factor(c("a", "b")),
    stringsAsFactors = FALSE
  )
  value[[unicode_name]] <- c("\u00e9\u00e9n", NA)

  result <- fabric_lakehouse_write_table(
    lakehouse_table_test_item(),
    table = "orders",
    data = value,
    staging_root = "Files/staging",
    token = "fabric-token",
    storage_token = "storage-token"
  )

  expect_s3_class(result, "fabric_lakehouse_write_result")
  expect_equal(result$rows, 2L)
  expect_equal(result$schema, "dbo")
  expect_equal(result$file_count, 1L)
  expect_false(result$staging_retained)
  expect_equal(cleanup_calls, 1L)
  expect_equal(
    uploaded$target$path,
    paste0(result$staging_path, "/part-00001.parquet")
  )
  expect_equal(uploaded$data$kind, c("a", "b"))
  expect_equal(as.character(uploaded$data$id[[1L]]), "9007199254740993")
  expect_true(is.na(uploaded$data[[unicode_name]][[2L]]))
  expect_identical(
    fabric_get_token(uploaded$credential, .fabric_audience$storage),
    "storage-token"
  )
})

test_that("Lakehouse writer loads bounded Parquet folders", {
  skip_if_not_installed("arrow")
  uploads <- list()
  settings_seen <- NULL
  fake_operation <- structure(
    list(id = lakehouse_table_test_operation),
    class = "fabric_operation"
  )
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "multi-file",
    onelake_upload_target = function(target, source, ...) {
      uploads[[length(uploads) + 1L]] <<- list(
        path = target$path,
        data = as.data.frame(arrow::read_parquet(source))
      )
      tibble::tibble(path = target$path)
    },
    .fabric_lakehouse_load_submit = function(target, settings, ...) {
      settings_seen <<- settings
      fake_operation
    },
    fabric_operation_wait = function(operation, ...) {
      structure(
        list(status = "Succeeded", operation = operation),
        class = "fabric_operation_state"
      )
    },
    .fabric_lakehouse_remove_staging = function(...) TRUE
  )

  result <- fabric_lakehouse_write_table(
    lakehouse_table_test_item(),
    "orders",
    data.frame(id = 1:5),
    max_rows_per_file = 2,
    token = "fabric-token",
    storage_token = "storage-token"
  )

  expect_equal(result$rows, 5)
  expect_equal(result$file_count, 3L)
  expect_equal(length(uploads), 3L)
  expect_equal(
    vapply(uploads, function(x) nrow(x$data), integer(1)),
    c(2L, 2L, 1L)
  )
  expect_equal(unlist(lapply(uploads, function(x) x$data$id)), 1:5)
  expect_equal(settings_seen$path_type, "Folder")
  expect_equal(settings_seen$path, "Files/fabricqueryr-staging/multi-file")
  expect_equal(settings_seen$file_extension, "parquet")
  expect_equal(result$files, vapply(uploads, `[[`, character(1), "path"))
})

test_that("partitioned Parquet staging rotates by bytes and preserves empties", {
  skip_if_not_installed("arrow")
  directory <- tempfile("fabricqueryr-parts-")
  empty_directory <- tempfile("fabricqueryr-empty-parts-")
  on.exit(
    unlink(c(directory, empty_directory), recursive = TRUE, force = TRUE),
    add = TRUE
  )
  prepared <- .fabric_parquet_prepare_data(
    data.frame(id = seq_len(70000L)),
    "test"
  )
  parts <- .fabric_parquet_write_dataset(
    prepared,
    directory,
    compression = "snappy",
    target_file_size = 1,
    caller = "test",
    error_class = "fabric_test_error"
  )
  expect_equal(parts$file_count, 2L)
  expect_equal(parts$rows, 70000)
  expect_equal(parts$rows_per_file, c(65536, 4464))
  expect_true(all(parts$bytes > 0))

  empty <- .fabric_parquet_write_dataset(
    .fabric_parquet_prepare_data(data.frame(id = integer()), "test"),
    empty_directory,
    compression = "snappy",
    target_file_size = 1024,
    caller = "test",
    error_class = "fabric_test_error"
  )
  expect_equal(empty$file_count, 1L)
  expect_equal(empty$rows, 0)
  expect_equal(empty$rows_per_file, 0)
  expect_named(as.data.frame(arrow::read_parquet(empty$paths[[1L]])), "id")
})

test_that("Lakehouse writer reports retained staging on load failure", {
  skip_if_not_installed("arrow")
  cleanup_calls <- 0L
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "load-failed",
    onelake_upload_target = function(...) tibble::tibble(),
    .fabric_lakehouse_load_submit = function(...) {
      rlang::abort(
        "service rejected schema",
        class = "fabric_http_error",
        status = 400L
      )
    },
    .fabric_lakehouse_remove_staging = function(...) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )

  retained <- expect_error(
    fabric_lakehouse_write_table(
      lakehouse_table_test_item(),
      table = "orders",
      data = data.frame(id = 1L),
      token = "fabric-token",
      storage_token = "storage-token"
    ),
    class = "fabric_lakehouse_write_error"
  )
  expect_true(retained$staging_retained)
  expect_match(
    retained$staging_path,
    "Files/fabricqueryr-staging/load-failed",
    fixed = TRUE
  )
  expect_equal(cleanup_calls, 0L)

  removed <- expect_error(
    fabric_lakehouse_write_table(
      lakehouse_table_test_item(),
      table = "orders",
      data = data.frame(id = 1L),
      keep_staging_on_failure = FALSE,
      token = "fabric-token",
      storage_token = "storage-token"
    ),
    class = "fabric_lakehouse_write_error"
  )
  expect_false(removed$staging_retained)
  expect_equal(cleanup_calls, 1L)
})

test_that("Lakehouse writer never removes staging after an ambiguous timeout", {
  skip_if_not_installed("arrow")
  cleanup_calls <- 0L
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "load-timeout",
    onelake_upload_target = function(...) tibble::tibble(),
    .fabric_lakehouse_load_submit = function(...) {
      rlang::abort(
        "polling timed out",
        class = "fabric_operation_timeout"
      )
    },
    .fabric_lakehouse_remove_staging = function(...) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )

  timeout <- expect_error(
    fabric_lakehouse_write_table(
      lakehouse_table_test_item(),
      table = "orders",
      data = data.frame(id = 1L),
      keep_staging_on_failure = FALSE,
      token = "fabric-token",
      storage_token = "storage-token"
    ),
    class = "fabric_lakehouse_write_error"
  )
  expect_true(timeout$staging_retained)
  expect_equal(cleanup_calls, 0L)
})

test_that("Lakehouse writer validates names and unsupported R types", {
  skip_if_not_installed("arrow")
  expect_error(
    fabric_lakehouse_write_table(
      lakehouse_table_test_item(),
      "orders",
      data.frame(`bad name` = 1L, check.names = FALSE),
      token = "test-token"
    ),
    "Column names must"
  )
  expect_error(
    fabric_lakehouse_write_table(
      lakehouse_table_test_item(),
      "orders",
      data.frame(value = as.difftime(1, units = "days")),
      token = "test-token"
    ),
    "Unsupported column type"
  )
})

test_that("Lakehouse writer streams a lazy Arrow Dataset", {
  skip_if_not_installed("arrow")
  dataset_path <- tempfile("fabricqueryr-arrow-dataset-")
  dir.create(dataset_path)
  on.exit(unlink(dataset_path, recursive = TRUE, force = TRUE), add = TRUE)
  arrow::write_parquet(
    data.frame(id = 1:2, label = c("a", "b")),
    file.path(dataset_path, "part-1.parquet")
  )
  arrow::write_parquet(
    data.frame(id = 3:5, label = c("c", "d", "e")),
    file.path(dataset_path, "part-2.parquet")
  )
  dataset <- arrow::open_dataset(dataset_path)
  uploaded <- NULL
  fake_operation <- structure(
    list(id = lakehouse_table_test_operation),
    class = "fabric_operation"
  )
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "lazy-dataset",
    onelake_upload_target = function(target, credential, source, ...) {
      uploaded <<- as.data.frame(arrow::read_parquet(source))
      tibble::tibble(path = target$path)
    },
    .fabric_lakehouse_load_submit = function(...) fake_operation,
    fabric_operation_wait = function(operation, ...) {
      structure(
        list(status = "Succeeded", operation = operation),
        class = "fabric_operation_state"
      )
    },
    .fabric_lakehouse_remove_staging = function(...) TRUE
  )

  result <- fabric_lakehouse_write_table(
    lakehouse_table_test_item(),
    "orders",
    dataset,
    token = "fabric-token",
    storage_token = "storage-token"
  )

  expect_equal(result$rows, 5)
  expect_equal(uploaded$id, 1:5)
  expect_equal(uploaded$label, letters[1:5])
})
