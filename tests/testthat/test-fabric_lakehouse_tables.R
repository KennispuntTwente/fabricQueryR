lakehouse_table_test_workspace <- "11111111-2222-3333-4444-555555555555"
lakehouse_table_test_id <- "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
lakehouse_table_test_operation <- "99999999-8888-7777-6666-555555555555"

lakehouse_table_test_item <- function(default_schema = "dbo") {
  structure(
    list(
      id = lakehouse_table_test_id,
      workspaceId = lakehouse_table_test_workspace,
      displayName = "Curated",
      type = "Lakehouse",
      properties = list(defaultSchema = default_schema)
    ),
    class = "fabric_item"
  )
}

lakehouse_table_test_response <- function(
  body = NULL,
  status = 200L,
  headers = list(),
  url = "https://onelake.table.fabric.microsoft.com/test"
) {
  headers[["content-type"]] <- "application/json"
  raw_body <- if (is.null(body)) {
    raw()
  } else {
    charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE, null = "null"))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = headers,
    body = raw_body
  )
}

lakehouse_table_test_clock <- function() {
  clock <- new.env(parent = emptyenv())
  clock$time <- as.POSIXct("2026-08-13 12:00:00", tz = "UTC")
  clock$delays <- numeric()
  list(
    now = function() clock$time,
    sleep = function(seconds) {
      clock$delays <- c(clock$delays, seconds)
      clock$time <- clock$time + seconds
    },
    delays = function() clock$delays
  )
}

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
        `fabric-3` = "éxport_数据",
        "orders"
      )
      suffix <- if (identical(name, "éxport_数据")) {
        paste("sales", name, sep = "/")
      } else {
        name
      }
      list(
        data = list(list(
          name = name,
          type = if (identical(name, "éxport_数据")) "External" else "Managed",
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
        name <- if (identical(schema, "sales")) "éxport_数据" else "customers"
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
        "éxport_数据"
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
  expect_setequal(tables$name, c("orders", "customers", "éxport_数据"))
  expect_setequal(tables$schema, c("dbo", "sales"))
  expect_setequal(
    tables$full_name,
    c(
      "dbo.orders",
      "dbo.customers",
      "sales.éxport_数据"
    )
  )
  expect_equal(tables$type[tables$name == "orders"], "Managed")
  expect_equal(tables$type[tables$name == "éxport_数据"], "External")
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
    token = "test-token"
  )

  expect_equal(tables$full_name, "sales.orders")
  expect_equal(tables$type, "Managed")
  expect_length(calls, 2L)
  expect_false(any(grepl("/schemas", calls, fixed = TRUE)))
  expect_length(tables$columns[[1L]], 0L)
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
    token = "test-token"
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
    path = "Files/incoming/café-数据.csv",
    format = "csv",
    mode = "append",
    header = FALSE,
    delimiter = ";",
    token = "test-token"
  )

  expect_s3_class(operation, "fabric_operation")
  expect_null(operation$result_url)
  expect_equal(operation$schema, "dbo")
  expect_equal(operation$source_path, "Files/incoming/café-数据.csv")
  expect_equal(operation$retry_after, 5)
  expect_equal(captured$method, "POST")
  expect_match(
    captured$url,
    "/schemas/dbo/tables/orders_2026/load[?]beta=true$"
  )
  expect_equal(
    captured$body$data,
    list(
      relativePath = "Files/incoming/café-数据.csv",
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
  expect_error(invoke(path = "Tables/orders.csv"), "must begin with Files/")
  expect_error(invoke(schema = "bad-name"), "schema must contain")
  expect_error(invoke(recursive = TRUE), "requires path_type")
  expect_error(invoke(format = "csv", delimiter = " "), "delimiter must")
  expect_error(
    invoke(path_type = "folder", file_extension = "this-extension-is-too-long"),
    "file_extension must"
  )
  expect_error(
    fabric_lakehouse_tables(
      lakehouse_table_test_item(),
      table_api_base = "https://attacker.example/delta",
      token = "test-token"
    ),
    class = "fabric_lakehouse_endpoint_error"
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
        data = as.data.frame(arrow::read_parquet(source))
      )
      tibble::tibble(path = target$path)
    },
    .fabric_lakehouse_load_submit = function(
      target,
      settings,
      credential,
      ...
    ) {
      expect_equal(settings$path, "Files/staging/load-fixed/orders.parquet")
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
  value <- data.frame(
    id = bit64::as.integer64(c("9007199254740993", NA)),
    café_数据 = c("één", NA),
    amount = c(10.5, NA),
    observed_on = as.Date(c("2026-01-01", NA)),
    kind = factor(c("a", "b")),
    stringsAsFactors = FALSE
  )

  result <- fabric_lakehouse_write_table(
    lakehouse_table_test_item(),
    table = "orders",
    data = value,
    staging_root = "Files/staging",
    token = "test-token"
  )

  expect_s3_class(result, "fabric_lakehouse_write_result")
  expect_equal(result$rows, 2L)
  expect_equal(result$schema, "dbo")
  expect_false(result$staging_retained)
  expect_equal(cleanup_calls, 1L)
  expect_equal(uploaded$target$path, result$staging_path)
  expect_equal(uploaded$data$kind, c("a", "b"))
  expect_equal(as.character(uploaded$data$id[[1L]]), "9007199254740993")
  expect_true(is.na(uploaded$data$café_数据[[2L]]))
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
      token = "test-token"
    ),
    class = "fabric_lakehouse_write_error"
  )
  expect_true(retained$staging_retained)
  expect_match(
    retained$staging_path,
    "Files/fabricqueryr-staging/load-failed/orders.parquet",
    fixed = TRUE
  )
  expect_equal(cleanup_calls, 0L)

  removed <- expect_error(
    fabric_lakehouse_write_table(
      lakehouse_table_test_item(),
      table = "orders",
      data = data.frame(id = 1L),
      keep_staging_on_failure = FALSE,
      token = "test-token"
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
      token = "test-token"
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
