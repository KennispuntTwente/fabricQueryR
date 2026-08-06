# pbi_parse_connstr() -----------------------------------------------------

pbi_test_workspace_id <- "11111111-1111-4111-8111-111111111111"
pbi_test_dataset_id <- "22222222-2222-4222-8222-222222222222"

test_that("pbi_parse_connstr parses full conn str", {
  conn <- "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace%20Name;Initial Catalog=Dataset One;"
  p <- fabricQueryR:::pbi_parse_connstr(conn)
  expect_type(p, "list")
  expect_equal(
    p$server,
    "powerbi://api.powerbi.com/v1.0/myorg/Workspace%20Name"
  )
  expect_equal(p$workspace, "Workspace Name")
  expect_equal(p$dataset, "Dataset One")
})

test_that("pbi_parse_connstr supports bare powerbi:// and Catalog alias", {
  conn <- "powerbi://api.powerbi.com/v1.0/myorg/Another%20WS/;Catalog=MyData;"
  p <- fabricQueryR:::pbi_parse_connstr(conn)
  expect_equal(p$workspace, "Another WS")
  expect_equal(p$dataset, "MyData")
})

test_that("pbi_parse_connstr preserves quoted dataset delimiters", {
  quoted <- pbi_parse_connstr(paste0(
    "Data Source=\"powerbi://api.powerbi.com/v1.0/myorg/Workspace\";",
    "Initial Catalog=\"Dataset; \"\"North\"\"\";"
  ))
  expect_equal(quoted$workspace, "Workspace")
  expect_equal(quoted$dataset, 'Dataset; "North"')

  braced <- pbi_parse_connstr(paste0(
    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;",
    "Catalog={Dataset;East};"
  ))
  expect_equal(braced$dataset, "Dataset;East")
})

test_that("pbi_parse_connstr rejects incomplete and non-Power-BI strings", {
  expect_error(
    fabricQueryR:::pbi_parse_connstr("Initial Catalog=OnlyDataset;"),
    "unique Data Source"
  )
  expect_error(
    fabricQueryR:::pbi_parse_connstr(
      "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;"
    ),
    "exactly one non-empty"
  )
  expect_error(
    fabricQueryR:::pbi_parse_connstr(
      paste0(
        "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;",
        "Catalog=One;Dataset=Two;"
      )
    ),
    "exactly one non-empty"
  )
  expect_error(
    fabricQueryR:::pbi_parse_connstr(
      "Data Source=https://api.powerbi.com/Workspace;Catalog=Dataset;"
    ),
    "Power BI XMLA URL"
  )
})

test_that("fabric_pbi_dax_query validates include_nulls strictly", {
  for (value in list(NA, 1, "true", logical(), c(TRUE, FALSE))) {
    expect_error(
      fabric_pbi_dax_query(
        dax = "EVALUATE ROW(\"value\", 1)",
        dataset_id = "dataset-id",
        include_nulls = value,
        token = "token"
      ),
      "include_nulls must be TRUE or FALSE",
      fixed = TRUE
    )
  }
})

test_that("Arrow DAX options and result combinations validate strictly", {
  expect_equal(
    pbi_validate_arrow_options(list(
      culture = "en-US",
      queryTimeout = 300,
      resultSetRowCountLimit = 100000,
      roles = c("Sales", "Auditor"),
      schemaOnly = FALSE
    )),
    list(
      culture = "en-US",
      queryTimeout = 300,
      resultSetRowCountLimit = 100000,
      roles = c("Sales", "Auditor"),
      schemaOnly = FALSE
    )
  )
  expect_error(
    pbi_validate_arrow_options(list(query = "EVALUATE ROW()")),
    "Unsupported arrow_options name(s): query",
    fixed = TRUE
  )
  expect_error(
    pbi_validate_arrow_options(list(queryTimeout = 1.5)),
    "positive integer",
    fixed = TRUE
  )
  expect_error(
    pbi_validate_arrow_options(list(schemaOnly = NA)),
    "TRUE or FALSE",
    fixed = TRUE
  )
  expect_error(
    pbi_validate_arrow_options(list(executionMetrics = TRUE)),
    "Unsupported arrow_options name(s): executionMetrics",
    fixed = TRUE
  )
  expect_error(
    fabric_pbi_dax_query(
      dax = "EVALUATE ROW()",
      dataset_id = pbi_test_dataset_id,
      token = "token",
      arrow_options = list(culture = "en-US")
    ),
    "api = \"arrow\"",
    fixed = TRUE
  )
  expect_error(
    fabric_pbi_dax_query(
      dax = "EVALUATE ROW()",
      dataset_id = pbi_test_dataset_id,
      token = "token",
      result = "arrow_stream"
    ),
    "requires api = \"arrow\"",
    fixed = TRUE
  )
})


# pbi_resolve_ids_from_connstr() ------------------------------------------

test_that("pbi_resolve_ids_from_connstr wires through to GUID lookups", {
  fake_credential <- fabric_credential(token = "tok")
  conn <- "Data Source=powerbi://api.powerbi.com/v1.0/myorg/WS;Initial Catalog=DS;"

  got_group <- NULL
  got_dataset <- NULL

  testthat::with_mocked_bindings(
    pbi_get_group_id_by_name = function(
      credential,
      workspace_name,
      api_base
    ) {
      expect_identical(credential, fake_credential)
      expect_equal(workspace_name, "WS")
      expect_match(api_base, "api.powerbi.com")
      got_group <<- TRUE
      "11111111-1111-1111-1111-111111111111"
    },
    pbi_get_dataset_id_by_name = function(
      credential,
      group_id,
      dataset_name,
      api_base
    ) {
      expect_identical(credential, fake_credential)
      expect_equal(group_id, "11111111-1111-1111-1111-111111111111")
      expect_equal(dataset_name, "DS")
      expect_match(api_base, "api.powerbi.com")
      got_dataset <<- TRUE
      "22222222-2222-2222-2222-222222222222"
    },
    {
      ids <- fabricQueryR:::pbi_resolve_ids_from_connstr(
        conn,
        credential = fake_credential
      )
      expect_true(got_group)
      expect_true(got_dataset)
      expect_equal(ids$group_id, "11111111-1111-1111-1111-111111111111")
      expect_equal(ids$dataset_id, "22222222-2222-2222-2222-222222222222")
      expect_equal(ids$workspace, "WS")
      expect_equal(ids$dataset, "DS")
    }
  )
})

test_that("fabric_pbi_dax_query uses a supplied access token", {
  token_requested <- FALSE

  testthat::with_mocked_bindings(
    pbi_get_token = function(...) {
      token_requested <<- TRUE
      "unexpected-token"
    },
    pbi_resolve_ids_from_connstr = function(
      connstr,
      credential,
      api_base
    ) {
      expect_equal(
        fabric_get_token(credential, .fabric_audience$power_bi),
        "supplied-token"
      )
      list(
        group_id = pbi_test_workspace_id,
        dataset_id = pbi_test_dataset_id
      )
    },
    pbi_execute_dax = function(
      credential,
      dataset_id,
      dax,
      group_id,
      include_nulls,
      api_base,
      impersonated_user
    ) {
      expect_equal(
        fabric_get_token(credential, .fabric_audience$power_bi),
        "supplied-token"
      )
      expect_equal(dataset_id, pbi_test_dataset_id)
      expect_equal(group_id, pbi_test_workspace_id)
      expect_null(impersonated_user)
      tibble::tibble(result = 3L)
    },
    {
      result <- fabric_pbi_dax_query(
        connstr = paste0(
          "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;",
          "Initial Catalog=Model;"
        ),
        dax = 'EVALUATE ROW("result", 3)',
        tenant_id = "",
        client_id = "",
        token = "supplied-token"
      )
    }
  )

  expect_false(token_requested)
  expect_equal(result$result, 3L)
})

test_that("fabric_pbi_dax_query accepts direct IDs without name lookup", {
  looked_up <- FALSE
  local_mocked_bindings(
    pbi_resolve_ids_from_connstr = function(...) {
      looked_up <<- TRUE
      rlang::abort("unexpected lookup")
    },
    pbi_execute_dax = function(
      credential,
      dataset_id,
      dax,
      group_id,
      include_nulls,
      api_base,
      impersonated_user
    ) {
      expect_equal(
        fabric_get_token(credential, .fabric_audience$power_bi),
        "token"
      )
      expect_equal(dataset_id, pbi_test_dataset_id)
      expect_equal(group_id, pbi_test_workspace_id)
      expect_equal(impersonated_user, "reader@example.com")
      tibble::tibble(value = 42L)
    }
  )

  result <- fabric_pbi_dax_query(
    dax = 'EVALUATE ROW("value", 42)',
    workspace_id = pbi_test_workspace_id,
    dataset_id = pbi_test_dataset_id,
    token = "token",
    impersonated_user = "reader@example.com"
  )

  expect_false(looked_up)
  expect_equal(result$value, 42L)
  expect_error(
    fabric_pbi_dax_query(dax = "EVALUATE ROW()", token = "token"),
    "Supply either connstr or dataset_id",
    fixed = TRUE
  )
  expect_error(
    fabric_pbi_dax_query(
      dax = "EVALUATE ROW()",
      workspace_id = "not-a-guid",
      dataset_id = pbi_test_dataset_id,
      token = "token"
    ),
    "workspace_id must be a GUID",
    fixed = TRUE
  )
  expect_error(
    fabric_pbi_dax_query(
      dax = "EVALUATE ROW()",
      dataset_id = "not-a-guid",
      token = "token"
    ),
    "dataset_id must be a GUID",
    fixed = TRUE
  )
})

test_that("fabric_pbi_dax_query forwards Arrow mode and effective identity", {
  skip_if_not_installed("arrow")
  called <- FALSE
  local_mocked_bindings(
    pbi_execute_dax_arrow = function(
      credential,
      dataset_id,
      dax,
      group_id,
      api_base,
      options,
      result
    ) {
      called <<- TRUE
      expect_equal(
        fabric_get_token(credential, .fabric_audience$power_bi),
        "token"
      )
      expect_equal(dataset_id, pbi_test_dataset_id)
      expect_equal(group_id, pbi_test_workspace_id)
      expect_equal(dax, "EVALUATE ROW(\"value\", 1)")
      expect_equal(options$effectiveUsername, "reader@example.com")
      expect_equal(options$queryTimeout, 30)
      expect_equal(result, "tibble")
      tibble::tibble(value = 1L)
    }
  )

  result <- fabric_pbi_dax_query(
    dax = "EVALUATE ROW(\"value\", 1)",
    workspace_id = pbi_test_workspace_id,
    dataset_id = pbi_test_dataset_id,
    token = "token",
    impersonated_user = "reader@example.com",
    api = "arrow",
    arrow_options = list(queryTimeout = 30)
  )

  expect_true(called)
  expect_equal(result$value, 1L)
})

test_that("DAX response parser preserves names, nulls, and empty results", {
  parsed <- pbi_parse_dax_response(list(
    results = list(list(
      tables = list(list(
        rows = list(
          list("Facts[id]" = 1L, "[amount]" = 10.5),
          list("Facts[id]" = 2L, "[amount]" = NULL)
        )
      ))
    ))
  ))

  expect_s3_class(parsed, "tbl_df")
  expect_named(parsed, c("Facts[id]", "[amount]"))
  expect_equal(parsed[["Facts[id]"]], c(1L, 2L))
  expect_equal(parsed[["[amount]"]], c(10.5, NA))
  expect_equal(pbi_parse_dax_response(list(results = list())), tibble::tibble())
  expect_equal(
    pbi_parse_dax_response(list(results = list(list(tables = list())))),
    tibble::tibble()
  )
})

test_that("DAX response parser raises every embedded error level", {
  expect_error(
    pbi_parse_dax_response(list(
      error = list(code = "BadRequest", message = "invalid payload")
    )),
    "DAX response failed: BadRequest: invalid payload",
    fixed = TRUE
  )
  expect_error(
    pbi_parse_dax_response(list(
      results = list(list(
        error = list(
          code = "PartialResult",
          message = "More than 100000 rows in a query result"
        ),
        tables = list(list(rows = list(list(x = 1L))))
      ))
    )),
    "incomplete DAX query result",
    fixed = TRUE
  )
  expect_error(
    pbi_parse_dax_response(list(
      results = list(list(
        tables = list(list(
          error = list(message = "15 MB response size limit exceeded"),
          rows = list(list(x = 1L))
        ))
      ))
    )),
    "Reduce the selected rows/columns",
    fixed = TRUE
  )
})

test_that("DAX response parser rejects unsupported multiplicity", {
  expect_error(
    pbi_parse_dax_response(list(
      results = list(
        list(tables = list()),
        list(tables = list())
      )
    )),
    "2 query results",
    fixed = TRUE
  )
  expect_error(
    pbi_parse_dax_response(list(
      results = list(list(
        tables = list(list(rows = list()), list(rows = list()))
      ))
    )),
    "2 result tables",
    fixed = TRUE
  )
})

test_that("DAX execution sends impersonation and parses one table", {
  local_mocked_bindings(
    .httr2_json = function(req, simplifyVector, bigint_as_char, ...) {
      expect_false(simplifyVector)
      expect_true(bigint_as_char)
      body <- req$body$data
      expect_equal(body$impersonatedUserName, "reader@example.com")
      expect_true(body$serializerSettings$includeNulls)
      list(
        results = list(list(
          tables = list(list(rows = list(list("[value]" = 7L))))
        ))
      )
    }
  )

  result <- pbi_execute_dax(
    credential = fabric_credential(token = "token"),
    dataset_id = "dataset",
    group_id = "workspace",
    dax = 'EVALUATE ROW("value", 7)',
    impersonated_user = "reader@example.com"
  )
  expect_equal(result[["[value]"]], 7L)
})

test_that("pbi_parse_connstr supports v2 personal-workspace XMLA URLs", {
  parsed <- pbi_parse_connstr(paste0(
    "Data Source=powerbi://api.powerbi.com/v2.0/",
    "11111111-1111-4111-8111-111111111111/home/myworkspace/",
    "owner%40example.com;Initial Catalog=Personal Model;"
  ))

  expect_true(parsed$personal)
  expect_identical(parsed$workspace, "My Workspace")
  expect_identical(parsed$tenant_id, pbi_test_workspace_id)
  expect_identical(parsed$owner, "owner@example.com")
  expect_identical(parsed$dataset, "Personal Model")
})

test_that("unscoped DAX endpoints require explicit My Workspace mode", {
  expect_error(
    fabric_pbi_dax_query(
      dax = "EVALUATE ROW()",
      dataset_id = pbi_test_dataset_id,
      token = "token"
    ),
    "workspace_id or explicit my_workspace = TRUE",
    fixed = TRUE
  )
  expect_error(
    fabric_pbi_dax_query(
      dax = "EVALUATE ROW()",
      workspace_id = pbi_test_workspace_id,
      dataset_id = pbi_test_dataset_id,
      my_workspace = TRUE,
      token = "token"
    ),
    "not both",
    fixed = TRUE
  )

  local_mocked_bindings(
    pbi_execute_dax = function(group_id, ...) {
      expect_null(group_id)
      tibble::tibble(value = 1L)
    }
  )
  result <- fabric_pbi_dax_query(
    dax = "EVALUATE ROW()",
    dataset_id = pbi_test_dataset_id,
    my_workspace = TRUE,
    token = "token"
  )
  expect_identical(result$value, 1L)
})

test_that("DAX execution preserves JSON whole numbers outside 2^53", {
  local_mocked_bindings(
    .httr2_json = function(req, bigint_as_char, ...) {
      expect_true(bigint_as_char)
      list(
        results = list(list(
          tables = list(list(
            rows = list(list(
              positive = "9007199254740993",
              negative = "-9007199254740993",
              fixed_decimal = 123.45,
              date = "2026-08-06T00:00:00"
            ))
          ))
        ))
      )
    }
  )

  result <- pbi_execute_dax(
    credential = fabric_credential(token = "token"),
    dataset_id = "dataset",
    dax = "EVALUATE ROW()"
  )

  expect_identical(result$positive, "9007199254740993")
  expect_identical(result$negative, "-9007199254740993")
  expect_identical(result$fixed_decimal, 123.45)
  expect_identical(result$date, "2026-08-06T00:00:00")
})

test_that("Arrow DAX execution sends documented endpoint and request body", {
  skip_if_not_installed("arrow")
  payload <- jsonlite::base64_dec(paste0(
    "/////3gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAI",
    "AAAABAAAAAEAAAAUAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAECEAAAABwAA",
    "AAEAAAAAAAAAAEAAAB4AAAACAAMAAgABwAIAAAAAAAAAUAAAAD/////mAAAAB",
    "QAAAAAAAAADAAYAAYABQAIAAwADAAAAAADBAAcAAAAMAAAAAAAAAAAAAAADAA",
    "cABAABAAIAAwADAAAAEgAAAAcAAAAFAAAAAMAAAAAAAAAAAAAAAQABAAEAAAA",
    "AgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACoAAAAAAAAAAAAAAAEAAAADAA",
    "AAAAAAAAAAAAAAAAAAGAAAAAAAAAAEIk0YYECCEwAAACIBAAEAEgIHAJAAAwA",
    "AAAAAAAAAAAAAAAAAAAAA/////wAAAAA="
  ))
  local_mocked_bindings(
    .httr2_perform = function(
      req,
      credential,
      audience,
      idempotent,
      download_path
    ) {
      expect_match(
        req$url,
        "/groups/workspace/datasets/dataset/executeDaxQueries$"
      )
      expect_true(idempotent)
      expect_identical(audience, .fabric_audience$power_bi)
      expect_equal(
        req$headers$Accept,
        "application/vnd.apache.arrow.stream"
      )
      expect_equal(req$body$data$query, "EVALUATE ROW(\"value\", 1)")
      expect_equal(req$body$data$culture, "en-US")
      expect_equal(req$body$data$queryTimeout, 60)
      expect_s3_class(req$body$data$roles, "AsIs")
      encoded <- jsonlite::toJSON(
        req$body$data,
        auto_unbox = req$body$params$auto_unbox,
        null = req$body$params$null
      )
      expect_match(encoded, '"roles":\\["Sales"\\]')
      writeBin(payload, download_path)
      httr2::new_response(
        method = "POST",
        url = req$url,
        status_code = 200L,
        headers = list(
          "content-type" = "application/vnd.apache.arrow.stream"
        ),
        body = raw(),
        request = req
      )
    }
  )

  result <- pbi_execute_dax_arrow(
    credential = fabric_credential(token = "token"),
    dataset_id = "dataset",
    dax = "EVALUATE ROW(\"value\", 1)",
    group_id = "workspace",
    options = list(culture = "en-US", queryTimeout = 60, roles = "Sales")
  )

  expect_equal(result$x, bit64::as.integer64(1:3))
})

test_that("Arrow DAX parser handles LZ4 and Arrow C stream compatibility", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  payload <- jsonlite::base64_dec(paste0(
    "/////3gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAI",
    "AAAABAAAAAEAAAAUAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAECEAAAABwAA",
    "AAEAAAAAAAAAAEAAAB4AAAACAAMAAgABwAIAAAAAAAAAUAAAAD/////mAAAAB",
    "QAAAAAAAAADAAYAAYABQAIAAwADAAAAAADBAAcAAAAMAAAAAAAAAAAAAAADAA",
    "cABAABAAIAAwADAAAAEgAAAAcAAAAFAAAAAMAAAAAAAAAAAAAAAQABAAEAAAA",
    "AgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACoAAAAAAAAAAAAAAAEAAAADAA",
    "AAAAAAAAAAAAAAAAAAGAAAAAAAAAAEIk0YYECCEwAAACIBAAEAEgIHAJAAAwA",
    "AAAAAAAAAAAAAAAAAAAAA/////wAAAAA="
  ))

  stream <- pbi_parse_dax_arrow_response(payload, "arrow_stream")
  expect_s3_class(stream, "nanoarrow_array_stream")
  reader <- arrow::as_record_batch_reader(stream)
  expect_equal(
    as.data.frame(reader$read_table())$x,
    bit64::as.integer64(1:3)
  )
})

test_that("Arrow DAX parser decodes tables and streams native dictionaries", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  name <- arrow::DictionaryArray$create(
    c(0L, 1L, 2L),
    c("alpha", "beta", "gamma")
  )
  amount <- arrow::DictionaryArray$create(
    c(0L, 1L, NA_integer_),
    c(10.5, 20)
  )
  table <- arrow::arrow_table(name = name, amount = amount)
  path <- tempfile(fileext = ".arrows")
  on.exit(unlink(path), add = TRUE)
  arrow::write_ipc_stream(table, path)
  payload <- readBin(path, "raw", n = file.info(path)$size)

  result <- pbi_parse_dax_arrow_response(payload)
  stream <- pbi_parse_dax_arrow_response(payload, "arrow_stream")
  streamed <- arrow::as_record_batch_reader(stream)$read_table()
  streamed <- suppressWarnings(as.data.frame(streamed))

  expect_equal(result$name, c("alpha", "beta", "gamma"))
  expect_equal(result$amount, c(10.5, 20, NA))
  expect_equal(as.character(streamed$name), c("alpha", "beta", "gamma"))
  expect_equal(
    as.numeric(as.character(streamed$amount)),
    c(10.5, 20, NA)
  )
})

test_that("Arrow DAX responses can be parsed from disk", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  path <- tempfile(fileext = ".arrows")
  on.exit(unlink(path), add = TRUE)
  arrow::write_ipc_stream(
    arrow::Table$create(data.frame(value = 1:3)),
    path
  )

  result <- pbi_parse_dax_arrow_response(path)
  stream <- pbi_parse_dax_arrow_response(path, "arrow_stream")
  streamed <- arrow::as_record_batch_reader(stream)$read_table()

  expect_equal(result$value, 1:3)
  expect_equal(as.data.frame(streamed)$value, 1:3)
})

test_that("Arrow DAX stream remains file-backed without collecting its table", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  payload <- arrow::BufferOutputStream$create()
  writer <- arrow::RecordBatchStreamWriter$create(
    payload,
    arrow::schema(value = arrow::int32())
  )
  for (index in seq_len(10L)) {
    writer$write_batch(arrow::record_batch(
      value = seq.int((index - 1L) * 100L + 1L, index * 100L)
    ))
  }
  writer$close()
  bytes <- payload$finish()$data()

  local_mocked_bindings(
    .httr2_perform = function(req, download_path, ...) {
      writeBin(bytes, download_path)
      httr2::new_response(
        method = "POST",
        url = req$url,
        status_code = 200L,
        headers = list(),
        body = raw(),
        request = req
      )
    }
  )

  stream <- pbi_execute_dax_arrow(
    credential = fabric_credential(token = "token"),
    dataset_id = "dataset",
    dax = "EVALUATE ROW()",
    group_id = "workspace",
    result = "arrow_stream"
  )
  resource <- attr(stream, "fabric_dax_resource")

  expect_s3_class(stream, "nanoarrow_array_stream")
  expect_true(file.exists(resource$path))
  expect_s3_class(resource$reader, "RecordBatchReader")
  reader <- arrow::as_record_batch_reader(stream)
  expect_equal(nrow(reader$read_table()), 1000L)
})

test_that("Arrow DAX parser rejects error and multiple data rowsets", {
  skip_if_not_installed("arrow")
  arrow_payload <- function(data, metadata = list()) {
    table <- arrow::arrow_table(data)
    if (length(metadata)) {
      table <- table$ReplaceSchemaMetadata(metadata)
    }
    path <- tempfile(fileext = ".arrows")
    on.exit(unlink(path), add = TRUE)
    arrow::write_ipc_stream(table, path)
    readBin(path, "raw", n = file.info(path)$size)
  }
  error_payload <- arrow_payload(
    data.frame(
      ErrorCode = "QueryError",
      ErrorMessage = "Invalid DAX"
    ),
    list(
      IsError = "true",
      FaultCode = "0xC1210001",
      FaultString = "The query is invalid"
    )
  )
  expect_error(
    pbi_parse_dax_arrow_response(error_payload),
    "Power BI Arrow DAX query failed: [0xC1210001] The query is invalid",
    fixed = TRUE,
    class = "fabric_pbi_dax_error"
  )

  multiple <- c(
    arrow_payload(data.frame(first = 1L)),
    arrow_payload(data.frame(second = 2L))
  )
  expect_error(
    pbi_parse_dax_arrow_response(multiple),
    "2 Arrow DAX data rowsets",
    fixed = TRUE
  )
  expect_error(
    pbi_parse_dax_arrow_response(raw()),
    "empty Arrow DAX response",
    fixed = TRUE
  )
})

test_that("Arrow DAX parser separates execution metrics from data", {
  skip_if_not_installed("arrow")
  arrow_payload <- function(data, metadata = list()) {
    table <- arrow::arrow_table(data)
    if (length(metadata)) {
      table <- table$ReplaceSchemaMetadata(metadata)
    }
    path <- tempfile(fileext = ".arrows")
    on.exit(unlink(path), add = TRUE)
    arrow::write_ipc_stream(table, path)
    readBin(path, "raw", n = file.info(path)$size)
  }
  payload <- c(
    arrow_payload(data.frame(value = 42L)),
    arrow_payload(
      data.frame(durationMs = 12L, rowsReturned = 1L),
      list(IsExecMetrics = "true")
    )
  )

  result <- pbi_parse_dax_arrow_response(payload)
  expect_equal(result$value, 42L)
  metrics <- attr(result, "execution_metrics")
  expect_s3_class(metrics, "tbl_df")
  expect_equal(metrics$durationMs, 12L)
  expect_equal(metrics$rowsReturned, 1L)
})

test_that("Power BI collection paging follows offsets and next links", {
  calls <- list()
  responses <- list(
    list(value = list(list(id = "one"), list(id = "two"))),
    list(value = list(list(id = "three")))
  )
  local_mocked_bindings(
    .httr2_json = function(req, simplifyVector, ...) {
      calls[[length(calls) + 1L]] <<- req$url
      responses[[length(calls)]]
    }
  )

  values <- pbi_get_collection(
    "https://example.test/groups",
    "token",
    offset_pagination = TRUE,
    page_size = 2L
  )
  expect_equal(
    vapply(values, `[[`, character(1), "id"),
    c("one", "two", "three")
  )
  expect_match(calls[[1]], "%24top=2")
  expect_match(calls[[1]], "%24skip=0")
  expect_match(calls[[2]], "%24skip=2")

  calls <- list()
  responses <- list(
    list(
      value = list(list(id = "one")),
      "@odata.nextLink" = "https://example.test/groups?page=2"
    ),
    list(value = list(list(id = "two")))
  )
  values <- pbi_get_collection("https://example.test/groups", "token")
  expect_equal(vapply(values, `[[`, character(1), "id"), c("one", "two"))
  expect_equal(calls[[2]], "https://example.test/groups?page=2")
})

test_that("Power BI name lookup rejects ambiguous case-insensitive names", {
  local_mocked_bindings(
    pbi_get_collection = function(...) {
      list(
        list(id = "one", name = "Sales"),
        list(id = "two", name = "SALES")
      )
    }
  )
  expect_error(
    pbi_get_group_id_by_name("token", "sales"),
    "ambiguous",
    fixed = TRUE
  )
  expect_error(
    pbi_get_dataset_id_by_name("token", "workspace", "sales"),
    "Use dataset_id",
    fixed = TRUE
  )
})

test_that("personal XMLA resolution uses the unscoped dataset collection", {
  urls <- character()
  local_mocked_bindings(
    pbi_get_collection = function(url, ...) {
      urls <<- c(urls, url)
      list(list(id = pbi_test_dataset_id, name = "Personal Model"))
    }
  )

  resolved <- pbi_resolve_ids_from_connstr(
    paste0(
      "Data Source=powerbi://api.powerbi.com/v2.0/",
      pbi_test_workspace_id,
      "/home/myworkspace/owner%40example.com;",
      "Initial Catalog=Personal Model;"
    ),
    credential = fabric_credential(token = "token")
  )

  expect_null(resolved$group_id)
  expect_true(resolved$personal)
  expect_match(urls, "/datasets$")
  expect_false(grepl("/groups/", urls, fixed = TRUE))
})
