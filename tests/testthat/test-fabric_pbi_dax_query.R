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
      executionMetrics = TRUE,
      schemaOnly = FALSE
    )),
    list(
      culture = "en-US",
      queryTimeout = 300,
      resultSetRowCountLimit = 100000,
      roles = c("Sales", "Auditor"),
      executionMetrics = TRUE,
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
    pbi_validate_arrow_options(list(executionMetrics = NA)),
    "TRUE or FALSE",
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
  testthat::with_mocked_bindings(
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
      timeout,
      api_base,
      impersonated_user
    ) {
      expect_equal(
        fabric_get_token(credential, .fabric_audience$power_bi),
        "supplied-token"
      )
      expect_equal(dataset_id, pbi_test_dataset_id)
      expect_equal(group_id, pbi_test_workspace_id)
      expect_equal(timeout, 300)
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
      timeout,
      api_base,
      impersonated_user
    ) {
      expect_equal(
        fabric_get_token(credential, .fabric_audience$power_bi),
        "token"
      )
      expect_equal(dataset_id, pbi_test_dataset_id)
      expect_equal(group_id, pbi_test_workspace_id)
      expect_equal(timeout, 17)
      expect_equal(impersonated_user, "reader@example.com")
      tibble::tibble(value = 42L)
    }
  )

  result <- fabric_pbi_dax_query(
    dax = 'EVALUATE ROW("value", 42)',
    workspace_id = pbi_test_workspace_id,
    dataset_id = pbi_test_dataset_id,
    token = "token",
    timeout = 17,
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
      timeout,
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
      expect_equal(timeout, 19)
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
    timeout = 19,
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

test_that("DAX target selectors cannot silently override each other", {
  connstr <- paste0(
    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;",
    "Initial Catalog=Model;"
  )
  expect_error(
    fabric_pbi_dax_query(
      connstr = connstr,
      dax = "EVALUATE ROW()",
      workspace_id = pbi_test_workspace_id,
      dataset_id = pbi_test_dataset_id,
      token = "token"
    ),
    "not both",
    class = "fabric_pbi_target_conflict"
  )
  expect_error(
    fabric_pbi_dax_query(
      connstr = "malformed and previously ignored",
      dax = "EVALUATE ROW()",
      dataset_id = pbi_test_dataset_id,
      my_workspace = TRUE,
      token = "token"
    ),
    "not both",
    class = "fabric_pbi_target_conflict"
  )

  discovered <- list(
    id = pbi_test_dataset_id,
    workspaceId = pbi_test_workspace_id,
    type = "SemanticModel",
    dax_connection_string = connstr
  )
  expect_error(
    fabric_pbi_dax_query(
      connstr = discovered,
      dax = "EVALUATE ROW()",
      dataset_id = "33333333-3333-4333-8333-333333333333",
      token = "token"
    ),
    "conflicts with the discovered SemanticModel id",
    class = "fabric_pbi_target_conflict"
  )
})

test_that("DAX client timeout is positive and validated before authentication", {
  invalid <- list(NULL, 0, -1, NA_real_, Inf, c(1, 2), "30")
  for (timeout in invalid) {
    error <- rlang::catch_cnd(
      fabric_pbi_dax_query(
        dax = "EVALUATE ROW()",
        workspace_id = pbi_test_workspace_id,
        dataset_id = pbi_test_dataset_id,
        timeout = timeout,
        token = "unsafe\ncredential"
      ),
      classes = "error"
    )
    expect_match(
      conditionMessage(error),
      "timeout must be one positive finite number of seconds",
      fixed = TRUE
    )
  }
})

test_that("DAX response parser promotes mixed-size Whole Numbers", {
  parsed <- pbi_parse_dax_response(list(
    results = list(list(
      tables = list(list(
        rows = list(
          list(x = 1L, y = -2L),
          list(x = "9007199254740993", y = NULL),
          list(y = "-9007199254740993"),
          list(x = NULL, y = 3L)
        )
      ))
    ))
  ))

  expect_identical(
    parsed$x,
    c("1", "9007199254740993", NA_character_, NA_character_)
  )
  expect_identical(
    parsed$y,
    c("-2", NA_character_, "-9007199254740993", "3")
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
      expect_equal(req$options$timeout_ms, 17000)
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
    dax = "EVALUATE ROW()",
    timeout = 17
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
      download_path,
      request_timeout
    ) {
      expect_match(
        req$url,
        "/groups/workspace/datasets/dataset/executeDaxQueries$"
      )
      expect_true(idempotent)
      expect_equal(request_timeout, 17)
      expect_identical(audience, .fabric_audience$power_bi)
      expect_equal(
        req$headers$Accept,
        "application/vnd.apache.arrow.stream"
      )
      expect_equal(req$body$data$query, "EVALUATE ROW(\"value\", 1)")
      expect_equal(req$body$data$culture, "en-US")
      expect_equal(req$body$data$queryTimeout, 60)
      expect_equal(req$options$timeout_ms, 17000)
      expect_true(req$body$data$executionMetrics)
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
    timeout = 17,
    options = list(
      culture = "en-US",
      queryTimeout = 60,
      executionMetrics = TRUE,
      roles = "Sales"
    )
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

test_that("Arrow DAX tibbles preserve decimal values exactly", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  expected <- c("123456789012345.6789", "-0.0100", NA_character_)
  currency <- arrow::Array$create(expected)$cast(arrow::decimal128(19, 4))
  table <- arrow::arrow_table(currency = currency)
  path <- tempfile(fileext = ".arrows")
  on.exit(unlink(path), add = TRUE)
  arrow::write_ipc_stream(table, path)
  payload <- readBin(path, "raw", n = file.info(path)$size)

  result <- pbi_parse_dax_arrow_response(payload)
  stream <- pbi_parse_dax_arrow_response(payload, "arrow_stream")
  streamed <- arrow::as_record_batch_reader(stream)$read_table()

  expect_identical(result$currency, expected)
  expect_s3_class(streamed$schema$fields[[1L]]$type, "Decimal128Type")
})

pbi_test_dense_union <- function(
  type_ids,
  offsets,
  children,
  child_types,
  type_codes = seq_along(children) - 1L,
  as_arrow = TRUE,
  validate = TRUE
) {
  schema <- nanoarrow::na_dense_union(child_types)
  schema$format <- paste0("+ud:", paste(type_codes, collapse = ","))
  type_buffer <- nanoarrow::nanoarrow_buffer_init()
  nanoarrow::nanoarrow_buffer_append(type_buffer, as.raw(type_ids))
  offset_buffer <- nanoarrow::nanoarrow_buffer_init()
  nanoarrow::nanoarrow_buffer_append(offset_buffer, as.integer(offsets))
  child_arrays <- Map(
    function(child, type) {
      if (inherits(child, "ArrowObject")) {
        return(nanoarrow::as_nanoarrow_array(child))
      }
      nanoarrow::as_nanoarrow_array(child, schema = type)
    },
    children,
    child_types
  )
  array <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(schema),
    list(
      length = length(type_ids),
      buffers = list(type_buffer, offset_buffer),
      children = child_arrays
    ),
    validate = validate
  )
  if (as_arrow) {
    return(arrow::as_arrow_array(array))
  }
  list(array = array, schema = schema)
}

test_that("Arrow DAX Variant covers every documented scalar branch exactly", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  type_names <- c(
    "integer",
    "currency",
    "logical",
    "date",
    "double",
    "string"
  )
  type_codes <- c(9L, 2L, 127L, 44L, 31L, 70L)
  integer <- arrow::as_arrow_array(c(
    "9223372036854775807",
    "-42",
    "-9223372036854775808"
  ))$cast(arrow::int64())
  currency <- arrow::as_arrow_array(c(
    "123456789012345.6789",
    "-0.0100",
    NA_character_
  ))$cast(arrow::decimal128(19, 4))
  children <- list(
    integer = integer,
    currency = currency,
    logical = c(TRUE, FALSE, NA),
    date = as.Date(c("1970-01-01", "2024-02-29", NA)),
    double = arrow::as_arrow_array(c(NaN, Inf, -0, -Inf, NA_real_)),
    string = c("", "caf\u00e9 \u6570\u636e", NA_character_)
  )
  child_types <- list(
    integer = nanoarrow::na_int64(),
    currency = nanoarrow::na_decimal128(19, 4),
    logical = nanoarrow::na_bool(),
    date = nanoarrow::na_date64(),
    double = nanoarrow::na_double(),
    string = nanoarrow::na_string()
  )
  type_ids <- c(rep(type_codes, 3L), type_codes[[5L]], type_codes[[5L]])
  offsets <- c(rep(0:2, each = 6L), 3L, 4L)
  array <- pbi_test_dense_union(
    type_ids,
    offsets,
    children,
    child_types,
    type_codes
  )
  variant <- arrow::chunked_array(
    array$Slice(0L, 7L),
    array$Slice(7L, length(type_ids) - 7L)
  )
  table <- arrow::arrow_table(variant = variant)
  path <- tempfile(fileext = ".arrows")
  on.exit(unlink(path), add = TRUE)
  arrow::write_ipc_stream(table, path)

  result <- pbi_parse_dax_arrow_response(path)

  expect_length(result$variant, length(type_ids))
  expect_identical(
    vapply(result$variant, `[[`, character(1), "type"),
    c(rep(type_names, 3L), "double", "double")
  )
  expect_identical(
    vapply(result$variant, inherits, logical(1), "fabric_pbi_variant"),
    rep(TRUE, length(type_ids))
  )
  expect_identical(
    result$variant[[1L]]$value,
    bit64::as.integer64("9223372036854775807")
  )
  expect_identical(result$variant[[7L]]$value, bit64::as.integer64(-42))
  expect_identical(result$variant[[13L]]$value, "-9223372036854775808")
  expect_identical(result$variant[[2L]]$value, "123456789012345.6789")
  expect_identical(result$variant[[8L]]$value, "-0.0100")
  expect_identical(result$variant[[14L]]$value, NA_character_)
  expect_identical(result$variant[[3L]]$value, TRUE)
  expect_identical(result$variant[[9L]]$value, FALSE)
  expect_identical(result$variant[[15L]]$value, NA)
  expect_identical(
    as.Date(result$variant[[4L]]$value, tz = "UTC"),
    as.Date("1970-01-01")
  )
  expect_identical(
    as.Date(result$variant[[10L]]$value, tz = "UTC"),
    as.Date("2024-02-29")
  )
  expect_identical(result$variant[[16L]]$value, as.POSIXct(NA, tz = "UTC"))
  expect_identical(is.nan(result$variant[[5L]]$value), TRUE)
  expect_identical(result$variant[[11L]]$value, Inf)
  expect_identical(1 / result$variant[[17L]]$value, -Inf)
  expect_identical(result$variant[[19L]]$value, -Inf)
  expect_identical(result$variant[[20L]]$value, NA_real_)
  expect_identical(result$variant[[6L]]$value, "")
  expect_identical(result$variant[[12L]]$value, "caf\u00e9 \u6570\u636e")
  expect_identical(result$variant[[18L]]$value, NA_character_)
})

test_that("Arrow DAX Variant handles empty unions and unnamed branches", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  child_types <- list(nanoarrow::na_int64(), nanoarrow::na_string())
  empty <- pbi_test_dense_union(
    integer(),
    integer(),
    list(bit64::integer64(), character()),
    child_types
  )
  unnamed <- pbi_test_dense_union(
    c(0L, 1L),
    c(0L, 0L),
    list(bit64::as.integer64(7), "seven"),
    child_types
  )
  reused <- pbi_test_dense_union(
    c(0L, 0L),
    c(0L, 0L),
    list(integer = bit64::as.integer64(7)),
    list(integer = nanoarrow::na_int64())
  )

  empty_result <- pbi_dax_arrow_tibble(arrow::arrow_table(
    variant = arrow::chunked_array(empty)
  ))
  unnamed_result <- pbi_dax_arrow_tibble(arrow::arrow_table(
    variant = arrow::chunked_array(unnamed)
  ))
  reused_result <- pbi_dax_arrow_tibble(arrow::arrow_table(
    variant = arrow::chunked_array(reused)
  ))

  expect_length(empty_result$variant, 0L)
  expect_identical(
    vapply(unnamed_result$variant, `[[`, character(1), "type"),
    c("integer", "string")
  )
  expect_identical(
    unnamed_result$variant[[1L]]$value,
    bit64::as.integer64(7)
  )
  expect_identical(unnamed_result$variant[[2L]]$value, "seven")
  expect_identical(
    lapply(reused_result$variant, `[[`, "value"),
    list(bit64::as.integer64(7), bit64::as.integer64(7))
  )
})

test_that("Arrow DAX Variant rejects malformed dense-union layouts", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  child_types <- list(integer = nanoarrow::na_int64())
  invalid_type <- pbi_test_dense_union(
    10L,
    0L,
    list(integer = bit64::as.integer64(1)),
    child_types,
    type_codes = 9L,
    as_arrow = FALSE,
    validate = FALSE
  )
  invalid_offset <- pbi_test_dense_union(
    9L,
    1L,
    list(integer = bit64::as.integer64(1)),
    child_types,
    type_codes = 9L,
    as_arrow = FALSE,
    validate = FALSE
  )
  decreasing <- pbi_test_dense_union(
    c(9L, 9L),
    c(1L, 0L),
    list(integer = bit64::as.integer64(c(1, 2))),
    child_types,
    type_codes = 9L,
    as_arrow = FALSE,
    validate = FALSE
  )
  malformed_schema <- pbi_test_dense_union(
    9L,
    0L,
    list(integer = bit64::as.integer64(1)),
    child_types,
    type_codes = 9L,
    as_arrow = FALSE
  )
  malformed_schema$schema <- list(
    format = "+ud:bad",
    children = malformed_schema$schema$children
  )

  errors <- lapply(
    list(invalid_type, invalid_offset, decreasing, malformed_schema),
    function(value) {
      tryCatch(
        pbi_dax_arrow_dense_union_array(value$array, value$schema),
        error = identity
      )
    }
  )
  expect_identical(
    vapply(errors, conditionMessage, character(1)),
    rep("Power BI returned an invalid Arrow Variant column", 4L)
  )
  expect_identical(
    vapply(errors, inherits, logical(1), "fabric_pbi_dax_arrow_error"),
    rep(TRUE, 4L)
  )
})

test_that("Arrow DAX tibbles decode Variant dense unions", {
  skip_if_not_installed("arrow")
  encoded <- paste0(
    "/////1gBAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAI",
    "AAAABAAAAAIAAADwAAAABAAAACj///8AAAEOHAAAACwAAAAEAAAAAwAAAJAAAAB",
    "gAAAANAAAAAcAAAB2YXJpYW50AAgADAAGAAgACAAAAAAAAQAEAAAAAwAAAAAAAA",
    "ABAAAAAgAAAHz///8AAAEGEAAAABgAAAAEAAAAAAAAAAcAAABsb2dpY2FsANj//",
    "/+k////AAABBRAAAAAcAAAABAAAAAAAAAAGAAAAc3RyaW5nAAAEAAQABAAAAND/",
    "//8AAAECEAAAABgAAAAEAAAAAAAAAAcAAABpbnRlZ2VyAMT///8AAAABQAAAABA",
    "AFAAIAAYABwAMAAAAEAAQAAAAAAABAhAAAAAcAAAABAAAAAAAAAADAAAAcm93AAg",
    "ADAAIAAcACAAAAAAAAAFAAAAAAAAAAP////9YAQAAFAAAAAAAAAAMABYABgAFAA",
    "gADAAMAAAAAAMEABgAAABoAAAAAAAAAAAACgAYAAwABAAIAAoAAADMAAAAEAAAA",
    "AQAAAAAAAAAAAAAAAsAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAA",
    "ACAAAAAAAAAABAAAAAAAAAAoAAAAAAAAABAAAAAAAAAAOAAAAAAAAAABAAAAAAAA",
    "AEAAAAAAAAAAEAAAAAAAAABQAAAAAAAAAAAAAAAAAAAAUAAAAAAAAAAIAAAAAAAA",
    "AFgAAAAAAAAAAwAAAAAAAABgAAAAAAAAAAAAAAAAAAAAYAAAAAAAAAABAAAAAAAA",
    "AAAAAAAFAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAACAAAAAAAA",
    "AAEAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAKAAAAAAAA",
    "ABQAAAAAAAAAHgAAAAAAAAAoAAAAAAAAAAABAgAAAAAAAAAAAAAAAAAAAAAAAQAA",
    "AAEAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAADAAAAdHdvAAAAAAABAAAAAAAA",
    "AP////8AAAAA"
  )

  result <- pbi_parse_dax_arrow_response(jsonlite::base64_dec(encoded))

  expect_equal(result$row, bit64::as.integer64(c(10, 20, 30, 40)))
  expect_s3_class(result$variant[[1L]], "fabric_pbi_variant")
  expect_identical(
    vapply(result$variant, `[[`, character(1), "type"),
    c("integer", "string", "logical", "integer")
  )
  expect_equal(result$variant[[1L]]$value, bit64::as.integer64(1))
  expect_identical(result$variant[[2L]]$value, "two")
  expect_identical(result$variant[[3L]]$value, TRUE)
  expect_true(is.na(result$variant[[4L]]$value))
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
  expect_s3_class(resource$readers[[1L]], "RecordBatchReader")
  reader <- arrow::as_record_batch_reader(stream)
  expect_equal(nrow(reader$read_table()), 1000L)
})

test_that("Arrow DAX parser rejects error rowsets", {
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

test_that("personal XMLA resolution rejects unrepresentable owner identity", {
  called <- FALSE
  local_mocked_bindings(
    pbi_get_collection = function(url, ...) {
      called <<- TRUE
      list()
    }
  )

  expect_error(
    pbi_resolve_ids_from_connstr(
      paste0(
        "Data Source=powerbi://api.powerbi.com/v2.0/",
        pbi_test_workspace_id,
        "/home/myworkspace/owner%40example.com;",
        "Initial Catalog=Personal Model;"
      ),
      credential = fabric_credential(token = "token")
    ),
    "cannot be resolved safely",
    class = "fabric_pbi_personal_workspace_error"
  )
  expect_false(called)
})

test_that("Arrow DAX parser returns multiple data rowsets in order", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  arrow_payload <- function(data) {
    path <- tempfile(fileext = ".arrows")
    on.exit(unlink(path), add = TRUE)
    arrow::write_ipc_stream(arrow::arrow_table(data), path)
    readBin(path, "raw", n = file.info(path)$size)
  }
  payload <- c(
    arrow_payload(data.frame(first = 1:2)),
    arrow_payload(data.frame(second = c("a", "b")))
  )

  result <- pbi_parse_dax_arrow_response(payload)
  streams <- pbi_parse_dax_arrow_response(payload, "arrow_stream")

  expect_s3_class(result, "fabric_pbi_dax_rowsets")
  expect_length(result, 2L)
  expect_equal(result[[1L]]$first, 1:2)
  expect_equal(result[[2L]]$second, c("a", "b"))
  expect_s3_class(streams, "fabric_pbi_dax_rowsets")
  expect_length(streams, 2L)
  expect_s3_class(streams[[1L]], "nanoarrow_array_stream")
  expect_s3_class(streams[[2L]], "nanoarrow_array_stream")
  first <- arrow::as_record_batch_reader(streams[[1L]])$read_table()
  second <- arrow::as_record_batch_reader(streams[[2L]])$read_table()
  expect_equal(as.data.frame(first)$first, 1:2)
  expect_equal(as.data.frame(second)$second, c("a", "b"))
})

test_that("Power BI API bases accept explicit custom HTTPS origins", {
  expect_equal(
    pbi_api_base("https://api.powerbi.com/v1.0/myorg/"),
    "https://api.powerbi.com/v1.0/myorg"
  )
  expect_equal(
    pbi_api_base("https://powerbi.test/v1.0/myorg"),
    "https://powerbi.test/v1.0/myorg"
  )

  invalid <- c(
    "http://api.powerbi.com/v1.0/myorg",
    "https://user@api.powerbi.com/v1.0/myorg",
    "https://api.powerbi.com:443/v1.0/myorg",
    "https://api.powerbi.com/v1.0/myorg/groups",
    "https://api.powerbi.com/v1.0/myorg?token=secret",
    "https://api.powerbi.com/v1.0/myorg#fragment"
  )
  for (endpoint in invalid) {
    expect_error(
      pbi_api_base(endpoint),
      class = "fabric_pbi_endpoint_error",
      info = endpoint
    )
  }
})
