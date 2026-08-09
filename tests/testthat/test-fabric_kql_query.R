kusto_test_response <- function(
  body,
  status = 200L,
  url = "https://cluster.test"
) {
  if (!is.raw(body)) {
    body <- charToRaw(jsonlite::toJSON(
      body,
      auto_unbox = TRUE,
      null = "null",
      digits = 22
    ))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = list("content-type" = "application/json"),
    body = body
  )
}

kusto_test_completion <- function(has_errors = FALSE, errors = NULL) {
  out <- list(
    FrameType = "DataSetCompletion",
    HasErrors = has_errors,
    Cancelled = FALSE
  )
  if (!is.null(errors)) {
    out$OneApiErrors <- errors
  }
  out
}

test_that("KQL targets normalize direct and discovered coordinates", {
  direct <- kusto_resolve_target(
    "https://cluster.kusto.fabric.microsoft.com/",
    "Telemetry"
  )
  expect_equal(
    direct$url,
    "https://cluster.kusto.fabric.microsoft.com/v2/rest/query"
  )
  expect_equal(direct$database, "Telemetry")

  upgraded <- kusto_resolve_target(
    "https://cluster.kusto.fabric.microsoft.com/v1/rest/query",
    "Telemetry"
  )
  expect_match(upgraded$url, "/v2/rest/query$", perl = TRUE)

  discovered <- kusto_resolve_target(list(
    id = "database-id",
    type = "KQLDatabase",
    displayName = "Events",
    query_service_uri = "https://cluster.kusto.fabric.microsoft.com"
  ))
  expect_equal(discovered$database, "Events")
  expect_match(discovered$url, "/v2/rest/query$", perl = TRUE)

  expect_error(
    kusto_resolve_target(list(
      id = "eventhouse-id",
      type = "Eventhouse",
      query_service_uri = "https://cluster.kusto.fabric.microsoft.com"
    )),
    "database is required",
    fixed = TRUE
  )
  expect_error(
    kusto_resolve_target("http://cluster.test", "Events"),
    "valid HTTPS",
    fixed = TRUE
  )
  expect_error(
    kusto_resolve_target(
      "https://cluster.kusto.fabric.microsoft.com/unexpected",
      "Events"
    ),
    "service root",
    fixed = TRUE
  )
  expect_error(
    kusto_resolve_target("https://attacker.example", "Events"),
    "Microsoft Kusto endpoint",
    fixed = TRUE
  )
  expect_equal(
    kusto_resolve_target(
      "https://trusted.example",
      "Events",
      allow_custom_endpoint = TRUE
    )$url,
    "https://trusted.example/v2/rest/query"
  )
  expect_error(
    kusto_resolve_target(
      "https://cluster.kusto.fabric.microsoft.com?token=leak",
      "Events"
    ),
    "valid HTTPS",
    fixed = TRUE
  )
})

test_that("KQL parameter values are encoded without query interpolation", {
  encoded <- kusto_encode_parameters(list(
    text = "safe ' value; --",
    count = bit64::as.integer64("9007199254740993"),
    enabled = TRUE,
    at = as.POSIXct("2026-07-24 12:30:00", tz = "UTC"),
    day = as.Date("2026-07-24"),
    elapsed = as.difftime(90, units = "secs"),
    values = c("A", "B")
  ))

  expect_equal(encoded$text, "safe ' value; --")
  expect_equal(encoded$count, "9007199254740993")
  expect_equal(encoded$enabled, "true")
  expect_match(encoded$at, "^datetime\\(2026-07-24T12:30:00")
  expect_equal(encoded$day, "datetime(2026-07-24)")
  expect_equal(encoded$elapsed, "timespan(90s)")
  expect_equal(encoded$values, 'dynamic(["A","B"])')
  expect_equal(kusto_encode_parameter(character()), "dynamic([])")
  expect_equal(kusto_encode_parameter(list()), "dynamic([])")
  expect_equal(
    kusto_encode_parameter(setNames(list(), character())),
    "dynamic({})"
  )
  expect_equal(kusto_encode_parameter(NaN), "real(nan)")
  expect_equal(kusto_encode_parameter(Inf), "real(+inf)")
  expect_equal(kusto_encode_parameter(-Inf), "real(-inf)")
  expect_error(
    kusto_encode_parameter(as.difftime(Inf, units = "secs")),
    "must be finite",
    fixed = TRUE
  )
  expect_error(kusto_encode_parameters(list("bad-name" = 1)), "identifiers")
  expect_error(kusto_encode_parameters(list(missing = NA)), "cannot be NULL")
  expect_error(kusto_encode_parameters(list(missing = NULL)), "cannot be NULL")
  expect_error(kusto_encode_parameters(list(1)), "unique, non-empty names")
  expect_error(
    kusto_encode_parameters(list(
      days = as.Date(c("2026-01-01", "2026-01-02"))
    )),
    "must be scalar",
    fixed = TRUE
  )
})

test_that("Fabric-only KQL request-property restrictions are enforced", {
  for (property in c(
    "queryconsistency",
    "QUERY_WEAKCONSISTENCY_SESSION_ID"
  )) {
    options <- setNames(list("unsupported"), property)
    expect_error(
      fabric_kql_query(
        "https://cluster.kusto.fabric.microsoft.com",
        "Events | take 1",
        database = "Telemetry",
        request_properties = options,
        token = "token"
      ),
      property,
      fixed = TRUE,
      info = property
    )
  }
  expect_silent(kusto_validate_request_properties(list(
    servertimeout = "30s",
    notruncation = TRUE
  )))
})

test_that("fabric_kql_query sends a read-only v2 request with Kusto auth", {
  captured <- NULL
  response <- list(
    list(FrameType = "DataSetHeader", Version = "v2.0", IsProgressive = FALSE),
    list(
      FrameType = "DataTable",
      TableId = 0L,
      TableKind = "PrimaryResult",
      TableName = "PrimaryResult",
      Columns = list(list(ColumnName = "value", ColumnType = "int")),
      Rows = list(list(42L))
    ),
    kusto_test_completion()
  )
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    kusto_test_response(response, url = req$url)
  })

  audiences <- character()
  result <- fabric_kql_query(
    "https://cluster.kusto.fabric.microsoft.com",
    query = paste(
      "declare query_parameters(input:string);",
      "print value=42"
    ),
    database = "Events",
    parameters = list(input = "not interpolated ' ; --"),
    request_properties = list(servertimeout = "30s"),
    timeout = 17,
    token = function(audience, force_refresh = FALSE) {
      audiences <<- c(audiences, audience)
      "kusto-token"
    }
  )

  expect_equal(result$value, 42L)
  expect_equal(audiences, "https://api.kusto.windows.net/.default")
  expect_equal(
    captured$url,
    "https://cluster.kusto.fabric.microsoft.com/v2/rest/query"
  )
  expect_equal(captured$headers[["x-ms-readonly"]], "true")
  expect_equal(captured$options$timeout_ms, 17000)
  expect_equal(captured$body$data$db, "Events")
  expect_match(captured$body$data$csl, "query_parameters", fixed = TRUE)
  properties <- jsonlite::fromJSON(
    captured$body$data$properties,
    simplifyVector = FALSE
  )
  expect_equal(
    properties$Parameters$input,
    "not interpolated ' ; --"
  )
  expect_equal(
    properties$Options$servertimeout,
    "30s"
  )
  expect_match(
    properties$ClientRequestId,
    "^fabricQueryR\\.Query;"
  )
  expect_equal(
    properties$ClientRequestId,
    captured$headers[["x-ms-client-request-id"]]
  )
})

test_that("Kusto v2 type metadata produces stable R columns", {
  json <- paste0(
    "[",
    '{"FrameType":"DataSetHeader","Version":"v2.0","IsProgressive":false},',
    '{"FrameType":"DataTable","TableId":0,"TableKind":"PrimaryResult",',
    '"TableName":"Typed","Columns":[',
    '{"ColumnName":"flag","ColumnType":"bool"},',
    '{"ColumnName":"at","ColumnType":"datetime"},',
    '{"ColumnName":"amount","ColumnType":"decimal"},',
    '{"ColumnName":"payload","ColumnType":"dynamic"},',
    '{"ColumnName":"id","ColumnType":"guid"},',
    '{"ColumnName":"small","ColumnType":"int"},',
    '{"ColumnName":"big","ColumnType":"long"},',
    '{"ColumnName":"ratio","ColumnType":"real"},',
    '{"ColumnName":"text","ColumnType":"string"},',
    '{"ColumnName":"elapsed","ColumnType":"timespan"}',
    '],"Rows":[',
    '[true,"2026-07-24T12:30:01.125Z","12.50","{\\"a\\":1}",',
    '"74be27de-1e4e-49d9-b579-fe0b331d3642",7,9007199254740993,',
    '1.5,"hello","1.02:03:04.5"],',
    '[null,null,null,null,null,null,null,null,null,null]',
    ']},',
    '{"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}',
    "]"
  )
  frames <- jsonlite::fromJSON(
    json,
    simplifyVector = FALSE,
    bigint_as_char = TRUE
  )

  result <- kusto_parse_response(frames)

  expect_s3_class(result, "tbl_df")
  expect_equal(result$flag, c(TRUE, NA))
  expect_s3_class(result$at, "POSIXct")
  expect_equal(
    as.numeric(result$at[[1L]]),
    as.numeric(as.POSIXct("2026-07-24 12:30:01.125", tz = "UTC")),
    tolerance = 1e-6
  )
  expect_equal(result$amount, c(12.5, NA))
  expect_equal(result$payload[[1L]]$a, 1L)
  expect_null(result$payload[[2L]])
  expect_equal(
    result$id[[1L]],
    "74be27de-1e4e-49d9-b579-fe0b331d3642"
  )
  expect_equal(result$small, c(7L, NA_integer_))
  expect_s3_class(result$big, "integer64")
  expect_equal(as.character(result$big), c("9007199254740993", NA))
  expect_equal(result$ratio, c(1.5, NA))
  expect_equal(result$text, c("hello", NA))
  expect_s3_class(result$elapsed, "difftime")
  expect_equal(as.numeric(result$elapsed, units = "secs"), c(93784.5, NA))
  expect_null(names(result$elapsed))
  expect_equal(
    attr(result, "kusto_schema")$type,
    c(
      "bool",
      "datetime",
      "decimal",
      "dynamic",
      "guid",
      "int",
      "long",
      "real",
      "string",
      "timespan"
    )
  )
})

test_that("Kusto signed integer boundaries remain exact", {
  expect_warning(
    int <- kusto_convert_column(
      list("-2147483648", "2147483647", NULL),
      "int"
    ),
    "returning it as character",
    class = "fabric_kql_integer_boundary_warning"
  )
  expect_identical(int, c("-2147483648", "2147483647", NA_character_))

  expect_warning(
    long <- kusto_convert_column(
      list("-9223372036854775808", "9223372036854775807", NULL),
      "long"
    ),
    "returning it as character",
    class = "fabric_kql_integer_boundary_warning"
  )
  expect_identical(
    long,
    c("-9223372036854775808", "9223372036854775807", NA_character_)
  )
  expect_error(
    kusto_convert_column(list("not-a-long"), "long"),
    "invalid integer value",
    fixed = TRUE
  )
})

test_that("Kusto real specials survive numeric conversion", {
  values <- kusto_numeric_column(list("NaN", "Infinity", "-Infinity"))

  expect_true(is.nan(values[[1L]]))
  expect_identical(values[[2L]], Inf)
  expect_identical(values[[3L]], -Inf)
})

test_that("multiple and progressive Kusto primary tables are assembled", {
  frames <- list(
    list(FrameType = "DataSetHeader", Version = "v2.0", IsProgressive = TRUE),
    list(
      FrameType = "TableHeader",
      TableId = 1L,
      TableKind = "PrimaryResult",
      TableName = "First",
      Columns = list(list(ColumnName = "value", ColumnType = "int"))
    ),
    list(
      FrameType = "TableFragment",
      TableId = 1L,
      FieldCount = 1L,
      TableFragmentType = "DataAppend",
      Rows = list(list(1L), list(2L))
    ),
    list(
      FrameType = "TableFragment",
      TableId = 1L,
      FieldCount = 1L,
      TableFragmentType = "DataReplace",
      Rows = list(list(3L))
    ),
    list(FrameType = "TableCompletion", TableId = 1L, RowCount = 1L),
    list(
      FrameType = "DataTable",
      TableId = 2L,
      TableKind = "PrimaryResult",
      TableName = "Second",
      Columns = list(list(ColumnName = "label", ColumnType = "string")),
      Rows = list(list("done"))
    ),
    list(
      FrameType = "DataTable",
      TableId = 3L,
      TableKind = "QueryProperties",
      TableName = "@ExtendedProperties",
      Columns = list(list(ColumnName = "Value", ColumnType = "dynamic")),
      Rows = list()
    ),
    kusto_test_completion()
  )

  result <- kusto_parse_response(frames)

  expect_s3_class(result, "fabric_kql_tables")
  expect_named(result, c("First", "Second"))
  expect_equal(result$First$value, 3L)
  expect_equal(result$Second$label, "done")
})

test_that("Kusto completion, cancellation, malformed, and HTTP errors fail", {
  header <- list(
    FrameType = "DataSetHeader",
    Version = "v2.0",
    IsProgressive = FALSE
  )
  expect_error(
    kusto_parse_response(list(
      header,
      kusto_test_completion(
        TRUE,
        list(list(code = "SEM0100", message = "missing table"))
      )
    )),
    "SEM0100.*missing table"
  )
  expect_error(
    kusto_parse_response(list(
      header,
      list(
        FrameType = "DataSetCompletion",
        HasErrors = FALSE,
        Cancelled = TRUE
      )
    )),
    "cancelled",
    fixed = TRUE
  )
  expect_error(kusto_parse_response(list()), "malformed v2 response")

  httr2::local_mocked_responses(function(req) {
    kusto_test_response(
      list(error = list(code = "BadRequest", message = "invalid KQL")),
      status = 400L,
      url = req$url
    )
  })
  expect_error(
    fabric_kql_query(
      "https://cluster.kusto.fabric.microsoft.com",
      query = "missing_table | take 1",
      database = "Events",
      token = "token"
    ),
    "HTTP 400.*invalid KQL"
  )
})

test_that("Kusto v2 parser rejects malformed envelopes and rows", {
  header <- list(
    FrameType = "DataSetHeader",
    Version = "v2.0",
    IsProgressive = FALSE
  )
  table <- list(
    FrameType = "DataTable",
    TableId = 0L,
    TableKind = "PrimaryResult",
    TableName = "PrimaryResult",
    Columns = list(
      list(ColumnName = "a", ColumnType = "int"),
      list(ColumnName = "b", ColumnType = "string")
    ),
    Rows = list(list(1L, "one"))
  )
  completion <- kusto_test_completion()

  expect_error(
    kusto_parse_response(list(table, header, completion)),
    "DataSetHeader must be the first",
    fixed = TRUE
  )
  expect_error(
    kusto_parse_response(list(header, completion, table)),
    "DataSetCompletion must be the final",
    fixed = TRUE
  )
  missing_has_errors <- completion
  missing_has_errors$HasErrors <- NULL
  expect_error(
    kusto_parse_response(list(header, table, missing_has_errors)),
    "HasErrors must be true or false",
    fixed = TRUE
  )

  long_row <- table
  long_row$Rows <- list(list(1L, "one", "extra"))
  expect_error(
    kusto_parse_response(list(header, long_row, completion)),
    "row width must equal the declared 2 columns",
    fixed = TRUE
  )
  short_row <- table
  short_row$Rows <- list(list(1L))
  expect_error(
    kusto_parse_response(list(header, short_row, completion)),
    "row width must equal the declared 2 columns",
    fixed = TRUE
  )
})

test_that("Kusto v2 parser validates progressive transitions", {
  header <- list(
    FrameType = "DataSetHeader",
    Version = "v2.0",
    IsProgressive = TRUE
  )
  table_header <- list(
    FrameType = "TableHeader",
    TableId = 4L,
    TableKind = "PrimaryResult",
    TableName = "Progressive",
    Columns = list(list(ColumnName = "value", ColumnType = "int"))
  )
  fragment <- list(
    FrameType = "TableFragment",
    TableId = 4L,
    FieldCount = 1L,
    TableFragmentType = "DataAppend",
    Rows = list(list(1L))
  )
  table_completion <- list(
    FrameType = "TableCompletion",
    TableId = 4L,
    RowCount = 1L
  )
  completion <- kusto_test_completion()

  wrong_fields <- fragment
  wrong_fields$FieldCount <- 2L
  expect_error(
    kusto_parse_response(list(
      header,
      table_header,
      wrong_fields,
      table_completion,
      completion
    )),
    "FieldCount does not match",
    fixed = TRUE
  )
  bad_operation <- fragment
  bad_operation$TableFragmentType <- "DataMerge"
  expect_error(
    kusto_parse_response(list(
      header,
      table_header,
      bad_operation,
      table_completion,
      completion
    )),
    "DataAppend or DataReplace",
    fixed = TRUE
  )
  expect_error(
    kusto_parse_response(list(header, table_header, fragment, completion)),
    "missing TableCompletion",
    fixed = TRUE
  )
  wrong_rows <- table_completion
  wrong_rows$RowCount <- 2L
  expect_error(
    kusto_parse_response(list(
      header,
      table_header,
      fragment,
      wrong_rows,
      completion
    )),
    "contains 1 rows",
    fixed = TRUE
  )
  expect_error(
    kusto_parse_response(list(
      header,
      table_header,
      fragment,
      table_completion,
      fragment,
      completion
    )),
    "after TableCompletion",
    fixed = TRUE
  )
})

test_that("fabric_kql_query validates query, timeout, and discovery types", {
  expect_error(
    fabric_kql_query(
      "https://cluster.kusto.fabric.microsoft.com",
      query = "",
      database = "Events",
      token = "token"
    ),
    "query must",
    fixed = TRUE
  )
  expect_error(
    fabric_kql_query(
      "https://cluster.kusto.fabric.microsoft.com",
      query = "print 1",
      database = "Events",
      timeout = 0,
      token = "token"
    ),
    "timeout",
    fixed = TRUE
  )
  expect_error(
    fabric_kql_query(
      list(
        id = "lakehouse-id",
        type = "Lakehouse",
        query_service_uri = "https://cluster.kusto.fabric.microsoft.com"
      ),
      query = "print 1",
      database = "Events",
      token = "token"
    ),
    "Eventhouse or KQLDatabase",
    fixed = TRUE
  )
})
