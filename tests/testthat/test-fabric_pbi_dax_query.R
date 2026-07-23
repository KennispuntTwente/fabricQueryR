# pbi_parse_connstr() -----------------------------------------------------

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
  conn <- "powerbi://api.powerbi.com/v1.0/myorg/Another%20WS;Catalog=MyData;"
  p <- fabricQueryR:::pbi_parse_connstr(conn)
  expect_equal(p$workspace, "Another WS")
  expect_equal(p$dataset, "MyData")
})

test_that("pbi_parse_connstr errors when Data Source missing", {
  expect_error(fabricQueryR:::pbi_parse_connstr("Initial Catalog=OnlyDataset;"))
})


# pbi_resolve_ids_from_connstr() ------------------------------------------

test_that("pbi_resolve_ids_from_connstr wires through to GUID lookups", {
  fake_credential <- fabric_credential(access_token = "tok")
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
      list(group_id = "group-id", dataset_id = "dataset-id")
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
      expect_equal(dataset_id, "dataset-id")
      expect_equal(group_id, "group-id")
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
        access_token = "supplied-token"
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
      stop("unexpected lookup")
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
      expect_equal(dataset_id, "dataset-id")
      expect_equal(group_id, "workspace-id")
      expect_equal(impersonated_user, "reader@example.com")
      tibble::tibble(value = 42L)
    }
  )

  result <- fabric_pbi_dax_query(
    dax = 'EVALUATE ROW("value", 42)',
    workspace_id = "workspace-id",
    dataset_id = "dataset-id",
    access_token = "token",
    impersonated_user = "reader@example.com"
  )

  expect_false(looked_up)
  expect_equal(result$value, 42L)
  expect_error(
    fabric_pbi_dax_query(dax = "EVALUATE ROW()", access_token = "token"),
    "Supply either connstr or dataset_id",
    fixed = TRUE
  )
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
    .httr2_json = function(req, simplifyVector, ...) {
      expect_false(simplifyVector)
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
    credential = fabric_credential(access_token = "token"),
    dataset_id = "dataset",
    group_id = "workspace",
    dax = 'EVALUATE ROW("value", 7)',
    impersonated_user = "reader@example.com"
  )
  expect_equal(result[["[value]"]], 7L)
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
