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
  fake_token <- "tok"
  conn <- "Data Source=powerbi://api.powerbi.com/v1.0/myorg/WS;Initial Catalog=DS;"

  got_group <- NULL
  got_dataset <- NULL

  testthat::with_mocked_bindings(
    pbi_get_group_id_by_name = function(
      access_token,
      workspace_name,
      api_base
    ) {
      expect_equal(access_token, fake_token)
      expect_equal(workspace_name, "WS")
      expect_match(api_base, "api.powerbi.com")
      got_group <<- TRUE
      "11111111-1111-1111-1111-111111111111"
    },
    pbi_get_dataset_id_by_name = function(
      access_token,
      group_id,
      dataset_name,
      api_base
    ) {
      expect_equal(access_token, fake_token)
      expect_equal(group_id, "11111111-1111-1111-1111-111111111111")
      expect_equal(dataset_name, "DS")
      expect_match(api_base, "api.powerbi.com")
      got_dataset <<- TRUE
      "22222222-2222-2222-2222-222222222222"
    },
    {
      ids <- fabricQueryR:::pbi_resolve_ids_from_connstr(
        conn,
        access_token = fake_token
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
      access_token,
      api_base
    ) {
      expect_equal(access_token, "supplied-token")
      list(group_id = "group-id", dataset_id = "dataset-id")
    },
    pbi_execute_dax = function(
      access_token,
      dataset_id,
      dax,
      group_id,
      include_nulls,
      api_base
    ) {
      expect_equal(access_token, "supplied-token")
      expect_equal(dataset_id, "dataset-id")
      expect_equal(group_id, "group-id")
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
