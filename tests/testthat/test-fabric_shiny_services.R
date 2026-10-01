test_that("delegated targets reach DAX, GraphQL, KQL and OneLake transports", {
  skip_if_no_shiny_targets()
  withr::local_options(shinyOAuth.skip_browser_token = TRUE)
  local_mocked_bindings(
    revoke_token = function(...) NULL,
    .package = "shinyOAuth"
  )
  local_mocked_bindings(
    get_azure_token = function(...) stop("Unexpected fallback login"),
    .package = "AzureAuth"
  )
  config <- shiny_test_config(c("fabric", "dax", "graphql", "kql", "onelake"))
  acquired <- character()
  received <- character()
  httr2::local_mocked_responses(function(req) {
    if (grepl("/token$", req$url)) {
      requested <- utils::URLdecode(as.character(req$body$data$scope))
      matches <- vapply(
        config$client@token_targets,
        function(target) {
          grepl(target$resource, requested, fixed = TRUE)
        },
        logical(1)
      )
      target <- names(which(matches))
      expect_length(target, 1L)
      acquired <<- c(acquired, target)
      return(json_response(
        body = list(
          access_token = paste0(target, "-user-token"),
          token_type = "Bearer",
          refresh_token = paste0("rotated-refresh-", length(acquired)),
          expires_in = 3600,
          scope = paste(
            c(
              "openid",
              "profile",
              config$client@token_targets[[target]]$scopes
            ),
            collapse = " "
          )
        ),
        url = req$url
      ))
    }
    received <<- c(
      received,
      unname(httr2::req_get_headers(req, redacted = "reveal")[[
        "Authorization"
      ]])
    )
    if (grepl("api.powerbi.com", req$url, fixed = TRUE)) {
      return(json_response(
        body = list(
          results = list(list(
            tables = list(list(
              rows = list(setNames(list(1), "[value]"))
            ))
          ))
        ),
        url = req$url
      ))
    }
    if (grepl("graphql.fabric.microsoft.com", req$url, fixed = TRUE)) {
      return(json_response(
        body = list(data = list(value = "result")),
        url = req$url
      ))
    }
    if (grepl("kusto.fabric.microsoft.com", req$url, fixed = TRUE)) {
      return(kusto_test_response(
        list(
          list(
            FrameType = "DataSetHeader",
            Version = "v2.0",
            IsProgressive = FALSE
          ),
          list(
            FrameType = "DataTable",
            TableId = 0L,
            TableKind = "PrimaryResult",
            TableName = "PrimaryResult",
            Columns = list(list(ColumnName = "value", ColumnType = "int")),
            Rows = list(list(1L))
          ),
          kusto_test_completion()
        ),
        url = req$url
      ))
    }
    expect_match(req$url, "onelake.dfs.fabric.microsoft.com", fixed = TRUE)
    json_response(
      body = list(
        paths = list(list(
          name = "00000000-0000-0000-0000-000000000002/Files/sales.csv",
          isDirectory = FALSE,
          contentLength = "3"
        ))
      ),
      url = req$url
    )
  })
  shiny::testServer(
    shinyOAuth::oauth_module_server,
    args = list(id = "auth", client = config$client, auto_redirect = FALSE),
    {
      fabric <- fabric_shiny_session(values, config$profiles, 60)
      operation <- .begin_auth_operation("login", NULL, new_epoch = TRUE)
      .accept_login_token(shiny_test_token(), NULL)
      .finish_auth_operation(operation, "login")
      session$flushReact()
      expect_true(fabric$ready())
      expect_identical(acquired, c("power_bi", "kusto", "storage"))
      dax <- fabric_pbi_dax_query(
        workspace_id = "00000000-0000-0000-0000-000000000001",
        dataset_id = "00000000-0000-0000-0000-000000000002",
        dax = 'EVALUATE ROW("value", 1)',
        token = shiny_test_await(fabric$access_token("dax", async = TRUE))
      )
      expect_equal(dax[["[value]"]], 1)
      graphql <- fabric_graphql_query(
        "https://example.graphql.fabric.microsoft.com/graphql",
        "query { value }",
        token = fabric$token_provider()
      )
      expect_identical(graphql$data$value, "result")
      kql <- fabric_kql_query(
        "https://cluster.z1.kusto.fabric.microsoft.com",
        "print value=1",
        database = "sales",
        token = shiny_test_await(fabric$access_token("kql", async = TRUE))
      )
      expect_equal(kql$value, 1)
      files <- fabric_onelake_list(
        workspace = "00000000-0000-0000-0000-000000000001",
        item = "00000000-0000-0000-0000-000000000002",
        path = "Files",
        token = fabric$token_provider()
      )
      expect_match(files$name, "sales.csv", fixed = TRUE)
      expect_identical(
        received,
        c(
          "Bearer power_bi-user-token",
          "Bearer power_bi-user-token",
          "Bearer kusto-user-token",
          "Bearer storage-user-token"
        )
      )
      expect_length(acquired, 3L)
    }
  )
})
