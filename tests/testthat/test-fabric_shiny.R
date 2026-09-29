test_that("the public UI and server start without acquiring credentials", {
  skip_if_no_shiny_targets()
  config <- shiny_test_config()
  expect_type(
    fabric_shiny_ui(shiny::fluidPage("App"), "fabric", config),
    "closure"
  )
  shiny::testServer(
    function(input, output, session) {
      fabric <- fabric_shiny_server(
        "fabric",
        config,
        refresh_proactively = FALSE
      )
    },
    {
      session$flushReact()
      expect_identical(fabric$ready(), FALSE)
      expect_null(fabric$generation())
      expect_identical(
        fabric$status(),
        list(connection_id = NULL, services = list())
      )
    }
  )
})

test_that("real module references isolate refresh, logout, replacement and foreign sessions", {
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
  config <- shiny_test_config()
  requests <- 0L
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    json_response(
      body = list(
        value = list(list(
          id = "22222222-2222-2222-2222-222222222222",
          displayName = "Workspace"
        ))
      ),
      url = req$url
    )
  })
  # Inject an already validated synthetic token at the module's acceptance seam.
  # All connection ownership, target access and consumer reactivity run normally.
  shiny::testServer(
    shinyOAuth::oauth_module_server,
    args = list(id = "auth", client = config$client, auto_redirect = FALSE),
    {
      fabric <- fabric_shiny_session(
        values,
        config$profiles,
        config$min_valid_for
      )
      operation <- .begin_auth_operation("login", NULL, new_epoch = TRUE)
      .accept_login_token(shiny_test_token(), NULL)
      .finish_auth_operation(operation, "login")
      session$flushReact()
      expect_identical(fabric$ready(), TRUE)
      old_generation <- fabric$generation()
      provider <- fabric$token_provider()
      workspace <- fabric$workspaces()[[1L]]
      expect_s3_class(workspace, "FabricWorkspace")
      expect_identical(fabric$identity()$id_token_claims$name, "Test User")
      reads <- 0L
      observer <- shiny::observe({
        fabric$generation()
        provider(.fabric_audience$fabric)
        reads <<- reads + 1L
      })
      session$flushReact()
      expect_identical(reads, 1L)
      values$token <- shiny_test_token("rotated-user-token")
      session$flushReact()
      expect_identical(reads, 1L)
      expect_identical(fabric$generation(), old_generation)
      expect_identical(provider(.fabric_audience$fabric), "rotated-user-token")
      observer$destroy()

      foreign <- shiny::MockShinySession$new()
      error <- shiny::withReactiveDomain(
        foreign,
        shiny::isolate(rlang::catch_cnd(provider(.fabric_audience$fabric)))
      )
      foreign$close()
      expect_s3_class(error, "shinyOAuth_access_error")
      expect_identical(error$context$reason, "authorization_unavailable")

      fabric$logout()
      session$flushReact()
      expect_null(fabric$generation())
      expect_identical(fabric$ready(), FALSE)
      before <- requests
      error <- rlang::catch_cnd(workspace$items(), classes = "error")
      expect_s3_class(error, "shinyOAuth_access_error")
      expect_identical(requests, before)
      operation <- .begin_auth_operation("login", NULL, new_epoch = TRUE)
      .accept_login_token(shiny_test_token("second-user-token"), NULL)
      .finish_auth_operation(operation, "login")
      session$flushReact()
      expect_identical(identical(fabric$generation(), old_generation), FALSE)
      expect_identical(
        fabric$token_provider()(.fabric_audience$fabric),
        "second-user-token"
      )
      expect_s3_class(
        rlang::catch_cnd(provider(.fabric_audience$fabric)),
        "shinyOAuth_access_error"
      )
      current_provider <- fabric$token_provider()
      session$close()
      expect_s3_class(
        rlang::catch_cnd(current_provider(.fabric_audience$fabric)),
        "shinyOAuth_access_error"
      )
    }
  )
})

test_that("an optional target failure preserves Fabric access and can be retried", {
  skip_if_no_shiny_targets()
  config <- shiny_test_config(c("fabric", "sql"))
  connection <- shiny_test_connection()
  target_state <- new.env(parent = emptyenv())
  target_state$sql_acquired <- FALSE
  target_state$sql_denied <- TRUE
  connection$access_token <- function(
    required_scopes,
    min_valid_for,
    force_refresh,
    async,
    target
  ) {
    if (target == "sql") {
      if (target_state$sql_denied) {
        rlang::abort(
          "Consent needed",
          class = "shinyOAuth_access_error",
          context = list(reason = "interaction_required")
        )
      }
      target_state$sql_acquired <- TRUE
    }
    "synthetic-token"
  }
  connection$has_scopes <- function(scopes, target) {
    target == "fabric" || target_state$sql_acquired
  }
  connection$targets <- function() {
    list(
      fabric = list(status = "active"),
      sql = list(
        status = if (target_state$sql_acquired) "active" else "not_acquired"
      )
    )
  }
  shiny::testServer(
    function(input, output, session) {
      fabric <- fabric_shiny_session(
        list(connection = function() connection),
        config$profiles,
        60
      )
    },
    {
      session$flushReact()
      expect_identical(fabric$ready("fabric"), TRUE)
      expect_identical(fabric$ready("sql"), FALSE)
      expect_identical(
        fabric$status()$services$sql$reason,
        "interaction_required"
      )
      target_state$sql_denied <- FALSE
      expect_identical(fabric$prepare("sql"), TRUE)
      expect_identical(fabric$ready("sql"), TRUE)
      expect_null(fabric$status()$services$sql$reason)
    }
  )
})

test_that("session requests and discovery cannot override authentication", {
  skip_if_no_shiny_targets()
  config <- shiny_test_config()
  connection <- shiny_test_connection()
  connection$has_scopes <- function(...) TRUE
  requests <- list()
  httr2::local_mocked_responses(function(req) {
    requests[[length(requests) + 1L]] <<- req
    json_response(body = list(value = list()), url = req$url)
  })
  shiny::testServer(
    function(input, output, session) {
      fabric <- fabric_shiny_session(
        list(connection = function() connection),
        config$profiles,
        60
      )
    },
    {
      session$flushReact()
      response <- fabric$request(
        "workspaces",
        query = list(continuationToken = "page")
      )
      expect_identical(httr2::resp_status(response), 200L)
      expect_identical(
        requests[[1L]]$url,
        "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=page"
      )
      expect_s3_class(
        rlang::catch_cnd(fabric$request("https://evil.example")),
        "fabric_shiny_error"
      )
      expect_s3_class(
        rlang::catch_cnd(fabric$request("../workspaces")),
        "fabric_shiny_error"
      )
      expect_s3_class(
        rlang::catch_cnd(fabric$workspaces(token = "override")),
        "fabric_shiny_error"
      )
      expect_length(requests, 1L)
    }
  )
})

test_that("real Microsoft target acquisition supplies SQL and GraphQL separately", {
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
  config <- shiny_test_config(c("fabric", "sql", "graphql"))
  acquired <- character()
  received <- character()
  httr2::local_mocked_responses(function(req) {
    if (grepl("/token$", req$url)) {
      requested <- utils::URLdecode(as.character(req$body$data$scope))
      target <- if (grepl("database.windows.net", requested, fixed = TRUE)) {
        "sql"
      } else {
        "graphql"
      }
      acquired <<- c(acquired, target)
      return(json_response(
        body = list(
          access_token = paste0(target, "-user-token"),
          token_type = "Bearer",
          refresh_token = paste0("rotated-refresh-", length(acquired)),
          expires_in = 3600,
          scope = paste(
            c("openid", "profile", config$profiles[[target]]$scopes),
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
    json_response(body = list(data = list(value = "result")), url = req$url)
  })
  local_mocked_bindings(
    fabric_sql_require_backend = function(...) NULL,
    .fabric_sql_db_connect = function(...) {
      received <<- c(received, list(...)$attributes$azure_token)
      structure(list(), class = "synthetic_connection")
    },
    .fabric_sql_db_disconnect = function(...) NULL
  )
  shiny::testServer(
    shinyOAuth::oauth_module_server,
    args = list(id = "auth", client = config$client, auto_redirect = FALSE),
    {
      fabric <- fabric_shiny_session(values, config$profiles, 60)
      operation <- .begin_auth_operation("login", NULL, new_epoch = TRUE)
      .accept_login_token(shiny_test_token(), NULL)
      .finish_auth_operation(operation, "login")
      session$flushReact()
      expect_identical(fabric$ready(), TRUE)
      expect_identical(acquired, c("sql", "graphql"))
      con <- fabric_sql_connect(
        "warehouse.datawarehouse.fabric.microsoft.com",
        token = fabric$token_provider(),
        verbose = FALSE
      )
      expect_s3_class(con, "synthetic_connection")
      result <- fabric_graphql_query(
        "https://example.graphql.fabric.microsoft.com/graphql",
        "query { value }",
        token = fabric$token_provider()
      )
      expect_identical(result$data$value, "result")
      expect_identical(
        received,
        c("sql-user-token", "Bearer graphql-user-token")
      )
    }
  )
})

test_that("two simultaneous module sessions cannot use each other's providers", {
  skip_if_no_shiny_targets()
  withr::local_options(shinyOAuth.skip_browser_token = TRUE)
  local_mocked_bindings(
    revoke_token = function(...) NULL,
    .package = "shinyOAuth"
  )
  config <- shiny_test_config()
  first_session <- shiny::MockShinySession$new()
  second_session <- shiny::MockShinySession$new()
  withr::defer(first_session$close())
  withr::defer(second_session$close())
  authorize <- function(root, user) {
    shiny::withReactiveDomain(
      root,
      shiny::isolate({
        fabric <- fabric_shiny_server(
          "fabric",
          config,
          refresh_proactively = FALSE
        )
        module <- root$env
        operation <- module$.begin_auth_operation(
          "login",
          NULL,
          new_epoch = TRUE
        )
        module$.accept_login_token(
          shiny_test_token(paste0(user, "-token"), subject = user),
          NULL
        )
        module$.finish_auth_operation(operation, "login")
        root$flushReact()
        fabric$token_provider()
      })
    )
  }
  first <- authorize(first_session, "user-a")
  second <- authorize(second_session, "user-b")
  shiny::withReactiveDomain(
    first_session,
    shiny::isolate({
      expect_identical(first(.fabric_audience$fabric), "user-a-token")
      error <- rlang::catch_cnd(
        second(.fabric_audience$fabric),
        classes = "error"
      )
      expect_s3_class(error, "shinyOAuth_access_error")
      expect_identical(error$context$reason, "authorization_unavailable")
    })
  )
  shiny::withReactiveDomain(
    second_session,
    shiny::isolate({
      expect_identical(second(.fabric_audience$fabric), "user-b-token")
      expect_s3_class(
        rlang::catch_cnd(first(.fabric_audience$fabric), classes = "error"),
        "shinyOAuth_access_error"
      )
    })
  )
})
