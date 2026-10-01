test_that("Shiny playground queries use fixture records and bound parameters", {
  env <- playground_shiny_test_environment()
  workspace <- list(id = "workspace")
  targets <- list(
    warehouse = list(id = "warehouse"),
    semantic_model = list(id = "model"),
    graphql_api = list(id = "graphql")
  )
  request <- function(source, ...) {
    env$playground_shiny_request(
      source,
      workspace,
      targets,
      "delegated",
      ...
    )
  }
  sql <- request("warehouse", min_id = 2L, backend = "adbc")
  dax <- request("dax", category = "B")
  expect_match(
    conditionMessage(rlang::catch_cnd(request("dax", category = 'B"}'))),
    "arg"
  )
  expect_match(
    conditionMessage(rlang::catch_cnd(request("warehouse", min_id = 1.5))),
    "Minimum ID"
  )
  expect_match(
    conditionMessage(rlang::catch_cnd(request("delta"))),
    "missing.*lakehouse"
  )
  calls <- character()
  local_mocked_bindings(
    fabric_sql_query = function(server, sql, params, backend, token, ...) {
      calls <<- c(calls, "sql")
      expect_identical(server, targets$warehouse)
      expect_identical(params, list(2L))
      expect_match(sql, "WHERE id >= ?", fixed = TRUE)
      expect_identical(backend, "adbc")
      expect_identical(token, "sql-token")
      data.frame(id = 2:3)
    },
    fabric_pbi_dax_query = function(workspace_id, dax, api, token, ...) {
      calls <<- c(calls, "dax")
      expect_identical(workspace_id, targets$semantic_model)
      expect_match(dax, 'TREATAS({"B"}', fixed = TRUE)
      expect_identical(api, "json")
      expect_identical(token, "dax-token")
      data.frame(category = "B", rows = 1L, amount = 20)
    }
  )
  result <- env$playground_shiny_run(
    list(warehouse = sql, dax = dax),
    list(sql = "sql-token", dax = "dax-token")
  )
  expect_identical(calls, c("sql", "dax"))
  expect_identical(result$warehouse$ok, TRUE)
  expect_equal(result$warehouse$data$id, 2:3)
  expect_identical(
    result$dax$data,
    data.frame(Category = "B", Rows = 1L, Amount = 20)
  )
  expect_identical(
    request("graphql")$audience,
    "https://analysis.windows.net/powerbi/api/GraphQLApi.Execute.All"
  )
  expect_identical(
    env$playground_shiny_request(
      "graphql",
      workspace,
      targets,
      "sandbox"
    )$audience,
    "https://api.fabric.microsoft.com/.default"
  )

  result <- env$playground_shiny_run(
    list(warehouse = sql, dax = dax),
    list(sql = simpleError("SQL consent missing"), dax = "dax-token")
  )
  expect_identical(result$warehouse$ok, FALSE)
  expect_identical(result$warehouse$error, "SQL consent missing")
  expect_identical(result$dax$ok, TRUE)
})

test_that("playground GraphQL collects actual paginated response envelopes", {
  env <- playground_shiny_test_environment()
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    rows <- if (calls == 1L) {
      list(list(id = 1L), list(id = 2L))
    } else {
      list(list(id = 3L))
    }
    graphql_test_response(
      list(
        data = list(
          fabricqueryr_basics = list(
            items = rows,
            hasNextPage = calls == 1L,
            endCursor = if (calls == 1L) "next-page" else NULL
          )
        )
      ),
      url = req$url
    )
  })
  request <- env$playground_shiny_request(
    "graphql",
    list(id = "workspace"),
    list(graphql_api = "https://example.test/graphql"),
    "sandbox"
  )
  audience <- NULL
  data <- env$playground_shiny_read(
    request,
    token = function(requested_audience) {
      audience <<- requested_audience
      "graphql-token"
    }
  )
  expect_identical(calls, 2L)
  expect_equal(data$id, 1:3)
  expect_identical(attr(data, "pages"), 2L)
  expect_identical(audience, "https://api.fabric.microsoft.com/.default")
})

test_that("partial sandbox discovery keeps named missing fixtures and rejects duplicates", {
  env <- new.env(parent = globalenv())
  sys.source(.playground_test_path("sandbox.R"), envir = env)
  names <- c(lakehouse = "TestLakehouse", semantic_model = "Model")
  types <- c(lakehouse = "Lakehouse", semantic_model = "SemanticModel")
  item <- list(id = "model", displayName = "Model", type = "SemanticModel")
  result <- env$playground_resolve_targets(
    list(item),
    names,
    types,
    optional = names(names)
  )
  expect_named(result, names(names))
  expect_null(result$lakehouse)
  expect_identical(result$semantic_model, item)
  expect_match(
    conditionMessage(rlang::catch_cnd(env$playground_resolve_targets(
      list(item, item),
      names,
      types,
      optional = names(names)
    ))),
    "found 2"
  )
  expect_match(
    conditionMessage(rlang::catch_cnd(env$connect_playground_sandbox(
      allow_partial = NA
    ))),
    "allow_partial"
  )
})

test_that("partially configured delegated mode never silently uses the shared identity", {
  env <- playground_shiny_test_environment()
  withr::local_envvar(c(
    ENTRA_TENANT_ID = "tenant",
    ENTRA_CLIENT_ID = "",
    ENTRA_CLIENT_SECRET = ""
  ))
  expect_match(
    conditionMessage(rlang::catch_cnd(env$playground_shiny_mode("auto"))),
    "Delegated mode needs"
  )
  expect_identical(env$playground_shiny_mode("sandbox"), "sandbox")
  withr::local_envvar(c(ENTRA_TENANT_ID = ""))
  expect_identical(env$playground_shiny_mode("auto"), "sandbox")
})

test_that("playground tasks stay responsive and discard a signed-out user's results", {
  env <- playground_shiny_test_environment()
  skip_if_not_installed("mirai")
  withr::local_envvar(LC_ALL = "C", LANG = "C", LANGUAGE = "en")
  mirai::daemons(1, .compute = "playground-test")
  withr::defer(mirai::daemons(0, .compute = "playground-test"))
  started <- withr::local_tempfile()
  release <- withr::local_tempfile()
  withr::defer(writeLines("release", release))
  state <- shiny::reactiveVal("user-one")
  local_mocked_bindings(
    fabric_shiny_ui = function(ui, ...) ui,
    fabric_shiny_server = function(...) {
      list(
        generation = function() state(),
        ready = function(...) !is.null(state()),
        status = function() list(services = list()),
        identity = function() list(name = state()),
        access_token = function(service, async) {
          expect_identical(async, TRUE)
          promises::promise_resolve(state())
        },
        logout = function() state(NULL),
        login = function() state("user-two"),
        prepare = function() NULL,
        reauthorize = function() NULL
      )
    }
  )
  dispatch <- function(requests, tokens) {
    mirai::mirai(
      {
        writeLines("started", started)
        deadline <- Sys.time() + 20
        while (!file.exists(release) && Sys.time() < deadline) {
          Sys.sleep(0.01)
        }
        if (!file.exists(release)) {
          stop("Worker timed out waiting for release")
        }
        lapply(requests, function(request) {
          list(
            ok = TRUE,
            error = NULL,
            data = data.frame(
              Category = tokens[[request$service]],
              Rows = 3L,
              Amount = 30.5
            ),
            request = request,
            seconds = 0.1,
            pid = Sys.getpid()
          )
        })
      },
      requests = requests,
      tokens = tokens,
      started = started,
      release = release,
      .compute = "playground-test"
    )
  }
  sandbox <- list(
    workspace = list(id = "workspace", displayName = "Sandbox"),
    targets = list(semantic_model = list(id = "model")),
    principal_type = "service_principal"
  )
  app <- env$playground_shiny_app(
    sandbox,
    getwd(),
    mode = "delegated",
    dispatch = dispatch,
    config = list()
  )
  shiny::testServer(app$serverFuncSource(), {
    session$flushReact()
    session$setInputs(
      source = "dax",
      category = "All",
      min_id = 1,
      backend = "odbc",
      load = 1L
    )
    shiny_test_wait(function() file.exists(started))
    expect_identical(query$status(), "running")
    session$setInputs(ping = 1L)
    expect_identical(output$ping_status, "Responses: 1")
    session$setInputs(logout = 1L)
    expect_null(generation())
    session$setInputs(login = 1L)
    writeLines("release", release)
    shiny_test_wait(function() identical(query$status(), "success"))
    session$flushReact()
    expect_identical(query$result()$generation, "user-one")
    expect_identical(query$result()$results$dax$pid == Sys.getpid(), FALSE)
    expect_s3_class(rlang::catch_cnd(selected_result()), "shiny.silent.error")
    session$setInputs(load_all = 1L)
    shiny_test_wait(function() identical(query$status(), "success"))
    session$flushReact()
    expect_identical(query$result()$generation, "user-two")
    expect_named(current_results(), c("dax", "discovery"))
    expect_identical(selected_result()$data$Category, "user-two")
    session$setInputs(category = "B")
    expect_s3_class(rlang::catch_cnd(selected_result()), "shiny.silent.error")
    session$setInputs(logout = 2L)
    expect_s3_class(rlang::catch_cnd(current_results()), "shiny.silent.error")
  })
})
