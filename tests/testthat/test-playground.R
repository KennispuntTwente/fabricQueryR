test_that("playground R files parse", {
  files <- list.files(
    .playground_test_path(),
    pattern = "[.]R$",
    full.names = TRUE
  )

  expect_setequal(
    basename(files),
    c("playground.R", "sandbox.R", "tour.R")
  )
  for (file in files) {
    expect_type(parse(file = file), "expression")
  }
})

test_that("playground exposes persistent sandbox demos", {
  environment <- new.env(parent = globalenv())
  sys.source(
    .playground_test_path("sandbox.R"),
    envir = environment
  )
  sys.source(
    .playground_test_path("playground.R"),
    envir = environment
  )

  functions <- c(
    "connect_playground_sandbox",
    "demo_discovery",
    "demo_sql",
    "demo_arrow_batches",
    "demo_onelake",
    "demo_onelake_write",
    "demo_onelake_shortcut",
    "demo_warehouse_write",
    "write_playground_lakehouse_table",
    "demo_kql",
    "demo_graphql",
    "demo_power_bi",
    "demo_power_bi_refresh",
    "demo_livy",
    "demo_job_history",
    "run_playground_job"
  )
  available <- vapply(
    functions,
    exists,
    logical(1),
    envir = environment,
    mode = "function",
    inherits = FALSE
  )

  expect_identical(unname(available), rep(TRUE, length(functions)))
})

test_that("playground discovery and refresh execute with R6 sandbox objects", {
  environment <- new.env(parent = globalenv())
  sys.source(.playground_test_path("sandbox.R"), envir = environment)
  sys.source(.playground_test_path("playground.R"), envir = environment)
  workspace <- fabric_r6_record(
    list(
      id = "22222222-2222-4222-8222-222222222222",
      displayName = "Sandbox",
      description = "test"
    ),
    legacy_class = c("fabric_workspace", "list")
  )
  sandbox <- structure(
    list(
      workspace = workspace,
      items = list(),
      targets = list(
        semantic_model = list(id = "push-model"),
        arrow_semantic_model = list(id = "import-model")
      ),
      token = function(...) "token"
    ),
    class = "fabricqueryr_playground_sandbox"
  )
  for (name in c(
    "fabric_lakehouses",
    "fabric_warehouses",
    "fabric_kql_databases",
    "fabric_semantic_models"
  )) {
    environment[[name]] <- function(...) list()
  }
  result <- environment$demo_discovery(sandbox)
  expect_identical(
    result$workspace,
    workspace$as_list()[c("displayName", "id", "description")]
  )
  principal <- NULL
  environment$fabric_pbi_refresh <- function(model, token, principal_type) {
    expect_identical(model$id, "import-model")
    principal <<- principal_type
    list(state = "Completed")
  }
  environment$fabric_pbi_refresh_wait <- function(refresh, ...) refresh
  environment$fabric_pbi_refresh_history <- function(...) list()
  for (type in c("delegated", "service_principal")) {
    sandbox$principal_type <- type
    expect_identical(
      environment$demo_power_bi_refresh(sandbox)$completed$state,
      "Completed"
    )
    expect_identical(principal, type)
  }
})

test_that("playground targets allow names shared by different item types", {
  environment <- new.env(parent = globalenv())
  sys.source(
    .playground_test_path("sandbox.R"),
    envir = environment
  )
  items <- list(
    list(id = "lake", displayName = "SharedLake", type = "Lakehouse"),
    list(id = "sql", displayName = "SharedLake", type = "SQLEndpoint"),
    list(id = "event", displayName = "Telemetry", type = "Eventhouse"),
    list(id = "kql", displayName = "Telemetry", type = "KQLDatabase")
  )
  target_names <- c(lakehouse = "SharedLake", kql_database = "Telemetry")
  target_types <- c(lakehouse = "Lakehouse", kql_database = "KQLDatabase")

  targets <- environment$playground_resolve_targets(
    items,
    target_names,
    target_types
  )
  expect_identical(
    vapply(targets, `[[`, character(1), "id"),
    c(
      lakehouse = "lake",
      kql_database = "kql"
    )
  )
  optional <- environment$playground_resolve_targets(
    items,
    c(target_names, sql_database = "TestSQLDatabase"),
    c(target_types, sql_database = "SQLDatabase"),
    optional = "sql_database"
  )
  expect_named(optional, c(names(targets), "sql_database"))
  expect_null(optional$sql_database)
  expect_identical(optional[names(targets)], targets)

  ambiguous <- c(
    items,
    list(
      list(id = "lake-2", displayName = "SharedLake", type = "Lakehouse")
    )
  )
  expect_error(
    environment$playground_resolve_targets(
      ambiguous,
      target_names,
      target_types
    ),
    "Expected one .*Lakehouse.* named .*SharedLake.* but found 2"
  )
})

test_that("playground examples do not embed live Fabric endpoints", {
  files <- .playground_test_path(c("playground.R", "sandbox.R", "tour.R"))
  source <- paste(
    unlist(lapply(files, readLines, warn = FALSE), use.names = FALSE),
    collapse = "\n"
  )

  expect_identical(
    grepl("datawarehouse.fabric.microsoft.com", source, fixed = TRUE),
    FALSE
  )
  expect_identical(
    grepl(
      "api.fabric.microsoft.com/v1/workspaces/[0-9a-f]",
      source
    ),
    FALSE
  )
})

test_that("playground SQL skips absent targets and forwards precision choices", {
  environment <- new.env(parent = globalenv())
  sys.source(.playground_test_path("playground.R"), envir = environment)
  sandbox <- structure(
    list(
      targets = list(warehouse = list(id = "warehouse")),
      token = function(...) "token"
    ),
    class = "fabricqueryr_playground_sandbox"
  )
  calls <- 0L
  environment$fabric_sql_tables <- function(server, ...) {
    expect_identical(server$id, "warehouse")
    data.frame(name = "fabricqueryr_sql_types", schema = "dbo")
  }
  read <- function(server, backend, numeric_policy, ...) {
    expect_identical(server$id, "warehouse")
    expect_identical(backend, "adbc")
    expect_identical(numeric_policy, "exact")
    calls <<- calls + 1L
    data.frame(amount = "10.50")
  }
  environment$fabric_sql_read_table <- read
  environment$fabric_sql_query <- read
  environment$fabric_sql_connection_info <- function(server) list()
  expect_message(
    result <- environment$demo_sql(
      sandbox,
      backend = "adbc",
      targets = c("warehouse", "sql_database"),
      numeric_policy = "exact"
    ),
    "Skipping unavailable SQL target"
  )
  expect_named(result, "warehouse")
  expect_identical(calls, 2L)
  expect_identical(result$warehouse$rows$amount, "10.50")

  error <- tryCatch(
    environment$open_playground_sql_connection(
      sandbox,
      target = "sql_database"
    ),
    error = identity
  )
  expect_s3_class(error, "error")
  expect_match(conditionMessage(error), "sandbox has no", fixed = TRUE)
})

test_that("playground batch demo processes and releases a real Arrow stream", {
  skip_if_not_installed("arrow", "9.0.0")
  skip_if_not_installed("nanoarrow", "0.6.0")
  environment <- new.env(parent = globalenv())
  sys.source(.playground_test_path("playground.R"), envir = environment)
  stream <- nanoarrow::as_nanoarrow_array_stream(data.frame(
    id = 1:3,
    name = c("alpha", "beta", "gamma"),
    amount = c("10.50", "20.00", NA_character_)
  ))
  sandbox <- structure(
    list(
      targets = list(
        warehouse = list(
          sql_query = function(sql, backend, result) {
            expect_identical(backend, "adbc")
            expect_identical(result, "arrow_stream")
            stream
          }
        )
      )
    ),
    class = "fabricqueryr_playground_sandbox"
  )
  result <- environment$demo_arrow_batches(sandbox, backend = "adbc")
  expect_identical(result$rows, 3)
  expect_identical(result$amount_sum, 30.5)
  expect_gte(result$batches, 1L)
  expect_identical(nanoarrow::nanoarrow_pointer_is_valid(stream), FALSE)
})
test_that("playground GraphQL objects retain application authentication", {
  environment <- new.env(parent = globalenv())
  sys.source(.playground_test_path("sandbox.R"), envir = environment)
  audiences <- character()
  provider <- function(audience, force_refresh = FALSE) {
    audiences <<- c(audiences, audience)
    if (!identical(audience, "https://api.fabric.microsoft.com/.default")) {
      rlang::abort("No local AzureAuth token for audience")
    }
    "test-token"
  }
  credential <- environment$playground_discovery_credential(
    provider,
    list(auth_type = "client_credentials")
  )
  api <- fabric_r6_record(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      type = "GraphQLApi"
    ),
    legacy_class = c("fabric_item", "list"),
    credential = credential
  )
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(list(data = list(typename = "Query")), url = req$url)
  })
  result <- api$query("{ typename: __typename }")
  expect_identical(result$data$typename, "Query")
  expect_identical(audiences, "https://api.fabric.microsoft.com/.default")
  delegated <- environment$playground_discovery_credential(provider, list())
  expect_identical(delegated$client_credentials, FALSE)
})
