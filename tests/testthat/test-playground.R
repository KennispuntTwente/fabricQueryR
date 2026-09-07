test_that("playground R files parse", {
  files <- list.files(
    .playground_test_path(),
    pattern = "[.]R$",
    full.names = TRUE
  )

  expect_setequal(
    basename(files),
    c("playground.R", "sandbox.R")
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
      targets = list(semantic_model = list(id = "model")),
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
  files <- .playground_test_path(c("playground.R", "sandbox.R"))
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
