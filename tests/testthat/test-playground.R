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
