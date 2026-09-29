test_that("profiles keep Fabric, SQL and GraphQL permissions separate", {
  profiles <- fabric_shiny_profiles(c("sql", "graphql", "fabric"))
  expect_identical(profiles$sql$resource, "https://database.windows.net/")
  expect_identical(
    profiles$sql$scopes,
    "https://database.windows.net//user_impersonation"
  )
  expect_identical(profiles$graphql$scopes, .fabric_audience$graphql)
  expect_identical(profiles$graphql$target, "power_bi")
  expect_identical(
    profiles$fabric$scopes,
    paste0(
      "https://api.fabric.microsoft.com/",
      c("Workspace.Read.All", "Item.Read.All")
    )
  )
  expect_identical(
    fabric_shiny_profiles(
      "fabric",
      list(fabric = "Item.Read.All")
    )$fabric$scopes,
    "https://api.fabric.microsoft.com/Item.Read.All"
  )
})

test_that("configuration rejects ambiguous scope and destination declarations", {
  cases <- list(
    list(services = character()),
    list(services = c("sql", "sql")),
    list(services = "storage"),
    list(services = "sql", scopes = list(sql = ".default")),
    list(
      services = "sql",
      scopes = list(sql = "https://graph.microsoft.com/User.Read")
    ),
    list(services = "sql", scopes = list(fabric = "Item.Read.All")),
    list(services = "graphql", scopes = list(graphql = "Dataset.Read.All")),
    list(services = "sql", endpoint_hosts = list(sql = "*.example.com"))
  )
  for (args in cases) {
    error <- rlang::catch_cnd(do.call(fabric_shiny_profiles, args))
    expect_s3_class(error, "fabric_shiny_error")
    expect_identical(error$reason, "invalid_configuration")
  }
})

test_that("configuration creates an ID-token validated Microsoft target client", {
  skip_if_not_installed("shinyOAuth")
  skip_if(!"token_targets" %in% names(formals(shinyOAuth::oauth_client)))
  config <- fabric_shiny_config(
    "11111111-1111-1111-1111-111111111111",
    "app",
    "synthetic-secret",
    "http://localhost:8100/",
    services = c("fabric", "sql", "graphql")
  )
  expect_s3_class(config, "fabric_shiny_config")
  expect_named(config$client@token_targets, c("fabric", "sql", "power_bi"))
  expect_identical(config$client@default_token_target, "fabric")
  expect_identical(config$client@provider@userinfo_required, FALSE)
  expect_identical(config$client@provider@id_token_validation, TRUE)
  expect_identical(config$client@provider@token_target_mode, "microsoft")
  printed <- paste(capture.output(print(config)), collapse = "\n")
  expect_match(printed, "fabric, sql, graphql", fixed = TRUE)
  expect_identical(grepl("synthetic-secret", printed, fixed = TRUE), FALSE)
})
