# Fabric integration coverage: authentication and resource discovery.
# These tests use the live sandbox credentials and manifest to confirm that a
# user can sign in, find the provisioned workspace, and resolve its test items.

test_that("fabricQueryR acquires a live Fabric token through AzureAuth", {
  manifest <- fabric_test_manifest()
  auth <- fabric_test_azure_auth_config()

  workspaces <- fabric_workspaces(
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args
  )

  expect_true(
    manifest$workspace_id %in% purrr::map_chr(workspaces, "id")
  )
})

test_that("Fabric discovery resolves sandbox workspaces and item targets", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()

  workspaces <- fabric_workspaces(token = token)
  workspace <- purrr::keep(
    workspaces,
    ~ .x$id == manifest$workspace_id
  )[[1L]]
  expect_equal(workspace$displayName, manifest$workspace_name)

  find_item <- function(items, id) {
    matches <- purrr::keep(items, ~ identical(.x$id, id))
    expect_length(matches, 1L, info = id)
    matches[[1L]]
  }

  items <- fabric_items(
    workspace,
    token = token
  )
  expected_items <- c(
    "TestLakehouse",
    "SeedFixtures",
    "JobFixtures",
    "TestPipeline",
    "TestSparkJob",
    "TestWarehouse",
    "TestSQLDatabase",
    "TestEventhouse",
    "TestKQLDatabase",
    "TestSemanticModel",
    "TestArrowSemanticModel",
    "TestGraphQL"
  )
  for (name in expected_items) {
    expected <- manifest$items[[name]]
    discovered <- find_item(items, expected$id)
    expect_equal(discovered$type, expected$type, info = name)
    expect_equal(discovered$displayName, expected$display_name, info = name)
  }

  lakehouses <- fabric_lakehouses(workspace, token = token)
  lakehouse <- find_item(lakehouses, manifest$items$TestLakehouse$id)
  expect_equal(
    lakehouse$sql_server,
    manifest$items$TestLakehouse$sql_endpoint
  )
  expect_equal(
    lakehouse$one_lake_tables_path,
    manifest$items$TestLakehouse$one_lake_tables_path
  )
  expect_equal(lakehouse$livy_url, manifest$items$TestLakehouse$livy_url)

  warehouses <- fabric_warehouses(workspace, token = token)
  warehouse <- find_item(warehouses, manifest$items$TestWarehouse$id)
  expect_equal(
    warehouse$sql_server,
    manifest$items$TestWarehouse$connection_string
  )
  expect_equal(
    warehouse$sql_database,
    manifest$items$TestWarehouse$database_name
  )

  sql_databases <- fabric_sql_databases(workspace, token = token)
  sql_database <- find_item(
    sql_databases,
    manifest$items$TestSQLDatabase$id
  )
  expect_equal(
    sql_database$sql_connection_string,
    manifest$items$TestSQLDatabase$connection_string
  )
  expect_equal(
    sql_database$sql_server,
    manifest$items$TestSQLDatabase$server_fqdn
  )
  expect_equal(
    sql_database$sql_database,
    manifest$items$TestSQLDatabase$database_name
  )

  semantic_models <- fabric_semantic_models(workspace, token = token)
  model <- find_item(semantic_models, manifest$items$TestSemanticModel$id)
  expect_equal(model$id, manifest$items$TestSemanticModel$id)
  expect_equal(model$workspaceId, manifest$workspace_id)
  expect_match(model$dax_connection_string, "powerbi://", fixed = TRUE)

  notebooks <- fabric_notebooks(workspace, token = token)
  notebook_ids <- purrr::map_chr(notebooks, "id")
  expect_true(manifest$items$SeedFixtures$id %in% notebook_ids)
  expect_true(manifest$items$JobFixtures$id %in% notebook_ids)

  graphql_apis <- fabric_graphql_apis(workspace, token = token)
  expect_true(
    manifest$items$TestGraphQL$id %in% purrr::map_chr(graphql_apis, "id")
  )

  eventhouses <- fabric_eventhouses(workspace, token = token)
  eventhouse <- find_item(eventhouses, manifest$items$TestEventhouse$id)
  expect_equal(
    eventhouse$query_service_uri,
    manifest$items$TestEventhouse$query_service_uri
  )

  kql_databases <- fabric_kql_databases(workspace, token = token)
  kql_database <- find_item(
    kql_databases,
    manifest$items$TestKQLDatabase$id
  )
  expect_equal(
    kql_database$query_service_uri,
    manifest$items$TestKQLDatabase$query_service_uri
  )
})
