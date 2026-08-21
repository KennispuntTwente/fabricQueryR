# Fabric integration coverage: authentication and resource discovery
# These tests use the live sandbox credentials and manifest to confirm that a
# user can sign in, find the provisioned workspace, and resolve its test items

test_that("fabricQueryR acquires a live Fabric token through AzureAuth", {
  manifest <- fabric_test_manifest()
  expected_lane <- Sys.getenv("FABRIC_SPARK_RUNTIME_LANE")
  if (nzchar(expected_lane)) {
    expect_identical(manifest$runtime$lane, expected_lane)
    core_lane <- identical(expected_lane, "core")
    expected_runtime <- if (core_lane) "1.3" else "2.0"
    expected_spark <- if (core_lane) "^3[.]5[.]" else "^4[.]1[.]"
    expected_delta <- if (core_lane) "^3[.]2[.]" else "^4[.]2[.]"
    expect_identical(manifest$runtime$fabric_runtime, expected_runtime)
    expect_match(manifest$runtime$spark_version, expected_spark)
    expect_match(manifest$runtime$delta_version, expected_delta)
  }
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
  credential <- fabric_credential(token = token)
  admin_monitoring <- Filter(
    function(value) identical(value$type, "AdminWorkspace"),
    workspaces
  )
  for (admin_workspace in admin_monitoring) {
    resolved_admin <- fabric_resolve_workspace(
      admin_workspace,
      credential,
      .fabric_api_base
    )
    expect_identical(resolved_admin$id, admin_workspace$id)
  }

  admin_workspaces <- fabric_workspaces(
    roles = "Admin",
    prefer_workspace_endpoints = TRUE,
    token = token
  )
  admin_workspace_ids <- purrr::map_chr(admin_workspaces, "id")
  expect_true(manifest$workspace_id %in% admin_workspace_ids)
  preferred_workspace <- purrr::keep(
    admin_workspaces,
    ~ identical(.x$id, manifest$workspace_id)
  )[[1L]]
  expect_match(
    preferred_workspace$oneLakeEndpoints$dfsEndpoint,
    "^https://"
  )
  workspace <- preferred_workspace

  find_item <- function(items, id) {
    matches <- purrr::keep(items, ~ identical(.x$id, id))
    expect_equal(length(matches), 1L, info = id)
    matches[[1L]]
  }

  items <- fabric_items(
    workspace,
    detail = TRUE,
    token = token
  )
  identity_items <- fabric_items(
    workspace,
    include = "DefaultIdentity",
    token = token
  )
  expect_setequal(
    purrr::map_chr(identity_items, "id"),
    purrr::map_chr(items, "id")
  )
  expected_items <- c(
    "TestLakehouse",
    "SeedFixtures",
    "JobFixtures",
    "TestPipeline",
    "TestSparkJob",
    "TestWarehouse",
    "TestWarehouseSnapshot",
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

  warehouse_snapshots <- fabric_warehouse_snapshots(workspace, token = token)
  warehouse_snapshot <- find_item(
    warehouse_snapshots,
    manifest$items$TestWarehouseSnapshot$id
  )
  expect_equal(
    warehouse_snapshot$sql_server,
    manifest$items$TestWarehouseSnapshot$connection_string
  )
  expect_equal(
    warehouse_snapshot$sql_database,
    manifest$items$TestWarehouseSnapshot$database_name
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

  pipelines <- fabric_data_pipelines(workspace, token = token)
  pipeline <- find_item(pipelines, manifest$items$TestPipeline$id)
  expect_identical(pipeline$type, "DataPipeline")

  spark_jobs <- fabric_spark_job_definitions(workspace, token = token)
  spark_job <- find_item(spark_jobs, manifest$items$TestSparkJob$id)
  expect_identical(spark_job$type, "SparkJobDefinition")

  environments <- fabric_environments(workspace, token = token)
  expect_true(all(
    purrr::map_chr(environments, "type") == "Environment"
  ))

  functions <- fabric_user_data_functions(workspace, token = token)
  expect_true(all(
    purrr::map_chr(functions, "type") == "UserDataFunction"
  ))

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

test_that("Fabric long-running operations complete a live Warehouse creation", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  credential <- fabric_credential(token = token)
  display_name <- paste0(
    "fabricqueryr_operation_",
    Sys.getpid(),
    "_",
    format(Sys.time(), "%Y%m%d%H%M%S", tz = "UTC")
  )
  item_id <- NULL
  cleaned <- FALSE

  cleanup <- function(strict = FALSE) {
    if (isTRUE(cleaned)) {
      return(invisible(TRUE))
    }
    ids <- item_id
    if (is.null(ids)) {
      items <- try(
        fabric_items(manifest$workspace_id, token = token),
        silent = TRUE
      )
      if (!inherits(items, "try-error")) {
        matches <- Filter(
          function(item) identical(item$displayName, display_name),
          items
        )
        ids <- vapply(matches, `[[`, character(1), "id")
      }
    }
    outcomes <- lapply(ids, function(id) {
      request <- httr2::request(paste0(
        .fabric_api_base,
        "/workspaces/",
        manifest$workspace_id,
        "/items/",
        id
      )) |>
        httr2::req_url_query(hardDelete = "true") |>
        httr2::req_method("DELETE")
      try(
        .httr2_perform(
          request,
          credential = credential,
          audience = .fabric_audience$fabric,
          idempotent = TRUE,
          accepted_status = 404L
        ),
        silent = TRUE
      )
    })
    failures <- vapply(outcomes, inherits, logical(1), "try-error")
    if (isTRUE(strict) && any(failures)) {
      rlang::cnd_signal(attr(outcomes[[which(failures)[[1L]]]], "condition"))
    }
    cleaned <<- !any(failures)
    invisible(TRUE)
  }
  on.exit(cleanup(strict = FALSE), add = TRUE)

  request <- httr2::request(paste0(
    .fabric_api_base,
    "/workspaces/",
    manifest$workspace_id,
    "/items"
  )) |>
    httr2::req_method("POST") |>
    httr2::req_body_json(
      list(
        displayName = display_name,
        type = "Warehouse",
        description = "Temporary fabricQueryR long-running-operation test",
        creationPayload = list(
          collationType = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"
        )
      ),
      auto_unbox = TRUE
    )
  operation <- .fabric_operation_submit(request, credential)

  expect_s3_class(operation, "fabric_operation")
  expect_false(
    operation$immediate,
    info = "Live Warehouse creation must exercise Fabric's documented 202 path"
  )
  expect_true(fabric_is_guid(operation$id))
  expect_true(is.numeric(operation$retry_after))

  initial_state <- fabric_operation_status(operation)
  expect_s3_class(initial_state, "fabric_operation_state")
  expect_true(initial_state$status %in% c("NotStarted", "Running", "Succeeded"))
  expect_true(
    is.null(initial_state$percent_complete) ||
      initial_state$percent_complete >= 0
  )

  result <- fabric_operation_result(
    initial_state,
    timeout = 900
  )
  item_id <- result$value$id

  expect_s3_class(result, "fabric_operation_result")
  expect_false(result$empty)
  expect_true(fabric_is_guid(item_id))
  expect_identical(result$value$displayName, display_name)
  expect_identical(result$value$type, "Warehouse")
  expect_identical(result$value$workspaceId, manifest$workspace_id)

  cleanup(strict = TRUE)
  expect_true(cleaned)
})

test_that("the default AzureAuth flow works with a delegated identity", {
  auth <- fabric_test_delegated_auth_config()
  manifest <- fabric_test_manifest()

  workspaces <- fabric_workspaces(
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args
  )

  expect_true(manifest$workspace_id %in% purrr::map_chr(workspaces, "id"))
})

test_that("a valid least-privilege identity returns a typed 403", {
  limited_token <- fabric_test_optional_environment(
    "FABRIC_TEST_LIMITED_API_TOKEN",
    "Least-privilege Fabric API coverage"
  )
  denied_workspace <- fabric_test_optional_environment(
    "FABRIC_TEST_DENIED_WORKSPACE_ID",
    "Least-privilege Fabric API coverage"
  )

  error <- expect_error(
    fabric_items(denied_workspace, token = limited_token),
    class = "fabric_http_error"
  )
  expect_identical(error$status, 403L)
  expect_null(error$response_metadata$body$token)
})
