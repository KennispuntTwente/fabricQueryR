test_that("Fabric items preserve discovery fields and legacy records", {
  record <- list(
    id = "11111111-1111-4111-8111-111111111111",
    workspaceId = "22222222-2222-4222-8222-222222222222",
    displayName = "Sales Lake",
    type = "Lakehouse",
    properties = list(defaultSchema = "dbo"),
    print = "service-field"
  )
  item <- fabric_r6_record(
    record,
    legacy_class = c("fabric_item", "list")
  )

  expect_s3_class(item, "FabricLakehouse")
  expect_identical(item$id, record$id)
  expect_identical(item$properties, record$properties)
  expect_identical(item$get("print"), "service-field")
  expect_identical(item$get("missing", "fallback"), "fallback")
  expect_identical(
    item$as_list(),
    structure(
      record,
      class = c("fabric_item", "list")
    )
  )
  expect_identical(as.list(item), item$as_list())
  expect_setequal(names(item), names(record))
  expect_identical(length(item), length(record))
  expect_identical(bindingIsActive("id", item), TRUE)
})

test_that("Fabric item factories select actionable R6 subclasses", {
  make <- function(type) {
    fabric_r6_record(
      list(
        id = "11111111-1111-4111-8111-111111111111",
        workspaceId = "22222222-2222-4222-8222-222222222222",
        displayName = type,
        type = type
      ),
      legacy_class = c("fabric_item", "list")
    )
  }

  expect_s3_class(make("Lakehouse"), "FabricLakehouse")
  expect_s3_class(make("Warehouse"), "FabricWarehouse")
  expect_s3_class(make("WarehouseSnapshot"), "FabricWarehouseSnapshot")
  expect_s3_class(make("MirroredDatabase"), "FabricMirroredDatabase")
  expect_s3_class(make("SQLDatabase"), "FabricSqlDatabase")
  expect_s3_class(make("SemanticModel"), "FabricSemanticModel")
  expect_s3_class(make("Eventhouse"), "FabricEventhouse")
  expect_s3_class(make("KQLDatabase"), "FabricKqlDatabase")
  expect_s3_class(make("GraphQLApi"), "FabricGraphQLApi")
  expect_s3_class(make("Notebook"), "FabricJobItem")
  expect_s3_class(make("DataPipeline"), "FabricJobItem")
  expect_s3_class(make("SparkJobDefinition"), "FabricJobItem")
  expect_s3_class(make("Environment"), "FabricItem")
  expect_s3_class(
    fabric_r6_record(
      list(id = "11111111-1111-4111-8111-111111111111", type = NA_character_),
      legacy_class = c("fabric_item", "list")
    ),
    "FabricItem"
  )

  workspace <- fabric_r6_record(
    list(
      id = "22222222-2222-4222-8222-222222222222",
      displayName = "Analytics",
      type = "Workspace"
    ),
    legacy_class = c("fabric_workspace", "list")
  )
  expect_s3_class(workspace, "FabricWorkspace")
})

test_that("serialized R6 records omit discovery credentials", {
  item <- fabric_r6_record(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      displayName = "Sales Lake",
      type = "Lakehouse"
    ),
    legacy_class = c("fabric_item", "list"),
    credential = fabric_credential(token = "discovery-token")
  )

  copy <- unserialize(serialize(item, NULL))

  expect_identical(copy$as_list(), item$as_list())
  expect_null(copy$.__enclos_env__$private$credential())
})

test_that("R6 records pass through the shared discovery adapter", {
  item <- fabric_r6_record(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      displayName = "Sales Lake",
      type = "Lakehouse"
    ),
    legacy_class = c("fabric_item", "list")
  )

  expect_identical(fabric_as_record(item), item$as_list())
  expect_identical(fabric_record_value(item, "displayName"), "Sales Lake")
})

test_that("actionable R6 records resolve through existing target adapters", {
  warehouse <- fabric_r6_record(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      displayName = "Sales Warehouse",
      type = "Warehouse",
      sql_server = "sales.datawarehouse.fabric.microsoft.com"
    ),
    legacy_class = c("fabric_item", "list")
  )
  info <- warehouse$sql_connection_info()
  expect_equal(info$server, "sales.datawarehouse.fabric.microsoft.com")
  expect_equal(info$database, "Sales Warehouse")
  expect_equal(info$target_type, "warehouse")

  database <- fabric_r6_record(
    list(
      id = "33333333-3333-4333-8333-333333333333",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      displayName = "Telemetry",
      type = "KQLDatabase",
      query_service_uri = "https://query.kusto.fabric.microsoft.com"
    ),
    legacy_class = c("fabric_item", "list")
  )
  kql_target <- kusto_resolve_target(database)
  expect_equal(
    kql_target$url,
    "https://query.kusto.fabric.microsoft.com/v2/rest/query"
  )
  expect_equal(kql_target$database, "Telemetry")

  api <- fabric_r6_record(
    list(
      id = "44444444-4444-4444-8444-444444444444",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      displayName = "Products API",
      type = "GraphQLApi",
      graphql_endpoint = paste0(
        "https://api.fabric.microsoft.com/v1/workspaces/",
        "22222222-2222-4222-8222-222222222222/graphqlapis/",
        "44444444-4444-4444-8444-444444444444/graphql"
      )
    ),
    legacy_class = c("fabric_item", "list")
  )
  expect_equal(graphql_resolve_endpoint(api), api$graphql_endpoint)
})

test_that("Lakehouse methods delegate identity and discovery credentials", {
  calls <- new.env(parent = emptyenv())
  local_mocked_bindings(
    fabric_lakehouse_read_table = function(
      lakehouse,
      table,
      limit = NULL,
      token = NULL,
      ...
    ) {
      calls$read <- list(
        lakehouse = lakehouse,
        table = table,
        limit = limit,
        token = token
      )
      "read-result"
    },
    fabric_livy_session = function(livy_url, token = NULL, ...) {
      calls$livy <- list(livy_url = livy_url, token = token)
      "session-result"
    },
    fabric_onelake_shortcut_get = function(
      item,
      path,
      name,
      token = NULL,
      ...
    ) {
      calls$shortcut <- list(
        item = item,
        path = path,
        name = name,
        token = token
      )
      "shortcut-result"
    }
  )
  credential <- fabric_credential(token = "discovery-token")
  item <- fabric_r6_record(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      displayName = "Sales Lake",
      type = "Lakehouse",
      livy_url = "https://api.fabric.test/livy"
    ),
    legacy_class = c("fabric_item", "list"),
    credential = credential
  )

  expect_identical(item$read_table("orders", limit = 5), "read-result")
  expect_identical(calls$read$lakehouse, item)
  expect_identical(calls$read$table, "orders")
  expect_identical(calls$read$limit, 5)
  expect_s3_class(calls$read$token, "fabric_credential")

  expect_identical(item$livy_session(), "session-result")
  expect_identical(calls$livy$livy_url, item)
  expect_s3_class(calls$livy$token, "fabric_credential")

  expect_identical(
    item$shortcut("Files", "orders"),
    "shortcut-result"
  )
  expect_identical(calls$shortcut$item, item)
  expect_identical(calls$shortcut$path, "Files")
  expect_identical(calls$shortcut$name, "orders")
  expect_s3_class(calls$shortcut$token, "fabric_credential")

  item$read_table("orders", token = "override-token")
  expect_identical(calls$read$token, "override-token")
})

test_that("SQL item subclasses delegate common and workload-specific methods", {
  calls <- new.env(parent = emptyenv())
  local_mocked_bindings(
    fabric_sql_query = function(server, sql, token = NULL, ...) {
      calls$sql <- list(server = server, sql = sql, token = token)
      "sql-result"
    },
    fabric_warehouse_read_table = function(
      warehouse,
      table,
      token = NULL,
      ...
    ) {
      calls$warehouse <- list(
        warehouse = warehouse,
        table = table,
        token = token
      )
      "warehouse-result"
    },
    fabric_mirrored_database_tables = function(
      mirrored_database,
      token = NULL,
      ...
    ) {
      calls$mirrored <- list(
        mirrored_database = mirrored_database,
        token = token
      )
      "mirrored-result"
    }
  )
  credential <- fabric_credential(token = "discovery-token")
  make <- function(type) {
    fabric_r6_record(
      list(
        id = "11111111-1111-4111-8111-111111111111",
        workspaceId = "22222222-2222-4222-8222-222222222222",
        displayName = type,
        type = type
      ),
      legacy_class = c("fabric_item", "list"),
      credential = credential
    )
  }
  warehouse <- make("Warehouse")
  mirrored <- make("MirroredDatabase")
  sql_database <- make("SQLDatabase")

  expect_identical(sql_database$sql_query("SELECT 1"), "sql-result")
  expect_identical(calls$sql$server, sql_database)
  expect_identical(calls$sql$sql, "SELECT 1")
  expect_s3_class(calls$sql$token, "fabric_credential")

  expect_identical(warehouse$read_table("orders"), "warehouse-result")
  expect_identical(calls$warehouse$warehouse, warehouse)
  expect_identical(calls$warehouse$table, "orders")
  expect_s3_class(calls$warehouse$token, "fabric_credential")

  expect_identical(mirrored$tables(), "mirrored-result")
  expect_identical(calls$mirrored$mirrored_database, mirrored)
  expect_s3_class(calls$mirrored$token, "fabric_credential")
})

test_that("KQL and GraphQL subclasses delegate query workflows", {
  calls <- new.env(parent = emptyenv())
  local_mocked_bindings(
    fabric_kql_query = function(cluster, query, token = NULL, ...) {
      calls$kql <- list(cluster = cluster, query = query, token = token)
      "kql-result"
    },
    fabric_kql_ingest = function(
      cluster,
      table,
      sources,
      format,
      token = NULL,
      ...
    ) {
      calls$ingest <- list(
        cluster = cluster,
        table = table,
        sources = sources,
        format = format,
        token = token
      )
      "ingest-result"
    },
    fabric_graphql_query = function(api, query, token = NULL, ...) {
      calls$graphql <- list(api = api, query = query, token = token)
      "graphql-result"
    }
  )
  credential <- fabric_credential(token = "discovery-token")
  make <- function(type) {
    fabric_r6_record(
      list(
        id = "11111111-1111-4111-8111-111111111111",
        workspaceId = "22222222-2222-4222-8222-222222222222",
        displayName = type,
        type = type
      ),
      legacy_class = c("fabric_item", "list"),
      credential = credential
    )
  }
  database <- make("KQLDatabase")
  api <- make("GraphQLApi")

  expect_identical(database$query("StormEvents | take 5"), "kql-result")
  expect_identical(calls$kql$cluster, database)
  expect_identical(calls$kql$query, "StormEvents | take 5")
  expect_s3_class(calls$kql$token, "fabric_credential")

  expect_identical(
    database$ingest("events", "https://data.test/events.csv", "csv"),
    "ingest-result"
  )
  expect_identical(calls$ingest$cluster, database)
  expect_identical(calls$ingest$table, "events")
  expect_identical(calls$ingest$format, "csv")
  expect_s3_class(calls$ingest$token, "fabric_credential")

  expect_identical(api$query("{ products { id } }"), "graphql-result")
  expect_identical(calls$graphql$api, api)
  expect_identical(calls$graphql$query, "{ products { id } }")
  expect_s3_class(calls$graphql$token, "fabric_credential")
})

test_that("Workspace methods keep R6 output through discovery chains", {
  calls <- new.env(parent = emptyenv())
  local_mocked_bindings(
    fabric_lakehouses = function(
      workspace,
      detail = TRUE,
      token = NULL,
      output = "list",
      ...
    ) {
      calls$lakehouses <- list(
        workspace = workspace,
        detail = detail,
        token = token,
        output = output
      )
      list("lakehouse-result")
    }
  )
  workspace <- fabric_r6_record(
    list(
      id = "22222222-2222-4222-8222-222222222222",
      displayName = "Analytics",
      type = "Workspace"
    ),
    legacy_class = c("fabric_workspace", "list"),
    credential = fabric_credential(token = "discovery-token")
  )

  expect_identical(
    workspace$lakehouses(detail = FALSE),
    list("lakehouse-result")
  )
  expect_identical(calls$lakehouses$workspace, workspace)
  expect_identical(calls$lakehouses$detail, FALSE)
  expect_identical(calls$lakehouses$output, "r6")
  expect_s3_class(calls$lakehouses$token, "fabric_credential")
})

test_that("Semantic-model and job methods delegate to public functions", {
  calls <- new.env(parent = emptyenv())
  local_mocked_bindings(
    fabric_pbi_dax_query = function(connstr, dax, token = NULL, ...) {
      calls$dax <- list(connstr = connstr, dax = dax, token = token)
      "dax-result"
    },
    fabric_job_run = function(item, token = NULL, ...) {
      calls$job <- list(item = item, token = token)
      "job-result"
    }
  )
  credential <- fabric_credential(token = "discovery-token")
  model <- fabric_r6_record(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      displayName = "Sales Model",
      type = "SemanticModel"
    ),
    legacy_class = c("fabric_item", "list"),
    credential = credential
  )
  notebook <- fabric_r6_record(
    list(
      id = "33333333-3333-4333-8333-333333333333",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      displayName = "Load Sales",
      type = "Notebook"
    ),
    legacy_class = c("fabric_item", "list"),
    credential = credential
  )

  expect_identical(model$dax_query("EVALUATE ROW(\"x\", 1)"), "dax-result")
  expect_identical(calls$dax$connstr, model)
  expect_s3_class(calls$dax$token, "fabric_credential")
  expect_identical(notebook$run(), "job-result")
  expect_identical(calls$job$item, notebook)
  expect_s3_class(calls$job$token, "fabric_credential")
})

test_that("Discovery factories return R6 by default with an explicit list mode", {
  record <- list(
    id = "11111111-1111-4111-8111-111111111111",
    workspaceId = "22222222-2222-4222-8222-222222222222",
    displayName = "Sales Lake",
    type = "Lakehouse"
  )

  object <- fabric_item_list(list(record))[[1L]]
  legacy <- fabric_item_list(list(record), output = "list")[[1L]]
  expect_s3_class(legacy, "fabric_item")
  expect_s3_class(object, "FabricLakehouse")
  expect_identical(object$as_list(), legacy)
})
