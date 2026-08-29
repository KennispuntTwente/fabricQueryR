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

test_that("R6 records validate construction, access, and method arguments", {
  invalid_record <- rlang::catch_cnd(
    FabricItem$new(
      record = "not-a-record",
      legacy_class = c("fabric_item", "list")
    )
  )
  expect_match(conditionMessage(invalid_record), "one named Fabric record")

  invalid_class <- rlang::catch_cnd(
    FabricItem$new(record = list(id = "item-id"), legacy_class = character())
  )
  expect_match(conditionMessage(invalid_class), "non-empty class names")

  invalid_credential <- rlang::catch_cnd(
    FabricItem$new(
      record = list(id = "item-id"),
      legacy_class = c("fabric_item", "list"),
      credential = "token"
    )
  )
  expect_match(
    conditionMessage(invalid_credential),
    "internal Fabric credential"
  )

  item <- r6_test_record("Environment")
  read_only <- rlang::catch_cnd({
    item$id <- "replacement"
  })
  expect_s3_class(read_only, "fabric_r6_read_only_error")
  expect_identical(item$id, "11111111-1111-4111-8111-111111111111")

  invalid_name <- rlang::catch_cnd(item$get(character()))
  expect_match(conditionMessage(invalid_name), "one non-empty field name")

  unnamed <- rlang::catch_cnd(item$details(1))
  expect_match(conditionMessage(unnamed), "must be named")
  duplicate <- rlang::catch_cnd(item$details(item = "replacement"))
  expect_s3_class(duplicate, "fabric_r6_argument_error")

  workspace <- r6_test_record("Workspace")
  list_output <- rlang::catch_cnd(workspace$items(output = "list"))
  expect_s3_class(list_output, "fabric_r6_argument_error")

  no_workspace <- fabric_r6_record(
    list(id = "item-id", displayName = "orphan", type = "Environment"),
    legacy_class = c("fabric_item", "list")
  )
  missing_context <- rlang::catch_cnd(no_workspace$details())
  expect_s3_class(missing_context, "fabric_r6_context_error")

  output <- capture.output(print(item))
  expect_match(paste(output, collapse = "\n"), "Fabric Item")
  expect_match(paste(output, collapse = "\n"), "Environment")
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

test_that("R6 classes expose the complete supported method surface", {
  expected <- list(
    Lakehouse = c(
      "details",
      "sql_connection_info",
      "sql_connect",
      "sql_query",
      "sql_tables",
      "sql_views",
      "sql_read_table",
      "schemas",
      "table",
      "tables",
      "read_table",
      "read_delta_table",
      "load_table",
      "write_table",
      "livy_query",
      "livy_session",
      "livy_batch_submit",
      "onelake_list",
      "onelake_metadata",
      "onelake_read_file",
      "onelake_write_file",
      "onelake_download",
      "onelake_upload",
      "onelake_delete",
      "schema_exists",
      "table_exists",
      "shortcuts",
      "shortcut",
      "shortcut_create",
      "shortcuts_bulk_create",
      "shortcut_delete"
    ),
    Warehouse = c(
      "sql_connection_info",
      "sql_connect",
      "sql_query",
      "sql_tables",
      "sql_views",
      "sql_read_table",
      "schemas",
      "table",
      "tables",
      "read_table",
      "write_table",
      "read_delta_table"
    ),
    KQLDatabase = c(
      "query",
      "tables",
      "read_table",
      "ingest",
      "write_table",
      "export",
      "ingestion_status",
      "ingestion_wait"
    ),
    GraphQLApi = c("query", "schema", "paginate"),
    SemanticModel = c(
      "dax_query",
      "refresh",
      "refresh_history",
      "refresh_status",
      "refresh_wait",
      "refresh_cancel"
    ),
    Notebook = c(
      "run",
      "status",
      "wait",
      "cancel",
      "instances",
      "schedules",
      "schedule_create",
      "schedule_update",
      "schedule_delete"
    )
  )

  for (type in names(expected)) {
    object <- r6_test_record(type)
    expect_setequal(
      intersect(ls(object, all.names = TRUE), expected[[type]]),
      expected[[type]]
    )
  }
})

test_that("Workspace and generic item methods all delegate their context", {
  calls <- new.env(parent = emptyenv())
  mapping <- c(
    items = "fabric_items",
    item = "fabric_item",
    lakehouses = "fabric_lakehouses",
    warehouses = "fabric_warehouses",
    warehouse_snapshots = "fabric_warehouse_snapshots",
    mirrored_databases = "fabric_mirrored_databases",
    sql_databases = "fabric_sql_databases",
    semantic_models = "fabric_semantic_models",
    eventhouses = "fabric_eventhouses",
    kql_databases = "fabric_kql_databases",
    notebooks = "fabric_notebooks",
    data_pipelines = "fabric_data_pipelines",
    spark_job_definitions = "fabric_spark_job_definitions",
    environments = "fabric_environments",
    user_data_functions = "fabric_user_data_functions",
    graphql_apis = "fabric_graphql_apis",
    shortcut_cache_reset = "fabric_onelake_shortcut_cache_reset"
  )
  local_r6_method_mocks(unique(unname(mapping)), calls)
  workspace <- r6_test_record("Workspace")
  arguments <- list(item = list(item = "target"))

  for (method in names(mapping)) {
    call <- expect_r6_delegation(
      workspace,
      method,
      arguments[[method]] %||% list(),
      mapping[[method]],
      calls,
      "workspace"
    )
    if (!identical(method, "shortcut_cache_reset")) {
      expect_identical(call$output, "r6", info = method)
    }
  }

  item <- r6_test_record("Environment")
  call <- expect_r6_delegation(
    item,
    "details",
    list(),
    "fabric_item",
    calls,
    "item"
  )
  expect_identical(call$workspace, item$workspaceId)
  expect_identical(call$output, "r6")
})

test_that("Lakehouse methods all delegate their item identity", {
  calls <- new.env(parent = emptyenv())
  mapping <- c(
    sql_connection_info = "fabric_sql_connection_info",
    sql_connect = "fabric_sql_connect",
    sql_query = "fabric_sql_query",
    sql_tables = "fabric_sql_tables",
    sql_views = "fabric_sql_views",
    sql_read_table = "fabric_sql_read_table",
    schemas = "fabric_lakehouse_schemas",
    table = "fabric_lakehouse_table",
    tables = "fabric_lakehouse_tables",
    read_table = "fabric_lakehouse_read_table",
    read_delta_table = "fabric_onelake_read_delta_table",
    load_table = "fabric_lakehouse_load_table",
    write_table = "fabric_lakehouse_write_table",
    livy_query = "fabric_livy_query",
    livy_session = "fabric_livy_session",
    livy_batch_submit = "fabric_livy_batch_submit",
    onelake_list = "fabric_onelake_list",
    onelake_metadata = "fabric_onelake_metadata",
    onelake_read_file = "fabric_onelake_read_file",
    onelake_write_file = "fabric_onelake_write_file",
    onelake_download = "fabric_onelake_download",
    onelake_upload = "fabric_onelake_upload",
    onelake_delete = "fabric_onelake_delete",
    schema_exists = "fabric_onelake_schema_exists",
    table_exists = "fabric_onelake_table_exists",
    shortcuts = "fabric_onelake_shortcuts",
    shortcut = "fabric_onelake_shortcut_get",
    shortcut_create = "fabric_onelake_shortcut_create",
    shortcuts_bulk_create = "fabric_onelake_shortcuts_bulk_create",
    shortcut_delete = "fabric_onelake_shortcut_delete"
  )
  local_r6_method_mocks(unique(unname(mapping)), calls)
  lakehouse <- r6_test_record("Lakehouse")
  arguments <- list(
    sql_query = list(sql = "SELECT 1"),
    sql_read_table = list(table = "orders"),
    table = list(table = "orders"),
    read_table = list(table = "orders"),
    read_delta_table = list(table_path = "Tables/orders"),
    load_table = list(table = "orders", path = "Files/orders.csv"),
    write_table = list(table = "orders", data = data.frame(id = 1L)),
    livy_query = list(code = "1 + 1"),
    livy_batch_submit = list(file = "abfss://job.py"),
    onelake_read_file = list(path = "Files/orders.csv"),
    onelake_write_file = list(
      path = "Files/orders.csv",
      data = data.frame(id = 1L)
    ),
    onelake_download = list(path = "Files/orders.csv"),
    onelake_upload = list(path = "Files/orders.csv", source = charToRaw("x")),
    onelake_delete = list(path = "Files/orders.csv"),
    schema_exists = list(schema = "dbo"),
    table_exists = list(table = "orders"),
    shortcut = list(path = "Files", name = "orders"),
    shortcut_create = list(
      path = "Files",
      name = "orders",
      target = list(oneLake = list(path = "Files/source"))
    ),
    shortcuts_bulk_create = list(shortcuts = list(list(name = "orders"))),
    shortcut_delete = list(path = "Files", name = "orders")
  )
  context <- c(
    sql_connection_info = "server",
    sql_connect = "server",
    sql_query = "server",
    sql_tables = "server",
    sql_views = "server",
    sql_read_table = "server",
    schemas = "lakehouse",
    table = "lakehouse",
    tables = "lakehouse",
    read_table = "lakehouse",
    read_delta_table = "lakehouse_name",
    load_table = "lakehouse",
    write_table = "lakehouse",
    livy_query = "livy_url",
    livy_session = "livy_url",
    livy_batch_submit = "livy_url",
    onelake_list = "item",
    onelake_metadata = "item",
    onelake_read_file = "item",
    onelake_write_file = "item",
    onelake_download = "item",
    onelake_upload = "item",
    onelake_delete = "item",
    schema_exists = "item",
    table_exists = "item",
    shortcuts = "item",
    shortcut = "item",
    shortcut_create = "item",
    shortcuts_bulk_create = "item",
    shortcut_delete = "item"
  )

  for (method in names(mapping)) {
    call <- expect_r6_delegation(
      lakehouse,
      method,
      arguments[[method]] %||% list(),
      mapping[[method]],
      calls,
      context[[method]]
    )
    if (
      grepl(
        "^onelake_(list|metadata|read|write|download|upload|delete)$",
        method
      )
    ) {
      expect_identical(call$workspace, lakehouse$workspaceId, info = method)
    }
  }
})

test_that("Warehouse and mirrored methods all delegate their item identity", {
  calls <- new.env(parent = emptyenv())
  mapping <- c(
    schemas = "fabric_warehouse_schemas",
    table = "fabric_warehouse_table",
    tables = "fabric_warehouse_tables",
    read_table = "fabric_warehouse_read_table",
    write_table = "fabric_warehouse_write_table",
    read_delta_table = "fabric_onelake_read_delta_table",
    mirrored_schemas = "fabric_mirrored_database_schemas",
    mirrored_table = "fabric_mirrored_database_table",
    mirrored_tables = "fabric_mirrored_database_tables",
    mirrored_read_table = "fabric_mirrored_database_read_table"
  )
  local_r6_method_mocks(unique(unname(mapping)), calls)
  warehouse <- r6_test_record("Warehouse")
  mirrored <- r6_test_record("MirroredDatabase")

  expect_r6_delegation(
    warehouse,
    "schemas",
    list(),
    mapping[["schemas"]],
    calls,
    "warehouse"
  )
  expect_r6_delegation(
    warehouse,
    "table",
    list(table = "orders"),
    mapping[["table"]],
    calls,
    "warehouse"
  )
  expect_r6_delegation(
    warehouse,
    "tables",
    list(),
    mapping[["tables"]],
    calls,
    "warehouse"
  )
  expect_r6_delegation(
    warehouse,
    "read_table",
    list(table = "orders"),
    mapping[["read_table"]],
    calls,
    "warehouse"
  )
  expect_r6_delegation(
    warehouse,
    "write_table",
    list(
      table = "orders",
      data = data.frame(id = 1L),
      staging_lakehouse = "stage"
    ),
    mapping[["write_table"]],
    calls,
    "warehouse"
  )
  delta <- expect_r6_delegation(
    warehouse,
    "read_delta_table",
    list(table_path = "Tables/dbo/orders"),
    mapping[["read_delta_table"]],
    calls,
    "lakehouse_name"
  )
  expect_identical(delta$workspace_name, warehouse$workspaceId)

  mirrored_methods <- c(
    schemas = "mirrored_schemas",
    table = "mirrored_table",
    tables = "mirrored_tables",
    read_table = "mirrored_read_table"
  )
  for (method in names(mirrored_methods)) {
    alias <- mirrored_methods[[method]]
    expect_r6_delegation(
      mirrored,
      method,
      if (method %in% c("table", "read_table")) {
        list(table = "orders")
      } else {
        list()
      },
      mapping[[alias]],
      calls,
      "mirrored_database"
    )
  }
  mirrored_delta <- expect_r6_delegation(
    mirrored,
    "read_delta_table",
    list(table_path = "Tables/dbo/orders"),
    mapping[["read_delta_table"]],
    calls,
    "lakehouse_name"
  )
  expect_identical(mirrored_delta$workspace_name, mirrored$workspaceId)
})

test_that("KQL and GraphQL methods all delegate their item identity", {
  calls <- new.env(parent = emptyenv())
  mapping <- c(
    query = "fabric_kql_query",
    tables = "fabric_kql_tables",
    read_table = "fabric_kql_read_table",
    ingest = "fabric_kql_ingest",
    write_table = "fabric_kql_write_table",
    export = "fabric_kql_export",
    ingestion_status = "fabric_kql_ingestion_status",
    graphql_query = "fabric_graphql_query",
    schema = "fabric_graphql_schema",
    paginate = "fabric_graphql_paginate"
  )
  local_r6_method_mocks(unique(unname(mapping)), calls)
  credential <- fabric_credential(token = "discovery-token")
  database <- r6_test_record("KQLDatabase", credential)
  api <- r6_test_record("GraphQLApi")
  arguments <- list(
    query = list(query = "StormEvents | take 1"),
    read_table = list(table = "StormEvents"),
    ingest = list(table = "events", sources = "source.csv", format = "csv"),
    write_table = list(table = "events", data = data.frame(id = 1L)),
    export = list(query = "events", destination = "Files/export")
  )

  for (method in c(
    "query",
    "tables",
    "read_table",
    "ingest",
    "write_table",
    "export"
  )) {
    expect_r6_delegation(
      database,
      method,
      arguments[[method]] %||% list(),
      mapping[[method]],
      calls,
      "cluster"
    )
  }

  raw_status <- expect_r6_delegation(
    database,
    "ingestion_status",
    list(ingestion = "operation-id", table = "events"),
    mapping[["ingestion_status"]],
    calls,
    "cluster"
  )
  expect_identical(raw_status$wait, FALSE)
  expect_s3_class(raw_status$token, "fabric_credential")
  ingestion <- structure(
    list(id = "operation-id"),
    class = "fabric_kql_ingestion"
  )
  handle_status <- expect_r6_delegation(
    database,
    "ingestion_wait",
    list(ingestion = ingestion),
    mapping[["ingestion_status"]],
    calls
  )
  expect_null(handle_status$cluster)
  expect_identical(handle_status$wait, TRUE)

  expect_r6_delegation(
    api,
    "query",
    list(query = "{ products { id } }"),
    mapping[["graphql_query"]],
    calls,
    "api"
  )
  expect_r6_delegation(
    api,
    "schema",
    list(),
    mapping[["schema"]],
    calls,
    "api"
  )
  expect_r6_delegation(
    api,
    "paginate",
    list(query = "query", next_cursor = identity),
    mapping[["paginate"]],
    calls,
    "api"
  )
})

test_that("Semantic-model lifecycle methods preserve raw and handle context", {
  calls <- new.env(parent = emptyenv())
  functions <- c(
    "fabric_pbi_dax_query",
    "fabric_pbi_refresh",
    "fabric_pbi_refresh_history",
    "fabric_pbi_refresh_status",
    "fabric_pbi_refresh_wait",
    "fabric_pbi_refresh_cancel"
  )
  local_r6_method_mocks(functions, calls)
  model <- r6_test_record(
    "SemanticModel",
    fabric_credential(token = "discovery-token")
  )

  expect_r6_delegation(
    model,
    "dax_query",
    list(dax = "EVALUATE ROW(\"x\", 1)"),
    "fabric_pbi_dax_query",
    calls,
    "connstr"
  )
  expect_r6_delegation(
    model,
    "refresh",
    list(),
    "fabric_pbi_refresh",
    calls,
    "connstr"
  )
  expect_r6_delegation(
    model,
    "refresh_history",
    list(),
    "fabric_pbi_refresh_history",
    calls,
    "connstr"
  )
  raw_status <- expect_r6_delegation(
    model,
    "refresh_status",
    list(refresh = "33333333-3333-4333-8333-333333333333"),
    "fabric_pbi_refresh_status",
    calls,
    "connstr"
  )
  expect_identical(raw_status$refresh, "33333333-3333-4333-8333-333333333333")

  handle <- structure(list(id = "refresh-id"), class = "fabric_pbi_refresh")
  handle_status <- expect_r6_delegation(
    model,
    "refresh_status",
    list(refresh = handle),
    "fabric_pbi_refresh_status",
    calls
  )
  expect_null(handle_status$connstr)
  expect_s3_class(handle_status$token, "fabric_credential")
  expect_r6_delegation(
    model,
    "refresh_wait",
    list(refresh = handle),
    "fabric_pbi_refresh_wait",
    calls
  )
  raw_cancel <- expect_r6_delegation(
    model,
    "refresh_cancel",
    list(refresh = "33333333-3333-4333-8333-333333333333"),
    "fabric_pbi_refresh_cancel",
    calls,
    "connstr"
  )
  expect_identical(raw_cancel$refresh, "33333333-3333-4333-8333-333333333333")
})

test_that("Runnable item methods cover the complete job lifecycle", {
  calls <- new.env(parent = emptyenv())
  functions <- c(
    "fabric_job_run",
    "fabric_job_status",
    "fabric_job_wait",
    "fabric_job_cancel",
    "fabric_job_instances",
    "fabric_job_schedules",
    "fabric_job_schedule_create",
    "fabric_job_schedule_update",
    "fabric_job_schedule_delete"
  )
  local_r6_method_mocks(functions, calls)
  notebook <- r6_test_record(
    "Notebook",
    fabric_credential(token = "discovery-token")
  )
  job <- structure(list(id = "job-id"), class = "fabric_job")

  item_methods <- c(
    run = "fabric_job_run",
    instances = "fabric_job_instances",
    schedules = "fabric_job_schedules"
  )
  for (method in names(item_methods)) {
    expect_r6_delegation(
      notebook,
      method,
      list(),
      item_methods[[method]],
      calls,
      "item"
    )
  }
  expect_r6_delegation(
    notebook,
    "status",
    list(job = job),
    "fabric_job_status",
    calls,
    "item"
  )
  wait_call <- expect_r6_delegation(
    notebook,
    "wait",
    list(job = job),
    "fabric_job_wait",
    calls
  )
  expect_identical(wait_call$job, job)
  expect_s3_class(wait_call$token, "fabric_credential")
  expect_r6_delegation(
    notebook,
    "cancel",
    list(job = "33333333-3333-4333-8333-333333333333"),
    "fabric_job_cancel",
    calls,
    "item"
  )

  configuration <- list(type = "Daily", interval = 1)
  create <- expect_r6_delegation(
    notebook,
    "schedule_create",
    list(configuration = configuration),
    "fabric_job_schedule_create",
    calls,
    "item"
  )
  expect_identical(create$configuration, configuration)
  update <- expect_r6_delegation(
    notebook,
    "schedule_update",
    list(schedule_id = "schedule-id", configuration = configuration),
    "fabric_job_schedule_update",
    calls,
    "item"
  )
  expect_identical(update$schedule_id, "schedule-id")
  delete <- expect_r6_delegation(
    notebook,
    "schedule_delete",
    list(schedule_id = "schedule-id"),
    "fabric_job_schedule_delete",
    calls,
    "item"
  )
  expect_identical(delete$schedule_id, "schedule-id")
})
