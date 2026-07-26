test_that("Fabric discovery resolves sandbox workspaces and item targets", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()

  workspaces <- fabric_workspaces(token = token)
  workspace <- workspaces[workspaces$id == manifest$workspace_id, ]
  expect_equal(nrow(workspace), 1L)
  expect_equal(workspace$displayName, manifest$workspace_name)

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
    "TestGraphQL"
  )
  for (name in expected_items) {
    expected <- manifest$items[[name]]
    discovered <- items[items$id == expected$id, ]
    expect_equal(nrow(discovered), 1L, info = name)
    expect_equal(discovered$type, expected$type, info = name)
    expect_equal(discovered$displayName, expected$display_name, info = name)
  }

  lakehouses <- fabric_lakehouses(workspace, token = token)
  lakehouse <- lakehouses[
    lakehouses$id == manifest$items$TestLakehouse$id,
  ]
  expect_equal(nrow(lakehouse), 1L)
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
  warehouse <- warehouses[
    warehouses$id == manifest$items$TestWarehouse$id,
  ]
  expect_equal(nrow(warehouse), 1L)
  expect_equal(
    warehouse$sql_server,
    manifest$items$TestWarehouse$connection_string
  )
  expect_equal(
    warehouse$sql_database,
    manifest$items$TestWarehouse$database_name
  )

  sql_databases <- fabric_sql_databases(workspace, token = token)
  sql_database <- sql_databases[
    sql_databases$id == manifest$items$TestSQLDatabase$id,
  ]
  expect_equal(nrow(sql_database), 1L)
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
  model <- semantic_models[
    semantic_models$id == manifest$items$TestSemanticModel$id,
  ]
  expect_equal(nrow(model), 1L)
  expect_equal(model$id, manifest$items$TestSemanticModel$id)
  expect_equal(model$workspaceId, manifest$workspace_id)
  expect_match(model$dax_connection_string, "powerbi://", fixed = TRUE)

  notebooks <- fabric_notebooks(workspace, token = token)
  expect_true(manifest$items$SeedFixtures$id %in% notebooks$id)
  expect_true(manifest$items$JobFixtures$id %in% notebooks$id)

  graphql_apis <- fabric_graphql_apis(workspace, token = token)
  expect_true(manifest$items$TestGraphQL$id %in% graphql_apis$id)

  eventhouses <- fabric_eventhouses(workspace, token = token)
  eventhouse <- eventhouses[
    eventhouses$id == manifest$items$TestEventhouse$id,
  ]
  expect_equal(nrow(eventhouse), 1L)
  expect_equal(
    eventhouse$query_service_uri,
    manifest$items$TestEventhouse$query_service_uri
  )

  kql_databases <- fabric_kql_databases(workspace, token = token)
  kql_database <- kql_databases[
    kql_databases$id == manifest$items$TestKQLDatabase$id,
  ]
  expect_equal(nrow(kql_database), 1L)
  expect_equal(
    kql_database$query_service_uri,
    manifest$items$TestKQLDatabase$query_service_uri
  )
})

test_that("fabric_kql_query returns typed seeded Eventhouse data", {
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  token <- fabric_test_token_provider()
  result <- fabric_kql_query(
    database$query_service_uri,
    query = paste(
      database$tables$events,
      "| order by id asc"
    ),
    database = database$database_name,
    token = token
  )

  expect_s3_class(result, "tbl_df")
  expect_named(
    result,
    c(
      "id",
      "name",
      "category",
      "amount",
      "observed_at",
      "active",
      "correlation_id",
      "metadata"
    )
  )
  expect_equal(result$id, c(1L, 2L, 3L))
  expect_equal(result$name, c("alpha", "beta", "gamma"))
  expect_equal(result$category, c("A", "B", "A"))
  expect_equal(result$amount, c(10.5, 20, NA))
  expect_s3_class(result$observed_at, "POSIXct")
  expect_equal(
    as.Date(result$observed_at),
    as.Date(c("2026-01-01", "2026-01-02", "2026-01-03"))
  )
  expect_equal(result$active, c(TRUE, FALSE, TRUE))
  expect_equal(
    result$correlation_id,
    c(
      "11111111-1111-1111-1111-111111111111",
      "22222222-2222-2222-2222-222222222222",
      "33333333-3333-3333-3333-333333333333"
    )
  )
  expect_equal(result$metadata[[1L]]$source, "sandbox")
  expect_equal(result$metadata[[1L]]$rank, 1L)
})

test_that("fabric_kql_query discovers targets and binds safe parameters", {
  manifest <- fabric_test_manifest()
  provisioned <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  kusto_token <- fabric_test_token("FABRIC_TEST_KUSTO_TOKEN")
  target <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = "KQLDatabase",
    token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )

  selected <- fabric_kql_query(
    target,
    query = paste(
      "declare query_parameters(selected_category:string);",
      provisioned$tables$events,
      "| where category == selected_category",
      "| order by id asc"
    ),
    parameters = list(selected_category = "A"),
    token = kusto_token
  )
  expect_equal(selected$id, c(1L, 3L))

  hostile <- fabric_kql_query(
    target,
    query = paste(
      "declare query_parameters(selected_name:string);",
      provisioned$tables$events,
      "| where name == selected_name"
    ),
    parameters = list(
      selected_name = "alpha'; drop table fabricqueryr_events; --"
    ),
    token = kusto_token
  )
  expect_s3_class(hostile, "tbl_df")
  expect_equal(nrow(hostile), 0L)

  still_present <- fabric_kql_query(
    target,
    query = paste(provisioned$tables$events, "| count"),
    token = kusto_token
  )
  expect_equal(as.numeric(still_present$Count), 3)
})

test_that("fabric_kql_query returns multiple live primary tables", {
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  table <- database$tables$events
  result <- fabric_kql_query(
    database$query_service_uri,
    query = paste0(
      table,
      " | summarize row_count=count(); ",
      table,
      " | summarize amount_sum=sum(amount)"
    ),
    database = database$database_name,
    token = fabric_test_token("FABRIC_TEST_KUSTO_TOKEN")
  )

  expect_s3_class(result, "fabric_kql_tables")
  expect_length(result, 2L)
  expect_equal(as.numeric(result[[1L]]$row_count), 3)
  expect_equal(result[[2L]]$amount_sum, 30.5)
})

test_that("fabric_kql_query surfaces live Kusto service errors", {
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  expect_error(
    fabric_kql_query(
      database$query_service_uri,
      query = "fabricqueryr_table_that_does_not_exist | take 1",
      database = database$database_name,
      token = fabric_test_token("FABRIC_TEST_KUSTO_TOKEN")
    ),
    "(?i)(failed|HTTP 4)"
  )
})

test_that("fabric_graphql_query executes variables and preserves nulls", {
  manifest <- fabric_test_manifest()
  provisioned <- fabric_test_manifest_item(manifest, "TestGraphQL")
  token <- fabric_test_token_provider()
  api <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = "GraphQLApi",
    token = token
  )
  expect_equal(api$graphql_endpoint, provisioned$endpoint)
  root_field <- provisioned$root_field

  result <- fabric_graphql_query(
    api,
    query = paste(
      "query Filtered($category: String!) {",
      paste0("  ", root_field, "("),
      "    filter: {category: {eq: $category}},",
      "    orderBy: {id: ASC}",
      "  ) {",
      "    items { id name category amount loaded_at }",
      "    hasNextPage",
      "    endCursor",
      "  }",
      "}"
    ),
    variables = list(category = "A"),
    operation_name = "Filtered",
    error_policy = "error",
    token = token,
    audience = "https://api.fabric.microsoft.com/.default"
  )

  expect_s3_class(result, "fabric_graphql_result")
  expect_length(result$errors, 0L)
  expect_length(result$data[[root_field]]$items, 2L)
  expect_equal(
    vapply(
      result$data[[root_field]]$items,
      `[[`,
      integer(1),
      "id"
    ),
    c(1L, 3L)
  )
  expect_equal(
    vapply(
      result$data[[root_field]]$items,
      `[[`,
      character(1),
      "name"
    ),
    c("alpha", "gamma")
  )
  expect_equal(result$data[[root_field]]$items[[1L]]$amount, 10.5)
  expect_null(result$data[[root_field]]$items[[2L]]$amount)
})

test_that("Fabric GraphQL cursor pagination traverses every seeded row", {
  manifest <- fabric_test_manifest()
  api <- fabric_test_manifest_item(manifest, "TestGraphQL")
  token <- fabric_test_token_provider()
  root_field <- api$root_field
  pages <- fabric_graphql_paginate(
    api$endpoint,
    query = paste(
      "query Paged($first: Int!, $after: String) {",
      paste0("  ", root_field, "("),
      "    first: $first, after: $after, orderBy: {id: ASC}",
      "  ) {",
      "    items { id name amount }",
      "    hasNextPage",
      "    endCursor",
      "  }",
      "}"
    ),
    variables = list(first = 2L, after = NULL),
    operation_name = "Paged",
    next_cursor = fabric_graphql_cursor(root_field),
    error_policy = "error",
    token = token,
    audience = "https://api.fabric.microsoft.com/.default"
  )
  items <- unlist(
    lapply(
      pages$pages,
      function(page) page$data[[root_field]]$items
    ),
    recursive = FALSE
  )

  expect_s3_class(pages, "fabric_graphql_pages")
  expect_true(pages$complete)
  expect_length(pages$pages, 2L)
  expect_equal(vapply(items, `[[`, integer(1), "id"), c(1L, 2L, 3L))
  expect_equal(
    vapply(items, `[[`, character(1), "name"),
    c("alpha", "beta", "gamma")
  )
})

test_that("Fabric GraphQL surfaces schema and authentication failures", {
  manifest <- fabric_test_manifest()
  api <- fabric_test_manifest_item(manifest, "TestGraphQL")
  token <- fabric_test_token_provider()

  invalid_query <- fabric_graphql_query(
    api$endpoint,
    query = "{ fabricqueryr_field_that_does_not_exist }",
    token = token,
    audience = "https://api.fabric.microsoft.com/.default"
  )
  expect_null(invalid_query$data)
  expect_gt(length(invalid_query$errors), 0L)
  expect_match(invalid_query$errors[[1L]]$message, "(?i)(field|query)")

  expect_error(
    fabric_graphql_query(
      api$endpoint,
      query = "{ __typename }",
      token = "fabricqueryr-invalid-token"
    ),
    "HTTP (401|403)"
  )
})

test_that("fabric_onelake_read_delta_table reads schema-enabled Delta data", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  token <- fabric_test_token_provider()

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$basic,
    workspace_name = manifest$workspace_name,
    lakehouse_name = lakehouse$display_name,
    schema = lakehouse$schema,
    tenant_id = "",
    client_id = "",
    token = token,
    verbose = FALSE
  )
  result <- result[order(result$id), ]

  expect_s3_class(result, "tbl_df")
  expect_named(
    result,
    c("id", "name", "category", "amount", "loaded_at"),
    ignore.order = TRUE
  )
  expect_equal(nrow(result), 3L)
  expect_equal(result$id, c(1L, 2L, 3L))
  expect_equal(result$name, c("alpha", "beta", "gamma"))
  expect_equal(result$category, c("A", "B", "A"))
  expect_equal(result$amount, c(10.5, 20, NA))
  expect_true(inherits(result$loaded_at, "POSIXct"))
  expect_equal(
    as.numeric(result$loaded_at),
    rep(as.numeric(as.POSIXct("2026-01-01", tz = "UTC")), 3)
  )
})

test_that("Delta reader preserves empty schemas and typed log partitions", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  read_table <- function(table) {
    fabric_onelake_read_delta_table(
      table_path = table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = token,
      verbose = FALSE
    )
  }

  empty <- read_table(lakehouse$tables$empty)
  expect_s3_class(empty, "tbl_df")
  expect_equal(nrow(empty), 0L)
  expect_named(empty, c("id", "name", "category", "amount"))
  expect_type(empty$id, "integer")
  expect_type(empty$name, "character")
  expect_type(empty$category, "character")
  expect_type(empty$amount, "double")

  partitioned <- read_table(lakehouse$tables$typed_partitions)
  partitioned <- partitioned[order(partitioned$id), ]
  expect_equal(partitioned$id, 1:3)
  expect_equal(partitioned$name, c("alpha", "beta", "gamma"))
  expect_equal(partitioned$amount, c(10.5, 20, NA))
  expect_s3_class(partitioned$event_date, "Date")
  expect_equal(
    partitioned$event_date,
    as.Date(c("2026-01-01", "2026-01-02", NA))
  )
  expect_type(partitioned$active, "logical")
  expect_equal(partitioned$active, c(TRUE, FALSE, NA))
})

test_that("OneLake file helpers cover hierarchy, ranges, conflicts, and Unicode", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()

  fixtures <- fabric_onelake_list(
    manifest$workspace_id,
    lakehouse$id,
    path = "Files/fixtures/nested",
    recursive = TRUE,
    page_size = 2L,
    token = token
  )
  expect_setequal(
    fixtures$path[!fixtures$is_directory],
    c(
      "Files/fixtures/nested/a/duplicate.txt",
      "Files/fixtures/nested/b/duplicate.txt",
      "Files/fixtures/nested/unicode/café-数据.txt"
    )
  )
  duplicate_paths <- fixtures$path[fixtures$name == "duplicate.txt"]
  expect_setequal(
    duplicate_paths,
    c(
      "Files/fixtures/nested/a/duplicate.txt",
      "Files/fixtures/nested/b/duplicate.txt"
    )
  )
  unicode_path <- "Files/fixtures/nested/unicode/café-数据.txt"
  expect_true(unicode_path %in% fixtures$path)

  unicode_metadata <- fabric_onelake_metadata(
    paste0(
      "abfss://",
      manifest$workspace_id,
      "@onelake.dfs.fabric.microsoft.com/",
      lakehouse$id,
      "/",
      unicode_path
    ),
    token = token
  )
  expect_false(unicode_metadata$is_directory)
  expect_true(nzchar(unicode_metadata$etag))
  expect_gt(unicode_metadata$content_length, 0)
  unicode_contents <- fabric_onelake_download(
    manifest$workspace_id,
    lakehouse$id,
    unicode_path,
    token = token
  )
  expect_identical(
    trimws(rawToChar(unicode_contents)),
    "OneLake Unicode fixture"
  )

  ranged <- fabric_onelake_download(
    manifest$workspace_id,
    lakehouse$id,
    "Files/fixtures/nested/a/duplicate.txt",
    range = c(0, 4),
    token = token
  )
  expect_identical(rawToChar(ranged), "alpha")

  test_root <- paste0(
    "Files/fabricqueryr-tests/one-lake-",
    gsub("[^A-Za-z0-9]", "", manifest$workspace_id),
    "-",
    format(Sys.time(), "%Y%m%d%H%M%S"),
    "-",
    Sys.getpid()
  )
  test_path <- paste0(test_root, "/nested/échantillon-数据.txt")
  on.exit(
    try(
      fabric_onelake_delete(
        manifest$workspace_id,
        lakehouse$id,
        test_root,
        recursive = TRUE,
        confirm = TRUE,
        token = token
      ),
      silent = TRUE
    ),
    add = TRUE
  )

  fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    test_path,
    source = charToRaw("first-version"),
    content_type = "text/plain; charset=utf-8",
    token = token
  )
  first <- fabric_onelake_metadata(
    manifest$workspace_id,
    lakehouse$id,
    test_path,
    token = token
  )
  expect_equal(first$content_length, nchar("first-version", type = "bytes"))
  expect_true(nzchar(first$etag))
  expect_match(first$content_type, "^text/plain")

  expect_error(
    fabric_onelake_upload(
      manifest$workspace_id,
      lakehouse$id,
      test_path,
      source = charToRaw("conflict"),
      token = token
    ),
    "HTTP (409|412)"
  )
  fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    test_path,
    source = charToRaw("second-version"),
    overwrite = TRUE,
    if_match = first$etag,
    token = token
  )
  second <- fabric_onelake_metadata(
    manifest$workspace_id,
    lakehouse$id,
    test_path,
    token = token
  )
  expect_true(nzchar(second$etag))
  expect_false(identical(second$etag, first$etag))
  expect_error(
    fabric_onelake_upload(
      manifest$workspace_id,
      lakehouse$id,
      test_path,
      source = charToRaw("stale-write"),
      overwrite = TRUE,
      if_match = first$etag,
      token = token
    ),
    "HTTP 412"
  )
  expect_identical(
    rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      test_path,
      token = token
    )),
    "second-version"
  )
  expect_identical(
    rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      test_path,
      range = c(7, 13),
      token = token
    )),
    "version"
  )

  empty_path <- paste0(test_root, "/empty.bin")
  empty_metadata <- fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    empty_path,
    source = raw(),
    token = token
  )
  expect_equal(empty_metadata$content_length, 0)
  expect_length(
    fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      empty_path,
      token = token
    ),
    0L
  )

  expect_error(
    fabric_onelake_delete(
      manifest$workspace_id,
      lakehouse$id,
      test_root,
      recursive = TRUE,
      token = token
    ),
    "disabled by default",
    fixed = TRUE
  )
  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    test_root,
    recursive = TRUE,
    confirm = TRUE,
    token = token
  ))
})

test_that("fabric_onelake_read_delta_table resolves Delta removals and partitions", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  dest_dir <- tempfile("fabricqueryr-integration-")
  on.exit(
    if (fs::dir_exists(dest_dir)) {
      fs::dir_delete(dest_dir)
    },
    add = TRUE
  )

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$partitioned,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    tenant_id = "",
    client_id = "",
    token = fabric_test_token("FABRIC_TEST_STORAGE_TOKEN"),
    dest_dir = dest_dir,
    verbose = FALSE
  )
  id_counts <- table(result$id)
  replaced <- result[result$id == 2L, ]

  expect_s3_class(result, "tbl_df")
  expect_true("category" %in% names(result))
  expect_equal(nrow(result), 13L)
  expect_equal(as.integer(id_counts[c("1", "2", "3")]), c(11L, 1L, 1L))
  expect_equal(sort(unique(result$category)), c("A", "B"))
  expect_equal(replaced$name, "beta-updated")
  expect_equal(replaced$amount, 21)
  expect_true(fs::dir_exists(fs::path(dest_dir, "category=A")))
  expect_true(fs::dir_exists(fs::path(dest_dir, "category=B")))
  expect_gt(
    length(fs::dir_ls(dest_dir, recurse = TRUE, regexp = "\\.parquet$")),
    0L
  )
  expect_true(
    any(fs::file_exists(
      fs::dir_ls(
        fs::path(dest_dir, "_delta_log"),
        regexp = "checkpoint.*\\.parquet$"
      )
    ))
  )

  historical <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$partitioned,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = fabric_test_token("FABRIC_TEST_STORAGE_TOKEN"),
    version = 10,
    verbose = FALSE
  )
  historical_beta <- historical[historical$id == 2L, ]
  expect_equal(historical_beta$name, "beta")
  expect_equal(historical_beta$amount, 20)
})

test_that("Delta reader covers schema evolution and rejects unsupported features", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  read_table <- function(table) {
    fabric_onelake_read_delta_table(
      table_path = table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = token,
      verbose = FALSE
    )
  }

  evolved <- read_table(lakehouse$tables$schema_evolved)
  evolved <- evolved[order(evolved$id), ]
  expect_equal(evolved$id, 1:3)
  expect_true("evolved_value" %in% names(evolved))
  expect_true(all(is.na(evolved$evolved_value[1:2])))
  expect_equal(evolved$evolved_value[[3L]], "introduced")

  expect_error(
    read_table(lakehouse$tables$column_mapped),
    "column mapping mode"
  )
  expect_error(
    read_table(lakehouse$tables$deletion_vectors),
    "deletion vectors"
  )
})

test_that("fabric_sql_connect opens a usable connection and disconnects", {
  backends <- fabric_test_sql_backends()
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  token <- fabric_test_token_provider()
  target <- fabric_item(
    manifest$workspace_id,
    lakehouse$id,
    type = "Lakehouse",
    token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )

  for (backend in backends) {
    con <- fabric_sql_connect(
      server = target,
      backend = backend,
      tenant_id = "",
      client_id = "",
      token = token,
      verbose = FALSE
    )
    expect_true(DBI::dbIsValid(con), info = backend)
    result <- DBI::dbGetQuery(
      con,
      paste(
        "SELECT id, name, category, amount, loaded_at",
        "FROM dbo.fabricqueryr_basic",
        "ORDER BY id"
      )
    )
    expect_equal(result$id, c(1L, 2L, 3L), info = backend)
    expect_equal(result$name, c("alpha", "beta", "gamma"), info = backend)
    expect_equal(result$category, c("A", "B", "A"), info = backend)
    expect_equal(result$amount, c(10.5, 20, NA), info = backend)
    expect_s3_class(result$loaded_at, "POSIXct")
    expect_equal(
      as.numeric(result$loaded_at),
      rep(as.numeric(as.POSIXct("2026-01-01", tz = "UTC")), 3),
      info = backend
    )
    expect_true(
      DBI::dbExistsTable(
        con,
        DBI::Id(schema = "dbo", table = "fabricqueryr_basic")
      ),
      info = backend
    )

    DBI::dbDisconnect(con)
    expect_false(DBI::dbIsValid(con), info = backend)
  }
})

test_that("fabric_sql_query returns tibbles and consumable Arrow streams", {
  backends <- fabric_test_sql_backends()
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  for (backend in backends) {
    result <- fabric_sql_query(
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste(
        "SELECT COUNT(*) AS row_count,",
        "SUM(amount) AS amount_sum",
        "FROM dbo.fabricqueryr_basic"
      ),
      backend = backend,
      tenant_id = "",
      client_id = "",
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )

    expect_s3_class(result, "tbl_df")
    expect_equal(nrow(result), 1L, info = backend)
    expect_equal(as.numeric(result$row_count), 3, info = backend)
    expect_equal(as.numeric(result$amount_sum), 30.5, info = backend)

    empty <- fabric_sql_query(
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste(
        "SELECT id, name",
        "FROM dbo.fabricqueryr_basic",
        "WHERE 1 = 0"
      ),
      backend = backend,
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_s3_class(empty, "tbl_df")
    expect_equal(nrow(empty), 0L, info = backend)
    expect_named(empty, c("id", "name"), info = backend)

    metacharacters <- "Robert'); DROP TABLE dbo.fabricqueryr_basic;--"
    bound <- fabric_sql_query(
      server = paste0(
        "Server=tcp:",
        lakehouse$sql_endpoint,
        ";Initial Catalog=",
        lakehouse$display_name,
        ";MultipleActiveResultSets=False"
      ),
      sql = paste(
        "SELECT CAST(? AS nvarchar(200)) AS text_value,",
        "CAST(? AS date) AS date_value,",
        "CAST(? AS nvarchar(20)) AS null_value"
      ),
      params = list(
        metacharacters,
        as.Date("2026-07-24"),
        NA_character_
      ),
      backend = backend,
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_equal(bound$text_value, metacharacters, info = backend)
    expect_equal(
      as.Date(bound$date_value),
      as.Date("2026-07-24"),
      info = backend
    )
    expect_true(is.na(bound$null_value), info = backend)

    still_present <- fabric_sql_query(
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = "SELECT COUNT(*) AS row_count FROM dbo.fabricqueryr_basic",
      backend = backend,
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_equal(as.numeric(still_present$row_count), 3, info = backend)

    stream <- fabric_sql_query(
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste(
        "SELECT id, name",
        "FROM dbo.fabricqueryr_basic",
        "ORDER BY id"
      ),
      backend = backend,
      result = "arrow_stream",
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_s3_class(stream, "nanoarrow_array_stream")
    streamed <- as.data.frame(nanoarrow::collect_array_stream(stream))
    expect_equal(streamed$id, c(1L, 2L, 3L), info = backend)
    expect_equal(streamed$name, c("alpha", "beta", "gamma"), info = backend)

    arrow_stream <- fabric_sql_query(
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste(
        "SELECT id, name",
        "FROM dbo.fabricqueryr_basic",
        "ORDER BY id"
      ),
      backend = backend,
      result = "arrow_stream",
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    reader <- arrow::as_record_batch_reader(arrow_stream)
    arrow_result <- as.data.frame(reader$read_table())
    expect_s3_class(reader, "RecordBatchReader")
    expect_equal(arrow_result$id, c(1L, 2L, 3L), info = backend)
    expect_equal(
      arrow_result$name,
      c("alpha", "beta", "gamma"),
      info = backend
    )
  }
})

fabric_test_sql_item <- function(name, backend) {
  manifest <- fabric_test_manifest()
  api_token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  sql_token <- fabric_test_token("FABRIC_TEST_SQL_TOKEN")

  provisioned <- fabric_test_manifest_item(manifest, name)
  target <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = provisioned$type,
    token = api_token
  )
  result <- fabric_sql_query(
    target,
    "SELECT CAST(? AS int) AS bound_value",
    params = list(42L),
    backend = backend,
    token = sql_token,
    verbose = FALSE
  )
  expect_equal(result$bound_value, 42L, info = paste(name, backend))

  info <- fabric_sql_connection_info(target)
  expect_equal(
    info$database,
    provisioned$database_name,
    info = paste(name, backend)
  )
  expect_equal(
    info$target_type,
    if (identical(name, "TestWarehouse")) "warehouse" else "sql_database",
    info = paste(name, backend)
  )

  from_manifest <- fabric_sql_query(
    provisioned$connection_string,
    "SELECT CAST(? AS nvarchar(100)) AS bound_value",
    params = list("safe ' value; --"),
    backend = backend,
    database = if (identical(name, "TestWarehouse")) {
      provisioned$database_name
    } else {
      NULL
    },
    token = sql_token,
    verbose = FALSE
  )
  expect_equal(
    from_manifest$bound_value,
    "safe ' value; --",
    info = paste(name, backend)
  )

  stream <- fabric_sql_query(
    target,
    "SELECT CAST(? AS int) AS bound_value",
    params = list(43L),
    backend = backend,
    result = "arrow_stream",
    token = sql_token,
    verbose = FALSE
  )
  expect_s3_class(
    stream,
    "nanoarrow_array_stream"
  )
  streamed <- as.data.frame(nanoarrow::collect_array_stream(stream))
  expect_equal(streamed$bound_value, 43L, info = paste(name, backend))
}

test_that("provisioned Warehouse target is discoverable and connectable", {
  for (backend in fabric_test_sql_backends()) {
    fabric_test_sql_item("TestWarehouse", backend)
  }
})

test_that("provisioned SQL Database target is discoverable and connectable", {
  for (backend in fabric_test_sql_backends()) {
    fabric_test_sql_item("TestSQLDatabase", backend)
  }
})

test_that("fabric_livy_query executes Spark and returns its output", {
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  table_name <- fabric_test_spark_table(manifest, lakehouse)
  token <- fabric_test_token_provider()

  result <- fabric_livy_query(
    livy_url = lakehouse$livy_url,
    code = sprintf(
      paste0(
        'row_count = spark.sql("SELECT COUNT(*) FROM %s").first()[0]\n',
        'print("FABRICQUERYR_ROW_COUNT=" + str(row_count))\n',
        'print("FABRICQUERYR_SHUFFLE_PARTITIONS=" + ',
        'spark.conf.get("spark.sql.shuffle.partitions"))'
      ),
      table_name
    ),
    kind = "pyspark",
    tenant_id = "",
    client_id = "",
    token = token,
    conf = list("spark.sql.shuffle.partitions" = "2"),
    verbose = FALSE
  )

  expect_equal(result$state, "available")
  expect_equal(result$output$status, "ok")
  expect_match(
    paste(result$output$parsed, collapse = "\n"),
    "FABRICQUERYR_ROW_COUNT=3",
    fixed = TRUE
  )
  expect_match(
    paste(result$output$parsed, collapse = "\n"),
    "FABRICQUERYR_SHUFFLE_PARTITIONS=2",
    fixed = TRUE
  )
  expect_true(is.finite(result$duration_sec))
  expect_gte(result$duration_sec, 0)
  expect_length(result$id, 1L)
  expect_true(is.numeric(result$id))
  expect_gt(result$id, -1L)
  expect_s3_class(result$started_local, "POSIXct")
  expect_s3_class(result$completed_local, "POSIXct")
  expect_gte(result$completed_local, result$started_local)
  expect_match(
    result$url,
    sprintf("/statements/%s$", result$id)
  )
  expect_true("text/plain" %in% names(result$output$data))
  expect_true(is.numeric(result$output$execution_count))
})

test_that("FabricLivySession shares state and preserves statement failures", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  session <- fabric_livy_session(
    lakehouse$livy_url,
    token = token,
    name = "fabricqueryr-integration-session",
    tags = list(test = "multiple-statements"),
    conf = list("spark.sql.shuffle.partitions" = "2"),
    verbose = FALSE
  )
  on.exit(try(session$close(), silent = TRUE), add = TRUE)
  session$wait(timeout = 900, poll_interval = 5)

  assignment <- session$run(
    "fabricqueryr_shared_value = 40",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(assignment$output$status, "ok")

  reused <- session$run(
    "print('FABRICQUERYR_SHARED_VALUE=' + str(fabricqueryr_shared_value + 2))",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_match(
    paste(reused$output$parsed, collapse = "\n"),
    "FABRICQUERYR_SHARED_VALUE=42",
    fixed = TRUE
  )

  scala <- session$run(
    "println(\"FABRICQUERYR_SCALA_OK\")",
    kind = "spark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(scala$output$status, "ok")
  expect_match(
    paste(scala$output$parsed, collapse = "\n"),
    "FABRICQUERYR_SCALA_OK",
    fixed = TRUE
  )

  sparkr <- session$run(
    "cat(\"FABRICQUERYR_SPARKR_OK\\n\")",
    kind = "sparkr",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(sparkr$output$status, "ok")
  expect_match(
    paste(sparkr$output$parsed, collapse = "\n"),
    "FABRICQUERYR_SPARKR_OK",
    fixed = TRUE
  )

  sql <- session$run(
    "SELECT 42 AS fabricqueryr_sql_value",
    kind = "sql",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(sql$output$status, "ok")
  expect_gt(length(sql$output$data), 0L)
  expect_match(
    jsonlite::toJSON(sql$output$data, auto_unbox = TRUE),
    "fabricqueryr_sql_value",
    fixed = TRUE
  )
  expect_match(
    jsonlite::toJSON(sql$output$data, auto_unbox = TRUE),
    "42",
    fixed = TRUE
  )

  failed <- session$submit(
    "raise RuntimeError('FABRICQUERYR_INTENTIONAL_STATEMENT_FAILURE')",
    kind = "pyspark"
  )
  error <- expect_error(
    failed$wait(timeout = 300, poll_interval = 2),
    class = "fabric_livy_statement_error"
  )
  expect_match(
    paste(
      conditionMessage(error),
      error$output$evalue %||% "",
      error$traceback,
      collapse = "\n"
    ),
    "FABRICQUERYR_INTENTIONAL_STATEMENT_FAILURE",
    fixed = TRUE
  )
  raw_failure <- failed$result(
    refresh = FALSE,
    error_on_failure = FALSE
  )
  expect_equal(raw_failure$output$status, "error")
  expect_gt(length(raw_failure$output$traceback), 0L)

  expect_true(session$close())
  expect_true(session$closed)
  expect_false(session$close())
})

test_that("high-concurrency Livy sessions pack but isolate their REPLs", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  tag <- paste0("fabricqueryr-", manifest$workspace_id)
  session_a <- fabric_livy_session(
    lakehouse$livy_url,
    high_concurrency = TRUE,
    session_tag = tag,
    artifact_name = lakehouse$display_name,
    token = token,
    verbose = FALSE
  )
  on.exit(try(session_a$close(), silent = TRUE), add = TRUE)
  session_a$wait(timeout = 900, poll_interval = 5)
  session_b <- fabric_livy_session(
    lakehouse$livy_url,
    high_concurrency = TRUE,
    session_tag = tag,
    artifact_name = lakehouse$display_name,
    token = token,
    verbose = FALSE
  )
  on.exit(try(session_b$close(), silent = TRUE), add = TRUE)
  session_b$wait(timeout = 900, poll_interval = 5)

  expect_false(identical(session_a$id, session_b$id))
  expect_identical(session_a$session_id, session_b$session_id)
  expect_false(identical(session_a$repl_id, session_b$repl_id))
  expect_true(nzchar(session_a$session_id))
  expect_true(nzchar(session_a$repl_id))
  expect_true(nzchar(session_b$repl_id))

  assigned <- session_a$run(
    "fabricqueryr_hc_secret = 'session-a-only'",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(assigned$output$status, "ok")
  isolated <- session_b$run(
    paste0(
      "print('FABRICQUERYR_HC_VARIABLE_VISIBLE=' + ",
      "str('fabricqueryr_hc_secret' in globals()))"
    ),
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(isolated$output$status, "ok")
  expect_match(
    paste(isolated$output$parsed, collapse = "\n"),
    "FABRICQUERYR_HC_VARIABLE_VISIBLE=False",
    fixed = TRUE
  )
  expect_true(session_a$close())
  expect_true(session_b$close())
})

test_that("Livy batches cover success, failure, and cancellation", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  storage_token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  marker <- function() {
    fabric_onelake_read_delta_table(
      table_path = lakehouse$tables$livy_batch_result,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = storage_token,
      verbose = FALSE
    )
  }
  wait_for_marker <- function(expected_mode, timeout = 120) {
    deadline <- Sys.time() + timeout
    repeat {
      value <- try(marker(), silent = TRUE)
      if (
        !inherits(value, "try-error") &&
          nrow(value) == 1L &&
          identical(value$mode[[1L]], expected_mode)
      ) {
        return(value)
      }
      if (Sys.time() >= deadline) {
        rlang::abort(paste(
          "Livy batch marker did not reach mode",
          shQuote(expected_mode),
          "in time"
        ))
      }
      Sys.sleep(2)
    }
  }

  success <- fabric_livy_batch_submit(
    lakehouse$livy_url,
    file = lakehouse$livy_batch_file,
    name = "fabricqueryr-batch-success",
    args = "success",
    target_lakehouse_id = lakehouse$id,
    token = token,
    verbose = FALSE
  )
  success$wait(timeout = 1200, poll_interval = 5)
  success_result <- success$result(refresh = FALSE)
  expect_s3_class(success_result, "fabric_livy_batch_result")
  expect_identical(success_result$id, success$id)
  expect_equal(tolower(success_result$state), "success")
  success_marker <- wait_for_marker("success")
  expect_equal(success_marker$mode, "success")
  expect_equal(as.numeric(success_marker$row_count), 3)

  failure <- fabric_livy_batch_submit(
    lakehouse$livy_url,
    file = lakehouse$livy_batch_file,
    name = "fabricqueryr-batch-failure",
    args = "failure",
    target_lakehouse_id = lakehouse$id,
    token = token,
    verbose = FALSE
  )
  failure_error <- expect_error(
    failure$wait(timeout = 1200, poll_interval = 5),
    class = "fabric_livy_batch_error"
  )
  expect_equal(tolower(failure_error$batch$state), "dead")
  failure_marker <- wait_for_marker("failure")
  expect_equal(failure_marker$mode, "failure")
  expect_equal(as.numeric(failure_marker$row_count), -1)

  slow <- fabric_livy_batch_submit(
    lakehouse$livy_url,
    file = lakehouse$livy_batch_file,
    name = "fabricqueryr-batch-cancel",
    args = "slow",
    target_lakehouse_id = lakehouse$id,
    token = token,
    verbose = FALSE
  )
  on.exit(try(slow$cancel(), silent = TRUE), add = TRUE)
  deadline <- Sys.time() + 900
  repeat {
    slow_status <- slow$status()
    if (tolower(slow_status$state %||% "") == "running") {
      break
    }
    if (Sys.time() >= deadline) {
      rlang::abort("Slow Livy batch did not start in time")
    }
    Sys.sleep(5)
  }
  slow_marker <- wait_for_marker("slow")
  expect_equal(slow_marker$mode, "slow")
  expect_equal(as.numeric(slow_marker$row_count), -1)
  expect_true(slow$cancel())
  slow$wait(
    timeout = 600,
    poll_interval = 5,
    error_on_failure = FALSE
  )
  cancelled <- slow$result(
    refresh = FALSE,
    error_on_failure = FALSE
  )
  terminal <- tolower(c(
    cancelled$state,
    cancelled$result %||% "",
    cancelled$raw$fabricBatchStateInfo$state %||% ""
  ))
  expect_true(any(
    terminal %in%
      c(
        "killed",
        "dead",
        "cancelled",
        "canceled"
      )
  ))
})

test_that("Fabric item jobs complete, fail, time out, and cancel", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  notebook <- fabric_test_manifest_item(manifest, "JobFixtures")
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  item <- list(
    id = notebook$id,
    workspaceId = manifest$workspace_id,
    type = notebook$type,
    displayName = notebook$display_name
  )
  session_tag <- paste0(
    "fabricqueryr_job_integration_",
    substr(notebook$id, 1L, 8L)
  )
  completed_job <- fabric_job_run(
    item,
    parameters = list(mode = "success", marker = "integration"),
    default_lakehouse = lakehouse$id,
    session_tag = session_tag,
    token = token
  )
  expect_s3_class(completed_job, "fabric_job")
  completed <- fabric_job_wait(
    completed_job,
    timeout = 900,
    cancel_on_timeout = TRUE
  )
  expect_s3_class(completed, "fabric_job_instance")
  expect_equal(completed$status, "Completed")
  expect_equal(
    completed$exit_value,
    "fabricqueryr-job-success:integration"
  )
  expect_true(nzchar(completed$root_activity_id))
  expect_s3_class(completed$start_time, "POSIXct")
  expect_s3_class(completed$end_time, "POSIXct")
  status <- fabric_job_status(completed_job)
  expect_s3_class(status, "fabric_job_instance")
  expect_equal(status$id, completed$id)
  expect_equal(status$status, "Completed")

  failed_job <- fabric_job_run(
    item,
    parameters = list(mode = "failure"),
    default_lakehouse = lakehouse$id,
    session_tag = session_tag,
    token = token
  )
  failed <- rlang::catch_cnd(
    fabric_job_wait(
      failed_job,
      timeout = 900,
      cancel_on_timeout = TRUE
    ),
    classes = "error"
  )
  expect_s3_class(failed, "fabric_job_failed")
  expect_equal(failed$job_status$status, "Failed")
  expect_true(nzchar(failed$job_status$root_activity_id))
  expect_true(nzchar(
    .fabric_job_failure_text(failed$job_status$failure_reason)
  ))
  expect_match(
    .fabric_job_failure_text(failed$job_status$failure_reason),
    "FABRICQUERYR_INTENTIONAL_JOB_FAILURE",
    fixed = TRUE
  )

  slow_job <- fabric_job_run(
    item,
    parameters = list(mode = "slow", delay_seconds = 600L),
    default_lakehouse = lakehouse$id,
    session_tag = session_tag,
    token = token
  )
  on.exit(try(fabric_job_cancel(slow_job), silent = TRUE), add = TRUE)
  timed_out <- rlang::catch_cnd(
    fabric_job_wait(
      slow_job,
      timeout = 2,
      cancel_on_timeout = TRUE
    ),
    classes = "error"
  )
  expect_s3_class(timed_out, "fabric_job_timeout")

  slow_job$retry_after <- 2
  cancelled <- fabric_job_wait(
    slow_job,
    poll_interval = 2,
    timeout = 600,
    error_on_failure = FALSE
  )
  expect_equal(cancelled$status, "Cancelled")
  expect_true(nzchar(cancelled$root_activity_id))
})

test_that("Fabric pipeline and Spark job definition jobs complete", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  storage_token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  fixtures <- c("TestPipeline", "TestSparkJob")

  for (name in fixtures) {
    fixture <- fabric_test_manifest_item(manifest, name)
    item <- list(
      id = fixture$id,
      workspaceId = manifest$workspace_id,
      type = fixture$type,
      displayName = fixture$display_name
    )
    job <- fabric_job_run(item, token = token)
    result <- fabric_job_wait(job, timeout = 1200)

    expect_s3_class(result, "fabric_job_instance")
    expect_equal(result$status, "Completed", info = name)
    expect_equal(result$item_id, fixture$id, info = name)

    if (identical(name, "TestSparkJob")) {
      lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
      marker <- fabric_onelake_read_delta_table(
        table_path = lakehouse$tables$spark_job_result,
        workspace_name = manifest$workspace_id,
        lakehouse_name = lakehouse$id,
        schema = lakehouse$schema,
        token = storage_token,
        verbose = FALSE
      )
      expect_equal(marker$mode, "success")
      expect_equal(as.numeric(marker$row_count), 3)
    }
  }
})

test_that("fabric_pbi_dax_query resolves and queries a semantic model", {
  manifest <- fabric_test_manifest()
  semantic_model <- manifest$items$TestSemanticModel
  token <- fabric_test_token_provider()

  result <- fabric_pbi_dax_query(
    connstr = semantic_model$connection_string,
    dax = paste0(
      'EVALUATE ROW("row_count", COUNTROWS(\'Facts\'), ',
      '"amount_sum", SUM(\'Facts\'[amount]))'
    ),
    tenant_id = "",
    client_id = "",
    token = token
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 1L)
  expect_equal(as.numeric(result[["[row_count]"]]), 3)
  expect_equal(as.numeric(result[["[amount_sum]"]]), 30.5)

  rows <- fabric_pbi_dax_query(
    connstr = semantic_model$connection_string,
    dax = paste(
      "EVALUATE",
      "SELECTCOLUMNS(",
      "  'Facts',",
      '  "id", \'Facts\'[id],',
      '  "name", \'Facts\'[name],',
      '  "category", \'Facts\'[category],',
      '  "amount", \'Facts\'[amount]',
      ")",
      "ORDER BY [id]",
      sep = "\n"
    ),
    token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )
  expect_s3_class(rows, "tbl_df")
  expect_equal(nrow(rows), 3L)
  expect_named(rows, c("[id]", "[name]", "[category]", "[amount]"))
  expect_equal(as.numeric(rows[["[id]"]]), c(1, 2, 3))
  expect_equal(rows[["[name]"]], c("alpha", "beta", "gamma"))
  expect_equal(rows[["[category]"]], c("A", "B", "A"))
  expect_equal(as.numeric(rows[["[amount]"]]), c(10.5, 20, NA))

  empty <- fabric_pbi_dax_query(
    connstr = semantic_model$connection_string,
    dax = paste0(
      "EVALUATE FILTER(",
      "SELECTCOLUMNS('Facts', \"id\", 'Facts'[id]), ",
      "[id] > 100)"
    ),
    token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )
  expect_s3_class(empty, "tbl_df")
  expect_equal(nrow(empty), 0L)

  discovered_model <- fabric_item(
    manifest$workspace_id,
    semantic_model$id,
    type = "SemanticModel",
    token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )
  by_id <- fabric_pbi_dax_query(
    connstr = discovered_model,
    dax = 'EVALUATE ROW("row_count", COUNTROWS(\'Facts\'))',
    token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )
  expect_s3_class(by_id, "tbl_df")
  expect_equal(as.numeric(by_id[["[row_count]"]]), 3)

  multiple_tables <- expect_error(
    fabric_pbi_dax_query(
      connstr = semantic_model$connection_string,
      dax = paste(
        'EVALUATE ROW("first", 1)',
        'EVALUATE ROW("second", 2)',
        sep = "\n"
      ),
      token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
    )
  )
  expect_match(conditionMessage(multiple_tables), "Power BI", fixed = TRUE)
  expect_match(
    conditionMessage(multiple_tables),
    "(?i)(more than (one|1)|result (table|set))"
  )
})
