# Fabric integration coverage: KQL analytics queries and the GraphQL API.
# The tests query seeded Eventhouse and Warehouse data in the live sandbox,
# covering types, parameters, pagination, mutations, and service errors.

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
  expect_identical(result$amount, c(10.5, 20, NA_real_))
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

  exact_decimal <- fabric_kql_query(
    database$query_service_uri,
    query = paste0(
      "print positive=decimal(1234567890123456789.123456789012345), ",
      "negative=decimal(-1234567890123456789.123456789012345)"
    ),
    database = database$database_name,
    token = token
  )
  expect_identical(
    exact_decimal$positive,
    "1234567890123456789.123456789012345"
  )
  expect_identical(
    exact_decimal$negative,
    "-1234567890123456789.123456789012345"
  )
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

  empty_dynamic <- fabric_kql_query(
    target,
    query = paste(
      "declare query_parameters(selected:dynamic, options:dynamic);",
      "print selected_count=array_length(selected),",
      "option_count=array_length(bag_keys(options))"
    ),
    parameters = list(
      selected = character(),
      options = setNames(list(), character())
    ),
    token = kusto_token
  )
  expect_equal(empty_dynamic$selected_count, 0L)
  expect_equal(empty_dynamic$option_count, 0L)
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

test_that("tracked Eventhouse ingestion completes and prevents duplicates", {
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  table <- database$tables$ingestion %||% "fabricqueryr_ingestion"
  mapping <- database$mappings$ingestion_csv %||%
    "fabricqueryr_ingestion_csv"
  source <- paste0(
    lakehouse$one_lake_files_path,
    "/fixtures/basic.csv;impersonate"
  )
  idempotency_key <- paste0(
    "fabricqueryr-priority7-",
    format(Sys.time(), "%Y%m%d%H%M%OS6", tz = "UTC"),
    "-",
    Sys.getpid()
  )
  count_rows <- function() {
    result <- fabric_kql_query(
      database$query_service_uri,
      query = paste(table, "| count"),
      database = database$database_name,
      token = token
    )
    as.numeric(result$Count)
  }
  before <- count_rows()

  ingestion <- fabric_kql_ingest(
    database,
    table = table,
    sources = source,
    format = "csv",
    source_ids = kusto_ingestion_source_id(),
    raw_sizes = 66,
    mapping = mapping,
    tags = "fabricqueryr-integration",
    ingest_if_not_exists = idempotency_key,
    ignore_first_record = TRUE,
    skip_batching = TRUE,
    token = token
  )
  expect_s3_class(ingestion, "fabric_kql_ingestion")
  expect_true(fabric_is_guid(ingestion$sources$source_id))
  expect_true(
    paste0("ingest-by:", idempotency_key) %in% ingestion$tags
  )

  status <- fabric_kql_ingestion_status(
    ingestion,
    wait = TRUE,
    timeout = 600,
    poll_interval = 2
  )
  expect_equal(status$state, "Succeeded")
  expect_true(status$complete)
  expect_equal(status$succeeded, 1)
  expect_equal(status$details$source_id, ingestion$sources$source_id)

  rows <- fabric_kql_query(
    database$query_service_uri,
    query = paste(table, "| order by id asc"),
    database = database$database_name,
    token = token
  )
  expect_equal(nrow(rows), before + 3)
  expect_true(all(c("alpha", "beta", "gamma") %in% rows$name))

  duplicate <- fabric_kql_ingest(
    database,
    table = table,
    sources = source,
    format = "csv",
    source_ids = kusto_ingestion_source_id(),
    raw_sizes = 66,
    mapping = mapping,
    ingest_if_not_exists = idempotency_key,
    ignore_first_record = TRUE,
    skip_batching = TRUE,
    token = token
  )
  duplicate_status <- fabric_kql_ingestion_status(
    duplicate,
    wait = TRUE,
    timeout = 600,
    poll_interval = 2,
    error_on_failure = FALSE
  )
  expect_true(duplicate_status$complete)
  expect_gt(duplicate_status$failed, 0)
  expect_equal(count_rows(), before + 3)
})

test_that("R and lazy Arrow objects write through tracked Eventhouse staging", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  token <- fabric_test_token_provider()
  table <- database$tables$ingestion %||% "fabricqueryr_ingestion"
  nonce <- paste0(
    format(Sys.time(), "%Y%m%d%H%M%OS6", tz = "UTC"),
    "-",
    Sys.getpid()
  )
  base_id <- as.integer(as.numeric(Sys.time()) %% 100000000) * 10L
  frame_category <- paste0("r-frame-", nonce)
  arrow_category <- paste0("r-arrow-", nonce)

  frame <- data.frame(
    id = base_id + 1:2,
    name = c("frame-a", "frame-b"),
    category = frame_category,
    amount = c(10.5, 20.5),
    stringsAsFactors = FALSE
  )
  frame_result <- fabric_kql_write_table(
    database,
    table = table,
    data = frame,
    ingest_if_not_exists = paste0("frame-", nonce),
    skip_batching = TRUE,
    timeout = 600,
    token = token
  )
  expect_equal(frame_result$status$state, "Succeeded")
  expect_equal(frame_result$rows, 2)
  expect_false(frame_result$staging_retained)

  dataset_path <- tempfile("fabricqueryr-kql-dataset-")
  dir.create(dataset_path)
  on.exit(unlink(dataset_path, recursive = TRUE, force = TRUE), add = TRUE)
  arrow::write_parquet(
    data.frame(
      id = base_id + 3:4,
      name = c("arrow-a", "arrow-b"),
      category = arrow_category,
      amount = c(30.5, 40.5),
      stringsAsFactors = FALSE
    ),
    file.path(dataset_path, "part-1.parquet")
  )
  arrow::write_parquet(
    data.frame(
      id = base_id + 5L,
      name = "arrow-c",
      category = arrow_category,
      amount = 50.5,
      stringsAsFactors = FALSE
    ),
    file.path(dataset_path, "part-2.parquet")
  )
  dataset <- arrow::open_dataset(dataset_path)
  arrow_result <- fabric_kql_write_table(
    database,
    table = table,
    data = dataset,
    ingest_if_not_exists = paste0("arrow-", nonce),
    skip_batching = TRUE,
    max_rows_per_file = 1,
    timeout = 600,
    token = token
  )
  expect_equal(arrow_result$status$state, "Succeeded")
  expect_equal(arrow_result$rows, 3)
  expect_equal(arrow_result$file_count, 3L)
  expect_false(arrow_result$staging_retained)

  rows <- fabric_test_eventually(function() {
    value <- fabric_kql_query(
      database,
      query = paste(
        "declare query_parameters(frame_category:string,",
        "arrow_category:string);",
        table,
        "| where category == frame_category or category == arrow_category",
        "| summarize rows=count() by category"
      ),
      parameters = list(
        frame_category = frame_category,
        arrow_category = arrow_category
      ),
      token = token
    )
    if (sum(as.numeric(value$rows)) != 5L) {
      return(NULL)
    }
    value
  })
  counts <- setNames(as.numeric(rows$rows), rows$category)
  expect_equal(unname(counts[[frame_category]]), 2)
  expect_equal(unname(counts[[arrow_category]]), 3)
})

test_that("R data creates a missing KQL table from its Arrow schema", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  token <- fabric_test_token_provider()
  table <- paste0(
    "fabricqueryr_r_create_",
    format(Sys.time(), "%Y%m%d%H%M%S", tz = "UTC"),
    "_",
    Sys.getpid()
  )
  target <- kusto_resolve_target(database)
  credential <- fabric_credential(token = token)
  drop_command <- paste(
    ".drop table",
    kusto_write_identifier(table, "table"),
    "ifexists"
  )
  drop_table <- function() {
    kusto_export_management(
      target,
      drop_command,
      credential,
      deadline = Sys.time() + 60,
      idempotent = TRUE,
      operation = "DropCreatedTable"
    )
  }
  try(drop_table(), silent = TRUE)
  on.exit(try(drop_table(), silent = TRUE), add = TRUE)

  written <- fabric_kql_write_table(
    database,
    table,
    data.frame(
      id = 1:2,
      label = c("created-a", "created-b"),
      amount = c(10.5, 20.5),
      active = c(TRUE, FALSE),
      observed_at = as.POSIXct(
        c("2026-08-14 10:00:00", "2026-08-14 11:00:00"),
        tz = "UTC"
      )
    ),
    create_if_missing = TRUE,
    skip_batching = TRUE,
    timeout = 600,
    token = token
  )
  expect_true(written$table_creation_requested)
  expect_equal(written$status$state, "Succeeded")

  rows <- fabric_test_eventually(function() {
    value <- fabric_kql_query(
      database,
      query = paste0(
        kusto_write_identifier(table, "table"),
        " | project id, label, amount, active | order by id asc"
      ),
      token = token
    )
    if (nrow(value) != 2L) NULL else value
  })
  expect_equal(as.integer(rows$id), 1:2)
  expect_equal(rows$label, c("created-a", "created-b"))
  expect_equal(rows$amount, c(10.5, 20.5))
  expect_equal(rows$active, c(TRUE, FALSE))
})

test_that("server-side KQL export writes readable Parquet artifacts to OneLake", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  root <- paste0(
    "Files/fabricqueryr-kql-export/",
    format(Sys.time(), "%Y%m%d%H%M%OS6", tz = "UTC"),
    "-",
    Sys.getpid()
  )
  removed <- FALSE
  on.exit(
    if (!removed) {
      try(
        fabric_onelake_delete(
          manifest$workspace_id,
          lakehouse$id,
          root,
          recursive = TRUE,
          confirm = TRUE,
          token = token
        ),
        silent = TRUE
      )
    },
    add = TRUE
  )

  exported <- fabric_kql_export(
    database,
    query = paste(
      database$tables$events,
      "| project id, name, category, amount"
    ),
    destination = lakehouse,
    workspace = manifest$workspace_id,
    path = root,
    format = "parquet",
    name_prefix = "events",
    compression_type = "snappy",
    timeout = 600,
    poll_interval = 2,
    token = token
  )
  expect_s3_class(exported, "fabric_kql_export_result")
  expect_equal(exported$state, "Completed")
  expect_gte(exported$file_count, 1L)
  expect_equal(as.numeric(exported$records), 3)

  files <- fabric_test_eventually(function() {
    value <- fabric_onelake_list(
      manifest$workspace_id,
      lakehouse$id,
      path = root,
      recursive = TRUE,
      token = token
    )
    parquet <- value[
      !value$is_directory & grepl("[.]parquet$", value$path),
      ,
      drop = FALSE
    ]
    if (!nrow(parquet)) NULL else parquet
  })
  rows <- dplyr::bind_rows(lapply(files$path, function(file) {
    fabric_onelake_read_file(
      manifest$workspace_id,
      lakehouse$id,
      path = file,
      format = "parquet",
      token = token
    )
  }))
  rows <- rows[order(rows$id), , drop = FALSE]
  expect_equal(rows$id, 1:3)
  expect_equal(rows$name, c("alpha", "beta", "gamma"))
  expect_equal(rows$category, c("A", "B", "A"))
  expect_equal(rows$amount, c(10.5, 20, NA_real_))

  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    root,
    recursive = TRUE,
    confirm = TRUE,
    token = token
  ))
  removed <- TRUE
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

test_that("Fabric GraphQL introspection reflects the live API setting", {
  manifest <- fabric_test_manifest()
  api <- fabric_test_manifest_item(manifest, "TestGraphQL")
  token <- fabric_test_token_provider()

  outcome <- tryCatch(
    fabric_graphql_schema(
      api$endpoint,
      token = token,
      audience = "https://api.fabric.microsoft.com/.default"
    ),
    fabric_graphql_introspection_error = identity
  )

  if (inherits(outcome, "fabric_graphql_introspection_error")) {
    expect_match(outcome$message, "API Settings > Introspection", fixed = TRUE)
    expect_gt(length(outcome$errors), 0L)
  } else {
    expect_s3_class(outcome, "fabric_graphql_schema")
    expect_equal(outcome$queryType$name, "Query")
    query_types <- Filter(
      function(type) identical(type$name, outcome$queryType$name),
      outcome$types
    )
    expect_length(query_types, 1L)
    expect_true(
      api$root_field %in%
        vapply(
          query_types[[1L]]$fields,
          `[[`,
          character(1),
          "name"
        )
    )
  }
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

  rows <- fabric_graphql_collect(pages, c(root_field, "items"))
  expect_s3_class(rows, "fabric_graphql_rows")
  expect_identical(rows$id, c(1L, 2L, 3L))
  expect_identical(rows$name, c("alpha", "beta", "gamma"))
  expect_true(attr(rows, "complete"))
  expect_identical(attr(rows, "page_count"), 2L)
  expect_length(attr(rows, "errors"), 0L)
})

test_that("Fabric GraphQL executes a live mutation", {
  manifest <- fabric_test_manifest()
  api <- fabric_test_manifest_item(manifest, "TestGraphQL")
  warehouse <- fabric_test_manifest_item(manifest, "TestWarehouse")
  token <- fabric_test_token_provider()
  sql_token <- fabric_test_token("FABRIC_TEST_SQL_TOKEN")
  con <- fabric_sql_connect(
    warehouse$connection_string,
    database = warehouse$database_name,
    token = sql_token,
    verbose = FALSE
  )
  on.exit(
    {
      try(
        DBI::dbExecute(
          con,
          "DELETE FROM dbo.fabricqueryr_sql_types WHERE id = -99"
        ),
        silent = TRUE
      )
      try(DBI::dbDisconnect(con), silent = TRUE)
    },
    add = TRUE
  )
  DBI::dbExecute(
    con,
    "DELETE FROM dbo.fabricqueryr_sql_types WHERE id = -99"
  )

  result <- fabric_graphql_query(
    api$endpoint,
    query = paste0(
      "mutation CreateFixture {",
      "  ",
      api$create_field,
      "(",
      "    item: {",
      "      id: -99,",
      '      name: "mutation",',
      '      category: "M",',
      "      amount: 12.5,",
      '      loaded_at: "2026-01-01T00:00:00Z"',
      "    }",
      "  ) { __typename }",
      "}"
    ),
    operation_name = "CreateFixture",
    error_policy = "error",
    token = token,
    audience = "https://api.fabric.microsoft.com/.default"
  )

  expect_equal(
    result$data[[api$create_field]]$`__typename`,
    "DbOperationResult"
  )
  created <- DBI::dbGetQuery(
    con,
    paste(
      "SELECT id, name, category, amount",
      "FROM dbo.fabricqueryr_sql_types",
      "WHERE id = -99"
    )
  )
  expect_equal(created$id, -99L)
  expect_equal(created$name, "mutation")
  expect_equal(created$category, "M")
  expect_equal(as.numeric(created$amount), 12.5)
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
