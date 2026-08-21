# Fabric integration coverage: querying a Power BI semantic model with DAX
# These tests use the sandbox model to check target resolution, typed query
# results, and the Arrow-based query path used for efficient data transfer

test_that("semantic-model refresh completes with history and execution details", {
  manifest <- fabric_test_manifest()
  semantic_model <- fabric_test_manifest_item(
    manifest,
    "TestArrowSemanticModel"
  )
  token <- fabric_test_token_provider()

  refresh <- fabric_pbi_refresh(
    workspace_id = manifest$workspace_id,
    dataset_id = semantic_model$id,
    mode = "enhanced",
    type = "Full",
    commit_mode = "Transactional",
    objects = "Facts",
    max_parallelism = 2L,
    retry_count = 1L,
    timeout = "00:10:00",
    token = token
  )
  expect_s3_class(refresh, "fabric_pbi_refresh")
  expect_identical(refresh$mode, "enhanced")

  completed <- fabric_pbi_refresh_wait(
    refresh,
    poll_interval = 2,
    timeout = 600
  )
  expect_s3_class(completed, "fabric_pbi_refresh_detail")
  expect_true(
    completed$state %in% c("Completed", "CompletedWithWarnings")
  )
  expect_identical(completed$id, refresh$id)
  expect_true(completed$number_of_attempts >= 1L)
  expect_true(length(completed$attempts) >= 1L)
  expect_match(completed$details_url, refresh$id, fixed = TRUE)

  history <- fabric_pbi_refresh_history(
    workspace_id = manifest$workspace_id,
    dataset_id = semantic_model$id,
    top = 10L,
    token = token
  )
  expect_s3_class(history, "fabric_pbi_refresh_history")
  history_ids <- vapply(history, `[[`, character(1), "id")
  expect_true(refresh$id %in% history_ids)
  history_entry <- history[[match(refresh$id, history_ids)]]
  expect_identical(history_entry$state, completed$state)

  execution <- fabric_pbi_refresh_status(history_entry)
  expect_identical(execution$state, completed$state)
  expect_true(length(execution$attempts) >= 1L)

  rows <- fabric_pbi_dax_query(
    workspace_id = manifest$workspace_id,
    dataset_id = semantic_model$id,
    dax = 'EVALUATE ROW("row_count", COUNTROWS(\'Facts\'))',
    token = token
  )
  expect_identical(as.numeric(rows[["[row_count]"]]), 3)
})

test_that("service-principal standard refresh omits email notification", {
  manifest <- fabric_test_manifest()
  semantic_model <- fabric_test_manifest_item(
    manifest,
    "TestArrowSemanticModel"
  )

  refresh <- fabric_pbi_refresh(
    workspace_id = manifest$workspace_id,
    dataset_id = semantic_model$id,
    notify_option = NULL,
    token = fabric_test_token_provider()
  )
  completed <- fabric_pbi_refresh_wait(
    refresh,
    poll_interval = 2,
    timeout = 600
  )

  expect_identical(refresh$mode, "standard")
  expect_true(completed$state %in% c("Completed", "CompletedWithWarnings"))
})

test_that("an enhanced semantic-model refresh can be cancelled", {
  manifest <- fabric_test_manifest()
  semantic_model <- fabric_test_manifest_item(
    manifest,
    "TestArrowSemanticModel"
  )
  token <- fabric_test_token_provider()

  refresh <- fabric_pbi_refresh(
    workspace_id = manifest$workspace_id,
    dataset_id = semantic_model$id,
    mode = "enhanced",
    type = "Full",
    commit_mode = "Transactional",
    objects = "Facts",
    max_parallelism = 1L,
    timeout = "00:10:00",
    token = token
  )
  cleanup_needed <- TRUE
  on.exit(
    if (cleanup_needed) {
      try(fabric_pbi_refresh_cancel(refresh), silent = TRUE)
    },
    add = TRUE
  )

  accepted <- fabric_pbi_refresh_cancel(refresh)
  expect_identical(accepted, TRUE)

  detail <- fabric_pbi_refresh_wait(
    refresh,
    poll_interval = 1,
    timeout = 300,
    error_on_failure = FALSE
  )
  cleanup_needed <- FALSE

  expect_s3_class(detail, "fabric_pbi_refresh_detail")
  expect_identical(detail$id, refresh$id)
  expect_identical(detail$state, "Cancelled")
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

test_that("JSON DAX preserves large whole numbers and scalar representations", {
  manifest <- fabric_test_manifest()
  semantic_model <- fabric_test_manifest_item(
    manifest,
    "TestSemanticModel"
  )

  result <- fabric_pbi_dax_query(
    workspace_id = manifest$workspace_id,
    dataset_id = semantic_model$id,
    dax = paste0(
      "EVALUATE ROW(",
      "\"positive\", CONVERT(\"9007199254740993\", INTEGER), ",
      "\"negative\", CONVERT(\"-9007199254740993\", INTEGER), ",
      "\"fixed_decimal\", CONVERT(\"123.45\", CURRENCY), ",
      "\"date\", DATE(2026, 8, 6))"
    ),
    token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )

  expect_identical(result$`[positive]`, "9007199254740993")
  expect_identical(result$`[negative]`, "-9007199254740993")
  expect_equal(result$`[fixed_decimal]`, 123.45)
  expect_match(as.character(result$`[date]`), "^2026-08-06")

  mixed <- fabric_pbi_dax_query(
    workspace_id = manifest$workspace_id,
    dataset_id = semantic_model$id,
    dax = paste0(
      "EVALUATE UNION(",
      "ROW(\"mixed\", CONVERT(\"1\", INTEGER)), ",
      "ROW(\"mixed\", CONVERT(\"9007199254740993\", INTEGER)))"
    ),
    token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )
  expect_identical(mixed$`[mixed]`, c("1", "9007199254740993"))
})

test_that("fabric_pbi_dax_query consumes the Arrow DAX API", {
  fabric_test_require_package("arrow")
  fabric_test_require_package("nanoarrow")
  manifest <- fabric_test_manifest()
  semantic_model <- fabric_test_manifest_item(
    manifest,
    "TestArrowSemanticModel"
  )
  token <- fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  query <- paste(
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
  )

  rows <- fabric_pbi_dax_query(
    connstr = semantic_model$connection_string,
    dax = query,
    api = "arrow",
    arrow_options = list(
      culture = "en-US",
      executionMetrics = TRUE,
      queryTimeout = 120,
      resultSetRowCountLimit = 2
    ),
    token = token
  )
  names(rows) <- sub("^.*\\[([^]]+)\\]$", "\\1", names(rows))
  expect_s3_class(rows, "tbl_df")
  expect_named(rows, c("id", "name", "category", "amount"))
  expect_equal(nrow(rows), 2L)
  expect_equal(as.numeric(rows$id), c(1, 2))
  expect_equal(rows$name, c("alpha", "beta"))
  expect_equal(rows$category, c("A", "B"))
  expect_equal(as.numeric(rows$amount), c(10.5, 20))
  metrics <- attr(rows, "execution_metrics")
  # The preview endpoint can omit its optional requested metrics rowset
  if (!is.null(metrics)) {
    expect_s3_class(metrics, "tbl_df")
  }
  stream <- fabric_pbi_dax_query(
    workspace_id = manifest$workspace_id,
    dataset_id = semantic_model$id,
    dax = query,
    api = "arrow",
    result = "arrow_stream",
    token = token
  )
  expect_s3_class(stream, "nanoarrow_array_stream")
  streamed <- arrow::as_record_batch_reader(stream)$read_table()
  streamed <- suppressWarnings(as.data.frame(streamed))
  names(streamed) <- sub("^.*\\[([^]]+)\\]$", "\\1", names(streamed))
  expect_equal(nrow(streamed), 3L)
  expect_equal(as.numeric(streamed$id), c(1, 2, 3))
  expect_equal(as.character(streamed$name), c("alpha", "beta", "gamma"))
  expect_equal(
    as.numeric(as.character(streamed$amount)),
    c(10.5, 20, NA)
  )

  schema <- fabric_pbi_dax_query(
    workspace_id = manifest$workspace_id,
    dataset_id = semantic_model$id,
    dax = query,
    api = "arrow",
    arrow_options = list(schemaOnly = TRUE),
    token = token
  )
  names(schema) <- sub("^.*\\[([^]]+)\\]$", "\\1", names(schema))
  expect_s3_class(schema, "tbl_df")
  expect_named(schema, c("id", "name", "category", "amount"))
  expect_equal(nrow(schema), 0L)

  query_error <- expect_error(
    fabric_pbi_dax_query(
      workspace_id = manifest$workspace_id,
      dataset_id = semantic_model$id,
      dax = "EVALUATE 'This table does not exist'",
      api = "arrow",
      token = token
    )
  )
  expect_match(
    conditionMessage(query_error),
    "(?i)(Power BI|DAX|query)"
  )
})

test_that("delegated DAX queries can target My Workspace", {
  fabric_test_delegated_auth_config()
  dataset_id <- fabric_test_optional_environment(
    "FABRIC_TEST_PERSONAL_DATASET_ID",
    "My Workspace DAX coverage"
  )

  result <- fabric_pbi_dax_query(
    dataset_id = dataset_id,
    my_workspace = TRUE,
    dax = 'EVALUATE ROW("fabricQueryR", 1)',
    token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )

  expect_s3_class(result, "tbl_df")
  expect_identical(as.numeric(result[["[fabricQueryR]"]]), 1)
})

test_that("delegated default standard refresh satisfies the Power BI contract", {
  fabric_test_delegated_auth_config()
  dataset_id <- fabric_test_optional_environment(
    "FABRIC_TEST_PERSONAL_DATASET_ID",
    "My Workspace delegated refresh coverage"
  )
  token <- fabric_test_token("FABRIC_TEST_PBI_TOKEN")

  refresh <- fabric_pbi_refresh(
    dataset_id = dataset_id,
    my_workspace = TRUE,
    token = token
  )
  completed <- fabric_pbi_refresh_wait(
    refresh,
    poll_interval = 2,
    timeout = 600
  )

  expect_identical(refresh$mode, "standard")
  expect_true(completed$state %in% c("Completed", "CompletedWithWarnings"))
})
