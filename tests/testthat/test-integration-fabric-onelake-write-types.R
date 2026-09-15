# Fabric integration coverage: exact, nested, and empty Lakehouse writes
test_that("Lakehouse writes retain decimals and nested Arrow values with and without schemas", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  fabric_test_use_table_delta_runtime()
  token <- fabric_test_token_provider()
  decimal_text <- c("9007199254740993.1234", "-0.0001", NA_character_)
  expected <- arrow::Table$create(
    id = 1:3,
    amount = arrow::Array$create(decimal_text)$cast(arrow::decimal128(24, 4)),
    profile = arrow::Array$create(data.frame(
      label = c("caf\u00e9", NA, ""),
      active = c(TRUE, FALSE, NA)
    )),
    values = arrow::Array$create(
      list(c(1L, NA_integer_), integer(), NULL),
      type = arrow::list_of(arrow::int32())
    )
  )
  for (name in c("TestLakehouse", "TestLakehouseNoSchemas")) {
    fixture <- fabric_test_manifest_item(manifest, name)
    target <- fabric_item(manifest$workspace_id, fixture$id, token = token)
    table <- "fabricqueryr_complex_arrow_load"
    result <- fabric_lakehouse_write_table(
      target,
      table,
      expected,
      max_rows_per_file = 2L,
      token = token
    )
    expect_identical(result$operation_status$status, "Succeeded", info = name)
    expect_equal(result$rows, 3)
    expect_identical(result$file_count, 2L)
    expect_false(result$staging_retained)
    expect_identical(is.null(result$schema), name == "TestLakehouseNoSchemas")
    actual <- fabric_test_eventually(function() {
      stream <- fabric_lakehouse_read_table(
        target,
        table,
        token = token,
        result = "arrow_stream",
        verbose = FALSE
      )
      reader <- arrow::as_record_batch_reader(stream)
      on.exit(reader$Close())
      value <- reader$read_table()
      if (value$num_rows != 3L) {
        return(NULL)
      }
      value
    })
    order <- order(as.vector(actual[["id"]]))
    expect_identical(
      as.vector(actual[["amount"]]$cast(arrow::utf8()))[order],
      decimal_text
    )
    # Delta reads intentionally return exact decimal text to R. Verify the
    # stored precision independently through OneLake's catalog metadata.
    tables <- fabric_lakehouse_tables(target, token = token)
    columns <- tables$columns[[which(tables$name == table)]]
    amount <- Filter(function(column) column$name == "amount", columns)[[1L]]
    expect_identical(amount$type_name, "decimal(24,4)")
    rows <- as.data.frame(actual)
    expect_equal(rows$profile[order, ], as.data.frame(expected)$profile)
    expect_equal(
      lapply(rows$values[order], identity),
      list(c(1L, NA_integer_), integer(), NULL)
    )

    # An empty overwrite must remove old rows while retaining the declared
    # schema. An empty append must then leave that valid zero-row table intact.
    empty <- expected$Slice(0L, 0L)
    for (mode in c("Overwrite", "Append")) {
      written <- fabric_lakehouse_write_table(
        target,
        table,
        empty,
        mode = mode,
        token = token
      )
      expect_equal(written$rows, 0)
      expect_false(written$staging_retained)
      stream <- fabric_lakehouse_read_table(
        target,
        table,
        token = token,
        result = "arrow_stream",
        verbose = FALSE
      )
      reader <- arrow::as_record_batch_reader(stream)
      observed <- tryCatch(reader$read_table(), finally = reader$Close())
      expect_equal(observed$num_rows, 0L, info = paste(name, mode))
      expect_identical(observed$schema$ToString(), actual$schema$ToString())
    }
  }
})
