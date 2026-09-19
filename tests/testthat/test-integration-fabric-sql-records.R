# Fabric integration coverage: SQL table records
test_that("SQL table records read the same live table as explicit identifiers", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  warehouse <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestWarehouse")$id,
    token = token
  )
  con <- fabric_sql_connect(warehouse, token = token, verbose = FALSE)
  withr::defer(DBI::dbDisconnect(con))
  table <- paste0("fabricqueryr_records_", Sys.getpid())
  sql <- paste0("[dbo].[", table, "]")
  withr::defer(DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", sql)))
  DBI::dbExecute(con, paste("CREATE TABLE", sql, "(id varchar(10))"))
  DBI::dbExecute(con, paste("INSERT INTO", sql, "VALUES ('marker')"))
  tables <- fabric_sql_tables(
    warehouse,
    schema = "dbo",
    detail = FALSE,
    token = token,
    verbose = FALSE
  )
  row <- tables[tables$name == table, ]
  expect_equal(nrow(row), 1L)
  for (record in list(
    table,
    row,
    as.list(row),
    list(name = table, schema = "dbo")
  )) {
    rows <- fabric_sql_read_table(
      warehouse,
      record,
      token = token,
      verbose = FALSE
    )
    expect_identical(rows$id, "marker")
  }
})
