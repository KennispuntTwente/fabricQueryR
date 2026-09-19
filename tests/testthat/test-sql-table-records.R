test_that("SQL table reads accept named lists and discovered rows consistently", {
  queried <- NULL
  local_mocked_bindings(fabric_sql_query = function(...) {
    queried <<- list(...)
    tibble::tibble(id = 1L)
  })
  row <- tibble::tibble(name = "orders]archive", schema = "sales data")
  for (record in list(
    row,
    as.data.frame(row),
    as.list(row),
    list(table = row$name, schema_name = row$schema),
    list(displayName = row$name, schema = row$schema)
  )) {
    fabric_sql_read_table("warehouse.example", record, token = "token")
    expect_identical(
      queried$sql,
      "SELECT * FROM [sales data].[orders]]archive]"
    )
    fabric_sql_read_table(
      "warehouse.example",
      record,
      schema = "override",
      token = "token"
    )
    expect_identical(queried$sql, "SELECT * FROM [override].[orders]]archive]")
  }
  fabric_sql_read_table(
    "warehouse.example",
    list(name = "orders"),
    token = "token"
  )
  expect_identical(queried$sql, "SELECT * FROM [dbo].[orders]")
})

test_that("malformed SQL table lists fail before querying", {
  local_mocked_bindings(fabric_sql_query = function(...) {
    stop("Unexpected query")
  })
  for (record in list(
    list("orders"),
    list(schema = "sales"),
    list(name = c("one", "two")),
    list(name = NA_character_),
    list(name = ""),
    list(name = list("orders")),
    list(name = "orders", schema = c("one", "two")),
    list(name = "orders", name = "other")
  )) {
    expect_error(
      fabric_sql_read_table("warehouse.example", record, token = "token"),
      class = "fabric_sql_table_error"
    )
  }
})
