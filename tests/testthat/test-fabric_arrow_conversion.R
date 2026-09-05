test_that("Arrow tibble conversion preserves decimals and integer sentinels", {
  skip_if_not_installed("arrow")
  table <- arrow::read_ipc_stream(
    test_path("fixtures", "exact-numerics.arrow"),
    as_data_frame = FALSE
  )
  stream <- nanoarrow::as_nanoarrow_array_stream(table)
  withr::defer(nanoarrow::nanoarrow_pointer_release(stream))
  result <- .fabric_arrow_exact_tibble(stream)
  expect_identical(result$amount, c("12345678901234567890.1234", NA))
  expect_identical(result$i32, c(-2147483648, NA))
  expect_identical(result$i64, c("-9223372036854775808", NA))
  expect_s3_class(result$large, "integer64")
  expect_identical(as.character(result$large), c("2147483648", NA))
})

test_that("nested and dictionary Arrow numeric values use exact prototypes", {
  skip_if_not_installed("arrow")
  table <- arrow::read_ipc_stream(
    test_path("fixtures", "exact-numerics.arrow"),
    as_data_frame = FALSE
  )
  stream <- nanoarrow::as_nanoarrow_array_stream(table)
  withr::defer(nanoarrow::nanoarrow_pointer_release(stream))
  result <- .fabric_arrow_exact_tibble(stream)
  expect_identical(result$nested$value, c("-9223372036854775808", NA))
  expect_identical(result$dictionary, c("-9223372036854775808", NA))
  expect_identical(result$list[[1L]], c("-9223372036854775808", NA))
  expect_identical(result$list[[2L]], character())
})
