test_that("Parquet staging writes batches and bounded files with Arrow 9 APIs", {
  skip_if_not_installed("arrow")
  table <- arrow::Table$create(value = 1:5)
  path <- withr::local_tempfile(fileext = ".parquet")
  prepared <- .fabric_parquet_prepare_data(table, "test")
  written <- .fabric_parquet_write_stream(
    prepared,
    path,
    "snappy",
    "test",
    "fabric_arrow_error"
  )
  expect_equal(written$rows, 5)
  expect_identical(arrow::read_parquet(path)$value, 1:5)

  directory <- withr::local_tempdir()
  prepared <- .fabric_parquet_prepare_data(table, "test")
  files <- .fabric_parquet_write_dataset(
    prepared,
    directory,
    "snappy",
    target_file_size = 1024^2,
    max_rows_per_file = 2,
    caller = "test",
    error_class = "fabric_arrow_error"
  )
  expect_identical(files$rows_per_file, c(2, 2, 1))
  expect_identical(
    unlist(lapply(files$paths, function(path) {
      arrow::read_parquet(path)$value
    })),
    1:5
  )
})
