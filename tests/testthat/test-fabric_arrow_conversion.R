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

test_that("uint64 buffers convert exactly without the Arrow package", {
  hexadecimal <- c(
    "0000000000000000",
    "0000000000000001",
    "0000000100000000",
    "001fffffffffffff",
    "0020000000000000",
    "0020000000000001",
    "7fffffffffffffff",
    "8000000000000000",
    "ffffffffffffffff",
    "0000000000000000"
  )
  bytes <- lapply(hexadecimal, function(value) {
    pairs <- substring(value, seq(1L, 15L, 2L), seq(2L, 16L, 2L))
    result <- as.raw(strtoi(pairs, base = 16L))
    if (.Platform$endian == "little") rev(result) else result
  })
  schema <- nanoarrow::na_struct(list(value = nanoarrow::na_uint64()))
  array <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(nanoarrow::na_uint64()),
    list(
      length = 10L,
      null_count = 1L,
      buffers = list(as.raw(c(255L, 1L)), do.call(c, bytes))
    )
  )
  batch <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(schema),
    list(length = 10L, children = list(value = array))
  )
  expected <- c(
    "0",
    "1",
    "4294967296",
    "9007199254740991",
    "9007199254740992",
    "9007199254740993",
    "9223372036854775807",
    "9223372036854775808",
    "18446744073709551615",
    NA_character_
  )

  for (batches in list(list(batch), list(batch, batch), list())) {
    stream <- nanoarrow::basic_array_stream(batches, schema = schema)
    withr::defer(nanoarrow::nanoarrow_pointer_release(stream))
    result <- .fabric_arrow_exact_tibble(stream)
    expect_identical(result$value, rep(expected, length(batches)))
  }
})

test_that("uint64 conversion preserves slices and nested dictionary values", {
  skip_if_not_installed("arrow")
  values <- c("0", "9223372036854775808", "18446744073709551615", NA)
  unsigned <- arrow::Array$create(values)$cast(arrow::uint64())
  table <- arrow::Table$create(
    unsigned = unsigned,
    signed = arrow::Array$create(c(
      "0",
      "-9223372036854775808",
      "9223372036854775807",
      NA
    ))$cast(arrow::int64()),
    nested = arrow::StructArray$create(value = unsigned),
    dictionary = arrow::DictionaryArray$create(
      arrow::Array$create(c(2L, 1L, NA_integer_, 0L)),
      unsigned
    )
  )$Slice(1L, 3L)
  stream <- nanoarrow::as_nanoarrow_array_stream(table)
  withr::defer(nanoarrow::nanoarrow_pointer_release(stream))

  result <- .fabric_arrow_exact_tibble(stream)
  expect_identical(result$unsigned, values[-1L])
  expect_identical(result$nested$value, values[-1L])
  expect_identical(result$dictionary, c("9223372036854775808", NA, "0"))
  expect_identical(
    result$signed,
    c("-9223372036854775808", "9223372036854775807", NA)
  )
})

test_that("uint64 list conversion retains null elements and empty lists", {
  skip_if_not_installed("arrow")
  values <- list(
    c("9223372036854775808", "18446744073709551615", NA),
    character(),
    NULL,
    c("0", "9007199254740993")
  )
  unsigned <- arrow::Array$create(values)$cast(arrow::list_of(arrow::uint64()))
  stream <- nanoarrow::as_nanoarrow_array_stream(arrow::Table$create(
    value = unsigned
  ))
  withr::defer(nanoarrow::nanoarrow_pointer_release(stream))

  result <- .fabric_arrow_exact_tibble(stream)
  expect_identical(as.list(result$value), values)
})

test_that("uint64 nested struct slices apply parent null masks", {
  skip_if_not_installed("arrow")
  values <- c("0", "9223372036854775808", "10", "18446744073709551615")
  array <- nanoarrow::as_nanoarrow_array(arrow::StructArray$create(
    value = arrow::Array$create(values)$cast(arrow::uint64())
  ))
  array <- nanoarrow::nanoarrow_array_modify(
    array,
    list(
      buffers = list(as.raw(11L)),
      null_count = 1L,
      offset = 1L,
      length = 3L
    )
  )
  schema <- nanoarrow::na_struct(list(
    nested = nanoarrow::infer_nanoarrow_schema(array)
  ))
  batch <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(schema),
    list(length = 3L, children = list(nested = array))
  )
  stream <- nanoarrow::basic_array_stream(list(batch))
  withr::defer(nanoarrow::nanoarrow_pointer_release(stream))

  result <- .fabric_arrow_exact_tibble(stream)
  expect_identical(
    result$nested$value,
    c("9223372036854775808", NA, "18446744073709551615")
  )
})
