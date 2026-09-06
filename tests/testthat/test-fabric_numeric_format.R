test_that("numeric text recovers finite binary64 values across their range", {
  withr::local_seed(3701)
  values <- readBin(
    as.raw(sample.int(256L, 8L * 20000L, replace = TRUE) - 1L),
    "double",
    n = 20000L,
    size = 8L
  )
  values <- c(
    values[is.finite(values)],
    as.numeric(c("-0.0", "0.0")),
    .Machine$double.xmin * .Machine$double.eps,
    -.Machine$double.xmin * .Machine$double.eps,
    .Machine$double.xmin,
    .Machine$double.xmax,
    -.Machine$double.xmax,
    1 - .Machine$double.eps / 2,
    1 + .Machine$double.eps,
    pi,
    2^53 + 2
  )
  withr::local_options(OutDec = ",", digits = 3L, scipen = 999L)

  encoded <- vapply(values, fabric_format_number, character(1))
  decoded <- as.numeric(encoded)

  expect_identical(
    writeBin(decoded, raw(), size = 8L),
    writeBin(values, raw(), size = 8L)
  )
})

test_that("numeric text retains scalar shape and simple integer spelling", {
  expect_identical(fabric_format_number(c(value = 42L)), "42")
  expect_identical(fabric_format_number(10000000000), "10000000000")
  expect_identical(fabric_format_number(-123L), "-123")
  expect_identical(fabric_format_number(1.5), "1.5")
  expect_identical(fabric_format_number(as.numeric("-0.0")), "-0.0")
  expect_identical(fabric_format_number(as.numeric("0.0")), "0")
})

test_that("fixed numeric text expands exponents without changing values", {
  values <- c(
    as.numeric(c("-0.0", "0.0")),
    1.5,
    1e-7,
    -1e-7,
    5e-7,
    1e20,
    -1e20,
    as.numeric("0x1.a4a3bd7d8804dp+709"),
    .Machine$double.xmin * .Machine$double.eps,
    -.Machine$double.xmin * .Machine$double.eps,
    .Machine$double.xmax
  )

  encoded <- vapply(values, fabric_format_number_fixed, character(1))

  expect_identical(
    grepl("^-?[0-9]+(?:\\.[0-9]+)?$", encoded),
    rep(TRUE, length(values))
  )
  expect_identical(
    writeBin(as.numeric(encoded), raw(), size = 8L),
    writeBin(values, raw(), size = 8L)
  )
  expect_identical(fabric_format_number_fixed(1e20), "100000000000000000000")
  expect_identical(
    fabric_format_number_fixed(1e-7),
    "0.000000099999999999999995"
  )
  expect_identical(
    fabric_format_number_fixed(-1e-7),
    "-0.000000099999999999999995"
  )
})

test_that("numeric literal formatting rejects values requiring typed dispatch", {
  for (value in list(
    NA_real_,
    NaN,
    Inf,
    -Inf,
    numeric(),
    c(1, 2),
    "1",
    TRUE,
    1 + 2i,
    bit64::as.integer64("9007199254740993"),
    as.Date("2026-01-01")
  )) {
    error <- rlang::catch_cnd(fabric_format_number(value))
    expect_s3_class(error, "fabric_numeric_format_error")
  }
})
