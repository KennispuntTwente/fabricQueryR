test_that("dynamic parameters reject implicit temporal and nonfinite conversions", {
  stamp <- as.POSIXct("2026-09-19 12:13:14", tz = "Europe/Amsterdam") + 0.123456
  values <- list(
    stamp,
    as.POSIXlt(stamp),
    c(stamp, stamp),
    as.Date("2026-09-19"),
    as.difftime(1, units = "hours"),
    Inf,
    -Inf,
    NaN,
    c(1, Inf)
  )
  for (value in values) {
    for (nested in list(list(value = value), list(outer = list(value)))) {
      error <- rlang::catch_cnd(fabric_kql_query(
        "https://cluster.kusto.fabric.microsoft.com",
        "declare query_parameters(x:dynamic); print x",
        database = "Events",
        parameters = list(x = nested),
        token = function(...) stop("Unexpected token acquisition")
      ))
      expect_s3_class(error, "fabric_kql_parameter_error")
    }
  }
  for (value in list(c(Inf, 1), c(NaN, 1))) {
    expect_s3_class(
      rlang::catch_cnd(kusto_encode_parameter(value)),
      "fabric_kql_parameter_error"
    )
  }
  expect_identical(
    kusto_encode_parameter(stamp),
    "datetime(2026-09-19T10:13:14.1234560Z)"
  )
  expect_identical(kusto_encode_parameter(Inf), "real(+inf)")
  expect_identical(kusto_encode_parameter(NaN), "real(nan)")
  expect_identical(
    kusto_encode_parameter(list(moment = "2026-09-19T10:13:14.1234560Z")),
    'dynamic({"moment":"2026-09-19T10:13:14.1234560Z"})'
  )
})

test_that("dynamic integer64 missingness respects the class at every depth", {
  skip_if_not_installed("bit64")
  for (text in c("-1", "9223372036854775807", "-9223372036854775807")) {
    value <- bit64::as.integer64(text)
    expect_identical(
      kusto_encode_parameter(list(value = value)),
      paste0('dynamic({"value":', text, '})')
    )
    expect_identical(
      kusto_encode_parameter(list(outer = list(value = value))),
      paste0('dynamic({"outer":{"value":', text, '}})')
    )
    expect_identical(
      kusto_encode_parameter(list(value = c(value, value))),
      paste0('dynamic({"value":[', text, ',', text, ']})')
    )
  }
  for (missing in list(
    bit64::as.integer64(NA),
    NA_real_,
    NA_character_,
    NULL
  )) {
    expect_error(
      kusto_encode_parameter(list(value = missing)),
      "cannot be NULL or NA"
    )
    expect_error(
      kusto_encode_parameter(list(outer = list(value = missing))),
      "cannot be NULL or NA"
    )
  }
})
