test_that("shared JSON serialization preserves only numeric negative zero", {
  negative_zero <- -0
  value <- list(
    scalar = negative_zero,
    nested = list(values = c(0, negative_zero, -0.5)),
    text = c("-0", "quoted -0 remains text")
  )

  encoded <- fabric_json_serialize(
    value,
    auto_unbox = TRUE,
    null = "null",
    digits = 22
  )

  expect_match(encoded, '"scalar":-0.0', fixed = TRUE)
  expect_match(encoded, '"values":[0,-0.0,-0.5]', fixed = TRUE)
  expect_match(encoded, '"text":["-0","quoted -0 remains text"]', fixed = TRUE)
  decoded <- jsonlite::fromJSON(encoded)
  expect_identical(
    writeBin(decoded$scalar, raw(), size = 8L),
    writeBin(negative_zero, raw(), size = 8L)
  )
})
