test_that("external shortcut matrix cannot silently omit a required provider", {
  withr::local_envvar(c(
    FABRIC_TEST_REQUIRED_FEATURES = "external-shortcuts",
    FABRIC_TEST_EXTERNAL_SHORTCUT_MATRIX_JSON = '{"amazonS3":{"target":{"amazonS3":{"connectionId":"fixture"}}}}',
    FABRIC_TEST_EXTERNAL_SHORTCUT_JSON = NA
  ))
  expect_named(fabric_test_external_shortcut("amazonS3")$target, "amazonS3")
  expect_error(
    fabric_test_external_shortcut("adlsGen2"),
    "missing for provider adlsGen2",
    fixed = TRUE
  )
  withr::local_envvar(c(
    FABRIC_TEST_EXTERNAL_SHORTCUT_MATRIX_JSON = '{"adlsGen2":{"target":{"amazonS3":{}}}}'
  ))
  expect_error(fabric_test_external_shortcut("adlsGen2"), "does not match")
})
