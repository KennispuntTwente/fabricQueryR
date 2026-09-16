test_that("designated feature lanes fail when prerequisites are missing", {
  withr::local_envvar(c(
    FABRIC_TEST_REQUIRED_FEATURES = "introspection, functions",
    FABRIC_TEST_GATE_EXAMPLE = NA
  ))
  expect_true(fabric_test_feature_required("introspection"))
  expect_true(fabric_test_feature_required("functions"))
  expect_false(fabric_test_feature_required("external-shortcuts"))
  expect_error(
    fabric_test_feature_environment(
      "introspection",
      "FABRIC_TEST_GATE_EXAMPLE"
    ),
    "requires FABRIC_TEST_GATE_EXAMPLE",
    fixed = TRUE
  )
  expect_condition(
    fabric_test_feature_environment(
      "external-shortcuts",
      "FABRIC_TEST_GATE_EXAMPLE"
    ),
    class = "skip"
  )
  withr::local_envvar(c(
    FABRIC_TEST_REQUIRED_FEATURES = "all",
    FABRIC_TEST_GATE_EXAMPLE = "configured"
  ))
  expect_true(fabric_test_feature_required("external-shortcuts"))
  expect_identical(
    fabric_test_feature_environment(
      "introspection",
      "FABRIC_TEST_GATE_EXAMPLE"
    ),
    "configured"
  )
  withr::local_envvar(c(FABRIC_TEST_REQUIRED_FEATURES = "function"))
  expect_error(
    fabric_test_feature_required("functions"),
    "Unknown required Fabric features"
  )
})
