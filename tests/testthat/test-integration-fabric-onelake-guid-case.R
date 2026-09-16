# Fabric integration coverage: onelake guid case
test_that("OneLake accepts uppercase workspace and item GUIDs in listings", {
  manifest <- fabric_test_manifest()
  lake <- fabric_test_manifest_item(manifest, "TestLakehouse")
  provider <- fabric_test_token_provider()
  list_files <- function(workspace, item) {
    result <- fabric_onelake_list(
      workspace,
      item,
      path = "Files",
      token = provider
    )
    result[order(result$path), c("path", "is_directory", "content_length")]
  }
  expected <- list_files(tolower(manifest$workspace_id), tolower(lake$id))
  expect_gt(nrow(expected), 0L)
  expect_identical(
    list_files(toupper(manifest$workspace_id), toupper(lake$id)),
    expected
  )
})
