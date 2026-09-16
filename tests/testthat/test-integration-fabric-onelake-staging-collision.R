# Fabric integration coverage: OneLake upload staging collisions
# Force a staging-name collision with a file created by this test only.
test_that("OneLake upload preserves an existing staging file on collision", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  prefix <- paste0("Files/fabricqueryr-collision-", kusto_ingestion_source_id())
  existing <- paste0(prefix, "-existing.txt")
  destination <- paste0(prefix, "-destination.txt")
  expected <- charToRaw("This file belongs to the other uploader")
  fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    existing,
    source = expected,
    token = token
  )
  withr::defer(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    existing,
    confirm = TRUE,
    token = token
  ))
  staging <- onelake_resolve_target(
    manifest$workspace_id,
    lakehouse$id,
    existing
  )
  local_mocked_bindings(onelake_upload_temporary_target = function(target) {
    staging
  })
  error <- tryCatch(
    fabric_onelake_upload(
      manifest$workspace_id,
      lakehouse$id,
      destination,
      source = charToRaw("New upload"),
      token = token
    ),
    error = identity
  )
  expect_s3_class(error, "fabric_http_error")
  expect_true(error$status %in% c(409L, 412L))
  expect_identical(
    fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      existing,
      token = token
    ),
    expected
  )
  missing <- tryCatch(
    fabric_onelake_metadata(
      manifest$workspace_id,
      lakehouse$id,
      destination,
      token = token
    ),
    error = identity
  )
  expect_s3_class(missing, "fabric_http_error")
  expect_identical(missing$status, 404L)
})
