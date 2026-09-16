# Fabric integration coverage: stored OneLake content encoding
# This fixture creates and removes only its own file; it needs no seeded tables.
test_that("OneLake preserves files with gzip content encoding", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  path <- paste0(
    "Files/fabricqueryr-encoding-",
    kusto_ingestion_source_id(),
    ".gz"
  )
  original <- withr::local_tempfile(fileext = ".gz")
  connection <- gzfile(original, "wb")
  writeBin(
    charToRaw("OneLake must preserve these compressed bytes"),
    connection
  )
  close(connection)
  expected <- readBin(original, "raw", n = file.info(original)$size)
  uploaded <- fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    path,
    source = original,
    token = token
  )
  withr::defer(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    path,
    confirm = TRUE,
    token = token
  ))
  target <- onelake_resolve_target(manifest$workspace_id, lakehouse$id, path)
  request <- onelake_request(
    onelake_path_url(target),
    "PATCH",
    headers = list(`x-ms-content-encoding` = "gzip", `If-Match` = uploaded$etag)
  ) |>
    httr2::req_url_query(action = "setProperties") |>
    httr2::req_body_raw(raw())
  .httr2_perform(
    request,
    credential = fabric_credential(token = token),
    audience = .fabric_audience$storage,
    idempotent = FALSE
  )
  headers <- .httr2_perform(
    onelake_request(onelake_path_url(target), "HEAD"),
    credential = fabric_credential(token = token),
    audience = .fabric_audience$storage
  )
  expect_identical(httr2::resp_header(headers, "content-encoding"), "gzip")
  expect_identical(
    fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      path,
      token = token
    ),
    expected
  )
  dest <- withr::local_tempfile(fileext = ".gz")
  fabric_onelake_download(
    manifest$workspace_id,
    lakehouse$id,
    path,
    dest = dest,
    token = token
  )
  expect_identical(readBin(dest, "raw", n = file.info(dest)$size), expected)
  expect_identical(
    fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      path,
      range = c(0, 9),
      token = token
    ),
    expected[1:10]
  )
})
