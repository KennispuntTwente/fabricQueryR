test_that("OneLake downloads preserve stored gzip bytes through real HTTP", {
  # Starts a local HTTP server in a subprocess.
  skip_on_cran()
  skip_if_not_installed("webfakes")
  app <- webfakes::new_app()
  app$get("/workspace/item/Files/content.gz", function(req, res) {
    path <- tempfile()
    on.exit(unlink(path), add = TRUE)
    connection <- gzfile(path, "wb")
    writeBin(charToRaw("A complete file kept compressed"), connection)
    close(connection)
    bytes <- readBin(path, "raw", n = file.info(path)$size)
    res$set_header("Content-Encoding", "gzip")
    res$set_header("Content-Type", "application/octet-stream")
    res$send(bytes)
  })
  server <- webfakes::new_app_process(app)
  withr::defer(server$stop())
  target <- structure(
    list(
      dfs_base = sub("/$", "", server$url()),
      workspace = "workspace",
      item = "item",
      path = "Files/content.gz"
    ),
    class = "fabric_onelake_target"
  )
  local_mocked_bindings(onelake_resolve_target = function(...) target)

  original <- withr::local_tempfile(fileext = ".gz")
  connection <- gzfile(original, "wb")
  writeBin(charToRaw("A complete file kept compressed"), connection)
  close(connection)
  expected <- readBin(original, "raw", n = file.info(original)$size)
  expect_identical(expected[1:2], as.raw(c(31, 139)))
  actual <- fabric_onelake_download(
    "workspace",
    "item.Lakehouse",
    "Files/content.gz",
    token = "test-token"
  )
  expect_identical(actual, expected)

  dest <- withr::local_tempfile(fileext = ".gz")
  fabric_onelake_download(
    "workspace",
    "item.Lakehouse",
    "Files/content.gz",
    dest = dest,
    token = "test-token"
  )
  expect_identical(readBin(dest, "raw", n = file.info(dest)$size), expected)
  connection <- gzfile(dest, "rb")
  withr::defer(close(connection))
  expect_identical(
    readBin(connection, "raw", n = 100L),
    charToRaw("A complete file kept compressed")
  )
})
